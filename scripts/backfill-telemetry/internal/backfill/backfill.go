// Package backfill loads generated history into the canonical TimescaleDB
// schema with COPY and measures storage and query behaviour (ADR 0013).
package backfill

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sotream/evcharge-lab/packages/platform-go/pg"
	"github.com/sotream/evcharge-lab/scripts/backfill-telemetry/internal/gen"
)

// Config is one backfill run.
type Config struct {
	DSN           string // target database (must be dedicated)
	MigrationsDir string // services/telemetry-ingest/migrations
	Stations      int
	Days          float64
	Start         time.Time // hour aligned; zero = now truncated to the hour minus Days
	Workers       int
	Seed          uint64
	DCShare       float64 // fleet DC share, default 0.2
	QueryRuns     int     // per query, >= 200 for the committed run
	Log           func(format string, args ...any)
}

// Fig is a labelled figure in the report.
type Fig struct {
	Value   any    `json:"value"`
	Unit    string `json:"unit,omitempty"`
	Label   string `json:"label"` // "measured" or "derived"
	Formula string `json:"formula,omitempty"`
}

func measured(v any, unit string) Fig { return Fig{Value: v, Unit: unit, Label: "measured"} }

// Report is the JSON written to docs/load-output.
type Report struct {
	Tool        string         `json:"tool"`
	GeneratedAt time.Time      `json:"generatedAt"`
	Config      map[string]any `json:"config"`
	Load        map[string]Fig `json:"load"`
	Size        map[string]Fig `json:"size"`
	Chunks      map[string]any `json:"chunks"`
	Queries     map[string]any `json:"queriesMs"`
	Derived     map[string]Fig `json:"fullFleetExtrapolation"`
	Notes       []string       `json:"notes"`
}

func (c *Config) logf(f string, a ...any) {
	if c.Log != nil {
		c.Log(f, a...)
	}
}

// ApplyMigrations applies the canonical migration files exactly as written, using the same
// runner as the services (schema_migrations, one transaction per file).
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool, dir string) error {
	_, err := pg.Migrate(ctx, pool, os.DirFS(dir), ".")
	return err
}

type rowSource struct {
	rows []gen.Row
	i    int
	vals [10]any
}

func (r *rowSource) Next() bool { r.i++; return r.i <= len(r.rows) }
func (r *rowSource) Err() error { return nil }
func (r *rowSource) Values() ([]any, error) {
	x := &r.rows[r.i-1]
	var soc any
	if x.SocPct != nil {
		soc = *x.SocPct
	}
	r.vals = [10]any{x.Time, x.StationID, x.ConnectorID, x.TransactionID, x.EnergyWh, x.PowerW, x.CurrentA, x.VoltageV, soc, false}
	return r.vals[:], nil
}

var copyCols = []string{"time", "station_id", "connector_id", "transaction_id", "energy_wh", "power_w", "current_a", "voltage_v", "soc_pct", "replayed"}

// Run executes the whole backfill and returns the report.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if cfg.DCShare == 0 {
		cfg.DCShare = gen.DefaultDCShare
	}
	if cfg.Workers < 1 {
		cfg.Workers = 4
	}
	if cfg.QueryRuns < 1 {
		cfg.QueryRuns = 300
	}
	dur := time.Duration(cfg.Days * 24 * float64(time.Hour))
	start := cfg.Start
	if start.IsZero() {
		start = time.Now().UTC().Truncate(time.Hour).Add(-dur)
	}
	start = start.UTC().Truncate(time.Hour)
	end := start.Add(dur).Truncate(time.Hour)
	if !end.After(start) {
		return nil, fmt.Errorf("empty time range")
	}

	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}
	pcfg.MaxConns = int32(cfg.Workers + 2)
	pcfg.ConnConfig.RuntimeParams["synchronous_commit"] = "off"
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, err
	}
	defer pool.Close()

	if err := ApplyMigrations(ctx, pool, cfg.MigrationsDir); err != nil {
		return nil, err
	}
	// Pause background jobs for the dedicated database so they do not
	// compete with (or alter) the measured phases; restored at the end.
	const jobFilter = `proc_name IN ('policy_compression','policy_refresh_continuous_aggregate','policy_retention')`
	if _, err := pool.Exec(ctx, `SELECT alter_job(job_id, scheduled => false) FROM timescaledb_information.jobs WHERE `+jobFilter); err != nil {
		return nil, err
	}
	defer func() {
		_, _ = pool.Exec(context.WithoutCancel(ctx), `SELECT alter_job(job_id, scheduled => true) FROM timescaledb_information.jobs WHERE `+jobFilter)
	}()

	rep := &Report{
		Tool: "scripts/backfill-telemetry", GeneratedAt: time.Now().UTC(),
		Config: map[string]any{
			"stations": cfg.Stations, "connectorsPerStation": gen.ConnectorsPerStation, "days": cfg.Days,
			"start": start, "end": end, "workers": cfg.Workers, "seed": cfg.Seed, "queryRuns": cfg.QueryRuns,
			"readingIntervalS": gen.Step.Seconds(), "meanUtilisation": gen.MeanUtilisation,
		},
		Load: map[string]Fig{}, Size: map[string]Fig{}, Chunks: map[string]any{}, Queries: map[string]any{},
		Derived: map[string]Fig{},
	}

	// ---- load
	var rowsTotal, genNs, copyNs atomic.Int64
	var wg sync.WaitGroup
	errc := make(chan error, cfg.Workers)
	loadStart := time.Now()
	for w := 0; w < cfg.Workers; w++ {
		lo, hi := cfg.Stations*w/cfg.Workers, cfg.Stations*(w+1)/cfg.Workers
		wg.Add(1)
		go func() {
			defer wg.Done()
			var conns []*gen.Connector
			for i := lo; i < hi; i++ {
				st := gen.NewStation(i, cfg.DCShare)
				for c := int16(1); c <= gen.ConnectorsPerStation; c++ {
					conns = append(conns, gen.NewConnector(st, c, cfg.Seed))
				}
			}
			buf := make([]gen.Row, 0, 64*1024)
			for s := start; s.Before(end); s = s.Add(time.Hour) {
				g0 := time.Now()
				buf = buf[:0]
				for _, c := range conns {
					c.Advance(s, s.Add(time.Hour), func(r gen.Row) { buf = append(buf, r) })
				}
				genNs.Add(int64(time.Since(g0)))
				if len(buf) == 0 {
					continue
				}
				c0 := time.Now()
				_, err := pool.CopyFrom(ctx, pgx.Identifier{"meter_readings"}, copyCols, &rowSource{rows: buf})
				copyNs.Add(int64(time.Since(c0)))
				if err != nil {
					errc <- err
					return
				}
				rowsTotal.Add(int64(len(buf)))
			}
		}()
	}
	wg.Wait()
	close(errc)
	if err := <-errc; err != nil {
		return nil, fmt.Errorf("load: %w", err)
	}
	loadDur := time.Since(loadStart)
	rows := rowsTotal.Load()
	cfg.logf("loaded %d rows in %s", rows, loadDur.Round(time.Millisecond))
	rep.Load["rowsInserted"] = measured(rows, "rows")
	rep.Load["wallSeconds"] = measured(round(loadDur.Seconds(), 2), "s")
	rep.Load["rowsPerSecond"] = measured(round(float64(rows)/loadDur.Seconds(), 0), "rows/s")
	rep.Load["generateSecondsSummedOverWorkers"] = measured(round(float64(genNs.Load())/1e9, 2), "s")
	rep.Load["copySecondsSummedOverWorkers"] = measured(round(float64(copyNs.Load())/1e9, 2), "s")
	var dbRows int64
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM meter_readings").Scan(&dbRows); err != nil {
		return nil, err
	}
	rep.Load["rowCountInDatabase"] = measured(dbRows, "rows")

	// ---- sizes and chunks before compression
	sizeBefore, err := hypertableSize(ctx, pool, "meter_readings")
	if err != nil {
		return nil, err
	}
	rep.Size["beforeCompressionTotalBytes"] = measured(sizeBefore.total, "bytes")
	rep.Size["beforeCompressionTableBytes"] = measured(sizeBefore.table, "bytes")
	rep.Size["beforeCompressionIndexBytes"] = measured(sizeBefore.index, "bytes")
	rep.Chunks["beforeCompression"], err = chunkStats(ctx, pool)
	if err != nil {
		return nil, err
	}

	// ---- power_1m refresh (before compression: avoids decompressing)
	r0 := time.Now()
	for d := start; d.Before(end); d = d.Add(24 * time.Hour) {
		e := d.Add(24 * time.Hour)
		if e.After(end) {
			e = end
		}
		if _, err := pool.Exec(ctx, "CALL refresh_continuous_aggregate('power_1m', $1::timestamptz, $2::timestamptz)", d, e); err != nil {
			return nil, fmt.Errorf("refresh power_1m: %w", err)
		}
	}
	rep.Load["refreshPower1mSeconds"] = measured(round(time.Since(r0).Seconds(), 2), "s")
	cfg.logf("refreshed power_1m in %s", time.Since(r0).Round(time.Millisecond))

	// ---- compression of chunks older than 1 day (relative to the end of the data)
	cutoff := end.Add(-24 * time.Hour)
	chunks, err := listChunks(ctx, pool, cutoff)
	if err != nil {
		return nil, err
	}
	c0 := time.Now()
	if err := compressChunks(ctx, pool, cfg.Workers, chunks); err != nil {
		return nil, fmt.Errorf("compress: %w", err)
	}
	compDur := time.Since(c0)
	rep.Load["compressedChunks"] = measured(len(chunks), "chunks")
	rep.Load["compressionSeconds"] = measured(round(compDur.Seconds(), 2), "s")
	cfg.logf("compressed %d chunks in %s", len(chunks), compDur.Round(time.Millisecond))

	sizeAfter, err := hypertableSize(ctx, pool, "meter_readings")
	if err != nil {
		return nil, err
	}
	rep.Size["afterCompressionTotalBytes"] = measured(sizeAfter.total, "bytes")
	rep.Size["afterCompressionTableBytes"] = measured(sizeAfter.table, "bytes")
	rep.Size["afterCompressionIndexBytes"] = measured(sizeAfter.index, "bytes")
	rep.Size["afterCompressionToastBytes"] = measured(sizeAfter.toast, "bytes")
	if sizeAfter.total > 0 {
		rep.Size["overallRatioBeforeOverAfter"] = measured(round(float64(sizeBefore.total)/float64(sizeAfter.total), 2), "x")
	}
	var cb, ca int64
	err = pool.QueryRow(ctx, `SELECT coalesce(sum(before_compression_total_bytes),0)::bigint, coalesce(sum(after_compression_total_bytes),0)::bigint
		FROM hypertable_compression_stats('meter_readings')`).Scan(&cb, &ca)
	if err != nil {
		return nil, err
	}
	rep.Size["compressedChunksBeforeBytes"] = measured(cb, "bytes")
	rep.Size["compressedChunksAfterBytes"] = measured(ca, "bytes")
	if ca > 0 {
		rep.Size["compressedChunksRatioBeforeOverAfter"] = measured(round(float64(cb)/float64(ca), 2), "x")
	}
	rep.Chunks["afterCompression"], err = chunkStats(ctx, pool)
	if err != nil {
		return nil, err
	}

	// power_1m (materialized_only, compression after 7 days per migration 0003). The data span is
	// shorter than the policy age, so chunks older than 1 day are compressed here to measure the ratio.
	var matSchema, matName string
	if err := pool.QueryRow(ctx, `SELECT materialization_hypertable_schema, materialization_hypertable_name
		FROM timescaledb_information.continuous_aggregates WHERE view_name='power_1m'`).Scan(&matSchema, &matName); err != nil {
		return nil, err
	}
	mat := fmt.Sprintf("%s.%s", pgx.Identifier{matSchema}.Sanitize(), pgx.Identifier{matName}.Sanitize())
	var matRows int64
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+mat).Scan(&matRows); err != nil {
		return nil, err
	}
	matBefore, err := hypertableSize(ctx, pool, mat)
	if err != nil {
		return nil, err
	}
	matChunks, err := listChunksOf(ctx, pool, mat, cutoff)
	if err != nil {
		return nil, err
	}
	if err := compressChunks(ctx, pool, cfg.Workers, matChunks); err != nil {
		return nil, fmt.Errorf("compress power_1m: %w", err)
	}
	matAfter, err := hypertableSize(ctx, pool, mat)
	if err != nil {
		return nil, err
	}
	var mcb, mca, mcRows int64
	if err := pool.QueryRow(ctx, `SELECT coalesce(sum(before_compression_total_bytes),0)::bigint, coalesce(sum(after_compression_total_bytes),0)::bigint
		FROM hypertable_compression_stats($1::regclass)`, mat).Scan(&mcb, &mca); err != nil {
		return nil, err
	}
	if len(matChunks) > 0 {
		// rows living in the compressed chunks: rows with bucket older than the cutoff
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+mat+" WHERE bucket < $1::timestamptz", cutoff).Scan(&mcRows); err != nil {
			return nil, err
		}
	}
	rep.Size["power1mRows"] = measured(matRows, "rows")
	rep.Size["power1mBeforeCompressionBytes"] = measured(matBefore.total, "bytes")
	rep.Size["power1mAfterCompressionBytes"] = measured(matAfter.total, "bytes")
	rep.Size["power1mCompressedChunks"] = measured(len(matChunks), "chunks")
	rep.Size["power1mCompressedChunksBeforeBytes"] = measured(mcb, "bytes")
	rep.Size["power1mCompressedChunksAfterBytes"] = measured(mca, "bytes")
	rep.Size["power1mCompressedRows"] = measured(mcRows, "rows")
	if mca > 0 {
		rep.Size["power1mCompressedChunksRatioBeforeOverAfter"] = measured(round(float64(mcb)/float64(mca), 2), "x")
	}

	// ---- query latency
	if err := measureQueries(ctx, pool, &cfg, rep, start, end, cutoff); err != nil {
		return nil, err
	}

	// ---- derived full-fleet numbers
	derive(rep, cfg, rows, dur, sizeBefore.total, cb, ca, matBefore.total, matRows, mcb, mca, mcRows)
	rep.Notes = []string{
		"Background policy jobs (compression, cagg refresh, retention) were paused during the run and re-enabled afterwards; compression and refresh were run manually.",
		"power_1m was refreshed before compression to avoid decompressing every chunk; raw chunks and power_1m chunks older than 1 day before the end of the data were then compressed (the power_1m policy would compress after 7 days).",
		"Station-local time is approximated by UTC hours in the utilisation profile.",
		"Session-by-transaction queries on compressed chunks cannot use the partial transaction_id index; they are bounded by a time range.",
	}
	return rep, nil
}

func round(v float64, places int) float64 {
	p := 1.0
	for i := 0; i < places; i++ {
		p *= 10
	}
	return float64(int64(v*p+0.5)) / p
}
