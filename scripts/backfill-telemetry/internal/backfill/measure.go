package backfill

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/sotream/evcharge-lab/scripts/backfill-telemetry/internal/gen"
)

type sizes struct{ table, index, toast, total int64 }

func hypertableSize(ctx context.Context, pool *pgxpool.Pool, name string) (sizes, error) {
	var s sizes
	err := pool.QueryRow(ctx, `SELECT coalesce(table_bytes,0)::bigint, coalesce(index_bytes,0)::bigint,
		coalesce(toast_bytes,0)::bigint, coalesce(total_bytes,0)::bigint FROM hypertable_detailed_size($1::regclass)`, name).
		Scan(&s.table, &s.index, &s.toast, &s.total)
	return s, err
}

func listChunks(ctx context.Context, pool *pgxpool.Pool, olderThan time.Time) ([]string, error) {
	return listChunksOf(ctx, pool, "meter_readings", olderThan)
}

// compressChunks compresses chunks with up to workers parallel connections. The first error
// cancels the rest, so a failing worker fails the run instead of leaving the feeder blocked.
func compressChunks(ctx context.Context, pool *pgxpool.Pool, workers int, chunks []string) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for _, ch := range chunks {
		g.Go(func() error {
			_, err := pool.Exec(gctx, "SELECT compress_chunk($1::regclass, if_not_compressed => true)", ch)
			return err
		})
	}
	return g.Wait()
}

func listChunksOf(ctx context.Context, pool *pgxpool.Pool, hypertable string, olderThan time.Time) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT c::text FROM show_chunks($1::regclass, older_than => $2::timestamptz) c`, hypertable, olderThan)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p*float64(len(sorted))+0.999999) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

// chunkStats summarises chunk count and total_bytes distribution
// (chunks_detailed_size covers the compressed chunk of a compressed one).
func chunkStats(ctx context.Context, pool *pgxpool.Pool) (map[string]any, error) {
	rows, err := pool.Query(ctx, `SELECT coalesce(total_bytes,0)::bigint FROM chunks_detailed_size('meter_readings')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var b []float64
	var sum float64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		b = append(b, float64(v))
		sum += float64(v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Float64s(b)
	var compressed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM timescaledb_information.chunks
		WHERE hypertable_name='meter_readings' AND is_compressed`).Scan(&compressed); err != nil {
		return nil, err
	}
	m := map[string]any{"label": "measured", "chunkCount": len(b), "compressedChunkCount": compressed, "totalBytes": int64(sum)}
	if len(b) > 0 {
		m["bytesMin"], m["bytesP50"], m["bytesP95"], m["bytesMax"], m["bytesMean"] =
			int64(b[0]), int64(pct(b, .5)), int64(pct(b, .95)), int64(b[len(b)-1]), int64(sum/float64(len(b)))
	}
	return m, nil
}

func latencyStats(label string, ms []float64) map[string]any {
	s := append([]float64(nil), ms...)
	sort.Float64s(s)
	return map[string]any{
		"label": "measured", "query": label, "runs": len(s),
		"p50": round(pct(s, .50), 3), "p95": round(pct(s, .95), 3), "p99": round(pct(s, .99), 3),
		"max": round(s[len(s)-1], 3),
	}
}

func timeRuns(n int, f func(i int) error) ([]float64, error) {
	for i := 0; i < 20; i++ { // warm-up, not recorded
		if err := f(i); err != nil {
			return nil, err
		}
	}
	out := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		if err := f(i); err != nil {
			return nil, err
		}
		out = append(out, float64(time.Since(t0))/float64(time.Millisecond))
	}
	return out, nil
}

func measureQueries(ctx context.Context, pool *pgxpool.Pool, cfg *Config, rep *Report, start, end, cutoff time.Time) error {
	rng := rand.New(rand.NewPCG(cfg.Seed+99, 7))
	n := cfg.QueryRuns
	drain := func(sql string, args ...any) error {
		rows, err := pool.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
		}
		return rows.Err()
	}

	// 1. latest value per connector for one station
	stations := make([]string, n+20)
	for i := range stations {
		stations[i] = gen.StationID(rng.IntN(cfg.Stations))
	}
	ms, err := timeRuns(n, func(i int) error {
		return drain(`SELECT c.connector_id, l.time, l.power_w, l.energy_wh, l.soc_pct
			FROM unnest(ARRAY[1,2]::smallint[]) AS c(connector_id)
			CROSS JOIN LATERAL (SELECT time, power_w, energy_wh, soc_pct FROM meter_readings
				WHERE station_id = $1 AND connector_id = c.connector_id ORDER BY time DESC LIMIT 1) l`, stations[i%len(stations)])
	})
	if err != nil {
		return fmt.Errorf("latest query: %w", err)
	}
	rep.Queries["latestPerConnectorForStation"] = latencyStats("latest reading per connector, one station (LATERAL ... ORDER BY time DESC LIMIT 1)", ms)

	// 2. one session's readings by transaction_id + time range
	type sess struct {
		tx     int64
		lo, hi time.Time
	}
	pick := func(recent bool) ([]sess, error) {
		var out []sess
		for len(out) < n+20 {
			lo, hi := start, cutoff
			if recent {
				lo, hi = cutoff, end
			}
			if !hi.After(lo.Add(time.Hour)) {
				return nil, nil
			}
			t := lo.Add(time.Duration(rng.Int64N(int64(hi.Sub(lo)))))
			var tx int64
			var at time.Time
			err := pool.QueryRow(ctx, `SELECT transaction_id, time FROM meter_readings
				WHERE station_id=$1 AND connector_id=$2 AND time >= $3 AND time < $3 + interval '6 hours' AND transaction_id IS NOT NULL
				ORDER BY time LIMIT 1`, gen.StationID(rng.IntN(cfg.Stations)), int16(1+rng.IntN(2)), t).Scan(&tx, &at)
			if err != nil {
				continue // no session in that window
			}
			out = append(out, sess{tx, at.Add(-5 * time.Minute), at.Add(2 * time.Hour)})
		}
		return out, nil
	}
	for _, set := range []struct {
		key    string
		recent bool
		desc   string
	}{
		{"sessionReadingsCompressedChunks", false, "readings of one session by transaction_id and a 2 h time range, chunks older than 1 day (compressed)"},
		{"sessionReadingsRecentChunks", true, "readings of one session by transaction_id and a 2 h time range, last day (uncompressed)"},
	} {
		ss, err := pick(set.recent)
		if err != nil {
			return err
		}
		if ss == nil {
			rep.Queries[set.key] = map[string]any{"label": "measured", "skipped": "data range shorter than 1 day"}
			continue
		}
		ms, err := timeRuns(n, func(i int) error {
			s := ss[i%len(ss)]
			return drain(`SELECT time, power_w, energy_wh, current_a, voltage_v, soc_pct FROM meter_readings
				WHERE transaction_id = $1 AND time >= $2 AND time < $3 ORDER BY time`, s.tx, s.lo, s.hi)
		})
		if err != nil {
			return fmt.Errorf("session query: %w", err)
		}
		rep.Queries[set.key] = latencyStats(set.desc, ms)
	}

	// 3. power_1m, one site, one day
	days := int(end.Sub(start) / (24 * time.Hour))
	sites := (cfg.Stations + gen.StationsPerSite - 1) / gen.StationsPerSite
	type sd struct {
		stations []string
		day      time.Time
	}
	// Days entirely before the cutoff live in compressed power_1m chunks, the last day does not.
	for _, set := range []struct {
		key, desc string
		lo, hi    int // day index range [lo, hi)
	}{
		{"power1mOneSiteOneDayCompressed", "power_1m (materialized only) summed per bucket for the 10 stations of one site over one day, compressed chunks", 0, days - 1},
		{"power1mOneSiteOneDayRecent", "power_1m (materialized only) summed per bucket for the 10 stations of one site over one day, last day (uncompressed)", days - 1, days},
	} {
		if set.hi <= set.lo {
			rep.Queries[set.key] = map[string]any{"label": "measured", "skipped": "data range shorter than 2 days"}
			continue
		}
		sds := make([]sd, n+20)
		for i := range sds {
			site := rng.IntN(sites)
			var st []string
			for k := 0; k < gen.StationsPerSite && site*gen.StationsPerSite+k < cfg.Stations; k++ {
				st = append(st, gen.StationID(site*gen.StationsPerSite+k))
			}
			sds[i] = sd{st, start.Add(time.Duration(set.lo+rng.IntN(set.hi-set.lo)) * 24 * time.Hour)}
		}
		ms, err = timeRuns(n, func(i int) error {
			s := sds[i%len(sds)]
			return drain(`SELECT bucket, sum(avg_power_w)::bigint, sum(energy_wh_delta)::bigint FROM power_1m
				WHERE station_id = ANY($1) AND bucket >= $2 AND bucket < $2 + interval '1 day' GROUP BY bucket ORDER BY bucket`, s.stations, s.day)
		})
		if err != nil {
			return fmt.Errorf("power_1m query: %w", err)
		}
		rep.Queries[set.key] = latencyStats(set.desc, ms)
	}
	return nil
}

// derive extrapolates to the full ADR 0000 fleet. Everything here is
// computed from measured figures with the formula noted.
func derive(rep *Report, cfg Config, rows int64, dur time.Duration, beforeTotal, cb, ca, matBeforeTotal, matRows, mcb, mca, mcRows int64) {
	const fleet, rawDays, rollupDays, rollupHotDays = 50000.0, 90.0, 730.0, 7.0
	days := dur.Hours() / 24
	scale := fleet / float64(cfg.Stations)
	rowsPerDay := float64(rows) / days * scale
	bytesPerRowRaw := float64(beforeTotal) / float64(rows)
	bytesPerRowComp := bytesPerRowRaw
	if ca > 0 && cb > 0 {
		bytesPerRowComp = bytesPerRowRaw * float64(ca) / float64(cb)
	}
	rollRowsPerDay := float64(matRows) / days * scale
	bytesPerRollRaw := float64(matBeforeTotal) / float64(matRows)
	bytesPerRollComp := bytesPerRollRaw
	if mcRows > 0 && mca > 0 {
		bytesPerRollComp = float64(mca) / float64(mcRows) // measured compressed bytes per rollup row
	}
	gb := func(b float64) float64 { return round(b/1e9, 1) }
	rep.Derived["assumptions"] = Fig{Value: "50,000 stations, 90 days raw (last 1 day uncompressed), 2 years of power_1m (first 7 days uncompressed, then compressed), same per-station profile as measured", Label: "derived"}
	rep.Derived["rowsPerDayFullFleet"] = Fig{Value: int64(rowsPerDay), Unit: "rows/day", Label: "derived",
		Formula: "rowsInserted / days * (50000 / stations)"}
	rep.Derived["averageIngestRowsPerSecondFullFleet"] = Fig{Value: round(rowsPerDay/86400, 0), Unit: "rows/s", Label: "derived",
		Formula: "rowsPerDayFullFleet / 86400"}
	rep.Derived["bytesPerRowUncompressed"] = Fig{Value: round(bytesPerRowRaw, 1), Unit: "bytes", Label: "derived",
		Formula: "beforeCompressionTotalBytes / rowsInserted (table + indexes)"}
	rep.Derived["bytesPerRowCompressed"] = Fig{Value: round(bytesPerRowComp, 1), Unit: "bytes", Label: "derived",
		Formula: "bytesPerRowUncompressed * compressedChunksAfterBytes / compressedChunksBeforeBytes"}
	raw := rowsPerDay * (bytesPerRowComp*(rawDays-1) + bytesPerRowRaw)
	rep.Derived["raw90dSizeGB"] = Fig{Value: gb(raw), Unit: "GB", Label: "derived",
		Formula: "rowsPerDayFullFleet * (bytesPerRowCompressed * 89 + bytesPerRowUncompressed * 1)"}
	rep.Derived["rollupBytesPerRowUncompressed"] = Fig{Value: round(bytesPerRollRaw, 1), Unit: "bytes", Label: "derived",
		Formula: "power1mBeforeCompressionBytes / power1mRows"}
	rep.Derived["rollupBytesPerRowCompressed"] = Fig{Value: round(bytesPerRollComp, 1), Unit: "bytes", Label: "derived",
		Formula: "power1mCompressedChunksAfterBytes / power1mCompressedRows (measured compressed bytes per row)"}
	rollup := rollRowsPerDay * (rollupHotDays*bytesPerRollRaw + (rollupDays-rollupHotDays)*bytesPerRollComp)
	rep.Derived["rollup2ySizeGB"] = Fig{Value: gb(rollup), Unit: "GB", Label: "derived",
		Formula: "power1mRows / days * (50000 / stations) * (7 * rollupBytesPerRowUncompressed + 723 * rollupBytesPerRowCompressed)"}
	rep.Derived["totalSizeGB"] = Fig{Value: gb(raw + rollup), Unit: "GB", Label: "derived",
		Formula: "raw90dSizeGB + rollup2ySizeGB"}
}
