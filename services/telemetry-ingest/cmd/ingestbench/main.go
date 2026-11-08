// Command ingestbench measures one telemetry-ingest instance (ADR 0008): it
// produces MeterValuesReceived records to a dedicated, temporary topic at
// increasing rates and reports sustained rows/s and durable lag percentiles,
// for the COPY+staging writer and for the row-by-row INSERT baseline.
//
// It runs the real ingest.Pipeline in-process against the compose Kafka and a
// dedicated TimescaleDB database (never the shared "telemetry" one).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
	"github.com/sotream/evcharge-lab/packages/platform-go/pg"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/internal/ingest"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/migrations"
)

type step struct {
	Writer          string  `json:"writer"`
	TargetRowsPerS  int     `json:"target_rows_per_s"`
	DurationS       float64 `json:"produce_duration_s"`
	Produced        int64   `json:"rows_produced"`
	Inserted        int64   `json:"rows_inserted"`
	DrainedS        float64 `json:"time_to_drain_after_produce_s"`
	SustainedRowsPS float64 `json:"measured_sustained_rows_per_s"`
	LagP50S         float64 `json:"measured_durable_lag_p50_s"`
	LagP95S         float64 `json:"measured_durable_lag_p95_s"`
	LagP99S         float64 `json:"measured_durable_lag_p99_s"`
	LagMaxS         float64 `json:"measured_durable_lag_max_s"`
	KeepingUp       bool    `json:"keeping_up"`
	ProduceErrors   int64   `json:"produce_errors"`
}

type environment struct {
	GoVersion  string `json:"go"`
	NumCPU     int    `json:"num_cpu"`
	Partitions int    `json:"partitions"`
	Brokers    string `json:"brokers"`
	Flush      string `json:"flush"`
}

type report struct {
	GeneratedAt string         `json:"generated_at_utc"`
	Note        string         `json:"note"`
	Env         environment    `json:"environment"`
	Copy        []step         `json:"copy_staging"`
	Baseline    []step         `json:"row_by_row_baseline"`
	Summary     map[string]any `json:"summary"`
}

func main() {
	brokers := flag.String("brokers", envOr("TEST_KAFKA_BROKERS", "localhost:29091,localhost:29092,localhost:29093"), "Kafka brokers")
	pgURL := flag.String("pg", envOr("TEST_PG_TELEMETRY_URL", "postgres://evcharge:evcharge@localhost:25433/telemetry"), "admin DSN on the TimescaleDB instance (a dedicated database is created from it)")
	db := flag.String("db", "telemetry_ingestbench", "dedicated database name (dropped and recreated)")
	parts := flag.Int("partitions", 24, "topic partitions")
	dur := flag.Duration("duration", 20*time.Second, "produce duration per step")
	rates := flag.String("rates", "1000,2500,5000,10000,20000,40000", "COPY ladder, rows/s")
	baseRates := flag.String("baseline-rates", "100,250,500,1000,2500", "row-by-row ladder, rows/s")
	out := flag.String("out", "docs/load-output/telemetry-ingestbench.json", "output file")
	flag.Parse()

	if err := run(*brokers, *pgURL, *db, int32(*parts), *dur, parseInts(*rates), parseInts(*baseRates), *out); err != nil {
		fmt.Fprintln(os.Stderr, "ingestbench:", err)
		os.Exit(1)
	}
}

func run(brokers, adminURL, dbName string, parts int32, dur time.Duration, copyRates, baseRates []int, outPath string) error {
	ctx := context.Background()
	seeds := strings.Split(brokers, ",")
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close(ctx) }()
	if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)"); err != nil {
		return err
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		return err
	}
	defer func() { _, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)") }()
	u, err := url.Parse(adminURL)
	if err != nil {
		return err
	}
	u.Path = "/" + dbName
	dsn := u.String()

	setup, err := pg.Connect(ctx, pg.Config{URL: dsn, ApplicationName: "ingestbench-setup"})
	if err != nil {
		return err
	}
	defer setup.Close()
	if _, err := pg.Migrate(ctx, setup, migrations.FS, "."); err != nil {
		return err
	}
	// Measure insert cost, not background jobs.
	if _, err := setup.Exec(ctx, "SELECT remove_compression_policy('meter_readings')"); err != nil {
		slog.Warn("remove compression policy", "err", err)
	}
	if _, err := setup.Exec(ctx, "SELECT remove_retention_policy('meter_readings')"); err != nil {
		slog.Warn("remove retention policy", "err", err)
	}
	ingPool, err := ingest.ConnectIngestPool(ctx, dsn, "ingestbench", 4)
	if err != nil {
		return err
	}
	defer ingPool.Close()

	kcl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return err
	}
	defer kcl.Close()
	adm := kadm.NewClient(kcl)

	rep := report{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Note: "All figures measured by this tool in one process (producer + one telemetry-ingest pipeline instance) against the lab compose Kafka " +
			"(3 brokers, RF 3, acks=all) and TimescaleDB on the same Docker VM. Lag = durable DB commit time minus reading timestamp (stamped at produce time).",
		Env: environment{GoVersion: runtime.Version(), NumCPU: runtime.NumCPU(), Partitions: int(parts), Brokers: brokers, Flush: "5000 rows or 250 ms, buffer 50000 rows"},
	}

	for _, r := range copyRates {
		s, err := runStep(ctx, adm, seeds, setup, &ingest.CopyWriter{Pool: ingPool}, "copy_staging", r, parts, dur)
		if err != nil {
			return err
		}
		rep.Copy = append(rep.Copy, s)
		fmt.Printf("copy      target=%6d sustained=%8.0f rows/s lag p99=%.3fs keepingUp=%v\n", r, s.SustainedRowsPS, s.LagP99S, s.KeepingUp)
		if !s.KeepingUp {
			break
		}
	}
	for _, r := range baseRates {
		s, err := runStep(ctx, adm, seeds, setup, &ingest.RowByRowWriter{Pool: ingPool}, "row_by_row", r, parts, dur)
		if err != nil {
			return err
		}
		rep.Baseline = append(rep.Baseline, s)
		fmt.Printf("row-by-row target=%6d sustained=%8.0f rows/s lag p99=%.3fs keepingUp=%v\n", r, s.SustainedRowsPS, s.LagP99S, s.KeepingUp)
		if !s.KeepingUp {
			break
		}
	}
	rep.Summary = map[string]any{
		"copy_max_rate_keeping_up_rows_per_s":       maxKeepingUp(rep.Copy),
		"copy_peak_sustained_rows_per_s":            peak(rep.Copy),
		"row_by_row_max_rate_keeping_up_rows_per_s": maxKeepingUp(rep.Baseline),
		"row_by_row_peak_sustained_rows_per_s":      peak(rep.Baseline),
		"adr0000_target_rows_per_s":                 2500,
		"keeping_up_definition":                     "all produced rows durable within 5 s after the produce phase ended and durable lag p99 < 5 s",
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(outPath, append(b, '\n'), 0o644)
}

func maxKeepingUp(ss []step) int {
	m := 0
	for _, s := range ss {
		if s.KeepingUp && s.TargetRowsPerS > m {
			m = s.TargetRowsPerS
		}
	}
	return m
}

func peak(ss []step) float64 {
	m := 0.0
	for _, s := range ss {
		m = max(m, s.SustainedRowsPS)
	}
	return m
}

func runStep(ctx context.Context, adm *kadm.Client, seeds []string, db *pgxpool.Pool, sink ingest.Sink, name string, rate int, parts int32, dur time.Duration) (step, error) {
	topic := fmt.Sprintf("bench.telemetry-ingest.%d", time.Now().UnixNano())
	group := topic + ".g"
	cfg := map[string]*string{"min.insync.replicas": new("2")}
	if _, err := adm.CreateTopic(ctx, parts, 3, cfg, topic); err != nil {
		return step{}, fmt.Errorf("create topic: %w", err)
	}
	if _, err := adm.CreateTopic(ctx, 1, 3, cfg, topic+".dlq"); err != nil {
		return step{}, err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = adm.DeleteTopics(c, topic, topic+".dlq")
		_, _ = adm.DeleteGroups(c, group)
	}()
	if _, err := db.Exec(ctx, "TRUNCATE meter_readings"); err != nil {
		return step{}, err
	}

	var (
		mu   sync.Mutex
		lags []float64
		ins  atomic.Int64
		last atomic.Int64 // unix nano of the last durable flush
	)
	m, err := ingest.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		return step{}, err
	}
	pipe, err := ingest.New(ingest.Config{
		Brokers: seeds, Group: group, Topic: topic, ClientID: "ingestbench",
		Logger: slog.New(slog.DiscardHandler),
		OnFlushed: func(rows []ingest.Row, inserted int64, at time.Time) {
			mu.Lock()
			for i := range rows {
				if !rows[i].Replayed {
					lags = append(lags, at.Sub(rows[i].Time).Seconds())
				}
			}
			mu.Unlock()
			ins.Add(inserted)
			last.Store(at.UnixNano())
		},
	}, sink, nil, m)
	if err != nil {
		return step{}, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = pipe.Run(runCtx); close(done) }()
	defer func() { cancel(); <-done; _ = pipe.Close(context.Background()) }()
	time.Sleep(3 * time.Second) // let the group join before the clock starts

	prod, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ProducerLinger(5*time.Millisecond),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerBatchMaxBytes(1<<20), kgo.MaxBufferedRecords(200000))
	if err != nil {
		return step{}, err
	}
	defer prod.Close()

	var produced, perr atomic.Int64
	start := time.Now()
	var n int64
	for now := start; now.Sub(start) < dur; now = time.Now() {
		target := int64(float64(rate) * now.Sub(start).Seconds())
		for ; n < target; n++ {
			prod.Produce(ctx, record(topic, n, time.Now()), func(_ *kgo.Record, err error) {
				if err != nil {
					perr.Add(1)
				}
			})
			produced.Add(1)
		}
		time.Sleep(10 * time.Millisecond)
	}
	produceEnd := time.Now()
	if err := prod.Flush(ctx); err != nil {
		return step{}, err
	}
	// Wait for drain: all produced rows durable, or 15 s without progress.
	deadline := time.Now().Add(120 * time.Second)
	prev, prevAt := int64(-1), time.Now()
	for ins.Load() < produced.Load()-perr.Load() && time.Now().Before(deadline) {
		if cur := ins.Load(); cur != prev {
			prev, prevAt = cur, time.Now()
		} else if time.Since(prevAt) > 15*time.Second {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	endAt := time.Unix(0, last.Load())
	if endAt.Before(produceEnd) {
		endAt = produceEnd
	}
	mu.Lock()
	defer mu.Unlock()
	sort.Float64s(lags)
	pct := func(q float64) float64 {
		if len(lags) == 0 {
			return 0
		}
		return lags[min(len(lags)-1, int(q*float64(len(lags))))]
	}
	s := step{
		Writer: name, TargetRowsPerS: rate, DurationS: produceEnd.Sub(start).Seconds(),
		Produced: produced.Load(), Inserted: ins.Load(), DrainedS: endAt.Sub(produceEnd).Seconds(),
		SustainedRowsPS: float64(ins.Load()) / endAt.Sub(start).Seconds(),
		LagP50S:         pct(0.50), LagP95S: pct(0.95), LagP99S: pct(0.99), ProduceErrors: perr.Load(),
	}
	if len(lags) > 0 {
		s.LagMaxS = lags[len(lags)-1]
	}
	s.KeepingUp = s.Inserted >= s.Produced-s.ProduceErrors && s.DrainedS < 5 && s.LagP99S < 5
	return s, nil
}

func record(topic string, n int64, at time.Time) *kgo.Record {
	tx := 1 + n%50000
	st := fmt.Sprintf("st-%04d-%03d", 1+(n/2)%5000/10, 1+(n/2)%10)
	at = at.UTC().Truncate(time.Microsecond)
	mv := contracts.MeterValues{
		StationID: st, ConnectorID: int(1 + n%2), TransactionID: &tx, OcppMessageID: strconv.FormatInt(n, 10),
		Readings: []contracts.MeterReading{{Timestamp: at, Samples: []contracts.MeterSample{
			{Measurand: "Energy.Active.Import.Register", Value: float64(n%100000) / 10, Unit: "kWh"},
			{Measurand: "Power.Active.Import", Value: 7000, Unit: "W"},
			{Measurand: "Current.Import", Value: 30.4, Unit: "A"},
			{Measurand: "Voltage", Value: 230, Unit: "V"},
			{Measurand: "SoC", Value: 55, Unit: "Percent"},
		}}},
	}
	env := contracts.Envelope[contracts.MeterValues]{
		EventID: fmt.Sprintf("00000000-0000-4000-8000-%012d", n), Type: contracts.EventTypeMeterValues, Version: 1,
		OccurredAt: at, Producer: "ingestbench", Key: st, Data: mv,
	}
	b, err := json.Marshal(env)
	if err != nil {
		panic(err)
	}
	return &kgo.Record{Topic: topic, Key: []byte(st), Value: b}
}

func parseInts(s string) []int {
	var out []int
	for p := range strings.SplitSeq(s, ",") {
		if v, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
			out = append(out, v)
		}
	}
	return out
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
