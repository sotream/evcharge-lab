package ingest_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sotream/evcharge-lab/services/telemetry-ingest/internal/ingest"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/internal/testinfra"
)

type harness struct {
	t      *testing.T
	topic  string
	group  string
	dsn    string
	pool   *pgxpool.Pool
	prod   *kgo.Client
	m      *ingest.Metrics
	pipe   *ingest.Pipeline
	cancel context.CancelFunc
	done   chan error
}

func quietLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func newHarness(t *testing.T, partitions int32) *harness {
	testinfra.Require(t)
	h := &harness{t: t, topic: testinfra.Topic(t, partitions), group: testinfra.Unique("tingest-grp-"), prod: testinfra.Producer(t)}
	h.dsn, h.pool = testinfra.TempDB(t)
	return h
}

// start runs a pipeline; wrap may replace the sink.
func (h *harness) start(cfg ingest.Config, wrap func(ingest.Sink) ingest.Sink) {
	h.t.Helper()
	ingestPool, err := ingest.ConnectIngestPool(context.Background(), h.dsn, "ingest-test", 4)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(ingestPool.Close)
	var sink ingest.Sink = &ingest.CopyWriter{Pool: ingestPool}
	if wrap != nil {
		sink = wrap(sink)
	}
	if h.m, err = ingest.NewMetrics(nil); err != nil {
		h.t.Fatal(err)
	}
	cfg.Brokers, cfg.Group, cfg.Topic, cfg.Logger = testinfra.Brokers(), h.group, h.topic, quietLogger()
	h.pipe, err = ingest.New(cfg, sink, &ingest.RedisLatest{Client: testinfra.Redis(h.t)}, h.m)
	if err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel, h.done = cancel, make(chan error, 1)
	go func() { h.done <- h.pipe.Run(ctx) }()
	h.t.Cleanup(h.stop)
}

func (h *harness) stop() {
	if h.cancel == nil {
		return
	}
	h.cancel()
	h.cancel = nil
	select {
	case err := <-h.done:
		if err != nil {
			h.t.Errorf("pipeline run: %v", err)
		}
	case <-time.After(30 * time.Second):
		h.t.Error("pipeline did not stop")
	}
	_ = h.pipe.Close(context.Background())
}

func (h *harness) produce(recs ...*kgo.Record) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := h.prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		h.t.Fatalf("produce: %v", err)
	}
}

func (h *harness) count(where string, args ...any) int {
	h.t.Helper()
	var n int
	if err := h.pool.QueryRow(context.Background(), "SELECT count(*) FROM meter_readings WHERE "+where, args...).Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	return n
}

func (h *harness) committed() int64 {
	cl, err := kgo.NewClient(kgo.SeedBrokers(testinfra.Brokers()...))
	if err != nil {
		h.t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	offs, err := kadm.NewClient(cl).FetchOffsets(ctx, h.group)
	if err != nil {
		return -1
	}
	var total int64
	offs.Each(func(o kadm.OffsetResponse) {
		if o.Topic == h.topic {
			total += o.At
		}
	})
	return total
}

func TestIngestDedupReplayedLatestAndDLQ(t *testing.T) {
	h := newHarness(t, 3)
	h.start(ingest.Config{}, nil)
	station := testinfra.Unique("IT-")
	now := time.Now().UTC().Truncate(time.Millisecond)

	var recs []*kgo.Record
	for i := range 20 {
		recs = append(recs, testinfra.MeterRecord(h.topic, testinfra.Reading(station, 1, now.Add(time.Duration(i-20)*time.Second), float64(1000+i), false)))
	}
	// A replayed reading that is NEWER than every live one must not become "latest".
	recs = append(recs, testinfra.MeterRecord(h.topic, testinfra.Reading(station, 1, now.Add(time.Minute), 99999, true)))
	recs = append(recs, &kgo.Record{Topic: h.topic, Key: []byte(station), Value: []byte(`{"not":"an envelope"}`)})
	h.produce(recs...)

	testinfra.Eventually(t, 30*time.Second, "21 rows", func() bool { return h.count("station_id = $1", station) == 21 })
	if n := h.count("station_id = $1 AND replayed", station); n != 1 {
		t.Fatalf("replayed rows = %d", n)
	}
	var energy int64
	var power int32
	if err := h.pool.QueryRow(context.Background(), `SELECT energy_wh, power_w FROM meter_readings WHERE station_id=$1 AND time=$2`, station, now.Add(-time.Second)).Scan(&energy, &power); err != nil {
		t.Fatal(err)
	}
	if power != 1019 || energy <= 0 {
		t.Fatalf("energy=%d power=%d", energy, power)
	}

	rdb := testinfra.Redis(t)
	key := ingest.PowerKey(station, 1)
	t.Cleanup(func() { rdb.Del(context.Background(), key) })
	// The Redis write follows the database commit by a moment.
	testinfra.Eventually(t, 10*time.Second, "latest in redis", func() bool { return rdb.Exists(context.Background(), key).Val() == 1 })
	v, err := rdb.HGetAll(context.Background(), key).Result()
	if err != nil || v["w"] != "1019" || v["ts"] != now.Add(-time.Second).Format(ingest.TSFormat) {
		t.Fatalf("latest = %v (%v)", v, err)
	}
	if ttl := rdb.TTL(context.Background(), key).Val(); ttl <= 0 || ttl > ingest.LatestTTL {
		t.Fatalf("ttl = %s", ttl)
	}
	testinfra.Eventually(t, 10*time.Second, "dead-lettered record", func() bool { return testutil.ToFloat64(h.m.DeadLettered) == 1 })
	testinfra.Eventually(t, 10*time.Second, "offsets committed", func() bool { return h.committed() == int64(len(recs)) })

	// Redelivery (same readings again, new event ids as a producer retry would
	// have): nothing new is stored, every row counts as deduplicated.
	var again []*kgo.Record
	for _, r := range recs[:21] {
		again = append(again, &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value})
	}
	h.produce(again...)
	testinfra.Eventually(t, 30*time.Second, "dedup of 21 rows", func() bool { return testutil.ToFloat64(h.m.DedupDropped) == 21 })
	if n := h.count("station_id = $1", station); n != 21 {
		t.Fatalf("rows after redelivery = %d", n)
	}
	if got := testutil.ToFloat64(h.m.RowsIngested); got != 21 {
		t.Fatalf("rows ingested = %v", got)
	}
	if h.m.Lag.WithLabelValues("durable") == nil {
		t.Fatal("lag histogram missing")
	}
}

func TestRestartResumesFromCommittedOffsets(t *testing.T) {
	h := newHarness(t, 3)
	h.start(ingest.Config{}, nil)
	station := testinfra.Unique("IT-")
	now := time.Now().UTC().Truncate(time.Millisecond)
	batch := func(from, n int) []*kgo.Record {
		var out []*kgo.Record
		for i := from; i < from+n; i++ {
			out = append(out, testinfra.MeterRecord(h.topic, testinfra.Reading(station, i%2+1, now.Add(time.Duration(i)*time.Millisecond), 500, false)))
		}
		return out
	}
	h.produce(batch(0, 100)...)
	testinfra.Eventually(t, 30*time.Second, "100 rows", func() bool { return h.count("station_id = $1", station) == 100 })
	h.stop()
	// Produced while down: the new instance continues after the last commit.
	h.produce(batch(100, 50)...)
	h.start(ingest.Config{}, nil)
	testinfra.Eventually(t, 30*time.Second, "150 rows", func() bool { return h.count("station_id = $1", station) == 150 })
	if d := testutil.ToFloat64(h.m.DedupDropped); d != 0 {
		t.Fatalf("restart re-processed %v committed rows", d)
	}
}

// failingSink fails the first n writes, like a Postgres outage.
type failingSink struct {
	inner ingest.Sink
	fail  atomic.Int32
}

func (s *failingSink) Write(ctx context.Context, rows []ingest.Row) (int64, error) {
	if s.fail.Add(-1) >= 0 {
		return 0, errors.New("injected: connection refused")
	}
	return s.inner.Write(ctx, rows)
}

func TestFailedFlushIsRetriedWithoutLoss(t *testing.T) {
	h := newHarness(t, 3)
	var fs *failingSink
	h.start(ingest.Config{RetryBackoff: 20 * time.Millisecond}, func(s ingest.Sink) ingest.Sink {
		fs = &failingSink{inner: s}
		fs.fail.Store(3)
		return fs
	})
	station := testinfra.Unique("IT-")
	now := time.Now().UTC().Truncate(time.Millisecond)
	var recs []*kgo.Record
	for i := range 200 {
		recs = append(recs, testinfra.MeterRecord(h.topic, testinfra.Reading(station, 1, now.Add(time.Duration(i)*time.Millisecond), 100, false)))
	}
	h.produce(recs...)
	testinfra.Eventually(t, 30*time.Second, "200 rows", func() bool { return h.count("station_id = $1", station) == 200 })
	if e := testutil.ToFloat64(h.m.FlushErrors); e < 3 {
		t.Fatalf("flush errors = %v", e)
	}
	testinfra.Eventually(t, 10*time.Second, "offsets committed", func() bool { return h.committed() == 200 })
}

// gatedSink blocks every write until the gate opens (a stalled Postgres).
type gatedSink struct {
	inner ingest.Sink
	gate  chan struct{}
	once  sync.Once
}

func (s *gatedSink) Write(ctx context.Context, rows []ingest.Row) (int64, error) {
	select {
	case <-s.gate:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return s.inner.Write(ctx, rows)
}

func TestBackpressurePausesFetchAndDrains(t *testing.T) {
	h := newHarness(t, 3)
	const maxBuffered, pollMax = 200, 20
	var gs *gatedSink
	h.start(ingest.Config{FlushRows: 50, MaxBufferedRows: maxBuffered, MaxPollRecords: pollMax, WriteTimeout: time.Minute}, func(s ingest.Sink) ingest.Sink {
		gs = &gatedSink{inner: s, gate: make(chan struct{})}
		return gs
	})
	station := testinfra.Unique("IT-")
	now := time.Now().UTC().Truncate(time.Millisecond)
	const total = 3000
	var recs []*kgo.Record
	for i := range total {
		recs = append(recs, testinfra.MeterRecord(h.topic, testinfra.Reading(station, i%4+1, now.Add(time.Duration(i)*time.Millisecond), 100, false)))
	}
	h.produce(recs...)

	testinfra.Eventually(t, 30*time.Second, "fetch paused", func() bool { return testutil.ToFloat64(h.m.Paused) == 1 })
	// Memory stays bounded while the sink is stalled: one poll may overshoot.
	for range 10 {
		if b := h.pipe.Buffered(); b > maxBuffered+pollMax {
			t.Fatalf("buffered %d rows > bound %d", b, maxBuffered+pollMax)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := h.count("station_id = $1", station); n != 0 {
		t.Fatalf("rows written while stalled: %d", n)
	}
	gs.once.Do(func() { close(gs.gate) })
	testinfra.Eventually(t, 60*time.Second, "all rows after drain", func() bool { return h.count("station_id = $1", station) == total })
	testinfra.Eventually(t, 10*time.Second, "fetch resumed", func() bool { return testutil.ToFloat64(h.m.Paused) == 0 })
}

func TestIngestConnectionHasSynchronousCommitOff(t *testing.T) {
	testinfra.Require(t)
	dsn, pool := testinfra.TempDB(t)
	ip, err := ingest.ConnectIngestPool(context.Background(), dsn, "ingest-test", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer ip.Close()
	var ingestSC, querySC string
	if err := ip.QueryRow(context.Background(), "SHOW synchronous_commit").Scan(&ingestSC); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), "SHOW synchronous_commit").Scan(&querySC); err != nil {
		t.Fatal(err)
	}
	if ingestSC != "off" || querySC != "on" {
		t.Fatalf("synchronous_commit ingest=%s query=%s", ingestSC, querySC)
	}
}

func TestRowByRowBaselineWritesSameRows(t *testing.T) {
	testinfra.Require(t)
	_, pool := testinfra.TempDB(t)
	p := func(v int32) *int32 { return &v }
	now := time.Now().UTC().Truncate(time.Millisecond)
	rows := []ingest.Row{{Time: now, StationID: "s", ConnectorID: 1, PowerW: p(1)}, {Time: now, StationID: "s", ConnectorID: 1, PowerW: p(1)}, {Time: now.Add(time.Second), StationID: "s", ConnectorID: 1}}
	n, err := (&ingest.RowByRowWriter{Pool: pool}).Write(context.Background(), rows)
	if err != nil || n != 2 {
		t.Fatalf("row-by-row inserted %d (%v)", n, err)
	}
	n, err = (&ingest.CopyWriter{Pool: pool}).Write(context.Background(), rows)
	if err != nil || n != 0 {
		t.Fatalf("copy after row-by-row inserted %d (%v)", n, err)
	}
}

// poisonSink makes PostgreSQL "reject" any batch containing the poison station,
// the way a row the database cannot store would (SQLSTATE 22021).
type poisonSink struct {
	inner  ingest.Sink
	poison string
}

func (s *poisonSink) Write(ctx context.Context, rows []ingest.Row) (int64, error) {
	for i := range rows {
		if rows[i].StationID == s.poison {
			return 0, fmt.Errorf("copy: %w", &pgconn.PgError{Code: "22021", Message: "invalid byte sequence for encoding \"UTF8\""})
		}
	}
	return s.inner.Write(ctx, rows)
}

func TestPoisonRecordIsBisectedAndDeadLettered(t *testing.T) {
	h := newHarness(t, 3)
	poison := testinfra.Unique("POISON-")
	h.start(ingest.Config{FlushInterval: 500 * time.Millisecond}, func(s ingest.Sink) ingest.Sink { return &poisonSink{inner: s, poison: poison} })
	station := testinfra.Unique("IT-")
	now := time.Now().UTC().Truncate(time.Millisecond)
	var recs []*kgo.Record
	for i := range 30 {
		st := station
		if i == 17 {
			st = poison
		}
		recs = append(recs, testinfra.MeterRecord(h.topic, testinfra.Reading(st, 1, now.Add(time.Duration(i)*time.Millisecond), 100, false)))
	}
	h.produce(recs...)

	testinfra.Eventually(t, 30*time.Second, "29 good rows", func() bool { return h.count("station_id = $1", station) == 29 })
	if n := h.count("station_id = $1", poison); n != 0 {
		t.Fatalf("poison rows stored: %d", n)
	}
	testinfra.Eventually(t, 10*time.Second, "offsets committed past the poison record", func() bool { return h.committed() == 30 })
	if got := testutil.ToFloat64(h.m.DeadLettered); got != 1 {
		t.Fatalf("dead-lettered = %v", got)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(testinfra.Brokers()...), kgo.ConsumeTopics(h.topic+".dlq"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fetches := cl.PollRecords(ctx, 1)
	if err := fetches.Err(); err != nil {
		t.Fatal(err)
	}
	var reason string
	fetches.EachRecord(func(r *kgo.Record) {
		for _, hd := range r.Headers {
			if hd.Key == "dlq-reason" {
				reason = string(hd.Value)
			}
		}
	})
	if reason != "write" {
		t.Fatalf("dlq-reason = %q", reason)
	}
}
