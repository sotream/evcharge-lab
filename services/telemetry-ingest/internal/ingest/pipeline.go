package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config configures a Pipeline. Zero values pick the ADR 0008 defaults.
type Config struct {
	Brokers  []string
	Group    string
	Topic    string
	ClientID string
	// FlushRows flushes a batch once this many rows are queued (default 5,000).
	FlushRows int
	// FlushInterval flushes a non-empty batch after this long (default 250 ms).
	FlushInterval time.Duration
	// MaxBufferedRows bounds queued + in-flight rows; at the bound fetching
	// pauses until a flush frees space (default 50,000).
	MaxBufferedRows int
	// MaxPollRecords caps records per poll (default 1,000).
	MaxPollRecords int
	// RetryBackoff / MaxRetryBackoff pace retries of a failed flush
	// (defaults 200 ms / 5 s). A failed batch is retried, never dropped.
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
	// WriteTimeout bounds one flush attempt (default 30 s).
	WriteTimeout time.Duration
	// RevokeFlushTimeout bounds the flush done when partitions are revoked
	// (default 10 s, well below the group rebalance timeout).
	RevokeFlushTimeout time.Duration
	// StartAtLatest starts a group without committed offsets at the log end.
	StartAtLatest bool
	// DLQSuffix is appended to the topic for undecodable records (default ".dlq").
	DLQSuffix string
	Logger    *slog.Logger
	// OnFlushed, if set, is called after every durable flush (bench hook).
	OnFlushed func(rows []Row, inserted int64, durableAt time.Time)
	// Opts are appended to the franz-go client options.
	Opts []kgo.Opt
}

func (c *Config) defaults() {
	if c.FlushRows <= 0 {
		c.FlushRows = 5000
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 250 * time.Millisecond
	}
	if c.MaxBufferedRows <= 0 {
		c.MaxBufferedRows = 50000
	}
	if c.MaxPollRecords <= 0 {
		c.MaxPollRecords = 1000
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 200 * time.Millisecond
	}
	if c.MaxRetryBackoff <= 0 {
		c.MaxRetryBackoff = 5 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 30 * time.Second
	}
	if c.RevokeFlushTimeout <= 0 {
		c.RevokeFlushTimeout = 10 * time.Second
	}
	if c.DLQSuffix == "" {
		c.DLQSuffix = ".dlq"
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type item struct {
	rec      *kgo.Record
	rows     []Row
	bad      error // decode failure: dead-letter, no rows
	queuedAt time.Time
}

type tp struct {
	topic     string
	partition int32
}

// Pipeline consumes telemetry.meter-values.v1 and writes batches through a
// Sink with at-least-once semantics: offsets are committed only after the
// batch's database transaction committed.
//
// platform-go's kafka.Consumer is not used because it commits per poll and
// partition; ADR 0008 batches across polls and partitions instead.
//
// One goroutine polls Kafka and appends decoded rows to a queue; another
// flushes the queue (one flush in flight) at FlushRows or FlushInterval.
// When queued + in-flight rows reach MaxBufferedRows the poller pauses the
// topic instead of growing memory; Kafka holds the backlog.
type Pipeline struct {
	cfg    Config
	cl     *kgo.Client
	sink   Sink
	latest LatestSink
	m      *Metrics
	log    *slog.Logger

	mu           sync.Mutex
	queue        []item
	queuedRows   int
	inflightRows int
	hwm          map[tp]int64

	flushMu sync.Mutex // one flush (or rebalance flush) at a time

	kick  chan struct{} // poller -> flusher: new rows
	space chan struct{} // flusher -> poller: rows became durable
	stop  chan struct{}

	stateMu sync.Mutex
	running bool
	stopped bool
}

// New creates the pipeline and its group client (connected lazily by Run).
// latest may be nil (no Redis).
func New(cfg Config, sink Sink, latest LatestSink, m *Metrics) (*Pipeline, error) {
	cfg.defaults()
	switch {
	case len(cfg.Brokers) == 0:
		return nil, errors.New("ingest: no brokers")
	case cfg.Group == "" || cfg.Topic == "":
		return nil, errors.New("ingest: group and topic are required")
	case sink == nil || m == nil:
		return nil, errors.New("ingest: sink and metrics are required")
	}
	p := &Pipeline{
		cfg: cfg, sink: sink, latest: latest, m: m,
		log:   cfg.Logger.With("group", cfg.Group, "topic", cfg.Topic),
		hwm:   map[tp]int64{},
		kick:  make(chan struct{}, 1),
		space: make(chan struct{}, 1),
		stop:  make(chan struct{}),
	}
	reset := kgo.NewOffset().AtStart()
	if cfg.StartAtLatest {
		reset = kgo.NewOffset().AtEnd()
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.ConsumeResetOffset(reset),
		kgo.FetchMaxWait(250 * time.Millisecond),
		kgo.OnPartitionsRevoked(p.onRevoked),
		kgo.OnPartitionsLost(p.onLost),
		kgo.RequiredAcks(kgo.AllISRAcks()), // DLQ produce
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	cl, err := kgo.NewClient(append(opts, cfg.Opts...)...)
	if err != nil {
		return nil, fmt.Errorf("ingest: kafka client: %w", err)
	}
	p.cl = cl
	return p, nil
}

// Client exposes the franz-go client (readiness, tests).
func (p *Pipeline) Client() *kgo.Client { return p.cl }

// ReadinessCheck fails once stopped or when no broker answers.
func (p *Pipeline) ReadinessCheck(ctx context.Context) error {
	p.stateMu.Lock()
	stopped := p.stopped
	p.stateMu.Unlock()
	if stopped {
		return errors.New("ingest pipeline stopped")
	}
	return p.cl.Ping(ctx)
}

// Buffered returns queued + in-flight rows.
func (p *Pipeline) Buffered() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.queuedRows + p.inflightRows
}

// Run polls and flushes until ctx is cancelled, then flushes what is queued
// (bounded by WriteTimeout) and returns nil. Call Close afterwards.
func (p *Pipeline) Run(ctx context.Context) error {
	p.stateMu.Lock()
	if p.running || p.stopped {
		p.stateMu.Unlock()
		return errors.New("ingest: pipeline already running or stopped")
	}
	p.running = true
	p.stateMu.Unlock()

	var flusher sync.WaitGroup
	flusher.Go(p.flushLoop)

	p.pollLoop(ctx)

	close(p.stop)
	flusher.Wait()
	// Final flush of whatever is still queued; anything not durable here is
	// simply not committed and will be redelivered.
	fctx, cancel := context.WithTimeout(context.Background(), p.cfg.WriteTimeout)
	defer cancel()
	if err := p.flushOnce(fctx, true); err != nil {
		p.log.Warn("final flush failed; records will be redelivered", "err", err)
	}
	p.stateMu.Lock()
	p.stopped = true
	p.stateMu.Unlock()
	return nil
}

// Close leaves the group and closes the client. Call after Run returned.
func (p *Pipeline) Close(context.Context) error {
	p.stateMu.Lock()
	p.stopped = true
	p.stateMu.Unlock()
	p.cl.Close()
	return nil
}

func (p *Pipeline) pollLoop(ctx context.Context) {
	paused := false
	fetchFailures := 0
	for {
		for p.Buffered() >= p.cfg.MaxBufferedRows {
			if !paused {
				p.cl.PauseFetchTopics(p.cfg.Topic)
				paused = true
				p.m.Paused.Set(1)
				p.log.Warn("buffer full, pausing fetch", "buffered_rows", p.Buffered())
			}
			select {
			case <-p.space:
			case <-ctx.Done():
				return
			}
		}
		if paused {
			p.cl.ResumeFetchTopics(p.cfg.Topic)
			paused = false
			p.m.Paused.Set(0)
			p.log.Info("buffer drained, resuming fetch", "buffered_rows", p.Buffered())
		}

		fetches := p.cl.PollRecords(ctx, p.cfg.MaxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			p.cl.AllowRebalance()
			return
		}
		errs := 0
		fetches.EachError(func(topic string, part int32, err error) {
			errs++
			p.log.Warn("kafka fetch error", "topic", topic, "partition", part, "err", err)
		})
		p.enqueue(fetches)
		p.cl.AllowRebalance()

		if errs > 0 && fetches.NumRecords() == 0 {
			fetchFailures++
			wait := min(p.cfg.RetryBackoff<<min(fetchFailures-1, 10), p.cfg.MaxRetryBackoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			continue
		}
		fetchFailures = 0
	}
}

func (p *Pipeline) enqueue(fetches kgo.Fetches) {
	now := time.Now()
	var items []item
	rows := 0
	hwm := map[tp]int64{}
	fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
		if len(fp.Records) == 0 {
			return
		}
		hwm[tp{fp.Topic, fp.Partition}] = fp.HighWatermark
		for _, rec := range fp.Records {
			it := item{rec: rec, queuedAt: now}
			mv, err := Decode(rec.Value)
			if err != nil {
				it.bad = err
			} else {
				it.rows = Map(mv, func(m string) { p.m.Unknown.WithLabelValues(KnownMeasurandLabel(m)).Inc() })
				if empty := len(mv.Readings) - len(it.rows); empty > 0 {
					p.m.EmptyReadings.Add(float64(empty))
				}
			}
			rows += len(it.rows)
			items = append(items, it)
		}
	})
	if len(items) == 0 {
		return
	}
	p.m.RecordsConsumed.Add(float64(len(items)))
	p.mu.Lock()
	p.queue = append(p.queue, items...)
	p.queuedRows += rows
	maps.Copy(p.hwm, hwm)
	buffered := p.queuedRows + p.inflightRows
	p.mu.Unlock()
	p.m.Buffered.Set(float64(buffered))
	signal(p.kick)
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (p *Pipeline) flushLoop() {
	failures := 0
	for {
		p.mu.Lock()
		n := len(p.queue)
		rows := p.queuedRows
		var oldest time.Time
		if n > 0 {
			oldest = p.queue[0].queuedAt
		}
		p.mu.Unlock()

		if n == 0 {
			select {
			case <-p.kick:
				continue
			case <-p.stop:
				return
			}
		}
		if rows < p.cfg.FlushRows {
			if wait := time.Until(oldest.Add(p.cfg.FlushInterval)); wait > 0 {
				t := time.NewTimer(wait)
				select {
				case <-p.kick:
					t.Stop()
					continue
				case <-p.stop:
					t.Stop()
					return
				case <-t.C:
				}
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), p.cfg.WriteTimeout)
		err := p.flushOnce(ctx, false)
		cancel()
		if err == nil {
			failures = 0
			continue
		}
		failures++
		wait := min(p.cfg.RetryBackoff<<min(failures-1, 10), p.cfg.MaxRetryBackoff)
		p.log.Error("flush failed, retrying", "err", err, "attempt", failures, "backoff", wait.String())
		select {
		case <-p.stop:
			return
		case <-time.After(wait):
		}
	}
}

// take moves records from the front of the queue into a batch: at least one
// record, then whole records while the batch stays within FlushRows (all
// when all is set).
func (p *Pipeline) take(all bool) []item {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.queue) == 0 {
		return nil
	}
	n, rows := 0, 0
	for n < len(p.queue) {
		r := len(p.queue[n].rows)
		if !all && n > 0 && rows+r > p.cfg.FlushRows {
			break
		}
		rows += r
		n++
	}
	batch := make([]item, n)
	copy(batch, p.queue[:n])
	p.queue = p.queue[n:]
	p.queuedRows -= rows
	p.inflightRows += rows
	return batch
}

// putBack returns a failed batch to the front of the queue, in order.
func (p *Pipeline) putBack(batch []item) {
	rows := countRows(batch)
	p.mu.Lock()
	p.queue = append(batch, p.queue...)
	p.queuedRows += rows
	p.inflightRows -= rows
	p.mu.Unlock()
}

func (p *Pipeline) finish(batch []item) {
	rows := countRows(batch)
	p.mu.Lock()
	p.inflightRows -= rows
	buffered := p.queuedRows + p.inflightRows
	p.mu.Unlock()
	p.m.Buffered.Set(float64(buffered))
	signal(p.space)
}

func countRows(batch []item) int {
	n := 0
	for i := range batch {
		n += len(batch[i].rows)
	}
	return n
}

// flushOnce writes one batch; on failure the batch goes back to the queue.
func (p *Pipeline) flushOnce(ctx context.Context, all bool) error {
	p.flushMu.Lock()
	defer p.flushMu.Unlock()
	batch := p.take(all)
	if len(batch) == 0 {
		return nil
	}
	if err := p.persist(ctx, batch); err != nil {
		p.m.FlushErrors.Inc()
		p.putBack(batch)
		return err
	}
	p.finish(batch)
	return nil
}

// persist dead-letters undecodable records, writes the rows in one
// transaction, then commits the offsets and refreshes the Redis latest values.
// When PostgreSQL rejects the data itself (a poison row), the batch is bisected,
// the offending records are dead-lettered and the rest is written.
func (p *Pipeline) persist(ctx context.Context, batch []item) error {
	start := time.Now()
	good := make([]*item, 0, len(batch))
	var dlq []*item
	for i := range batch {
		if batch[i].bad != nil {
			dlq = append(dlq, &batch[i])
			continue
		}
		good = append(good, &batch[i])
	}
	rows := rowsOf(good)
	inserted, err := p.sink.Write(ctx, rows)
	if err != nil && isDataError(err) {
		var rejected []*item
		rows, inserted, rejected, err = p.bisectWrite(ctx, good)
		dlq = append(dlq, rejected...)
	}
	if err != nil {
		return fmt.Errorf("write %d rows: %w", len(rows), err)
	}
	if err := p.deadLetterAll(ctx, dlq); err != nil {
		return err
	}
	durableAt := time.Now()
	p.m.FlushSeconds.Observe(durableAt.Sub(start).Seconds())
	p.m.FlushRows.Observe(float64(len(rows)))
	p.m.RowsIngested.Add(float64(inserted))
	p.m.DedupDropped.Add(float64(int64(len(rows)) - inserted))
	durable := p.m.Lag.WithLabelValues("durable")
	for i := range rows {
		if !rows[i].Replayed {
			durable.Observe(durableAt.Sub(rows[i].Time).Seconds())
		}
	}

	p.commit(ctx, batch)

	if p.latest != nil {
		if err := p.latest.WriteLatest(ctx, rows); err != nil {
			p.m.LatestErrors.Inc()
			p.log.Warn("latest power update failed", "err", err)
		} else {
			visibleAt := time.Now()
			visible := p.m.Lag.WithLabelValues("visible")
			for _, r := range NewestLive(rows) {
				visible.Observe(visibleAt.Sub(r.Time).Seconds())
			}
		}
	}
	if p.cfg.OnFlushed != nil {
		p.cfg.OnFlushed(rows, inserted, durableAt)
	}
	return nil
}

// commit commits past the last record of every partition in the batch. A
// failed commit only means redelivery, which the unique index absorbs.
func (p *Pipeline) commit(ctx context.Context, batch []item) {
	last := map[tp]*kgo.Record{}
	for i := range batch {
		r := batch[i].rec
		k := tp{r.Topic, r.Partition}
		if cur, ok := last[k]; !ok || r.Offset > cur.Offset {
			last[k] = r
		}
	}
	recs := make([]*kgo.Record, 0, len(last))
	for _, r := range last {
		recs = append(recs, r)
	}
	if err := p.cl.CommitRecords(ctx, recs...); err != nil {
		p.log.Warn("offset commit failed; rows are durable and will be deduplicated on redelivery", "err", err)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, r := range last {
		if h, ok := p.hwm[k]; ok {
			p.m.ConsumerLag.WithLabelValues(p.cfg.Group, k.topic, strconv.Itoa(int(k.partition))).Set(float64(max(h-(r.Offset+1), 0)))
		}
	}
}

func rowsOf(items []*item) []Row {
	var n int
	for _, it := range items {
		n += len(it.rows)
	}
	rows := make([]Row, 0, n)
	for _, it := range items {
		rows = append(rows, it.rows...)
	}
	return rows
}

// isDataError reports a rejection of the data itself (SQLSTATE class 22 data
// exception, 23 integrity violation, 54 limit exceeded), which retrying the same
// rows can never fix. Connection and server availability errors are not data errors.
func isDataError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || len(pgErr.Code) < 2 {
		return false
	}
	switch pgErr.Code[:2] {
	case "22", "23", "54":
		return true
	}
	return false
}

// bisectWrite writes items in halves until the records PostgreSQL rejects are
// isolated. It returns the rows that were written, how many were new, and the
// rejected items (their bad field is set). Any non-data error aborts, so an
// outage never dead-letters healthy records.
func (p *Pipeline) bisectWrite(ctx context.Context, items []*item) (rows []Row, inserted int64, rejected []*item, err error) {
	if len(items) == 0 {
		return nil, 0, nil, nil
	}
	r := rowsOf(items)
	n, werr := p.sink.Write(ctx, r)
	switch {
	case werr == nil:
		return r, n, nil, nil
	case !isDataError(werr):
		return nil, 0, nil, werr
	case len(items) == 1:
		items[0].bad = fmt.Errorf("write rejected: %w", werr)
		return nil, 0, items, nil
	}
	mid := len(items) / 2
	rows1, n1, rej1, err := p.bisectWrite(ctx, items[:mid])
	if err != nil {
		return nil, 0, nil, err
	}
	rows2, n2, rej2, err := p.bisectWrite(ctx, items[mid:])
	if err != nil {
		return nil, 0, nil, err
	}
	return append(rows1, rows2...), n1 + n2, append(rej1, rej2...), nil
}

func (p *Pipeline) deadLetterAll(ctx context.Context, items []*item) error {
	if len(items) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, len(items))
	for i, it := range items {
		recs[i] = p.deadLetter(it)
	}
	if err := p.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("dead-letter %d records: %w", len(recs), err)
	}
	p.m.DeadLettered.Add(float64(len(recs)))
	for _, r := range recs {
		p.log.Warn("record dead-lettered", "dlq_topic", r.Topic, "reason", headerValue(r, "dlq-reason"), "err", headerValue(r, "dlq-error"))
	}
	return nil
}

func (p *Pipeline) deadLetter(it *item) *kgo.Record {
	reason := "write"
	if errors.Is(it.bad, ErrDecode) {
		reason = "decode"
	}
	src := it.rec
	headers := append([]kgo.RecordHeader{}, src.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: "dlq-reason", Value: []byte(reason)},
		kgo.RecordHeader{Key: "dlq-error", Value: []byte(truncate(it.bad.Error(), 1000))},
		kgo.RecordHeader{Key: "dlq-attempts", Value: []byte("0")},
		kgo.RecordHeader{Key: "dlq-source-topic", Value: []byte(src.Topic)},
		kgo.RecordHeader{Key: "dlq-source-partition", Value: []byte(strconv.Itoa(int(src.Partition)))},
		kgo.RecordHeader{Key: "dlq-source-offset", Value: []byte(strconv.FormatInt(src.Offset, 10))},
		kgo.RecordHeader{Key: "dlq-consumer-group", Value: []byte(p.cfg.Group)},
	)
	return &kgo.Record{Topic: src.Topic + p.cfg.DLQSuffix, Key: src.Key, Value: src.Value, Headers: headers}
}

func headerValue(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// onRevoked makes everything polled so far durable and committed before the
// partitions move (cooperative rebalance), so the new owner starts after it.
// If that fails, records of the revoked partitions are released (the new
// owner re-reads them from the last commit) and the rest stays queued.
func (p *Pipeline) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	p.flushMu.Lock()
	defer p.flushMu.Unlock()
	p.forgetLag(revoked)
	batch := p.take(true)
	if len(batch) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.RevokeFlushTimeout)
	defer cancel()
	if err := p.persist(ctx, batch); err != nil {
		p.m.FlushErrors.Inc()
		p.log.Warn("flush on revoke failed; releasing revoked partitions", "err", err)
		p.putBack(batch)
		p.drop(revoked)
		return
	}
	p.finish(batch)
}

// onLost drops buffered records of partitions this member no longer owns;
// their offsets were not committed, so the new owner redelivers them.
func (p *Pipeline) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	p.flushMu.Lock()
	defer p.flushMu.Unlock()
	p.forgetLag(lost)
	p.drop(lost)
}

func (p *Pipeline) drop(parts map[string][]int32) {
	gone := map[tp]bool{}
	for t, ps := range parts {
		for _, part := range ps {
			gone[tp{t, part}] = true
		}
	}
	p.mu.Lock()
	kept := p.queue[:0:0]
	dropped := 0
	for _, it := range p.queue {
		if gone[tp{it.rec.Topic, it.rec.Partition}] {
			p.queuedRows -= len(it.rows)
			dropped++
			continue
		}
		kept = append(kept, it)
	}
	p.queue = kept
	buffered := p.queuedRows + p.inflightRows
	p.mu.Unlock()
	if dropped > 0 {
		p.m.RebalanceDrops.Add(float64(dropped))
		p.log.Info("released buffered records of moved partitions", "records", dropped)
	}
	p.m.Buffered.Set(float64(buffered))
	signal(p.space)
}

func (p *Pipeline) forgetLag(parts map[string][]int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for t, ps := range parts {
		for _, part := range ps {
			delete(p.hwm, tp{t, part})
			p.m.ConsumerLag.DeleteLabelValues(p.cfg.Group, t, strconv.Itoa(int(part)))
		}
	}
}
