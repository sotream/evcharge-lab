package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/sotream/evcharge-lab/packages/platform-go/telemetry"
)

// BatchHandler processes the records fetched for ONE partition in one poll,
// in offset order. Returning nil commits past the last record; returning an
// error commits nothing for that partition and redelivers the whole batch
// after a backoff (handlers must therefore be idempotent).
type BatchHandler func(ctx context.Context, records []*kgo.Record) error

// RecordHandler processes a single record.
type RecordHandler func(ctx context.Context, r *kgo.Record) error

// PerRecord adapts a RecordHandler to a BatchHandler. Each call gets a
// consumer span whose parent is the trace context found in the record's
// headers. The first failure aborts the batch (it is redelivered in full).
func PerRecord(h RecordHandler) BatchHandler {
	tracer := telemetry.Tracer("github.com/sotream/evcharge-lab/packages/platform-go/kafka")
	return func(ctx context.Context, records []*kgo.Record) error {
		for _, r := range records {
			rctx, span := tracer.Start(telemetry.ExtractKafka(ctx, r), r.Topic+" process",
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "kafka"),
					attribute.String("messaging.destination.name", r.Topic),
					attribute.Int("messaging.destination.partition.id", int(r.Partition)),
					attribute.Int64("messaging.kafka.offset", r.Offset),
				))
			err := h(rctx, r)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
			if err != nil {
				return fmt.Errorf("offset %d: %w", r.Offset, err)
			}
		}
		return nil
	}
}

// ConsumerConfig configures NewConsumer. Zero values pick the defaults noted.
type ConsumerConfig struct {
	Brokers  []string
	Group    string
	Topics   []string
	ClientID string
	// MaxPollRecords caps records returned by one poll across partitions (default 1000).
	MaxPollRecords int
	// FetchMaxWait is how long the broker may wait to fill a fetch (default 500ms).
	FetchMaxWait time.Duration
	// RetryBackoff is the initial pause after a failed batch; it doubles per
	// consecutive failing poll up to MaxRetryBackoff (defaults 200ms / 10s).
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
	// CommitTimeout bounds one synchronous offset commit (default 15s). A
	// failed commit is logged and superseded by the next successful commit.
	CommitTimeout time.Duration
	// StartAtLatest makes a group without committed offsets start at the end
	// of the log instead of the beginning.
	StartAtLatest bool
	// Registerer receives kafka_consumer_lag and
	// kafka_consumer_handler_errors_total (nil: metrics are not registered).
	Registerer prometheus.Registerer
	Logger     *slog.Logger
	// Opts are appended last and can override any default.
	Opts []kgo.Opt
}

// Consumer is a consumer-group member with at-least-once processing.
//
// Each poll is split per partition; partitions are handled concurrently,
// records within a partition strictly in order. After all handlers of a poll
// returned, successful partitions are committed synchronously and failed ones
// are rewound to their first unprocessed offset. Rebalances are blocked while
// a poll is being processed, so a partition is never committed by a member
// that lost it mid-batch.
//
// Broker or coordinator loss never stops Run: franz-go reconnects, follows
// new partition leaders and rejoins the group while Run logs fetch/commit
// errors and keeps polling with backoff. Only ctx cancellation or Close end it.
type Consumer struct {
	cl      *kgo.Client
	cfg     ConsumerConfig
	handler BatchHandler
	log     *slog.Logger
	lag     *prometheus.GaugeVec
	errs    *prometheus.CounterVec

	// procCtx is given to handlers. It is deliberately not derived from
	// Run's ctx so an in-flight batch can finish (and commit) during a
	// graceful stop; Close cancels it when its own deadline expires.
	procCtx    context.Context
	procCancel context.CancelFunc

	mu      sync.Mutex
	stop    chan struct{}
	stopped bool
	running bool
	done    chan struct{}
	closed  bool
}

// NewConsumer creates the group client (it connects lazily, on Run).
func NewConsumer(cfg ConsumerConfig, handler BatchHandler) (*Consumer, error) {
	switch {
	case len(cfg.Brokers) == 0:
		return nil, errors.New("kafka: no brokers configured")
	case cfg.Group == "":
		return nil, errors.New("kafka: consumer group is required")
	case len(cfg.Topics) == 0:
		return nil, errors.New("kafka: at least one topic is required")
	case handler == nil:
		return nil, errors.New("kafka: handler is required")
	}
	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = 1000
	}
	if cfg.FetchMaxWait <= 0 {
		cfg.FetchMaxWait = 500 * time.Millisecond
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 200 * time.Millisecond
	}
	if cfg.MaxRetryBackoff <= 0 {
		cfg.MaxRetryBackoff = 10 * time.Second
	}
	if cfg.CommitTimeout <= 0 {
		cfg.CommitTimeout = 15 * time.Second
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	c := &Consumer{
		cfg:     cfg,
		handler: handler,
		log:     logger.With("group", cfg.Group),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	c.procCtx, c.procCancel = context.WithCancel(context.Background())
	if err := c.initMetrics(cfg.Registerer); err != nil {
		return nil, err
	}

	reset := kgo.NewOffset().AtStart()
	if cfg.StartAtLatest {
		reset = kgo.NewOffset().AtEnd()
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(cfg.FetchMaxWait),
		kgo.ConsumeResetOffset(reset),
		kgo.OnPartitionsRevoked(c.forget),
		kgo.OnPartitionsLost(c.forget),
		kgo.WithLogger(newLogger(logger)),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	cl, err := kgo.NewClient(append(opts, cfg.Opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer: %w", err)
	}
	c.cl = cl
	return c, nil
}

func (c *Consumer) initMetrics(reg prometheus.Registerer) error {
	lag := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Records between the last processed offset and the partition high watermark.",
	}, []string{"group", "topic", "partition"})
	errs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consumer_handler_errors_total",
		Help: "Batches whose handler failed and that will be redelivered.",
	}, []string{"group", "topic"})
	var err error
	if c.lag, err = register(reg, lag); err != nil {
		return err
	}
	if c.errs, err = register(reg, errs); err != nil {
		return err
	}
	return nil
}

// register registers col, reusing an identical collector already present
// (several consumers may share one registry).
func register[T prometheus.Collector](reg prometheus.Registerer, col T) (T, error) {
	if reg == nil {
		return col, nil
	}
	if err := reg.Register(col); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(T); ok {
				return existing, nil
			}
		}
		return col, fmt.Errorf("kafka: register metrics: %w", err)
	}
	return col, nil
}

func (c *Consumer) forget(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	for topic, parts := range lost {
		for _, p := range parts {
			c.lag.DeleteLabelValues(c.cfg.Group, topic, strconv.Itoa(int(p)))
		}
	}
}

// Client exposes the underlying franz-go client.
func (c *Consumer) Client() *kgo.Client { return c.cl }

// ReadinessCheck reports an error when the consumer was stopped or the
// cluster cannot be reached.
func (c *Consumer) ReadinessCheck(ctx context.Context) error {
	c.mu.Lock()
	stopped := c.stopped
	c.mu.Unlock()
	if stopped {
		return errors.New("kafka: consumer stopped")
	}
	return c.cl.Ping(ctx)
}

// Run polls and processes until ctx is cancelled or Close is called, then
// returns nil after the in-flight batch finished and was committed. It may be
// called once.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.running || c.closed {
		c.mu.Unlock()
		return errors.New("kafka: consumer already running or closed")
	}
	c.running = true
	c.mu.Unlock()
	defer close(c.done)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-c.stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	failures := 0
	for {
		fetches := c.cl.PollRecords(ctx, c.cfg.MaxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			c.cl.AllowRebalance()
			c.markStopped()
			return nil
		}
		fetchErrs := 0
		fetches.EachError(func(topic string, p int32, err error) {
			fetchErrs++
			c.log.Warn("kafka fetch error", "topic", topic, "partition", p, "err", err)
		})

		failed := c.process(fetches)
		c.cl.AllowRebalance()

		// Broker loss surfaces as fetch errors while franz-go reconnects and
		// re-resolves leaders: keep polling, with backoff so that error-only
		// polls do not spin, instead of stopping.
		if !failed && (fetchErrs == 0 || fetches.NumRecords() > 0) {
			failures = 0
			continue
		}
		failures++
		wait := min(c.cfg.RetryBackoff<<min(failures-1, 16), c.cfg.MaxRetryBackoff)
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
}

type partResult struct {
	topic     string
	partition int32
	records   []*kgo.Record
	hwm       int64
	err       error
}

// process runs the handler for every partition of the poll, commits
// successes and rewinds failures. It reports whether any partition failed.
func (c *Consumer) process(fetches kgo.Fetches) bool {
	var results []*partResult
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) > 0 {
			results = append(results, &partResult{topic: p.Topic, partition: p.Partition, records: p.Records, hwm: p.HighWatermark})
		}
	})
	if len(results) == 0 {
		return false
	}

	var wg sync.WaitGroup
	for _, r := range results {
		wg.Go(func() { r.err = c.safeHandle(r.records) })
	}
	wg.Wait()

	var commit []*kgo.Record
	rewind := map[string]map[int32]kgo.EpochOffset{}
	for _, r := range results {
		if r.err == nil {
			last := r.records[len(r.records)-1]
			commit = append(commit, last)
			c.lag.WithLabelValues(c.cfg.Group, r.topic, strconv.Itoa(int(r.partition))).Set(float64(max(r.hwm-(last.Offset+1), 0)))
			continue
		}
		c.errs.WithLabelValues(c.cfg.Group, r.topic).Inc()
		first := r.records[0]
		c.log.Error("kafka batch failed, will redeliver", "topic", r.topic, "partition", r.partition,
			"from_offset", first.Offset, "records", len(r.records), "err", r.err)
		if rewind[r.topic] == nil {
			rewind[r.topic] = map[int32]kgo.EpochOffset{}
		}
		rewind[r.topic][r.partition] = kgo.EpochOffset{Epoch: first.LeaderEpoch, Offset: first.Offset}
	}

	if len(commit) > 0 {
		// procCtx, not Run's ctx: a graceful stop must not abort the commit
		// of work that is already done.
		cctx, cancel := context.WithTimeout(c.procCtx, c.cfg.CommitTimeout)
		if err := c.cl.CommitRecords(cctx, commit...); err != nil {
			c.log.Warn("kafka commit failed; records may be redelivered", "err", err)
		}
		cancel()
	}
	if len(rewind) > 0 {
		c.cl.SetOffsets(rewind)
	}
	return len(rewind) > 0
}

func (c *Consumer) safeHandle(records []*kgo.Record) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("handler panic: %v", v)
		}
	}()
	return c.handler(c.procCtx, records)
}

func (c *Consumer) markStopped() {
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()
}

// Close stops polling, waits for the in-flight batch to finish and commit
// (cancelling the handler context if ctx expires first), then leaves the
// group and closes the client. Safe to call more than once and without Run.
func (c *Consumer) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.stopped = true
	running := c.running
	close(c.stop)
	c.mu.Unlock()

	var err error
	if running {
		select {
		case <-c.done:
		case <-ctx.Done():
			err = fmt.Errorf("kafka: consumer did not stop in time: %w", ctx.Err())
			c.procCancel()
			<-c.done
		}
	}
	c.procCancel()
	c.cl.Close()
	return err
}
