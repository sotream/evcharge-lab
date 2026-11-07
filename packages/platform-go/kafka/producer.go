// Package kafka wraps franz-go for the services: an idempotent JSON producer
// that propagates trace context in record headers, and a consumer-group
// runner with at-least-once semantics (offsets are committed only after the
// handler succeeded), batch handlers, a lag metric, a readiness check and
// graceful stop.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sotream/evcharge-lab/packages/platform-go/telemetry"
)

// ProducerConfig configures NewProducer.
type ProducerConfig struct {
	Brokers  []string
	ClientID string
	// Linger lets the client batch records per partition (default 5ms).
	Linger time.Duration
	// MaxRetries bounds how often a batch is retried on retriable broker
	// errors (0 = franz-go default: retry until ctx / DeliveryTimeout).
	// Set it (e.g. 3-5) when the caller must apply backpressure on
	// NOT_ENOUGH_REPLICAS: the broker error then surfaces from Produce and
	// IsNotEnoughReplicas reports true, instead of the call blocking until
	// its ctx deadline. Retries remain safe because writes are idempotent.
	MaxRetries int
	// DeliveryTimeout bounds the total time a record may spend buffered and
	// retried (delivery.timeout.ms; 0 = unbounded, rely on ctx).
	DeliveryTimeout time.Duration
	// Logger receives franz-go warnings and errors (default slog.Default()).
	Logger *slog.Logger
	// Opts are appended last and can override any default.
	Opts []kgo.Opt
}

// Producer produces records synchronously. Defaults: idempotent writes,
// acks=all, lz4 compression, 5ms linger.
type Producer struct {
	cl *kgo.Client
}

// NewProducer creates the client (it connects lazily).
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers configured")
	}
	linger := cfg.Linger
	if linger <= 0 {
		linger = 5 * time.Millisecond
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()), // required for idempotence (franz-go's default)
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerLinger(linger),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	if cfg.MaxRetries > 0 {
		opts = append(opts, kgo.RecordRetries(cfg.MaxRetries))
	}
	if cfg.DeliveryTimeout > 0 {
		opts = append(opts, kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout))
	}
	opts = append(opts, kgo.WithLogger(newLogger(cfg.Logger)))
	cl, err := kgo.NewClient(append(opts, cfg.Opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

// Client exposes the underlying franz-go client.
func (p *Producer) Client() *kgo.Client { return p.cl }

// Produce injects the trace context of ctx into r's headers and waits until
// the record is acknowledged.
func (p *Producer) Produce(ctx context.Context, r *kgo.Record) error {
	telemetry.InjectKafka(ctx, r)
	if err := p.cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		if IsNotEnoughReplicas(err) {
			return fmt.Errorf("kafka: produce to %s: %w: %w", r.Topic, ErrNotEnoughReplicas, err)
		}
		return fmt.Errorf("kafka: produce to %s: %w", r.Topic, err)
	}
	return nil
}

// ErrNotEnoughReplicas is wrapped into Produce errors caused by the broker
// rejecting acks=all writes because the ISR shrank below min.insync.replicas
// (NOT_ENOUGH_REPLICAS or NOT_ENOUGH_REPLICAS_AFTER_APPEND). Callers should
// treat it as backpressure: stop accepting work / return 503 and retry later.
var ErrNotEnoughReplicas = errors.New("kafka: not enough in-sync replicas")

// IsNotEnoughReplicas reports whether err stems from an ISR shortage.
func IsNotEnoughReplicas(err error) bool {
	return errors.Is(err, ErrNotEnoughReplicas) ||
		errors.Is(err, kerr.NotEnoughReplicas) ||
		errors.Is(err, kerr.NotEnoughReplicasAfterAppend)
}

// ProduceJSON marshals value to JSON and produces it to topic with key and
// the extra headers (may be nil); traceparent is added from ctx.
func (p *Producer) ProduceJSON(ctx context.Context, topic, key string, value any, headers []kgo.RecordHeader) error {
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("kafka: marshal %s value: %w", topic, err)
	}
	r := &kgo.Record{
		Topic:   topic,
		Key:     []byte(key),
		Value:   b,
		Headers: append([]kgo.RecordHeader{{Key: "content-type", Value: []byte("application/json")}}, headers...),
	}
	return p.Produce(ctx, r)
}

// ReadinessCheck pings the cluster.
func (p *Producer) ReadinessCheck(ctx context.Context) error { return p.cl.Ping(ctx) }

// Close flushes buffered records (bounded by ctx) and closes the client.
func (p *Producer) Close(ctx context.Context) error {
	err := p.cl.Flush(ctx)
	p.cl.Close()
	if err != nil {
		return fmt.Errorf("kafka: flush: %w", err)
	}
	return nil
}
