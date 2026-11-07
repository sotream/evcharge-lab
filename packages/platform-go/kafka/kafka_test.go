package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/sotream/evcharge-lab/packages/platform-go/telemetry"
)

func noop(context.Context, []*kgo.Record) error { return nil }

func TestNewConsumerValidates(t *testing.T) {
	base := ConsumerConfig{Brokers: []string{"localhost:1"}, Group: "g", Topics: []string{"t"}}
	cases := map[string]func(*ConsumerConfig){
		"brokers": func(c *ConsumerConfig) { c.Brokers = nil },
		"group":   func(c *ConsumerConfig) { c.Group = "" },
		"topics":  func(c *ConsumerConfig) { c.Topics = nil },
	}
	for name, mut := range cases {
		cfg := base
		mut(&cfg)
		if _, err := NewConsumer(cfg, noop); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := NewConsumer(base, nil); err == nil {
		t.Error("nil handler: expected error")
	}
	if _, err := NewProducer(ProducerConfig{}); err == nil {
		t.Error("producer without brokers: expected error")
	}
}

func TestConsumersShareRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	for _, g := range []string{"a", "b"} {
		c, err := NewConsumer(ConsumerConfig{Brokers: []string{"localhost:1"}, Group: g, Topics: []string{"t"}, Registerer: reg}, noop)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCloseWithoutRunAndTwice(t *testing.T) {
	c, err := NewConsumer(ConsumerConfig{Brokers: []string{"localhost:1"}, Group: "g", Topics: []string{"t"}}, noop)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Run(context.Background()); err == nil {
		t.Fatal("Run after Close should fail")
	}
	if err := c.ReadinessCheck(context.Background()); err == nil {
		t.Fatal("closed consumer must not be ready")
	}
}

func TestIsNotEnoughReplicas(t *testing.T) {
	for _, err := range []error{
		kerr.NotEnoughReplicas,
		kerr.NotEnoughReplicasAfterAppend,
		fmt.Errorf("%w, last err: %w", kgo.ErrRecordTimeout, kerr.NotEnoughReplicas),
		fmt.Errorf("x: %w", ErrNotEnoughReplicas),
	} {
		if !IsNotEnoughReplicas(err) {
			t.Errorf("%v: want true", err)
		}
	}
	for _, err := range []error{nil, kerr.LeaderNotAvailable, context.DeadlineExceeded} {
		if IsNotEnoughReplicas(err) {
			t.Errorf("%v: want false", err)
		}
	}
}

// A shrunken ISR (the drill killing 2 of 3 brokers) makes acks=all produces
// fail with NOT_ENOUGH_REPLICAS. With MaxRetries bounded, the producer must
// surface it as ErrNotEnoughReplicas, then recover once the ISR is back.
// kfake (in-process fake cluster) injects the broker error.
func TestProduceSurfacesNotEnoughReplicas(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(3), kfake.SeedTopics(1, "events"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	var isrShort atomic.Bool
	isrShort.Store(true)
	cluster.ControlKey(kmsg.Produce.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if !isrShort.Load() {
			return nil, nil, false
		}
		preq := req.(*kmsg.ProduceRequest)
		resp := preq.ResponseKind().(*kmsg.ProduceResponse)
		for _, rt := range preq.Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic, st.TopicID = rt.Topic, rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.NotEnoughReplicas.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	p, err := NewProducer(ProducerConfig{Brokers: cluster.ListenAddrs(), MaxRetries: 2, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = p.ProduceJSON(ctx, "events", "k", map[string]int{"n": 1}, nil)
	if !errors.Is(err, ErrNotEnoughReplicas) || !IsNotEnoughReplicas(err) {
		t.Fatalf("want ErrNotEnoughReplicas, got %v", err)
	}

	isrShort.Store(false)
	if err := p.ProduceJSON(ctx, "events", "k", map[string]int{"n": 2}, nil); err != nil {
		t.Fatalf("produce after ISR recovered: %v", err)
	}
}

// Broker loss must not stop the runner: with no reachable broker Run keeps
// polling until Close.
func TestRunSurvivesUnreachableBrokers(t *testing.T) {
	c, err := NewConsumer(ConsumerConfig{
		Brokers: []string{"127.0.0.1:1"}, Group: "g", Topics: []string{"t"},
		RetryBackoff: 10 * time.Millisecond, Logger: slog.New(slog.DiscardHandler),
	}, noop)
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- c.Run(context.Background()) }()
	select {
	case err := <-errc:
		t.Fatalf("Run stopped on broker loss: %v", err)
	case <-time.After(time.Second):
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestPerRecordStopsAtFirstErrorAndContinuesTrace(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(telemetry.Propagator)
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	pctx, parent := telemetry.Tracer("t").Start(context.Background(), "producer")
	parent.End()
	r1 := &kgo.Record{Topic: "t", Offset: 1}
	telemetry.InjectKafka(pctx, r1)
	records := []*kgo.Record{r1, {Topic: "t", Offset: 2}, {Topic: "t", Offset: 3}}

	var seen []int64
	var firstTrace trace.TraceID
	h := PerRecord(func(ctx context.Context, r *kgo.Record) error {
		seen = append(seen, r.Offset)
		if r.Offset == 1 {
			firstTrace = trace.SpanContextFromContext(ctx).TraceID()
		}
		if r.Offset == 2 {
			return errors.New("bad record")
		}
		return nil
	})
	err := h(context.Background(), records)
	if err == nil || len(seen) != 2 {
		t.Fatalf("err=%v seen=%v", err, seen)
	}
	if firstTrace != parent.SpanContext().TraceID() {
		t.Fatal("record span is not a child of the producer trace")
	}
	if n := len(rec.Ended()); n != 3 { // producer + 2 record spans
		t.Fatalf("spans %d", n)
	}
}
