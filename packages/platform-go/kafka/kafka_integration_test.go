package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sotream/evcharge-lab/packages/platform-go/internal/testenv"
)

const partitions = 3

// topic creates a unique topic (RF 3; the cluster does not auto-create
// topics) and deletes it when t ends.
func topic(t *testing.T) string {
	t.Helper()
	testenv.Require(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers(testenv.KafkaBrokers()...))
	if err != nil {
		t.Fatal(err)
	}
	adm := kadm.NewClient(cl)
	name := testenv.Unique("platform-go-test-")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := adm.CreateTopic(ctx, partitions, 3, nil, name)
	if err == nil {
		err = res.Err
	}
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() {
		_, _ = adm.DeleteTopics(context.Background(), name)
		cl.Close()
	})
	return name
}

type event struct {
	N int `json:"n"`
}

func produceN(t *testing.T, p *Producer, topic string, n int) {
	t.Helper()
	for i := range n {
		if err := p.ProduceJSON(context.Background(), topic, "k"+strconv.Itoa(i%7), event{N: i},
			[]kgo.RecordHeader{{Key: "x-test", Value: []byte("1")}}); err != nil {
			t.Fatal(err)
		}
	}
}

func newProducer(t *testing.T) *Producer {
	t.Helper()
	p, err := NewProducer(ProducerConfig{Brokers: testenv.KafkaBrokers(), ClientID: "platform-go-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close(context.Background()) })
	return p
}

func committed(t *testing.T, group, topic string) int64 {
	t.Helper()
	cl, _ := kgo.NewClient(kgo.SeedBrokers(testenv.KafkaBrokers()...))
	defer cl.Close()
	offs, err := kadm.NewClient(cl).FetchOffsets(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	offs.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic {
			sum += o.At
		}
	})
	return sum
}

func startConsumer(t *testing.T, cfg ConsumerConfig, h BatchHandler) (*Consumer, chan error) {
	t.Helper()
	cfg.Brokers = testenv.KafkaBrokers()
	cfg.FetchMaxWait = 100 * time.Millisecond
	cfg.RetryBackoff = 50 * time.Millisecond
	cfg.Logger = slog.New(slog.DiscardHandler)
	c, err := NewConsumer(cfg, h)
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- c.Run(context.Background()) }()
	return c, errc
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestIntegrationProduceConsumeAtLeastOnce(t *testing.T) {
	tp := topic(t)
	group := testenv.Unique("platform-go-test-g-")
	p := newProducer(t)
	if err := p.ReadinessCheck(context.Background()); err != nil {
		t.Fatal(err)
	}
	const n = 60
	produceN(t, p, tp, n)

	reg := prometheus.NewRegistry()
	var mu sync.Mutex
	seen := map[int]int{}
	var failedOnce sync.Map // partition -> struct{}: fail the first batch of every partition
	var batches atomic.Int32
	h := func(_ context.Context, recs []*kgo.Record) error {
		batches.Add(1)
		for _, r := range recs[1:] {
			if r.Partition != recs[0].Partition || r.Offset <= recs[0].Offset {
				return fmt.Errorf("batch not single-partition ordered")
			}
		}
		if _, loaded := failedOnce.LoadOrStore(recs[0].Partition, struct{}{}); !loaded {
			return fmt.Errorf("transient failure p%d", recs[0].Partition)
		}
		mu.Lock()
		defer mu.Unlock()
		for _, r := range recs {
			var e event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				return err
			}
			if hdr(r, "x-test") != "1" || hdr(r, "content-type") != "application/json" {
				return fmt.Errorf("missing headers: %v", r.Headers)
			}
			seen[e.N]++
		}
		return nil
	}
	c, errc := startConsumer(t, ConsumerConfig{Group: group, Topics: []string{tp}, Registerer: reg}, h)
	if err := c.ReadinessCheck(context.Background()); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "all records", 30*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == n
	})
	waitFor(t, "commits", 10*time.Second, func() bool { return committed(t, group, tp) == n })
	if v := testutil.ToFloat64(c.errs.WithLabelValues(group, tp)); v < 1 {
		t.Fatalf("handler errors metric = %v", v)
	}
	var lag float64
	for p := range partitions {
		lag += testutil.ToFloat64(c.lag.WithLabelValues(group, tp, strconv.Itoa(p)))
	}
	if lag != 0 {
		t.Fatalf("lag = %v", lag)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Close(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}

	// A new member of the same group only sees records produced afterwards.
	produceN(t, p, tp, 1) // n=0 again, key k0
	got := make(chan int, 10)
	c2, errc2 := startConsumer(t, ConsumerConfig{Group: group, Topics: []string{tp}}, PerRecord(func(_ context.Context, r *kgo.Record) error {
		got <- int(r.Offset)
		return nil
	}))
	select {
	case <-got:
	case <-time.After(30 * time.Second):
		t.Fatal("second consumer got nothing")
	}
	time.Sleep(500 * time.Millisecond)
	if extra := len(got); extra != 0 {
		t.Fatalf("committed records were redelivered (%d extra)", extra)
	}
	if err := c2.Close(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-errc2; err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationCloseWaitsForInFlightBatch(t *testing.T) {
	tp := topic(t)
	group := testenv.Unique("platform-go-test-g-")
	p := newProducer(t)
	produceN(t, p, tp, 5)

	started := make(chan struct{})
	var once sync.Once
	var finished atomic.Bool
	var count atomic.Int32
	h := func(ctx context.Context, recs []*kgo.Record) error {
		once.Do(func() { close(started) })
		select {
		case <-time.After(300 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		count.Add(int32(len(recs)))
		finished.Store(true)
		return nil
	}
	c, errc := startConsumer(t, ConsumerConfig{Group: group, Topics: []string{tp}, MaxPollRecords: 1}, h)
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("handler never started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if !finished.Load() {
		t.Fatal("in-flight batch was not allowed to finish")
	}
	if got := committed(t, group, tp); got != int64(count.Load()) {
		t.Fatalf("committed %d, processed %d", got, count.Load())
	}
}

func hdr(r *kgo.Record, k string) string {
	for _, h := range r.Headers {
		if h.Key == k {
			return string(h.Value)
		}
	}
	return ""
}
