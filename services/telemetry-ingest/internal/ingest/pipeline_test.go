package ingest

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func queued(p *Pipeline, rowsPer ...int) {
	for i, n := range rowsPer {
		p.queue = append(p.queue, item{rec: &kgo.Record{Topic: "t", Partition: int32(i % 2), Offset: int64(i)}, rows: make([]Row, n)})
		p.queuedRows += n
	}
}

func newTestPipeline(t *testing.T, flushRows int) *Pipeline {
	t.Helper()
	m, err := NewMetrics(nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{FlushRows: flushRows}
	cfg.defaults()
	return &Pipeline{cfg: cfg, m: m, log: cfg.Logger, hwm: map[tp]int64{}, kick: make(chan struct{}, 1), space: make(chan struct{}, 1)}
}

func TestTakeRespectsFlushRows(t *testing.T) {
	p := newTestPipeline(t, 10)
	queued(p, 4, 4, 4, 30, 1)
	b := p.take(false)
	if len(b) != 2 || countRows(b) != 8 {
		t.Fatalf("first batch %d records / %d rows", len(b), countRows(b))
	}
	if p.queuedRows != 35 || p.inflightRows != 8 {
		t.Fatalf("queued=%d inflight=%d", p.queuedRows, p.inflightRows)
	}
	p.finish(b)
	b = p.take(false)
	if len(b) != 1 || countRows(b) != 4 {
		t.Fatalf("second batch: %d", countRows(b))
	}
	p.finish(b)
	// A record bigger than FlushRows still goes alone.
	b = p.take(false)
	if len(b) != 1 || countRows(b) != 30 {
		t.Fatalf("oversized batch: %d", countRows(b))
	}
	// Failure puts the batch back in front, in order.
	p.putBack(b)
	if p.queue[0].rec.Offset != 3 || p.queuedRows != 31 || p.inflightRows != 0 {
		t.Fatalf("putBack: front=%d queued=%d inflight=%d", p.queue[0].rec.Offset, p.queuedRows, p.inflightRows)
	}
	b = p.take(true)
	if len(b) != 2 || p.queuedRows != 0 || p.inflightRows != 31 {
		t.Fatalf("take all: %d records", len(b))
	}
}

func TestDropPartitions(t *testing.T) {
	p := newTestPipeline(t, 10)
	queued(p, 1, 2, 3, 4) // partitions 0,1,0,1
	p.drop(map[string][]int32{"t": {1}})
	if len(p.queue) != 2 || p.queuedRows != 4 {
		t.Fatalf("queue=%d rows=%d", len(p.queue), p.queuedRows)
	}
	for _, it := range p.queue {
		if it.rec.Partition != 0 {
			t.Fatal("partition 1 should be gone")
		}
	}
}

func TestDeadLetterHeaders(t *testing.T) {
	p := newTestPipeline(t, 10)
	p.cfg.Group = "g"
	r := p.deadLetter(&item{rec: &kgo.Record{Topic: "telemetry.meter-values.v1", Partition: 3, Offset: 9, Key: []byte("k"), Value: []byte("v")}, bad: ErrDecode})
	if r.Topic != "telemetry.meter-values.v1.dlq" || string(r.Key) != "k" || headerValue(r, "dlq-reason") != "decode" ||
		headerValue(r, "dlq-source-offset") != "9" || headerValue(r, "dlq-consumer-group") != "g" {
		t.Fatalf("dlq record: %+v", r)
	}
}
