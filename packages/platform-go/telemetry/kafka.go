package telemetry

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
)

// KafkaHeaderCarrier adapts a record's headers to propagation.TextMapCarrier.
type KafkaHeaderCarrier struct {
	Record *kgo.Record
}

func (c KafkaHeaderCarrier) Get(key string) string {
	for _, h := range c.Record.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set replaces an existing header with the same key or appends a new one.
func (c KafkaHeaderCarrier) Set(key, value string) {
	for i, h := range c.Record.Headers {
		if h.Key == key {
			c.Record.Headers[i].Value = []byte(value)
			return
		}
	}
	c.Record.Headers = append(c.Record.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c.Record.Headers))
	for _, h := range c.Record.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// InjectKafka writes the trace context of ctx (traceparent/tracestate,
// baggage) into r's headers using the global propagator.
func InjectKafka(ctx context.Context, r *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, KafkaHeaderCarrier{Record: r})
}

// ExtractKafka returns ctx enriched with the remote trace context found in
// r's headers.
func ExtractKafka(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, KafkaHeaderCarrier{Record: r})
}
