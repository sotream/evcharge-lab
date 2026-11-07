package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestSetupNoopWithoutEndpoint(t *testing.T) {
	shutdown, err := Setup(context.Background(), "svc", "")
	if err != nil {
		t.Fatal(err)
	}
	_, span := Tracer("t").Start(context.Background(), "x")
	if span.IsRecording() {
		t.Fatal("no-op provider should not record")
	}
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSetupExportsOverOTLPHTTP(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/traces" && r.Method == http.MethodPost {
			hits.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	shutdown, err := Setup(context.Background(), "svc", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, span := Tracer("t").Start(context.Background(), "x")
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if hits.Load() == 0 {
		t.Fatal("expected an OTLP export request")
	}
}

func TestSetupRejectsBadEndpoint(t *testing.T) {
	if _, err := Setup(context.Background(), "svc", "jaeger:4318"); err == nil {
		t.Fatal("expected error")
	}
}

func TestTracesURL(t *testing.T) {
	for in, want := range map[string]string{
		"http://jaeger:4318":         "http://jaeger:4318/v1/traces",
		"https://otel.example/base/": "https://otel.example/base/v1/traces",
	} {
		got, err := tracesURL(in)
		if err != nil || got != want {
			t.Errorf("%s: got %q %v", in, got, err)
		}
	}
}

func installRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(Propagator)
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return rec
}

func TestHTTPPropagation(t *testing.T) {
	installRecorder(t)
	var serverTrace trace.TraceID
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/things/{id}", func(_ http.ResponseWriter, r *http.Request) {
		serverTrace = trace.SpanContextFromContext(r.Context()).TraceID()
	})
	srv := httptest.NewServer(HTTPHandler(mux, "http.server"))
	defer srv.Close()

	ctx, span := Tracer("t").Start(context.Background(), "client-op")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/things/1", nil)
	resp, err := (&http.Client{Transport: HTTPTransport(nil)}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	span.End()

	if serverTrace != span.SpanContext().TraceID() {
		t.Fatalf("trace not propagated: server %s client %s", serverTrace, span.SpanContext().TraceID())
	}
}

func TestKafkaInjectExtract(t *testing.T) {
	installRecorder(t)
	ctx, span := Tracer("t").Start(context.Background(), "produce")
	defer span.End()

	r := &kgo.Record{Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte("stale")}, {Key: "x", Value: []byte("y")}}}
	InjectKafka(ctx, r)

	n := 0
	for _, h := range r.Headers {
		if h.Key == "traceparent" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("traceparent should be set exactly once, headers=%v", r.Headers)
	}
	got := trace.SpanContextFromContext(ExtractKafka(context.Background(), r))
	if !got.IsRemote() || got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("extract mismatch: %v vs %v", got, span.SpanContext())
	}
	if (KafkaHeaderCarrier{Record: r}).Get("x") != "y" {
		t.Fatal("other headers must be preserved")
	}
}
