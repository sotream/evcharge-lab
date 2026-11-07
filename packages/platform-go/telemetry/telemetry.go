// Package telemetry configures OpenTelemetry tracing: an OTLP/HTTP exporter
// (no-op when no endpoint is configured), the W3C trace-context + baggage
// propagator, otelhttp wrappers for servers and clients, and helpers that
// carry `traceparent` in Kafka record headers.
package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Propagator is the process-wide propagator (W3C traceparent + baggage).
var Propagator propagation.TextMapPropagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{}, propagation.Baggage{},
)

// Setup installs the global tracer provider and propagator. With an empty
// endpoint the tracer provider is a no-op (incoming trace context is still
// propagated, so log correlation keeps working). endpoint is the
// OTEL_EXPORTER_OTLP_ENDPOINT base URL (e.g. http://jaeger:4318); /v1/traces
// is appended. The returned shutdown flushes and stops the exporter.
func Setup(ctx context.Context, serviceName, endpoint string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(Propagator)
	if endpoint == "" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}
	target, err := tracesURL(endpoint)
	if err != nil {
		return nil, err
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(target))
	if err != nil {
		return nil, fmt.Errorf("telemetry: otlp exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

func tracesURL(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("telemetry: invalid OTLP endpoint %q", endpoint)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/traces"
	return u.String(), nil
}

// Tracer returns a tracer from the global provider.
func Tracer(name string) trace.Tracer { return otel.Tracer(name) }

// HTTPHandler wraps h with otelhttp server instrumentation. Span names are
// refined to "METHOD /route" from http.Request.Pattern after routing.
func HTTPHandler(h http.Handler, operation string) http.Handler {
	return otelhttp.NewHandler(h, operation)
}

// HTTPTransport wraps rt (nil means http.DefaultTransport) with otelhttp
// client instrumentation, which injects traceparent into outbound requests.
func HTTPTransport(rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		rt = http.DefaultTransport
	}
	return otelhttp.NewTransport(rt)
}
