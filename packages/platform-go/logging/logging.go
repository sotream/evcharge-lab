// Package logging builds the service's log/slog logger: JSON to stdout, a
// constant `service` attribute, and `trace_id`/`span_id` taken from the
// OpenTelemetry span in the record's context (use the *Context logging
// methods, e.g. logger.InfoContext(ctx, ...)).
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// ParseLevel maps debug|info|warn|error (case-insensitive) to a slog.Level.
func ParseLevel(s string) (slog.Level, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(strings.ToUpper(strings.TrimSpace(s)))); err != nil {
		return slog.LevelInfo, fmt.Errorf("logging: unknown level %q", s)
	}
	return l, nil
}

// New returns a JSON logger writing to w at the given level, tagging every
// record with service=<service> and trace context. Unknown levels fall back to
// info (config.Base.Validate rejects them earlier).
func New(w io.Writer, service, level string) *slog.Logger {
	lvl, _ := ParseLevel(level)
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	return slog.New(NewTraceHandler(h)).With(slog.String("service", service))
}

// TraceHandler decorates another slog.Handler with trace_id and span_id when
// the record's context carries a valid span context.
type TraceHandler struct {
	inner slog.Handler
}

// NewTraceHandler wraps inner.
func NewTraceHandler(inner slog.Handler) *TraceHandler { return &TraceHandler{inner: inner} }

func (h *TraceHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r = r.Clone()
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *TraceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TraceHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *TraceHandler) WithGroup(name string) slog.Handler {
	return &TraceHandler{inner: h.inner.WithGroup(name)}
}
