package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func decode(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("not json: %s", b)
	}
	return m
}

func TestNewAddsServiceAndTrace(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "tariff", "info")

	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled,
	}))
	log.InfoContext(ctx, "hello", "k", 1)

	m := decode(t, buf.Bytes())
	if m["service"] != "tariff" || m["msg"] != "hello" || m["trace_id"] != tid.String() || m["span_id"] != sid.String() {
		t.Fatalf("unexpected record: %v", m)
	}
}

func TestNoTraceWithoutSpan(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, "svc", "info").Info("x")
	m := decode(t, buf.Bytes())
	if _, ok := m["trace_id"]; ok {
		t.Fatalf("unexpected trace_id: %v", m)
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "svc", "warn")
	log.Info("dropped")
	if buf.Len() != 0 {
		t.Fatalf("info should be filtered: %s", buf.String())
	}
	log.Warn("kept")
	if buf.Len() == 0 {
		t.Fatal("warn should be logged")
	}
}

func TestParseLevel(t *testing.T) {
	for in, want := range map[string]slog.Level{"debug": slog.LevelDebug, "INFO": slog.LevelInfo, "warn": slog.LevelWarn, "error": slog.LevelError} {
		got, err := ParseLevel(in)
		if err != nil || got != want {
			t.Errorf("%s: got %v %v", in, got, err)
		}
	}
	if _, err := ParseLevel("loud"); err == nil {
		t.Error("expected error")
	}
}
