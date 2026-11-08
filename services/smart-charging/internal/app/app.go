// Package app wires the smart-charging service: configuration, the
// POST /v1/allocate handler behind the internal token (ADR 0010), metrics,
// tracing and the platform server lifecycle.
package app

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/sotream/evcharge-lab/packages/platform-go/config"
	"github.com/sotream/evcharge-lab/packages/platform-go/health"
	"github.com/sotream/evcharge-lab/packages/platform-go/httpx"
	"github.com/sotream/evcharge-lab/packages/platform-go/metrics"
	"github.com/sotream/evcharge-lab/packages/platform-go/server"
	"github.com/sotream/evcharge-lab/packages/platform-go/telemetry"

	"github.com/sotream/evcharge-lab/services/smart-charging/internal/alloc"
)

// Config is the service configuration. The allocator is stateless, so the
// shared base (SERVICE_NAME, LOG_LEVEL, HTTP_PORT, INTERNAL_TOKEN,
// OTEL_EXPORTER_OTLP_ENDPOINT, SHUTDOWN_GRACE) is all it needs.
type Config struct {
	config.Base
}

// Validate checks every field (fail fast at startup).
func (c Config) Validate() error {
	var p config.Problems
	p.Merge(c.Base.Validate())
	return p.Err()
}

// Routes returns the application mux (without /health and /metrics, which
// server.Run adds).
func Routes(cfg Config, log *slog.Logger, m *Metrics) http.Handler {
	mux := http.NewServeMux()
	h := &AllocateHandler{Log: log, Metrics: m, Allocate: alloc.Allocate, Now: time.Now}
	mux.Handle("POST /v1/allocate", httpx.RequireInternalToken(cfg.InternalToken)(h))
	return mux
}

// Run serves until ctx is cancelled or SIGTERM/SIGINT, then shuts down
// gracefully. Extra server options (listener, drain delay) are for tests.
func Run(ctx context.Context, cfg Config, log *slog.Logger, opts ...server.Option) error {
	shutdownTracing, err := telemetry.Setup(ctx, cfg.ServiceName, cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	reg := metrics.New()
	m := NewMetrics(reg)
	// No dependencies: ready as soon as the listener is up, 503 while draining.
	hc := health.New()

	all := append([]server.Option{
		server.WithLogger(log), server.WithHealth(hc), server.WithMetrics(reg),
		server.WithCloser("tracer", shutdownTracing),
	}, opts...)
	return server.Run(ctx, cfg.Base, Routes(cfg, log, m), all...)
}
