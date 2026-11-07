// Package server runs a service's HTTP server and owns its lifecycle:
// health and metrics endpoints, tracing/metrics/recovery middleware, sane
// timeouts, SIGTERM/SIGINT handling, draining, graceful shutdown and ordered
// release of resources.
//
// Shutdown sequence (on signal, ctx cancellation or a failing runner):
//  1. readiness flips to 503 (draining) so load balancers stop routing;
//  2. wait DrainDelay;
//  3. http.Server.Shutdown bounded by the remaining SHUTDOWN_GRACE budget;
//  4. cancel runners (e.g. Kafka consumers) and wait for them to return;
//  5. run closers in reverse registration order (register tracer provider
//     first, then pools, then consumers: they close consumers → pools → tracer).
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sotream/evcharge-lab/packages/platform-go/config"
	"github.com/sotream/evcharge-lab/packages/platform-go/health"
	"github.com/sotream/evcharge-lab/packages/platform-go/httpx"
	"github.com/sotream/evcharge-lab/packages/platform-go/metrics"
	"github.com/sotream/evcharge-lab/packages/platform-go/telemetry"
)

// Default HTTP server timeouts.
const (
	DefaultReadHeaderTimeout = 5 * time.Second
	DefaultReadTimeout       = 15 * time.Second
	DefaultWriteTimeout      = 30 * time.Second
	DefaultIdleTimeout       = 120 * time.Second
	DefaultDrainDelay        = 3 * time.Second
	minCloserBudget          = 2 * time.Second
)

type named struct {
	name string
	fn   func(context.Context) error
}

type options struct {
	logger     *slog.Logger
	health     *health.Checker
	metrics    *metrics.Registry
	listener   net.Listener
	drainDelay time.Duration
	signals    []os.Signal
	closers    []named
	runners    []named
	configure  []func(*http.Server)
}

// Option configures Run.
type Option func(*options)

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

// WithHealth uses c for /health/* (default: a fresh checker). Register
// readiness checks on it before or after calling Run.
func WithHealth(c *health.Checker) Option { return func(o *options) { o.health = c } }

// WithMetrics uses r for /metrics and request metrics (default: metrics.New()).
func WithMetrics(r *metrics.Registry) Option { return func(o *options) { o.metrics = r } }

// WithListener serves on l instead of listening on cfg.HTTPAddr() (tests).
func WithListener(l net.Listener) Option { return func(o *options) { o.listener = l } }

// WithDrainDelay sets the pause between flipping readiness and stopping the
// listener (default 3s, capped at a third of SHUTDOWN_GRACE).
func WithDrainDelay(d time.Duration) Option { return func(o *options) { o.drainDelay = d } }

// WithSignals overrides the shutdown signals (default SIGTERM, SIGINT).
func WithSignals(sig ...os.Signal) Option { return func(o *options) { o.signals = sig } }

// WithCloser registers a resource release function. Closers run after the
// HTTP server and runners stopped, in reverse registration order.
func WithCloser(name string, fn func(context.Context) error) Option {
	return func(o *options) { o.closers = append(o.closers, named{name, fn}) }
}

// WithRunner starts fn in a goroutine once the listener is up. Its context is
// cancelled during shutdown (after HTTP drained); fn should then return nil.
// A runner returning early (error or not) triggers shutdown and Run reports
// the error.
func WithRunner(name string, fn func(context.Context) error) Option {
	return func(o *options) { o.runners = append(o.runners, named{name, fn}) }
}

// WithHTTPServer lets the caller tweak the *http.Server (timeouts, TLS, ...).
func WithHTTPServer(fn func(*http.Server)) Option {
	return func(o *options) { o.configure = append(o.configure, fn) }
}

// Handler builds the root handler Run serves: /health/live, /health/ready and
// /metrics uninstrumented; everything else goes through tracing → metrics →
// panic recovery → app.
func Handler(app http.Handler, hc *health.Checker, reg *metrics.Registry, logger *slog.Logger) http.Handler {
	root := http.NewServeMux()
	hc.Register(root)
	root.Handle("GET /metrics", reg.Handler())
	root.Handle("/", telemetry.HTTPHandler(reg.Middleware(recoverer(logger, app)), "http.server"))
	return root
}

// Run serves handler until ctx is cancelled or a shutdown signal arrives,
// then performs the graceful shutdown sequence. It returns nil after a clean
// shutdown, or the first serve/runner error joined with closer errors.
func Run(ctx context.Context, cfg config.Base, handler http.Handler, opts ...Option) error {
	o := options{drainDelay: DefaultDrainDelay, signals: []os.Signal{syscall.SIGTERM, syscall.SIGINT}}
	for _, opt := range opts {
		opt(&o)
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	if o.health == nil {
		o.health = health.New()
	}
	if o.metrics == nil {
		o.metrics = metrics.New()
	}
	grace := cfg.ShutdownGrace
	if grace <= 0 {
		grace = 15 * time.Second
	}
	o.drainDelay = min(o.drainDelay, grace/3)

	srv := &http.Server{
		Handler:           Handler(handler, o.health, o.metrics, o.logger),
		ReadHeaderTimeout: DefaultReadHeaderTimeout,
		ReadTimeout:       DefaultReadTimeout,
		WriteTimeout:      DefaultWriteTimeout,
		IdleTimeout:       DefaultIdleTimeout,
		ErrorLog:          slog.NewLogLogger(o.logger.Handler(), slog.LevelWarn),
	}
	for _, fn := range o.configure {
		fn(srv)
	}

	ln := o.listener
	if ln == nil {
		var err error
		ln, err = (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTPAddr())
		if err != nil {
			return errors.Join(fmt.Errorf("server: listen %s: %w", cfg.HTTPAddr(), err), closeAll(o, grace))
		}
	}

	sigCtx, stopSignals := signal.NotifyContext(ctx, o.signals...)
	defer stopSignals()

	fatal := make(chan error, 1+len(o.runners))
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal <- fmt.Errorf("server: serve: %w", err)
		}
	}()
	o.logger.InfoContext(ctx, "http server listening", "addr", ln.Addr().String())

	runCtx, cancelRunners := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRunners()
	var runners sync.WaitGroup
	for _, r := range o.runners {
		runners.Go(func() {
			err := r.fn(runCtx)
			if runCtx.Err() != nil {
				if err != nil && !errors.Is(err, context.Canceled) {
					o.logger.Error("runner failed during shutdown", "runner", r.name, "err", err)
				}
				return
			}
			if err == nil {
				err = errors.New("returned unexpectedly")
			}
			fatal <- fmt.Errorf("server: runner %s: %w", r.name, err)
		})
	}

	var runErr error
	select {
	case <-sigCtx.Done():
		o.logger.Info("shutdown requested", "cause", context.Cause(sigCtx))
	case runErr = <-fatal:
		o.logger.Error("shutting down after failure", "err", runErr)
	}

	start := time.Now()
	deadline := start.Add(grace)
	o.health.SetDraining(true)
	if o.drainDelay > 0 {
		time.Sleep(o.drainDelay)
	}

	shutdownCtx, cancel := context.WithDeadline(context.Background(), deadline)
	if err := srv.Shutdown(shutdownCtx); err != nil {
		o.logger.Warn("http shutdown incomplete, closing", "err", err)
		_ = srv.Close()
	}
	cancel()

	cancelRunners()
	done := make(chan struct{})
	go func() { runners.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Until(deadline)):
		o.logger.Warn("runners did not stop before the grace deadline")
	}

	closeErr := closeAll(o, max(time.Until(deadline), minCloserBudget))
	o.logger.Info("shutdown complete", "took", time.Since(start).String())
	return errors.Join(runErr, closeErr)
}

func closeAll(o options, budget time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	var errs []error
	for i := len(o.closers) - 1; i >= 0; i-- {
		c := o.closers[i]
		if err := c.fn(ctx); err != nil {
			o.logger.Error("closer failed", "closer", c.name, "err", err)
			errs = append(errs, fmt.Errorf("server: close %s: %w", c.name, err))
		}
	}
	return errors.Join(errs...)
}

func recoverer(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity, as net/http does
					panic(v)
				}
				logger.ErrorContext(r.Context(), "panic in handler", "panic", fmt.Sprint(v), "method", r.Method, "path", r.URL.Path)
				httpx.Error(w, http.StatusInternalServerError, "internal", "")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
