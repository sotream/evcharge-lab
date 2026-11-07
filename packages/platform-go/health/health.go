// Package health serves liveness (/health/live) and readiness (/health/ready)
// probes. Readiness runs every registered check concurrently, each under its
// own timeout, and reports 503 when any check fails or the service is
// draining (shutdown in progress).
package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultCheckTimeout bounds a single readiness check.
const DefaultCheckTimeout = 2 * time.Second

// CheckFunc reports nil when the dependency is usable.
type CheckFunc func(ctx context.Context) error

type check struct {
	name    string
	fn      CheckFunc
	timeout time.Duration
}

// Checker holds readiness checks and the draining flag. It is safe for
// concurrent use.
type Checker struct {
	mu       sync.RWMutex
	checks   []check
	draining atomic.Bool
	timeout  time.Duration
}

// Option configures a Checker.
type Option func(*Checker)

// WithDefaultTimeout sets the timeout used by checks added without WithTimeout.
func WithDefaultTimeout(d time.Duration) Option { return func(c *Checker) { c.timeout = d } }

// New returns an empty Checker (ready until a check is added or draining).
func New(opts ...Option) *Checker {
	c := &Checker{timeout: DefaultCheckTimeout}
	for _, o := range opts {
		o(c)
	}
	return c
}

// CheckOption configures one readiness check.
type CheckOption func(*check)

// WithTimeout overrides the per-check timeout.
func WithTimeout(d time.Duration) CheckOption { return func(c *check) { c.timeout = d } }

// AddReadinessCheck registers a named check. Names should be unique.
func (c *Checker) AddReadinessCheck(name string, fn CheckFunc, opts ...CheckOption) {
	ch := check{name: name, fn: fn, timeout: c.timeout}
	for _, o := range opts {
		o(&ch)
	}
	c.mu.Lock()
	c.checks = append(c.checks, ch)
	c.mu.Unlock()
}

// SetDraining flips the draining flag; while set, readiness returns 503.
func (c *Checker) SetDraining(v bool) { c.draining.Store(v) }

// Draining reports the draining flag.
func (c *Checker) Draining() bool { return c.draining.Load() }

// Report is the JSON body of the readiness endpoint.
type Report struct {
	Status string            `json:"status"` // ok | fail | draining
	Checks map[string]string `json:"checks,omitempty"`
}

// Check runs all readiness checks and returns the aggregated report.
func (c *Checker) Check(ctx context.Context) (Report, bool) {
	c.mu.RLock()
	checks := append([]check(nil), c.checks...)
	c.mu.RUnlock()

	results := make([]string, len(checks))
	var wg sync.WaitGroup
	for i, ch := range checks {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, ch.timeout)
			defer cancel()
			results[i] = runCheck(cctx, ch.fn)
		})
	}
	wg.Wait()

	rep := Report{Status: "ok", Checks: make(map[string]string, len(checks))}
	ok := true
	for i, ch := range checks {
		rep.Checks[ch.name] = results[i]
		if results[i] != "ok" {
			ok = false
		}
	}
	if !ok {
		rep.Status = "fail"
	}
	if c.Draining() {
		rep.Status, ok = "draining", false
	}
	return rep, ok
}

// runCheck runs fn but returns as soon as ctx expires even if fn ignores ctx.
func runCheck(ctx context.Context, fn CheckFunc) string {
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- errPanic
			}
		}()
		done <- fn(ctx)
	}()
	select {
	case err := <-done:
		if err != nil {
			return err.Error()
		}
		return "ok"
	case <-ctx.Done():
		return "timeout"
	}
}

var errPanic = errors.New("check panicked")

// LiveHandler always answers 200 while the process can serve HTTP.
func (c *Checker) LiveHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, Report{Status: "ok"})
	})
}

// ReadyHandler answers 200 when all checks pass and the service is not
// draining, 503 otherwise.
func (c *Checker) ReadyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rep, ok := c.Check(r.Context())
		status := http.StatusOK
		if !ok {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, rep)
	})
}

// Register mounts GET /health/live and GET /health/ready on mux.
func (c *Checker) Register(mux *http.ServeMux) {
	mux.Handle("GET /health/live", c.LiveHandler())
	mux.Handle("GET /health/ready", c.ReadyHandler())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
