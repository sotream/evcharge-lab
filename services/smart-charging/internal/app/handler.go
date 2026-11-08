package app

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
	"github.com/sotream/evcharge-lab/packages/platform-go/httpx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/sotream/evcharge-lab/services/smart-charging/internal/alloc"
)

// MaxBodyBytes bounds POST /v1/allocate. 1,000 sessions (alloc.MaxSessions) of
// ~300 B each fit; a real site (≤ 80 connectors) needs ~25 KiB.
const MaxBodyBytes = 512 << 10

// maxDetailProblems caps how many validation problems one 400 lists.
const maxDetailProblems = 10

// Allocator is alloc.Allocate; tests swap it to exercise the 500 path.
type Allocator func(req *contracts.AllocateReq, now time.Time) (alloc.Result, error)

// Metrics are the ADR 0006 allocator metrics.
type Metrics struct {
	allocations         *prometheus.CounterVec
	duration            prometheus.Histogram
	sessions            prometheus.Histogram
	suspended           prometheus.Counter
	invariantViolations prometheus.Counter
}

// NewMetrics registers the allocator metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		allocations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "allocations_total",
			Help: "POST /v1/allocate requests by outcome (ok, invalid, infeasible, invariant_violation, error).",
		}, []string{"outcome"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "allocation_duration_seconds",
			Help:    "Time to compute one site allocation (validation + algorithm + invariant check).",
			Buckets: []float64{0.000_005, 0.00001, 0.000_025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.02},
		}),
		sessions: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "allocation_sessions",
			Help:    "Active sessions per allocation request.",
			Buckets: []float64{0, 1, 2, 5, 10, 20, 40, 60, 80, 160, 500, 1000},
		}),
		suspended: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "allocation_suspended_sessions_total",
			Help: "Sessions answered with suspended=true (limitW = startW) because the budget could not admit them.",
		}),
		invariantViolations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "invariant_violations_total",
			Help: "Results rejected because Σ limitW + reserveW > siteCapW (or a bound broke); answered with 500. Must stay 0.",
		}),
	}
	for _, o := range []string{"ok", "invalid", "infeasible", "invariant_violation", "error"} {
		m.allocations.WithLabelValues(o)
	}
	reg.MustRegister(m.allocations, m.duration, m.sessions, m.suspended, m.invariantViolations)
	return m
}

// AllocateHandler serves POST /v1/allocate (mount it behind
// httpx.RequireInternalToken).
type AllocateHandler struct {
	Log      *slog.Logger
	Metrics  *Metrics
	Allocate Allocator
	Now      func() time.Time
}

func (h *AllocateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := trace.SpanFromContext(ctx)

	var wire allocateReqWire
	if err := httpx.DecodeJSON(w, r, &wire, MaxBodyBytes); err != nil {
		h.Metrics.allocations.WithLabelValues("invalid").Inc()
		httpx.WriteError(w, err)
		return
	}
	start := time.Now()
	req, problems := wire.toContract()
	if len(problems) == 0 {
		problems = alloc.Validate(&req)
	}
	if len(problems) > 0 {
		h.Metrics.allocations.WithLabelValues("invalid").Inc()
		httpx.Error(w, http.StatusBadRequest, "validation_failed", joinProblems(problems))
		return
	}
	now := h.Now()
	if req.ComputedAt != nil {
		now = *req.ComputedAt
	}
	span.SetAttributes(attribute.String("site.id", req.SiteID), attribute.Int("allocation.sessions", len(req.Sessions)))

	res, err := h.Allocate(&req, now)
	h.Metrics.duration.Observe(time.Since(start).Seconds())
	h.Metrics.sessions.Observe(float64(len(req.Sessions)))
	switch {
	case errors.Is(err, alloc.ErrInfeasible):
		h.Metrics.allocations.WithLabelValues("infeasible").Inc()
		httpx.Error(w, http.StatusBadRequest, "infeasible_reserve", err.Error())
		return
	case errors.Is(err, alloc.ErrInvariant):
		h.Metrics.allocations.WithLabelValues("invariant_violation").Inc()
		h.Metrics.invariantViolations.Inc()
		span.SetStatus(codes.Error, "invariant violated")
		h.Log.ErrorContext(ctx, "allocation invariant violated; refusing to answer", "site_id", req.SiteID, "err", err)
		httpx.Error(w, http.StatusInternalServerError, "invariant_violation", "allocation would exceed the site cap")
		return
	case err != nil:
		h.Metrics.allocations.WithLabelValues("error").Inc()
		h.Log.ErrorContext(ctx, "allocation failed", "site_id", req.SiteID, "err", err)
		httpx.Error(w, http.StatusInternalServerError, "internal", "")
		return
	}
	h.Metrics.allocations.WithLabelValues("ok").Inc()
	if res.Suspended > 0 {
		h.Metrics.suspended.Add(float64(res.Suspended))
	}
	span.SetAttributes(
		attribute.Int("allocation.suspended", res.Suspended),
		attribute.Int64("allocation.total_w", res.Res.TotalAllocatedW),
		attribute.Int64("allocation.reserve_w", res.Res.ReserveW),
	)
	h.Log.DebugContext(ctx, "allocated", "site_id", req.SiteID, "sessions", len(req.Sessions),
		"suspended", res.Suspended, "total_w", res.Res.TotalAllocatedW, "reserve_w", res.Res.ReserveW, "cap_w", req.SiteCapW)
	httpx.WriteJSON(w, http.StatusOK, res.Res)
}

func joinProblems(p []string) string {
	if len(p) > maxDetailProblems {
		p = append(p[:maxDetailProblems:maxDetailProblems], "...")
	}
	return strings.Join(p, "; ")
}

// allocateReqWire mirrors contracts.AllocateReq with pointers for the fields
// the zod schema requires, so a missing field is a 400 instead of a silent 0.
type allocateReqWire struct {
	SiteID         *string                `json:"siteId"`
	SiteCapW       *int64                 `json:"siteCapW"`
	IdleConnectors *int64                 `json:"idleConnectors"`
	StartW         *int64                 `json:"startW"`
	ComputedAt     *time.Time             `json:"computedAt"`
	Sessions       *[]allocateSessionWire `json:"sessions"`
}

type allocateSessionWire struct {
	SessionID   *string    `json:"sessionId"`
	StationID   *string    `json:"stationId"`
	ConnectorID *int       `json:"connectorId"`
	MaxW        *int64     `json:"maxW"`
	MinW        *int64     `json:"minW"`
	MeasuredW   *int64     `json:"measuredW"`
	Priority    *int       `json:"priority"`
	StartedAt   *time.Time `json:"startedAt"`
}

func (w *allocateReqWire) toContract() (contracts.AllocateReq, []string) {
	var missing []string
	need := func(ok bool, path string) {
		if !ok {
			missing = append(missing, path+": is required")
		}
	}
	need(w.SiteID != nil, "siteId")
	need(w.SiteCapW != nil, "siteCapW")
	need(w.IdleConnectors != nil, "idleConnectors")
	need(w.StartW != nil, "startW")
	need(w.Sessions != nil, "sessions")
	if len(missing) > 0 {
		return contracts.AllocateReq{}, missing
	}
	if len(*w.Sessions) > alloc.MaxSessions {
		return contracts.AllocateReq{}, []string{"sessions: at most " + strconv.Itoa(alloc.MaxSessions) + " sessions"}
	}
	req := contracts.AllocateReq{
		SiteID: *w.SiteID, SiteCapW: *w.SiteCapW, IdleConnectors: *w.IdleConnectors, StartW: *w.StartW,
		ComputedAt: w.ComputedAt, Sessions: make([]contracts.AllocateSession, len(*w.Sessions)),
	}
	for i, s := range *w.Sessions {
		at := "sessions[" + strconv.Itoa(i) + "]."
		need(s.SessionID != nil, at+"sessionId")
		need(s.StationID != nil, at+"stationId")
		need(s.ConnectorID != nil, at+"connectorId")
		need(s.MaxW != nil, at+"maxW")
		need(s.MinW != nil, at+"minW")
		need(s.StartedAt != nil, at+"startedAt")
		if len(missing) > 0 {
			continue
		}
		cs := contracts.AllocateSession{
			SessionID: *s.SessionID, StationID: *s.StationID, ConnectorID: *s.ConnectorID,
			MaxW: *s.MaxW, MinW: *s.MinW, MeasuredW: s.MeasuredW, StartedAt: *s.StartedAt,
		}
		if s.Priority != nil {
			cs.Priority = *s.Priority
		}
		req.Sessions[i] = cs
	}
	return req, missing
}
