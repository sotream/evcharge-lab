// Package api serves the tariff HTTP API (ADR 0011): POST /v1/quote and
// GET /v1/tariffs/{id}, both behind x-internal-token (ADR 0010).
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
	"github.com/sotream/evcharge-lab/packages/platform-go/httpx"
	"github.com/sotream/evcharge-lab/services/tariff/internal/pricing"
	"github.com/sotream/evcharge-lab/services/tariff/internal/store"
)

// Getter returns a tariff by id (the store cache).
type Getter interface {
	Get(ctx context.Context, id string) (*pricing.Tariff, error)
}

// API holds the handler dependencies.
type API struct {
	Tariffs Getter
	Logger  *slog.Logger
	quotes  *prometheus.CounterVec
	calc    prometheus.Histogram
}

// New builds the API and registers its metrics on reg (may be nil).
func New(tariffs Getter, logger *slog.Logger, reg prometheus.Registerer) *API {
	a := &API{
		Tariffs: tariffs, Logger: logger,
		quotes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tariff_quotes_total",
			Help: "Quotes by tariff id (\"other\" before the id is known to exist) and result.",
		}, []string{"tariff_id", "result"}),
		calc: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tariff_quote_calculation_seconds",
			Help:    "CPU time of one price calculation after the tariff is loaded.",
			Buckets: []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3},
		}),
	}
	if reg != nil {
		reg.MustRegister(a.quotes, a.calc)
	}
	return a
}

// Routes returns the mux with the internal-token middleware on every route.
func (a *API) Routes(internalToken string) *http.ServeMux {
	auth := httpx.RequireInternalToken(internalToken)
	mux := http.NewServeMux()
	mux.Handle("POST /v1/quote", auth(http.HandlerFunc(a.quote)))
	mux.Handle("GET /v1/tariffs/{id}", auth(http.HandlerFunc(a.getTariff)))
	return mux
}

func (a *API) quote(w http.ResponseWriter, r *http.Request) {
	var req contracts.QuoteReq
	if err := httpx.DecodeJSON(w, r, &req, 16<<10); err != nil {
		a.quotes.WithLabelValues("other", "invalid").Inc()
		httpx.WriteError(w, err)
		return
	}
	req.TariffID = strings.TrimSpace(req.TariffID)
	if req.TariffID == "" {
		a.quotes.WithLabelValues("other", "invalid").Inc()
		httpx.Error(w, http.StatusBadRequest, "validation", "tariffId is required")
		return
	}
	t, err := a.Tariffs.Get(r.Context(), req.TariffID)
	if err != nil {
		a.writeLoadError(w, r, req.TariffID, err)
		return
	}
	v, err := t.At(req.StartedAt)
	if err != nil {
		a.quotes.WithLabelValues(req.TariffID, "no_version").Inc()
		httpx.Error(w, http.StatusUnprocessableEntity, "no_tariff_version", err.Error())
		return
	}
	start := time.Now()
	res, err := pricing.Quote(v, pricing.Request{StartedAt: req.StartedAt, StoppedAt: req.StoppedAt, EnergyWh: req.EnergyWh})
	a.calc.Observe(time.Since(start).Seconds())
	if err != nil {
		if errors.Is(err, pricing.ErrInvalidRequest) {
			a.quotes.WithLabelValues(req.TariffID, "invalid").Inc()
			httpx.Error(w, http.StatusBadRequest, "validation", err.Error())
			return
		}
		a.quotes.WithLabelValues(req.TariffID, "error").Inc()
		a.Logger.ErrorContext(r.Context(), "quote failed", "tariff_id", req.TariffID, "err", err)
		httpx.Error(w, http.StatusInternalServerError, "internal", "")
		return
	}
	a.quotes.WithLabelValues(req.TariffID, "ok").Inc()
	httpx.WriteJSON(w, http.StatusOK, res)
}

func (a *API) writeLoadError(w http.ResponseWriter, r *http.Request, id string, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		a.quotes.WithLabelValues("other", "not_found").Inc()
		httpx.Error(w, http.StatusNotFound, "tariff_not_found", "unknown tariff "+id)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		a.quotes.WithLabelValues("other", "error").Inc()
		httpx.Error(w, http.StatusServiceUnavailable, "unavailable", "tariff store timeout")
	default:
		a.quotes.WithLabelValues("other", "error").Inc()
		a.Logger.ErrorContext(r.Context(), "tariff load failed", "tariff_id", id, "err", err)
		// Invalid stored tariffs are an operator problem; both map to 503 so
		// billing retries (ADR 0007) instead of failing the saga.
		httpx.Error(w, http.StatusServiceUnavailable, "unavailable", "tariff store unavailable")
	}
}

// WindowDTO is a TOU window in API form.
type WindowDTO struct {
	Days             []string `json:"days,omitempty"`
	From             string   `json:"from"`
	To               string   `json:"to"`
	PriceMinorPerKWh int64    `json:"priceMinorPerKwh"`
	Label            string   `json:"label,omitempty"`
}

// VersionDTO is one tariff version in API form.
type VersionDTO struct {
	Version                int         `json:"version"`
	ValidFrom              time.Time   `json:"validFrom"`
	Currency               string      `json:"currency"`
	Timezone               string      `json:"timezone"`
	SessionFeeMinor        int64       `json:"sessionFeeMinor"`
	EnergyPriceMinorPerKWh int64       `json:"energyPriceMinorPerKwh"`
	TouWindows             []WindowDTO `json:"touWindows"`
	TimePriceMinorPerHour  int64       `json:"timePriceMinorPerHour"`
	IdleFeeMinorPerMin     int64       `json:"idleFeeMinorPerMin"`
	IdleGraceMin           int64       `json:"idleGraceMin"`
	MinBillableWh          int64       `json:"minBillableWh"`
}

// TariffDTO is the GET /v1/tariffs/{id} body. Current is the version in
// force now (null if none yet).
type TariffDTO struct {
	TariffID string       `json:"tariffId"`
	Current  *int         `json:"currentVersion"`
	Versions []VersionDTO `json:"versions"`
}

func toDTO(t *pricing.Tariff, now time.Time) TariffDTO {
	out := TariffDTO{TariffID: t.ID, Versions: make([]VersionDTO, 0, len(t.Versions))}
	if v, err := t.At(now); err == nil {
		n := v.Version
		out.Current = &n
	}
	for _, v := range t.Versions {
		d := VersionDTO{Version: v.Version, ValidFrom: v.ValidFrom.UTC(), Currency: v.Currency, Timezone: v.Timezone,
			SessionFeeMinor: v.SessionFeeMinor, EnergyPriceMinorPerKWh: v.EnergyPriceMinorPerKWh,
			TouWindows: make([]WindowDTO, 0, len(v.Windows)), TimePriceMinorPerHour: v.TimePriceMinorPerHour,
			IdleFeeMinorPerMin: v.IdleFeeMinorPerMin, IdleGraceMin: v.IdleGraceMin, MinBillableWh: v.MinBillableWh}
		for _, w := range v.Windows {
			d.TouWindows = append(d.TouWindows, WindowDTO{Days: w.Days, From: w.From, To: w.To, PriceMinorPerKWh: w.PriceMinorPerKWh, Label: w.Label})
		}
		out.Versions = append(out.Versions, d)
	}
	return out
}

func (a *API) getTariff(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, err := a.Tariffs.Get(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			httpx.Error(w, http.StatusNotFound, "tariff_not_found", "unknown tariff "+id)
		default:
			a.Logger.ErrorContext(r.Context(), "tariff load failed", "tariff_id", id, "err", err)
			httpx.Error(w, http.StatusServiceUnavailable, "unavailable", "tariff store unavailable")
		}
		return
	}
	httpx.WriteJSON(w, http.StatusOK, toDTO(t, time.Now()))
}
