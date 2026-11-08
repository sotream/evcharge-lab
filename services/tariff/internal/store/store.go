// Package store loads versioned tariffs from Postgres and caches them
// in-process (ADR 0011: 60 s).
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/sotream/evcharge-lab/services/tariff/internal/pricing"
)

// ErrNotFound means the tariff id does not exist.
var ErrNotFound = errors.New("tariff not found")

// Loader loads one tariff with all its versions.
type Loader func(ctx context.Context, id string) (*pricing.Tariff, error)

// PG reads tariffs from the tariff database.
type PG struct {
	Pool *pgxpool.Pool
	// Timeout bounds one load (default 2 s).
	Timeout time.Duration
}

const loadSQL = `
SELECT version, valid_from, currency, timezone, session_fee_minor, energy_price_minor_per_kwh,
       tou_windows, time_price_minor_per_hour, idle_fee_minor_per_min, idle_grace_min, min_billable_wh
FROM tariff_versions WHERE tariff_id = $1 ORDER BY valid_from`

// Load implements Loader.
func (s PG) Load(ctx context.Context, id string) (*pricing.Tariff, error) {
	ctx, span := otel.Tracer("tariff/store").Start(ctx, "tariff.load")
	defer span.End()
	span.SetAttributes(attribute.String("tariff.id", id))
	to := s.Timeout
	if to <= 0 {
		to = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	rows, err := s.Pool.Query(ctx, loadSQL, id)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("load tariff %s: %w", id, err)
	}
	defer rows.Close()
	var versions []*pricing.Version
	for rows.Next() {
		v := &pricing.Version{TariffID: id}
		var windows []byte
		var grace int32
		if err := rows.Scan(&v.Version, &v.ValidFrom, &v.Currency, &v.Timezone, &v.SessionFeeMinor,
			&v.EnergyPriceMinorPerKWh, &windows, &v.TimePriceMinorPerHour, &v.IdleFeeMinorPerMin,
			&grace, &v.MinBillableWh); err != nil {
			return nil, fmt.Errorf("scan tariff %s: %w", id, err)
		}
		v.IdleGraceMin = int64(grace)
		if err := json.Unmarshal(windows, &v.Windows); err != nil {
			return nil, fmt.Errorf("%w %s v%d: tou_windows: %w", pricing.ErrInvalidTariff, id, v.Version, err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("load tariff %s: %w", id, err)
	}
	if len(versions) == 0 {
		return nil, ErrNotFound
	}
	return pricing.NewTariff(id, versions)
}

// CacheConfig configures Cache. Zero values pick the defaults noted.
type CacheConfig struct {
	TTL         time.Duration // fresh entries (default 60 s, ADR 0011)
	NegativeTTL time.Duration // remembered ErrNotFound (default 10 s)
	// StaleFor serves an expired entry while reloads fail (default 10 min):
	// versions are immutable, so a stale entry only misses versions added
	// since, and quotes keep working through a database outage.
	StaleFor   time.Duration
	Registerer prometheus.Registerer
	Logger     *slog.Logger
	Now        func() time.Time
}

type entry struct {
	tariff  *pricing.Tariff
	err     error         // ErrNotFound only
	loaded  time.Time     // freshness clock (reset to "expired" after a failed reload)
	fetched time.Time     // when tariff was actually read from the database
	pending chan struct{} // non-nil while a load is in flight
}

// retryAfterFailure spaces reload attempts while serving stale entries.
const retryAfterFailure = time.Second

// Cache is a TTL cache in front of a Loader with in-flight deduplication.
type Cache struct {
	cfg    CacheConfig
	load   Loader
	mu     sync.Mutex
	m      map[string]*entry
	events *prometheus.CounterVec
}

// NewCache wraps load.
func NewCache(load Loader, cfg CacheConfig) *Cache {
	if cfg.TTL <= 0 {
		cfg.TTL = 60 * time.Second
	}
	if cfg.NegativeTTL <= 0 {
		cfg.NegativeTTL = 10 * time.Second
	}
	if cfg.StaleFor <= 0 {
		cfg.StaleFor = 10 * time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	c := &Cache{cfg: cfg, load: load, m: map[string]*entry{},
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tariff_cache_events_total",
			Help: "Tariff cache lookups by result (hit, miss, stale, negative, error).",
		}, []string{"result"})}
	if cfg.Registerer != nil {
		cfg.Registerer.MustRegister(c.events)
	}
	return c
}

// Warm reports whether the cache can still serve at least one tariff without the database: an entry
// read from the database within TTL + StaleFor, the window Get serves while reloads fail.
func (c *Cache) Warm() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.cfg.Now()
	for _, e := range c.m {
		if e.tariff != nil && now.Sub(e.fetched) < c.cfg.TTL+c.cfg.StaleFor {
			return true
		}
	}
	return false
}

// Get returns the tariff id, loading it when absent or expired.
func (c *Cache) Get(ctx context.Context, id string) (*pricing.Tariff, error) {
	for {
		c.mu.Lock()
		e := c.m[id]
		now := c.cfg.Now()
		if e != nil && e.pending == nil {
			age := now.Sub(e.loaded)
			switch {
			case e.tariff != nil && age < c.cfg.TTL:
				c.mu.Unlock()
				c.events.WithLabelValues("hit").Inc()
				return e.tariff, nil
			case e.err != nil && age < c.cfg.NegativeTTL:
				c.mu.Unlock()
				c.events.WithLabelValues("negative").Inc()
				return nil, e.err
			}
		}
		if e != nil && e.pending != nil {
			ch := e.pending
			c.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		// Become the loader for id.
		var prev *entry
		if e != nil {
			prev = &entry{tariff: e.tariff, err: e.err, loaded: e.loaded, fetched: e.fetched}
		}
		ch := make(chan struct{})
		// Keep the previous value visible to Warm while the reload is in flight.
		pe := &entry{pending: ch}
		if prev != nil {
			pe.tariff, pe.fetched = prev.tariff, prev.fetched
		}
		c.m[id] = pe
		c.mu.Unlock()

		t, err := c.load(context.WithoutCancel(ctx), id)

		c.mu.Lock()
		now = c.cfg.Now()
		switch {
		case err == nil:
			c.m[id] = &entry{tariff: t, loaded: now, fetched: now}
			c.events.WithLabelValues("miss").Inc()
		case errors.Is(err, ErrNotFound):
			c.m[id] = &entry{err: ErrNotFound, loaded: now, fetched: now}
			c.events.WithLabelValues("miss").Inc()
		case prev != nil && prev.tariff != nil && now.Sub(prev.fetched) < c.cfg.TTL+c.cfg.StaleFor:
			c.cfg.Logger.WarnContext(ctx, "tariff reload failed, serving stale", "tariff_id", id, "err", err)
			// Serve stale for retryAfterFailure, then the next request retries the load.
			prev.loaded = now.Add(-c.cfg.TTL + retryAfterFailure)
			c.m[id] = prev
			t, err = prev.tariff, nil
			c.events.WithLabelValues("stale").Inc()
		default:
			delete(c.m, id)
			c.events.WithLabelValues("error").Inc()
		}
		close(ch)
		c.mu.Unlock()
		return t, err
	}
}
