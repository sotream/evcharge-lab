// Command tariff serves deterministic session pricing (ADR 0011).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/sotream/evcharge-lab/packages/platform-go/config"
	"github.com/sotream/evcharge-lab/packages/platform-go/health"
	"github.com/sotream/evcharge-lab/packages/platform-go/logging"
	"github.com/sotream/evcharge-lab/packages/platform-go/metrics"
	"github.com/sotream/evcharge-lab/packages/platform-go/pg"
	"github.com/sotream/evcharge-lab/packages/platform-go/server"
	"github.com/sotream/evcharge-lab/packages/platform-go/telemetry"
	"github.com/sotream/evcharge-lab/services/tariff/internal/api"
	"github.com/sotream/evcharge-lab/services/tariff/internal/store"
	"github.com/sotream/evcharge-lab/services/tariff/migrations"
)

// Config is the tariff service environment.
type Config struct {
	config.Base
	DatabaseURL    string        `env:"DATABASE_URL"`
	DBMaxConns     int32         `env:"DB_MAX_CONNS"     envDefault:"8"`
	CacheTTL       time.Duration `env:"TARIFF_CACHE_TTL" envDefault:"60s"`
	DBLoadTimeout  time.Duration `env:"DB_LOAD_TIMEOUT"  envDefault:"2s"`
	ReadinessCheck time.Duration `env:"READINESS_TIMEOUT" envDefault:"2s"`
}

// Validate implements config.Validator.
func (c Config) Validate() error {
	var p config.Problems
	p.Merge(c.Base.Validate())
	p.Require("DATABASE_URL", c.DatabaseURL)
	if c.DBMaxConns < 1 || c.DBMaxConns > 100 {
		p.Addf("DB_MAX_CONNS must be in 1..100, got %d", c.DBMaxConns)
	}
	if c.CacheTTL <= 0 || c.CacheTTL > time.Hour {
		p.Addf("TARIFF_CACHE_TTL must be in (0, 1h], got %s", c.CacheTTL)
	}
	if c.DBLoadTimeout <= 0 {
		p.Addf("DB_LOAD_TIMEOUT must be positive, got %s", c.DBLoadTimeout)
	}
	if c.ReadinessCheck <= 0 {
		p.Addf("READINESS_TIMEOUT must be positive, got %s", c.ReadinessCheck)
	}
	return p.Err()
}

func main() {
	var cfg Config
	if err := config.Load(&cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := logging.New(os.Stdout, cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(log)
	if err := run(context.Background(), cfg, log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

// cacheAwareReadiness keeps the instance ready while the tariff cache can still serve quotes, even
// when Postgres is down: dropping every instance during a database outage would turn a degraded
// service into an unavailable one. A cold cache with Postgres down is not ready.
func cacheAwareReadiness(pgCheck func(context.Context) error, warm func() bool) func(context.Context) error {
	return func(ctx context.Context) error {
		err := pgCheck(ctx)
		if err != nil && warm() {
			return nil
		}
		return err
	}
}

func run(ctx context.Context, cfg Config, log *slog.Logger) error {
	shutdownTracing, err := telemetry.Setup(ctx, cfg.ServiceName, cfg.OTLPEndpoint)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	pool, err := pg.Connect(ctx, pg.Config{URL: cfg.DatabaseURL, ApplicationName: cfg.ServiceName, MaxConns: cfg.DBMaxConns})
	if err != nil {
		_ = shutdownTracing(ctx)
		return err
	}
	applied, err := pg.Migrate(ctx, pool, migrations.FS, ".")
	if err != nil {
		pool.Close()
		_ = shutdownTracing(ctx)
		return err
	}
	log.Info("migrations applied", "versions", applied)

	reg := metrics.New()
	cache := store.NewCache(store.PG{Pool: pool, Timeout: cfg.DBLoadTimeout}.Load,
		store.CacheConfig{TTL: cfg.CacheTTL, Registerer: reg, Logger: log})
	hc := health.New()
	hc.AddReadinessCheck("tariff", cacheAwareReadiness(pg.ReadinessCheck(pool), cache.Warm), health.WithTimeout(cfg.ReadinessCheck))
	a := api.New(cache, log, reg)

	return server.Run(ctx, cfg.Base, a.Routes(cfg.InternalToken),
		server.WithLogger(log), server.WithHealth(hc), server.WithMetrics(reg),
		server.WithCloser("tracer", shutdownTracing),
		server.WithCloser("postgres", pg.Closer(pool)),
	)
}
