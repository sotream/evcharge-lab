// Package app wires telemetry-ingest: config, migrations, the ingest
// pipeline, the query API and the shared server lifecycle.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/sotream/evcharge-lab/packages/platform-go/config"
	"github.com/sotream/evcharge-lab/packages/platform-go/health"
	"github.com/sotream/evcharge-lab/packages/platform-go/metrics"
	"github.com/sotream/evcharge-lab/packages/platform-go/pg"
	"github.com/sotream/evcharge-lab/packages/platform-go/redisx"
	"github.com/sotream/evcharge-lab/packages/platform-go/server"
	"github.com/sotream/evcharge-lab/packages/platform-go/telemetry"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/internal/api"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/internal/ingest"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/migrations"
)

// Config is the service environment (fails fast on any invalid value).
type Config struct {
	config.Base
	DatabaseURL     string        `env:"DATABASE_URL"`
	RedisURL        string        `env:"REDIS_URL"`
	KafkaBrokers    []string      `env:"KAFKA_BROKERS"          envSeparator:","`
	Topic           string        `env:"INGEST_TOPIC"           envDefault:"telemetry.meter-values.v1"`
	Group           string        `env:"INGEST_GROUP"           envDefault:"telemetry-ingest.meter-values"`
	FlushRows       int           `env:"INGEST_FLUSH_ROWS"      envDefault:"5000"`
	FlushInterval   time.Duration `env:"INGEST_FLUSH_INTERVAL"  envDefault:"250ms"`
	MaxBufferedRows int           `env:"INGEST_MAX_BUFFERED"    envDefault:"50000"`
	IngestConns     int32         `env:"INGEST_DB_CONNS"        envDefault:"4"`
	QueryConns      int32         `env:"QUERY_DB_CONNS"         envDefault:"8"`
}

func (c Config) Validate() error {
	var p config.Problems
	p.Merge(c.Base.Validate())
	p.Require("DATABASE_URL", c.DatabaseURL)
	p.Require("REDIS_URL", c.RedisURL)
	if len(c.KafkaBrokers) == 0 {
		p.Add("KAFKA_BROKERS is required")
	}
	if c.FlushRows < 1 {
		p.Addf("INGEST_FLUSH_ROWS must be >= 1, got %d", c.FlushRows)
	}
	if c.FlushInterval <= 0 {
		p.Addf("INGEST_FLUSH_INTERVAL must be positive, got %s", c.FlushInterval)
	}
	if c.MaxBufferedRows < c.FlushRows {
		p.Addf("INGEST_MAX_BUFFERED (%d) must be >= INGEST_FLUSH_ROWS (%d)", c.MaxBufferedRows, c.FlushRows)
	}
	if c.IngestConns < 1 || c.QueryConns < 1 {
		p.Add("INGEST_DB_CONNS and QUERY_DB_CONNS must be >= 1")
	}
	return p.Err()
}

// Options adjust Run for tests.
type Options struct {
	Listener   net.Listener
	DrainDelay *time.Duration
}

// Run starts everything and blocks until shutdown.
func Run(ctx context.Context, cfg Config, log *slog.Logger, o Options) error {
	shutdownTracing, err := telemetry.Setup(ctx, cfg.ServiceName, cfg.OTLPEndpoint)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	queryPool, err := pg.Connect(ctx, pg.Config{URL: cfg.DatabaseURL, ApplicationName: cfg.ServiceName + "-query", MaxConns: cfg.QueryConns, StatementTimeout: 5 * time.Second})
	if err != nil {
		return err
	}
	applied, err := pg.Migrate(ctx, queryPool, migrations.FS, ".")
	if err != nil {
		queryPool.Close()
		return err
	}
	log.InfoContext(ctx, "migrations up to date", "applied", applied)
	ingestPool, err := ingest.ConnectIngestPool(ctx, cfg.DatabaseURL, cfg.ServiceName+"-ingest", cfg.IngestConns)
	if err != nil {
		queryPool.Close()
		return err
	}
	rdb, err := redisx.Connect(ctx, redisx.Config{URL: cfg.RedisURL})
	if err != nil {
		queryPool.Close()
		ingestPool.Close()
		return err
	}

	reg := metrics.New()
	m, err := ingest.NewMetrics(reg)
	if err != nil {
		return err
	}
	pipe, err := ingest.New(ingest.Config{
		Brokers: cfg.KafkaBrokers, Group: cfg.Group, Topic: cfg.Topic, ClientID: cfg.ServiceName,
		FlushRows: cfg.FlushRows, FlushInterval: cfg.FlushInterval, MaxBufferedRows: cfg.MaxBufferedRows,
		Logger: log,
	}, &ingest.CopyWriter{Pool: ingestPool}, &ingest.RedisLatest{Client: rdb}, m)
	if err != nil {
		return err
	}

	hc := health.New()
	hc.AddReadinessCheck("postgres", pg.ReadinessCheck(queryPool))
	hc.AddReadinessCheck("redis", redisx.ReadinessCheck(rdb))
	hc.AddReadinessCheck("kafka", pipe.ReadinessCheck)

	mux := http.NewServeMux()
	(&api.Handler{DB: queryPool, Redis: rdb}).Register(mux, cfg.InternalToken)

	opts := []server.Option{
		server.WithLogger(log), server.WithHealth(hc), server.WithMetrics(reg),
		server.WithRunner("ingest", pipe.Run),
		// Closers run in reverse: pipeline (leave group), redis, pools, tracer.
		server.WithCloser("tracer", shutdownTracing),
		server.WithCloser("postgres-query", pg.Closer(queryPool)),
		server.WithCloser("postgres-ingest", pg.Closer(ingestPool)),
		server.WithCloser("redis", redisx.Closer(rdb)),
		server.WithCloser("ingest", pipe.Close),
	}
	if o.Listener != nil {
		opts = append(opts, server.WithListener(o.Listener))
	}
	if o.DrainDelay != nil {
		opts = append(opts, server.WithDrainDelay(*o.DrainDelay))
	}
	return server.Run(ctx, cfg.Base, mux, opts...)
}
