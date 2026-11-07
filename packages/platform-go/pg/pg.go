// Package pg opens pgx/v5 connection pools with production defaults
// (connect timeout, server-side statement_timeout), exposes a readiness
// check, and applies embedded SQL migrations.
package pg

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config configures Connect. Zero values pick the defaults noted.
type Config struct {
	URL string // postgres://user:pass@host:port/db?...
	// ConnectTimeout bounds establishing one connection (default 5s).
	ConnectTimeout time.Duration
	// StatementTimeout is set as the session statement_timeout (default 10s;
	// negative disables). Long-running jobs can override per transaction
	// with SET LOCAL statement_timeout.
	StatementTimeout time.Duration
	// MaxConns caps the pool (default: pgxpool's default, max(4, NumCPU)).
	MaxConns int32
	// ApplicationName is reported in pg_stat_activity.
	ApplicationName string
}

// Connect parses cfg, creates the pool and pings it once.
func Connect(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("pg: parse url: %w", err)
	}
	ct := cfg.ConnectTimeout
	if ct <= 0 {
		ct = 5 * time.Second
	}
	pc.ConnConfig.ConnectTimeout = ct
	st := cfg.StatementTimeout
	if st == 0 {
		st = 10 * time.Second
	}
	if st > 0 {
		pc.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(st.Milliseconds(), 10)
	}
	if cfg.ApplicationName != "" {
		pc.ConnConfig.RuntimeParams["application_name"] = cfg.ApplicationName
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("pg: new pool: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, ct)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	return pool, nil
}

// ReadinessCheck returns a health check that pings the pool.
func ReadinessCheck(pool *pgxpool.Pool) func(context.Context) error {
	return pool.Ping
}

// Closer adapts pool.Close to the server.WithCloser signature.
func Closer(pool *pgxpool.Pool) func(context.Context) error {
	return func(context.Context) error { pool.Close(); return nil }
}
