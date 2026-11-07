// Package redisx creates go-redis v9 clients with explicit timeouts and a
// readiness check. (Named redisx so callers can import it next to
// github.com/redis/go-redis/v9 without aliasing.)
package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config configures Connect. Zero durations pick the defaults noted.
type Config struct {
	URL          string        // redis://[:password@]host:port[/db]
	DialTimeout  time.Duration // default 2s
	ReadTimeout  time.Duration // default 1s
	WriteTimeout time.Duration // default 1s
	PoolTimeout  time.Duration // wait for a free connection, default 2s
	PoolSize     int           // default go-redis' 10*GOMAXPROCS
}

// Options turns cfg into *redis.Options (URL parsed, defaults applied).
func Options(cfg Config) (*redis.Options, error) {
	o, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("redisx: parse url: %w", err)
	}
	o.DialTimeout = orDefault(cfg.DialTimeout, 2*time.Second)
	o.ReadTimeout = orDefault(cfg.ReadTimeout, time.Second)
	o.WriteTimeout = orDefault(cfg.WriteTimeout, time.Second)
	o.PoolTimeout = orDefault(cfg.PoolTimeout, 2*time.Second)
	o.ContextTimeoutEnabled = true // honour ctx deadlines shorter than Read/WriteTimeout
	if cfg.PoolSize > 0 {
		o.PoolSize = cfg.PoolSize
	}
	return o, nil
}

// Connect builds the client and pings it once (bounded by DialTimeout).
func Connect(ctx context.Context, cfg Config) (*redis.Client, error) {
	o, err := Options(cfg)
	if err != nil {
		return nil, err
	}
	c := redis.NewClient(o)
	pctx, cancel := context.WithTimeout(ctx, o.DialTimeout)
	defer cancel()
	if err := c.Ping(pctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("redisx: ping: %w", err)
	}
	return c, nil
}

// ReadinessCheck returns a health check that PINGs the server.
func ReadinessCheck(c redis.UniversalClient) func(context.Context) error {
	return func(ctx context.Context) error { return c.Ping(ctx).Err() }
}

// Closer adapts c.Close to the server.WithCloser signature.
func Closer(c redis.UniversalClient) func(context.Context) error {
	return func(context.Context) error { return c.Close() }
}

func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}
