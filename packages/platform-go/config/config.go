// Package config loads service configuration from environment variables into
// a struct (via `env` tags) and validates it, failing fast with every problem
// listed at once.
//
// Services embed Base in their own struct:
//
//	type Config struct {
//		config.Base
//		DatabaseURL string `env:"DATABASE_URL"`
//	}
//
//	func (c Config) Validate() error {
//		var p config.Problems
//		p.Merge(c.Base.Validate())
//		p.Require("DATABASE_URL", c.DatabaseURL)
//		return p.Err()
//	}
//
//	var cfg Config
//	if err := config.Load(&cfg); err != nil { log.Fatal(err) }
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Base holds the settings every Go service shares.
type Base struct {
	ServiceName   string        `env:"SERVICE_NAME"`
	LogLevel      string        `env:"LOG_LEVEL"                   envDefault:"info"`
	HTTPPort      int           `env:"HTTP_PORT"                   envDefault:"8080"`
	InternalToken string        `env:"INTERNAL_TOKEN"`
	OTLPEndpoint  string        `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	ShutdownGrace time.Duration `env:"SHUTDOWN_GRACE"              envDefault:"15s"`
}

// MinInternalTokenLen is the minimum accepted INTERNAL_TOKEN length.
const MinInternalTokenLen = 16

// HTTPAddr is the listen address derived from HTTPPort (":8080").
func (b Base) HTTPAddr() string { return ":" + strconv.Itoa(b.HTTPPort) }

// Validate checks the base fields and reports every problem.
func (b Base) Validate() error {
	var p Problems
	p.Require("SERVICE_NAME", b.ServiceName)
	switch strings.ToLower(b.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		p.Addf("LOG_LEVEL must be one of debug|info|warn|error, got %q", b.LogLevel)
	}
	if b.HTTPPort < 1 || b.HTTPPort > 65535 {
		p.Addf("HTTP_PORT must be in 1..65535, got %d", b.HTTPPort)
	}
	if len(b.InternalToken) < MinInternalTokenLen {
		p.Addf("INTERNAL_TOKEN must be at least %d characters", MinInternalTokenLen)
	}
	if b.OTLPEndpoint != "" && !strings.HasPrefix(b.OTLPEndpoint, "http://") && !strings.HasPrefix(b.OTLPEndpoint, "https://") {
		p.Addf("OTEL_EXPORTER_OTLP_ENDPOINT must be an http(s) URL, got %q", b.OTLPEndpoint)
	}
	if b.ShutdownGrace <= 0 {
		p.Addf("SHUTDOWN_GRACE must be a positive duration, got %s", b.ShutdownGrace)
	}
	return p.Err()
}

// Validator is implemented by configuration structs that can check themselves.
type Validator interface {
	Validate() error
}

// Load parses the process environment into cfg and then calls cfg.Validate
// (if cfg implements Validator). Parse and validation problems are reported
// together in a single *ValidationError.
func Load[T any](cfg *T) error {
	return LoadFrom(cfg, envMap(os.Environ()))
}

// LoadFrom is Load with an explicit environment (handy in tests).
func LoadFrom[T any](cfg *T, environ map[string]string) error {
	var p Problems
	if err := env.ParseWithOptions(cfg, env.Options{Environment: environ}); err != nil {
		var agg env.AggregateError
		if errors.As(err, &agg) {
			for _, e := range agg.Errors {
				p.Add(e.Error())
			}
		} else {
			p.Add(err.Error())
		}
	}
	if v, ok := any(cfg).(Validator); ok { // *T also carries T's value methods
		p.Merge(v.Validate())
	}
	return p.Err()
}

func envMap(kv []string) map[string]string {
	m := make(map[string]string, len(kv))
	for _, e := range kv {
		if k, v, ok := strings.Cut(e, "="); ok {
			m[k] = v
		}
	}
	return m
}

// ValidationError lists every configuration problem found.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "invalid configuration:\n  - " + strings.Join(e.Problems, "\n  - ")
}

// Problems accumulates validation messages.
type Problems []string

// Add appends a message.
func (p *Problems) Add(msg string) { *p = append(*p, msg) }

// Addf appends a formatted message.
func (p *Problems) Addf(format string, args ...any) { p.Add(fmt.Sprintf(format, args...)) }

// Require adds "<name> is required" when value is empty.
func (p *Problems) Require(name, value string) {
	if strings.TrimSpace(value) == "" {
		p.Addf("%s is required", name)
	}
}

// Merge appends the problems carried by err (a *ValidationError) or err's
// message. A nil err is ignored.
func (p *Problems) Merge(err error) {
	if err == nil {
		return
	}
	var ve *ValidationError
	if errors.As(err, &ve) {
		*p = append(*p, ve.Problems...)
		return
	}
	p.Add(err.Error())
}

// Err returns nil when there are no problems, otherwise a *ValidationError.
func (p Problems) Err() error {
	if len(p) == 0 {
		return nil
	}
	return &ValidationError{Problems: append([]string(nil), p...)}
}
