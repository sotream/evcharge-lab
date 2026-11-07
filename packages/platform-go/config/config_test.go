package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func validEnv() map[string]string {
	return map[string]string{
		"SERVICE_NAME":   "tariff",
		"INTERNAL_TOKEN": "0123456789abcdef",
	}
}

func TestLoadDefaults(t *testing.T) {
	var cfg Base
	if err := LoadFrom(&cfg, validEnv()); err != nil {
		t.Fatal(err)
	}
	if cfg.LogLevel != "info" || cfg.HTTPPort != 8080 || cfg.ShutdownGrace != 15*time.Second || cfg.OTLPEndpoint != "" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.HTTPAddr() != ":8080" {
		t.Fatalf("addr %q", cfg.HTTPAddr())
	}
}

func TestLoadOverrides(t *testing.T) {
	e := validEnv()
	e["LOG_LEVEL"] = "debug"
	e["HTTP_PORT"] = "9000"
	e["SHUTDOWN_GRACE"] = "3s"
	e["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://jaeger:4318"
	var cfg Base
	if err := LoadFrom(&cfg, e); err != nil {
		t.Fatal(err)
	}
	if cfg.LogLevel != "debug" || cfg.HTTPPort != 9000 || cfg.ShutdownGrace != 3*time.Second || cfg.OTLPEndpoint != "http://jaeger:4318" {
		t.Fatalf("unexpected: %+v", cfg)
	}
}

func TestLoadListsEveryProblem(t *testing.T) {
	e := map[string]string{
		"LOG_LEVEL":                   "loud",
		"HTTP_PORT":                   "70000",
		"INTERNAL_TOKEN":              "short",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "jaeger:4318",
		"SHUTDOWN_GRACE":              "soon",
	}
	var cfg Base
	err := LoadFrom(&cfg, e)
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want *ValidationError, got %v", err)
	}
	msg := err.Error()
	for _, want := range []string{"SERVICE_NAME is required", "LOG_LEVEL", "HTTP_PORT", "INTERNAL_TOKEN", "OTEL_EXPORTER_OTLP_ENDPOINT", "SHUTDOWN_GRACE"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error does not mention %s:\n%s", want, msg)
		}
	}
}

type svcConfig struct {
	Base
	DatabaseURL string `env:"DATABASE_URL"`
}

func (c svcConfig) Validate() error {
	var p Problems
	p.Merge(c.Base.Validate())
	p.Require("DATABASE_URL", c.DatabaseURL)
	return p.Err()
}

func TestEmbeddedValidate(t *testing.T) {
	var cfg svcConfig
	err := LoadFrom(&cfg, map[string]string{"INTERNAL_TOKEN": "0123456789abcdef"})
	var ve *ValidationError
	if !errors.As(err, &ve) || len(ve.Problems) != 2 {
		t.Fatalf("want 2 problems, got %v", err)
	}
	e := validEnv()
	e["DATABASE_URL"] = "postgres://x"
	if err := LoadFrom(&cfg, e); err != nil {
		t.Fatal(err)
	}
}

func TestProblemsMergePlainError(t *testing.T) {
	var p Problems
	p.Merge(nil)
	if p.Err() != nil {
		t.Fatal("expected nil")
	}
	p.Merge(errors.New("boom"))
	if len(p) != 1 || p[0] != "boom" {
		t.Fatalf("%v", p)
	}
}
