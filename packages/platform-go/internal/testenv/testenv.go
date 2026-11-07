// Package testenv gates integration tests on INTEGRATION=1 and exposes the
// compose infra endpoints (TEST_PG_URL, TEST_PG_TELEMETRY_URL, TEST_REDIS_URL,
// TEST_KAFKA_BROKERS as a comma list).
package testenv

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

// Require skips t unless INTEGRATION is set.
func Require(t testing.TB) {
	t.Helper()
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("integration test: set INTEGRATION=1 (needs `make infra-up`)")
	}
}

func get(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// PGURL is the admin connection URL of the test Postgres.
func PGURL() string { return get("TEST_PG_URL", "postgres://evcharge:evcharge@localhost:25432") }

// PGTelemetryURL is the admin URL of the TimescaleDB-enabled telemetry instance.
func PGTelemetryURL() string {
	return get("TEST_PG_TELEMETRY_URL", "postgres://evcharge:evcharge@localhost:25433/telemetry")
}

// RedisURL is the test Redis URL.
func RedisURL() string { return get("TEST_REDIS_URL", "redis://localhost:26379") }

// KafkaBrokers is the test broker list.
func KafkaBrokers() []string {
	return strings.Split(get("TEST_KAFKA_BROKERS", "localhost:29091,localhost:29092,localhost:29093"), ",")
}

// Unique returns prefix plus a random lowercase hex suffix.
func Unique(prefix string) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}
