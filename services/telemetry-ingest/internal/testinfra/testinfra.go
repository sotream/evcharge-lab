// Package testinfra provides integration-test fixtures against the compose
// infra: a temporary TimescaleDB database, unique Kafka topics (RF 3) and a
// Redis client. Tests using it are skipped unless INTEGRATION is set.
package testinfra

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
	"github.com/sotream/evcharge-lab/packages/platform-go/pg"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/migrations"
)

// Require skips t unless INTEGRATION is set.
func Require(t testing.TB) {
	t.Helper()
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("integration test: set INTEGRATION=1 (needs the compose infra)")
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func TelemetryURL() string {
	return env("TEST_PG_TELEMETRY_URL", "postgres://evcharge:evcharge@localhost:25433/telemetry")
}
func RedisURL() string { return env("TEST_REDIS_URL", "redis://localhost:26379") }
func Brokers() []string {
	return strings.Split(env("TEST_KAFKA_BROKERS", "localhost:29091,localhost:29092,localhost:29093"), ",")
}

// Unique returns prefix + 12 random hex chars.
func Unique(prefix string) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}

func withDB(raw, db string) string {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + db
	return u.String()
}

// TempDB creates a unique database on the Timescale instance, applies the
// service migrations and drops it (WITH FORCE) on cleanup.
func TempDB(tb testing.TB) (dsn string, pool *pgxpool.Pool) {
	tb.Helper()
	ctx := context.Background()
	name := Unique("tingest_")
	admin, err := pgx.Connect(ctx, TelemetryURL())
	if err != nil {
		tb.Fatalf("connect telemetry pg: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		tb.Fatalf("create db: %v", err)
	}
	_ = admin.Close(ctx)
	dsn = withDB(TelemetryURL(), name)
	pool, err = pg.Connect(ctx, pg.Config{URL: dsn, ApplicationName: "telemetry-ingest-test"})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := pg.Migrate(ctx, pool, migrations.FS, "."); err != nil {
		tb.Fatalf("migrate: %v", err)
	}
	tb.Cleanup(func() {
		pool.Close()
		c, err := pgx.Connect(context.Background(), TelemetryURL())
		if err != nil {
			tb.Logf("drop db: %v", err)
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		// Timescale background workers may hold a connection; FORCE ends them.
		if _, err := c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			tb.Logf("drop db: %v", err)
		}
	})
	return dsn, pool
}

// Topic creates a unique topic (and its .dlq) with RF 3 and deletes both on cleanup.
func Topic(tb testing.TB, partitions int32) string {
	tb.Helper()
	name := Unique("test.telemetry-ingest.")
	cl, err := kgo.NewClient(kgo.SeedBrokers(Brokers()...))
	if err != nil {
		tb.Fatal(err)
	}
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := map[string]*string{"min.insync.replicas": new("2")}
	if _, err := adm.CreateTopic(ctx, partitions, 3, cfg, name); err != nil {
		tb.Fatalf("create topic: %v", err)
	}
	if _, err := adm.CreateTopic(ctx, 1, 3, cfg, name+".dlq"); err != nil {
		tb.Fatalf("create dlq topic: %v", err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = adm.DeleteTopics(ctx, name, name+".dlq")
		cl.Close()
	})
	return name
}

// Producer returns a synchronous test producer (acks=all).
func Producer(tb testing.TB) *kgo.Client {
	tb.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(Brokers()...), kgo.ProducerLinger(2*time.Millisecond))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(cl.Close)
	return cl
}

// Redis returns a client on the test Redis.
func Redis(tb testing.TB) *redis.Client {
	tb.Helper()
	o, err := redis.ParseURL(RedisURL())
	if err != nil {
		tb.Fatal(err)
	}
	c := redis.NewClient(o)
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// MeterRecord builds a MeterValuesReceived record keyed by station.
func MeterRecord(topic string, mv contracts.MeterValues) *kgo.Record {
	env := contracts.Envelope[contracts.MeterValues]{
		EventID: uuid(), Type: contracts.EventTypeMeterValues, Version: 1, OccurredAt: time.Now().UTC(),
		Producer: "test", Key: mv.StationID, Data: mv,
	}
	b, err := json.Marshal(env)
	if err != nil {
		panic(err)
	}
	return &kgo.Record{Topic: topic, Key: []byte(mv.StationID), Value: b}
}

// Reading is a one-reading MeterValues payload with the usual 5 measurands.
func Reading(station string, connector int, at time.Time, powerW float64, replayed bool) contracts.MeterValues {
	tx := int64(1)
	return contracts.MeterValues{
		StationID: station, ConnectorID: connector, TransactionID: &tx, OcppMessageID: uuid(), Replayed: replayed,
		Readings: []contracts.MeterReading{{Timestamp: at.UTC(), Samples: []contracts.MeterSample{
			{Measurand: "Energy.Active.Import.Register", Value: float64(at.Unix()%100000) / 10, Unit: "kWh"},
			{Measurand: "Power.Active.Import", Value: powerW, Unit: "W"},
			{Measurand: "Current.Import", Value: powerW / 230, Unit: "A"},
			{Measurand: "Voltage", Value: 230, Unit: "V"},
			{Measurand: "SoC", Value: 50, Unit: "Percent"},
		}}},
	}
}

func uuid() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// Eventually polls cond every 50 ms until it is true or timeout elapses.
func Eventually(tb testing.TB, timeout time.Duration, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
