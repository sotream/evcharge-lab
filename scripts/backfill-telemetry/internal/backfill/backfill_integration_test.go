package backfill

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestIntegrationTinyRun(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("set INTEGRATION=1")
	}
	base := os.Getenv("TEST_PG_TELEMETRY_URL")
	if base == "" {
		base = "postgres://evcharge:evcharge@localhost:25433/telemetry"
	}
	ctx := context.Background()
	name := fmt.Sprintf("backfill_t_%d", time.Now().UnixNano())
	u, _ := url.Parse(base)
	u.Path = "/" + name
	if err := CreateDatabaseIfMissing(ctx, base, name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = DropDatabase(ctx, base, name) })

	mig, _ := filepath.Abs("../../../../services/telemetry-ingest/migrations")
	start := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	rep, err := Run(ctx, Config{
		DSN: u.String(), MigrationsDir: mig, Stations: 10, Days: 2.0 / 24, Start: start,
		Workers: 2, Seed: 1, QueryRuns: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	rows := rep.Load["rowsInserted"].Value.(int64)
	if rows == 0 || rows != rep.Load["rowCountInDatabase"].Value.(int64) {
		t.Fatalf("rows %v vs db %v", rows, rep.Load["rowCountInDatabase"].Value)
	}
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var buckets int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM power_1m").Scan(&buckets); err != nil || buckets == 0 {
		t.Fatalf("power_1m empty: %v %d", err, buckets)
	}
	// Dedup index must reject a replay of the same reading.
	if _, err := conn.Exec(ctx, `INSERT INTO meter_readings (time, station_id, connector_id)
		SELECT time, station_id, connector_id FROM meter_readings LIMIT 1`); err == nil {
		t.Fatal("duplicate reading accepted")
	}
	if _, ok := rep.Queries["latestPerConnectorForStation"]; !ok {
		t.Fatal("latest query not measured")
	}
	if len(rep.Derived) == 0 {
		t.Fatal("no extrapolation")
	}
}

func TestCompressChunksFailsInsteadOfHanging(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("set INTEGRATION=1")
	}
	base := os.Getenv("TEST_PG_TELEMETRY_URL")
	if base == "" {
		base = "postgres://evcharge:evcharge@localhost:25433/telemetry"
	}
	pool, err := pgxpool.New(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	bad := []string{"no.such1", "no.such2", "no.such3", "no.such4", "no.such5"}
	if err := compressChunks(ctx, pool, 2, bad); err == nil || ctx.Err() != nil {
		t.Fatalf("want fast error, got %v (ctx %v)", err, ctx.Err())
	}
}
