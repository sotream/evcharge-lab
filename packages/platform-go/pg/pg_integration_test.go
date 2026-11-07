package pg

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sotream/evcharge-lab/packages/platform-go/internal/testenv"
)

// tempDB creates a uniquely named database on the OLTP instance and drops it
// when t ends.
func tempDB(t *testing.T) string { return tempDBOn(t, testenv.PGURL()) }

func tempDBOn(t *testing.T, adminURL string) string {
	t.Helper()
	testenv.Require(t)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	name := testenv.Unique("pgtest_")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = admin.Close(context.Background())
	})
	u, _ := url.Parse(adminURL)
	u.Path = "/" + name
	return u.String()
}

// Migrations on the TimescaleDB telemetry instance (telemetry-ingest layout).
func TestIntegrationMigrateTimescale(t *testing.T) {
	dsn := tempDBOn(t, testenv.PGTelemetryURL())
	ctx := context.Background()
	pool, err := Connect(ctx, Config{URL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	fsys := fstest.MapFS{"m/0001_meter_values.sql": {Data: []byte(`
		CREATE EXTENSION IF NOT EXISTS timescaledb;
		CREATE TABLE meter_values (ts timestamptz NOT NULL, station_id text NOT NULL, value double precision NOT NULL);
		SELECT create_hypertable('meter_values', by_range('ts'));`)}}
	if applied, err := Migrate(ctx, pool, fsys, "m"); err != nil || len(applied) != 1 {
		t.Fatalf("%v %v", applied, err)
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name='meter_values'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("hypertable missing: %d %v", n, err)
	}
}

func TestIntegrationConnectAndStatementTimeout(t *testing.T) {
	dsn := tempDB(t)
	ctx := context.Background()
	pool, err := Connect(ctx, Config{URL: dsn, StatementTimeout: 200 * time.Millisecond, ApplicationName: "pgtest"})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := ReadinessCheck(pool)(ctx); err != nil {
		t.Fatal(err)
	}
	var st string
	if err := pool.QueryRow(ctx, "SHOW statement_timeout").Scan(&st); err != nil || st != "200ms" {
		t.Fatalf("statement_timeout=%q %v", st, err)
	}
	_, err = pool.Exec(ctx, "SELECT pg_sleep(2)")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "57014" { // query_canceled
		t.Fatalf("want statement timeout, got %v", err)
	}
	if err := Closer(pool)(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationConnectFailsFast(t *testing.T) {
	testenv.Require(t)
	start := time.Now()
	_, err := Connect(context.Background(), Config{URL: "postgres://x:y@10.255.255.1:5432/db", ConnectTimeout: 300 * time.Millisecond})
	if err == nil {
		t.Fatal("expected error")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("connect timeout not honoured: %s", time.Since(start))
	}
}

var migrations = fstest.MapFS{
	"migrations/0001_init.sql": {Data: []byte(`
		CREATE TABLE tariffs (id text PRIMARY KEY, currency text NOT NULL);
		INSERT INTO tariffs VALUES ('t1', 'EUR');`)},
	"migrations/0002_add_price.sql": {Data: []byte(`ALTER TABLE tariffs ADD COLUMN price_minor bigint NOT NULL DEFAULT 0;`)},
}

func TestIntegrationMigrate(t *testing.T) {
	dsn := tempDB(t)
	ctx := context.Background()
	pool, err := Connect(ctx, Config{URL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Concurrent instances: the advisory lock makes exactly one apply.
	var wg sync.WaitGroup
	results := make([][]int, 4)
	errs := make([]error, 4)
	for i := range 4 {
		wg.Go(func() { results[i], errs[i] = Migrate(ctx, pool, migrations, "migrations") })
	}
	wg.Wait()
	total := 0
	for i := range 4 {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		total += len(results[i])
	}
	if total != 2 {
		t.Fatalf("migrations applied %d times, want 2 (%v)", total, results)
	}

	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&n); err != nil || n != 2 {
		t.Fatalf("schema_migrations rows=%d %v", n, err)
	}
	var price int64
	if err := pool.QueryRow(ctx, "SELECT price_minor FROM tariffs WHERE id='t1'").Scan(&price); err != nil {
		t.Fatal(err)
	}

	// Re-run is a no-op.
	again, err := Migrate(ctx, pool, migrations, "migrations")
	if err != nil || len(again) != 0 {
		t.Fatalf("rerun applied %v %v", again, err)
	}

	// A new file is applied incrementally.
	more := fstest.MapFS{}
	for k, v := range migrations {
		more[k] = v
	}
	more["migrations/0003_seed.sql"] = &fstest.MapFile{Data: []byte(`INSERT INTO tariffs VALUES ('t2', 'USD', 5);`)}
	applied, err := Migrate(ctx, pool, more, "migrations")
	if err != nil || len(applied) != 1 || applied[0] != 3 {
		t.Fatalf("incremental %v %v", applied, err)
	}

	// Editing an applied migration is detected.
	more["migrations/0001_init.sql"] = &fstest.MapFile{Data: []byte(`SELECT 1;`)}
	if _, err := Migrate(ctx, pool, more, "migrations"); err == nil || !strings.Contains(err.Error(), "modified") {
		t.Fatalf("want checksum error, got %v", err)
	}
}

func TestIntegrationMigrateFailureRollsBack(t *testing.T) {
	dsn := tempDB(t)
	ctx := context.Background()
	pool, err := Connect(ctx, Config{URL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	bad := fstest.MapFS{
		"m/0001_ok.sql":     {Data: []byte(`CREATE TABLE a (id int);`)},
		"m/0002_broken.sql": {Data: []byte(`CREATE TABLE b (id int); SELECT * FROM nope;`)},
	}
	applied, err := Migrate(ctx, pool, bad, "m")
	if err == nil || len(applied) != 1 {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('b') IS NOT NULL").Scan(&exists); err != nil || exists {
		t.Fatalf("table b should be rolled back (exists=%v, %v)", exists, err)
	}
	// Lock was released: another migrate call proceeds (and fails the same way).
	lctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := Migrate(lctx, pool, bad, "m"); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second run: %v", err)
	}
}
