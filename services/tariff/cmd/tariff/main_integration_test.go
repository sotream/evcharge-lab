package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
	"github.com/sotream/evcharge-lab/packages/platform-go/config"
)

const token = "integration-internal-token"

// tempDB creates a unique database on the OLTP instance (TEST_PG_URL) and
// drops it when t ends.
func tempDB(t *testing.T) string {
	t.Helper()
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("integration test: set INTEGRATION=1")
	}
	admin := os.Getenv("TEST_PG_URL")
	if admin == "" {
		admin = "postgres://evcharge:evcharge@localhost:25432"
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "tariff_test_" + hex.EncodeToString(b)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = conn.Close(context.Background())
	})
	u, _ := url.Parse(admin)
	u.Path = "/" + name
	return u.String()
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func call(t *testing.T, method, u, body string, hdr map[string]string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), method, u, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func TestIntegrationService(t *testing.T) {
	dsn := tempDB(t)
	port := freePort(t)
	var cfg Config
	if err := config.LoadFrom(&cfg, map[string]string{
		"SERVICE_NAME": "tariff-it", "INTERNAL_TOKEN": token, "HTTP_PORT": strconv.Itoa(port),
		"DATABASE_URL": dsn, "SHUTDOWN_GRACE": "3s", "TARIFF_CACHE_TTL": "1s",
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, slog.New(slog.DiscardHandler)) }()
	base := "http://127.0.0.1:" + strconv.Itoa(port)
	deadline := time.Now().Add(15 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, "GET", base+"/health/ready", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("service not ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	auth := map[string]string{"x-internal-token": token}

	// Seeded tou across the off-peak start (22:30 → 00:30 Kyiv).
	q := `{"tariffId":"tou","startedAt":"2026-01-14T20:30:00Z","stoppedAt":"2026-01-14T22:30:00Z","energyWh":3000}`
	status, body := call(t, "POST", base+"/v1/quote", q, auth)
	if status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	var res contracts.QuoteRes
	_ = json.Unmarshal(body, &res)
	if res.TotalMinor != 103 || res.Currency != "EUR" {
		t.Fatalf("%s", body)
	}
	for id, want := range map[string]int64{"std": 49 + 117, "fleet": 75} {
		status, body := call(t, "POST", base+"/v1/quote", strings.Replace(q, `"tou"`, `"`+id+`"`, 1), auth)
		_ = json.Unmarshal(body, &res)
		if status != 200 || res.TotalMinor != want {
			t.Fatalf("%s: %d %s", id, status, body)
		}
	}
	// ADR 0010: no token → 401 even with a forwarded driver identity.
	if status, _ := call(t, "POST", base+"/v1/quote", q, map[string]string{"x-driver-id": "d1"}); status != 401 {
		t.Fatalf("status %d", status)
	}
	if status, _ := call(t, "GET", base+"/v1/tariffs/std", "", nil); status != 401 {
		t.Fatalf("status %d", status)
	}
	status, body = call(t, "GET", base+"/v1/tariffs/tou", "", auth)
	if status != 200 || !strings.Contains(string(body), `"timezone":"Europe/Kyiv"`) || !strings.Contains(string(body), `"priceMinorPerKwh":29`) {
		t.Fatalf("%d %s", status, body)
	}

	// Versions: immutable rows; a new version applies from its valid_from only.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, `UPDATE tariff_versions SET energy_price_minor_per_kwh = 1 WHERE tariff_id = 'std'`); err == nil {
		t.Fatal("update of a tariff version must fail")
	}
	// A3: a new version must start at least 15 minutes ahead (cache TTL + stale window).
	for _, vf := range []time.Time{time.Now().Add(-time.Hour), time.Now(), time.Now().Add(10 * time.Minute)} {
		_, err := conn.Exec(ctx, `INSERT INTO tariff_versions (tariff_id, version, valid_from, currency, timezone,
			session_fee_minor, energy_price_minor_per_kwh) VALUES ('std', 2, $1, 'EUR', 'Europe/Berlin', 49, 45)`, vf)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Fatalf("valid_from %s must be rejected with check_violation, got %v", vf, err)
		}
	}
	vf := time.Now().UTC().Add(time.Hour).Truncate(time.Minute)
	if _, err := conn.Exec(ctx, `INSERT INTO tariff_versions (tariff_id, version, valid_from, currency, timezone,
		session_fee_minor, energy_price_minor_per_kwh) VALUES ('std', 2, $1, 'EUR', 'Europe/Berlin', 49, 45)`, vf); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond) // TARIFF_CACHE_TTL=1s
	quoteAt := func(start time.Time) string {
		return fmt.Sprintf(`{"tariffId":"std","startedAt":%q,"stoppedAt":%q,"energyWh":1000}`,
			start.Format(time.RFC3339), start.Add(time.Hour).Format(time.RFC3339))
	}
	before := quoteAt(vf.Add(-90 * time.Minute))
	after := quoteAt(vf.Add(time.Minute))
	for body, want := range map[string]int64{before: 49 + 39, after: 49 + 45} {
		status, b := call(t, "POST", base+"/v1/quote", body, auth)
		_ = json.Unmarshal(b, &res)
		if status != 200 || res.TotalMinor != want {
			t.Fatalf("%d %s want %d", status, b, want)
		}
	}

	status, body = call(t, "GET", base+"/metrics", "", nil)
	if status != 200 || !strings.Contains(string(body), `tariff_quotes_total{result="ok",tariff_id="std"}`) ||
		!strings.Contains(string(body), "tariff_cache_events_total") {
		t.Fatalf("metrics: %s", body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no graceful shutdown")
	}
}

func TestConfigValidation(t *testing.T) {
	var cfg Config
	err := config.LoadFrom(&cfg, map[string]string{"SERVICE_NAME": "tariff", "INTERNAL_TOKEN": "short", "DB_MAX_CONNS": "0"})
	if err == nil {
		t.Fatal("expected validation error")
	}
	for _, want := range []string{"DATABASE_URL", "INTERNAL_TOKEN", "DB_MAX_CONNS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
