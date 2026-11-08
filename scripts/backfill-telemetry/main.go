// Command backfill-telemetry loads synthetic history into TimescaleDB with
// COPY and measures compression and query latency (ADR 0013).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/sotream/evcharge-lab/scripts/backfill-telemetry/internal/backfill"
	"github.com/sotream/evcharge-lab/scripts/backfill-telemetry/internal/gen"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg := backfill.Config{Log: log.Printf}
	var start, out string
	flag.StringVar(&cfg.DSN, "dsn", "postgres://evcharge:evcharge@localhost:25433/telemetry_backfill", "target database (dedicated; created if missing)")
	flag.StringVar(&cfg.MigrationsDir, "migrations", "services/telemetry-ingest/migrations", "canonical migrations directory")
	flag.IntVar(&cfg.Stations, "stations", 5000, "stations (2 connectors each, 10 per site)")
	flag.Float64Var(&cfg.Days, "days", 7, "days of history")
	flag.StringVar(&start, "start", "", "RFC3339 start (default: now truncated to the hour minus days)")
	flag.IntVar(&cfg.Workers, "workers", 4, "parallel COPY workers")
	flag.IntVar(&cfg.QueryRuns, "query-runs", 300, "runs per measured query (>= 200)")
	flag.Float64Var(&cfg.DCShare, "dc-share", gen.DefaultDCShare, "DC share of the fleet")
	flag.Uint64Var(&cfg.Seed, "seed", 1, "generator seed")
	flag.StringVar(&out, "out", "", "report path (default docs/load-output/telemetry-backfill-<UTC timestamp>.json)")
	flag.Parse()

	if start != "" {
		t, err := time.Parse(time.RFC3339, start)
		if err != nil {
			return fmt.Errorf("-start: %w", err)
		}
		cfg.Start = t
	}
	if out == "" {
		out = filepath.Join("docs", "load-output", "telemetry-backfill-"+time.Now().UTC().Format("20060102T150405Z")+".json")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := ensureDatabase(ctx, cfg.DSN); err != nil {
		return err
	}
	rep, err := backfill.Run(ctx, cfg)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(out, append(b, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Println("wrote", out)
	return nil
}

// ensureDatabase creates the target database when it does not exist. It
// refuses the shared "telemetry" database.
func ensureDatabase(ctx context.Context, dsn string) error {
	u, err := url.Parse(dsn)
	if err != nil {
		return err
	}
	name := strings.TrimPrefix(u.Path, "/")
	if name == "" || name == "telemetry" || name == "postgres" {
		return fmt.Errorf("refusing to backfill into shared database %q; use a dedicated one", name)
	}
	return backfill.CreateDatabaseIfMissing(ctx, dsn, name)
}
