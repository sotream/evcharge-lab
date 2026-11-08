// Command telemetry-ingest consumes telemetry.meter-values.v1 into
// TimescaleDB and serves power queries (ADR 0008).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/sotream/evcharge-lab/packages/platform-go/config"
	"github.com/sotream/evcharge-lab/packages/platform-go/logging"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/internal/app"
)

func main() {
	var cfg app.Config
	if err := config.Load(&cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := logging.New(os.Stdout, cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(log)
	if err := app.Run(context.Background(), cfg, log, app.Options{}); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}
