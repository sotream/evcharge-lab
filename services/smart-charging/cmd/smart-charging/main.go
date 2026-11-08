// Command smart-charging is the stateless site power allocator (ADR 0006).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/sotream/evcharge-lab/packages/platform-go/config"
	"github.com/sotream/evcharge-lab/packages/platform-go/logging"

	"github.com/sotream/evcharge-lab/services/smart-charging/internal/app"
)

func main() {
	var cfg app.Config
	if err := config.Load(&cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := logging.New(os.Stdout, cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(log)
	if err := app.Run(context.Background(), cfg, log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}
