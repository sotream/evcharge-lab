package app

import (
	"errors"
	"strings"
	"testing"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
	"github.com/sotream/evcharge-lab/packages/platform-go/config"
)

func TestConfigDefaultsMatchContracts(t *testing.T) {
	var cfg Config
	err := config.LoadFrom(&cfg, map[string]string{
		"SERVICE_NAME": "telemetry-ingest", "INTERNAL_TOKEN": "0123456789abcdef",
		"DATABASE_URL": "postgres://x", "REDIS_URL": "redis://x", "KAFKA_BROKERS": "a:9092,b:9092",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Topic != contracts.TopicMeterValues || cfg.Group != contracts.GroupTelemetryIngest {
		t.Fatalf("topic/group = %s/%s", cfg.Topic, cfg.Group)
	}
	if cfg.FlushRows != 5000 || cfg.FlushInterval.Milliseconds() != 250 || cfg.MaxBufferedRows != 50000 || len(cfg.KafkaBrokers) != 2 {
		t.Fatalf("ADR 0008 defaults: %+v", cfg)
	}
}

func TestConfigFailsFastListingEveryProblem(t *testing.T) {
	var cfg Config
	err := config.LoadFrom(&cfg, map[string]string{"INGEST_FLUSH_ROWS": "10", "INGEST_MAX_BUFFERED": "5"})
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v", err)
	}
	for _, want := range []string{"SERVICE_NAME", "INTERNAL_TOKEN", "DATABASE_URL", "REDIS_URL", "KAFKA_BROKERS", "INGEST_MAX_BUFFERED"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s in %v", want, err)
		}
	}
}
