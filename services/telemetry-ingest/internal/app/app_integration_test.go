package app_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sotream/evcharge-lab/packages/platform-go/config"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/internal/api"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/internal/app"
	"github.com/sotream/evcharge-lab/services/telemetry-ingest/internal/testinfra"
)

const token = "it-internal-token-0123456789"

func get(t *testing.T, url string, withToken bool, out any) int {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if withToken {
		req.Header.Set("x-internal-token", token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	b, _ := io.ReadAll(res.Body)
	if out != nil && res.StatusCode == http.StatusOK {
		if s, ok := out.(*string); ok {
			*s = string(b)
		} else if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("decode %s: %v", b, err)
		}
	}
	return res.StatusCode
}

func TestServiceEndToEnd(t *testing.T) {
	testinfra.Require(t)
	topic := testinfra.Topic(t, 3)
	dsn, pool := testinfra.TempDB(t)
	var cfg app.Config
	if err := config.LoadFrom(&cfg, map[string]string{
		"SERVICE_NAME": "telemetry-ingest", "INTERNAL_TOKEN": token, "LOG_LEVEL": "error",
		"DATABASE_URL": dsn, "REDIS_URL": testinfra.RedisURL(), "KAFKA_BROKERS": strings.Join(testinfra.Brokers(), ","),
		"INGEST_TOPIC": topic, "INGEST_GROUP": testinfra.Unique("tingest-app-"),
	}); err != nil {
		t.Fatal(err)
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	noDrain := time.Duration(0)
	go func() {
		done <- app.Run(ctx, cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)), app.Options{Listener: ln, DrainDelay: &noDrain})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("service did not stop")
		}
	})
	testinfra.Eventually(t, 30*time.Second, "ready", func() bool { return get(t, base+"/health/ready", false, nil) == http.StatusOK })

	station := testinfra.Unique("APP-")
	t.Cleanup(func() { testinfra.Redis(t).Del(context.Background(), "power:"+station+":1") })
	start := time.Now().UTC().Truncate(3 * time.Minute).Add(-3 * time.Minute) // time_bucket aligns to the epoch
	prod := testinfra.Producer(t)
	var recs []*kgo.Record
	for i := range 6 { // two readings per minute over three minutes
		at := start.Add(time.Duration(i) * 30 * time.Second)
		recs = append(recs, testinfra.MeterRecord(topic, testinfra.Reading(station, 1, at, float64(1000*(i+1)), false)))
	}
	pctx, pcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer pcancel()
	if err := prod.ProduceSync(pctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	var latest api.LatestRes
	testinfra.Eventually(t, 30*time.Second, "latest", func() bool {
		// The endpoint falls back to the database until the Redis hash is written (just after the flush).
		return get(t, base+"/v1/stations/"+station+"/connectors/1/latest", true, &latest) == http.StatusOK && latest.Source == "redis"
	})
	if latest.PowerW != 6000 || latest.Source != "redis" || !latest.At.Equal(start.Add(150*time.Second)) {
		t.Fatalf("latest = %+v", latest)
	}

	// power_1m serves materialized data only; the 1-minute refresh policy would make this test wait.
	if _, err := pool.Exec(context.Background(), "CALL refresh_continuous_aggregate('power_1m', $1::timestamptz, $2::timestamptz)", start.Add(-time.Hour), start.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var series api.PowerSeriesRes
	q := "?from=" + start.Format(time.RFC3339) + "&to=" + start.Add(3*time.Minute).Format(time.RFC3339) + "&step=1m"
	if code := get(t, base+"/v1/stations/"+station+"/connectors/1/power"+q, true, &series); code != http.StatusOK {
		t.Fatalf("power status %d", code)
	}
	if len(series.Points) != 3 || series.Points[0].AvgW != 1500 || series.Points[0].MaxW != 2000 || series.Points[2].AvgW != 5500 {
		t.Fatalf("series = %+v", series)
	}
	// Re-bucketed to 3 minutes: one point, sample-weighted average.
	q = strings.Replace(q, "step=1m", "step=3m", 1)
	if code := get(t, base+"/v1/stations/"+station+"/connectors/1/power"+q, true, &series); code != http.StatusOK || len(series.Points) != 1 || series.Points[0].AvgW != 3500 || series.Points[0].MaxW != 6000 {
		t.Fatalf("3m series (%d) = %+v", code, series)
	}

	// Internal auth (ADR 0010): no token -> 401, even with x-driver-id.
	if code := get(t, base+"/v1/stations/"+station+"/connectors/1/latest", false, nil); code != http.StatusUnauthorized {
		t.Fatalf("no-token status %d", code)
	}
	if code := get(t, base+"/v1/stations/nope/connectors/1/latest", true, nil); code != http.StatusNotFound {
		t.Fatalf("unknown connector status %d", code)
	}
	var metricsText string
	get(t, base+"/metrics", false, &metricsText)
	for _, m := range []string{"telemetry_rows_ingested_total 6", "telemetry_flush_seconds_bucket", `telemetry_ingest_lag_seconds_bucket{stage="durable"`, "telemetry_dedup_dropped_total", "kafka_consumer_lag{"} {
		if !strings.Contains(metricsText, m) {
			t.Errorf("metrics missing %q", m)
		}
	}
	if get(t, base+"/health/live", false, nil) != http.StatusOK {
		t.Fatal("live")
	}
}
