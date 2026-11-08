package ingest

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the ADR 0008 ingest metrics.
type Metrics struct {
	RowsIngested    prometheus.Counter
	DedupDropped    prometheus.Counter
	Unknown         *prometheus.CounterVec
	DeadLettered    prometheus.Counter
	FlushSeconds    prometheus.Histogram
	FlushRows       prometheus.Histogram
	FlushErrors     prometheus.Counter
	Lag             *prometheus.HistogramVec
	Buffered        prometheus.Gauge
	Paused          prometheus.Gauge
	LatestErrors    prometheus.Counter
	ConsumerLag     *prometheus.GaugeVec
	EmptyReadings   prometheus.Counter
	RebalanceDrops  prometheus.Counter
	RecordsConsumed prometheus.Counter
}

// lagBuckets cover the "durable p99 < 5 s" SLO with resolution around it.
var lagBuckets = []float64{.05, .1, .25, .5, .75, 1, 1.5, 2, 3, 5, 7.5, 10, 20, 30, 60, 120, 300}

// NewMetrics creates the metrics and registers them on reg (nil: unregistered,
// for tests and the bench). Collectors already registered under the same name
// are reused.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		RowsIngested: prometheus.NewCounter(prometheus.CounterOpts{Name: "telemetry_rows_ingested_total", Help: "Meter reading rows newly inserted into meter_readings."}),
		DedupDropped: prometheus.NewCounter(prometheus.CounterOpts{Name: "telemetry_dedup_dropped_total", Help: "Rows skipped by ON CONFLICT DO NOTHING (redelivery or station replay)."}),
		Unknown: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "telemetry_unknown_measurand_total", Help: "Samples dropped because the measurand (or its unit) is not mapped to the wide row."},
			[]string{"measurand"}),
		DeadLettered: prometheus.NewCounter(prometheus.CounterOpts{Name: "telemetry_dead_lettered_total", Help: "Undecodable records written to <topic>.dlq."}),
		FlushSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "telemetry_flush_seconds", Help: "Duration of one successful batch flush (COPY + merge + commit).",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}}),
		FlushRows: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "telemetry_flush_rows", Help: "Rows per flushed batch.",
			Buckets: []float64{1, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000}}),
		FlushErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "telemetry_flush_errors_total", Help: "Failed flush attempts (the batch is retried)."}),
		Lag: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "telemetry_ingest_lag_seconds",
			Help: "Reading timestamp to stage, non-replayed rows only. stage=durable: DB commit; stage=visible: Redis latest written.", Buckets: lagBuckets},
			[]string{"stage"}),
		Buffered:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "telemetry_buffered_rows", Help: "Rows polled from Kafka and not yet durable (queued + in flight)."}),
		Paused:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "telemetry_ingest_paused", Help: "1 while fetching is paused because the buffer is full (backpressure)."}),
		LatestErrors:    prometheus.NewCounter(prometheus.CounterOpts{Name: "telemetry_latest_write_errors_total", Help: "Failed Redis latest-power pipelines (rows stay durable)."}),
		ConsumerLag:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_consumer_lag", Help: "Records between the last committed offset and the partition high watermark."}, []string{"group", "topic", "partition"}),
		EmptyReadings:   prometheus.NewCounter(prometheus.CounterOpts{Name: "telemetry_empty_readings_total", Help: "Readings without any mapped measurand (no row written)."}),
		RebalanceDrops:  prometheus.NewCounter(prometheus.CounterOpts{Name: "telemetry_rebalance_dropped_records_total", Help: "Buffered records released to the new owner on partition loss (redelivered there)."}),
		RecordsConsumed: prometheus.NewCounter(prometheus.CounterOpts{Name: "telemetry_records_consumed_total", Help: "Kafka records polled."}),
	}
	if reg == nil {
		return m, nil
	}
	var err error
	if m.RowsIngested, err = register(reg, m.RowsIngested); err != nil {
		return nil, err
	}
	if m.DedupDropped, err = register(reg, m.DedupDropped); err != nil {
		return nil, err
	}
	if m.Unknown, err = register(reg, m.Unknown); err != nil {
		return nil, err
	}
	if m.DeadLettered, err = register(reg, m.DeadLettered); err != nil {
		return nil, err
	}
	if m.FlushSeconds, err = register(reg, m.FlushSeconds); err != nil {
		return nil, err
	}
	if m.FlushRows, err = register(reg, m.FlushRows); err != nil {
		return nil, err
	}
	if m.FlushErrors, err = register(reg, m.FlushErrors); err != nil {
		return nil, err
	}
	if m.Lag, err = register(reg, m.Lag); err != nil {
		return nil, err
	}
	if m.Buffered, err = register(reg, m.Buffered); err != nil {
		return nil, err
	}
	if m.Paused, err = register(reg, m.Paused); err != nil {
		return nil, err
	}
	if m.LatestErrors, err = register(reg, m.LatestErrors); err != nil {
		return nil, err
	}
	if m.ConsumerLag, err = register(reg, m.ConsumerLag); err != nil {
		return nil, err
	}
	if m.EmptyReadings, err = register(reg, m.EmptyReadings); err != nil {
		return nil, err
	}
	if m.RebalanceDrops, err = register(reg, m.RebalanceDrops); err != nil {
		return nil, err
	}
	if m.RecordsConsumed, err = register(reg, m.RecordsConsumed); err != nil {
		return nil, err
	}
	return m, nil
}

func register[T prometheus.Collector](reg prometheus.Registerer, col T) (T, error) {
	if err := reg.Register(col); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(T); ok {
				return existing, nil
			}
		}
		return col, fmt.Errorf("register metrics: %w", err)
	}
	return col, nil
}
