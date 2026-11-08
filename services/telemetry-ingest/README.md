# telemetry-ingest

Go service. Consumes `telemetry.meter-values.v1` (franz-go, group `telemetry-ingest.meter-values`),
maps each reading to one wide `meter_readings` row, writes batches into TimescaleDB, publishes the
latest power per connector to Redis and serves power queries. Governing ADRs: 0008 (storage and
ingest), 0002, 0010, 0012; load targets in 0000.

- **Flush:** 5,000 rows or 250 ms. `COPY` into a temp staging table, then
  `INSERT ... SELECT ... ON CONFLICT DO NOTHING` in one transaction, on a connection with
  `synchronous_commit=off`. Offsets are committed only after the transaction (at-least-once; the
  unique index `(station_id, connector_id, time)` makes redelivery harmless).
- **Backpressure:** at most 50,000 buffered rows; at the bound the topic is paused and Kafka holds the backlog.
  A failed flush is retried with backoff, never dropped. Undecodable records go to `<topic>.dlq`.
- **Mapping:** Wh from `Energy.Active.Import.Register` (kWh converted), W/kW, A, V, SoC. Unknown
  measurands are counted (`telemetry_unknown_measurand_total`) and dropped.
- **Latest value:** after each flush a pipelined `HSET power:{station}:{connector} w ts` (TTL 5 min)
  from the newest non-replayed reading.
- **Schema:** `migrations/0001_meter_readings.sql`, `0002_power_1m.sql` (canonical, shared with
  `scripts/backfill-telemetry`), applied at startup with `pg.Migrate`.

API (needs `x-internal-token`): `GET /v1/stations/:id/connectors/:cid/power?from&to&step`
(`PowerSeriesRes` from `power_1m`), `GET /v1/stations/:id/connectors/:cid/latest`.

Metrics: `telemetry_rows_ingested_total`, `telemetry_flush_seconds`,
`telemetry_ingest_lag_seconds{stage}` (`durable`, `visible`), `telemetry_dedup_dropped_total`,
`telemetry_unknown_measurand_total`, `telemetry_buffered_rows`, consumer lag.

Config: `DATABASE_URL`, `REDIS_URL`, `KAFKA_BROKERS` (required); `INGEST_TOPIC`, `INGEST_GROUP`,
`INGEST_FLUSH_ROWS`, `INGEST_FLUSH_INTERVAL`, `INGEST_MAX_BUFFERED`, `INGEST_DB_CONNS`, `QUERY_DB_CONNS`.

## Test and measure

```sh
cd services/telemetry-ingest
go test -race ./...                          # integration tests need INTEGRATION=1 and the compose infra
make -o infra-up go-check                    # from the repo root
go run ./cmd/ingestbench -out ../../docs/load-output/telemetry-ingestbench.json
```

`ingestbench` runs one pipeline instance in-process against a dedicated database
(`telemetry_ingestbench`, dropped afterwards) and temporary RF-3 topics, at increasing rates, for
the COPY writer and for the row-by-row INSERT baseline that ADR 0008 rejects. The result file holds
sustained rows/s and durable lag percentiles per step.
