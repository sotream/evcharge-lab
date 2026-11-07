# 0008. Telemetry: ingest path and time-series store

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: MeterValues 333/s average, 1,333/s peak; 28.8 M rows/day ≈ 5.8 GB/day at ~200 B/row;
  raw retention 90 days (~520 GB), 1-min rollups 2 years; durable p99 < 5 s, visible in driver API < 10 s;
  loss ≤ 0.01%; RPO ≤ 5 min; lab target ≥ 2,500 rows/s per instance (2 instances, N+1).

## Context

Every active connector sends a MeterValues message every 30 s. At peak that is about 40,000
active connectors, or 1,333 messages/s. Replays after outages of up to 24 h arrive in bursts. The
data is append-only and queried by time range, and both the latest value and the per-minute
aggregates have to be cheap.

## Decision

### Row model: one row per reading, wide

ADR 0000 sizes the data as one row per meter value, about 200 B. A row-per-measurand model would
multiply rows by 4–5 (about 130 M rows/day, around 25 GB/day). The table is therefore wide:

`meter_readings(time, station_id, connector_id, transaction_id, energy_wh, power_w, current_a, voltage_v, soc_pct, replayed)`

- Measurands missing from a reading are NULL.
- Unknown measurands are counted (`telemetry_unknown_measurand_total`) and dropped. The raw frame
  is still in `ocpp.raw-log.v1` for 14 days (ADR 0003).

### Store: TimescaleDB on a **dedicated** Postgres 18 instance (`postgres-telemetry`)

Telemetry does not share an instance with `sessions` and `billing`. The two workloads conflict on every resource that
Postgres shares inside one instance:

| Concern | Telemetry | Sessions + billing (OLTP) | Why sharing hurts |
|---|---|---|---|
| Write volume | 5.8 GB/day of rows [ADR 0000], about 2× that in WAL with indexes, so ~12 GB/day [derived] | sessions ~320 MB/day + invoices [ADR 0000] | WAL is one stream per instance. OLTP commits queue behind telemetry WAL flushes, and checkpoints are sized by the larger writer |
| Durability | `synchronous_commit=off`, RPO ≤ 5 min | `synchronous_commit=on`, RPO 0 | per-session settings coexist, but a synchronous standby for RPO 0 would have to replicate ~12 GB/day of telemetry WAL that OLTP does not need |
| IO pattern | sequential appends, hourly chunk creation, daily compression jobs that rewrite ~5.8 GB | random small reads and writes, latency-sensitive (OCPP p99 < 500 ms, invoice p99 < 5 min) | compression and retention jobs saturate IO and evict the OLTP working set from `shared_buffers` |
| Backups and retention | 90 days raw, rebuildable from Kafka for 24 h, nightly snapshot acceptable | 10 years, PITR required | one instance means one backup policy, either too expensive for telemetry or too weak for billing |
| Scaling | grows with fleet × time (~520 GB raw for 90 days) | grows with sessions (~117 GB/year) | vertical scaling decisions diverge |

The cost is one more container, about 600 MB in the lab. The `telemetry` database holds a hypertable with 1-hour chunks. At fleet peak that is about
4.8 M rows per chunk, about 1 GB [derived], which fits in memory for current-chunk inserts.

| Concern | Decision | Sizing |
|---|---|---|
| Dedup | unique index `(station_id, connector_id, time)` | replays are idempotent |
| Compression | after 1 day, segmentby `station_id, connector_id`, orderby `time` | 90 days × 5.8 GB = 520 GB raw [ADR 0000]; ~10× compression typical for this shape gives ~50–60 GB on disk [derived; ratio measured on lab data] |
| Raw retention | 90 days (`add_retention_policy`) | ADR 0000 |
| Rollups | continuous aggregate `power_1m(bucket, station_id, connector_id, avg_power_w, max_power_w, energy_wh_delta)`, refreshed every 1 min, retention 2 years | 100,000 connectors × 1,440/day, written only while active (~10% avg) gives ~14 M rows/day [derived], compressed as well |

### Ingest (Go, franz-go)

- **Batching.** Batch consumption from `telemetry.meter-values.v1` (24 partitions, about 56
  msgs/s per partition at peak). A batch is flushed at 5,000 rows or 250 ms, whichever comes
  first.
- **Flush.** `COPY` into a temporary staging table, then
  `INSERT … SELECT … ON CONFLICT DO NOTHING` into the hypertable, in one transaction.
- **Offsets.** Committed only after the transaction commits (at-least-once). Dedup makes
  redelivery harmless, so loss in normal operation is 0. The SLO allows ≤ 0.01%.
- **`synchronous_commit = off` for the ingest connection only.** RPO ≤ 5 min (ADR 0000) allows it.
  It removes the WAL-flush wait from each batch. The bounded loss window is about 3 ×
  `wal_writer_delay` = 600 ms of commits, and only on a Postgres crash. On a Postgres crash the
  offsets for those rows may already be committed, so this is the one path that can lose data. It
  is an abnormal event, outside the "normal operation" loss SLO. Sessions and billing keep
  `synchronous_commit = on` (RPO 0).
- **Backpressure.** One in-flight flush per consumer and a bounded buffer of 50,000 rows. When
  Postgres slows down, the consumer stops polling instead of growing memory, and Kafka holds the
  backlog for 24 h (a full day of peak is about 115 M rows). The slow-Postgres drill measures lag
  growth and drain.
- **Latency budget for "durable p99 < 5 s".** Gateway produce, then at most 250 ms of batching,
  then a flush (target p99 < 500 ms at 5,000 rows). The
  `telemetry_ingest_lag_seconds{stage="durable"}` histogram tracks it, from the reading timestamp
  for non-replayed rows to commit.
- **Latest value for "visible < 10 s".** After each flush, a pipelined Redis write runs:
  `HSET power:{station}:{connector} w <W> ts <time>` with a 5 min TTL, from the newest
  non-replayed reading. `driver-api` and `sessions` read it, so a value is visible about 1 s after
  arrival.
- **Capacity.** The fleet peak is 1,333 rows/s. The lab target per instance is at least 2,500
  rows/s, and 2 instances give N+1. Partitions (24) allow scaling to 24 consumers.

## Rejected alternatives

- **A shared instance with sessions/billing** (the first draft). It is simpler, but every row in the table above is a coupling
  that ADR 0000's RPO 0 and latency SLOs for billing cannot afford.
- **A separate time-series database** (InfluxDB, VictoriaMetrics, ClickHouse). At 1,333 rows/s
  and about 60 GB compressed for 90 days, one Postgres node with Timescale is comfortably in
  range. A separate store would mean another stateful service on an 8 GB VM, another query
  language and another backup path. The trigger to revisit: sustained ingest above roughly 50k
  rows/s, or analytics across years of raw data (ClickHouse).
- **One row per measurand.** It is about 4.5× the rows and bytes of the ADR 0000 budget, and
  queries pivot it back anyway.
- **Plain Postgres with BRIN.** Chunking, compression, retention and continuous aggregates would
  all have to be built by hand.
- **Row-by-row INSERT.** It is the measured baseline in `load/`, kept to show why COPY plus
  staging is needed.
- **Writing from the gateway directly to the database.** This couples WebSocket latency to
  database latency and loses the buffer that Kafka gives during database slowness.

## Consequences

- Metrics: `telemetry_rows_ingested_total`, `telemetry_flush_seconds`,
  `telemetry_ingest_lag_seconds`, `telemetry_dedup_dropped_total`.
- During a database slowdown, lag grows and then drains. No data is lost within the 24 h Kafka
  retention.
