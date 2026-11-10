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

The cost is one more container (its memory footprint was not measured). The `telemetry` database holds a hypertable with 1-hour chunks. At fleet peak that is about
4.8 M rows per chunk, about 1 GB [derived], which fits in memory for current-chunk inserts.

| Concern | Decision | Sizing |
|---|---|---|
| Dedup | unique index `(station_id, connector_id, time)` | replays are idempotent |
| Compression | after 1 day, segmentby `station_id, connector_id`, orderby `time` | 90 days × 5.8 GB = 520 GB raw [ADR 0000]; ~10× compression typical for this shape gives ~50–60 GB on disk [derived; the measured compressed-chunk ratio is 10.27, see "Measured" below] |
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
  growth and drain, and the OCPP latency during the slowdown (see the amendment below).
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
  retention. Whether the OCPP path stays unaffected is NOT supported by the drill; see the
  amendment below.

## Slow-Postgres drill result (amendment, 2026-11-08)

An earlier version of this ADR said that the OCPP path is unaffected by a slow telemetry database.
The slow-Postgres run `docs/drill-output/slow-postgres-20261108T150304Z/` (measured, with a
`NOTE.md`) does not support that claim. Two later runs exist: `slow-postgres-20261108T160959Z` (FAIL, confounded by an overloaded lab VM:
OCPP CALLRESULT p99 2.48 s during the slowdown, durable lag 9.9 s after recovery) and
`slow-postgres-20261108T162708Z` (PASS, the clean run).

- **Held:** ingest stalled and the backlog stayed in Kafka (peak consumer lag 133 records). The
  durable lag p99 was 1.37 s 90 s after recovery (target < 5 s). 0 of 2,103 probe readings were
  missing. A `lag_drained_within_s` of 0 does not show a drain: that check is the time until lag
  is below 200 records (`drills/slow-postgres.sh`), and a peak of 133 is already below 200.
- **Failed:** the OCPP CALLRESULT p99 rose to the 10 s histogram cap during the slowdown (target
  0.5 s), and fell to about 0.7 s afterwards. Heartbeat, answered locally by the gateway, stayed at
  about 8 ms, so the gateway event loop was not blocked. Calls that wait for a Kafka ack and
  StopTransaction slowed together. The p99 was already above the target before the database was
  capped: 0.24-0.47 s until 15:03:44, then 0.84-0.85 s (maximum 0.849 s) from 15:03:45, before the
  database cap (15:04:06) and before the 15:04:31 leader move. That pre-fault rise is not isolated.
- **Cause (isolated afterwards from the broker and gateway logs, see the run's NOTE.md):** a Kafka preferred-leader rebalance (`auto.leader.rebalance.enable`, every 300 s) moved partition leadership at 15:04:31, 5 min after a rolling broker restart, and the gateway's in-flight produces hung for the full 30 s send timeout (50 x "kafka send timed out after 30000ms" in the gateway log, observed, not saved as a file), holding every Kafka-acked CALLRESULT. The slow database itself was not the cause. The gateway now
  fails a produce after 3 s (ADR 0012 amendment), which bounds the stall.
- **Clean rerun:** `slow-postgres-20261108T162708Z` shows CALLRESULT p99 0.076 s while the
  database was capped, durable lag 0.49 s 90 s after recovery, 0 of 140 probe readings missing.
  This is consistent with no OCPP impact from a slow telemetry database, but it does not confirm it.
  Its peak consumer lag was 0 (no backlog built at the default 30 s meter interval) and it has only
  140 probe readings. Both 2 s-meter runs that did build a backlog failed the CALLRESULT check: one
  with an isolated Kafka leader-move cause (`150304Z`), one on an overloaded VM (`160959Z`). Whether a
  large ingest backlog affects the OCPP path is therefore not demonstrated either way.

The miss stays visible, as ADR 0000 requires. An earlier run
(`slow-postgres-20261108T142904Z`) failed for other reasons (a slow Kafka, a feedback loop in
`sessions`, and a lag check taken while the backlog drained), all fixed afterwards; see its
`NOTE.md`.

## Measured (amendment, 2026-11-08)

**Ingest** (`docs/load-output/telemetry-ingestbench.json`, measured; COPY plus staging). Each step offers a fixed rate.
"Keeping up" means all produced rows durable within 5 s after the produce phase ended and durable lag p99 < 5 s.

| Offered rows/s | Sustained rows/s | Durable lag p99 | Keeping up |
|---|---|---|---|
| 1,000 | 989.72 | 0.64 s | yes |
| 2,500 | 2,471.23 | 0.94 s | yes |
| 5,000 | 4,733.51 | 1.76 s | yes |
| 10,000 | 9,936.56 | 3.03 s | yes |
| 20,000 | 18,109.07 | 4.09 s | yes |
| 40,000 | 24,597.61 | 14.73 s | no |

- At the 2,500 rows/s target the sustained rate is 2,471.23 rows/s, which is **below** the target by 1.15% [derived:
  2,471.23 / 2,500 − 1]. The figure is rows inserted divided by produce plus drain time [derived: 49,998 / (20.01 s +
  0.22 s)]; all 49,998 rows were durable and none was lost, and the durable lag p99 of 0.94 s is inside the 5 s target.
  The strict "sustained ≥ 2,500" check is missed, and the file records the run as keeping up.
- The row-by-row INSERT baseline in the same file keeps up to 500 rows/s offered and peaks at 755.93 rows/s sustained.
- The sustained maximum in the file is 24,597.61 rows/s (at 40,000 offered, not keeping up). The highest offered rate that
  kept up is 20,000 rows/s. These are one-machine figures; see the `environment` block of the file.

**Storage** (`docs/load-output/telemetry-backfill-20261108T082949Z.json`; 5,000 stations, 2 connectors each, 7 days, one
reading every 30 s):

- Compression ratio of the compressed raw chunks: 10.27 (measured, `compressedChunksRatioBeforeOverAfter`). The ratio
  over all chunks, including indexes and the `power_1m` aggregate, is 4.41 (measured).
- Raw 90-day size for the full fleet of 50,000 stations: 59.4 GB [derived; formula in the file:
  `rowsPerDayFullFleet * (bytesPerRowCompressed * 89 + bytesPerRowUncompressed * 1)`].
- Rollup (`power_1m`) size for 2 years: 85.7 GB [derived; formula in the file]. Together 145.1 GB [derived].
- Query latency was measured for five queries (300 runs each): the latest value per connector of one station, one
  session's readings on recent and on compressed chunks, and the 1-minute rollup of one site and one day on recent and on
  compressed chunks. The p99 values range from 4.29 ms to 36.59 ms. The per-query figures are in the README metrics table.
- Compression and query numbers were measured with the background policy jobs paused and run manually (the file's
  `notes`).

## Amendment (2026-11-10): late replayed readings reach the 1-minute rollup

The `power_1m` refresh policy only covers `start_offset` 2 hours back, so a station replaying up to 24 h of buffered readings
(ADR 0003) wrote raw rows that never entered the rollup. After each durable batch the pipeline reports the time range of rows
older than `LateAfter` (2 h) to a `LateRefresher`; once a minute it runs `CALL refresh_continuous_aggregate('power_1m', from, to)`
for the merged range, widened to whole minutes. On error the range is kept for the next tick. No migration and no config key.
Billing's meter profile (power series, ADR 0011) reads this rollup, so late data now also reaches it.
