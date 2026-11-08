# evcharge-lab

evcharge-lab is the charge point operator (CPO) side of an EV charging platform, built as a system-design lab. Seven services (NestJS and Go) and an OCPP 1.6J station simulator answer concrete questions: how to hold thousands of long-lived WebSockets across several instances, how to take reservations under contention without double-booking, how to split a hard site power cap fairly and safely, how to get an exactly-once billing effect out of at-least-once delivery, and how to absorb a bursty telemetry stream. Every decision is written down in an [ADR](docs/adr/README.md) against explicit load targets ([ADR 0000](docs/adr/0000-requirements-and-capacity.md)). The numbers in the generated blocks of this README (the metrics table and the charts) are read from raw files in `docs/load-output/` and `docs/drill-output/` by a script, labelled as measured or derived, and the targets that were missed stay visible as missed. The prose in "What the drills and reviews caught" and "Known limits" also quotes numbers; those were observed during the build, and each one is labelled as measured (with its file), derived (with its formula), observed but not saved as a file, or a review scenario.

![architecture](docs/img/architecture.svg)

Solid blue lines are synchronous HTTP requests, dashed purple lines are asynchronous Kafka event flows, thick teal lines are the station WebSockets, and dotted grey lines are data access. The decisions behind it are in [ADR 0001](docs/adr/0001-architecture-overview.md); the ports are in the same ADR and in [`docker-compose.yml`](docker-compose.yml).

## 5-minute quick start

Prerequisites: Docker with a VM of about 8 GB (the measurements below ran on 8 CPUs and 8.3 GB), plus Node 26 and pnpm for `make check`. Go and `golangci-lint` are also required for `make check`, because it runs `go vet`, `golangci-lint run` and `go test -race` on the Go modules.

```sh
docker compose up -d --build      # infra, services, HAProxy, observability and the simulator
docker compose ps                 # wait until every service is healthy
curl -s 127.0.0.1:28089/status    # simulator: connected stations, active sessions, queue depth
curl -s localhost:28081/health/ready   # any service: ports are listed below
```

By default the simulator connects 20 sites with 10 stations each (200 stations) and 50 simulated drivers, so sessions, meter values and allocation rounds start flowing immediately.

| What | URL | Notes |
|---|---|---|
| Driver API | http://localhost:28082 | the only public entry point; `POST /v1/auth/register`, then the routes in [its README](services/driver-api/README.md) |
| Station edge (OCPP) | ws://localhost:28080/ocpp/{stationId} | HAProxy `leastconn` in front of two gateway instances |
| HAProxy stats | http://localhost:28099 | backend health of the two gateways |
| Simulator control | http://127.0.0.1:28089/status | bound to localhost, unauthenticated |
| Grafana | http://localhost:23000 | anonymous viewer; Prometheus and Jaeger datasources and six dashboards in the `evcharge` folder are provisioned from [`infra/grafana/dashboards/`](infra/grafana/dashboards/): `overview` (the ADR 0000 SLOs, open it first), `gateway`, `sessions-allocation`, `billing`, `telemetry` and `kafka-and-infra` |
| Prometheus | http://localhost:29090 | queries used by the alerts in the [runbook](docs/runbook.md) |
| Jaeger | http://localhost:26686 | traces, 10% parent-based sampling |
| Services | `localhost:28081` sessions, `28083` billing, `28084` smart-charging, `28085` tariff, `28086` telemetry-ingest, `28087` and `28088` gateways | each serves `/health/live`, `/health/ready` and `/metrics` |

Run the full local check (lint, types, build, unit and integration tests, Go vet, lint and race tests):

```sh
make check
```

`make check` (through `make infra-up`) stops the application containers first, because the integration tests share Redis and Kafka with the stack; the infrastructure keeps running. Run `docker compose up -d` afterwards to bring the application containers back. `make down` stops everything and deletes the data. Operating notes and failure recipes are in the [runbook](docs/runbook.md).

## The system-design question each service answers

| Service | Language | System-design question | ADR |
|---|---|---|---|
| [`station-gateway`](services/station-gateway/README.md) | NestJS | How do you hold thousands of long-lived WebSockets across N instances and route a command to the one instance that owns a station? How do you survive reconnect storms and offline replay? | [0003](docs/adr/0003-ocpp-subset-station-auth-and-offline-replay.md), [0004](docs/adr/0004-websocket-connection-routing.md), [0012](docs/adr/0012-kafka-replication-and-producer-backpressure.md) |
| [`sessions`](services/sessions/README.md) | NestJS | How do you take reservations under contention without double-booking a connector? How do you run session state machines driven by unreliable station messages? | [0005](docs/adr/0005-reservation-contention.md) |
| [`smart-charging`](services/smart-charging/README.md) | Go | How do you split a hard site cap across sessions fairly, fast and safely, and what happens when the allocator is down? | [0006](docs/adr/0006-smart-charging-allocation.md) |
| [`tariff`](services/tariff/README.md) | Go | How do you price a session deterministically (integer money, time-of-use boundaries, rounding)? | [0011](docs/adr/0011-tariff-model.md) |
| [`billing`](services/billing/README.md) | NestJS | How do you get an exactly-once *effect* (one invoice, one capture) out of at-least-once delivery, and how do you compensate across services? | [0007](docs/adr/0007-billing-saga-and-exactly-once-effect.md) |
| [`telemetry-ingest`](services/telemetry-ingest/README.md) | Go | How do you absorb a high, bursty write rate of meter values with dedup and bounded memory? | [0008](docs/adr/0008-telemetry-storage.md), [0012](docs/adr/0012-kafka-replication-and-producer-backpressure.md) |
| [`driver-api`](services/driver-api/README.md) | NestJS | How do you authenticate drivers and compose calls to downstream services with timeouts, retries and idempotency? | [0010](docs/adr/0010-authentication.md) |
| [`simulator`](simulator/README.md) | Node | How do you produce realistic load from thousands of stations, including floods, offline periods and replays? | [0003](docs/adr/0003-ocpp-subset-station-auth-and-offline-replay.md), [0013](docs/adr/0013-traffic-generators-simulator-and-backfill.md) |

Cross-cutting decisions: [0000](docs/adr/0000-requirements-and-capacity.md) requirements, load model and SLOs; [0001](docs/adr/0001-architecture-overview.md) architecture; [0002](docs/adr/0002-contracts-events-and-delivery-semantics.md) contracts, event envelope and at-least-once plus idempotency; [0009](docs/adr/0009-toolchain-and-infrastructure-choices.md) toolchain and versions; [0012](docs/adr/0012-kafka-replication-and-producer-backpressure.md) Kafka replication and backpressure.

## Results

The lab measures **one instance per service** on one laptop, with all containers sharing one Docker VM, and derives the fleet size from it. [ADR 0000](docs/adr/0000-requirements-and-capacity.md) names a MacBook Air M1 as the intended machine; the raw files record only `darwin/arm64`, a Docker VM with 8 CPUs and 8.3 GB of memory, and (in `docs/load-output/*.env.json`) the containers running during each run. No raw file records the laptop model. The targets come from [`load/targets.json`](load/targets.json), which is transcribed from ADR 0000. The chart shows each measured value divided by its target.

![targets vs measured](docs/img/targets-vs-measured.svg)

How to read it: every verdict compares the measured value with the target, with no tolerance. The **station gateway misses its message-rate and CALLRESULT p99 targets** in the 2,000-station run, which is the only run that offers enough load to test the rate (see [Known limits](#known-limits-and-production-gaps)); the 1,000-station run cannot test a 2,000 msg/s target and is labelled "not tested", but it also missed the CALLRESULT p99 target (1,446 ms against 500 ms, `ocpp_callresult_ms` p(99) in `k6-gateway-ws-1000st-1hz.json`). The gateway does hold 10,000 WebSocket connections. The sessions rate (199.99 against 200) and the driver-api read rate (955.59 against 1,000) are below their targets and are drawn as misses; the notes under the metrics table explain each (a 60.003 s run for sessions, and setup requests plus dropped iterations for driver-api). The telemetry-ingest sustained rate at the target offered rate (2,471.23 against 2,500) is also a miss. The smart-charging (999.97 against 1,000) and tariff (1,999.97 against 2,000) k6 rates are drawn as misses too; they are duration artefacts, explained in the notes under the metrics table. The closed-loop saturation figures in the table (for example 10,507.2 allocations/s and 14,399.8 quotes/s) are best-case benchmarks, not the charted verdicts.

### Metrics table

Labels: **measured** is a value read from a raw file; **derived** is calculated from raw values with the formula shown; **measured (PromQL increase, extrapolated)** is a drill count from a Prometheus `increase()`, which extrapolates to the window edges and is therefore a float. Rows marked "not tested" had an offered load below the target. Values are rounded to at most two decimals; nothing else is changed. `n/a` in the target column means the raw file records no target for that figure. This table is generated by `node scripts/charts/build.mjs`.

<!-- metrics:start (generated by scripts/charts/build.mjs) -->
| Metric | Target | Value | Label | Source file | Formula |
|---|---|---|---|---|---|
| Gateway: WebSocket connections on one instance | ≥ 10,000 | 10,000 (met) | measured | [`gateway-10k-connections.json`](docs/load-output/gateway-10k-connections.json) |  |
| Gateway: peak RSS at that load | < 1.5 GB | 525 MB | measured | [`gateway-10k-connections.json`](docs/load-output/gateway-10k-connections.json) |  |
| Gateway: inbound OCPP frames/s, 10,000 simulated stations, stub sessions (not the pass/fail run) | n/a | peak 6,918; last-30 s average 5,521 | measured | [`gateway-10k-connections.json`](docs/load-output/gateway-10k-connections.json) |  |
| Gateway: msg/s, 1000 stations at 1 msg/s (k6) | ≥ 2,000 | 910.28 (not tested: offered load is at most 1,000 msg/s) | measured; offered load derived | [`k6-gateway-ws-1000st-1hz.json`](docs/load-output/k6-gateway-ws-1000st-1hz.json), [`k6-gateway-ws-1000st-1hz.env.json`](docs/load-output/k6-gateway-ws-1000st-1hz.env.json) | `offered = stations × Hz (one outstanding CALL per station)` |
| Gateway: CALLRESULT p99, 1000 stations, at 910.28 msg/s achieved (k6) | ≤ 500 ms | 1,446 ms (**MISSED**); p50 9 ms; CALLERROR 0 | measured | [`k6-gateway-ws-1000st-1hz.json`](docs/load-output/k6-gateway-ws-1000st-1hz.json), [`k6-gateway-ws-1000st-1hz.env.json`](docs/load-output/k6-gateway-ws-1000st-1hz.env.json) |  |
| Gateway: msg/s, 2000 stations at 1 msg/s (k6) | ≥ 2,000 | 1,139.03 (**MISSED**) | measured | [`k6-gateway-ws-2000st-1hz.json`](docs/load-output/k6-gateway-ws-2000st-1hz.json), [`k6-gateway-ws-2000st-1hz.env.json`](docs/load-output/k6-gateway-ws-2000st-1hz.env.json) |  |
| Gateway: CALLRESULT p99, 2000 stations, at 1,139.03 msg/s achieved (k6) | ≤ 500 ms | 3,228 ms (**MISSED**); p50 137 ms; CALLERROR 0 | measured | [`k6-gateway-ws-2000st-1hz.json`](docs/load-output/k6-gateway-ws-2000st-1hz.json), [`k6-gateway-ws-2000st-1hz.env.json`](docs/load-output/k6-gateway-ws-2000st-1hz.env.json) |  |
| Telemetry ingest: sustained rows/s at 2,500 rows/s offered (COPY) | ≥ 2,500 | 2,471.23 (**MISSED**; keeping up: true, defined as all produced rows durable within 5 s after the produce phase ended and durable lag p99 < 5 s) | measured | [`telemetry-ingestbench.json`](docs/load-output/telemetry-ingestbench.json) |  |
| Telemetry ingest: durable lag p99 at that rate | ≤ 5,000 ms | 942.95 ms (met) | derived | [`telemetry-ingestbench.json`](docs/load-output/telemetry-ingestbench.json) | `measured_durable_lag_p99_s × 1000` |
| Telemetry ingest: highest offered rate that kept up (COPY) | ≥ 2,500 | 20,000 offered, 18,109.07 sustained (met), lag p99 4.09 s | measured | [`telemetry-ingestbench.json`](docs/load-output/telemetry-ingestbench.json) |  |
| Telemetry ingest: peak sustained rows/s (COPY, did not keep up) | n/a | 24,597.61 at 40,000 offered; lag p99 14.73 s | measured | [`telemetry-ingestbench.json`](docs/load-output/telemetry-ingestbench.json) |  |
| Telemetry ingest: row-by-row INSERT baseline | n/a | keeps up to 500 offered; peak sustained 755.93 | measured | [`telemetry-ingestbench.json`](docs/load-output/telemetry-ingestbench.json) |  |
| Telemetry ingest: loss ratio at the target rate | ≤ 0.0001 | 0 (met; 49998 produced, 49998 inserted) | derived | [`telemetry-ingestbench.json`](docs/load-output/telemetry-ingestbench.json) | `(rows_produced - rows_inserted) / rows_produced` |
| Backfill: load rate (5,000 stations, 7 days) | n/a | 106,504 rows/s (20,216,542 rows in 189.82 s) | measured | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) |  |
| Storage: compression ratio, compressed raw chunks / all chunks | n/a | 10.27 x / 4.41 x | measured | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) |  |
| Storage: compression ratio, power_1m compressed chunks | n/a | 21.19 x | measured | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) |  |
| Storage query: latestPerConnectorForStation | n/a | p50 5.28 ms, p95 23.36 ms, p99 36.59 ms (300 runs) | measured | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) |  |
| Storage query: power1mOneSiteOneDayCompressed | n/a | p50 5 ms, p95 9.82 ms, p99 12.96 ms (300 runs) | measured | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) |  |
| Storage query: power1mOneSiteOneDayRecent | n/a | p50 5.24 ms, p95 12.47 ms, p99 19.18 ms (300 runs) | measured | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) |  |
| Storage query: sessionReadingsCompressedChunks | n/a | p50 7.7 ms, p95 13.58 ms, p99 28.6 ms (300 runs) | measured | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) |  |
| Storage query: sessionReadingsRecentChunks | n/a | p50 2.02 ms, p95 3.45 ms, p99 4.29 ms (300 runs) | measured | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) |  |
| Full fleet (50,000 stations): raw90dSizeGB | n/a | 59.4 GB | derived | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) | `rowsPerDayFullFleet * (bytesPerRowCompressed * 89 + bytesPerRowUncompressed * 1)` |
| Full fleet (50,000 stations): rollup2ySizeGB | n/a | 85.7 GB | derived | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) | `power1mRows / days * (50000 / stations) * (7 * rollupBytesPerRowUncompressed + 723 * rollupBytesPerRowCompressed)` |
| Full fleet (50,000 stations): totalSizeGB | n/a | 145.1 GB | derived | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) | `raw90dSizeGB + rollup2ySizeGB` |
| Full fleet: average ingest | n/a | 334 rows/s | derived | [`telemetry-backfill-20261108T082949Z.json`](docs/load-output/telemetry-backfill-20261108T082949Z.json) | `rowsPerDayFullFleet / 86400` |
| Sessions: commands/s at 50 offered (k6) | n/a | 50.03; p99 start 314.44 ms, stop 130.48 ms (script threshold < 500 ms: start met, stop met); dropped 0 | measured | [`k6-sessions-ocpp-rate50.json`](docs/load-output/k6-sessions-ocpp-rate50.json), [`k6-sessions-ocpp-rate50.env.json`](docs/load-output/k6-sessions-ocpp-rate50.env.json) |  |
| Sessions: commands/s at 100 offered (k6) | n/a | 99.77; p99 start 94.75 ms, stop 57.23 ms (script threshold < 500 ms: start met, stop met); dropped 0 | measured | [`k6-sessions-ocpp-rate100.json`](docs/load-output/k6-sessions-ocpp-rate100.json), [`k6-sessions-ocpp-rate100.env.json`](docs/load-output/k6-sessions-ocpp-rate100.env.json) |  |
| Sessions: commands/s at 150 offered (k6) | n/a | 149.98; p99 start 621.77 ms, stop 482.01 ms (script threshold < 500 ms: start **MISSED**, stop met); dropped 0 | measured | [`k6-sessions-ocpp-rate150.json`](docs/load-output/k6-sessions-ocpp-rate150.json), [`k6-sessions-ocpp-rate150.env.json`](docs/load-output/k6-sessions-ocpp-rate150.env.json) |  |
| Sessions: commands/s at 200 offered (k6) | ≥ 200 | 199.99 (**MISSED**); p99 start 177.39 ms, stop 124.54 ms (script threshold < 500 ms: start met, stop met); dropped 0 | measured | [`k6-sessions-ocpp-rate200.json`](docs/load-output/k6-sessions-ocpp-rate200.json), [`k6-sessions-ocpp-rate200.env.json`](docs/load-output/k6-sessions-ocpp-rate200.env.json) |  |
| Sessions: one allocation round for 5,000 sites, 15,000 active sessions (2 instances, concurrency 8 each) | ≤ 60 s | 7.32 s (met; 683.53 rounds/s, 15,000 SetChargingProfile commands). Setup recorded in the file: stub allocator on localhost, Postgres/Redis on the compose infra, workers off (no outbox relay); wall clock from start() to every site having completed a round | measured | [`sessions-allocation-rounds.json`](docs/load-output/sessions-allocation-rounds.json) |  |
| Reservations: concurrent attempts on one connector, winners / conflicts | exactly 1 winner | 1 / 99 | measured | [`k6-reservation-contention.json`](docs/load-output/k6-reservation-contention.json), [`k6-reservation-contention.env.json`](docs/load-output/k6-reservation-contention.env.json) |  |
| Reservations: double bookings | 0 | 0 | derived | [`k6-reservation-contention.json`](docs/load-output/k6-reservation-contention.json) | `reservation_won - 1` |
| Smart charging: saturation-c16 (closed-loop) | ≥ 1,000 /s, p99 ≤ 20 ms | 10,507.2 /s, p99 9.77 ms (met), 0 cap violations | measured | [`smart-charging-allocbench.json`](docs/load-output/smart-charging-allocbench.json) |  |
| Smart charging: fixed-rate-1000 (open-loop) | ≥ 1,000 /s, p99 ≤ 20 ms | 1,000.02 /s, p99 19.45 ms (met), 0 cap violations | measured | [`smart-charging-allocbench.json`](docs/load-output/smart-charging-allocbench.json) |  |
| Smart charging: server-side invariant violations | 0 | 0 over 408642 allocations | measured | [`smart-charging-allocbench.json`](docs/load-output/smart-charging-allocbench.json) |  |
| Smart charging: k6 against the service | ≥ 1,000 /s, p99 ≤ 20 ms | 999.97 /s (**MISSED**), p99 11.72 ms (met) | measured | [`k6-smart-charging.json`](docs/load-output/k6-smart-charging.json), [`k6-smart-charging.env.json`](docs/load-output/k6-smart-charging.env.json) |  |
| Tariff: closed loop, concurrency 8 | ≥ 2,000 /s, p99 ≤ 20 ms | 9,095.7 /s, p99 4.01 ms (met) | measured | [`tariff-quotebench-c8.json`](docs/load-output/tariff-quotebench-c8.json) |  |
| Tariff: open loop at 2,000 quotes/s | ≥ 2,000 /s, p99 ≤ 20 ms | 2,000 /s, p99 13.9 ms (met) | measured | [`tariff-quotebench-openloop.json`](docs/load-output/tariff-quotebench-openloop.json) |  |
| Tariff: closed loop, concurrency 32 | ≥ 2,000 /s, p99 ≤ 20 ms | 14,399.8 /s, p99 7.96 ms (met) | measured | [`tariff-quotebench.json`](docs/load-output/tariff-quotebench.json) |  |
| Tariff: k6 against the service | ≥ 2,000 /s, p99 ≤ 20 ms | 1,999.97 /s (**MISSED**), p99 5.14 ms (met) | measured | [`k6-tariff.json`](docs/load-output/k6-tariff.json), [`k6-tariff.env.json`](docs/load-output/k6-tariff.env.json) |  |
| Driver API: read requests/s (GET /v1/sites, k6) | ≥ 1,000 | 955.59 (**MISSED**); 331 of 60000 iterations dropped | measured | [`k6-driver-api-reads.json`](docs/load-output/k6-driver-api-reads.json), [`k6-driver-api-reads.env.json`](docs/load-output/k6-driver-api-reads.env.json) |  |
| Driver API: read p99 | ≤ 300 ms | 255.2 ms (met) | measured | [`k6-driver-api-reads.json`](docs/load-output/k6-driver-api-reads.json), [`k6-driver-api-reads.env.json`](docs/load-output/k6-driver-api-reads.env.json) |  |
| Simulator: 10,000 stations at time scale 1 | n/a | all connected after 5 s; peak 8,141 msg/s at the stub; peak RSS 253 MB; event-loop lag max 37.49 ms | measured | [`simulator-scale-10k-scale1.jsonl`](docs/load-output/simulator-scale-10k-scale1.jsonl) |  |
| Simulator: 10,000 stations at time scale 60 | n/a | all connected after 6 s; peak 14,956 msg/s at the stub; peak RSS 529 MB; event-loop lag max 134.35 ms | measured | [`simulator-scale-10k-scale60.jsonl`](docs/load-output/simulator-scale-10k-scale60.jsonl) |  |
| Drill gateway-instance-loss (20261108T115832Z): stations_reconnected_within_s | ≤ 60 | no measurement (**FAIL**) | measured | [`result.json`](docs/drill-output/gateway-instance-loss-20261108T115832Z/result.json) |  |
| Drill gateway-instance-loss (20261108T115832Z): routes_present_after_loss | ≥ 200 | 199 (**FAIL**) | measured | [`result.json`](docs/drill-output/gateway-instance-loss-20261108T115832Z/result.json) |  |
| Drill gateway-instance-loss (20261108T115832Z): fleet_connected_after_recovery | ≥ 200 | 199 (**FAIL**) | measured | [`result.json`](docs/drill-output/gateway-instance-loss-20261108T115832Z/result.json) |  |
| Drill gateway-instance-loss (20261108T122353Z): stations_reconnected_within_s | ≤ 60 | 31 (pass) | measured | [`result.json`](docs/drill-output/gateway-instance-loss-20261108T122353Z/result.json) |  |
| Drill gateway-instance-loss (20261108T122353Z): routes_present_after_loss | ≥ 200 | 200 (pass) | measured | [`result.json`](docs/drill-output/gateway-instance-loss-20261108T122353Z/result.json) |  |
| Drill gateway-instance-loss (20261108T122353Z): fleet_connected_after_recovery | ≥ 200 | 200 (pass) | measured | [`result.json`](docs/drill-output/gateway-instance-loss-20261108T122353Z/result.json) |  |
| Drill kafka-broker-loss-1 (20261108T122708Z): gateway_ready_during_outage | = 200 | 200 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-1-20261108T122708Z/result.json) |  |
| Drill kafka-broker-loss-1 (20261108T122708Z): producer_recovery_s | ≤ 60 | 0 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-1-20261108T122708Z/result.json) |  |
| Drill kafka-broker-loss-1 (20261108T122708Z): probe_readings_generated | ≥ 1 | 100 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-1-20261108T122708Z/result.json) |  |
| Drill kafka-broker-loss-1 (20261108T122708Z): probe_readings_missing | = 0 | 30 (**FAIL**) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-1-20261108T122708Z/result.json) |  |
| Drill kafka-broker-loss-1 (20261108T140530Z): gateway_ready_during_outage | = 200 | 200 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-1-20261108T140530Z/result.json) |  |
| Drill kafka-broker-loss-1 (20261108T140530Z): producer_recovery_s | ≤ 60 | 1 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-1-20261108T140530Z/result.json) |  |
| Drill kafka-broker-loss-1 (20261108T140530Z): probe_readings_generated | ≥ 1 | 96 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-1-20261108T140530Z/result.json) |  |
| Drill kafka-broker-loss-1 (20261108T140530Z): probe_readings_missing | = 0 | 0 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-1-20261108T140530Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T123144Z): gateway_ready_during_outage | = 200 | 0 (**FAIL**) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T123144Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T123144Z): producer_recovery_s | ≤ 60 | 0 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T123144Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T123144Z): max_produce_errors_per_s_during_outage | ≥ 0.01 | 34.79 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T123144Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T123144Z): max_produce_buffer_depth | ≥ 1 | 549 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T123144Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T123144Z): probe_readings_generated | ≥ 1 | 280 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T123144Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T123144Z): probe_readings_missing | = 0 | 126 (**FAIL**) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T123144Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T140952Z): gateway_ready_during_outage | = 200 | 200 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T140952Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T140952Z): producer_recovery_s | ≤ 60 | 0 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T140952Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T140952Z): max_produce_errors_per_s_during_outage | ≥ 0.01 | 7.2 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T140952Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T140952Z): max_produce_buffer_depth | ≥ 1 | 187 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T140952Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T140952Z): probe_readings_generated | ≥ 1 | 254 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T140952Z/result.json) |  |
| Drill kafka-broker-loss-2 (20261108T140952Z): probe_readings_missing | = 0 | 0 (pass) | measured | [`result.json`](docs/drill-output/kafka-broker-loss-2-20261108T140952Z/result.json) |  |
| Drill reconnect-storm (20261108T120311Z): fleet_reconnected_within_s | ≤ 60 | 11 (pass) | measured | [`result.json`](docs/drill-output/reconnect-storm-20261108T120311Z/result.json) |  |
| Drill reconnect-storm (20261108T120311Z): gateways_ready_after_storm | = 400 | 400 (pass) | measured | [`result.json`](docs/drill-output/reconnect-storm-20261108T120311Z/result.json) |  |
| Drill slow-postgres (20261108T142904Z): ocpp_callresult_p99_s_during | ≤ 0.5 | 2.24 (**FAIL**) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T142904Z/result.json) |  |
| Drill slow-postgres (20261108T142904Z): lag_drained_within_s | ≤ 300 | 7 (pass) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T142904Z/result.json) |  |
| Drill slow-postgres (20261108T142904Z): ingest_lag_p99_s_after_recovery | ≤ 5 | 118.52 (**FAIL**) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T142904Z/result.json) |  |
| Drill slow-postgres (20261108T142904Z): probe_readings_missing | = 0 | 0 (pass) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T142904Z/result.json) |  |
| Drill slow-postgres (20261108T150304Z): ocpp_callresult_p99_s_during | ≤ 0.5 | 10 (**FAIL**) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T150304Z/result.json) |  |
| Drill slow-postgres (20261108T150304Z): lag_drained_within_s | ≤ 300 | 0 (pass) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T150304Z/result.json) |  |
| Drill slow-postgres (20261108T150304Z): ingest_lag_p99_s_90s_after_recovery | ≤ 5 | 1.37 (pass) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T150304Z/result.json) |  |
| Drill slow-postgres (20261108T150304Z): probe_readings_missing | = 0 | 0 (pass) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T150304Z/result.json) |  |
| Drill slow-postgres (20261108T160959Z): ocpp_callresult_p99_s_during | ≤ 0.5 | 2.48 (**FAIL**) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T160959Z/result.json) |  |
| Drill slow-postgres (20261108T160959Z): lag_drained_within_s | ≤ 300 | 9 (pass) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T160959Z/result.json) |  |
| Drill slow-postgres (20261108T160959Z): ingest_lag_p99_s_90s_after_recovery | ≤ 5 | 9.9 (**FAIL**) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T160959Z/result.json) |  |
| Drill slow-postgres (20261108T160959Z): probe_readings_missing | = 0 | 0 (pass) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T160959Z/result.json) |  |
| Drill slow-postgres (20261108T162708Z): ocpp_callresult_p99_s_during | ≤ 0.5 | 0.08 (pass) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T162708Z/result.json) |  |
| Drill slow-postgres (20261108T162708Z): lag_drained_within_s | ≤ 300 | 0 (pass) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T162708Z/result.json) |  |
| Drill slow-postgres (20261108T162708Z): ingest_lag_p99_s_90s_after_recovery | ≤ 5 | 0.49 (pass) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T162708Z/result.json) |  |
| Drill slow-postgres (20261108T162708Z): probe_readings_missing | = 0 | 0 (pass) | measured | [`result.json`](docs/drill-output/slow-postgres-20261108T162708Z/result.json) |  |
| Drill smart-charging-down (20261108T114310Z): fallback_rounds_seen | ≥ 1 | 188.87 (pass) | measured (PromQL increase, extrapolated) | [`result.json`](docs/drill-output/smart-charging-down-20261108T114310Z/result.json) |  |
| Drill smart-charging-down (20261108T114310Z): sessions_still_charging | ≥ 1 | 16 (pass) | measured | [`result.json`](docs/drill-output/smart-charging-down-20261108T114310Z/result.json) |  |
| Drill smart-charging-down (20261108T114310Z): optimal_rounds_resumed | ≥ 1 | 80.73 (pass) | measured (PromQL increase, extrapolated) | [`result.json`](docs/drill-output/smart-charging-down-20261108T114310Z/result.json) |  |
| Drill smart-charging-down (20261108T114310Z): max_site_utilisation_over_drill | ≤ 1 | 0.68 (pass) | measured | [`result.json`](docs/drill-output/smart-charging-down-20261108T114310Z/result.json) |  |
| Drill station-offline-replay (20261108T141434Z): stations_taken_offline | ≥ 20 | 20 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T141434Z/result.json) |  |
| Drill station-offline-replay (20261108T141434Z): connected_dropped_during_offline | ≥ 20 | 20 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T141434Z/result.json) |  |
| Drill station-offline-replay (20261108T141434Z): replay_drained_within_s | ≤ 600 | 31 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T141434Z/result.json) |  |
| Drill station-offline-replay (20261108T141434Z): queue_dropped_total | = 0 | 0 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T141434Z/result.json) |  |
| Drill station-offline-replay (20261108T141434Z): probe_readings_generated | ≥ 1 | 1,272 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T141434Z/result.json) |  |
| Drill station-offline-replay (20261108T141434Z): probe_readings_missing | = 0 | 0 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T141434Z/result.json) |  |
| Drill station-offline-replay (20261108T141434Z): duplicate_rows | = 0 | 0 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T141434Z/result.json) |  |
| Drill station-offline-replay (20261108T141434Z): replayed_rows_flagged | ≥ 1 | 960 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T141434Z/result.json) |  |
| Drill station-offline-replay (20261108T151006Z): stations_taken_offline | ≥ 20 | 20 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T151006Z/result.json) |  |
| Drill station-offline-replay (20261108T151006Z): connected_dropped_during_offline | ≥ 20 | 20 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T151006Z/result.json) |  |
| Drill station-offline-replay (20261108T151006Z): replay_drained_within_s | ≤ 600 | no measurement (**FAIL**) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T151006Z/result.json) |  |
| Drill station-offline-replay (20261108T151006Z): queue_dropped_total | = 0 | 0 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T151006Z/result.json) |  |
| Drill station-offline-replay (20261108T151006Z): probe_readings_generated | ≥ 1 | 164,203 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T151006Z/result.json) |  |
| Drill station-offline-replay (20261108T151006Z): probe_readings_missing | = 0 | 82,914 (**FAIL**) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T151006Z/result.json) |  |
| Drill station-offline-replay (20261108T151006Z): duplicate_rows | = 0 | 0 (pass) | measured | [`result.json`](docs/drill-output/station-offline-replay-20261108T151006Z/result.json) |  |

Notes on individual rows:

- **Sessions: commands/s at 200 offered (k6):** 12,000 requests in 60.003 s with 0 dropped iterations: every offered request was sent. The rate is below 200 only because k6 divides by the test run duration (60.003 s); it is a rounding artefact, not a throughput shortfall. Derived: http_reqs.count / testRunDurationMs × 1000.
- **Smart charging: k6 against the service:** 60,000 requests in 60.0018 s. The target is 1,000 /s and the reported rate is 999.97 /s only because the run lasted slightly longer than 60 s, so the miss is a duration artefact and not a throughput shortfall. Derived: http_reqs.count / testRunDurationMs × 1000 (60,000 / 60.0018 s = 999.97).
- **Tariff: k6 against the service:** 120,003 requests in 60.0024 s. The target is 2,000 /s and the reported rate is 1,999.97 /s only because the run lasted slightly longer than 60 s, so the miss is a duration artefact and not a throughput shortfall. Derived: http_reqs.count / testRunDurationMs × 1000 (120,003 / 60.0024 s = 1,999.97).
- **Driver API: read requests/s (GET /v1/sites, k6):** The reported rate is http_reqs.count / test run duration, and http_reqs also counts 40 setup requests (register and login). The scenario itself completed 59,669 iterations in 60 s, about 994.48 per second (derived: iterations / ((iterations + dropped_iterations) / 1,000 per second)), and 331 iterations were dropped. The k6 script's own threshold on dropped_iterations (rate<10, held) is a 1% tolerance set in the script; ADR 0000 does not allow it, so the row counts as missed.
<!-- metrics:end -->

## Charts

Every chart is an SVG generated from the raw files by [`scripts/charts/`](scripts/charts/README.md) (`node scripts/charts/build.mjs`), works in GitHub light and dark themes, and carries a footer naming its source files and the measurement setup. One timeline chart is generated per directory in `docs/drill-output/`.

<!-- charts:start (generated by scripts/charts/build.mjs) -->
#### Target versus measured, per service

One bar per metric: the measured value divided by the target in `load/targets.json`. Bars are red where the measured value misses its target, with no tolerance: station-gateway: Messages per second; station-gateway: CALLRESULT p99; telemetry-ingest: Sustained rows per second (2,500 offered); sessions: Commands per second (StartTransaction + StopTransaction); smart-charging: Allocations per second (k6, service); tariff: Quotes per second (k6, service); driver-api: Read requests per second (GET /v1/sites).

![Target versus measured, per service](docs/img/targets-vs-measured.svg)

#### Throughput and p99 per service

Achieved rate and p99 latency from the k6 summaries.

![Throughput and p99 per service](docs/img/throughput-p99.svg)

#### sessions: latency versus offered rate

4 constant-arrival-rate runs against `sessions`.

![sessions: latency versus offered rate](docs/img/sessions-rate-sweep.svg)

#### WebSocket connections per gateway instance

The 10,000-connection run (one native gateway process) and the k6 runs against one gateway container.

![WebSocket connections per gateway instance](docs/img/gateway-connections.svg)

#### telemetry-ingest: COPY versus row-by-row

Sustained rate and durable lag against the offered rate. Hollow markers did not keep up.

![telemetry-ingest: COPY versus row-by-row](docs/img/telemetry-ingest.svg)

#### telemetry storage

Compression, chunk sizes and query latency on a 7-day backfill; the full-fleet sizes are derived and marked as such.

![telemetry storage](docs/img/telemetry-storage.svg)

#### smart-charging: allocation under the site cap

Drawn power against the sum of the site caps through the allocator outage drill, before, during and after.

![smart-charging: allocation under the site cap](docs/img/smart-charging-cap.svg)

#### Drill timeline: gateway-instance-loss-20261108T115832Z

Key series with before, during and after phases and the checks from `result.json`. Result: FAILED.

![Drill timeline: gateway-instance-loss-20261108T115832Z](docs/img/drill-gateway-instance-loss-20261108T115832Z.svg)

#### Drill timeline: gateway-instance-loss-20261108T122353Z

Key series with before, during and after phases and the checks from `result.json`. Result: PASSED.

![Drill timeline: gateway-instance-loss-20261108T122353Z](docs/img/drill-gateway-instance-loss-20261108T122353Z.svg)

#### Drill timeline: kafka-broker-loss-1-20261108T122708Z

Key series with before, during and after phases and the checks from `result.json`. Result: FAILED.

![Drill timeline: kafka-broker-loss-1-20261108T122708Z](docs/img/drill-kafka-broker-loss-1-20261108T122708Z.svg)

#### Drill timeline: kafka-broker-loss-1-20261108T140530Z

Key series with before, during and after phases and the checks from `result.json`. Result: PASSED.

![Drill timeline: kafka-broker-loss-1-20261108T140530Z](docs/img/drill-kafka-broker-loss-1-20261108T140530Z.svg)

#### Drill timeline: kafka-broker-loss-2-20261108T123144Z

Key series with before, during and after phases and the checks from `result.json`. Result: FAILED.

![Drill timeline: kafka-broker-loss-2-20261108T123144Z](docs/img/drill-kafka-broker-loss-2-20261108T123144Z.svg)

#### Drill timeline: kafka-broker-loss-2-20261108T140952Z

Key series with before, during and after phases and the checks from `result.json`. Result: PASSED.

![Drill timeline: kafka-broker-loss-2-20261108T140952Z](docs/img/drill-kafka-broker-loss-2-20261108T140952Z.svg)

#### Drill timeline: reconnect-storm-20261108T120311Z

Key series with before, during and after phases and the checks from `result.json`. Result: PASSED.

![Drill timeline: reconnect-storm-20261108T120311Z](docs/img/drill-reconnect-storm-20261108T120311Z.svg)

#### Drill timeline: slow-postgres-20261108T142904Z

Key series with before, during and after phases and the checks from `result.json`. Result: FAILED.

![Drill timeline: slow-postgres-20261108T142904Z](docs/img/drill-slow-postgres-20261108T142904Z.svg)

#### Drill timeline: slow-postgres-20261108T150304Z

Key series with before, during and after phases and the checks from `result.json`. Result: FAILED.

![Drill timeline: slow-postgres-20261108T150304Z](docs/img/drill-slow-postgres-20261108T150304Z.svg)

#### Drill timeline: slow-postgres-20261108T160959Z

Key series with before, during and after phases and the checks from `result.json`. Result: FAILED.

![Drill timeline: slow-postgres-20261108T160959Z](docs/img/drill-slow-postgres-20261108T160959Z.svg)

#### Drill timeline: slow-postgres-20261108T162708Z

Key series with before, during and after phases and the checks from `result.json`. Result: PASSED.

![Drill timeline: slow-postgres-20261108T162708Z](docs/img/drill-slow-postgres-20261108T162708Z.svg)

#### Drill timeline: smart-charging-down-20261108T114310Z

Key series with before, during and after phases and the checks from `result.json`. Result: PASSED.

![Drill timeline: smart-charging-down-20261108T114310Z](docs/img/drill-smart-charging-down-20261108T114310Z.svg)

#### Drill timeline: station-offline-replay-20261108T141434Z

Key series with before, during and after phases and the checks from `result.json`. Result: PASSED.

![Drill timeline: station-offline-replay-20261108T141434Z](docs/img/drill-station-offline-replay-20261108T141434Z.svg)

#### Drill timeline: station-offline-replay-20261108T151006Z

Key series with before, during and after phases and the checks from `result.json`. Result: FAILED.

![Drill timeline: station-offline-replay-20261108T151006Z](docs/img/drill-station-offline-replay-20261108T151006Z.svg)
<!-- charts:end -->

## What the drills and reviews caught

Every item below was found during this build by a code review, a load test or a failure drill. Labels: **measured** means the number comes from a raw file in this repository (named) or a test assertion; **derived** means calculated from stated values with the formula given; **observed, not saved** means it was read from live `/metrics`, `free` or k6 output during a run and no file records it, so it cannot be reproduced from this repository; **review scenario** means a worked example from a review that was not measured on the running system.

### 1. Allocation command storm towards offline stations

- **Found by:** a k6 sessions run with the simulator stopped.
- **Symptom (observed, not saved as a file):** 22,190 `SetChargingProfile` commands published and 3,144 trigger rounds within minutes while the stations were offline; Kafka sat at about 128% CPU per broker.
- **Cause:** every non-Accepted profile result (`StationOffline`, `Timeout`) re-triggered the site's round, which re-sent the same profiles.
- **Fix:** only Accepted results re-trigger. Other outcomes wait for the periodic 60 s round.
- **Regression test:** [`services/sessions/test/int/two-phase.test.ts`](services/sessions/test/int/two-phase.test.ts) (the assertion carries the comment "A non-Accepted result must not re-trigger the site"; that comment is not a test name).

### 2. Site cap exceeded during reallocation

- **Found by:** the Opus review of `sessions`.
- **Symptom (review scenario, not measured):** cap 30 kW, station A at 22 kW. A round gives A and B 15 kW each, A's decrease is lost because the station is offline, and B's increase lands: 37 kW in force.
- **Cause:** decreases and increases were sent together, on different partitions, with no ordering; a non-Accepted result also nulled the stored limit.
- **Fix:** two-phase apply. The in-force limit is tracked, increases are sent only when the headroom covers the worst case, and an unconfirmed profile is never forgotten ([ADR 0006](docs/adr/0006-smart-charging-allocation.md), "Applying an allocation").
- **Regression test:** [`services/sessions/test/int/two-phase.test.ts`](services/sessions/test/int/two-phase.test.ts).

### 3. Gateway safe share could exceed the cap, and one profile id was shared by all connectors

- **Found by:** the Opus review of the gateway.
- **Symptom (review scenario, not measured):** cap 30 kW and 3 connectors with a safe share of 10 kW. A is at 20 kW, B is raised to 10 kW, C is at its start power of 4.14 kW: 34.1 kW. Separately, connector 2's profile replaced connector 1's.
- **Cause:** the safe share was raised per connector instead of per site, and every connector used the same profile id.
- **Fix:** one profile id per connector, `txProfileId(c) = 100 + c`, and the safe share is applied only when the whole site is stale (Redis key `alloc:site:{id}`, written by `sessions` before each round commits).
- **Regression tests:** [`services/station-gateway/test/int/gateway.int.test.ts`](services/station-gateway/test/int/gateway.int.test.ts), [`simulator/test/unit/profiles.test.ts`](simulator/test/unit/profiles.test.ts).

### 4. Invoices lost after a short Postgres outage

- **Found by:** the Opus review of `billing`.
- **Symptom (review scenario; derived from the consumer defaults: 5 attempts with full-jitter backoff capped at 200, 400, 800 and 1,600 ms, so at most about 3 s in total and about 1.5 s on average):** a Postgres blip of a few seconds dead-lettered `session.completed`, and no invoice was ever issued.
- **Cause:** handler errors were retried only a few times before the record went to the dead-letter topic.
- **Fix:** the platform consumer retries handler errors indefinitely and dead-letters only undecodable records.
- **Regression test:** [`services/billing/test/int/outage.test.ts`](services/billing/test/int/outage.test.ts) (about 10 s with connections refused; the invoice is still issued and the DLQ stays empty).

### 5. The outbox broke per-key event order with two replicas

- **Found by:** the ponytail audit, confirmed by the Opus review.
- **Symptom (review scenario, not measured):** with two replicas, events for one key could be published out of order.
- **Cause:** every replica ran a `SKIP LOCKED` relay, and `bigserial` ids are allocated before commit.
- **Fix:** a single active relay per database (advisory lock) that publishes rows whose transaction finished before every running one, ordered by `(tx, id)` ([ADR 0002](docs/adr/0002-contracts-events-and-delivery-semantics.md) erratum).
- **Regression test:** [`packages/platform/test/int/outbox.test.ts`](packages/platform/test/int/outbox.test.ts) ("does not publish a later-committed row before an earlier-started one", "a second relay is a no-op").

### 6. `NOTIFY` serialised every commit

- **Found by:** a k6 sessions run, then traces and `pg_stat_activity`.
- **Symptom (observed, not saved as a file):** `COMMIT` waiting on `Lock:object`, connection pool exhaustion ("timeout exceeded when trying to connect"), and the OCPP hot path at multi-second p99.
- **Cause:** the outbox `AFTER INSERT` trigger ran `pg_notify`. Postgres takes one global lock at commit for every notifying transaction and holds it across the WAL flush.
- **Fix:** no trigger; the relay polls every 50 ms. Migration `0004_outbox_drop_notify.sql` in [`sessions`](services/sessions/migrations/0004_outbox_drop_notify.sql) and [`billing`](services/billing/migrations/0004_outbox_drop_notify.sql).
- **Regression test:** [`packages/platform/test/int/outbox.test.ts`](packages/platform/test/int/outbox.test.ts) (asserts that there is no trigger and that publication happens within 1 s).

### 7. Concurrent `StartTransaction` on one connector returned 500

- **Found by:** a k6 sessions run. The error (unique violation `active_sessions_connector_pk_key`) was observed in the k6 output and not saved as a file.
- **Cause:** two transactions raced to insert the active session of one connector.
- **Fix:** `SELECT ... FOR NO KEY UPDATE OF c` on the connector row.
- **Regression test:** [`services/sessions/test/int/ocpp.test.ts`](services/sessions/test/int/ocpp.test.ts) ("concurrent StartTransactions on one connector").

### 8. `/v1/sites` returned 429,276 bytes per call

- **Found by:** the k6 driver-api run.
- **Symptom (observed, not saved as a file):** 429,276 bytes per call for 146 sites and 1,460 stations, which capped driver-api reads at about 527 RPS.
- **Fix:** the nearest 20 sites by default (limit up to 100). Afterwards the saved run ([`k6-driver-api-reads.json`](docs/load-output/k6-driver-api-reads.json), measured) reached 955.59 req/s (`http_reqs` rate) at p99 255.2 ms against 1,000 req/s offered. The rate is a **miss** against the 1,000 req/s target, and the p99 passes (target 300 ms). `http_reqs` includes 40 setup requests, and the scenario iterations ran at about 994.5 per second [derived: 59,669 iterations / 60 s], with 331 of 60,000 iterations dropped. The 1% dropped-iteration tolerance belongs to the k6 script; ADR 0000 does not define it.
- **Regression test:** none; the load script [`load/k6/driver-api-reads.js`](load/k6/driver-api-reads.js) is the guard.

### 9. Memory pressure in the lab VM

- **Found by:** k6 results that degraded over time.
- **Symptom (observed with `free` in a container, not saved as a file):** swap at 1,023 MB with 1.3 MB free, the Jaeger in-memory store at 1.15 GB with 100% sampling, and Kafka GC at about 150% CPU per broker.
- **Fix:** 10% parent-based trace sampling, documented in [`docker-compose.yml`](docker-compose.yml). That was not enough; see item 22 for the bounded Jaeger store.
- **Regression test:** none; it is a configuration change.

### 10. Fleet convention drift between generators

- **Found by:** the Opus review.
- **Symptom (review scenario, not measured):** `sessions`, the simulator and the backfill each had their own DC power rule (for example station g=24 was 50 kW in one and 150 kW in another).
- **Fix:** one convention in [`packages/contracts/src/fleet.ts`](packages/contracts/src/fleet.ts) with a generated fixture.
- **Regression tests:** `TestFleetFixture` in [`scripts/backfill-telemetry/internal/gen/`](scripts/backfill-telemetry/internal/gen/) and the simulator fleet test [`simulator/test/unit/fleet-demand.test.ts`](simulator/test/unit/fleet-demand.test.ts).

### 11. Billing under-collected totals above the hold and could strand a charge

- **Found by:** the Opus review.
- **Symptom (review scenario, not measured):** invoice totals above the authorized hold were under-collected, and a charge could be stranded when the card changed in the middle of a retry.
- **Fix:** the remainder is captured with the idempotency key `capture:{invoiceId}:rest`, and the exact capture request is persisted and replayed.
- **Regression test:** [`services/billing/test/int/psp-edge.test.ts`](services/billing/test/int/psp-edge.test.ts).

### 12. One bad telemetry row stopped ingest globally

- **Found by:** the Opus review.
- **Symptom (review scenario, not measured):** one bad row stopped ingest for everything.
- **Fix:** bisect the failed batch and dead-letter only the rejected record.
- **Regression test:** `TestPoisonRecordIsBisectedAndDeadLettered` in [`services/telemetry-ingest/internal/ingest/pipeline_integration_test.go`](services/telemetry-ingest/internal/ingest/pipeline_integration_test.go).

### 13. A builder agent reported "committed" while its review fixes were still uncommitted

- **Found by:** checking the running compose stack (the rate limits were missing from the container environment).
- **Process fix:** every worktree is verified clean before it is merged.
- **Regression test:** none; it is a process rule.

### 14. A late duplicate connect attempt left one station stuck after a gateway loss

- **Found by:** the gateway-instance-loss drill, first run ([`gateway-instance-loss-20261108T115832Z`](docs/drill-output/gateway-instance-loss-20261108T115832Z/result.json), **FAILED**, kept as evidence).
- **Symptom (measured, `result.json`):** 199 of 200 stations reconnected (`routes_present_after_loss` 199, `fleet_connected_after_recovery` 199, and no measurement for `stations_reconnected_within_s`).
- **Cause:** a late duplicate connect attempt completed on the server after the current one, and the gateway replaced the current socket with close code 4000. The simulator then stopped reconnecting.
- **Fix:** the simulator closes superseded sockets when they open and reconnects after a 10 to 30 s pause on close code 4000.
- **Tests:** the simulator unit tests, and the drill itself. The rerun ([`gateway-instance-loss-20261108T122353Z`](docs/drill-output/gateway-instance-loss-20261108T122353Z/result.json), passed, measured) reconnected in 31 s with 200 of 200 routes.

### 15. The gateway crashed with two Kafka brokers down

- **Found by:** the kafka-broker-loss drill with two brokers, first run ([`kafka-broker-loss-2-20261108T123144Z`](docs/drill-output/kafka-broker-loss-2-20261108T123144Z/result.json), **FAILED**, kept as evidence with a [`NOTE.md`](docs/drill-output/kafka-broker-loss-2-20261108T123144Z/NOTE.md)).
- **Symptom (measured, `result.json`):** `gateway_ready_during_outage` got no HTTP answer (recorded as 0, expected 200), and 126 of 280 probe readings were missing. The produce error rate and the buffer depth checks passed (34.79 errors/s at most, buffer depth 549).
- **Cause:** the `@platformatic/kafka` consumer client emitted an unhandled `error` event ("joinGroup failed 4 times"), which crashed the process, and the restart then blocked on joining the group.
- **Fix:** the platform consumer handles client errors, rebuilds the client, and joins in the background, so `start()` no longer blocks the service.
- **Regression test:** [`packages/platform/test/int/kafka.test.ts`](packages/platform/test/int/kafka.test.ts) ("kafka consumer without reachable brokers"). The three stations stuck before the outage (item 16) explain at most 84 of the 126 missing readings [derived: 3 × 28 readings per probe station in `probes/readings.csv`]; the remaining 42 have no named cause and are not isolated.

### 16. OCPP deduplication returned another message's stored response after a simulator restart

- **Found by:** the kafka-broker-loss drill with one broker, first run ([`kafka-broker-loss-1-20261108T122708Z`](docs/drill-output/kafka-broker-loss-1-20261108T122708Z/result.json), **FAILED**, kept with a [`NOTE.md`](docs/drill-output/kafka-broker-loss-1-20261108T122708Z/NOTE.md)).
- **Symptom (measured, `result.json`):** 30 of 100 probe readings were missing. All of them belonged to three stations that were stuck before the outage.
- **Cause:** `sessions` deduplicated OCPP messages on `stationId:uniqueId`. After a simulator restart the uniqueId counters restarted, so a new `StartTransaction` received the stored response of an old message, which had no `transactionId`, and the station retried forever.
- **Fix:** deduplicate on uniqueId, action and payload hash (`messageKey` in [`services/sessions/src/ocpp/ocpp.service.ts`](services/sessions/src/ocpp/ocpp.service.ts)), and give the simulator boot-prefixed message ids.
- **Regression test:** [`services/sessions/test/int/ocpp.test.ts`](services/sessions/test/int/ocpp.test.ts) ("uniqueId reuse after a firmware restart").

### 17. The first station-offline-replay run was invalid and still passed

- **Found by:** reading the first run of `drills/station-offline-replay.sh`.
- **Symptom:** `POST /offline` returned HTTP 400, so no station went offline, yet the drill passed. The run was deleted.
- **Fix:** the drill now checks that the stations really went offline (`stations_taken_offline`) and that the connected count dropped (`connected_dropped_during_offline`). The valid rerun ([`station-offline-replay-20261108T141434Z`](docs/drill-output/station-offline-replay-20261108T141434Z/result.json), passed, measured) simulated 10 minutes offline at time scale 1 with 20 stations: 31 s to drain the replay queues, 0 dropped queue entries, 0 of 1,272 probe readings missing, 0 duplicate rows, and 960 replayed rows flagged. The 24 h offline replay at time scale 60 ([`station-offline-replay-20261108T151006Z`](docs/drill-output/station-offline-replay-20261108T151006Z/result.json), **FAILED** and **inconclusive**, kept with a [`NOTE.md`](docs/drill-output/station-offline-replay-20261108T151006Z/NOTE.md)) did not show a loss. The replay queue was still draining (140,564 messages at its peak, 89,114 when the 600 s wait ended), and then the harness restored the simulator before the queue had drained, which discarded the remaining in-memory queue and produced the 82,914 "missing" readings. That is a harness bug, and a rerun that waits for the queue to drain is still to do.

### 18. The drill harness wrote curl's "000" as a number and produced invalid JSON

- **Found by:** the `result.json` of the two-broker drill (item 15), which contained `"measured":000`.
- **Fix:** the harness now normalises every check value. In the kept two-broker run that one token was changed to `0` by hand so that the file parses (stated in its `NOTE.md`). The chart generator also skips and reports any malformed `result.json` instead of failing.

### 19. Allocation feedback loop: one SetChargingProfile result caused a round that resent every profile

- **Found by:** the failed slow-Postgres run ([`slow-postgres-20261108T142904Z`](docs/drill-output/slow-postgres-20261108T142904Z/result.json), **FAILED**, kept with a [`NOTE.md`](docs/drill-output/slow-postgres-20261108T142904Z/NOTE.md)), followed by the Prometheus metrics `sessions_charging_profiles_sent_total` and `outbox_published_total`.
- **Symptom (observed, not saved as a file):** about 140 SetChargingProfile/s published for a 20-site, 200-station fleet, with all three Kafka brokers at 140-160% CPU.
- **Cause, part 1:** every Accepted SetChargingProfile result immediately triggered a new round at the site.
- **Cause, part 2:** every round resent every session's profile at the site (the reason was site-level), so one change or one result cost one command per session.
- **Fix:** an Accepted result now makes the site due in 2 s (debounced, picked up by the tick) instead of kicking a round. Profiles are sent per session: new, changed by at least max(2%, 200 W), within 60 s of `validTo`, unconfirmed and last sent at least 30 s ago (retry), or all of them when the site is unsafe. See [ADR 0006](docs/adr/0006-smart-charging-allocation.md).
- **Result (observed via Prometheus, not saved as a file):** about 3 profiles/s in total (changed 1.49, refresh 1.15, retry 0.37, new 0.04).
- **Regression tests:** [`services/sessions/test/unit/plan.test.ts`](services/sessions/test/unit/plan.test.ts) ("re-sends only the session that changed", "retries an unconfirmed profile ... 30 s") and [`services/sessions/test/int/two-phase.test.ts`](services/sessions/test/int/two-phase.test.ts).

### 20. HAProxy kept stale gateway IPs

- **Found by:** three drill reruns that ran with 0 of 200 stations connected. They were invalid and were deleted.
- **Cause:** HAProxy resolved `station-gateway-1` and `station-gateway-2` only at startup. Recreated gateway containers got new IPs, so every upgrade got 503.
- **Fix:** `resolvers docker` (127.0.0.11) with `init-addr libc,none` in [`infra/haproxy/haproxy.cfg`](infra/haproxy/haproxy.cfg). Every drill now calls `require_fleet` and aborts if the fleet is not fully connected. See [ADR 0004](docs/adr/0004-websocket-connection-routing.md) and the [runbook](docs/runbook.md).
- **Regression test:** none; the guard is the `require_fleet` check in the drills.

### 21. The slow-Postgres drill never lifted its CPU cap

- **Found by:** the notes of the two failed Kafka runs ([`kafka-broker-loss-1-20261108T122708Z`](docs/drill-output/kafka-broker-loss-1-20261108T122708Z/NOTE.md), [`kafka-broker-loss-2-20261108T123144Z`](docs/drill-output/kafka-broker-loss-2-20261108T123144Z/NOTE.md)), whose telemetry database was still capped.
- **Cause:** `docker update --cpus 0` does not remove a limit, so the telemetry Postgres stayed at 0.05 CPU after the drill. The affected slow-Postgres run was deleted, and the two failed Kafka runs carry a note.
- **Fix:** the drill restores the Docker VM CPU count. The telemetry postgres-exporter also skips per-table collectors.
- **Regression test:** none.

### 22. Unbounded Jaeger memory

- **Found by:** the failed slow-Postgres run again: Kafka itself was slow.
- **Symptom (observed, not saved as a file):** 864 MB even at 10% sampling, with swap full again. `kafka-producer-perf-test` with acks=all at 50 msg/s gave p50 493 ms and p99 1,230 ms (observed, not saved).
- **Fix:** a Jaeger v2 config, [`infra/jaeger/config.yaml`](infra/jaeger/config.yaml), with memory `max_traces` 20000, plus `mem_limit: 400m`. After a rolling Kafka restart the same producer test gave p50 14 ms and p99 295 ms at 50 msg/s (observed, not saved).
- **Regression test:** none; it is a configuration change.

### 23. The first slow-Postgres failures were a Kafka leader move and an overloaded VM, not the slow database

- **Found by:** a slow-Postgres run ([`slow-postgres-20261108T150304Z`](docs/drill-output/slow-postgres-20261108T150304Z/result.json), **FAILED** on one check, kept with a [`NOTE.md`](docs/drill-output/slow-postgres-20261108T150304Z/NOTE.md)).
- **What held (measured, `result.json` and `NOTE.md`):** ingest stalled and the backlog stayed in Kafka (peak consumer lag 133 records, below the 200-record threshold of `lag_drained_within_s`, so its value of 0 does not show a drain); the durable lag p99 was 1.37 s 90 s after recovery (target < 5 s); 0 of 2,103 probe readings were missing.
- **What failed (measured, `result.json` and `timeline.jsonl`):** the OCPP CALLRESULT p99 rose to the 10 s histogram cap during the slowdown (target 0.5 s), then fell to about 0.7 s afterwards. It was already above the target before the database was capped: 0.24-0.47 s until 15:03:44, then 0.84-0.85 s (maximum 0.849 s) from 15:03:45, before the 15:04:06 cap and the 15:04:31 leader move. That pre-fault rise is not isolated. Heartbeat, answered locally by the gateway, stayed at about 8 ms. Calls that wait for a Kafka ack and StopTransaction slowed together.
- **Cause (isolated afterwards from the broker and gateway logs, observed, not saved as a file; details in `NOTE.md`):** a Kafka preferred-leader rebalance (`auto.leader.rebalance.enable`, every 300 s) moved partition leadership at 15:04:31, 5 min after a rolling broker restart, and the gateway's in-flight produces hung for the full 30 s send timeout (50 x "kafka send timed out after 30000ms" in the gateway log, observed, not saved as a file), holding every Kafka-acked CALLRESULT. The slow database itself was not the cause.
- **Fix:** the gateway send timeout is now 3 s (`KAFKA_SEND_TIMEOUT_MS`); a hung produce fails fast with CALLERROR and the station retries the same uniqueId. This bounds the stall during a leader move at about 3 s; it does not meet the 500 ms SLO during the move.
- **Consequence:** the run stays a FAIL in the metrics table, as [ADR 0000](docs/adr/0000-requirements-and-capacity.md) requires. It does not show that a slow telemetry database hurts the OCPP path; it shows that a partition leader move does.
- **Clean rerun:** [`slow-postgres-20261108T162708Z`](docs/drill-output/slow-postgres-20261108T162708Z/NOTE.md) **PASSED**: it ran after a forced PREFERRED election and more than 5 min of waiting (both observed by hand, not saved in that run's `events.log`). CALLRESULT p99 was 0.076 s during the slowdown, durable lag 0.49 s 90 s after recovery, and 0 probe readings were missing (measured, `result.json`). At its default 30 s meter interval no Kafka backlog built up (peak consumer lag 0, 140 probe readings), so its `lag_drained_within_s` of 0 shows no drain, and the run does not confirm that a backlog leaves the OCPP path unaffected: both 2 s-meter runs that built a backlog failed the CALLRESULT check (150304Z with an isolated Kafka leader move, 160959Z on an overloaded VM). In 160959Z the CALLRESULT p99 was 2.48 s during the slowdown and the durable lag 9.9 s after recovery. A further run at 2 s meters ([`slow-postgres-20261108T160959Z`](docs/drill-output/slow-postgres-20261108T160959Z/NOTE.md)) was confounded by an overloaded VM and is kept with a note.
- **Isolated reproduction:** `load/repro/leader-move.sh` ([results](docs/load-output/leader-move-README.md)) shows the client stalling produces for up to 4.4 s on a leader move with its default retries, and about 1.3-1.9 s across the tuned runs (p99 1,317 and 1,387 ms, max 1,693 and 1,894 ms). The rest is recorded as a named library limitation in ADR 0012.

**Passing reruns.** [`kafka-broker-loss-1-20261108T140530Z`](docs/drill-output/kafka-broker-loss-1-20261108T140530Z/result.json) passed with 0 of 96 probe readings missing. [`kafka-broker-loss-2-20261108T140952Z`](docs/drill-output/kafka-broker-loss-2-20261108T140952Z/result.json) passed (measured): the gateway stayed live (HTTP 200 on `/health/live`), produce errors rose to 7.20/s at most, the buffer depth to 187 at most, and 0 of 254 probe readings were missing. The failed first runs stay as evidence with their notes. The gateway-instance-loss rerun passed with 31 s (item 14), and the station-offline-replay rerun passed (item 17).

**Runs before the fixes.** The reconnect-storm run ([`reconnect-storm-20261108T120311Z`](docs/drill-output/reconnect-storm-20261108T120311Z/timeline.jsonl)) shows at most 199 connected stations of 200 in its timeline (`sim_connected`). A slow-postgres run from the same period passed its checks, but it was deleted after item 21 showed that the CPU cap was never lifted; three of the later slow-Postgres runs (items 19 and 23: 142904Z, 150304Z, 160959Z) failed, and the fourth (162708Z, item 23) passed. The reconnect-storm run passed its checks.

## Known limits and production gaps

**The station gateway misses its per-instance target.** [ADR 0000](docs/adr/0000-requirements-and-capacity.md) asks for at least 10,000 connections and at least 2,000 msg/s with CALLRESULT p99 under 500 ms per instance. One gateway holds 10,000 connections at a peak RSS of 525 MB ([`gateway-10k-connections.json`](docs/load-output/gateway-10k-connections.json)). Under k6 load with the real `sessions` service behind it, it delivers 910.28 msg/s (1,000 stations) and 1,139.03 msg/s (2,000 stations) with p99 of 1,446 ms and 3,228 ms and zero CALLERRORs (measured, [`k6-gateway-ws-1000st-1hz.json`](docs/load-output/k6-gateway-ws-1000st-1hz.json) and [`k6-gateway-ws-2000st-1hz.json`](docs/load-output/k6-gateway-ws-2000st-1hz.json)). The 1,000-station run offers at most 1,000 msg/s (1 msg/s per station, one outstanding CALL each [derived]), so it cannot test the 2,000 msg/s target: its rate is **not tested**. Only the 2,000-station run tests the rate, and it **missed** at 1,139.03 msg/s. The CALLRESULT p99 target is **missed** in both runs. The target was recorded as measured and not lowered, as ADR 0000 requires.

- **Bottleneck ([ADR 0004](docs/adr/0004-websocket-connection-routing.md), measured amendment):** one Node.js core per instance. A single gateway process was observed at about 108% CPU during these runs (observed, not saved as a file; the saved 10,000-connection run is a different load, with `samples[].cpuPct` peaking at 110 in [`gateway-10k-connections.json`](docs/load-output/gateway-10k-connections.json)). Per message it does a JSON parse, zod validation, a Kafka produce with `acks=all` through a pure-JS client, and a second produce for the raw log.
- **Raw-log experiment (unreproducible, not used as evidence):** an earlier experiment reported 802 and 1,068 msg/s with the raw log on and off. No file was saved, and the figures are inconsistent with the kept 2,000-station run (1,139.03 msg/s with the raw log on). The effect of removing the second produce is not measured.
- **The fleet requirement is not demonstrated:** the 2,400 msg/s design peak spread over 5 surviving instances is 480 msg/s per instance [derived: 2,400 / 5]. No run measured the latency at 480 msg/s. The 910.28 msg/s run had no errors but missed the p99 target (1,446 ms against 500 ms).
- **Scaling path:** one gateway process per core (Node cluster or more replicas behind HAProxy; the registry already supports any number of instances), sampling or batching the raw log, and a precompiled validator instead of a per-frame zod parse.

**Other limits of the measurements**

- Everything is single-instance on one laptop, and all containers share one Docker VM with the load generator (see the setup records, `docs/load-output/*.env.json`). Absolute numbers are not production numbers; they show where each design stands against its own target.
- The sessions sweep is not monotone: at 150 commands/s the StartTransaction p99 was 621.77 ms, above the script's 500 ms threshold, while 200 commands/s passed with 177.39 ms. Each point is one 60 s run, and the cause of the 150/s outlier was not isolated.
- The smart-charging open-loop p99 of 19.45 ms ([`smart-charging-allocbench.json`](docs/load-output/smart-charging-allocbench.json), measured) is close to its 20 ms target.
- Gaps in the evidence: no saved run measures the driver-api write p99 (target 800 ms), the telemetry visible-in-driver-api p99 (target 10 s), the remote-start SLOs or the invoice-after-session-end p99. Those targets in `load/targets.json` have no measurement behind them yet.
- The full-fleet storage sizes are extrapolated from a 5,000-station, 7-day sample and are labelled derived. Compression and query numbers were measured with the background policy jobs paused and run manually (notes in `docs/load-output/telemetry-backfill-*.json`).
- Drill results in the metrics table and the drill charts are regenerated from `docs/drill-output/`; a failed check appears as FAIL.
- **Slow-Postgres:** the clean run (162708Z) passes (item 23), but it built no backlog (peak consumer lag 0). Earlier runs failed because of a coincident Kafka leader move (150304Z, with a pre-fault p99 rise that is not isolated) and an overloaded lab VM (160959Z), and are kept as evidence. A partition leader move still stalls the gateway's produces for about 1.3-1.9 s across the tuned runs (1.7 s at most with the shipped setting), a named limitation of `@platformatic/kafka` (ADR 0012).
- **Lab VM limit:** three Kafka JVM brokers on one 8 GB Docker VM show 100-170% CPU per broker (observed, not saved as a file) even at low traffic once memory gets tight (GC). Rolling restarts recover them. Measurements taken in that state were discarded and rerun, so the saved runs should be read as taken on a healthy VM, not a tight one.
- **Drills need a connected fleet:** every drill aborts through `require_fleet` unless all stations are connected, so a drill never reports on an empty fleet (item 20).
- **Jaeger keeps at most 20000 traces** in memory (`mem_limit` 400m), so old traces disappear under load.
- **24 h offline replay:** failed and inconclusive ([`station-offline-replay-20261108T151006Z`](docs/drill-output/station-offline-replay-20261108T151006Z/NOTE.md)). The 10-minute run at time scale 1 passed. In the 24 h run at time scale 60 the harness restored the simulator before the replay queue drained, so the result says nothing about loss; the rerun is still to do.
- NOT_ENOUGH_REPLICAS cannot be provoked on the compose Kafka cluster, so that path is covered by a unit test with a failing producer ([gateway README](services/station-gateway/README.md)). The two-broker drill therefore uses produce errors and buffer growth as its evidence, not the literal error code.
- The reconnect-storm drill disconnected 199 stations (`events.log` of the run; the default fleet has 200) and a warm Redis, not with the 833 connects/s of ADR 0003, and the billing service has no load measurement.

**Production gaps (lab simplifications, from the [runbook](docs/runbook.md) and the ADRs)**

- **Service authentication:** a shared internal token instead of mTLS ([ADR 0010](docs/adr/0010-authentication.md)).
- **Station security:** OCPP security profile 1 (Basic auth) instead of TLS client certificates ([ADR 0003](docs/adr/0003-ocpp-subset-station-auth-and-offline-replay.md)).
- **Durability:** single-node Postgres, so RPO 0 holds for commits but not for node loss; production needs a synchronous standby ([ADR 0001](docs/adr/0001-architecture-overview.md)).
- **Kafka:** combined broker and controller nodes; production would use dedicated controllers ([ADR 0012](docs/adr/0012-kafka-replication-and-producer-backpressure.md)).
- **Simulator control API:** unauthenticated, bound to 127.0.0.1 only.
- **PSP:** a mock. A real adapter must support idempotency keys and voiding by authorization key ([ADR 0007](docs/adr/0007-billing-saga-and-exactly-once-effect.md)).
- **Out of scope:** OCPI roaming, OCPP 2.0.1, ISO 15118, a real payment provider, multi-region active-active and grid or energy-market integration ([ADR 0000](docs/adr/0000-requirements-and-capacity.md)).

## Repo map

```
docs/
  adr/                 14 ADRs (0000 to 0013) plus the index: requirements, architecture, contracts, per-service decisions
  runbook.md           start, stop, signals worth alerting on, failure recipes, known gaps
  load-output/         raw k6 summaries (+ .env.json setup records) and tool outputs: the source of every measured number
  drill-output/        raw failure-drill output: timeline.jsonl, events.log, result.json per run
  img/                 architecture.svg (hand-made) and the generated charts
services/
  station-gateway/     NestJS: OCPP 1.6J WebSocket edge, routing, offline replay
  sessions/            NestJS: catalog, reservations, session state machine, allocation rounds
  billing/             NestJS: saga, mock PSP, invoices
  driver-api/          NestJS: driver authentication and the public API
  smart-charging/      Go: water-filling allocator, allocbench tool
  tariff/              Go: deterministic pricing, quotebench tool
  telemetry-ingest/    Go: Kafka to TimescaleDB, batched COPY
simulator/             Node: OCPP station fleet, demand and physics, control API
packages/
  contracts/           TypeScript contracts, event schemas, shared fleet convention
  contracts-go/        Go side of the contracts
  platform/            shared NestJS building blocks: consumers, outbox, HTTP client, metrics
  platform-go/         shared Go building blocks
load/                  k6 scripts, run.sh (records the setup next to each summary) and targets.json
drills/                failure drills: broker loss, gateway loss, reconnect storm, slow Postgres, allocator down, station offline replay
scripts/
  charts/              chart and metrics-table generator (plain Node, no dependencies)
  backfill-telemetry/  Go: history straight into TimescaleDB, compression and query measurements
  check-topics.mjs     checks that the Kafka topic list in code and in infra/kafka/topics.sh agree
infra/                 compose configuration: HAProxy, Kafka topics, Postgres init, Prometheus, Grafana
docker-compose.yml     the whole stack
Makefile               make check, make up, make down
```

## Regenerating the charts and the metrics table

```sh
node scripts/charts/build.mjs
```

This rewrites `docs/img/*.svg` and the two generated blocks in this README from the raw files only. See [`scripts/charts/README.md`](scripts/charts/README.md).
