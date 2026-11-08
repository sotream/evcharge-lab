# evcharge-lab runbook

How to run, observe and recover the platform. Each failure section names the alert signal, the first
checks, the action, and the drill in `drills/` that rehearses it. Raw drill output is in
`docs/drill-output/`. Architecture decisions are in `docs/adr/`.

## Start, stop, check

| Task | Command |
|---|---|
| Start everything (infra, services, simulator) | `docker compose up -d --build` |
| Only infra (for tests); stops the application containers first, then starts Postgres, Redis and Kafka | `make infra-up` |
| Full local check (lint, types, build, unit + integration, Go vet/lint/race) | `make check` |
| Stop and delete data | `make down` |
| Service health | `curl localhost:<port>/health/ready` (ports in ADR 0001) |
| Simulator state | `curl 127.0.0.1:28089/status` |

`make check` (through `make infra-up`) stops the application containers before the integration tests. After `make infra-up` or `make check`, run `docker compose up -d` to bring the application containers back. The tests share Redis
and Kafka with the stack, and live stations would claim the same routes. Infra keeps running.

Dashboards are Grafana on `localhost:23000`. Traces are in Jaeger on `localhost:26686`. The Jaeger store is
bounded: `infra/jaeger/config.yaml` keeps at most 20000 traces in memory and the container has `mem_limit: 400m`, so
old traces disappear under load. Without the bound it grew to 864 MB even at 10% sampling and filled the VM swap
(observed, not saved as a file). Raw metrics
are in Prometheus on `localhost:29090`. HAProxy stats are on `localhost:28099`.

To find a slow OCPP call in Jaeger, choose service `station-gateway`, operation `ocpp StartTransaction` (or another `ocpp <Action>`) and set `minDuration`, for example `500ms`; the child spans show whether sessions or a Kafka produce was slow.

Grafana provisions six dashboards in the `evcharge` folder from `infra/grafana/dashboards/`:

- `overview` shows the SLOs from ADR 0000. Open it first.
- `gateway` shows the station gateway: upgrades, produce buffer, commands, safe-share.
- `sessions-allocation` shows OCPP handling, allocation rounds, cap violations and reservations.
- `billing` shows sagas, invoice latency, captures and PSP calls.
- `telemetry` shows ingest rate, durable lag, backpressure and dead letters.
- `kafka-and-infra` shows consumer lag, producer outcomes, DLQ and HAProxy.

## Signals worth alerting on

| Signal | Query | Meaning |
|---|---|---|
| OCPP CALLRESULT p99 | `histogram_quantile(0.99, sum by (le) (rate(ocpp_call_duration_seconds_bucket[5m])))` | ADR 0000 target < 500 ms |
| Stations connected | `sum(gateway_connected_stations)` | sudden drop: gateway or LB problem |
| Produce buffer | `sum(gateway_produce_buffer_depth)`, `rate(gateway_produce_rejected_total[1m])` | broker cannot acknowledge (ADR 0012) |
| Consumer lag | `sum by (group) (kafka_consumer_lag)` | a consumer is blocked; handler errors retry indefinitely by design |
| Outbox backlog | `sum(outbox_pending)`, `outbox_publish_lag_seconds` | broker down or relay stuck |
| Telemetry durable lag | `histogram_quantile(0.99, sum by (le) (rate(telemetry_ingest_lag_seconds_bucket[5m])))` | ADR 0000 target < 5 s |
| Allocation backlog | `max(sessions_allocation_backlog_seconds)` | rounds fall behind the 60 s period |
| Allocator fallback | `rate(sessions_allocation_rounds_total{strategy="fallback-equal-share"}[5m])` | smart-charging unavailable |
| Cap invariant | `sessions_allocation_cap_violations_rejected_total`, `invariant_violations_total` | must stay 0 |
| Billing | `billing_sagas_pending`, `billing_sagas_parked_total`, `billing_psp_idempotency_mismatch_total` | stuck or parked sagas need a human |
| DLQ | `kafka_consumer_messages_total{outcome="dlq"}`, `telemetry_dead_lettered_total` | undecodable or unstorable records |

### Alerts and recording rules

The signals above are evaluated continuously by the rules in `infra/prometheus/rules/` (ADR 0014).
Firing alerts are visible at `localhost:29090/alerts`; there is no Alertmanager, so nothing pages.
Every alert carries a `severity` label and a `runbook` annotation that points at this file.

| Alert group | Alerts | Where to look |
|---|---|---|
| SLO burn rate | `DriverApiErrorBudgetBurnFast`/`Slow`, `GatewayErrorBudgetBurnFast`/`Slow` | overview panels "5xx ratio" and "CALLERROR ratio"; gateway and driver-api logs |
| Invariants | `SiteCapExceeded`, `AllocationCapViolationRejected`, `InvariantViolation` | sessions-allocation dashboard; any firing is a bug |
| Allocation | `AllocationBacklog` | see "Smart-charging allocator down" |
| Outbox and Kafka | `OutboxPublishLagHigh`, `OutboxPendingGrowing`, `KafkaConsumerLagHigh`, `KafkaExporterConsumerLagHigh` | see "Kafka broker lost"; kafka-and-infra dashboard |
| Dead letters | `DlqMessages`, `TelemetryDeadLettered` | see "Dead-letter topics" |
| Telemetry | `TelemetryDurableLagSloBreach`, `TelemetryIngestPaused` | see "Slow or unavailable telemetry Postgres" |
| Billing | `BillingSagaParked`, `BillingPspIdempotencyMismatch` | see "Billing saga parked" |

Recording rules (`slo:*`, `service:*`, `group:*`) are the single definition of each SLO and feed the
dashboards. The exporters (kafka-exporter on 29308, postgres-exporter on 29187 and 29188, redis-exporter
on 29121) back the panels at the bottom of the kafka-and-infra dashboard. `make prom-check` validates
the rules and runs their unit tests without touching the stack.

## Failures

**Drills need a connected fleet.** Every drill calls `require_fleet` and aborts if the 200-station fleet is not fully
connected, so check `curl 127.0.0.1:28089/status` before a drill.

**HAProxy and gateway redeploys.** HAProxy re-resolves `station-gateway-1` and `station-gateway-2` through Docker DNS
(`resolvers docker`, `init-addr libc,none` in `infra/haproxy/haproxy.cfg`), so a recreated gateway container is picked
up. If every upgrade gets 503 after a redeploy, check the HAProxy stats page on `localhost:28099` for both servers
before anything else (ADR 0004).

### Kafka broker lost (`drills/kafka-broker-loss.sh 1|2`)

- **One broker down.** Topics have RF 3 and `min.insync.replicas=2`, so producers keep writing after
  a leader election. Check `docker compose ps kafka-1 kafka-2 kafka-3`, then start the broker. No
  action is needed on services.
- **Two brokers down.** Producers get send errors (`NOT_ENOUGH_REPLICAS` by design; the drill checks the produce error rate and the buffer depth, because the literal code could not be provoked on the compose cluster, see ADR 0012). The gateway holds CALLRESULTs and
  buffers records; once the buffer is full it answers CALLERROR, and stations keep their messages
  queued. Outbox rows accumulate in Postgres. Charging continues on the profiles stations already
  have.
  - Action: restore a broker.
  - Afterwards, watch `outbox_pending` and the consumer lag drain.
  - Nothing needs replaying by hand.

### Gateway instance lost (`drills/gateway-instance-loss.sh`)

HAProxy marks the instance down within about 4 s, and stations reconnect to the survivors with a
random 0–10 s first delay. Routes in Redis are rewritten on upgrade. Commands for a station that has
not reconnected yet complete as `StationOffline`, and the producer retries.

- Action: restart the instance.
- Check `gateway_connected_stations` per instance.
- HAProxy `leastconn` only balances new connections. To rebalance, restart the other instance
  gracefully; close code 1012 makes its stations reconnect.

### Reconnect storm (`drills/reconnect-storm.sh`)

Admission control sheds upgrades above `GATEWAY_MAX_UPGRADES_PER_S` with 503 and `Retry-After`.
Bad credentials are limited separately (`GATEWAY_MAX_BAD_AUTH_PER_S`).

- Watch `gateway_upgrades_total` by outcome and `gateway_credential_lookups_total`. The Redis
  credential cache should absorb the storm.
- If `sessions` sees a lookup storm, the Redis cache was cold. It recovers once the cache is warm.

### Slow or unavailable telemetry Postgres (`drills/slow-postgres.sh`)

Ingest pauses (`telemetry_ingest_paused`) instead of growing memory. The backlog stays in Kafka for
up to 24 h.

**OCPP impact during the drill (measured; cause: a Kafka leader move).** An earlier version of this runbook said the OCPP path is unaffected. The
run [`slow-postgres-20261108T150304Z`](drill-output/slow-postgres-20261108T150304Z/NOTE.md) does not
support that: the CALLRESULT p99 rose to the 10 s histogram cap while the database was capped, even
though Heartbeat stayed at about 8 ms. The p99 was already 0.84-0.85 s (above the 0.5 s target) from 15:03:45, before the database cap and before the leader move; that pre-fault rise is not isolated (0.24-0.47 s before it). The cause was a Kafka preferred-leader rebalance that coincided with the run: the
gateway's produces hung for the 30 s send timeout. The gateway now fails them after 3 s. During a partition leader
move expect Kafka-acked OCPP calls to take up to about 3 s and stations to retry.
Later runs: [`slow-postgres-20261108T160959Z`](drill-output/slow-postgres-20261108T160959Z/NOTE.md) FAILED on an overloaded lab VM (CALLRESULT p99 2.48 s during the slowdown, durable lag 9.9 s after recovery), and [`slow-postgres-20261108T162708Z`](drill-output/slow-postgres-20261108T162708Z/NOTE.md) is the clean PASS (CALLRESULT p99 0.076 s, durable lag 0.49 s, 0 of 140 probe readings missing). The clean run had a peak consumer lag of 0, so it shows no drain; see ADR 0008 for the limits of that evidence.

- Action: fix the database (CPU, IO, locks: `pg_stat_activity`).
- Watch `kafka_consumer_lag{group="telemetry-ingest.meter-values"}` drain.
- Readings are deduplicated, so redelivery is harmless.

### Smart-charging allocator down (`drills/smart-charging-down.sh`)

`sessions` falls back to `safeShareW` per active session (strategy `fallback-equal-share`), which is
safe for any number of active sessions. If `sessions`, Redis or the broker are down too, profiles
expire after 180 s: stations drop to `startW`, and once the whole site is stale, the gateway raises
them to the safe share (ADR 0006).

- Action: restart smart-charging. Optimal rounds resume on the next tick.

### Stations offline for hours (`drills/station-offline-replay.sh`)

Stations that lose their network keep charging and queue `StartTransaction`, `StopTransaction` and `MeterValues`. On reconnect they
replay the queue in order with the original uniqueIds (ADR 0003). The drill takes 20 probe stations offline for
`OFFLINE_MIN` simulated minutes (default 1,440, which needs a high `SIM_TIME_SCALE` to finish quickly).

- Signals: `sim_replay_queue_depth` (simulator), `sessions_ocpp_replays_total`, `telemetry_dedup_dropped_total`.
- The drill passes only if stations really went offline (`stations_taken_offline`, `connected_dropped_during_offline`), the
  replay queues drain within 600 s, nothing was dropped from a queue, no probe reading is missing, there are no
  duplicate rows, and (at time scale 1 only) replayed rows are flagged.
- Action: none by hand. A station whose queue never drains is retrying a message that keeps failing; look at the
  response for that uniqueId in the `sessions` logs. OCPP deduplication keys on uniqueId, action and payload hash.
- Evidence: `docs/drill-output/station-offline-replay-20261108T141434Z/` passed (10 simulated minutes at time scale 1,
  20 stations, 960 replayed rows flagged). The 24 h run at time scale 60 (`station-offline-replay-20261108T151006Z`) FAILED and is inconclusive: the harness restored the simulator before the replay queue drained, which discarded the queued readings (see its [`NOTE.md`](drill-output/station-offline-replay-20261108T151006Z/NOTE.md)). A rerun that waits for the queue to drain is still to do. The first run was invalid
  (`POST /offline` returned HTTP 400, so no station went offline, yet the drill passed). It was deleted, and the drill
  now checks that the stations went offline.

### OLTP Postgres unavailable

- **Sessions:** StartTransaction and StopTransaction fail. Stations get CALLERROR and retry with the
  same uniqueId, and `sessions` is idempotent on it.
- **Consumers:** they retry handler errors indefinitely, with no DLQ, so no events are lost.
- **Billing:** sagas wait.
- Action: restore Postgres. Everything resumes without manual replay.

### Billing saga parked

`billing_sagas_parked_total` increases when a tariff answer is permanent (4xx) or after 24 h of
retries, or on a PSP idempotency mismatch (`billing_psp_idempotency_mismatch_total`).

- Inspect with `SELECT session_id, state, failure_reason FROM billing_sagas WHERE next_attempt_at IS NULL`.
- A mismatch means the capture request changed between retries. Verify the PSP-side capture before
  anything else, because money may already have moved.

### Dead-letter topics

`<topic>.dlq` holds records that could not be decoded or stored. Their headers carry the reason,
the source offset and the consumer group. Fix the producer or the data, then republish the record
to the source topic. Consumers are idempotent on `eventId`.

## Known production gaps (lab simplifications)

- **Service authentication:** a shared internal token between services instead of mTLS (ADR 0010).
- **Station security:** OCPP security profile 1 (Basic auth) instead of TLS client certificates
  (ADR 0003).
- **Durability:** single-node Postgres, so RPO 0 holds for commits but not for node loss. Production
  needs a synchronous standby (ADR 0001).
- **Kafka:** combined broker/controller nodes; production would use dedicated controllers
  (ADR 0012).
- **Simulator control API:** unauthenticated, and bound to 127.0.0.1 only.
- **PSP:** a mock. A real adapter must support idempotency keys and voiding by authorization key
  (ADR 0007).
