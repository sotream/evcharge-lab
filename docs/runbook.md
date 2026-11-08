# evcharge-lab runbook

How to run, observe and recover the platform. Each failure section names the alert signal, the first
checks, the action, and the drill in `drills/` that rehearses it. Raw drill output is in
`docs/drill-output/`. Architecture decisions are in `docs/adr/`.

## Start, stop, check

| Task | Command |
|---|---|
| Start everything (infra, services, simulator) | `docker compose up -d --build` |
| Only infra (for tests) | `make infra-up` |
| Full local check (lint, types, build, unit + integration, Go vet/lint/race) | `make check` |
| Stop and delete data | `make down` |
| Service health | `curl localhost:<port>/health/ready` (ports in ADR 0001) |
| Simulator state | `curl 127.0.0.1:28089/status` |

`make check` stops the application containers before the integration tests. The tests share Redis
and Kafka with the stack, and live stations would claim the same routes. Infra keeps running.

Dashboards are Grafana on `localhost:23000`. Traces are in Jaeger on `localhost:26686`. Raw metrics
are in Prometheus on `localhost:29090`. HAProxy stats are on `localhost:28099`.

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

## Failures

### Kafka broker lost (`drills/kafka-broker-loss.sh 1|2`)

- **One broker down.** Topics have RF 3 and `min.insync.replicas=2`, so producers keep writing after
  a leader election. Check `docker compose ps kafka-1 kafka-2 kafka-3`, then start the broker. No
  action is needed on services.
- **Two brokers down.** Producers get `NOT_ENOUGH_REPLICAS`. The gateway holds CALLRESULTs and
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
up to 24 h, and the OCPP path is unaffected.

- Action: fix the database (CPU, IO, locks: `pg_stat_activity`).
- Watch `kafka_consumer_lag{group="telemetry-ingest.meter-values"}` drain.
- Readings are deduplicated, so redelivery is harmless.

### Smart-charging allocator down (`drills/smart-charging-down.sh`)

`sessions` falls back to `safeShareW` per active session (strategy `fallback-equal-share`), which is
safe for any number of active sessions. If `sessions`, Redis or the broker are down too, profiles
expire after 180 s: stations drop to `startW`, and once the whole site is stale, the gateway raises
them to the safe share (ADR 0006).

- Action: restart smart-charging. Optimal rounds resume on the next tick.

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
