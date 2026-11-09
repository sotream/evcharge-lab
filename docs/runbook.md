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
- Action: restore Postgres. Everything resumes without manual replay. If the primary is gone for good, see "OLTP Postgres failover" below: promote the replica (RPO is not 0 with the asynchronous replica, ADR 0018).

### OLTP Postgres failover (`drills/postgres-failover.sh`)

Run it from the repository root. It destroys the primary, so it ends by rebuilding the stack from empty volumes (at least 2 minutes: `down -v`, `up -d`, fleet reconnect, then a 120 s settle).

```
MODE=auto   SYNC=0 STALL_S=3 drills/postgres-failover.sh   # script promotes and repoints; async replication, replica cut off 3 s before the kill
MODE=auto   SYNC=1 STALL_S=3 drills/postgres-failover.sh   # same with synchronous_standby_names = 'FIRST 1 (replica_1)'
MODE=manual SYNC=0 STALL_S=0 drills/postgres-failover.sh   # you promote: the script prints four commands and waits
```

**How to read this output.** The run writes `docs/drill-output/postgres-failover-<mode>-<sync>-stall<N>-<stamp>/`. Start with `timeline.txt`, also printed at the end:

- Rows are 2 s buckets relative to the kill (0 = the moment `docker kill` returned). `db-acks` is how many commits the database probe got acknowledged in the bucket (about 180 when healthy). `drv ok/rej/err` is what the reservation drivers saw: accepted / refused (409, normal under contention) / errors and timeouts.
- Look for the **hole**: a bucket where `db-acks` drops to 0 is the write outage. In the sync run with a partition it starts before the kill, because the primary cannot commit without its standby. In the async run there is no hole before the kill and the acknowledgements in that stretch are the ones that get lost.
- Look for `drv err` after the database is back: a column of errors with `db-acks` already healthy is the driver-api circuit breaker (opens after 5 failures, stays open 10 s).
- The last lines give DB RTO, the longest write gap, driver RTO, acknowledged probe commits lost, and the error counts.

**What a pass looks like.** `result.json` checks: `rto_db_write_s` and `rto_driver_reservation_s` at most 60 s (auto only; a manual run reports them without a threshold), `acknowledged_probe_writes_lost` = 0, `history_checker_violations` = 0 (the measured value is the checker's exit code: 0 pass, 1 violation, 2 bad input; the count is in `history/check-report.txt`; the checker is ADR 0019) and the fleet reconnected. In the lab an async run with `STALL_S=3` is **expected to fail** the two loss checks: that is the finding of ADR 0018. A sync run passes them.

**Knobs.** `SYNC=1` turns on the synchronous standby (no loss, writes block while the standby is unreachable). `STALL_S` is how long the replica is cut off before the kill (0 = clean kill). `openMs` and `failureThreshold` of the driver-api breaker (`packages/platform/src/http-client/circuit-breaker.ts`) decide how long drivers keep seeing 503 after the database is back. `MANUAL_WAIT_S` is how long manual mode waits for you.

**By hand, outside the drill.** If the primary is really gone: `docker compose exec postgres-replica psql -U evcharge -d postgres -c "select pg_promote()"`, then move the name `postgres` to the replica: `docker network disconnect <net> <replica-container>` and `docker network connect --alias postgres --alias postgres-replica <net> <replica-container>`. Services reconnect by themselves. There is no replica afterwards and nothing stops the old primary from coming back, so do not start it.

### Backup and restore (`drills/backup-restore.sh`)

Takes a physical backup of the whole instance with `pg_basebackup -X stream`, keeps the load running for 60 s, restores into a separate container on host port 25435, and checks the restored copy. The live stack is not touched. Raw output: `docs/drill-output/backup-restore-<stamp>/`.

- **Read `summary.json`:** `backupMs`, `restoreTotalMs`, and `dataAgeS` (how much newer the source was than the restored copy at the disaster moment: the data a restore from this backup cannot bring back). `rowsLostPerTable` shows it in rows.
- **What a pass looks like:** `restore_instance_ready` = 1 and the 9 integrity checks in `result.json` are 0 (`restored_counts_within_backup_window`, `reservation_overlaps_in_restored_db`, `invoices_total_differs_from_lines`, `sessions_with_more_than_one_invoice`, `invoices_minus_registry_rows`, `payments_amount_differs_from_invoice`, `payments_without_invoice`, `payments_minus_psp_captures_sum`, `sessions_stopped_before_started`) (each table's count lies between the source counts before and after the backup).
- **Knob:** `BACKUP_DELAY_S` (how long the load runs after the backup, which sets the data age). To keep the restored container for inspection run with `KEEP=1`, then `psql -h localhost -p 25435 -U evcharge -d sessions`.
- **Not covered:** point-in-time recovery. A base backup brings back the state at its end and nothing after; WAL archiving would be the next step.

### Reservation invariant under faults (`drills/reservation-invariant.sh`)

```
FAULT=pause   drills/reservation-invariant.sh    # sessions frozen for PAUSE_S (default 15 s)
FAULT=restart drills/reservation-invariant.sh    # sessions restarted
MUTATE=1 FAULT=none drills/reservation-invariant.sh   # the protection is switched off: the checker MUST fail
```

- **Read `history/check-report.txt`.** The first line counts operations: acknowledged (accepted by the system), rejected (refused, normal), indeterminate (the client gave up; some of these did take effect, the report says how many were found in the database). Then `RESULT: PASS` or a list of violations: `acknowledged overlap` (two acknowledged reservations held one connector at the same time), `acknowledged reservation lost` (missing, on another connector, or its id was reissued to a different reservation) and `db-row overlap` (overlapping rows in the dump itself).
- **What a pass looks like:** the normal runs end `PASS`; the `MUTATE=1` run passes only if the checker reports violations (`checker_detects_mutation` = 1) and the constraints were put back (`constraints_restored`).
- **Knobs:** `DRIVERS`, the connector pool size (`CONNECTORS` of `load/history/generate.mjs`) and the think time set how hard the connectors are contended. `PAUSE_S` sets the freeze.
- The checker is not a linearizability check. ADR 0019 lists what it does not prove.

### Consumer group rebalance (`drills/consumer-rebalance.sh`)

Starts a second telemetry-ingest instance (host port 28096), kills it with SIGKILL, restarts it, then stops the whole group, moves the committed offsets back by `REPLAY_SHIFT` (200) records per partition and starts it again. If the simulator does not already run with a 2 s meter interval it recreates it that way, and puts the default back at the end.

- **Read `timings.json`:** `killToCoordinatorDropsMemberS` (about 45 s: the session timeout of the dead member, partitions idle meanwhile), `killToSurvivorOwnsAll24S`, `restartToTwoMembersOwnAll24S`, the lag peak and the time to drain. `group.jsonl` is the raw coordinator view (members and assigned partitions per poll). `summary.json` has the redelivery counts.
- **What a pass looks like:** both rebalances within 60 s, 0 probe readings missing, 0 duplicate rows, and `forced_redelivery_records_suppressed` at least 1 (4,800 = 24 x `REPLAY_SHIFT` were observed in both runs: the unique index swallowed every duplicate). A kill alone usually redelivers nothing (offsets are committed after the flush), so that number is informational.
- **Knob:** the consumer's session timeout decides the 45 s; `METER_INTERVAL_S` sets the volume.

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

## Pulling images from GHCR

Every push to `main` publishes the seven service images and the simulator to GitHub Container Registry (ADR 0020).
The names are public and lowercase: `ghcr.io/sotream/evcharge-lab-<name>` with `<name>` one of `station-gateway`, `sessions`,
`smart-charging`, `tariff`, `billing`, `telemetry-ingest`, `driver-api`, `simulator`. Tags are the short commit SHA and `main`.

```sh
docker pull ghcr.io/sotream/evcharge-lab-sessions:main
```

`docker-compose.yml` still builds the images locally by default (`docker compose up -d --build`); the registry images are
for running a single service without building it.

## Running load from another machine

Use this to take the generator off the stack's machine (for example a MacBook Air M1 against the M4).

- **Ports k6 needs.** Compose already publishes them on all interfaces, so nothing to change. One port per
  script: 28081 sessions (`sessions-ocpp`, `reservation-contention`), 28082 driver-api (`driver-api-reads`),
  28084 smart-charging, 28085 tariff, 28087 station-gateway-1 (`gateway-ws`).
- **Do not expose** Postgres (25432, 25433), Redis (26379), Kafka (29091-29093) or Grafana (23000) to the
  LAN, and do not forward any port on the router.
- **Firewall.** If the macOS firewall is on (System Settings > Network > Firewall), allow incoming
  connections for Docker. The script's preflight lists any port it cannot reach.
- **Address of the stack's machine:** `ipconfig getifaddr en0` (Wi-Fi), or its `.local` name
  (`scutil --get LocalHostName` plus `.local`). Prefer the IP when k6 runs in Docker.
- **Run from the generator** (repo checkout, plus `k6` or Docker):

  ```
  TARGET=192.168.1.20 load/run-remote.sh tariff
  ```

  Extra arguments go to k6, for example `-e DURATION=30s`. An unreachable port exits 2 before k6 starts.
- **Result:** `docs/load-output/remote/<name>-<UTC stamp>/` with the k6 summary JSON, `stdout.txt`, `ping.txt`
  (round trip to the target) and `env.json` (`generator: "remote"`, generator hardware, k6 version, target).
- These numbers include the network hop and a different generator. Never put them in the same chart series
  as local runs (`docs/load-output/k6-*.json`).

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
