# What the drills and reviews caught

Every item below was found during this build by a code review, a load test or a failure drill. Labels: **measured** means the number comes from a raw file in this repository (named) or a test assertion; **derived** means calculated from stated values with the formula given; **observed, not saved** means it was read from live `/metrics`, `free` or k6 output during a run and no file records it, so it cannot be reproduced from this repository; **review scenario** means a worked example from a review that was not measured on the running system.

<!-- findings:start -->
| # | Finding | Found by | Item |
|---:|---|---|---|
| 1 | Allocation command storm towards offline stations | k6 | [details](#finding-1) |
| 2 | Site cap exceeded during reallocation | review | [details](#finding-2) |
| 3 | Gateway safe share could exceed the cap, and one profile id was shared by all connectors | review | [details](#finding-3) |
| 4 | Invoices lost after a short Postgres outage | review | [details](#finding-4) |
| 5 | The outbox broke per-key event order with two replicas | review | [details](#finding-5) |
| 6 | <code>NOTIFY</code> serialised every commit | k6 | [details](#finding-6) |
| 7 | Concurrent <code>StartTransaction</code> on one connector returned 500 | k6 | [details](#finding-7) |
| 8 | <code>/v1/sites</code> returned 429,276 bytes per call | k6 | [details](#finding-8) |
| 9 | Memory pressure in the lab VM | k6 | [details](#finding-9) |
| 10 | Fleet convention drift between generators | review | [details](#finding-10) |
| 11 | Billing under-collected totals above the hold and could strand a charge | review | [details](#finding-11) |
| 12 | One bad telemetry row stopped ingest globally | review | [details](#finding-12) |
| 13 | A builder agent reported "committed" while its review fixes were still uncommitted | manual check | [details](#finding-13) |
| 14 | A late duplicate connect attempt left one station stuck after a gateway loss | drill | [details](#finding-14) |
| 15 | The gateway crashed with two Kafka brokers down | drill | [details](#finding-15) |
| 16 | OCPP deduplication returned another message's stored response after a simulator restart | drill | [details](#finding-16) |
| 17 | The first station-offline-replay run was invalid and still passed | drill | [details](#finding-17) |
| 18 | The drill harness wrote curl's "000" as a number and produced invalid JSON | drill | [details](#finding-18) |
| 19 | Allocation feedback loop: one SetChargingProfile result caused a round that resent every profile | drill | [details](#finding-19) |
| 20 | HAProxy kept stale gateway IPs | drill | [details](#finding-20) |
| 21 | The slow-Postgres drill never lifted its CPU cap | drill | [details](#finding-21) |
| 22 | Unbounded Jaeger memory | manual check | [details](#finding-22) |
| 23 | The first slow-Postgres failures were a Kafka leader move and an overloaded VM, not the slow database | drill | [details](#finding-23) |
<!-- findings:end -->

<details id="finding-1">
<summary>1. Allocation command storm towards offline stations</summary>

- **Found by:** a k6 sessions run with the simulator stopped.
- **Symptom (observed, not saved as a file):** 22,190 `SetChargingProfile` commands published and 3,144 trigger rounds within minutes while the stations were offline; Kafka sat at about 128% CPU per broker.
- **Cause:** every non-Accepted profile result (`StationOffline`, `Timeout`) re-triggered the site's round, which re-sent the same profiles.
- **Fix:** only Accepted results re-trigger. Other outcomes wait for the periodic 60 s round.
- **Regression test:** [`services/sessions/test/int/two-phase.test.ts`](../services/sessions/test/int/two-phase.test.ts) (the assertion carries the comment "A non-Accepted result must not re-trigger the site"; that comment is not a test name).

</details>

<details id="finding-2">
<summary>2. Site cap exceeded during reallocation</summary>

- **Found by:** the Opus review of `sessions`.
- **Symptom (review scenario, not measured):** cap 30 kW, station A at 22 kW. A round gives A and B 15 kW each, A's decrease is lost because the station is offline, and B's increase lands: 37 kW in force.
- **Cause:** decreases and increases were sent together, on different partitions, with no ordering; a non-Accepted result also nulled the stored limit.
- **Fix:** two-phase apply. The in-force limit is tracked, increases are sent only when the headroom covers the worst case, and an unconfirmed profile is never forgotten ([ADR 0006](adr/0006-smart-charging-allocation.md), "Applying an allocation").
- **Regression test:** [`services/sessions/test/int/two-phase.test.ts`](../services/sessions/test/int/two-phase.test.ts).

</details>

<details id="finding-3">
<summary>3. Gateway safe share could exceed the cap, and one profile id was shared by all connectors</summary>

- **Found by:** the Opus review of the gateway.
- **Symptom (review scenario, not measured):** cap 30 kW and 3 connectors with a safe share of 10 kW. A is at 20 kW, B is raised to 10 kW, C is at its start power of 4.14 kW: 34.1 kW. Separately, connector 2's profile replaced connector 1's.
- **Cause:** the safe share was raised per connector instead of per site, and every connector used the same profile id.
- **Fix:** one profile id per connector, `txProfileId(c) = 100 + c`, and the safe share is applied only when the whole site is stale (Redis key `alloc:site:{id}`, written by `sessions` before each round commits).
- **Regression tests:** [`services/station-gateway/test/int/gateway.int.test.ts`](../services/station-gateway/test/int/gateway.int.test.ts), [`simulator/test/unit/profiles.test.ts`](../simulator/test/unit/profiles.test.ts).

</details>

<details id="finding-4">
<summary>4. Invoices lost after a short Postgres outage</summary>

- **Found by:** the Opus review of `billing`.
- **Symptom (review scenario; derived from the consumer defaults: 5 attempts with full-jitter backoff capped at 200, 400, 800 and 1,600 ms, so at most about 3 s in total and about 1.5 s on average):** a Postgres blip of a few seconds dead-lettered `session.completed`, and no invoice was ever issued.
- **Cause:** handler errors were retried only a few times before the record went to the dead-letter topic.
- **Fix:** the platform consumer retries handler errors indefinitely and dead-letters only undecodable records.
- **Regression test:** [`services/billing/test/int/outage.test.ts`](../services/billing/test/int/outage.test.ts) (about 10 s with connections refused; the invoice is still issued and the DLQ stays empty).

</details>

<details id="finding-5">
<summary>5. The outbox broke per-key event order with two replicas</summary>

- **Found by:** the ponytail audit, confirmed by the Opus review.
- **Symptom (review scenario, not measured):** with two replicas, events for one key could be published out of order.
- **Cause:** every replica ran a `SKIP LOCKED` relay, and `bigserial` ids are allocated before commit.
- **Fix:** a single active relay per database (advisory lock) that publishes rows whose transaction finished before every running one, ordered by `(tx, id)` ([ADR 0002](adr/0002-contracts-events-and-delivery-semantics.md) erratum).
- **Regression test:** [`packages/platform/test/int/outbox.test.ts`](../packages/platform/test/int/outbox.test.ts) ("does not publish a later-committed row before an earlier-started one", "a second relay is a no-op").

</details>

<details id="finding-6">
<summary>6. <code>NOTIFY</code> serialised every commit</summary>

- **Found by:** a k6 sessions run, then traces and `pg_stat_activity`.
- **Symptom (observed, not saved as a file):** `COMMIT` waiting on `Lock:object`, connection pool exhaustion ("timeout exceeded when trying to connect"), and the OCPP hot path at multi-second p99.
- **Cause:** the outbox `AFTER INSERT` trigger ran `pg_notify`. Postgres takes one global lock at commit for every notifying transaction and holds it across the WAL flush.
- **Fix:** no trigger; the relay polls every 50 ms. Migration `0004_outbox_drop_notify.sql` in [`sessions`](../services/sessions/migrations/0004_outbox_drop_notify.sql) and [`billing`](../services/billing/migrations/0004_outbox_drop_notify.sql).
- **Regression test:** [`packages/platform/test/int/outbox.test.ts`](../packages/platform/test/int/outbox.test.ts) (asserts that there is no trigger and that publication happens within 1 s).

</details>

<details id="finding-7">
<summary>7. Concurrent <code>StartTransaction</code> on one connector returned 500</summary>

- **Found by:** a k6 sessions run. The error (unique violation `active_sessions_connector_pk_key`) was observed in the k6 output and not saved as a file.
- **Cause:** two transactions raced to insert the active session of one connector.
- **Fix:** `SELECT ... FOR NO KEY UPDATE OF c` on the connector row.
- **Regression test:** [`services/sessions/test/int/ocpp.test.ts`](../services/sessions/test/int/ocpp.test.ts) ("concurrent StartTransactions on one connector").

</details>

<details id="finding-8">
<summary>8. <code>/v1/sites</code> returned 429,276 bytes per call</summary>

- **Found by:** the k6 driver-api run.
- **Symptom (observed, not saved as a file):** 429,276 bytes per call for 146 sites and 1,460 stations, which capped driver-api reads at about 527 RPS.
- **Fix:** the nearest 20 sites by default (limit up to 100). Afterwards the saved run ([`k6-driver-api-reads.json`](load-output/k6-driver-api-reads.json), measured) reached 955.59 req/s (`http_reqs` rate) at p99 255.2 ms against 1,000 req/s offered. The rate is a **miss** against the 1,000 req/s target, and the p99 passes (target 300 ms). `http_reqs` includes 40 setup requests, and the scenario iterations ran at about 994.5 per second [derived: 59,669 iterations / 60 s], with 331 of 60,000 iterations dropped. The 1% dropped-iteration tolerance belongs to the k6 script; ADR 0000 does not define it.
- **Regression test:** none; the load script [`load/k6/driver-api-reads.js`](../load/k6/driver-api-reads.js) is the guard.

</details>

<details id="finding-9">
<summary>9. Memory pressure in the lab VM</summary>

- **Found by:** k6 results that degraded over time.
- **Symptom (observed with `free` in a container, not saved as a file):** swap at 1,023 MB with 1.3 MB free, the Jaeger in-memory store at 1.15 GB with 100% sampling, and Kafka GC at about 150% CPU per broker.
- **Fix:** 10% parent-based trace sampling, documented in [`docker-compose.yml`](../docker-compose.yml). That was not enough; see item 22 for the bounded Jaeger store.
- **Regression test:** none; it is a configuration change.

</details>

<details id="finding-10">
<summary>10. Fleet convention drift between generators</summary>

- **Found by:** the Opus review.
- **Symptom (review scenario, not measured):** `sessions`, the simulator and the backfill each had their own DC power rule (for example station g=24 was 50 kW in one and 150 kW in another).
- **Fix:** one convention in [`packages/contracts/src/fleet.ts`](../packages/contracts/src/fleet.ts) with a generated fixture.
- **Regression tests:** `TestFleetFixture` in [`scripts/backfill-telemetry/internal/gen/`](../scripts/backfill-telemetry/internal/gen/) and the simulator fleet test [`simulator/test/unit/fleet-demand.test.ts`](../simulator/test/unit/fleet-demand.test.ts).

</details>

<details id="finding-11">
<summary>11. Billing under-collected totals above the hold and could strand a charge</summary>

- **Found by:** the Opus review.
- **Symptom (review scenario, not measured):** invoice totals above the authorized hold were under-collected, and a charge could be stranded when the card changed in the middle of a retry.
- **Fix:** the remainder is captured with the idempotency key `capture:{invoiceId}:rest`, and the exact capture request is persisted and replayed.
- **Regression test:** [`services/billing/test/int/psp-edge.test.ts`](../services/billing/test/int/psp-edge.test.ts).

</details>

<details id="finding-12">
<summary>12. One bad telemetry row stopped ingest globally</summary>

- **Found by:** the Opus review.
- **Symptom (review scenario, not measured):** one bad row stopped ingest for everything.
- **Fix:** bisect the failed batch and dead-letter only the rejected record.
- **Regression test:** `TestPoisonRecordIsBisectedAndDeadLettered` in [`services/telemetry-ingest/internal/ingest/pipeline_integration_test.go`](../services/telemetry-ingest/internal/ingest/pipeline_integration_test.go).

</details>

<details id="finding-13">
<summary>13. A builder agent reported "committed" while its review fixes were still uncommitted</summary>

- **Found by:** checking the running compose stack (the rate limits were missing from the container environment).
- **Process fix:** every worktree is verified clean before it is merged.
- **Regression test:** none; it is a process rule.

</details>

<details id="finding-14">
<summary>14. A late duplicate connect attempt left one station stuck after a gateway loss</summary>

- **Found by:** the gateway-instance-loss drill, first run ([`gateway-instance-loss-20261108T115832Z`](drill-output/gateway-instance-loss-20261108T115832Z/result.json), **FAILED**, kept as evidence).
- **Symptom (measured, `result.json`):** 199 of 200 stations reconnected (`routes_present_after_loss` 199, `fleet_connected_after_recovery` 199, and no measurement for `stations_reconnected_within_s`).
- **Cause:** a late duplicate connect attempt completed on the server after the current one, and the gateway replaced the current socket with close code 4000. The simulator then stopped reconnecting.
- **Fix:** the simulator closes superseded sockets when they open and reconnects after a 10 to 30 s pause on close code 4000.
- **Tests:** the simulator unit tests, and the drill itself. The rerun ([`gateway-instance-loss-20261108T122353Z`](drill-output/gateway-instance-loss-20261108T122353Z/result.json), passed, measured) reconnected in 31 s with 200 of 200 routes.

</details>

<details id="finding-15">
<summary>15. The gateway crashed with two Kafka brokers down</summary>

- **Found by:** the kafka-broker-loss drill with two brokers, first run ([`kafka-broker-loss-2-20261108T123144Z`](drill-output/kafka-broker-loss-2-20261108T123144Z/result.json), **FAILED**, kept as evidence with a [`NOTE.md`](drill-output/kafka-broker-loss-2-20261108T123144Z/NOTE.md)).
- **Symptom (measured, `result.json`):** `gateway_ready_during_outage` got no HTTP answer (recorded as 0, expected 200), and 126 of 280 probe readings were missing. The produce error rate and the buffer depth checks passed (34.79 errors/s at most, buffer depth 549).
- **Cause:** the `@platformatic/kafka` consumer client emitted an unhandled `error` event ("joinGroup failed 4 times"), which crashed the process, and the restart then blocked on joining the group.
- **Fix:** the platform consumer handles client errors, rebuilds the client, and joins in the background, so `start()` no longer blocks the service.
- **Regression test:** [`packages/platform/test/int/kafka.test.ts`](../packages/platform/test/int/kafka.test.ts) ("kafka consumer without reachable brokers"). The three stations stuck before the outage (item 16) explain at most 84 of the 126 missing readings [derived: 3 × 28 readings per probe station in `probes/readings.csv`]; the remaining 42 have no named cause and are not isolated.

</details>

<details id="finding-16">
<summary>16. OCPP deduplication returned another message's stored response after a simulator restart</summary>

- **Found by:** the kafka-broker-loss drill with one broker, first run ([`kafka-broker-loss-1-20261108T122708Z`](drill-output/kafka-broker-loss-1-20261108T122708Z/result.json), **FAILED**, kept with a [`NOTE.md`](drill-output/kafka-broker-loss-1-20261108T122708Z/NOTE.md)).
- **Symptom (measured, `result.json`):** 30 of 100 probe readings were missing. All of them belonged to three stations that were stuck before the outage.
- **Cause:** `sessions` deduplicated OCPP messages on `stationId:uniqueId`. After a simulator restart the uniqueId counters restarted, so a new `StartTransaction` received the stored response of an old message, which had no `transactionId`, and the station retried forever.
- **Fix:** deduplicate on uniqueId, action and payload hash (`messageKey` in [`services/sessions/src/ocpp/ocpp.service.ts`](../services/sessions/src/ocpp/ocpp.service.ts)), and give the simulator boot-prefixed message ids.
- **Regression test:** [`services/sessions/test/int/ocpp.test.ts`](../services/sessions/test/int/ocpp.test.ts) ("uniqueId reuse after a firmware restart").

</details>

<details id="finding-17">
<summary>17. The first station-offline-replay run was invalid and still passed</summary>

- **Found by:** reading the first run of `drills/station-offline-replay.sh`.
- **Symptom:** `POST /offline` returned HTTP 400, so no station went offline, yet the drill passed. The run was deleted.
- **Fix:** the drill now checks that the stations really went offline (`stations_taken_offline`) and that the connected count dropped (`connected_dropped_during_offline`). The valid rerun ([`station-offline-replay-20261108T141434Z`](drill-output/station-offline-replay-20261108T141434Z/result.json), passed, measured) simulated 10 minutes offline at time scale 1 with 20 stations: 31 s to drain the replay queues, 0 dropped queue entries, 0 of 1,272 probe readings missing, 0 duplicate rows, and 960 replayed rows flagged. The 24 h offline replay at time scale 60 ([`station-offline-replay-20261108T151006Z`](drill-output/station-offline-replay-20261108T151006Z/result.json), **FAILED** and **inconclusive**, kept with a [`NOTE.md`](drill-output/station-offline-replay-20261108T151006Z/NOTE.md)) did not show a loss. The replay queue was still draining (140,564 messages at its peak, 89,114 when the 600 s wait ended), and then the harness restored the simulator before the queue had drained, which discarded the remaining in-memory queue and produced the 82,914 "missing" readings. That is a harness bug. The rerun that waits for the queue to drain ([`station-offline-replay-20261108T190213Z`](drill-output/station-offline-replay-20261108T190213Z/result.json), passed, measured) found 0 missing readings and 0 duplicates across 123,652 probe readings.

</details>

<details id="finding-18">
<summary>18. The drill harness wrote curl's "000" as a number and produced invalid JSON</summary>

- **Found by:** the `result.json` of the two-broker drill (item 15), which contained `"measured":000`.
- **Fix:** the harness now normalises every check value. In the kept two-broker run that one token was changed to `0` by hand so that the file parses (stated in its `NOTE.md`). The chart generator also skips and reports any malformed `result.json` instead of failing.

</details>

<details id="finding-19">
<summary>19. Allocation feedback loop: one SetChargingProfile result caused a round that resent every profile</summary>

- **Found by:** the failed slow-Postgres run ([`slow-postgres-20261108T142904Z`](drill-output/slow-postgres-20261108T142904Z/result.json), **FAILED**, kept with a [`NOTE.md`](drill-output/slow-postgres-20261108T142904Z/NOTE.md)), followed by the Prometheus metrics `sessions_charging_profiles_sent_total` and `outbox_published_total`.
- **Symptom (observed, not saved as a file):** about 140 SetChargingProfile/s published for a 20-site, 200-station fleet, with all three Kafka brokers at 140-160% CPU.
- **Cause, part 1:** every Accepted SetChargingProfile result immediately triggered a new round at the site.
- **Cause, part 2:** every round resent every session's profile at the site (the reason was site-level), so one change or one result cost one command per session.
- **Fix:** an Accepted result now makes the site due in 2 s (debounced, picked up by the tick) instead of kicking a round. Profiles are sent per session: new, changed by at least max(2%, 200 W), within 60 s of `validTo`, unconfirmed and last sent at least 30 s ago (retry), or all of them when the site is unsafe. See [ADR 0006](adr/0006-smart-charging-allocation.md).
- **Result (observed via Prometheus, not saved as a file):** about 3 profiles/s in total (changed 1.49, refresh 1.15, retry 0.37, new 0.04).
- **Regression tests:** [`services/sessions/test/unit/plan.test.ts`](../services/sessions/test/unit/plan.test.ts) ("re-sends only the session that changed", "retries an unconfirmed profile ... 30 s") and [`services/sessions/test/int/two-phase.test.ts`](../services/sessions/test/int/two-phase.test.ts).

</details>

<details id="finding-20">
<summary>20. HAProxy kept stale gateway IPs</summary>

- **Found by:** three drill reruns that ran with 0 of 200 stations connected. They were invalid and were deleted.
- **Cause:** HAProxy resolved `station-gateway-1` and `station-gateway-2` only at startup. Recreated gateway containers got new IPs, so every upgrade got 503.
- **Fix:** `resolvers docker` (127.0.0.11) with `init-addr libc,none` in [`infra/haproxy/haproxy.cfg`](../infra/haproxy/haproxy.cfg). Every drill now calls `require_fleet` and aborts if the fleet is not fully connected. See [ADR 0004](adr/0004-websocket-connection-routing.md) and the [runbook](runbook.md).
- **Regression test:** none; the guard is the `require_fleet` check in the drills.

</details>

<details id="finding-21">
<summary>21. The slow-Postgres drill never lifted its CPU cap</summary>

- **Found by:** the notes of the two failed Kafka runs ([`kafka-broker-loss-1-20261108T122708Z`](drill-output/kafka-broker-loss-1-20261108T122708Z/NOTE.md), [`kafka-broker-loss-2-20261108T123144Z`](drill-output/kafka-broker-loss-2-20261108T123144Z/NOTE.md)), whose telemetry database was still capped.
- **Cause:** `docker update --cpus 0` does not remove a limit, so the telemetry Postgres stayed at 0.05 CPU after the drill. The affected slow-Postgres run was deleted, and the two failed Kafka runs carry a note.
- **Fix:** the drill restores the Docker VM CPU count. The telemetry postgres-exporter also skips per-table collectors.
- **Regression test:** none.

</details>

<details id="finding-22">
<summary>22. Unbounded Jaeger memory</summary>

- **Found by:** the failed slow-Postgres run again: Kafka itself was slow.
- **Symptom (observed, not saved as a file):** 864 MB even at 10% sampling, with swap full again. `kafka-producer-perf-test` with acks=all at 50 msg/s gave p50 493 ms and p99 1,230 ms (observed, not saved).
- **Fix:** a Jaeger v2 config, [`infra/jaeger/config.yaml`](../infra/jaeger/config.yaml), with memory `max_traces` 20000, plus `mem_limit: 400m`. After a rolling Kafka restart the same producer test gave p50 14 ms and p99 295 ms at 50 msg/s (observed, not saved).
- **Regression test:** none; it is a configuration change.

</details>

<details id="finding-23">
<summary>23. The first slow-Postgres failures were a Kafka leader move and an overloaded VM, not the slow database</summary>

- **Found by:** a slow-Postgres run ([`slow-postgres-20261108T150304Z`](drill-output/slow-postgres-20261108T150304Z/result.json), **FAILED** on one check, kept with a [`NOTE.md`](drill-output/slow-postgres-20261108T150304Z/NOTE.md)).
- **What held (measured, `result.json` and `NOTE.md`):** ingest stalled and the backlog stayed in Kafka (peak consumer lag 133 records, below the 200-record threshold of `lag_drained_within_s`, so its value of 0 does not show a drain); the durable lag p99 was 1.37 s 90 s after recovery (target < 5 s); 0 of 2,103 probe readings were missing.
- **What failed (measured, `result.json` and `timeline.jsonl`):** the OCPP CALLRESULT p99 rose to the 10 s histogram cap during the slowdown (target 0.5 s), then fell to about 0.7 s afterwards. It was already above the target before the database was capped: 0.24-0.47 s until 15:03:44, then 0.84-0.85 s (maximum 0.849 s) from 15:03:45, before the 15:04:06 cap and the 15:04:31 leader move. That pre-fault rise is not isolated. Heartbeat, answered locally by the gateway, stayed at about 8 ms. Calls that wait for a Kafka ack and StopTransaction slowed together.
- **Cause (isolated afterwards from the broker and gateway logs, observed, not saved as a file; details in `NOTE.md`):** a Kafka preferred-leader rebalance (`auto.leader.rebalance.enable`, every 300 s) moved partition leadership at 15:04:31, 5 min after a rolling broker restart, and the gateway's in-flight produces hung for the full 30 s send timeout (50 x "kafka send timed out after 30000ms" in the gateway log, observed, not saved as a file), holding every Kafka-acked CALLRESULT. The slow database itself was not the cause.
- **Fix:** the gateway send timeout is now 3 s (`KAFKA_SEND_TIMEOUT_MS`); a hung produce fails fast with CALLERROR and the station retries the same uniqueId. This bounds the stall during a leader move at about 3 s; it does not meet the 500 ms SLO during the move.
- **Consequence:** the run stays a FAIL in the metrics table, as [ADR 0000](adr/0000-requirements-and-capacity.md) requires. It does not show that a slow telemetry database hurts the OCPP path; it shows that a partition leader move does.
- **Clean rerun:** [`slow-postgres-20261108T162708Z`](drill-output/slow-postgres-20261108T162708Z/NOTE.md) **PASSED**: it ran after a forced PREFERRED election and more than 5 min of waiting (both observed by hand, not saved in that run's `events.log`). CALLRESULT p99 was 0.076 s during the slowdown, durable lag 0.49 s 90 s after recovery, and 0 probe readings were missing (measured, `result.json`). At its default 30 s meter interval no Kafka backlog built up (peak consumer lag 0, 140 probe readings), so its `lag_drained_within_s` of 0 shows no drain, and the run does not confirm that a backlog leaves the OCPP path unaffected: both 2 s-meter runs that built a backlog failed the CALLRESULT check (150304Z with an isolated Kafka leader move, 160959Z on an overloaded VM). In 160959Z the CALLRESULT p99 was 2.48 s during the slowdown and the durable lag 9.9 s after recovery. A further run at 2 s meters ([`slow-postgres-20261108T160959Z`](drill-output/slow-postgres-20261108T160959Z/NOTE.md)) was confounded by an overloaded VM and is kept with a note.
- **Isolated reproduction:** `load/repro/leader-move.sh` ([results](load-output/leader-move-README.md)) shows the client stalling produces for up to 4.4 s on a leader move with its default retries, and about 1.3-1.9 s across the tuned runs (p99 1,317 and 1,387 ms, max 1,693 and 1,894 ms). The rest is recorded as a named library limitation in ADR 0012.

**Passing reruns.** [`kafka-broker-loss-1-20261108T140530Z`](drill-output/kafka-broker-loss-1-20261108T140530Z/result.json) passed with 0 of 96 probe readings missing. [`kafka-broker-loss-2-20261108T140952Z`](drill-output/kafka-broker-loss-2-20261108T140952Z/result.json) passed (measured): the gateway stayed live (HTTP 200 on `/health/live`), produce errors rose to 7.20/s at most, the buffer depth to 187 at most, and 0 of 254 probe readings were missing. The failed first runs stay as evidence with their notes. The gateway-instance-loss rerun passed with 31 s (item 14), and the station-offline-replay rerun passed (item 17).

**Runs before the fixes.** The reconnect-storm run ([`reconnect-storm-20261108T120311Z`](drill-output/reconnect-storm-20261108T120311Z/timeline.jsonl)) shows at most 199 connected stations of 200 in its timeline (`sim_connected`). A slow-postgres run from the same period passed its checks, but it was deleted after item 21 showed that the CPU cap was never lifted; three of the later slow-Postgres runs (items 19 and 23: 142904Z, 150304Z, 160959Z) failed, and the fourth (162708Z, item 23) passed. The reconnect-storm run passed its checks.

</details>
