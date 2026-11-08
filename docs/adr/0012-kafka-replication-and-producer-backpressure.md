# 0012. Kafka replication, producer durability and backpressure

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: billing "0 lost, 0 double charges"; telemetry loss ≤ 0.01%; gateway 99.95%;
  inbound OCPP design 2,400 msg/s; "charging never stops because the backend is down".

## Context

Every flow except the synchronous OCPP transaction calls goes through Kafka. Losing one broker must
not lose an acknowledged record or stop producers. Losing two brokers must fail loudly and
predictably, and must not corrupt data or silently drop it.

## Decision

- **Cluster.** Apache Kafka 4.3 in KRaft mode, 3 nodes, each a combined broker and controller
  (controller quorum of 3, so it tolerates 1 failure). In production, controllers would be
  dedicated. Combined roles are a lab memory trade-off.
- **Topics.** `replication.factor=3`, `min.insync.replicas=2`, `unclean.leader.election=false`,
  and auto-create disabled. Topics are created only by `infra/kafka/topics.sh` from
  `TOPIC_SPECS`.
- **Producers.** All producers use `acks=all` and `enable.idempotence=true` (no duplicates or
  reordering per partition on retry), lz4, and a linger of 5–10 ms.
  - A send is acknowledged only after at least 2 replicas have the record.
  - **Client retry tuning (amendment, 2026-11-08):** @platformatic/kafka 2.15 defaults to 3 retries with a 1,000 ms delay.
    In an isolated reproduction (load/repro/leader-move.sh, docs/load-output/leader-move-README.md) a partition leader
    move stalled produces for up to 4.4 s (p99 4,146 ms). With 10 retries and a 100 ms delay, now the platform default,
    the stall is p99 1,317 ms, max 1,693 ms, with 0 errors. A shorter delay (50 ms) and a 1 s metadata age did not
    improve on that. **Named library limitation:** this client still needs about 1.3-1.9 s to recover from a leader
    move, where clients that refresh metadata on NOT_LEADER_OR_FOLLOWER take a few hundred ms (typical behaviour of such clients, not measured here). The gateway keeps its 3 s
    send cap as the bound. Replacing the client, or contributing a fix upstream, is the way out.
  - Delivery timeout is 30 s for outbox relays. The station gateway uses 3 s (KAFKA_SEND_TIMEOUT_MS, amended
    2026-11-08): during a partition leader move its produces hung for the full 30 s and held CALLRESULTs
    (docs/drill-output/slow-postgres-20261108T150304Z/NOTE.md). After 3 s the station gets CALLERROR and retries the
    same uniqueId, which is idempotent downstream.
- **Consumers.** Offsets are committed after processing (ADR 0002). Group rebalancing uses the
  `cooperative-sticky` assignor (franz-go and `@platformatic/kafka` both support incremental
  rebalancing), so a broker or consumer restart does not stop the world for the group.

### Failure behaviour

| Event | Effect | Producers | Data |
|---|---|---|---|
| 1 broker lost | partitions it led elect a new leader from the ISR (seconds); ISR = 2 ≥ min.insync | brief retry, then continue | no acknowledged record lost |
| 2 brokers lost | ISR = 1 < min.insync | send errors (`NOT_ENOUGH_REPLICAS` by design; see the drill evidence below) | nothing is acknowledged, so nothing acknowledged is lost |
| broker recovers | catches up, rejoins the ISR | resume automatically | — |

### Backpressure when the broker cannot acknowledge (station-gateway)

The gateway answers a non-transaction CALL (`MeterValues`, `StatusNotification`, `BootNotification`)
only after the broker acknowledges the record (ADR 0002). With 2 brokers down:

1. Records wait in a bounded in-memory buffer per instance (`GATEWAY_PRODUCE_BUFFER`, default
   20,000 records, about 6 MB at 300 B).
2. While the buffer is filling, the gateway holds each CALLRESULT. The station keeps the message
   in its queue (one outstanding CALL per station), so the station's own queue is the
   backpressure, and nothing is dropped by the gateway.
3. When the buffer is full, new CALLs get CALLERROR `InternalError` ("backend busy"). The station
   retries later with the same uniqueId, and the stores are idempotent (ADR 0003).
4. Heartbeats are answered locally and never touch the broker.
5. `ocpp.raw-log` records are dropped first and counted (ADR 0003).
6. Charging continues on the profiles stations already have, and the gateway's safe-share
   fallback is local (ADR 0006).

Outbox producers (`sessions`, `billing`) need no buffer, because the outbox table is the buffer.
Rows accumulate and drain after recovery.

### Drills (`drills/`, raw output in `docs/drill-output/`)

- **One broker lost.** Kill one broker under simulator load. The drill (`drills/kafka-broker-loss.sh`) passes if:
  - the gateway answers HTTP 200 on `/health/live` during the outage (`gateway_ready_during_outage`; it checks
    liveness, not readiness);
  - the producer error rate returns to 0 within 60 s (`producer_recovery_s`, `RECOVERY_MAX_S` in the script; the 30 s
    delivery timeout above is a separate producer setting);
  - no reading of a tagged probe stream is missing in TimescaleDB (`probe_readings_missing` = 0, with
    `probe_readings_generated` at least 1), so no acknowledged message is lost.
  - Not checked: telemetry lag recovery. The drill has no check for it.
- **Two brokers lost.** The drill passes if:
  - the same four checks as above hold (gateway live, producer recovery, probe readings generated and none missing);
  - producers report send errors, and the gateway buffer grows. The drill checks the evidence it can observe: the
    maximum produce error rate (`max_produce_errors_per_s_during_outage`, at least 0.01/s) and the maximum buffer depth
    (`max_produce_buffer_depth`, at least 1). It does not check for the literal `NOT_ENOUGH_REPLICAS` code, because that code could
    not be provoked on the compose cluster (Kafka 4.3 does not enforce `min.insync.replicas` above the replication
    factor in the single-topic probe; see the station-gateway README). A unit test with a failing producer covers the
    code path;
  - after recovery, the station replay plus dedup gives zero gaps in the meter series for the probe stations
    (`probe_readings_missing`).
  - Not checked: that CALLERRORs start, that no process crashes (only the gateway's `/health/live` is polled, once
    during the outage), and that the backlog drains (it is covered only indirectly through the missing-readings check).

## Rejected alternatives

- **`acks=1`.** It has lower latency, but a leader crash before replication loses acknowledged
  records, which violates billing's zero-loss requirement.
- **RF=3 with `min.insync.replicas=1`.** It stays writable on one broker, but an acknowledged write
  can then exist on one disk only.
- **RF=2.** It cannot lose one broker and stay both writable and safe.
- **Unclean leader election.** It trades acknowledged data for availability.

## Consequences

- Producer p99 includes a replica round trip of a few milliseconds on one host (an expectation, not measured here). The load results
  report it.
- The k6 load generator runs in a container on the compose network and shares the Docker VM with the stack (the
  `generator` field of `docs/load-output/k6-*.env.json`). The memory use of the three brokers was not measured.
