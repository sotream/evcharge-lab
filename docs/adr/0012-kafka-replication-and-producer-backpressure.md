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
  - Delivery timeout is 30 s, then the error surfaces to the caller.
- **Consumers.** Offsets are committed after processing (ADR 0002). Group rebalancing uses the
  `cooperative-sticky` assignor (franz-go and `@platformatic/kafka` both support incremental
  rebalancing), so a broker or consumer restart does not stop the world for the group.

### Failure behaviour

| Event | Effect | Producers | Data |
|---|---|---|---|
| 1 broker lost | partitions it led elect a new leader from the ISR (seconds); ISR = 2 ≥ min.insync | brief retry, then continue | no acknowledged record lost |
| 2 brokers lost | ISR = 1 < min.insync | `NOT_ENOUGH_REPLICAS` on every send | nothing is acknowledged, so nothing acknowledged is lost |
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

- **One broker lost.** Kill one broker under simulator load. Pass if:
  - the producer error rate returns to 0 within 30 s;
  - the received count matches the produced count for a tagged probe stream, so no acknowledged
    message is lost;
  - telemetry lag recovers.
- **Two brokers lost.** Pass if:
  - producers report `NOT_ENOUGH_REPLICAS`;
  - the gateway buffer grows, then CALLERRORs start (backpressure is visible);
  - no process crashes;
  - after recovery, the backlog drains, and the station replay plus dedup gives zero gaps in the
    meter series for the probe stations.

## Rejected alternatives

- **`acks=1`.** It has lower latency, but a leader crash before replication loses acknowledged
  records, which violates billing's zero-loss requirement.
- **RF=3 with `min.insync.replicas=1`.** It stays writable on one broker, but an acknowledged write
  can then exist on one disk only.
- **RF=2.** It cannot lose one broker and stay both writable and safe.
- **Unclean leader election.** It trades acknowledged data for availability.

## Consequences

- Producer p99 includes a replica round trip of a few milliseconds on one host. The load results
  report it.
- Three brokers use about 2 GB of the 8 GB Docker VM. Load generators run on the host to keep the
  measurement honest.
