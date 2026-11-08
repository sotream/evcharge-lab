# 0017. Consistency models: what each piece of state guarantees

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000. This is the index. Mechanisms are described in the ADRs linked in the table and are not repeated here.

## Context

The system mixes a relational store with strong guarantees, an event log with at-least-once delivery, a time-series store, and a
cache that can be stale. Each piece of state needs a stated model, the anomaly it prevents, and a bound on how stale a reader can
be. Bounds are labelled: **measured** (from a file under `docs/drill-output/` or an ADR measurement), **derived** (from a formula
on constants in the code), or **to be measured**.

## Decision: the table

| State | Consistency model | Mechanism | Anomaly prevented | Staleness bound | ADR |
|---|---|---|---|---|---|
| Reservations | Serializable outcome for one connector's time range | Postgres exclusion constraint (`btree_gist`), partial unique index of one open reservation per driver | double booking, lost update between concurrent reservers; the loser gets `23P01`, HTTP 409 | none: decided in the committing transaction. The 15 min reservation TTL is a business limit, not staleness | [0005](0005-reservation-contention.md) |
| Session lifecycle and OCPP transactions | Linearizable per session row in Postgres, idempotent per OCPP transaction | state transitions in one transaction, message-id dedup for replays, outbox row in the same transaction as the state change | duplicate sessions on station retry or replay, a state change without its event | none for the row. The event reaches Kafka after the outbox publish: relay polls every 50 ms (`packages/platform/src/outbox/outbox.ts`) [derived]; the publish lag itself is the histogram `outbox_publish_lag_seconds` and is not saved in any drill output [to be measured] | [0002](0002-contracts-events-and-delivery-semantics.md), [0003](0003-ocpp-subset-station-auth-and-offline-replay.md) |
| Billing sagas and invoices | Exactly-once effect over at-least-once delivery | inbox `(consumer, eventId)`, unique business keys (`invoice.session_id`, `capture:{invoiceId}:rest`), idempotency keys to the PSP, persisted saga state | double charge, lost invoice, duplicate capture after a timeout | eventual: the invoice follows the `session.completed` event by the consumer lag. During a Postgres blip the handler retries and does not dead-letter (ADR 0002). Billing consumer lag: to be measured | [0007](0007-billing-saga-and-exactly-once-effect.md), [0002](0002-contracts-events-and-delivery-semantics.md) |
| Allocations (smart charging) | Postgres is the source of truth. Redis `alloc:site` is a monotonic fence that errs only on the safe side | two-phase apply with in-force limits; `alloc:site:{siteId}` holds the newest `validTo` (epoch ms) that may be in force, raised before each round by a Lua max (24 h key TTL) and never lowered; the gateway raises to safe share only when `now > alloc:site + ALLOCATION_GRACE_S`; a Redis failure during a round aborts it, and an unreadable key means the gateway does not raise | the sum of limits above the site cap during reallocation, a connector raised while another allocation is still live | a stale or missing `alloc:site` means no raise (stations stay at `startW`). A lost decrease lasts until the old profile's `validTo`: at most `ALLOCATION_VALIDITY_S` = 180 s [derived; constant in `packages/contracts/src/ocpp.ts`]. The grace is `ALLOCATION_GRACE_S` = 5 s, a contract constant, periodic reallocation `PERIODIC_REALLOCATION_S` = 60 s [derived: constants] | [0006](0006-smart-charging-allocation.md) |
| Redis route registry (`route:station:{id}`) | Best effort, epoch-fenced. Not a source of truth: the open socket is | `{instance, connectedAt, epoch}` with TTL 90 s refreshed every 30 s. The epoch comes from `INCR` on a separate TTL-less key `route:epoch:{id}`. Close and refresh are epoch-checked compare-and-delete or compare-and-extend in Lua | a late close of an old socket deleting the route of the new one | up to 90 s after a gateway dies without cleanup [derived: `ROUTE_TTL_S`]; a command to a dead instance fails as offline after at most 15 s without an instance heartbeat [derived: `INSTANCE_DEAD_AFTER_S`, `services/station-gateway/src/dispatcher.ts`]. In that window a command can hit a dead route, fails as offline or times out, and the sender retries | [0004](0004-websocket-connection-routing.md) |
| Telemetry | At-least-once into Kafka, deduplicated into Timescale (effectively once per `(station, connector, time)`). Eventual for readers | the station keeps the message until CALLRESULT, the gateway answers after the broker acknowledges, unique index `(station_id, connector_id, time)` | lost readings on a broker or gateway failure, duplicates on replay | durable lag p99 0.94 s in the ADR 0008 bench row offered 2,500 rows/s (achieved 2,471 rows/s; the ADR 0000 peak is 1,333 rows/s), and 1.37 s 90 s after the slow-Postgres drill (target < 5 s), both from ADR 0008 [measured]. The Redis latest value `power:{station}:{connector}` has a 5 min TTL and can lag by the ingest lag | [0008](0008-telemetry-storage.md), [0012](0012-kafka-replication-and-producer-backpressure.md) |
| Driver API reads from the sessions read replica | Bounded staleness, with read-your-writes by LSN | reads go to the replica. A write returns the primary's LSN, and a later read waits for the replica to replay it, or falls back to the primary | a driver not seeing the reservation or session they just created | replica lag 0 s in steady state and up to 239.4 s (peak `sessions_replica_lag_seconds`; the largest stale-read age seen by the probe was 239.6 s) under an injected replay pause, drained 9 s after release [measured: `docs/drill-output/replica-lag-20261108T183638Z`]. Read-your-writes waits at most `REPLICA_RYW_TIMEOUT_MS` = 200 ms (poll 20 ms), then reads the primary; 0 violations in 624 checks in that drill. The LSN tracker lives in each driver-api process, so the rule holds per driver-api instance only | [0015](0015-read-replica-and-read-your-writes-for-sessions.md) |
| Outbox to Kafka | Per-key ordering, at-least-once | one relay per database (`pg_try_advisory_xact_lock`), rows published in `(tx, id)` order only when older than every running transaction, idempotent producer with `acks=all` | event order inversion for one key with two relay replicas, an event without its state change | relay poll 50 ms [derived]; publish lag `outbox_publish_lag_seconds` [to be measured]. During a Kafka outage rows accumulate and drain after recovery: see `outbox_pending` in the drill timelines | [0002](0002-contracts-events-and-delivery-semantics.md), [0012](0012-kafka-replication-and-producer-backpressure.md) |
| Idempotency keys | Linearizable per key in Postgres | one row per `(scope, key)` with a request hash and a stored response, an in-flight lock with a TTL (60 s default, `lockTtlMs`), same key with a different body gives 422 | a retried POST doing its effect twice, a key reused for a different request | none for a completed key. An in-flight duplicate gets a conflict instead of a stale answer | [0002](0002-contracts-events-and-delivery-semantics.md), [0007](0007-billing-saga-and-exactly-once-effect.md) |

A row that says "none" has no stale read. A row with a bound says what an honest reader may see.

## Replication

- **Postgres.** Each service owns one primary. `sessions` has an asynchronous read replica for driver reads (ADR 0015, read
  replica), which is why those reads carry the LSN rule above. No synchronous replica or automatic failover is configured, so a
  primary loss is an outage of that service.
- **Kafka.** RF 3, `min.insync.replicas=2`, `acks=all`, idempotent producers, no unclean election. The failure table and drills are
  in [0012](0012-kafka-replication-and-producer-backpressure.md). The metadata quorum is in [0016](0016-consensus-kraft-quorum.md).
- **Redis.** One node with `appendonly yes` and no replica or Sentinel. **This is a stated gap.** Its data is rebuildable or safe
  by default: gateways refresh routes within 30 s, a missing `alloc:site` means no raise, and the latest-power value is a cache.
  A Redis loss degrades command routing and the safe-share raise, and it does not corrupt Postgres state. Adding asynchronous
  replication would shorten the outage but could lose the most recent `alloc:site` raise on failover, so it would need the raise to
  be re-published after a failover. That is not built.

## Partitioning

Covered elsewhere, so only pointers:

- **Kafka keys and partition counts:** [0002](0002-contracts-events-and-delivery-semantics.md). Keys give per-key order inside one
  partition. The partition count (12, 24 for telemetry, 6 for billing) caps consumer parallelism per group: extra consumers sit idle.
- **Command routing to the owning gateway:** [0004](0004-websocket-connection-routing.md).
- **Postgres monthly partitions:** reservations and sessions ([0005](0005-reservation-contention.md)), invoices
  ([0007](0007-billing-saga-and-exactly-once-effect.md)), meter readings ([0008](0008-telemetry-storage.md)).
- **What this ADR adds:** there is no order across keys or across topics, and consumers must not assume one. Two sessions at one
  site are kept within the cap by the two-phase allocation rule (ADR 0006), not by Kafka order.

## Rejected alternatives

- **One global consistency level.** Strong everywhere would put Postgres consensus on the telemetry path (1,333 msg/s at peak,
  ADR 0000). Eventual everywhere would allow double bookings and double charges.
- **Distributed transactions across services.** The saga plus inbox gives the same business effect without two-phase commit.

## Consequences

- A new piece of state adds a row to the table, with a bound labelled measured, derived or to be measured.
- Rows marked "to be measured" are open work: the outbox publish lag (`outbox_publish_lag_seconds`) and the billing consumer lag (`kafka_consumer_lag`). The replica-lag bound was measured by the drill above.
