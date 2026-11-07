# 0002. Contracts, events and delivery semantics

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: inbound OCPP design 2,400 msg/s; MeterValues 1,333/s peak; ~900 commands/s; billing "0 lost, 0 double charges"; telemetry loss ≤ 0.01%; instance counts (6 gateways, 2 per other service).

## Context

Seven services in two languages exchange Kafka events and internal HTTP calls. Contracts drift
silently unless one place defines them and something fails when they diverge. Kafka gives
at-least-once delivery, and the outbox relay can publish an event twice after a crash. The design
therefore has to make duplicates harmless, not try to prevent them.

## Decision

### Single source of truth

- `packages/contracts` (TypeScript + zod 4) defines every OCPP message (`ocpp.ts`), every event
  (`events.ts`), every internal HTTP body (`api.ts`), topic names and keys, consumer groups and
  Redis key conventions (`topics.ts`).
- `pnpm --filter @evcharge/contracts build` generates JSON Schema into
  `packages/contracts/schemas/`. These files are committed.
- `packages/contracts-go` mirrors the shapes the Go services need. Its `contract_test.go` marshals
  Go values and validates them against the generated JSON Schema, so `make check` fails on drift.
- `scripts/check-topics.mjs` fails when `infra/redpanda/topics.sh` diverges from `TOPIC_SPECS`.

### Envelope

Every record value is:

```json
{ "eventId": "uuid", "type": "session.completed", "version": 1, "occurredAt": "RFC3339",
  "producer": "sessions", "key": "<record key>", "data": { } }
```

The `traceparent` Kafka header carries the W3C trace context. A breaking change creates a new
topic (`*.v2`) and a period of dual publishing. Additive changes keep `version: 1`.

### Topics and keys

| Topic | Key | Partitions | Retention | Why this key |
|---|---|---|---|---|
| `station.events.v1` | stationId | 12 | 7d | Per-station order of connect, boot and status |
| `telemetry.meter-values.v1` | stationId | 24 | 24h | Per-station order; most partitions because it has the highest rate |
| `station.commands.v1` | stationId | 12 | 1h | Commands to one station are delivered in order |
| `station.command-results.v1` | stationId | 12 | 24h | Same |
| `session.events.v1` | siteId | 12 | 7d | Allocation and billing see a site's starts and stops in order |
| `billing.events.v1` | sessionId | 6 | 7d | Saga steps for one session are ordered |
| `ocpp.raw-log.v1` | stationId | 12 | 14d | Raw frames for audit and debug, a compact record without an envelope (ADR 0003) |

Partition counts cover the instance plan in ADR 0000: 12 partitions for at most 6 gateway dispatchers
and 2 `sessions` consumers, and 24 for `telemetry.meter-values` (about 56 msg/s per partition at the 1,333/s peak, with
room for 24 ingest consumers).

### Delivery semantics

- **Producers of domain events** (`sessions`, `billing`) write to an `outbox` table in the same
  transaction as the state change. A relay publishes with an idempotent producer and `acks=all`,
  then marks the row as published. A crash between publish and mark produces a duplicate, which is
  accepted.
- **Consumers** commit offsets only after the handler succeeds (at-least-once). Handlers that
  change state insert `(consumer, eventId)` into `processed_events` in the same transaction
  (inbox). A duplicate then does nothing.
- **Gateway telemetry and station events** are published straight to Kafka, without an outbox. At 1,333 MeterValues/s, an outbox would add a Postgres write per message that the gateway does not otherwise need.
  The station is the durable source: it keeps the message queued until it receives the CALLRESULT,
  and the gateway sends the CALLRESULT only after the broker acknowledges. Duplicates are
  deduplicated downstream on `(stationId, ocppMessageId)`.
- **Internal HTTP** mutations require `idempotency-key`. The server stores
  `(key, request hash) → response` for 24h. A replay returns the stored response, the same key
  with a different body returns 422, and a concurrent duplicate returns 409.
- After 5 handler failures a record goes to `<topic>.dlq` with the error in its headers, so one
  poison message cannot block a partition.

### Internal HTTP

Internal HTTP uses JSON, `application/problem+json` errors and a shared `x-internal-token`
(constant-time compare). Timeouts are set by the caller: 1 s for the OCPP hot path, 300 ms for
the smart-charging allocator, 1 s for tariff and 3 s for driver-api to downstream services.

## Rejected alternatives

- **Protobuf + Schema Registry.** It is stronger but requires codegen in two languages and another
  container. JSON Schema generated from zod with cross-language tests catches the same drift in
  this repo.
- **Kafka transactions for exactly-once.** These only cover read-process-write inside Kafka. The
  effects that matter here (a Postgres invoice, a PSP capture) are outside Kafka, so idempotent
  effects are still required, and transactions would only add latency.
- **CDC (Debezium) instead of a polling outbox.** It adds another JVM service. A `SKIP LOCKED`
  poller with LISTEN/NOTIFY wake-up gives sub-second latency at this volume.

## Consequences

- Every consumer has to be written to be idempotent. Reviewers check for this.
- An outbox row is published in tens to hundreds of milliseconds, not microseconds. The `outbox_publish_lag_seconds`
  histogram tracks this.
- When the broker is down, outbox rows accumulate in Postgres and drain after recovery. The
  broker-down drill measures this.
