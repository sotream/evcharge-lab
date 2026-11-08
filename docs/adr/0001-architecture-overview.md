# 0001. Architecture overview

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: the whole load model; instance counts come from its "Lab targets" table.

## Context

evcharge-lab is the charge point operator (CPO) side of an EV charging platform. Stations
connect over long-lived WebSockets with OCPP 1.6J. Drivers find stations, reserve a connector,
start and stop sessions, and get billed. Each site has a hard power cap that a smart charging
engine splits across active sessions. Meter values stream in all the time.

The project is a system design lab. Each service has to answer one concrete question:

| Service | Language | System-design question | ADR |
|---|---|---|---|
| `station-gateway` | NestJS | How do you hold thousands of long-lived WebSockets across N instances and route a command to the one instance that owns a station? How do you survive reconnect storms and offline replay? | [0003](0003-ocpp-subset-station-auth-and-offline-replay.md), [0004](0004-websocket-connection-routing.md), [0012](0012-kafka-replication-and-producer-backpressure.md) |
| `sessions` | NestJS | How do you take reservations under contention without double-booking a connector? How do you run session state machines driven by unreliable station messages? | [0005](0005-reservation-contention.md) |
| `smart-charging` | Go | How do you split a hard site cap across sessions fairly, fast and safely, and what happens when the allocator is down? | [0006](0006-smart-charging-allocation.md) |
| `tariff` | Go | How do you price a session deterministically (integer money, time-of-use boundaries, rounding)? | [0011](0011-tariff-model.md) |
| `billing` | NestJS | How do you get an exactly-once *effect* (one invoice, one capture) out of at-least-once delivery, and how do you compensate across services? | [0007](0007-billing-saga-and-exactly-once-effect.md) |
| `telemetry-ingest` | Go | How do you absorb a high, bursty write rate of meter values with dedup and bounded memory? | [0008](0008-telemetry-storage.md), [0012](0012-kafka-replication-and-producer-backpressure.md) |
| `driver-api` | NestJS | How do you authenticate drivers and compose calls to downstream services with timeouts, retries and idempotency? | [0010](0010-authentication.md) |
| `simulator` | Node | How do you produce realistic load from thousands of stations, including floods, offline periods and replays? | [0003](0003-ocpp-subset-station-auth-and-offline-replay.md), [0013](0013-traffic-generators-simulator-and-backfill.md) |

## Decision

### Topology

```
 stations (simulator) ──WS/OCPP──▶ station-lb (HAProxy, leastconn)
                                     │
                       ┌─────────────┴─────────────┐
                 station-gateway-1           station-gateway-2      ◀── Redis: station→instance registry,
                       │  sync HTTP (Start/Stop/Authorize/credential)       per-instance command channel, dedup
                       ▼
                    sessions ──HTTP──▶ smart-charging (Go, stateless allocator)
                       │ outbox
                       ▼
   Kafka (3 brokers, RF 3): station.events · telemetry.meter-values · station.commands · station.command-results
             session.events · billing.events · ocpp.raw-log (14 d)
                       │                         │
                    billing ──HTTP──▶ tariff (Go)   telemetry-ingest (Go) ──▶ postgres-telemetry (Timescale) + Redis(latest)
                       │ outbox
 drivers ──HTTPS/JWT──▶ driver-api ──HTTP──▶ sessions, billing, telemetry-ingest
```

### Data ownership

There is one database per service. The OLTP databases (`sessions`, `billing`, `tariff`, `drivers`) share one
Postgres 18 instance. `telemetry` has its own TimescaleDB instance, because its write volume and relaxed durability must
not touch OLTP (ADR 0008). No service reads another service's database. Data
crosses service boundaries only through the contracts in `packages/contracts` (ADR 0002): Kafka
events and internal HTTP. Sharing one OLTP instance is a lab simplification. The boundary is the
database, so splitting instances later only changes connection strings.

`sessions` owns the catalog (sites, stations, connectors, per-station credentials), reservations,
sessions and the idTag→driver mapping. `driver-api` owns driver accounts. `billing` owns payment
methods, authorizations, invoices and the saga. `tariff` owns tariffs. `telemetry-ingest` owns the
meter-value time series.

### Communication rules

1. Station → CSMS messages that need a synchronous OCPP answer (Authorize, StartTransaction,
   StopTransaction) are synchronous HTTP calls from the gateway to `sessions`, with a 1 s timeout (ADR 0000: CALLRESULT p99 < 500 ms)
   and idempotency keys. When they fail, the station gets a CALLERROR and retries, which OCPP
   stations already do for transaction messages.
2. Everything else from stations (status, boot, meter values) is published to Kafka by the gateway.
   The gateway answers the CALL after the broker has acknowledged the record.
3. Commands to stations are Kafka records on `station.commands.v1`. They reach the owning gateway
   instance through the Redis registry (ADR 0004).
4. Domain events (`session.events`, `billing.events`) are written through a transactional outbox.
   Every consumer is idempotent on `eventId` through an inbox table.
5. Every outbound call has a timeout. Retries are bounded, use jittered backoff and are idempotent.

### Ports (host side; checked free with `lsof` on 2026-11-08)

| Port | What |
|---|---|
| 25432 | Postgres (OLTP) |
| 25433 | Postgres + TimescaleDB (telemetry) |
| 26379 | Redis |
| 29091 / 29092 / 29093 | Kafka brokers 1–3 (external listeners) |
| 28080 | station-lb (OCPP WebSocket edge); 28099 HAProxy stats |
| 28081 | sessions |
| 28082 | driver-api |
| 28083 | billing |
| 28084 | smart-charging |
| 28085 | tariff |
| 28086 | telemetry-ingest |
| 28087 / 28088 | station-gateway-1 / -2 (direct, for metrics and drills) |
| 28089 | simulator control API |
| 29090 / 23000 / 26686 / 24318 | Prometheus / Grafana / Jaeger UI / OTLP HTTP |

Inside the compose network every service listens on `:8080`. Every service exposes
`/health/live`, `/health/ready` and `/metrics` on that port.

### Capacity plan (from ADR 0000)

| Service | Fleet peak need | Per-instance lab target | Production instances (N+1) | Compose instances |
|---|---|---|---|---|
| station-gateway | 50,000 conns, 2,400 msg/s | ≥ 10,000 conns, ≥ 2,000 msg/s | 6 | 2 (behind HAProxy) |
| sessions | 45 starts/s + 50 writes/s + ~115 allocations/s | ≥ 200 commands/s | 2 | 1 |
| smart-charging | ~115 allocations/s | ≥ 1,000/s, p99 < 20 ms | 2 | 1 |
| tariff | ~100 calculations/s | ≥ 2,000/s, p99 < 20 ms | 2 | 1 |
| telemetry-ingest | 1,333 rows/s | ≥ 2,500 rows/s | 2 | 1 |
| driver-api | 500 read RPS + 50 write RPS | ≥ 1,000 read RPS, p99 < 300 ms | 2 | 1 |
| billing | 15 invoices/s peak | none (not in ADR 0000); not measured, no billing load run exists | 2 | 1 |

The lab measures one instance per service. Fleet numbers are `[derived]` from the measured
per-instance figure. Every service is stateless apart from its database, so N+1 is a replica count,
not a redesign.

**Durability.** `sessions` and `billing` commit with `synchronous_commit=on` (RPO 0 on a single
node). Production needs a synchronous standby for RPO 0 to survive node loss, which is a documented gap
in the runbook. Telemetry relaxes to `synchronous_commit=off` per connection (ADR 0008, RPO ≤ 5 min).

**Long retention.** Sessions (117 GB/year) and invoices (10 years, append-only) live in tables
partitioned by month (`PARTITION BY RANGE (started_at / issued_at)`), so old partitions can be
detached to cold storage without a rewrite.

### Delivery order

1. Contracts, scaffold, compose, `make check` (this stage).
2. Wave 1, in parallel worktrees: station-gateway + simulator; sessions + reservations;
   smart-charging; tariff; billing + telemetry-ingest.
3. Wave 2: driver-api + auth, plus reviewers of wave 1 against the contracts and ADRs.
4. Wave 3: load tests, failure drills, charts, runbook, README.

## Rejected alternatives

- **A single modular monolith.** It is easier to run, but it would hide the questions this lab is
  about: cross-service consistency, routing to stateful connections, and fallback when a dependency
  is down.
- **Kafka for StartTransaction.** An async request/reply over Kafka for a call that a station
  blocks on adds two broker hops and correlation state, and it still needs a timeout. Synchronous
  HTTP with idempotency keys is simpler and has the same failure semantics, because the station
  retries either way.
- **gRPC between services.** This would mean one more toolchain (protoc) across two languages. The
  internal call volume is low. JSON over HTTP, validated by zod and JSON Schema contract tests, is
  enough.
- **A separate Postgres instance per service.** It adds memory on an 8 GB Docker VM and teaches
  nothing beyond what database-per-service on a shared instance already shows.

## Consequences

- `sessions` sits on the hot path of StartTransaction and StopTransaction, so its availability
  directly limits starting new sessions. Telemetry and status do not depend on it.
- The gateway depends on the broker for every non-transaction message. ADR 0004 and the
  broker-down drill show the behaviour: stations queue, and then replay.
- Database-per-service means queries that span services (such as an invoice joined with a session)
  are answered by composing APIs in `driver-api`, not by SQL joins.
