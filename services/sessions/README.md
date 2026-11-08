# sessions

Owns the catalog (sites, stations, connectors, station credentials), reservations, charging sessions,
the idTag-to-driver mapping and power-allocation orchestration. NestJS 12 + Fastify, Postgres database
`sessions` (synchronous commit), Kafka via the transactional outbox, Redis for measured power.

Governing ADRs: [0001](../../docs/adr/0001-architecture-overview.md) (data ownership, retention),
[0002](../../docs/adr/0002-contracts-events-and-delivery-semantics.md) (outbox/inbox),
[0003](../../docs/adr/0003-ocpp-subset-station-auth-and-offline-replay.md) (OCPP-facing API, idempotency),
[0005](../../docs/adr/0005-reservation-contention.md) (reservations),
[0006](../../docs/adr/0006-smart-charging-allocation.md) (allocation),
[0010](../../docs/adr/0010-authentication.md) (internal token, `x-driver-id` trust).

## API

All routes need `x-internal-token`. Errors are `application/problem+json` with a stable `code`.

| Route | Caller | Notes |
| --- | --- | --- |
| `GET /internal/stations/:id/credential` | station-gateway | `secretSha256` + `chargingDefaults {startW, safeShareW}` |
| `POST /internal/ocpp/authorize` | station-gateway | idTag -> Accepted / Invalid / Blocked |
| `POST /internal/ocpp/start-transaction`, `/stop-transaction` | station-gateway | idempotent on `stationId:ocppMessageId` (Postgres PK in `ocpp_messages`); a replay returns the same `transactionId`; one short transaction each |
| `POST /internal/drivers` | driver-api | registers `driverId` <-> `idTag` |
| `GET /v1/sites?lat&lng&radiusKm` | driver-api | sites with stations, connector status, online flag |
| `POST /v1/reservations` (needs `idempotency-key`), `GET`/`DELETE /v1/reservations/:id` | driver-api | see below |
| `POST /v1/sessions/remote-start` (needs `idempotency-key`), `GET /v1/sessions/:id`, `POST /v1/sessions/:id/stop`, `GET /v1/drivers/:id/sessions` | driver-api | commands go out through the outbox |

Driver routes also need `x-driver-id`. It is honoured only after the internal token was verified
(class-level `InternalTokenGuard` runs before `DriverGuard`), so a spoofed header without the token gets 401.

## Reservations (ADR 0005)

`reservations` is partitioned by month on `starts_at`. Each partition carries
`EXCLUDE USING gist (connector_pk WITH =, period WITH &&) WHERE status IN ('Pending','Active')` and a partial
unique index on `driver_id` for open reservations. `23P01` maps to `409 connector_unavailable`. A constraint
trigger covers the month boundary (rows that overlap two partitions). `starts_at` is truncated to
milliseconds so that the value stored by the command link round-trips through JavaScript `Date`.
`Pending` + `ReserveNow` outbox command are written in one transaction; the command result makes it
`Active` or `Rejected`; a sweeper (5 s, `FOR UPDATE SKIP LOCKED`) expires it.

## Allocation (ADR 0006)

A round runs per site on every start/stop (`sites.alloc_due_at = now()`) and every 60 s (each site has its own
phase). A site is claimed with `FOR UPDATE SKIP LOCKED` plus a 15 s lease, so replicas never run the same site
at once. The round builds an `AllocateReq` (idle connectors, `startW`, measured power from Redis), calls
smart-charging (300 ms timeout, circuit breaker), validates the answer against the cap from its own state, and
falls back to `safeShareW` per session (`fallback-equal-share`) when the call fails or the answer would exceed
the cap. `SetChargingProfile` (TxProfile, stack level 1, `validTo` = computedAt + 180 s) uses a two-phase apply, because
commands to different stations have no ordering and a decrease may never be applied. Per session the service keeps
the confirmed limit (command result `Accepted`) and an envelope of unconfirmed profiles that may be in force
(`active_sessions.confirmed_*`, `possible_*`; after expiry the gateway's safe share counts). A round sends decreases
and refreshes at once and an increase only if `cap - idle reserve - sum(worst case in force)` has room; withheld
increases wait for the next round, which a `SetChargingProfile` result triggers (`sites.alloc_deferred`). A
non-Accepted result never lowers what is assumed in force. A profile is (re)sent when its target differs from the
last sent by at least max(2 %, 200 W), is new, or is within 60 s of expiry; all of a site's profiles share one
`validTo`. `computedAt` is sent in the `AllocateReq`.

Load: rounds from the periodic tick and from start/stop kicks share `ALLOCATION_CONCURRENCY` (kicks go through a
deduplicated per-site queue); the tick loops again at once after a full batch; `sessions_allocation_backlog_seconds`
shows the oldest due site. A failed round reschedules the site in 5 s and keeps its trigger. 5,000 sites with 15,000
sessions cycle in about 7 s with 2 instances (`docs/load-output/sessions-allocation-rounds.json`). Rounds take locks in
the same order as Start/StopTransaction (`active_sessions`, then `sites`) to avoid deadlocks. Metric:
`sessions_reallocation_latency_seconds`. The sweeper also rejects Pending reservations whose ReserveNow got no
result 10 s after the command expired.

## Seed convention (shared with the simulator)

On first start with an empty `sites` table: sites `site-0001`.., stations `st-<site#4>-<n#3>`, 2 connectors each.
Station `g` (0-based over the fleet) is DC for exactly `SEED_DC_SHARE` of stations (AC 11/22 kW, DC 50-150 kW,
by `g`); site cap is 40-60 % of the summed connector maximum. Station secret =
`HMAC-SHA256(STATION_SECRET_SEED, stationId)` hex truncated to 32 chars; the database stores its SHA-256.

## Configuration

Validated at startup (`src/config.ts`); the process exits with the list of bad variables. Required:
`INTERNAL_TOKEN`, `DATABASE_URL`, `KAFKA_BROKERS`, `REDIS_URL`, `SMART_CHARGING_URL`, `STATION_SECRET_SEED`.
Optional: `SEED_SITES`, `SEED_STATIONS_PER_SITE`, `SEED_DC_SHARE`, `ALLOCATION_*`, `SWEEPER_INTERVAL_MS`,
`SMART_CHARGING_TIMEOUT_MS`, `WORKERS_ENABLED`, `PG_POOL_MAX`.

## Run and test

```sh
pnpm --filter @evcharge/sessions build
pnpm --filter @evcharge/sessions test:unit          # no infra
pnpm --filter @evcharge/sessions test:int           # compose infra: Postgres, Redis, Kafka (see make check)
docker build -f services/sessions/Dockerfile .      # image (context = repo root)
```

Integration tests start the whole service against a temporary database and unique Kafka topics, with a stub
smart-charging server. They cover: 50 concurrent reservations (1 x 201, 49 x 409), idempotent start/stop
(including concurrent duplicates), header spoofing, outbox rows reaching Kafka, fallback allocation, and the cap
invariant over random start/stop churn.

Health: `/health/live`, `/health/ready`; metrics: `/metrics`.
