# station-gateway

OCPP 1.6J WebSocket edge (NestJS + `ws`). Terminates station sockets at `/ocpp/{stationId}`, authenticates them,
translates OCPP into Kafka events and `sessions` calls, and routes commands back to the owning instance.
Governing ADRs: [0003](../../docs/adr/0003-ocpp-subset-station-auth-and-offline-replay.md),
[0004](../../docs/adr/0004-websocket-connection-routing.md), [0006](../../docs/adr/0006-smart-charging-allocation.md),
[0012](../../docs/adr/0012-kafka-replication-and-producer-backpressure.md) (and 0000, 0001, 0002).

## What it does

- Upgrade: subprotocol `ocpp1.6` (else 400), HTTP Basic (username = stationId) checked as SHA-256 in constant time against
  the credential (30 s LRU, Redis `cred:station:{id}` 5 min, `sessions`), 401 before the upgrade, token bucket
  (`GATEWAY_MAX_UPGRADES_PER_S`, charged only for stations that authenticated) and hard cap (`GATEWAY_MAX_CONNECTIONS`) answer 503 + `Retry-After`. Bad credentials draw on a separate, smaller bucket (`GATEWAY_MAX_BAD_AUTH_PER_S`), so an attacker gets 503 and good stations keep their budget. A second connection for a
  station replaces the first (old one closed with 4000, also across instances).
- Calls: Heartbeat is local. Authorize/StartTransaction/StopTransaction are one synchronous call to `sessions` (1 s, no retry,
  `idempotency-key = stationId:uniqueId`), CALLERROR `InternalError` on timeouts and 5xx. A permanent sessions 4xx is mapped: StartTransaction gets `idTagInfo.status` Invalid with `transactionId` 0, StopTransaction gets `{}` (ADR 0003: an unknown stop is accepted), Authorize gets Invalid; a 409 in_flight is retried once after about 200 ms. Boot/Status/connect/disconnect go to
  `station.events.v1`, MeterValues to `telemetry.meter-values.v1` (`replayed` when older than 60 s); the CALLRESULT is sent
  after the broker ack. A bounded produce buffer (`GATEWAY_PRODUCE_BUFFER`) turns overflow into `InternalError` (ADR 0012).
  Raw frames in both directions go to `ocpp.raw-log.v1`, dropped and counted under pressure.
- Routing: `route:station:{id}` (TTL 90 s, refreshed every 30 s, compare-and-delete on close) with the epoch in its own TTL-less key `route:epoch:{id}`, `gw:instances`
  heartbeat every 5 s, a dispatcher consumer group on `station.commands.v1` that forwards over `gw:cmd:{instance}`, one
  outstanding CALL per station (30 s timeout, queue 32), results on `station.command-results.v1`.
- Smart charging layers: TxDefaultProfile (id `DEFAULT_PROFILE_ID`, connector 0, stack 0, `startW`) after an accepted Boot. Allocation TxProfiles it
  delivered are tracked. A connector with an expired allocation is raised to the safe-share TxProfile (same id `ocpp.txProfileId(connectorId)`, stack 1,
  `safeShareW`, so it replaces that allocation) only when the WHOLE site is stale: `now > GET alloc:site:{siteId} + ALLOCATION_GRACE_S`. A missing key or
  a failed Redis read means "do not raise"; the station stays at `startW`. The sweep re-checks every second.
- `/health/live`, `/health/ready` (503 while draining or at the connection cap), `/metrics`, `/internal/stats` (internal token).
  Shutdown: readiness 503, sockets closed with 1012, producers flushed.

## Run and test

```sh
pnpm --filter @evcharge/station-gateway build
pnpm --filter @evcharge/station-gateway test:unit
make -o infra-up check                      # whole repo; integration tests use the compose infra on host ports
pnpm --filter @evcharge/station-gateway test:int
docker build -f services/station-gateway/Dockerfile .
```

Configuration is validated at start (`src/config.ts`; required: `SESSIONS_URL`, `KAFKA_BROKERS`, `REDIS_URL`, `INTERNAL_TOKEN`).
`docs/load-output/gateway-10k-connections.json` has the measured 10,000 connection run (`simulator/scripts/gateway-10k.cjs`).
NOT_ENOUGH_REPLICAS cannot be provoked on the compose cluster (Kafka 4.3 does not enforce min.insync.replicas above the replication factor), so that path is covered by a unit test with a failing producer.
Tests start real gateway instances against Redis + Kafka with a stub `sessions` HTTP server and unique topics.

Key metrics: `gateway_connected_stations`, `gateway_upgrades_total{outcome,reason}`, `ocpp_messages_total{direction,type,action}`,
`ocpp_call_duration_seconds` (CALLRESULT latency), `gateway_produce_buffer_depth`, `gateway_produce_rejected_total`,
`gateway_commands_total{action,outcome}`, `ocpp_raw_log_dropped_total`, `gateway_safe_share_applied_total`.
