# 0003. OCPP 1.6J subset, station authentication and offline replay

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: 50,000 stations; reconnect storm 833 connects/s; inbound OCPP design rate 2,400 msg/s;
  station offline window up to 24 h; OCPP CALLRESULT p99 < 500 ms; OCPP raw log 14 GB/day for 14 days.

## Context

Real stations speak OCPP 1.6J over WebSocket. They go offline for up to 24 h (ADR 0000), keep
transaction messages in a local queue, and replay that queue after reconnecting. Sometimes they
retry a message whose CALLRESULT was lost, which produces a duplicate. The CSMS has to
authenticate up to 833 reconnecting stations per second, handle duplicates, and answer the
transaction CALLs within p99 500 ms.

## Decision

### Message subset

`packages/contracts/src/ocpp.ts` is normative.

- Station → CSMS: `BootNotification`, `Heartbeat`, `StatusNotification`, `Authorize`,
  `StartTransaction`, `StopTransaction`, `MeterValues`.
- CSMS → Station: `RemoteStartTransaction`, `RemoteStopTransaction`, `ReserveNow`,
  `CancelReservation`, `SetChargingProfile`, `TriggerMessage`.
- Other actions get the CALLERROR `NotImplemented`. A malformed frame gets `FormationViolation`.
  A payload that fails its schema gets `TypeConstraintViolation` or `ProtocolError`.
- `BootNotification.conf.interval = 300` s, the heartbeat period from ADR 0000. 50,000 / 300 gives
  167 heartbeats/s fleet-wide. The meter sample interval is 30 s (`Timing` in the contracts).
- The gateway allows one outstanding CSMS → Station CALL per station, as OCPP requires, with a
  30 s response timeout. Further commands queue in memory per connection, bounded at 32. When the
  queue overflows, the oldest command completes with outcome `Error`.

### Hot-path budget (CALLRESULT p99 < 500 ms)

`Authorize`, `StartTransaction` and `StopTransaction` call `sessions` synchronously. Each call has
a single 1 s timeout and no retry inside the gateway: the station's own retry is the retry. A
timeout or 5xx returns CALLERROR `InternalError`, and the station resends with the same uniqueId.
p99 is measured against the 500 ms target in `load/`.

### Connection and authentication

- Stations connect to `ws://<edge>/ocpp/{stationId}` with subprotocol `ocpp1.6`. A connection
  without the subprotocol gets HTTP 400.
- They authenticate with HTTP Basic (OCPP security profile 1): username = stationId, password is a
  per-station **128-bit random secret** (32 hex chars).
- **Fast hash, not a KDF.** `sessions` stores `SHA-256(secret)`, and the gateway compares hashes
  in constant time. A slow KDF such as scrypt (about 50 ms of CPU) exists to protect
  low-entropy human passwords. A 128-bit random secret cannot be brute-forced from its SHA-256.
  At the 833 connects/s storm rate, scrypt would cost about 42 CPU-seconds per second across the
  fleet, about 7 cores per gateway instance. SHA-256 costs microseconds. Driver passwords still use
  scrypt (ADR 0010).
- Credential lookup is a 30 s in-process LRU, then Redis (`cred:station:{id}`, 5 min TTL), then
  `sessions` `GET /internal/stations/:id/credential`. A storm therefore mostly hits Redis, and a
  cold Redis is bounded by `sessions` read capacity. The flood drill measures this. The response
  also carries `chargingDefaults` (ADR 0006).
- Seeded credentials are deterministic for the lab: `secret = HMAC-SHA256(STATION_SECRET_SEED,
  stationId)` as hex, truncated to 32 characters. The simulator can then compute them. In
  production the secrets would be provisioned per station.
- A second connection for a stationId that is already connected **replaces** the first. The newer
  one wins. The old socket is closed with code 4000, and the registry epoch increases (ADR 0004).

### Offline replay and dedup

- While offline (up to 24 h) or unacknowledged, a station queues `StartTransaction`,
  `StopTransaction` and `MeterValues`. It replays them in order with their **original uniqueId**,
  one CALL at a time, as OCPP requires.
- **Duplicates are made harmless where state lives. The gateway does not cache them:**
  - `StartTransaction` and `StopTransaction` are idempotent in `sessions` (Postgres unique key
    `${stationId}:${uniqueId}`). The replay returns the same transactionId.
  - Meter values are deduplicated in telemetry-ingest on `(station, connector, timestamp)`
    (ADR 0008).
  - StatusNotification is applied last-write-wins on the station timestamp.
- A `MeterValues` reading whose station timestamp is more than 60 s old is marked
  `replayed: true`, so allocation does not treat it as live load.
- A `StopTransaction` for an unknown transactionId is accepted (the CALLRESULT carries no
  `idTagInfo`) and logged. Rejecting it would make the station retry forever.

### Raw message log

Every frame in either direction is published to `ocpp.raw-log.v1` as a compact
`OcppRawLogRecord`: key stationId, lz4, 14-day retention, without an envelope. Publishing is
fire-and-forget with a bounded buffer. When the broker is slow, raw-log records are dropped and
counted (`ocpp_raw_log_dropped_total`) and never delay a CALLRESULT. Sizing: about 550 msg/s ×
300 B gives 14 GB/day, and 196 GB for 14 days before compression [derived, ADR 0000]. A broker
topic fits this better than Postgres, because the data is append-only, time-bounded and read only
for debugging.

### Reconnect storm protection

- Reconnect policy (simulator, as firmware should): after an abnormal close or code 1012, the
  first attempt is at uniform random 0–10 s. Later attempts back off exponentially with full
  jitter, capped at 60 s. When one of 6 instances is lost, about 8,333 stations reconnect within
  10 s, about 833/s spread over 5 instances (about 167/s each). The SLO "routable within 60 s"
  holds if each instance admits at least 170 upgrades/s.
- Gateway admission control: a token bucket on upgrades per instance
  (`GATEWAY_MAX_UPGRADES_PER_S`, default 300, above the 167/s need). It returns HTTP 503 with
  `Retry-After: 1–5` when exhausted, and enforces a hard cap of `GATEWAY_MAX_CONNECTIONS`
  (default 15,000, 1.5× the per-instance target). The flood drill checks the 60 s target.

## Rejected alternatives

- **scrypt or argon2 for station secrets.** About 7 cores per instance during a storm, for no
  security gain on random secrets.
- **A gateway-side Redis dedup cache of CALLRESULTs for 24 h.** At peak, 1,333 MeterValues/s ×
  86,400 s is about 115 M keys/day, around 17 GB of Redis, to duplicate guarantees that the
  stores already give.
- **Security profile 2/3 (TLS client certificates).** This is what production should use, but in
  compose it is mostly PKI plumbing. Profile 1 exercises the same lookup and caching path.
- **Rejecting duplicates.** Stations retry until they get a CALLRESULT, so a rejection becomes an
  infinite retry.
- **Storing the raw log in Postgres.** 14 GB/day of write-once debug data would compete with
  session writes (RPO 0).

## Consequences

- Every store downstream of the gateway must be idempotent on its natural key. Reviewers check
  this.
- The simulator implements the station side of this ADR: queue, replay with original uniqueIds,
  backoff and storm mode. The drills use it.
