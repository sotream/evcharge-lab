# 0010. Authentication: drivers, stations, services

- Status: accepted
- Date: 2026-11-08
- Driven by ADR 0000: 2,000,000 registered / 200,000 daily active drivers; driver writes 50 RPS, reads 500 RPS; reconnect storm 833 station connects/s.

## Context

The system has three kinds of callers: drivers using mobile or web apps against `driver-api`,
stations over OCPP, and services calling each other internally. Each needs authentication suited
to how it connects and how often it does.

## Decision

- **Drivers (JWT).**
  - `driver-api` registers drivers with email and password. Passwords are hashed with scrypt
    (`N=16384, r=8, p=1`, 16-byte salt, 64-byte key).
  - Login issues an access token: JWT HS256, 15 min, claims `sub=driverId`, `iss=evcharge`,
    `aud=driver-api`, `kid`.
  - It also issues a refresh token: an opaque random 256-bit value, stored hashed (SHA-256), 30 days,
    rotated on every use. Reusing an already-rotated token revokes the whole token family (theft
    detection).
  - `JWT_SECRET` is at least 32 bytes and validated at startup. `kid` allows the secret to rotate:
    the config accepts `JWT_PREVIOUS_SECRETS` for verification.
  - Login is rate-limited per IP and per email with a Redis fixed window (10 per minute).
  - On registration, `driver-api` gives the driver a random 20-character `idTag` and registers it
    in `sessions` (`POST /internal/drivers`).
- **Stations.** HTTP Basic on the WebSocket upgrade with a per-station 128-bit random secret, stored as SHA-256
  in `sessions` (a slow KDF is unnecessary for random secrets and would cost about 7 cores per gateway at the
  833 connects/s storm rate) and cached by the gateway (ADR 0003).
- **Service to service.** A shared `x-internal-token`, compared in constant time. Internal routes
  are under `/internal/*` or require the token on all of `/v1/*` for services that only internal
  callers reach (`sessions`, `billing`, `tariff`, `smart-charging`, `telemetry-ingest`). The driver
  identity is forwarded as `x-driver-id` **only** by `driver-api`, after it verifies the JWT.
  Downstream services trust that header only when the internal token is valid.

## Rejected alternatives

- **RS256/EdDSA with JWKS.** This is the right choice when several independent issuers or
  verifiers exist. Here a single service both issues and verifies, so HMAC with `kid` rotation is
  simpler and has the same security properties.
- **Sessions in Redis instead of JWTs.** That would put a Redis lookup on every request. Short JWTs
  plus revocable refresh tokens give the same logout semantics within 15 minutes.
- **mTLS between services.** It is right for production, but in compose it is PKI plumbing. The
  shared token is a deliberate lab simplification, documented in the runbook as a production gap.

## Consequences

- A stolen access token is valid for up to 15 minutes. A stolen refresh token is detected on its
  next use.
- Every downstream service must reject `x-driver-id` without a valid internal token. Each service
  has an integration test for this.
