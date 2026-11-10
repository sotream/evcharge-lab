# driver-api

The only public entry point for drivers. It authenticates drivers (email and password, JWT) and proxies
their calls to `sessions`, `billing` and `telemetry-ingest` with a verified identity. NestJS 12 + Fastify,
Postgres database `drivers`, Redis for the rate limits.

Governing ADRs: [0000](../../docs/adr/0000-requirements-and-capacity.md) (SLOs: 99.9 %, read p99 < 300 ms,
write p99 < 800 ms, >= 1,000 read RPS per instance),
[0001](../../docs/adr/0001-architecture-overview.md) (data ownership),
[0002](../../docs/adr/0002-contracts-events-and-delivery-semantics.md) (internal HTTP, idempotency keys, timeouts),
[0005](../../docs/adr/0005-reservation-contention.md) (a reservation 409 passes through),
[0010](../../docs/adr/0010-authentication.md) (authentication design).

## API

Errors are `application/problem+json` with a stable `code`. All routes except `/v1/auth/*` need
`Authorization: Bearer <accessToken>`. "Key" means a required `Idempotency-Key` header (<= 200 chars),
passed on to the downstream service.

| Route | Key | Downstream | Notes |
| --- | --- | --- | --- |
| `POST /v1/auth/register` `{email, password}` | no | sessions `POST /internal/drivers` | 201 `{driverId, idTag, email}`; 409 `email_taken`; 429 (10 per minute per IP); password 8-128 chars |
| `POST /v1/auth/login` `{email, password}` | no | | 200 `{accessToken, tokenType, expiresIn, refreshToken, driverId}`; 401; 429 with `retry-after` |
| `POST /v1/auth/refresh` `{refreshToken}` | no | | rotates; a token replayed after the 5 s grace window revokes the family (401) |
| `POST /v1/auth/logout` `{refreshToken}` | no | | 204; revokes the token's family, idempotent |
| `GET /v1/me` | | | `{driverId, email, idTag, createdAt}` |
| `PUT /v1/me/payment-method` `{token}` | yes | billing `PUT /v1/drivers/:id/payment-method` | 204 |
| `GET /v1/sites?lat&lng&radiusKm` | | sessions `GET /v1/sites` | radius <= 100 (default 5), rounded up to 1/2/5/10/25/50/100 km; cached 3 s in process |
| `POST /v1/reservations` `{stationId, connectorId}` | yes | sessions | 409 `connector_unavailable` passes through |
| `DELETE /v1/reservations/:id` | yes | sessions | |
| `POST /v1/sessions` `{stationId, connectorId}` | yes | sessions `POST /v1/sessions/remote-start` | remote start, 202 |
| `POST /v1/sessions/:id/stop` | yes | sessions | 202 |
| `GET /v1/sessions/:id` | | sessions | sessions answers 404 for another driver's session |
| `GET /v1/sessions?limit&before` | | sessions `GET /v1/drivers/:id/sessions` | `{sessions}`, newest first |
| `GET /v1/sessions/:id/power?step=5m` | | sessions, then telemetry-ingest | `{sessionId, latest, series}`; `latest` is null before the first reading |
| `GET /v1/invoices?limit&before` | | billing | `{items, nextBefore}`; the driver is always the token's |
| `GET /v1/invoices/:id` | | billing | another driver's invoice is 404 |
| `GET /health/live`, `/health/ready`, `/metrics` | | | ready checks Postgres only |

## Authentication (ADR 0010)

- Passwords: scrypt N=16384 r=8 p=1, 16-byte salt, 64-byte key, stored as `scrypt$N$r$p$salt$key`.
  An unknown email costs the same scrypt as a wrong password and gets the same answer.
- Access token: HS256, 15 min, claims `sub`, `iss=evcharge`, `aud=driver-api`, header `kid` (first 8 hex of
  SHA-256 of the secret). `alg` is pinned to HS256. `JWT_SECRET` must be >= 32 bytes; `JWT_PREVIOUS_SECRETS`
  (comma separated, each >= 32 bytes) are accepted for verification only. Rotate by moving the old secret there.
- Refresh token: 256 random bits, stored as SHA-256, 30 days, rotated on every use inside one transaction
  (`SELECT ... FOR UPDATE`). Presenting a rotated or revoked token revokes the whole family, except inside a
  5 s grace window (`REFRESH_GRACE_S`) after the rotation: that is almost certainly a client that lost our
  response (or two tabs), so it gets another successor in the same family instead. Tokens are stored only as
  hashes, so the second successor is a new token, not a replay of the first; both stay valid.
- Login rate limit: Redis fixed window, 10 per minute per IP and per email (`LOGIN_RATE_LIMIT`,
  `LOGIN_RATE_WINDOW_S`); registration has its own per-IP limit (`REGISTER_RATE_LIMIT`, default 10 per minute).
  Both fail open if Redis is down (`driver_api_rate_limiter_errors_total`), so Redis is not a readiness check.
  Registration reports 409 `email_taken` (the account exists check is not hidden); the per-IP limit is what
  bounds email enumeration.
  **Trade-off:** the per-email counter counts every attempt, so anyone can lock a known email out of login for the
  rest of the window by sending wrong passwords (a targeted denial of service). It is accepted because the lockout
  lasts at most a minute, refresh tokens keep existing sessions alive, and per-email counting is what stops
  distributed password guessing. A production system would add a captcha or a device-bound exemption.
- `TRUST_PROXY` is a hop count (default `0`: the socket address is the client IP) or a list of proxy IPs/CIDRs.
  `true` is rejected. With one hop only the last `x-forwarded-for` entry (appended by our proxy) is used, so a
  client cannot pick its own rate-limit identity by prefixing the header.
- Registration calls sessions first, outside any database transaction (one attempt plus one retry, 1 s each), and
  inserts the account afterwards, so a slow sessions cannot pin database connections. sessions is idempotent on
  the driverId. If the local insert then fails (duplicate email race, crash) the idTag stays registered in sessions
  under a random driverId that has no account: that orphan is harmless and never used. A failed sessions call
  leaves nothing locally, so the client may retry.
- The driver id sent downstream comes only from the verified token. Client `x-driver-id` and `x-internal-token`
  headers are dropped, and every downstream header set is built from scratch.
- scrypt runs on the libuv threadpool; the image sets `UV_THREADPOOL_SIZE=16` (default 4) so a login burst does
  not queue behind four hashes or starve DNS and file IO.

## Downstream calls

Platform HTTP client per target with a circuit breaker each. Reads: 1.2 s per attempt, at most one retry
(`READ_TIMEOUT_MS`, `READ_RETRIES`), about 3 s overall. Writes: one attempt, 3 s, never retried on timeout; the
caller gets 504 and retries with the same `Idempotency-Key`. Mapping: a downstream 4xx keeps its status, and its
`code` and `detail` are shown only for an allow-listed set of codes (`connector_unavailable`, `forbidden`, ...);
anything else becomes `downstream_rejected` with a generic message. A downstream 401 becomes 502 (our own
service credentials are wrong); 5xx 502; timeout 504; unreachable or open circuit 503 with `retry-after`.

`GET /v1/sites` is shared map data: lat/lng are rounded to 3 decimals (~110 m), the radius up to a fixed step, answers are cached in process for
`SITES_CACHE_TTL_MS` (3 s) and concurrent misses share one downstream call.

## Configuration

`DATABASE_URL`, `REDIS_URL`, `SESSIONS_URL`, `BILLING_URL`, `TELEMETRY_URL`, `INTERNAL_TOKEN`, `JWT_SECRET`
are required. Optional: `JWT_PREVIOUS_SECRETS`, `ACCESS_TOKEN_TTL_S`, `REFRESH_TOKEN_TTL_S`, `LOGIN_RATE_LIMIT`,
`LOGIN_RATE_WINDOW_S`, `REGISTER_RATE_LIMIT`, `REGISTER_RATE_WINDOW_S`, `REDIS_KEY_PREFIX`, `TRUST_PROXY`,
`DOWNSTREAM_TIMEOUT_MS`, `READ_TIMEOUT_MS`, `READ_RETRIES`, `REGISTER_SESSIONS_TIMEOUT_MS`, `REFRESH_GRACE_S`,
`SITES_CACHE_TTL_MS`, `PG_POOL_MAX`, plus the platform's `HTTP_PORT`, `LOG_LEVEL`, `SHUTDOWN_GRACE_MS`,
`OTEL_EXPORTER_OTLP_ENDPOINT`.

## Metrics

`http_server_requests_seconds`, `driver_api_auth_total{op,outcome}`, `driver_api_token_verify_total{outcome}`,
`driver_api_sites_cache_total{result}`, `driver_api_rate_limiter_errors_total`, and the platform's
`http_client_requests_seconds{target,...}`.

## Tests

```sh
pnpm --filter @evcharge/driver-api test:unit   # JWT, scrypt, rate limiter, config, cache
pnpm --filter @evcharge/driver-api test:int    # needs Postgres and Redis (make infra-up); stub sessions/billing/telemetry
```

## Account deletion (`DELETE /v1/me`)

Driver JWT and an `idempotency-key` are required. The call asks sessions to block the driver's idTag (409 `active_session` while a session is Active or Stopping), asks billing to drop the stored card token and the tokens of settled sagas, then deletes the credentials and refresh tokens here. It answers 204 and can be repeated safely.

Invoices, sessions and billing records are **not** deleted: they are kept for the legal retention period (ADR 0000, "Data volume"), still keyed by the opaque `driver_id` and no longer linked to an email or a working idTag.
