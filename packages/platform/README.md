# @evcharge/platform

Shared production plumbing for every Node service (station-gateway, sessions, billing, driver-api,
simulator): config, logging, tracing, metrics, health, graceful shutdown, a resilient HTTP client,
internal auth, Postgres (migrations, transactions), transactional outbox, idempotent consumers,
HTTP idempotency keys, Kafka and Redis. CommonJS, TypeScript strict, NestJS 12 + Fastify.

## Typical `main.ts`

```ts
// instrument.ts: imports nothing but the telemetry entry point.
import { startTelemetry } from '@evcharge/platform/telemetry';
export const telemetry = startTelemetry({ serviceName: 'sessions' }); // no-op if OTEL_EXPORTER_OTLP_ENDPOINT unset

// main.ts: './instrument' MUST be the first import (instrumentations patch pg/ioredis/http on first require).
import { telemetry } from './instrument';
import { baseConfigSchema, bootstrapService, createPgPool, csv, loadConfig, pgReadinessCheck, registerReadinessCheck, runMigrations } from '@evcharge/platform';
import { z } from 'zod';
import { AppModule } from './app.module';

async function main() {
  const config = loadConfig(baseConfigSchema.extend({ DATABASE_URL: z.string(), KAFKA_BROKERS: csv() }));
  const pool = createPgPool(config.DATABASE_URL, { applicationName: config.SERVICE_NAME });
  await runMigrations(pool, `${__dirname}/../migrations`);
  registerReadinessCheck('postgres', pgReadinessCheck(pool));

  const svc = await bootstrapService({
    module: AppModule,
    config,
    telemetry, // shut down last
    onShutdown: [{ name: 'pg', run: () => pool.end() }],
  });
  svc.lifecycle.onShutdown('outbox-relay', () => relay.stop()); // registered later => stopped earlier
}
void main();
```

## API by module

| Module | Exports |
| --- | --- |
| **config** | `loadConfig(schema, env = process.env)` validates and coerces, treats `''` as unset, and throws `ConfigError` listing *every* bad variable (`.issues: string[]`). `baseConfigSchema` covers SERVICE_NAME, LOG_LEVEL=info, HTTP_PORT=8080, INTERNAL_TOKEN (min 16), OTEL_EXPORTER_OTLP_ENDPOINT?, SHUTDOWN_GRACE_MS=15000. Helpers: `csv()` and `envBool()`. Type: `BaseConfig`. |
| **logging** | `createLogger({ service, level?, destination? })` returns a pino JSON logger. Every line has `service`, plus `traceId`/`spanId` when a span is active. authorization, password, token, x-internal-token, cookie, secret, apiKey and similar fields are redacted up to 3 levels deep. `NestLoggerAdapter` implements Nest's `LoggerService`. |
| **telemetry** (`@evcharge/platform/telemetry`) | `startTelemetry({ serviceName, endpoint?, instrumentations?, ignorePaths? })` returns `{ enabled, shutdown() }`. It starts the NodeSDK with an OTLP/HTTP trace exporter (`<endpoint>/v1/traces`) and instrumentations for http, undici (fetch), fastify (`@fastify/otel`), pg and ioredis. Idempotent; no-op without an endpoint. Kafka helpers: `injectTraceHeaders(headers?, ctx?)`, `extractTraceContext(headers, ctx?)`, `currentTraceparent()`, `inSpan(name, {kind, parent, attributes}, fn)`. |
| **metrics** | `registry` is the shared prom-client registry. `initMetrics({ service })` adds the `service` default label and default metrics. `counter/histogram/gauge(cfg)` get-or-create on the registry. `registerHttpMetrics(fastify)` adds `http_server_requests_seconds{method,route,status}` (route template; `unmatched` for 404s) and `GET /metrics`. `LATENCY_BUCKETS`. |
| **health** | `HealthModule.forRoot(registry?)` serves `GET /health/live` and `GET /health/ready`. Ready returns 200, or 503 with `{status, draining, checks:{name:{status,durationMs,error?}}}`. `registerReadinessCheck(name, fn, { timeoutMs=2000 })` returns an unregister function; a check fails on throw, `false` or timeout. `HealthRegistry`, `defaultHealth`. |
| **lifecycle** | `bootstrapService({ module, config, port?, host?, logger?, telemetry?, onShutdown?, drainDelayMs?, health?, handleSignals?, exit?, fastify? })` returns `{ app, fastify, url, logger, lifecycle, health, shutdown() }`. It builds a Fastify Nest app wired with logging, metrics, health and the INTERNAL_TOKEN provider, then listens. On SIGTERM/SIGINT it marks draining (ready returns 503), waits the drain delay (default min(2s, grace/3)), closes HTTP (`app.close()`, which runs Nest hooks), runs hooks in reverse registration order, and exits 0 (1 on hook failure or deadline). The whole sequence is bounded by SHUTDOWN_GRACE_MS. A second signal exits at once. `Lifecycle` class (`onShutdown(name, fn)`). |
| **http client** | `createHttpClient({ name, baseUrl, timeoutMs=2000, retries=2, internalToken?, backoffBaseMs=50, backoffMaxMs=1000, circuitBreaker?: {failureThreshold=5, openMs=10000} \| false, headers?, fetch? })` provides `request/get/post/put/patch/delete`, which return `{status, headers, body}`. Behavior: per-attempt `AbortSignal.timeout`; full-jitter exponential backoff; retries only on network errors, timeouts, 5xx and 429, and only for GET/HEAD or requests with an `idempotencyKey`; honors Retry-After (capped). Errors: `HttpError` (`status`, `problem: ProblemDetails` parsed from problem+json), `HttpTimeoutError`, `HttpNetworkError`, `CircuitOpenError`, all `HttpClientError`. `client.circuit` (`CircuitBreaker`: `state`, `isOpen`) lets callers serve fallbacks. Metrics: `http_client_requests_seconds{target,method,outcome}` and `http_client_retries_total{target}`. |
| **internal auth** | `InternalTokenGuard` checks `x-internal-token` against INTERNAL_TOKEN with sha256 plus `timingSafeEqual` and returns 401 otherwise. `bootstrapService` provides the token globally, so `@UseGuards(InternalTokenGuard)` is all a service needs. `InternalAuthModule.forRoot(token)` is for custom setups. `safeTokenEqual`. |
| **postgres** | `createPgPool(url, { max=10, statementTimeoutMs=5000, connectTimeoutMs=3000, idleTimeoutMs, idleInTxTimeoutMs=30000, applicationName, logger })`. `pgReadinessCheck(pool)`. `withTx(pool, fn, { isolation? })`. `runMigrations(pool, dir)` applies `NNNN_name.sql` in order, each file in its own transaction with its `schema_migrations` row, under `pg_advisory_lock`. It rejects edited files (checksum) and bad names, and returns the files applied. Types `Pool`, `PoolClient`, `Queryable`. |
| **outbox** | `OUTBOX_DDL` (table, partial indexes, NOTIFY trigger) to paste into a migration. `enqueueOutbox(txClient, { topic, key?, event, headers? })` records the traceparent. `new OutboxRelay({ pool, producer, batchSize=100, pollIntervalMs=1000, listen=true, retentionMs=24h, cleanupIntervalMs=60s })` exposes `start()`, `stop()`, `runOnce()`, `cleanup()` and `refreshPending()`. Each cycle runs `SELECT … FOR UPDATE SKIP LOCKED LIMIT n`, produces, waits for acks, then marks rows published in the same transaction. Metrics: `outbox_pending`, `outbox_published_total{topic}`, `outbox_publish_lag_seconds`. |
| **inbox** | `PROCESSED_EVENTS_DDL`. `withInbox(txClient, consumerName, eventId, fn)` returns `{duplicate:true}` or `{duplicate:false, result}`. `purgeProcessedEvents(db, olderThanMs)`. |
| **idempotency** | `IDEMPOTENCY_KEYS_DDL`. `hashRequest(method, path, body)` uses canonical JSON. `withIdempotency(pool, {scope, key, requestHash, lockTtlMs=60s}, fn(txClient) => {status, body})` is atomic: the stored response commits with the side effects. Same key and hash replays; a different hash throws `IdempotencyMismatchError` (422); in flight throws `IdempotencyInFlightError` (409); a thrown fn releases the key; a claim older than lockTtlMs is taken over. Building blocks: `beginIdempotent`, `completeIdempotent`, `releaseIdempotent`, `purgeIdempotencyKeys`. Nest: `IdempotencyModule.forRoot({ pool, required? })` plus `@UseInterceptors(IdempotencyInterceptor)` replays status and body with `idempotent-replayed: true` and maps errors to 422/409. The interceptor stores the response *after* the handler, not in its transaction; use `withIdempotency` when that matters. |
| **kafka** | `makeEnvelope(type, key, data, producer, { eventId?, occurredAt? })` creates a uuid v4 and ISO time. `createProducer({ brokers, clientId, compression='lz4', lingerMs=5, maxBatchSize=500, timeoutMs, sendTimeoutMs=15s, autocreateTopics=false })` is idempotent with acks=all. It provides `send(msgs)`, which resolves on broker ack; `sendEnvelope(topic, env, {headers})` (JSON value, key=env.key, `traceparent`/`event-type`/`event-id` headers, PRODUCER span); `readinessCheck()`; and `close()`. `createConsumer({ brokers, groupId, topics, schema, handler, concurrency=8, maxAttempts=5, retryBackoffMs, dlqProducer?, startFrom='earliest', lagIntervalMs=15s, maxBuffered=1000 })` provides `start()`, `stop()`, `readinessCheck()` and `refreshLag()`. Metrics: `kafka_consumer_messages_total{group,topic,outcome}`, `kafka_consumer_handler_seconds`, `kafka_consumer_lag{group,topic,partition}`, `kafka_producer_messages_total`. |
| **redis** | `createRedis(url, { connectTimeoutMs=3000, commandTimeoutMs=2000, maxRetriesPerRequest=2, logger, options })`, `redisReadinessCheck(client)` (PING), `closeRedis(client)`. |

### Kafka consumer semantics

- **At-least-once.** An offset is committed only after the handler succeeded, or after the record
  was written to `<topic>.dlq`. Handlers must be idempotent: use `withInbox`.
- **Ordering.** Records of one partition are handled strictly in order. Up to `concurrency`
  partitions run in parallel. Retries happen in place (with backoff), so they block only their partition.
- **Poison messages.** JSON or schema failures go to the DLQ immediately (`dlq-reason: decode`).
  Handler failures go there after `maxAttempts` (`dlq-reason: handler`). DLQ records keep the original key, value
  and headers, plus `dlq-error`, `dlq-attempts`, `dlq-source-topic/partition/offset` and `dlq-consumer-group`.
  If the DLQ publish itself fails it is retried and the offset is not committed.
- **Stop.** Fetching pauses, unstarted buffered records are dropped (they are redelivered later),
  in-flight handlers finish, commits flush and the consumer leaves the group.
- **DLQ topics must exist.** The test cluster has `auto.create.topics.enable=false`, so create
  `<topic>.dlq` next to every consumed topic (topic-init job or admin client).

### Deviations and known gaps

- `@platformatic/kafka` has no producer linger setting on `send()`. `KafkaProducer` buffers itself
  for `lingerMs` or `maxBatchSize`, then issues one produce request per batch. `send()` can loop on
  metadata for unknown topics, so the wrapper applies an overall `sendTimeoutMs`. After that timeout
  the request may still complete, so a later retry can duplicate it (at-least-once).
- Offsets are committed via `message.commit()`, which the library coalesces monotonically per
  partition. During a rebalance a commit can fail; that is logged, and the new owner re-processes.
- Outbox with several relays: SKIP LOCKED guarantees no row is published twice concurrently and
  none is lost. **Strict per-key ordering across concurrently running relays is not guaranteed**:
  run a single relay per service when that matters, or rely on consumer-side versioning.
- `@opentelemetry/instrumentation-fastify` is deprecated, so the Fastify-maintained `@fastify/otel`
  is used instead (registered via the Fastify initialization diagnostics channel).
- NestJS 12 is ESM-only. This package is CommonJS and loads it through Node's `require(esm)`.
- Migrations do not support statements that cannot run in a transaction (`CREATE INDEX CONCURRENTLY`).

## Testing

```sh
pnpm --filter @evcharge/platform test:unit   # no infra needed
make infra-up                                 # compose Postgres, Redis, Kafka
pnpm --filter @evcharge/platform test:int
```

Integration tests read `TEST_PG_URL` (default `postgres://evcharge:evcharge@localhost:25432`),
`TEST_REDIS_URL` (`redis://localhost:26379`) and `TEST_KAFKA_BROKERS` (comma list, default
`localhost:29091,localhost:29092,localhost:29093`). Each test file creates and drops its own temporary
database, and creates unique topics (plus `.dlq`) with the admin client, using RF = min(3, brokers).
