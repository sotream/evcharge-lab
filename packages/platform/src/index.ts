/**
 * @evcharge/platform — shared production plumbing for evcharge Node services.
 * Telemetry must start before anything instrumented is loaded: import it from
 * `@evcharge/platform/telemetry` at the very top of main.ts, then load this entry point.
 */
export * from './config/config';
export * from './logging/logger';
export * from './telemetry/index';
export * from './metrics/metrics';
export * from './health/health';
export * from './lifecycle/lifecycle';
export * from './lifecycle/bootstrap';
export * from './http-client/circuit-breaker';
export * from './http-client/errors';
export * from './http-client/http-client';
export * from './auth/internal-token.guard';
export * from './postgres/postgres';
export * from './outbox/outbox';
export * from './inbox/inbox';
export * from './idempotency/idempotency';
export * from './idempotency/idempotency.interceptor';
export * from './kafka/envelope';
export * from './kafka/producer';
export * from './kafka/consumer';
export * from './redis/redis';
export { TimeoutError, sleep } from './util';
