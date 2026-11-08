import { createHttpClient, type HttpClient } from '@evcharge/platform';
import type { Config } from './config';

export interface Downstream {
  sessions: HttpClient;
  billing: HttpClient;
  telemetry: HttpClient;
}

/**
 * One platform HTTP client per downstream service (ADR 0002): shared internal token, 3 s per-attempt timeout,
 * and a circuit breaker per target. Clients do not retry by default: reads opt in through `readOpts`, writes are
 * not retried (a timeout is a 504 and the caller retries with the same idempotency key).
 */
export function createDownstream(c: Config): Downstream {
  const common = { internalToken: c.INTERNAL_TOKEN, timeoutMs: c.DOWNSTREAM_TIMEOUT_MS, retries: 0 };
  return {
    sessions: createHttpClient({ ...common, name: 'sessions', baseUrl: c.SESSIONS_URL }),
    billing: createHttpClient({ ...common, name: 'billing', baseUrl: c.BILLING_URL }),
    telemetry: createHttpClient({ ...common, name: 'telemetry-ingest', baseUrl: c.TELEMETRY_URL }),
  };
}

/** The only identity header we send downstream; always derived from the verified JWT, never from the client. */
export const asDriver = (driverId: string): Record<string, string> => ({ 'x-driver-id': driverId });

/** Per-call options for reads: short attempt timeout, at most one retry (about 3 s overall). */
export const readOpts = (c: Config) => ({ timeoutMs: c.READ_TIMEOUT_MS, retries: c.READ_RETRIES });

/** ADR 0015: sessions returns the primary's WAL position after each write in this header. */
export const COMMIT_LSN_HEADER = 'x-commit-lsn';
export const MIN_LSN_HEADER = 'x-min-lsn';
