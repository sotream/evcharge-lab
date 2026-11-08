import type { Rng } from './util';

export const FIRST_ATTEMPT_MAX_MS = 10_000;
export const BACKOFF_BASE_MS = 10_000;
export const BACKOFF_CAP_MS = 60_000;

/**
 * Reconnect policy (ADR 0003, as firmware should): the first attempt after a drop is uniform in
 * 0..10 s; later attempts use exponential backoff with full jitter, capped at 60 s. A server
 * `Retry-After` is a floor.
 *   attempt 0: U(0, 10 s)      attempt n >= 1: U(0, min(60 s, 10 s * 2^(n-1)))
 */
export function reconnectDelayMs(attempt: number, rng: Rng = Math.random, retryAfterMs = 0): number {
  const ceil = attempt <= 0 ? FIRST_ATTEMPT_MAX_MS : Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** (attempt - 1));
  return Math.max(Math.floor(rng() * ceil), retryAfterMs);
}
