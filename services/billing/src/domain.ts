/** Pure billing rules: the saga state machine, backoff and capture amounts (ADR 0007). */

export const SAGA_STATES = ['AUTHORIZING', 'AUTHORIZED', 'AUTH_FAILED', 'PRICING', 'INVOICED', 'CAPTURING', 'PAID', 'CAPTURE_FAILED'] as const;
export type SagaState = (typeof SAGA_STATES)[number];

/** States whose count is exported as billing_sagas_pending{state}. */
export const PENDING_STATES = ['AUTHORIZING', 'AUTHORIZED', 'PRICING', 'INVOICED', 'CAPTURING'] as const satisfies readonly SagaState[];

/** How many times an authorization is retried after unknown (timeout) outcomes before the saga gives up on the hold. */
export const AUTH_MAX_ATTEMPTS = 5;

export type InvoiceStatus = 'Issued' | 'Paid' | 'PaymentFailed';

/** Driver-facing invoice status derived from the saga state. */
export function invoiceStatus(state: SagaState | string): InvoiceStatus {
  if (state === 'PAID') return 'Paid';
  if (state === 'CAPTURE_FAILED') return 'PaymentFailed';
  return 'Issued';
}

/**
 * Delay before retry number `attempts` (1-based): base * 2^(attempts-1), capped at max, with
 * "equal jitter" (half fixed, half random) so a recovering dependency is not hit in lockstep.
 * Defaults give the ADR 0007 progression 1 s -> 2 s -> 4 s ... 30 s.
 */
export function retryDelayMs(attempts: number, baseMs: number, maxMs: number, random: () => number = Math.random): number {
  const exp = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempts - 1));
  return Math.floor(exp / 2 + random() * (exp / 2));
}

/**
 * Amount to capture: min(total, hold) when a hold exists (the PSP cannot capture more than it
 * authorized); the whole total for a session that has no hold (authorization failed or skipped).
 */
export function captureAmount(totalMinor: number, holdMinor: number, hasAuthorization: boolean): number {
  return hasAuthorization ? Math.min(totalMinor, holdMinor) : totalMinor;
}

/** Whether a tariff error can never succeed on retry (a 4xx other than throttling/timeouts). */
export function isPermanentHttpStatus(status: number): boolean {
  return status >= 400 && status < 500 && status !== 408 && status !== 429;
}
