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

/**
 * Splits a gross amount into net and VAT. Tariff prices are gross (B2C price display, PAngV), so the VAT is the
 * share rateBp / (10,000 + rateBp) of the gross, rounded half up to the minor unit; net is the rest, so
 * net + vat is always exactly the gross. rateBp is in basis points (1900 = 19%).
 */
export function splitVat(grossMinor: number, rateBp: number): { netMinor: number; vatMinor: number } {
  if (!Number.isSafeInteger(grossMinor) || grossMinor < 0) throw new RangeError(`gross must be a non-negative integer, got ${grossMinor}`);
  if (!Number.isSafeInteger(rateBp) || rateBp < 0) throw new RangeError(`VAT rate must be a non-negative integer in basis points, got ${rateBp}`);
  // integer half-up: floor((2 * g * r + d) / (2 * d)) with d = 10,000 + r; g * r stays far below 2^53 for amounts in minor units
  const d = 10_000 + rateBp;
  const vatMinor = Math.floor((2 * grossMinor * rateBp + d) / (2 * d));
  return { netMinor: grossMinor - vatMinor, vatMinor };
}

/** Document number, e.g. EVC-2026-000042 (invoices) or EVC-CN-2026-000001 (credit notes). */
export function formatInvoiceNumber(prefix: string, year: number, seq: number): string {
  return `${prefix}-${year}-${String(seq).padStart(6, '0')}`;
}

const BERLIN_YEAR = new Intl.DateTimeFormat('en', { timeZone: 'Europe/Berlin', year: 'numeric' });

/** The numbering year of a document issued at `issuedAt`: the calendar year in Germany. */
export function invoiceYear(issuedAt: Date): number {
  return Number(BERLIN_YEAR.format(issuedAt));
}

/**
 * Dunning schedule for a declined capture: retry number `round` (0-based) is due `delaysH[round]` hours after the
 * FIRST decline, so "24,72" means one day and three days after it. null when the schedule is exhausted.
 */
export function nextCaptureRetryAt(round: number, firstDeclinedAt: Date, delaysH: readonly number[]): Date | null {
  const h = delaysH[round];
  return h === undefined ? null : new Date(firstDeclinedAt.getTime() + Math.round(h * 3_600_000));
}

/**
 * Idempotency key of a capture in a retry round. Round 0 keeps the original key, so sagas in flight when this was
 * introduced replay their stored PSP outcome; a decline is final for its key, so every later round needs a new one.
 */
export function captureRetryKey(baseKey: string, round: number): string {
  return round === 0 ? baseKey : `${baseKey}:r${round}`;
}

/**
 * Declines that come from the card or the issuer, where waiting or a replaced card can help: only these enter dunning.
 * Anything else (unknown_authorization, authorization_released, amount_exceeds_authorization, a new PSP code) cannot
 * change by retrying, so it fails at once and the hold is released.
 */
export const DUNNABLE_DECLINE_REASONS: ReadonlySet<string> = new Set([
  'card_declined',
  'insufficient_funds',
  'expired_card',
  'do_not_honor',
  'card_not_supported',
  'lost_card',
  'stolen_card',
]);

/**
 * Parses CAPTURE_RETRY_DELAYS_H ("24,72"): positive hours, strictly increasing. "none" disables dunning (an empty
 * environment value cannot, because the config loader treats it as unset and applies the default).
 */
export function parseRetryDelaysH(raw: string): number[] {
  if (raw.trim() === 'none') return [];
  const parts = raw.split(',').map((s) => s.trim()).filter((s) => s !== '');
  const hours = parts.map(Number);
  const ok = hours.every((h, i) => Number.isFinite(h) && h > 0 && (i === 0 || h > (hours[i - 1] as number)));
  if (!ok) throw new RangeError(`CAPTURE_RETRY_DELAYS_H must be positive, strictly increasing hours, got "${raw}"`);
  return hours;
}
