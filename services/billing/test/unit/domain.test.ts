import { describe, expect, it } from 'vitest';
import { captureAmount, invoiceStatus, isPermanentHttpStatus, PENDING_STATES, retryDelayMs, SAGA_STATES } from '../../src/domain';

const hi = (a: number) => retryDelayMs(a, 1000, 30_000, () => 1 - Number.EPSILON);
const lo = (a: number) => retryDelayMs(a, 1000, 30_000, () => 0);

describe('retryDelayMs', () => {
  it('follows the capped 1 s -> 30 s progression (no jitter: random=1 gives the cap, 0 gives half)', () => {
    expect([1, 2, 3, 4, 5, 6].map(hi)).toEqual([999, 1999, 3999, 7999, 15_999, 29_999]);
    expect(lo(1)).toBe(500);
    expect(hi(20)).toBe(29_999);
    expect(lo(20)).toBe(15_000);
  });

  it('stays within [exp/2, exp) for any attempt', () => {
    for (let a = 1; a < 40; a++) {
      const exp = Math.min(30_000, 1000 * 2 ** (a - 1));
      const d = retryDelayMs(a, 1000, 30_000);
      expect(d).toBeGreaterThanOrEqual(Math.floor(exp / 2));
      expect(d).toBeLessThanOrEqual(exp);
    }
  });

  it('treats attempts below 1 as the first retry', () => {
    expect(retryDelayMs(0, 1000, 30_000, () => 0)).toBe(500);
  });
});

describe('captureAmount', () => {
  it('captures min(total, hold) when a hold exists', () => {
    expect(captureAmount(1200, 5000, true)).toBe(1200);
    expect(captureAmount(9000, 5000, true)).toBe(5000);
    expect(captureAmount(0, 5000, true)).toBe(0);
  });
  it('charges the whole total without a hold', () => {
    expect(captureAmount(9000, 0, false)).toBe(9000);
  });
});

describe('invoiceStatus', () => {
  it('maps saga states to the driver-facing status', () => {
    expect(invoiceStatus('PAID')).toBe('Paid');
    expect(invoiceStatus('CAPTURE_FAILED')).toBe('PaymentFailed');
    for (const s of ['INVOICED', 'CAPTURING']) expect(invoiceStatus(s)).toBe('Issued');
  });
});

describe('isPermanentHttpStatus', () => {
  it('only non-throttling 4xx are permanent', () => {
    expect([400, 404, 422].every(isPermanentHttpStatus)).toBe(true);
    expect([408, 429, 500, 502, 503].some(isPermanentHttpStatus)).toBe(false);
  });
});

describe('states', () => {
  it('pending states are a subset of the machine', () => {
    for (const s of PENDING_STATES) expect(SAGA_STATES).toContain(s);
    expect(SAGA_STATES).toHaveLength(8);
  });
});
