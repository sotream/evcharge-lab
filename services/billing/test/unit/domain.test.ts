import { describe, expect, it } from 'vitest';
import { captureAmount, captureRetryKey, DUNNABLE_DECLINE_REASONS, formatInvoiceNumber, nextCaptureRetryAt, parseRetryDelaysH, invoiceStatus, invoiceYear, isPermanentHttpStatus, PENDING_STATES, retryDelayMs, SAGA_STATES, splitVat } from '../../src/domain';

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

describe('splitVat (tariff prices are gross, PAngV)', () => {
  it('splits a gross amount at 19%', () => {
    expect(splitVat(1190, 1900)).toEqual({ netMinor: 1000, vatMinor: 190 });
    expect(splitVat(1, 1900)).toEqual({ netMinor: 1, vatMinor: 0 });
    expect(splitVat(0, 1900)).toEqual({ netMinor: 0, vatMinor: 0 });
    expect(splitVat(1000, 0)).toEqual({ netMinor: 1000, vatMinor: 0 });
  });
  it('rounds the VAT half up and net + vat is always the gross', () => {
    expect(splitVat(3, 1900)).toEqual({ netMinor: 3, vatMinor: 0 }); // 0.479
    expect(splitVat(6, 1900)).toEqual({ netMinor: 5, vatMinor: 1 }); // 0.958
    for (let g = 0; g < 5000; g += 7) {
      const { netMinor, vatMinor } = splitVat(g, 1900);
      expect(netMinor + vatMinor).toBe(g);
    }
  });
  it('rejects negative amounts and rates', () => {
    expect(() => splitVat(-1, 1900)).toThrow('gross must be a non-negative integer');
    expect(() => splitVat(100, -1)).toThrow('VAT rate must be a non-negative integer');
  });
});

describe('invoice numbering', () => {
  it('formats with a six-digit sequence', () => {
    expect(formatInvoiceNumber('EVC', 2026, 42)).toBe('EVC-2026-000042');
    expect(formatInvoiceNumber('EVC-CN', 2026, 1_234_567)).toBe('EVC-CN-2026-1234567');
  });
  it('takes the year in Europe/Berlin', () => {
    expect(invoiceYear(new Date('2026-12-31T22:59:59Z'))).toBe(2026);
    expect(invoiceYear(new Date('2026-12-31T23:00:00Z'))).toBe(2027); // 00:00 CET
  });
});

describe('capture retries (dunning)', () => {
  const first = new Date('2026-11-10T00:00:00Z');
  it('schedules retries at 24 h and 72 h after the first decline, then gives up', () => {
    expect(nextCaptureRetryAt(0, first, [24, 72])?.toISOString()).toBe('2026-11-11T00:00:00.000Z');
    expect(nextCaptureRetryAt(1, first, [24, 72])?.toISOString()).toBe('2026-11-13T00:00:00.000Z');
    expect(nextCaptureRetryAt(2, first, [24, 72])).toBeNull();
    expect(nextCaptureRetryAt(0, first, [])).toBeNull();
  });
  it('keeps the round-0 key unchanged and suffixes later rounds', () => {
    expect(captureRetryKey('capture:inv', 0)).toBe('capture:inv');
    expect(captureRetryKey('capture:inv:rest', 2)).toBe('capture:inv:rest:r2');
  });
  it('dunns card declines only', () => {
    expect(DUNNABLE_DECLINE_REASONS.has('insufficient_funds')).toBe(true);
    expect(DUNNABLE_DECLINE_REASONS.has('amount_exceeds_authorization')).toBe(false);
    expect(DUNNABLE_DECLINE_REASONS.has('authorization_released')).toBe(false);
  });
  it('parses fractional hours and rejects bad lists', () => {
    expect(parseRetryDelaysH('24,72')).toEqual([24, 72]);
    expect(parseRetryDelaysH(' 0.0003 , 0.0006 ')).toEqual([0.0003, 0.0006]);
    expect(parseRetryDelaysH('')).toEqual([]);
    expect(parseRetryDelaysH('none')).toEqual([]);
    expect(() => parseRetryDelaysH('24,abc')).toThrow('CAPTURE_RETRY_DELAYS_H');
    expect(() => parseRetryDelaysH('72,24')).toThrow('CAPTURE_RETRY_DELAYS_H');
  });
});
