/* oxlint-disable no-await-in-loop */
import type { Pool } from '@evcharge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { MockPsp } from '../../src/psp/mock-psp';
import { type AuthorizeRequest, type CaptureRequest, type PaymentProvider, PspDeclinedError, type RefundRequest, type ReleaseRequest } from '../../src/psp/payment-provider';
import { completedEvent, createHarness, driverIdOf, type Harness, sessionIdOf, startedEvent } from './harness';
import { startStubTariff, type StubTariff, waitFor } from './infra';

const count = async (h: Harness, sql: string, params: unknown[] = []) => Number((await h.db.pool.query<{ n: string }>(sql, params)).rows[0]?.n);
const sagaState = async (h: Harness, n: number) => (await h.db.pool.query<{ state: string }>('SELECT state FROM billing_sagas WHERE session_id = $1', [sessionIdOf(n)])).rows[0]?.state;

/** The real mock PSP, with captures declined by key (declineKeys) or for the token tok_expired, every capture recorded. */
class DunningPsp implements PaymentProvider {
  readonly inner: MockPsp;
  readonly captures: CaptureRequest[] = [];
  declineKeys = new Set<string>();
  constructor(pool: Pool) {
    this.inner = new MockPsp(pool);
  }
  authorize(req: AuthorizeRequest) {
    return this.inner.authorize(req);
  }
  async capture(req: CaptureRequest) {
    this.captures.push(req);
    if (this.declineKeys.has(req.idempotencyKey) || req.token === 'tok_expired') {
      throw new PspDeclinedError('insufficient_funds');
    }
    return this.inner.capture(req);
  }
  release(req: ReleaseRequest) {
    return this.inner.release(req);
  }
  refund(req: RefundRequest) {
    return this.inner.refund(req);
  }

}

// About 1 s and 2 s after the first decline instead of 24 h and 72 h.
const DELAYS = '0.0003,0.0006';

describe('capture dunning (M6): retries after a decline, then fails for good', () => {
  let tariff: StubTariff;
  let h: Harness;
  let psp: DunningPsp;
  const invoiceOf = async (n: number) => (await h.db.pool.query<{ invoice_id: string }>('SELECT invoice_id FROM billing_sagas WHERE session_id = $1', [sessionIdOf(n)])).rows[0]?.invoice_id;
  const addCard = (n: number, token: string) => h.db.pool.query('INSERT INTO payment_methods (driver_id, token) VALUES ($1, $2) ON CONFLICT (driver_id) DO UPDATE SET token = EXCLUDED.token', [driverIdOf(n), token]);
  const failedEvents = (n: number, step?: string) => h.billingEvents.filter((e) => e.type === 'billing.payment-failed' && e.data.sessionId === sessionIdOf(n) && (step === undefined || e.data.step === step));

  beforeAll(async () => {
    tariff = await startStubTariff();
    h = await createHarness(tariff.url, 3);
    await h.startInstance({ CAPTURE_RETRY_DELAYS_H: DELAYS }, (pool) => (psp = new DunningPsp(pool)));
  });
  afterAll(async () => {
    await h?.close();
    await tariff?.close();
  });

  it('a capture declined twice succeeds in the third round with new keys, one payment, no PaymentFailed', async () => {
    const n = 1;
    await addCard(n, 'tok_1');
    await h.publishSession(startedEvent(n, driverIdOf(n)));
    await waitFor(async () => (await sagaState(h, n)) === 'AUTHORIZED', { what: 'authorized' });
    // The invoice id is only known once priced: decline round 0 and round 1 by key, before the capture runs.
    const captureKeysSeen = () => psp.captures.filter((c) => c.authorizationId !== null).map((c) => c.idempotencyKey);
    const originalCapture = psp.capture.bind(psp);
    psp.capture = async (req) => {
      if (!req.idempotencyKey.endsWith(':r2')) psp.declineKeys.add(req.idempotencyKey);
      return originalCapture(req);
    };
    await h.publishSession(completedEvent(n, driverIdOf(n), 4000));
    await waitFor(async () => (await sagaState(h, n)) === 'PAID', { what: 'paid after dunning', timeoutMs: 30_000 });
    psp.capture = originalCapture;

    const inv = await invoiceOf(n);
    expect(captureKeysSeen()).toEqual([`capture:${inv}`, `capture:${inv}:r1`, `capture:${inv}:r2`]);
    expect(await count(h, 'SELECT count(*) AS n FROM payments WHERE session_id = $1', [sessionIdOf(n)])).toBe(1);
    const row = (await h.db.pool.query<{ capture_round: number; capture_first_declined_at: Date | null }>('SELECT capture_round, capture_first_declined_at FROM billing_sagas WHERE session_id = $1', [sessionIdOf(n)])).rows[0];
    expect(row?.capture_round).toBe(2);
    expect(row?.capture_first_declined_at).toBeInstanceOf(Date);
    // one non-final notice per scheduled retry, nothing final
    await waitFor(() => failedEvents(n, 'capture_retry').length === 2, { what: 'two retry notices' });
    expect(failedEvents(n).map((e) => (e.data as { step: string }).step)).toEqual(['capture_retry', 'capture_retry']);
  });

  it('a capture declined in every round fails once the schedule is exhausted: one PaymentFailed, hold released', async () => {
    const n = 2;
    await addCard(n, 'tok_2');
    await h.publishSession(startedEvent(n, driverIdOf(n)));
    await waitFor(async () => (await sagaState(h, n)) === 'AUTHORIZED', { what: 'authorized' });
    const originalCapture = psp.capture.bind(psp);
    psp.capture = async (req) => {
      psp.declineKeys.add(req.idempotencyKey);
      return originalCapture(req);
    };
    await h.publishSession(completedEvent(n, driverIdOf(n), 4000));
    await waitFor(async () => (await sagaState(h, n)) === 'CAPTURE_FAILED', { what: 'failed after the last round', timeoutMs: 30_000 });
    psp.capture = originalCapture;

    const inv = await invoiceOf(n);
    expect(psp.captures.filter((c) => c.idempotencyKey.startsWith(`capture:${inv}`)).map((c) => c.idempotencyKey)).toEqual([`capture:${inv}`, `capture:${inv}:r1`, `capture:${inv}:r2`]);
    await waitFor(() => failedEvents(n, 'capture').length === 1, { what: 'final payment-failed on Kafka' });
    await waitFor(async () => (await h.db.pool.query<{ hold_released: boolean }>('SELECT hold_released FROM billing_sagas WHERE session_id = $1', [sessionIdOf(n)])).rows[0]?.hold_released, { what: 'hold released' });
    expect(await count(h, 'SELECT count(*) AS n FROM payments WHERE session_id = $1', [sessionIdOf(n)])).toBe(0);
    await new Promise((r) => setTimeout(r, 500));
    expect(failedEvents(n, 'capture')).toHaveLength(1);
    expect(failedEvents(n, 'capture_retry')).toHaveLength(2);
  });

  it('a decline that no retry can fix fails at once', async () => {
    const n = 4;
    await addCard(n, 'tok_4');
    await h.publishSession(startedEvent(n, driverIdOf(n)));
    await waitFor(async () => (await sagaState(h, n)) === 'AUTHORIZED', { what: 'authorized' });
    const originalCapture = psp.capture.bind(psp);
    psp.capture = async (req) => {
      if (req.authorizationId) {
        psp.captures.push(req);
        throw new PspDeclinedError('amount_exceeds_authorization');
      }
      return originalCapture(req);
    };
    await h.publishSession(completedEvent(n, driverIdOf(n), 4000));
    await waitFor(async () => (await sagaState(h, n)) === 'CAPTURE_FAILED', { what: 'failed at once', timeoutMs: 15_000 });
    psp.capture = originalCapture;
    const inv = await invoiceOf(n);
    expect(psp.captures.filter((c) => c.idempotencyKey.startsWith(`capture:${inv}`))).toHaveLength(1);
    await waitFor(() => failedEvents(n, 'capture').length === 1, { what: 'final payment-failed' });
    expect(failedEvents(n, 'capture_retry')).toHaveLength(0);
  });

  it('the next round uses the card the driver added after the decline', async () => {
    const n = 3; // no session.started: no hold, the whole total is charged by token
    await addCard(n, 'tok_expired');
    await h.publishSession(completedEvent(n, driverIdOf(n), 4000));
    await waitFor(async () => (await h.db.pool.query<{ capture_round: number }>('SELECT capture_round FROM billing_sagas WHERE session_id = $1', [sessionIdOf(n)])).rows[0]?.capture_round === 1, { what: 'first decline', timeoutMs: 15_000 });
    await addCard(n, 'tok_renewed');
    await waitFor(async () => (await sagaState(h, n)) === 'PAID', { what: 'paid with the new card', timeoutMs: 30_000 });
    const inv = await invoiceOf(n);
    const mine = psp.captures.filter((c) => c.idempotencyKey.startsWith(`capture:${inv}`));
    expect(mine.map((c) => c.token)).toEqual(['tok_expired', 'tok_renewed']);
    expect(mine.map((c) => c.idempotencyKey)).toEqual([`capture:${inv}`, `capture:${inv}:r1`]);
  });
});
