import type { Pool } from '@evcharge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { metrics } from '../../src/metrics';
import { MockPsp } from '../../src/psp/mock-psp';
import { type AuthorizeRequest, type CaptureRequest, type PaymentProvider, PspDeclinedError, PspIdempotencyMismatchError, PspTimeoutError, type RefundRequest, type ReleaseRequest } from '../../src/psp/payment-provider';
import { completedEvent, createHarness, driverIdOf, type Harness, sessionIdOf, startedEvent } from './harness';
import { startStubTariff, type StubTariff, waitFor } from './infra';

const count = async (h: Harness, sql: string, params: unknown[] = []) => Number((await h.db.pool.query<{ n: string }>(sql, params)).rows[0]?.n);
const sagaState = async (h: Harness, n: number) => (await h.db.pool.query<{ state: string }>('SELECT state FROM billing_sagas WHERE session_id = $1', [sessionIdOf(n)])).rows[0]?.state;
const mismatches = async () => (await metrics.pspIdempotencyMismatch.get()).values.reduce((sum, v) => sum + v.value, 0);

/** A PSP whose behaviour a test can bend per call; everything else is the real mock. */
class ScriptedPsp implements PaymentProvider {
  readonly inner: MockPsp;
  authorizeHook?: (req: AuthorizeRequest, inner: MockPsp) => Promise<{ authorizationId: string }>;
  captureHook?: (req: CaptureRequest, inner: MockPsp) => Promise<{ pspReference: string; capturedMinor: number }>;
  releases: ReleaseRequest[] = [];

  constructor(pool: Pool) {
    this.inner = new MockPsp(pool);
  }
  authorize(req: AuthorizeRequest) {
    return this.authorizeHook ? this.authorizeHook(req, this.inner) : this.inner.authorize(req);
  }
  capture(req: CaptureRequest) {
    return this.captureHook ? this.captureHook(req, this.inner) : this.inner.capture(req);
  }
  release(req: ReleaseRequest) {
    this.releases.push(req);
    return this.inner.release(req);
  }
  refund(req: RefundRequest) {
    return this.inner.refund(req);
  }

}

describe('PSP edge cases', () => {
  let tariff: StubTariff;
  let h: Harness;
  let psp: ScriptedPsp;

  beforeAll(async () => {
    tariff = await startStubTariff();
    h = await createHarness(tariff.url, 3);
    await h.startInstance({ CAPTURE_RETRY_DELAYS_H: 'none' }, (pool) => (psp = new ScriptedPsp(pool))); // these cases expect a decline to be final
  });
  afterAll(async () => {
    await h?.close();
    await tariff?.close();
  });

  const addCard = (n: number, token: string) => h.db.pool.query('INSERT INTO payment_methods (driver_id, token) VALUES ($1, $2) ON CONFLICT (driver_id) DO UPDATE SET token = EXCLUDED.token', [driverIdOf(n), token]);

  it('voids a hold whose authorization answer was lost, and one whose request never arrived', async () => {
    const lostAnswer = 1;
    const lostRequest = 2;
    await addCard(lostAnswer, 'tok_a');
    await addCard(lostRequest, 'tok_b');
    psp.authorizeHook = async (req, inner) => {
      if (req.idempotencyKey === `auth:${sessionIdOf(lostAnswer)}`) {
        await inner.authorize(req); // the PSP placed the hold, the answer was lost
        throw new PspTimeoutError('answer lost');
      }
      throw new PspTimeoutError('request lost'); // never reached the PSP
    };
    await h.publishSession(startedEvent(lostAnswer, driverIdOf(lostAnswer)));
    await h.publishSession(startedEvent(lostRequest, driverIdOf(lostRequest)));

    await waitFor(async () => (await sagaState(h, lostAnswer)) === 'AUTH_FAILED' && (await sagaState(h, lostRequest)) === 'AUTH_FAILED', { what: 'both authorizations given up' });
    // The hold that existed is released, and the PSP refuses the key from now on.
    expect(await count(h, 'SELECT count(*) AS n FROM psp_authorizations WHERE released')).toBe(1);
    expect(await count(h, "SELECT count(*) AS n FROM psp_authorizations WHERE NOT released")).toBe(0);
    expect(await count(h, "SELECT count(*) AS n FROM psp_idempotency WHERE idempotency_key = $1 AND response->>'reason' = 'voided'", [`auth:${sessionIdOf(lostRequest)}`])).toBe(1);
    expect(psp.releases.filter((r) => r.authorizationKey).length).toBeGreaterThanOrEqual(2);
    await waitFor(() => h.billingEvents.filter((e) => e.type === 'billing.payment-failed' && e.data.step === 'authorize').length >= 2, { what: 'authorize failures on Kafka' });
    psp.authorizeHook = undefined;
  });

  it('replays the first capture request when the driver changes card between retries', async () => {
    const n = 11;
    await addCard(n, 'tok_old');
    const before = await mismatches();
    let firstTimeout = true;
    psp.captureHook = async (req, inner) => {
      const res = await inner.capture(req); // the card was charged
      if (firstTimeout) {
        firstTimeout = false;
        await addCard(n, 'tok_new'); // the driver swaps card while the answer is lost
        throw new PspTimeoutError('answer lost');
      }
      return res;
    };
    await h.publishSession(startedEvent(n, driverIdOf(n)));
    await h.publishSession(completedEvent(n, driverIdOf(n), 5_000));
    await waitFor(async () => (await sagaState(h, n)) === 'PAID', { what: 'PAID' });
    psp.captureHook = undefined;

    expect(await count(h, 'SELECT count(*) AS n FROM psp_captures')).toBe(1);
    expect(await count(h, "SELECT count(*) AS n FROM psp_idempotency WHERE operation = 'capture' AND idempotency_key = (SELECT 'capture:' || invoice_id FROM billing_sagas WHERE session_id = $1)", [sessionIdOf(n)])).toBe(1);
    expect(await mismatches()).toBe(before);
  });

  it('parks the saga, with a metric, when the PSP reports an idempotency mismatch', async () => {
    const n = 12;
    await addCard(n, 'tok_m');
    const before = await mismatches();
    psp.captureHook = () => Promise.reject(new PspIdempotencyMismatchError('different request'));
    await h.publishSession(startedEvent(n, driverIdOf(n)));
    await h.publishSession(completedEvent(n, driverIdOf(n), 5_000));
    await waitFor(async () => (await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE session_id = $1 AND state = 'CAPTURING' AND next_attempt_at IS NULL AND failure_reason LIKE 'PSP idempotency mismatch%'", [sessionIdOf(n)])) === 1, { what: 'parked on mismatch' });
    psp.captureHook = undefined;
    expect(await mismatches()).toBe(before + 1);
    expect(await count(h, 'SELECT count(*) AS n FROM payments WHERE session_id = $1', [sessionIdOf(n)])).toBe(0);
  });

  it('collects the part above the hold separately and reports PaymentFailed when only that part is declined', async () => {
    const n = 13;
    await addCard(n, 'tok_r');
    psp.captureHook = (req, inner) => (req.idempotencyKey.endsWith(':rest') ? Promise.reject(new PspDeclinedError('insufficient_funds')) : inner.capture(req));
    await h.publishSession(startedEvent(n, driverIdOf(n)));
    await h.publishSession(completedEvent(n, driverIdOf(n), 150_000)); // 6000 + 50 fee: hold is 5000
    await waitFor(async () => (await sagaState(h, n)) === 'CAPTURE_FAILED', { what: 'CAPTURE_FAILED' });
    psp.captureHook = undefined;

    const payments = await h.db.pool.query<{ kind: string; amount_minor: number }>('SELECT kind, amount_minor FROM payments WHERE session_id = $1', [sessionIdOf(n)]);
    expect(payments.rows).toEqual([{ kind: 'hold', amount_minor: 5000 }]);
    const saga = (await h.db.pool.query<{ hold_released: boolean; failure_reason: string }>('SELECT hold_released, failure_reason FROM billing_sagas WHERE session_id = $1', [sessionIdOf(n)])).rows[0];
    expect(saga).toEqual({ hold_released: true, failure_reason: 'insufficient_funds' });
    await waitFor(() => h.billingEvents.some((e) => e.type === 'billing.payment-failed' && e.data.sessionId === sessionIdOf(n) && e.data.step === 'capture_remainder'), { what: 'remainder failure on Kafka' });
    expect(h.billingEvents.some((e) => e.type === 'billing.payment-captured' && e.data.sessionId === sessionIdOf(n))).toBe(false);
  });
});
