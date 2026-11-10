/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (ordering)
import { bootstrapService, HealthRegistry, type RunningService } from '@evcharge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { AppModule } from '../../src/app.module';
import { scrubDeletedDrivers } from '../../src/gdpr';
import { metrics } from '../../src/metrics';
import { completedEvent, createHarness, driverIdOf, type Harness, sessionIdOf, startedEvent } from './harness';
import { INTERNAL_TOKEN, silentLogger, startStubTariff, type StubTariff, waitFor } from './infra';

const count = async (h: Harness, sql: string, params: unknown[] = []) => Number((await h.db.pool.query<{ n: string }>(sql, params)).rows[0]?.n);
const sagaState = async (h: Harness, n: number) => (await h.db.pool.query<{ state: string }>('SELECT state FROM billing_sagas WHERE session_id = $1', [sessionIdOf(n)])).rows[0]?.state;
const pspCalls = async (operation: string, outcome: string) => {
  const m = await metrics.pspCalls.get();
  return m.values.find((v) => v.labels['operation'] === operation && v.labels['outcome'] === outcome)?.value ?? 0;
};

const energyOf = (n: number) => (n % 6 === 0 ? 150_000 : 3_000 + n * 100); // multiples of 6 exceed the 50.00 hold

describe('exactly-once effect: replays, duplicates, PSP timeouts, two replicas', () => {
  let tariff: StubTariff;
  let h: Harness;
  const DRIVERS = 36; // sessions 1..36 have a driver and a payment method
  const NO_DRIVER = [37, 38, 39, 40]; // invoiced as unpaid, never authorized or captured
  const NO_STARTED = 35; // has a driver, but session.started never arrives: priced and charged directly, no hold
  const ALL = 40;

  beforeAll(async () => {
    tariff = await startStubTariff();
    h = await createHarness(tariff.url);
  });
  afterAll(async () => {
    await h?.close();
    await tariff?.close();
  });

  it('issues exactly one invoice and one capture per session', async () => {
    const timeoutsBefore = await pspCalls('capture', 'timeout');
    for (let n = 1; n <= DRIVERS; n++) await h.db.pool.query('INSERT INTO payment_methods (driver_id, token) VALUES ($1, $2)', [driverIdOf(n), `tok_${n}`]);
    // Two replicas in one consumer group, both injecting PSP timeouts (half of them after the card was charged).
    const env = { PSP_TIMEOUT_RATE: '0.4', SAGA_WORKERS: '3' };
    await h.startInstance(env);
    await h.startInstance(env);

    const driver = (n: number) => (n <= DRIVERS ? driverIdOf(n) : null);
    for (let n = 1; n <= ALL; n++) if (n !== NO_STARTED) await h.publishSession(startedEvent(n, driver(n)));
    for (let n = 1; n <= ALL; n++) {
      const first = completedEvent(n, driver(n), energyOf(n));
      await h.publishSession(first);
      await h.publishSession(first); // redelivery: same eventId (inbox)
      await h.publishSession(completedEvent(n, driver(n), energyOf(n))); // producer retry: new eventId (saga guard + UNIQUE(session_id))
    }

    // INVOICED is final only for sessions without a driver; sessions with one pass through it.
    await waitFor(
      async () =>
        (await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE state = 'PAID'")) === DRIVERS &&
        (await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE state = 'INVOICED' AND driver_id IS NULL")) === NO_DRIVER.length,
      { what: 'all sagas terminal', timeoutMs: 90_000 },
    );

    // invoices == sessions, one each
    expect(await count(h, 'SELECT count(*) AS n FROM invoices')).toBe(ALL);
    expect(await count(h, 'SELECT count(DISTINCT session_id) AS n FROM invoices')).toBe(ALL);
    expect(await count(h, 'SELECT count(*) AS n FROM invoice_registry')).toBe(ALL);
    expect(await count(h, 'SELECT count(*) AS n FROM invoice_lines')).toBe(ALL * 2);
    expect(await count(h, 'SELECT count(*) AS n FROM invoices i WHERE i.total_minor <> (SELECT sum(amount_minor) FROM invoice_lines l WHERE l.invoice_id = i.invoice_id)')).toBe(0);

    // exactly one capture per invoice that has a driver; none for the unpaid ones
    // Sessions whose total exceeds the hold capture the remainder separately. With a 0.4 timeout rate an authorization
    // can legitimately give up after 5 attempts (hold voided, whole total charged by token), so derive the number.
    const OVER_HOLD = await count(h, 'SELECT count(*) AS n FROM billing_sagas WHERE authorization_id IS NOT NULL AND total_minor > 5000');
    expect(OVER_HOLD).toBeGreaterThanOrEqual(3);
    expect(await count(h, "SELECT count(*) AS n FROM payments WHERE kind = 'hold'")).toBe(DRIVERS);
    expect(await count(h, "SELECT count(*) AS n FROM payments WHERE kind = 'remainder'")).toBe(OVER_HOLD);
    expect(await count(h, 'SELECT count(DISTINCT invoice_id) AS n FROM payments')).toBe(DRIVERS);
    expect(await count(h, "SELECT count(*) AS n FROM psp_idempotency WHERE operation = 'capture'")).toBe(DRIVERS + OVER_HOLD);
    expect(await count(h, "SELECT count(*) AS n FROM psp_idempotency WHERE operation = 'capture' AND idempotency_key LIKE '%:rest'")).toBe(OVER_HOLD);
    expect(await count(h, 'SELECT count(*) AS n FROM psp_captures')).toBe(DRIVERS + OVER_HOLD);
    expect(await count(h, "SELECT count(*) AS n FROM psp_idempotency WHERE operation = 'authorize'")).toBe(DRIVERS - 1); // all but the never-started session
    expect(await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE state = 'PAID'")).toBe(DRIVERS);
    expect(await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE state = 'INVOICED'")).toBe(NO_DRIVER.length);

    // every invoice is collected in full: hold capture plus remainder equals the total
    expect(await count(h, 'SELECT count(*) AS n FROM invoices i JOIN billing_sagas s ON s.invoice_id = i.invoice_id WHERE s.state = \'PAID\' AND (SELECT sum(amount_minor) FROM payments p WHERE p.invoice_id = i.invoice_id) <> i.total_minor')).toBe(0);
    expect(await count(h, "SELECT count(*) AS n FROM payments p JOIN billing_sagas s ON s.invoice_id = p.invoice_id WHERE p.kind = 'hold' AND s.authorization_id IS NOT NULL AND p.amount_minor > 5000")).toBe(0);
    // PSP never captured more than it authorized
    expect(await count(h, 'SELECT count(*) AS n FROM psp_authorizations WHERE captured_minor > amount_minor')).toBe(0);

    // the timeout path was actually exercised
    expect(await pspCalls('capture', 'timeout')).toBeGreaterThan(timeoutsBefore);

    // Review Focus 3: two replicas issued these concurrently; numbers are unique, gapless and match the document
    const numbers = (await h.db.pool.query<{ n: string }>('SELECT invoice_number AS n FROM invoice_registry ORDER BY invoice_number')).rows.map((r) => r.n);
    expect(numbers).toHaveLength(ALL);
    for (const n of numbers) expect(n).toMatch(/^EVC-\d{4}-\d{6}$/);
    const seqs = numbers.map((n) => Number(n.split('-')[2]));
    expect(new Set(seqs).size).toBe(ALL);
    expect(seqs.at(-1)! - seqs[0]! + 1).toBe(ALL);
    expect(seqs[0]).toBe(1);
    expect(await count(h, 'SELECT count(*) AS n FROM invoices i JOIN invoice_registry r ON r.invoice_id = i.invoice_id WHERE i.invoice_number IS DISTINCT FROM r.invoice_number')).toBe(0);
    // VAT: 19% of the gross total, net + vat = total on every invoice
    expect(await count(h, 'SELECT count(*) AS n FROM invoices WHERE vat_rate_bp <> 1900 OR net_minor + vat_minor <> total_minor OR vat_minor <> round(total_minor * 1900.0 / 11900)')).toBe(0);
    expect(await count(h, 'SELECT count(*) AS n FROM invoices WHERE vat_minor > 0')).toBeGreaterThan(0);

    // outbox: one invoice-issued and one payment-captured per paid session, written exactly once
    expect(await count(h, "SELECT count(*) AS n FROM outbox WHERE headers->>'event-type' = 'billing.invoice-issued'")).toBe(ALL);
    expect(await count(h, "SELECT count(DISTINCT key) AS n FROM outbox WHERE headers->>'event-type' = 'billing.payment-captured'")).toBe(DRIVERS);
    expect(await count(h, "SELECT count(*) AS n FROM outbox WHERE headers->>'event-type' = 'billing.payment-captured'")).toBe(DRIVERS);

    // and published to Kafka (at-least-once: distinct)
    await waitFor(() => new Set(h.billingEvents.filter((e) => e.type === 'billing.invoice-issued').map((e) => e.data.sessionId)).size === ALL, { what: 'invoice-issued on Kafka' });
    await waitFor(() => new Set(h.billingEvents.filter((e) => e.type === 'billing.payment-captured').map((e) => e.data.sessionId)).size === DRIVERS, { what: 'payment-captured on Kafka' });
    await waitFor(async () => (await count(h, 'SELECT count(*) AS n FROM outbox WHERE published_at IS NULL')) === 0, { what: 'outbox drained' });
    // one of each session's saga rows, whatever the duplicates did
    expect(await count(h, 'SELECT count(*) AS n FROM billing_sagas')).toBe(ALL);
    expect((await h.db.pool.query('SELECT session_id FROM billing_sagas WHERE session_id = $1', [sessionIdOf(NO_STARTED)])).rowCount).toBe(1);
  });
});

describe('tariff outage: the saga waits in PRICING and completes after recovery', () => {
  let tariff: StubTariff;
  let h: Harness;
  beforeAll(async () => {
    tariff = await startStubTariff();
    h = await createHarness(tariff.url, 3);
  });
  afterAll(async () => {
    await h?.close();
    await tariff?.close();
  });

  it('retries with backoff while tariff is down, then invoices and captures once', async () => {
    tariff.down = true;
    await h.db.pool.query('INSERT INTO payment_methods (driver_id, token) VALUES ($1, $2)', [driverIdOf(1), 'tok_1']);
    const { rt } = await h.startInstance();
    await h.publishSession(startedEvent(1, driverIdOf(1)));
    await h.publishSession(completedEvent(1, driverIdOf(1), 10_000));

    await waitFor(async () => (await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE state = 'PRICING' AND attempts >= 3")) === 1, { what: 'PRICING with retries' });
    await rt.engine.refreshPendingGauge();
    const pending = (await metrics.sagasPending.get()).values.find((v) => v.labels['state'] === 'PRICING')?.value;
    expect(pending).toBe(1);
    const row = (await h.db.pool.query<{ last_error: string; next_attempt_at: Date }>('SELECT last_error, next_attempt_at FROM billing_sagas')).rows[0];
    expect(row?.last_error).toBeTruthy();
    expect(await count(h, 'SELECT count(*) AS n FROM invoices')).toBe(0);
    expect(await count(h, 'SELECT count(*) AS n FROM psp_captures')).toBe(0);
    expect(tariff.calls).toBeGreaterThanOrEqual(3);

    tariff.down = false;
    await waitFor(async () => (await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE state = 'PAID'")) === 1, { what: 'PAID after recovery' });
    expect(await count(h, 'SELECT count(*) AS n FROM invoices')).toBe(1);
    expect(await count(h, 'SELECT count(*) AS n FROM payments')).toBe(1);
    // payment tokens are encrypted at rest
    const tok = (await h.db.pool.query<{ auth_token: string; capture_token: string }>('SELECT auth_token, capture_token FROM billing_sagas')).rows[0];
    expect(tok?.auth_token.startsWith('v1:')).toBe(true);
    expect(tok?.capture_token.startsWith('v1:')).toBe(true);
    const latency = await metrics.invoiceLatency.get();
    expect(latency.values.find((v) => v.metricName === 'billing_invoice_latency_seconds_count')?.value).toBeGreaterThanOrEqual(1);
  });
});

describe('a quote the tariff service rejects for good releases the hold', () => {
  let tariff: StubTariff;
  let h: Harness;
  beforeAll(async () => {
    tariff = await startStubTariff();
    h = await createHarness(tariff.url, 3);
  });
  afterAll(async () => {
    await h?.close();
    await tariff?.close();
  });

  it('parks in PRICING territory without leaving the authorization on the card', async () => {
    tariff.reject = 400;
    await h.db.pool.query('INSERT INTO payment_methods (driver_id, token) VALUES ($1, $2)', [driverIdOf(7), 'tok_7']);
    await h.startInstance();
    await h.publishSession(startedEvent(7, driverIdOf(7)));
    await h.publishSession(completedEvent(7, driverIdOf(7), 10_000));

    await waitFor(async () => (await sagaState(h, 7)) === 'CAPTURE_FAILED', { what: 'saga handed to compensation' });
    const row = (await h.db.pool.query<{ authorization_id: string; hold_released: boolean; failure_reason: string }>('SELECT authorization_id, hold_released, failure_reason FROM billing_sagas WHERE session_id = $1', [sessionIdOf(7)])).rows[0];
    expect(row?.authorization_id).toBeTruthy();
    expect(row?.failure_reason).toContain('tariff rejected the quote');
    await waitFor(async () => (await h.db.pool.query<{ released: boolean }>('SELECT released FROM psp_authorizations WHERE authorization_id = $1', [row?.authorization_id])).rows[0]?.released, { what: 'hold released' });
    await waitFor(async () => (await h.db.pool.query<{ hold_released: boolean }>('SELECT hold_released FROM billing_sagas WHERE session_id = $1', [sessionIdOf(7)])).rows[0]?.hold_released, { what: 'saga records the release' });
    await waitFor(() => h.billingEvents.some((e) => e.type === 'billing.payment-failed' && e.data.sessionId === sessionIdOf(7) && e.data.step === 'pricing'), { what: 'payment-failed on Kafka' });
    expect(await count(h, 'SELECT count(*) AS n FROM invoices')).toBe(0);
  });
});

describe('energy flagged by sessions is held for review (M3)', () => {
  let tariff: StubTariff;
  let h: Harness;
  beforeAll(async () => {
    tariff = await startStubTariff();
    h = await createHarness(tariff.url, 3);
  });
  afterAll(async () => {
    await h?.close();
    await tariff?.close();
  });

  it.each([
    [9101, 'implausible', 5000],
    [9102, 'meter_regressed', 0],
    [9103, 'stale', 0],
  ])('session %i flagged %s waits in PRICING for review: no invoice, hold kept, payment-failed emitted', async (n, flag, energy) => {
    await h.db.pool.query('INSERT INTO payment_methods (driver_id, token) VALUES ($1, $2) ON CONFLICT DO NOTHING', [driverIdOf(n), `tok_${n}`]);
    await h.startInstance();
    const ev = completedEvent(n, driverIdOf(n), energy);
    (ev.data as { energyFlag?: string }).energyFlag = flag;
    await h.publishSession(startedEvent(n, driverIdOf(n)));
    await h.publishSession(ev);
    await waitFor(async () => (await h.db.pool.query("SELECT 1 FROM billing_sagas WHERE session_id = $1 AND state = 'PRICING' AND next_attempt_at IS NULL AND failure_reason = $2", [sessionIdOf(n), `energy_review:${flag}`])).rowCount, { what: 'held for review' });
    await waitFor(() => h.billingEvents.some((e) => e.type === 'billing.payment-failed' && e.data.sessionId === sessionIdOf(n) && e.data.step === 'pricing'), { what: 'payment-failed on Kafka' });
    expect((await h.db.pool.query('SELECT 1 FROM invoice_registry WHERE session_id = $1', [sessionIdOf(n)])).rowCount).toBe(0);
    const auth = (await h.db.pool.query<{ released: boolean }>('SELECT a.released FROM psp_authorizations a JOIN billing_sagas s ON s.authorization_id = a.authorization_id WHERE s.session_id = $1', [sessionIdOf(n)])).rows[0];
    expect(auth?.released).toBe(false);
  });

  it('lists a held saga and resumes it with acceptEnergy, which then invoices; resuming again is a 409', async () => {
    const n = 9104;
    await h.db.pool.query('INSERT INTO payment_methods (driver_id, token) VALUES ($1, $2) ON CONFLICT DO NOTHING', [driverIdOf(n), `tok_${n}`]);
    const { rt, config } = await h.startInstance();
    const svc = await bootstrapService({ module: AppModule.register(rt.deps), config, logger: silentLogger, port: 0, handleSignals: false, health: new HealthRegistry() });
    try {
      const internal = async (method: string, path: string, body?: unknown) => {
        const res = await fetch(`${svc.url}${path}`, { method, headers: { 'x-internal-token': INTERNAL_TOKEN, ...(body ? { 'content-type': 'application/json' } : {}) }, body: body ? JSON.stringify(body) : undefined });
        const text = await res.text();
        return { status: res.status, body: text ? JSON.parse(text) : undefined };
      };
      const ev = completedEvent(n, driverIdOf(n), 5000);
      (ev.data as { energyFlag?: string }).energyFlag = 'implausible';
      await h.publishSession(startedEvent(n, driverIdOf(n)));
      await h.publishSession(ev);
      await waitFor(async () => (await h.db.pool.query("SELECT 1 FROM billing_sagas WHERE session_id = $1 AND state = 'PRICING' AND next_attempt_at IS NULL", [sessionIdOf(n)])).rowCount, { what: 'held' });
      const list = await internal('GET', '/internal/sagas/parked');
      expect(list.status).toBe(200);
      expect(list.body.find((x: { sessionId: string }) => x.sessionId === sessionIdOf(n))).toMatchObject({ state: 'PRICING', failureReason: 'energy_review:implausible' });
      expect((await internal('POST', `/internal/sagas/${sessionIdOf(n)}/resume`, { acceptEnergy: true })).status).toBe(200);
      await waitFor(async () => (await h.db.pool.query('SELECT 1 FROM invoice_registry WHERE session_id = $1', [sessionIdOf(n)])).rowCount, { what: 'invoice after resume' });
      expect((await internal('POST', `/internal/sagas/${sessionIdOf(n)}/resume`)).status).toBe(409);
      expect((await internal('POST', `/internal/sagas/${sessionIdOf(1)}/resume`)).status).toBe(404);
    } finally {
      await svc.shutdown('test');
    }
  });
});

describe('account deletion with an unfinished saga', () => {
  let tariff: StubTariff;
  let h: Harness;
  beforeAll(async () => {
    tariff = await startStubTariff();
    h = await createHarness(tariff.url, 3);
  });
  afterAll(async () => {
    await h?.close();
    await tariff?.close();
  });

  it('keeps the card and tokens until the saga is final, refuses a new card, then scrubs', async () => {
    const n = 9201;
    const d = driverIdOf(n);
    tariff.down = true;
    await h.db.pool.query('INSERT INTO payment_methods (driver_id, token) VALUES ($1, $2)', [d, 'tok_9201']);
    const { rt, config } = await h.startInstance();
    const svc = await bootstrapService({ module: AppModule.register(rt.deps), config, logger: silentLogger, port: 0, handleSignals: false, health: new HealthRegistry() });
    try {
      const call = async (method: string, path: string, body?: unknown) => {
        const res = await fetch(`${svc.url}${path}`, { method, headers: { 'x-internal-token': INTERNAL_TOKEN, ...(body ? { 'content-type': 'application/json' } : {}) }, body: body ? JSON.stringify(body) : undefined });
        return res.status;
      };
      await h.publishSession(startedEvent(n, d));
      await h.publishSession(completedEvent(n, d, 5000));
      await waitFor(async () => (await h.db.pool.query("SELECT 1 FROM billing_sagas WHERE session_id = $1 AND state = 'PRICING' AND attempts >= 1 AND auth_token IS NOT NULL", [sessionIdOf(n)])).rowCount, { what: 'PRICING with the hold taken' });

      expect(await call('DELETE', `/internal/drivers/${d}/payment-data`)).toBe(204);
      expect((await h.db.pool.query('SELECT 1 FROM deleted_drivers WHERE driver_id = $1', [d])).rowCount).toBe(1);
      expect((await h.db.pool.query('SELECT 1 FROM payment_methods WHERE driver_id = $1', [d])).rowCount).toBe(1); // the saga still needs it
      expect(await call('PUT', `/v1/drivers/${d}/payment-method`, { token: 'tok_new_card' })).toBe(410);

      tariff.down = false;
      await waitFor(async () => (await sagaState(h, n)) === 'PAID', { what: 'PAID' });
      expect(await scrubDeletedDrivers(h.db.pool)).toBeGreaterThan(0);
      expect((await h.db.pool.query('SELECT 1 FROM billing_sagas WHERE session_id = $1 AND auth_token IS NULL AND capture_token IS NULL', [sessionIdOf(n)])).rowCount).toBe(1);
      expect((await h.db.pool.query('SELECT 1 FROM payment_methods WHERE driver_id = $1', [d])).rowCount).toBe(0);
      expect((await h.db.pool.query('SELECT 1 FROM invoice_registry WHERE session_id = $1', [sessionIdOf(n)])).rowCount).toBe(1);
    } finally {
      await svc.shutdown('test');
    }
  });
});

describe('declines, compensation and the HTTP API', () => {
  let tariff: StubTariff;
  let h: Harness;
  let base: string;
  let svc: RunningService | undefined;
  const D_OK = driverIdOf(101);
  const D_CAPTURE_DECLINED = driverIdOf(102);
  const D_AUTH_DECLINED = driverIdOf(103);
  const auth = { 'x-internal-token': 'test-internal-token-0123456789' };

  beforeAll(async () => {
    tariff = await startStubTariff();
    h = await createHarness(tariff.url, 3);
    const { rt, config } = await h.startInstance({ PSP_DECLINE_TOKENS: 'tok_nofunds', PSP_AUTH_DECLINE_TOKENS: 'tok_stolen', CAPTURE_RETRY_DELAYS_H: 'none' }); // no dunning: a decline fails at once
    svc = await bootstrapService({ module: AppModule.register(rt.deps), config, logger: silentLogger, port: 0, handleSignals: false, health: new HealthRegistry() });
    base = svc.url;
  });
  afterAll(async () => {
    await svc?.shutdown('test');
    await h?.close();
    await tariff?.close();
  });

  const call = async (method: string, path: string, headers: Record<string, string> = {}, body?: unknown) => {
    const res = await fetch(`${base}${path}`, { method, headers: { ...(body ? { 'content-type': 'application/json' } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined });
    const text = await res.text();
    return { status: res.status, body: text ? (JSON.parse(text) as Record<string, unknown>) : undefined };
  };

  it('rejects x-driver-id without a valid internal token, and cross-driver access', async () => {
    expect((await call('GET', `/v1/invoices?driverId=${D_OK}`, { 'x-driver-id': D_OK })).status).toBe(401); // spoofed header, no token
    expect((await call('GET', `/v1/invoices?driverId=${D_OK}`, { 'x-driver-id': D_OK, 'x-internal-token': 'wrong-token-wrong-token' })).status).toBe(401);
    expect((await call('PUT', `/v1/drivers/${D_OK}/payment-method`, {}, { token: 'tok_x1' })).status).toBe(401);
    expect((await call('GET', `/v1/invoices?driverId=${D_OK}`, { ...auth, 'x-driver-id': D_CAPTURE_DECLINED })).status).toBe(403);
    expect((await call('PUT', `/v1/drivers/${D_OK}/payment-method`, { ...auth, 'x-driver-id': D_CAPTURE_DECLINED }, { token: 'tok_evil' })).status).toBe(403);
    expect((await call('PUT', `/v1/drivers/${D_OK}/payment-method`, auth, { token: 'x' })).status).toBe(400); // token min 4
    expect((await call('GET', '/v1/invoices', auth)).status).toBe(400); // no driver at all
    expect((await call('GET', '/v1/invoices/not-a-uuid', auth)).status).toBe(400);
  });

  it('declined capture releases the hold and reports PaymentFailed; declined authorization still invoices', async () => {
    expect((await call('PUT', `/v1/drivers/${D_OK}/payment-method`, { ...auth, 'x-driver-id': D_OK }, { token: 'tok_good' })).status).toBe(204);
    expect((await h.db.pool.query<{ token: string }>('SELECT token FROM payment_methods WHERE driver_id = $1', [D_OK])).rows[0]?.token.startsWith('v1:')).toBe(true); // encrypted at rest
    expect((await call('PUT', `/v1/drivers/${D_CAPTURE_DECLINED}/payment-method`, auth, { token: 'tok_nofunds' })).status).toBe(204);
    expect((await call('PUT', `/v1/drivers/${D_AUTH_DECLINED}/payment-method`, auth, { token: 'tok_stolen' })).status).toBe(204);

    const sessions: [number, string][] = [
      [1, D_OK],
      [2, D_CAPTURE_DECLINED],
      [3, D_AUTH_DECLINED],
    ];
    for (const [n, d] of sessions) await h.publishSession(startedEvent(n, d));
    for (const [n, d] of sessions) await h.publishSession(completedEvent(n, d, 5_000));
    await waitFor(async () => (await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE state IN ('PAID', 'CAPTURE_FAILED')")) === 3, { what: 'three terminal sagas' });

    expect(await sagaState(h, 1)).toBe('PAID');
    expect(await sagaState(h, 2)).toBe('CAPTURE_FAILED');
    expect(await sagaState(h, 3)).toBe('PAID'); // authorization declined: no hold, so the whole total is charged directly (this token only fails authorizations)
  });

  it('exposes invoices to their owner only', async () => {
    const list = await call('GET', `/v1/invoices?driverId=${D_OK}`, { ...auth, 'x-driver-id': D_OK });
    expect(list.status).toBe(200);
    const items = list.body?.['items'] as { invoiceId: string; status: string; total: { amountMinor: number; currency: string }; driverId: string }[];
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({ status: 'Paid', driverId: D_OK, total: { currency: 'EUR' } });
    // 5 kWh * 40 + 50 fee
    expect(items[0]?.total.amountMinor).toBe(250);

    const own = await call('GET', `/v1/invoices/${items[0]?.invoiceId}`, { ...auth, 'x-driver-id': D_OK });
    expect(own.status).toBe(200);
    expect(((own.body?.['lines'] ?? []) as unknown[]).length).toBe(2);
    expect((await call('GET', `/v1/invoices/${items[0]?.invoiceId}`, { ...auth, 'x-driver-id': D_CAPTURE_DECLINED })).status).toBe(404);
    expect((await call('GET', `/v1/invoices/${items[0]?.invoiceId}`, auth)).status).toBe(200); // internal caller

    const failed = await call('GET', `/v1/invoices?driverId=${D_CAPTURE_DECLINED}`, auth);
    expect(((failed.body?.['items'] ?? []) as { status: string }[])[0]?.status).toBe('PaymentFailed');
  });

  it('compensates a failed capture: hold released once, PaymentFailed emitted, no payment row', async () => {
    const row = (await h.db.pool.query<{ authorization_id: string; hold_released: boolean; failure_reason: string; invoice_id: string }>('SELECT authorization_id, hold_released, failure_reason, invoice_id FROM billing_sagas WHERE session_id = $1', [sessionIdOf(2)])).rows[0];
    expect(row?.failure_reason).toBe('insufficient_funds');
    await waitFor(async () => (await h.db.pool.query<{ released: boolean }>('SELECT released FROM psp_authorizations WHERE authorization_id = $1', [row?.authorization_id])).rows[0]?.released, { what: 'hold released' });
    expect(await count(h, 'SELECT count(*) AS n FROM payments WHERE session_id = $1', [sessionIdOf(2)])).toBe(0);
    expect(await count(h, "SELECT count(*) AS n FROM psp_idempotency WHERE idempotency_key = $1 AND operation = 'release'", [`release:${sessionIdOf(2)}`])).toBe(1);
    await waitFor(() => h.billingEvents.some((e) => e.type === 'billing.payment-failed' && e.data.sessionId === sessionIdOf(2) && e.data.step === 'capture'), { what: 'payment-failed on Kafka' });
    // the declined authorization emitted its own failure and the session was still invoiced
    await waitFor(() => h.billingEvents.some((e) => e.type === 'billing.payment-failed' && e.data.sessionId === sessionIdOf(3) && e.data.step === 'authorize'), { what: 'authorize failure on Kafka' });
    expect(await count(h, 'SELECT count(*) AS n FROM invoices WHERE session_id = $1', [sessionIdOf(3)])).toBe(1);
  });

  it('invoices and lines are append-only and land in monthly partitions', async () => {
    await expect(h.db.pool.query('UPDATE invoices SET total_minor = 1')).rejects.toMatchObject({ code: '23001' });
    await expect(h.db.pool.query('DELETE FROM invoices')).rejects.toMatchObject({ code: '23001' });
    await expect(h.db.pool.query('UPDATE invoice_lines SET amount_minor = 1')).rejects.toMatchObject({ code: '23001' });
    await expect(h.db.pool.query('DELETE FROM invoice_lines')).rejects.toMatchObject({ code: '23001' });
    const month = new Date().toISOString().slice(0, 7).replace('-', '_');
    const { rows } = await h.db.pool.query<{ part: string }>('SELECT DISTINCT tableoid::regclass::text AS part FROM invoices');
    expect(rows.map((r) => r.part)).toEqual([`invoices_${month}`]);
  });

  it('deletes payment data of a driver (GDPR) but keeps the invoices; a second call is a no-op', async () => {
    expect((await call('DELETE', `/internal/drivers/${D_OK}/payment-data`, auth)).status).toBe(204);
    expect(await count(h, 'SELECT count(*) AS n FROM payment_methods WHERE driver_id = $1', [D_OK])).toBe(0);
    expect(await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE driver_id = $1 AND state = 'PAID' AND auth_token IS NULL AND capture_token IS NULL", [D_OK])).toBe(1);
    expect(await count(h, 'SELECT count(*) AS n FROM invoices WHERE driver_id = $1', [D_OK])).toBeGreaterThanOrEqual(1);
    expect((await call('DELETE', `/internal/drivers/${D_OK}/payment-data`, auth)).status).toBe(204);
    expect((await call('DELETE', `/internal/drivers/${D_OK}/payment-data`, {})).status).toBe(401);
  });
});
