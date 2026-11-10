/* oxlint-disable no-await-in-loop */
import { bootstrapService, HealthRegistry, type RunningService } from '@evcharge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { AppModule } from '../../src/app.module';
import { completedEvent, createHarness, driverIdOf, type Harness, sessionIdOf, startedEvent } from './harness';
import { INTERNAL_TOKEN, silentLogger, startStubTariff, type StubTariff, waitFor } from './infra';

const count = async (h: Harness, sql: string, params: unknown[] = []) => Number((await h.db.pool.query<{ n: string }>(sql, params)).rows[0]?.n);

describe('credit notes with a PSP refund (M2)', () => {
  let tariff: StubTariff;
  let h: Harness;
  let svc: RunningService;
  const PAID = 1; // 150 kWh: 6,050 total, 5,000 captured on the hold and 1,050 as the remainder
  const UNPAID = 2; // no driver: invoiced, never charged
  let paidInvoice = '';
  let unpaidInvoice = '';

  const creditNote = async (invoiceId: string, body: unknown, key?: string) => {
    const res = await fetch(`${svc.url}/internal/invoices/${invoiceId}/credit-notes`, {
      method: 'POST',
      headers: { 'x-internal-token': INTERNAL_TOKEN, 'content-type': 'application/json', ...(key ? { 'idempotency-key': key } : {}) },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    return { status: res.status, body: text ? JSON.parse(text) : undefined };
  };
  const invoiceOf = async (n: number) => (await h.db.pool.query<{ invoice_id: string }>('SELECT invoice_id FROM billing_sagas WHERE session_id = $1', [sessionIdOf(n)])).rows[0]?.invoice_id ?? '';

  beforeAll(async () => {
    tariff = await startStubTariff();
    h = await createHarness(tariff.url, 3);
    await h.db.pool.query('INSERT INTO payment_methods (driver_id, token) VALUES ($1, $2)', [driverIdOf(PAID), 'tok_cn']);
    const { rt, config } = await h.startInstance();
    svc = await bootstrapService({ module: AppModule.register(rt.deps), config, logger: silentLogger, port: 0, handleSignals: false, health: new HealthRegistry() });
    await h.publishSession(startedEvent(PAID, driverIdOf(PAID)));
    await h.publishSession(completedEvent(PAID, driverIdOf(PAID), 150_000));
    await h.publishSession(completedEvent(UNPAID, null, 10_000));
    await waitFor(async () => (await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE state IN ('PAID', 'INVOICED') AND next_attempt_at IS NULL")) === 2, { what: 'paid and unpaid invoices', timeoutMs: 30_000 });
    paidInvoice = await invoiceOf(PAID);
    unpaidInvoice = await invoiceOf(UNPAID);
    if ((await count(h, 'SELECT count(*) AS n FROM payments WHERE invoice_id = $1', [paidInvoice])) !== 2) throw new Error('setup: expected a hold and a remainder capture');
  });
  afterAll(async () => {
    await svc?.shutdown('test');
    await h?.close();
    await tariff?.close();
  });

  it('issues a numbered partial credit note and refunds it across the remainder and the hold capture', async () => {
    const r = await creditNote(paidInvoice, { amountMinor: 2000, reason: 'charger fault, partial refund' }, 'cn-1');
    expect(r.status).toBe(201);
    expect(r.body).toMatchObject({ invoiceId: paidInvoice, amountMinor: 2000, netMinor: 1681, vatMinor: 319, vatRateBp: 1900, refund: { state: 'done', amountMinor: 2000 } });
    expect(r.body.creditNumber).toMatch(/^EVC-CN-\d{4}-000001$/);
    // newest capture first: the whole 1,050 remainder, then 950 of the hold capture
    const refunds = (await h.db.pool.query<{ amount_minor: number }>('SELECT r.amount_minor FROM psp_refunds r JOIN payments p ON p.psp_reference = r.psp_reference WHERE p.invoice_id = $1 ORDER BY p.kind DESC', [paidInvoice])).rows.map((x) => x.amount_minor);
    expect(refunds).toEqual([1050, 950]);
    await waitFor(() => h.billingEvents.some((e) => e.type === 'billing.credit-note-issued' && e.data.creditNoteId === r.body.creditNoteId), { what: 'credit-note-issued on Kafka' });
  });

  it('is idempotent on the idempotency-key and refuses a reused key with another amount', async () => {
    const again = await creditNote(paidInvoice, { amountMinor: 2000, reason: 'charger fault, partial refund' }, 'cn-1');
    expect(again.status).toBe(201);
    expect(again.body.creditNumber).toMatch(/-000001$/);
    expect(await count(h, 'SELECT count(*) AS n FROM credit_notes WHERE invoice_id = $1', [paidInvoice])).toBe(1);
    expect(await count(h, 'SELECT count(*) AS n FROM psp_refunds')).toBe(2);
    expect((await creditNote(paidInvoice, { amountMinor: 1999, reason: 'charger fault, partial refund' }, 'cn-1')).status).toBe(409);
  });

  it('refuses credit notes above what is left of the invoice, also when two race', async () => {
    expect((await creditNote(paidInvoice, { amountMinor: 4051, reason: 'too much' }, 'cn-over')).status).toBe(409);
    const [a, b] = await Promise.all([creditNote(paidInvoice, { amountMinor: 3000, reason: 'race a' }, 'cn-race-a'), creditNote(paidInvoice, { amountMinor: 3000, reason: 'race b' }, 'cn-race-b')]);
    expect([a.status, b.status].sort()).toEqual([201, 409]);
    expect(await count(h, 'SELECT coalesce(sum(amount_minor), 0) AS n FROM credit_notes WHERE invoice_id = $1', [paidInvoice])).toBe(5000);
    // refunds never exceed what was captured
    expect(await count(h, 'SELECT count(*) AS n FROM payments p WHERE (SELECT coalesce(sum(amount_minor), 0) FROM psp_refunds r WHERE r.psp_reference = p.psp_reference) > p.amount_minor')).toBe(0);
    expect(await count(h, "SELECT coalesce(sum(amount_minor), 0) AS n FROM credit_note_refunds WHERE state = 'done'")).toBe(5000);
    // gapless across credit notes; the refused ones took no number
    const numbers = (await h.db.pool.query<{ n: string }>('SELECT credit_number AS n FROM credit_notes ORDER BY credit_number')).rows.map((x) => Number(x.n.split('-').at(-1)));
    expect(numbers).toEqual([1, 2]);
  });

  it('credits an unpaid invoice without a refund', async () => {
    const r = await creditNote(unpaidInvoice, { amountMinor: 100, reason: 'goodwill' }, 'cn-unpaid');
    expect(r.status).toBe(201);
    expect(r.body.refund).toEqual({ state: 'none', amountMinor: 0 });
    expect(r.body.creditNumber).toMatch(/-000003$/);
  });

  it('validates input and unknown invoices', async () => {
    expect((await creditNote(paidInvoice, { amountMinor: 1, reason: 'no key' })).status).toBe(400);
    expect((await creditNote(paidInvoice, { amountMinor: 0, reason: 'zero' }, 'cn-zero')).status).toBe(400);
    expect((await creditNote(paidInvoice, { amountMinor: 1.5, reason: 'fraction' }, 'cn-frac')).status).toBe(400);
    expect((await creditNote(paidInvoice, { amountMinor: 1, reason: '' }, 'cn-empty')).status).toBe(400);
    expect((await creditNote(sessionIdOf(999), { amountMinor: 1, reason: 'unknown' }, 'cn-unknown')).status).toBe(404);
  });

  it('credit notes are append-only', async () => {
    await expect(h.db.pool.query('UPDATE credit_notes SET amount_minor = 1')).rejects.toThrow('append-only');
    await expect(h.db.pool.query('DELETE FROM credit_notes')).rejects.toThrow('append-only');
  });
});
