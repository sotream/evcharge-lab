import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { reissueRequestedStops } from '../../src/workers/maintenance';
import { type Harness, startHarness, waitFor } from './harness';

let h: Harness;
beforeAll(async () => {
  h = await startHarness();
}, 60_000);
afterAll(async () => h?.close());

async function startSession(station: string) {
  const d = await h.registerDriver();
  const s = await h.startTx(station, 1, d.idTag);
  return { driverId: d.driverId, sessionId: s.body.sessionId as string, transactionId: s.body.transactionId as number };
}
const stopCmd = (transactionId: number) => h.commands.find((c) => c.data.action === 'RemoteStopTransaction' && (c.data.payload as { transactionId?: number }).transactionId === transactionId);

describe('declined authorization stops charging (M5)', () => {
  it('sends RemoteStop when the authorization is declined', async () => {
    const s = await startSession('st-0001-001');
    await h.publishBilling('billing.payment-failed', { invoiceId: null, sessionId: s.sessionId, step: 'authorize', reason: 'card_declined' });
    await waitFor(() => stopCmd(s.transactionId));
    const sess = await h.driver(s.driverId)('GET', `/v1/sessions/${s.sessionId}`);
    expect(sess.body.status).toBe('Stopping');
  });

  it.each(['psp_unavailable', 'no_payment_method', 'voided', 'some_new_psp_code'])('ignores %s and stops only on the later definite decline (ADR 0000 degradation)', async (reason) => {
    const s = await startSession('st-0001-002');
    await h.publishBilling('billing.payment-failed', { invoiceId: null, sessionId: s.sessionId, step: 'authorize', reason });
    // Same key, same partition, so the consumer has handled the event above once it reaches this one: proof of consumption.
    await h.publishBilling('billing.payment-failed', { invoiceId: null, sessionId: s.sessionId, step: 'authorize', reason: 'card_declined' });
    await waitFor(() => stopCmd(s.transactionId));
    await new Promise((r) => setTimeout(r, 300));
    expect(h.commands.filter((c) => c.data.action === 'RemoteStopTransaction' && (c.data.payload as { transactionId?: number }).transactionId === s.transactionId)).toHaveLength(1);
  });

  it('re-issues the stop when the first RemoteStop could not reach the station (sweeper and station.connected)', async () => {
    const s = await startSession('st-0001-003');
    await h.publishBilling('billing.payment-failed', { invoiceId: null, sessionId: s.sessionId, step: 'authorize', reason: 'card_declined' });
    const first = await waitFor(() => stopCmd(s.transactionId));
    const stopsOf = () => h.commands.filter((c) => c.data.action === 'RemoteStopTransaction' && (c.data.payload as { transactionId?: number }).transactionId === s.transactionId);
    const status = async () => (await h.db.pool.query('SELECT status FROM sessions WHERE session_id = $1', [s.sessionId])).rows[0].status;

    // the station was offline: the result reverts the session to Active, the reason stays
    await h.publishCommandResult(first.data.commandId, 'st-0001-003', 'RemoteStopTransaction', 'StationOffline');
    await waitFor(async () => (await status()) === 'Active');
    expect((await h.db.pool.query('SELECT stop_requested_reason FROM sessions WHERE session_id = $1', [s.sessionId])).rows[0].stop_requested_reason).toBe('auth_declined');

    expect(await reissueRequestedStops(h.db.pool, h.topics, 0)).toBeGreaterThanOrEqual(1);
    await waitFor(() => stopsOf().length === 2);
    expect(await status()).toBe('Stopping');

    // lost again; the station reconnects and the stop goes out at once
    await h.publishCommandResult(stopsOf()[1]?.data.commandId as string, 'st-0001-003', 'RemoteStopTransaction', 'Timeout');
    await waitFor(async () => (await status()) === 'Active');
    await h.publishStationEvent('station.connected', 'st-0001-003', { gatewayInstance: 'gw-1' });
    await waitFor(() => stopsOf().length === 3);
    expect(await status()).toBe('Stopping');
  });
});
