/* oxlint-disable no-await-in-loop */
import { events, fleet, ocpp } from '@evcharge/contracts';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { type Harness, startHarness, waitFor } from './harness';

// cap 40 kW, 8 connectors: startW 4140, safe share 5000, reserve for 6 idle connectors 24 840, budget 15 160.
const SITE = 'site-0001';
const CAP = 40_000;
const defaults = fleet.chargingDefaults(CAP, 8);
let h: Harness;

beforeAll(async () => {
  h = await startHarness({ sites: 1, stationsPerSite: 4 });
  await h.db.pool.query('UPDATE sites SET cap_w = $1', [CAP]);
});
afterAll(async () => {
  await h?.close();
});

type Cmd = events.StationCommandRequested;
const profile = (c: Cmd) => ocpp.CsmsToStation.SetChargingProfile.req.parse(c.data.payload);
const profilesOf = (transactionId: number) => h.commands.filter((c) => c.data.action === 'SetChargingProfile' && profile(c).csChargingProfiles.transactionId === transactionId);
const limitOf = (c: Cmd) => profile(c).csChargingProfiles.chargingSchedule.chargingSchedulePeriod[0]?.limit as number;

async function inForce(): Promise<number> {
  const { rows } = await h.db.pool.query<{ w: number }>(
    `SELECT greatest($1::int,
              CASE WHEN confirmed_limit_w IS NULL THEN 0 WHEN confirmed_valid_to > now() THEN confirmed_limit_w ELSE $2::int END,
              CASE WHEN possible_limit_w IS NULL THEN 0 WHEN possible_valid_to > now() THEN possible_limit_w ELSE $2::int END) AS w
       FROM active_sessions`,
    [defaults.startW, defaults.safeShareW],
  );
  return rows.reduce((a, r) => a + r.w, 0) + (8 - rows.length) * defaults.startW;
}

async function round(): Promise<void> {
  const before = Number((await h.db.pool.query('SELECT count(*) AS n FROM outbox WHERE payload->>\'type\' = \'site.power-allocated\'')).rows[0].n);
  await h.db.pool.query('UPDATE sites SET alloc_due_at = now(), alloc_requested_at = coalesce(alloc_requested_at, now()) WHERE site_id = $1', [SITE]);
  await waitFor(async () => Number((await h.db.pool.query('SELECT count(*) AS n FROM outbox WHERE payload->>\'type\' = \'site.power-allocated\'')).rows[0].n) > before);
}

describe('two-phase profile apply (ADR 0006): a decrease that is never confirmed', () => {
  it('never lets the limits in force (confirmed or possibly applied) exceed the cap, and applies the increase once the decrease is confirmed', async () => {
    // A (22 kW station) runs alone and gets everything the budget allows.
    const a = (await h.startTx('st-0001-002', 1, 'anonymous')).body as { transactionId: number };
    const aFirst = await waitFor(() => profilesOf(a.transactionId)[0]);
    expect(limitOf(aFirst)).toBe(CAP - 7 * defaults.startW); // 11 020
    await h.publishCommandResult(aFirst.data.commandId, 'st-0001-002', 'SetChargingProfile', 'Accepted');
    await waitFor(async () => (await h.db.pool.query('SELECT 1 FROM active_sessions WHERE confirmed_limit_w = 11020')).rowCount === 1);
    expect(await inForce()).toBeLessThanOrEqual(CAP);

    // B starts: the fair share is 7 580 each. A must come down; B may only go up after A's decrease is confirmed.
    const b = (await h.startTx('st-0001-004', 1, 'anonymous')).body as { transactionId: number };
    const aDecrease = await waitFor(() => profilesOf(a.transactionId).find((c) => limitOf(c) === 7580));
    // The gateway result stream never confirms A's decrease (station offline / timeout / late). Check across rounds.
    for (let i = 0; i < 4; i++) {
      await round();
      expect(await inForce(), `after round ${i}`).toBeLessThanOrEqual(CAP);
      expect(profilesOf(b.transactionId), 'B must not be raised while A may still run at 11 020').toHaveLength(0);
    }
    const bad = await h.db.pool.query('SELECT confirmed_limit_w, possible_limit_w FROM active_sessions WHERE transaction_id = $1', [a.transactionId]);
    expect(bad.rows[0]).toEqual({ confirmed_limit_w: 11_020, possible_limit_w: 7580 });
    // a non-Accepted result never forgets what was in force
    await h.publishCommandResult(aDecrease.data.commandId, 'st-0001-002', 'SetChargingProfile', 'Timeout');
    await waitFor(async () => (await h.db.pool.query('SELECT 1 FROM commands WHERE command_id = $1 AND outcome = $2', [aDecrease.data.commandId, 'Timeout'])).rowCount === 1);
    const after = await h.db.pool.query('SELECT limit_w, confirmed_limit_w FROM active_sessions WHERE transaction_id = $1', [a.transactionId]);
    expect(after.rows[0].limit_w).not.toBeNull();
    expect(after.rows[0].confirmed_limit_w).toBe(11_020);
    await new Promise((r) => setTimeout(r, 600));
    expect(await inForce()).toBeLessThanOrEqual(CAP);
    expect(profilesOf(b.transactionId)).toHaveLength(0);

    // The decrease is finally confirmed (a later round re-sends it): the withheld increase is released by the result.
    await round();
    const latest = await waitFor(() => profilesOf(a.transactionId).findLast((c) => limitOf(c) === 7580));
    await h.publishCommandResult(latest.data.commandId, 'st-0001-002', 'SetChargingProfile', 'Accepted');
    const bProfile = await waitFor(() => profilesOf(b.transactionId)[0]);
    expect(limitOf(bProfile)).toBe(7580);
    expect(await inForce()).toBeLessThanOrEqual(CAP);
    // sanity: everything that can be in force now adds up to exactly the allocation
    expect(await inForce()).toBe(2 * 7580 + 6 * defaults.startW);
  });
});
