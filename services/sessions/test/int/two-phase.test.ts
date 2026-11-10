/* oxlint-disable no-await-in-loop */
import { events, fleet, ocpp } from '@evcharge/contracts';
import { randomUUID } from 'node:crypto';
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
    const allocated = async () => Number((await h.db.pool.query("SELECT count(*) AS n FROM outbox WHERE payload->>'type' = 'site.power-allocated'")).rows[0].n);
    const roundsBefore = await allocated();
    await new Promise((r) => setTimeout(r, 600));
    expect(await inForce()).toBeLessThanOrEqual(CAP);
    expect(profilesOf(b.transactionId)).toHaveLength(0);
    // A non-Accepted result must not re-trigger the site (it would resend the same profiles: a command storm).
    expect(await allocated()).toBe(roundsBefore);

    // The decrease is finally confirmed (a later round re-sends it): the withheld increase is released by the result.
    // An unconfirmed profile is re-sent once its last send is 30 s old; age it instead of waiting.
    await h.db.pool.query("UPDATE active_sessions SET limit_sent_at = now() - interval '31 seconds' WHERE transaction_id = $1", [a.transactionId]);
    const sentBefore = profilesOf(a.transactionId).length;
    await round();
    const latest = await waitFor(() => (profilesOf(a.transactionId).length > sentBefore ? profilesOf(a.transactionId).findLast((c) => limitOf(c) === 7580) : undefined));
    await h.publishCommandResult(latest.data.commandId, 'st-0001-002', 'SetChargingProfile', 'Accepted');
    const bProfile = await waitFor(() => profilesOf(b.transactionId)[0]);
    expect(limitOf(bProfile)).toBe(7580);
    expect(await inForce()).toBeLessThanOrEqual(CAP);
    // sanity: everything that can be in force now adds up to exactly the allocation
    expect(await inForce()).toBe(2 * 7580 + 6 * defaults.startW);
  });
});

const metric = async (hh: Harness, name: string) => {
  const text = (await hh.http('GET', '/metrics', { token: null })).body as string;
  return text.split('\n').filter((l) => l.startsWith(name)).reduce((a, l) => a + Number(l.split(' ').pop()), 0);
};
const hasStop = (hh: Harness, transactionId: number) => hh.commands.some((c) => c.data.action === 'RemoteStopTransaction' && (c.data.payload as { transactionId?: number }).transactionId === transactionId);
async function profileResults(hh: Harness, sessionId: string, station: string, outcome: 'Rejected' | 'Timeout', n: number) {
  for (let i = 0; i < n; i++) {
    const commandId = randomUUID();
    const { rows } = await hh.db.pool.query<{ started_at: Date }>('SELECT started_at FROM active_sessions WHERE session_id = $1', [sessionId]);
    await hh.db.pool.query(
      "INSERT INTO commands (command_id, station_id, action, session_id, session_started_at, expires_at) VALUES ($1, $2, 'SetChargingProfile', $3, $4, now() + interval '1 minute')",
      [commandId, station, sessionId, rows[0]?.started_at],
    );
    await hh.publishCommandResult(commandId, station, 'SetChargingProfile', outcome);
    await waitFor(async () => (await hh.db.pool.query('SELECT 1 FROM commands WHERE command_id = $1 AND outcome = $2', [commandId, outcome])).rowCount === 1);
  }
}


describe('stations that keep rejecting profiles (M10)', () => {
  it('counts three Rejected results once, never stops by default, and ignores Timeout', async () => {
    const hh = await startHarness({ sites: 1, stationsPerSite: 4 });
    try {
      const before = await metric(hh, 'sessions_profile_noncompliant_total');
      const s = (await hh.startTx('st-0001-001', 2, 'anonymous')).body as { transactionId: number; sessionId: string };
      await profileResults(hh, s.sessionId, 'st-0001-001', 'Timeout', 3);
      expect(await metric(hh, 'sessions_profile_noncompliant_total')).toBe(before);
      await profileResults(hh, s.sessionId, 'st-0001-001', 'Rejected', 3);
      expect(await metric(hh, 'sessions_profile_noncompliant_total')).toBe(before + 1);
      await new Promise((r) => setTimeout(r, 500));
      expect(hasStop(hh, s.transactionId)).toBe(false);
    } finally {
      await hh.close();
    }
  }, 60_000);

  it('sends RemoteStop after PROFILE_FAILURES_TO_STOP Rejected results, but not after Timeouts', async () => {
    const hh = await startHarness({ sites: 1, stationsPerSite: 4, env: { PROFILE_FAILURES_TO_STOP: '3' } });
    try {
      const t = (await hh.startTx('st-0001-001', 2, 'anonymous')).body as { transactionId: number; sessionId: string };
      await profileResults(hh, t.sessionId, 'st-0001-001', 'Timeout', 3);
      await new Promise((r) => setTimeout(r, 500));
      expect(hasStop(hh, t.transactionId)).toBe(false);
      await profileResults(hh, t.sessionId, 'st-0001-001', 'Rejected', 3);
      await waitFor(() => hasStop(hh, t.transactionId));
    } finally {
      await hh.close();
    }
  }, 60_000);
});
