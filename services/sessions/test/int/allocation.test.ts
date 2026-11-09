/* oxlint-disable no-await-in-loop */
import { events, fleet, ocpp, RedisKeys } from '@evcharge/contracts';
import { Redis } from 'ioredis';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { type Harness, REDIS_URL, startHarness, waitFor } from './harness';

type Allocated = Extract<events.SessionEvent, { type: 'site.power-allocated' }>;
const SITE = 'site-0001';
const CONNECTORS = 8;

let h: Harness;
let capW = 0;
let defaults = { startW: 0, safeShareW: 0 };
beforeAll(async () => {
  h = await startHarness({ sites: 1, stationsPerSite: 4 });
  capW = Number((await h.db.pool.query('SELECT cap_w FROM sites')).rows[0].cap_w);
  defaults = fleet.chargingDefaults(capW, CONNECTORS);
  h.ack.enabled = true;
});
afterAll(async () => {
  await h?.close();
});

const rounds = () => h.sessionEvents.filter((e): e is Allocated => e.type === 'site.power-allocated' && e.data.allocations.length > 0);
const profiles = () => h.commands.filter((c) => c.data.action === 'SetChargingProfile');
const idleOf = (e: Allocated) => CONNECTORS - e.data.allocations.length;

/** Waits until no round holds the lease and the outbox is drained, so no stale round or event can leak into the next step. */
async function quiesce(): Promise<void> {
  await waitFor(async () => (await h.db.pool.query('SELECT 1 FROM sites WHERE alloc_lease_until IS NOT NULL')).rowCount === 0);
  await waitFor(async () => (await h.db.pool.query('SELECT 1 FROM outbox WHERE published_at IS NULL')).rowCount === 0);
  await waitFor(async () => (await h.db.pool.query('SELECT 1 FROM sites WHERE alloc_deferred')).rowCount === 0);
  await new Promise((r) => setTimeout(r, 400));
}

/** Marks the site due and waits for a round that was computed after this call (events arrive through Kafka, so count nothing older). */
async function nextRound(): Promise<Allocated> {
  const t0 = Date.now();
  await h.db.pool.query('UPDATE sites SET alloc_due_at = now(), alloc_requested_at = coalesce(alloc_requested_at, now()) WHERE site_id = $1', [SITE]);
  return waitFor(() => rounds().find((r) => Date.parse(r.data.computedAt) >= t0));
}

const profileCount = async () => Number((await h.db.pool.query("SELECT count(*) AS n FROM commands WHERE action = 'SetChargingProfile'")).rows[0].n);

/** ADR 0006 invariant from the event alone: sum(limits) + idle x startW <= cap. */
function assertWithinCap(e: Allocated): void {
  const sum = e.data.allocations.reduce((a, x) => a + x.limitW, 0);
  expect(sum + idleOf(e) * defaults.startW, `round ${e.eventId}`).toBeLessThanOrEqual(capW);
}

/** Worst case in force right now, from the database alone: start default, valid confirmed/possible limits, safe share once expired. */
async function assertInForceWithinCap(): Promise<void> {
  const { rows } = await h.db.pool.query<{ w: number }>(
    `SELECT greatest($1::int,
              CASE WHEN confirmed_limit_w IS NULL THEN 0 WHEN confirmed_valid_to > now() THEN confirmed_limit_w ELSE $2::int END,
              CASE WHEN possible_limit_w IS NULL THEN 0 WHEN possible_valid_to > now() THEN possible_limit_w ELSE $2::int END) AS w
       FROM active_sessions`,
    [defaults.startW, defaults.safeShareW],
  );
  const sum = rows.reduce((a, r) => a + r.w, 0) + (CONNECTORS - rows.length) * defaults.startW;
  expect(sum).toBeLessThanOrEqual(capW);
}

const start = (st: number, c: number) => h.startTx(`st-0001-00${st}`, c, 'anonymous').then((r) => r.body as { transactionId: number; sessionId: string });

describe('allocation orchestration (ADR 0006)', () => {
  it('a session start triggers a round: TxProfile stackLevel 1, validTo = computedAt + 180 s, limits within the cap', async () => {
    h.allocator.mode = 'ok';
    const s1 = await start(1, 1);
    const s2 = await start(1, 2);
    await waitFor(() => rounds().some((r) => r.data.allocations.length === 2));
    const round = rounds().find((r) => r.data.allocations.length === 2)!;
    expect(round.data.strategy).toBe('optimal');
    expect(round.data.siteCapW).toBe(capW);
    assertWithinCap(round);
    const txOf = (c: (typeof h.commands)[number]) => (c.data.payload['csChargingProfiles'] as { transactionId: number }).transactionId;
    // increases wait for decreases to be confirmed (auto-acked here), so the second session's profile can come a round later
    await waitFor(() => [s1, s2].every((x) => profiles().some((c) => txOf(c) === x.transactionId)));
    const mine = profiles().filter((c) => [s1.transactionId, s2.transactionId].includes(txOf(c)));
    expect(mine.length).toBeGreaterThanOrEqual(2);
    expect(typeof h.allocator.lastReq?.['computedAt']).toBe('string');
    for (const cmd of mine) {
      const req = ocpp.CsmsToStation.SetChargingProfile.req.parse(cmd.data.payload);
      const p = req.csChargingProfiles;
      expect(p).toMatchObject({ chargingProfilePurpose: 'TxProfile', stackLevel: 1, chargingProfileId: ocpp.txProfileId(req.connectorId) });
      expect(p.chargingSchedule.chargingRateUnit).toBe('W');
      const validForMs = Date.parse(p.validTo ?? '') - Date.parse(p.chargingSchedule.startSchedule ?? '');
      expect(validForMs).toBe(ocpp.ChargingProfiles.ALLOCATION_VALIDITY_S * 1000);
      expect(cmd.data.requestedBy).toBe('sessions');
    }
    const row = await h.db.pool.query('SELECT limit_w, limit_valid_to FROM active_sessions WHERE session_id = $1', [s1.sessionId]);
    expect(row.rows[0].limit_w).toBeGreaterThan(0);
    expect(Date.parse(row.rows[0].limit_valid_to)).toBeGreaterThan(Date.now() + 100_000);
    // ADR 0006: the site-wide validTo the gateway checks before raising to the safe share covers every profile sent.
    const redis = new Redis(REDIS_URL);
    const siteValidTo = Number(await redis.get(RedisKeys.siteAllocationValidTo(SITE)));
    await redis.quit();
    expect(siteValidTo).toBeGreaterThanOrEqual(Date.parse(row.rows[0].limit_valid_to));
  });

  it('an unchanged round does not re-send profiles (2 % / 200 W threshold) but still reports the allocation', async () => {
    await quiesce();
    const before = await profileCount();
    const r = await nextRound();
    expect(r.data.allocations).toHaveLength(2);
    await quiesce();
    expect(await profileCount()).toBe(before);
  });

  it('two replicas never run the same site concurrently (lease + SKIP LOCKED)', async () => {
    await h.db.pool.query('UPDATE sites SET alloc_due_at = now() WHERE site_id = $1', [SITE]);
    const results = await Promise.all(Array.from({ length: 6 }, () => h.app.allocation.runClaimed(SITE)));
    expect(results.filter(Boolean).length).toBeLessThanOrEqual(1);
  });

  it('records reallocation latency (start -> profile command enqueued) and the round metrics', async () => {
    const text = (await h.http('GET', '/metrics', { token: null })).body as string;
    const count = /sessions_reallocation_latency_seconds_count\{[^}]*\} (\d+)/.exec(text);
    expect(Number(count?.[1])).toBeGreaterThan(0);
    expect(text).toMatch(/sessions_allocation_rounds_total\{[^}]*strategy="optimal"/);
  });

  it('the cap is never exceeded while sessions start and stop (random churn, every round checked)', async () => {
    h.allocator.mode = 'ok';
    const open: { transactionId: number; st: number; c: number }[] = [];
    let seed = 42;
    const rnd = () => (seed = (seed * 1664525 + 1013904223) % 2 ** 32) / 2 ** 32;
    // two sessions from the first test are still running on st-0001-001 connectors 1 and 2
    const busy = new Set<string>(['1:1', '1:2']);
    for (let i = 0; i < 40; i++) {
      const st = 1 + Math.floor(rnd() * 4);
      const c = 1 + Math.floor(rnd() * 2);
      const key = `${st}:${c}`;
      if (busy.has(key)) continue;
      if (open.length > 0 && rnd() < 0.4) {
        const x = open.splice(Math.floor(rnd() * open.length), 1)[0]!;
        await h.stopTx(`st-0001-00${x.st}`, x.transactionId);
        busy.delete(`${x.st}:${x.c}`);
      } else {
        const s = await start(st, c);
        open.push({ transactionId: s.transactionId, st, c });
        busy.add(key);
      }
      await assertInForceWithinCap();
      if (i % 5 === 0) await nextRound();
    }
    await nextRound();
    const all = rounds();
    expect(all.length).toBeGreaterThan(5);
    for (const e of all) assertWithinCap(e);
    for (const e of h.sessionEvents.filter((x): x is Allocated => x.type === 'site.power-allocated')) assertWithinCap(e);
    const cap = Number((await h.db.pool.query('SELECT max(c.n) AS n FROM (SELECT count(*) AS n FROM active_sessions GROUP BY site_id) c')).rows[0].n);
    expect(cap).toBeLessThanOrEqual(CONNECTORS);
    // limits that are in force in the database (the last profile sent to each session) also fit
    const inForce = Number((await h.db.pool.query('SELECT coalesce(sum(coalesce(limit_w, $1)), 0) AS s FROM active_sessions', [defaults.startW])).rows[0].s);
    const active = Number((await h.db.pool.query('SELECT count(*) AS n FROM active_sessions')).rows[0].n);
    expect(inForce + (CONNECTORS - active) * defaults.startW).toBeLessThanOrEqual(capW);
    // clean up for the next tests
    await h.db.pool.query('DELETE FROM active_sessions');
  });

  it('allocator answers that would exceed the cap are rejected and replaced by the fallback', async () => {
    await start(2, 1);
    await start(2, 2);
    await quiesce();
    h.allocator.mode = 'over-cap';
    const r = await nextRound();
    expect(r.data.strategy).toBe('fallback-equal-share');
    assertWithinCap(r);
    const text = (await h.http('GET', '/metrics', { token: null })).body as string;
    expect(text).toMatch(/sessions_allocation_cap_violations_rejected_total\{?[^ ]*\}? [1-9]/);
  });

  it('fallback: allocator timing out -> every session gets safeShareW (strategy fallback-equal-share) within 300 ms-bounded rounds', async () => {
    await quiesce();
    h.allocator.mode = 'hang';
    await h.db.pool.query('UPDATE active_sessions SET limit_w = NULL, limit_valid_to = NULL, confirmed_limit_w = NULL, confirmed_valid_to = NULL, possible_limit_w = NULL, possible_valid_to = NULL, pending_command_id = NULL');
    const t0 = Date.now();
    const r = await nextRound();
    expect(Date.now() - t0).toBeLessThan(3000);
    expect(r.data.strategy).toBe('fallback-equal-share');
    for (const a of r.data.allocations) expect(a.limitW).toBe(defaults.safeShareW);
    assertWithinCap(r);
    const last = profiles().slice(-r.data.allocations.length);
    for (const cmd of last) {
      const p = ocpp.CsmsToStation.SetChargingProfile.req.parse(cmd.data.payload).csChargingProfiles;
      expect(p.chargingSchedule.chargingSchedulePeriod[0]?.limit).toBe(defaults.safeShareW);
    }
  });

  it('fallback: allocator down (5xx) -> safe share, charging profiles keep being refreshed, and the service stays ready', async () => {
    await quiesce();
    h.allocator.mode = 'down';
    for (let i = 0; i < 3; i++) {
      const r = await nextRound();
      expect(r.data.strategy).toBe('fallback-equal-share');
      assertWithinCap(r);
    }
    expect((await h.http('GET', '/health/ready', { token: null })).status).toBe(200);
    const text = (await h.http('GET', '/metrics', { token: null })).body as string;
    expect(text).toMatch(/sessions_allocator_failures_total\{reason="(error|timeout|circuit_open)"[^}]*\} [1-9]/);
    // sessions still start while the allocator is down
    expect((await h.startTx('st-0001-003', 1, 'anonymous')).status).toBe(200);
  });

  it('recovers to optimal allocation once the allocator is back (circuit half-opens)', async () => {
    h.allocator.mode = 'ok';
    const r = await waitFor(
      async () => {
        const x = await nextRound();
        return x.data.strategy === 'optimal' ? x : undefined;
      },
      { timeoutMs: 40_000, intervalMs: 500 },
    );
    assertWithinCap(r);
    expect(r.data.strategy).toBe('optimal');
  });

  it('a failed round is retried soon and keeps its trigger (no 60 s wait, requested_at restored)', async () => {
    await quiesce();
    h.allocator.mode = 'ok';
    const spy = vi.spyOn(h.app.allocation, 'loadSite').mockRejectedValueOnce(new Error('boom'));
    const s = await start(4, 1);
    await waitFor(() => spy.mock.calls.length > 0);
    const row = await waitFor(async () => {
      const r = (await h.db.pool.query<{ due_in: number; requested: Date | null; leased: boolean }>("SELECT extract(epoch FROM alloc_due_at - now())::float8 AS due_in, alloc_requested_at AS requested, alloc_lease_token IS NOT NULL AS leased FROM sites WHERE site_id = $1", [SITE])).rows[0];
      return r && !r.leased ? r : undefined;
    });
    expect(row.requested).not.toBeNull();
    expect(row.due_in).toBeLessThanOrEqual(5.5);
    const round = await waitFor(() => rounds().find((r) => r.data.allocations.some((a) => a.sessionId === s.sessionId)), { timeoutMs: 20_000 });
    expect(round).toBeDefined();
    spy.mockRestore();
  });
});
