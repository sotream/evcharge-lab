import { randomUUID } from 'node:crypto';
import { ocpp } from '@evcharge/contracts';
import { describe, expect, it } from 'vitest';
import {
  type ActiveSession,
  buildAllocateReq,
  fallbackAllocation,
  inForceW,
  parseMeasured,
  planProfiles,
  setChargingProfilePayload,
  significantChange,
  type SiteState,
  siteDefaults,
  validateAllocation,
} from '../../src/allocation/plan';

const now = new Date('2026-11-08T10:00:00Z');
const validTo = (s: number) => new Date(now.getTime() + s * 1000);
const stable = (limit = 10_000) => [1, 2].map((n) => session(n, { limitW: limit, limitValidTo: validTo(120), confirmedW: limit, confirmedValidTo: validTo(120) }));
const session = (n: number, over: Partial<ActiveSession> = {}): ActiveSession => ({
  sessionId: randomUUID(),
  transactionId: n,
  stationId: `st-0001-00${n}`,
  connectorId: 1,
  maxW: 22_000,
  minW: 4140,
  startedAt: new Date(now.getTime() - n * 1000),
  limitW: null,
  limitValidTo: null,
  confirmedW: null,
  confirmedValidTo: null,
  possibleW: null,
  possibleValidTo: null,
  ...over,
});
const alloc = (s: SiteState, w: number | ((i: number) => number)) => ({ strategy: 'optimal' as const, limits: new Map(s.sessions.map((x, i) => [x.sessionId, typeof w === 'number' ? w : w(i)])) });
const stableSite = (cap = 100_000) => site(stable(), cap);
const runner = (over: Partial<ActiveSession> = {}) => session(1, { limitW: 11_020, limitValidTo: validTo(120), confirmedW: 11_020, confirmedValidTo: validTo(120), ...over });
const site = (sessions: ActiveSession[], capW = 40_000, connectorsAtSite = 10): SiteState => ({ siteId: 'site-0001', capW, connectorsAtSite, sessions });
const okBody = (s: SiteState, limits: number[]) => ({
  siteId: s.siteId,
  strategy: 'optimal',
  totalAllocatedW: limits.reduce((a, b) => a + b, 0),
  reserveW: siteDefaults(s).reserveW,
  allocations: s.sessions.map((x, i) => ({ sessionId: x.sessionId, limitW: limits[i] ?? 0, suspended: false })),
});

describe('buildAllocateReq', () => {
  it('sends idle connectors, startW and measured power; validates against the contract', () => {
    const s = site([session(1, { measuredW: 7000 }), session(2)], 40_000, 10);
    const req = buildAllocateReq(s, now);
    expect(req.computedAt).toBe(now.toISOString());
    expect(req).toMatchObject({ siteId: 'site-0001', siteCapW: 40_000, idleConnectors: 8, startW: 4000 });
    expect(req.sessions[0]?.measuredW).toBe(7000);
    expect(req.sessions[1]).not.toHaveProperty('measuredW');
  });
});

describe('validateAllocation (never trust the allocator with the cap)', () => {
  const s = site([session(1), session(2)]);
  const reserve = siteDefaults(s).reserveW;

  it('accepts a response that fits cap - reserve', () => {
    const budget = s.capW - reserve;
    const v = validateAllocation(s, okBody(s, [budget / 2, budget / 2]));
    expect(v.ok).toBe(true);
  });

  it('rejects one W over the cap, flagging a cap violation', () => {
    const budget = s.capW - reserve;
    const v = validateAllocation(s, okBody(s, [budget / 2 + 1, budget / 2]));
    expect(v).toMatchObject({ ok: false, reason: 'cap_exceeded', capViolation: true });
  });

  it('rejects wrong site, unknown/duplicate/missing sessions, negative limits and malformed bodies', () => {
    expect(validateAllocation(s, { ...okBody(s, [1000, 1000]), siteId: 'other' })).toMatchObject({ ok: false, reason: 'wrong_site' });
    const b = okBody(s, [1000, 1000]);
    expect(validateAllocation(s, { ...b, allocations: [b.allocations[0]] })).toMatchObject({ ok: false, reason: 'session_mismatch' });
    expect(validateAllocation(s, { ...b, allocations: [b.allocations[0], b.allocations[0]] })).toMatchObject({ ok: false, reason: 'session_mismatch' });
    expect(validateAllocation(s, { ...b, allocations: [...b.allocations.slice(0, 1), { sessionId: randomUUID(), limitW: 1, suspended: false }] })).toMatchObject({ ok: false });
    expect(validateAllocation(s, okBody(s, [-1, 1000]))).toMatchObject({ ok: false });
    expect(validateAllocation(s, { nope: true })).toMatchObject({ ok: false, reason: 'invalid_response' });
    expect(validateAllocation(s, null)).toMatchObject({ ok: false, reason: 'invalid_response' });
  });
});

describe('fallbackAllocation', () => {
  it('gives every active session safeShareW and stays within the cap for any site', () => {
    for (const cap of [0, 5000, 33_333, 120_000]) {
      for (const total of [1, 4, 17, 80]) {
        for (const active of [0, 1, Math.floor(total / 2), total]) {
          const s = site(Array.from({ length: active }, (_, i) => session(i + 1)), cap, total);
          const f = fallbackAllocation(s);
          expect(f.strategy).toBe('fallback-equal-share');
          const d = siteDefaults(s);
          const sum = [...f.limits.values()].reduce((a, b) => a + b, 0);
          expect(sum + d.reserveW).toBeLessThanOrEqual(cap);
          for (const v of f.limits.values()) expect(v).toBe(d.safeShareW);
        }
      }
    }
  });
});

describe('profile sending rules (ADR 0006: delta >= max(2 %, 200 W), or within 60 s of expiry)', () => {
  it('significantChange needs both thresholds and ignores no change', () => {
    expect(significantChange(10_000, 10_199)).toBe(false);
    expect(significantChange(10_000, 10_200)).toBe(true);
    expect(significantChange(5000, 5199)).toBe(false);
    expect(significantChange(5000, 5200)).toBe(true);
    expect(significantChange(50_000, 50_999)).toBe(false);
    expect(significantChange(50_000, 51_000)).toBe(true);
    expect(significantChange(0, 0)).toBe(false);
    expect(significantChange(0, 199)).toBe(false);
    expect(significantChange(0, 200)).toBe(true);
  });


  it('sends nothing when limits are unchanged and far from expiry', () => {
    const s = stableSite();
    expect(planProfiles(s, alloc(s, 10_050), now).reason).toBeUndefined();
  });

  it('a new session whose target is the start default needs no profile; a higher target does', () => {
    const s = site([session(1)], 100_000);
    expect(planProfiles(s, alloc(s, siteDefaults(s).startW), now).reason).toBeUndefined();
    const plan = planProfiles(s, alloc(s, 8000), now);
    expect(plan.reason).toBe('new');
    expect(plan.send).toMatchObject([{ limitW: 8000, kind: 'increase' }]);
  });

  it('a >= 200 W change re-sends the whole site (profiles share one validTo)', () => {
    const s = stableSite();
    const plan = planProfiles(s, alloc(s, (i) => (i === 0 ? 10_300 : 10_000)), now);
    expect(plan.reason).toBe('changed');
    expect(plan.send).toHaveLength(2);
  });

  it('refreshes when a profile is within 60 s of validTo', () => {
    const near = site([session(1, { limitW: 10_000, limitValidTo: validTo(59), confirmedW: 10_000, confirmedValidTo: validTo(59) })], 100_000);
    expect(planProfiles(near, alloc(near, 10_000), now)).toMatchObject({ reason: 'refresh', send: [{ kind: 'refresh', limitW: 10_000 }] });
    const far = site([session(1, { limitW: 10_000, limitValidTo: validTo(61), confirmedW: 10_000, confirmedValidTo: validTo(61) })], 100_000);
    expect(planProfiles(far, alloc(far, 10_000), now).reason).toBeUndefined();
  });

  it('sends when the limits in force would break a (lowered) cap', () => {
    const s = site(stable(30_000), 40_000, 10);
    const plan = planProfiles(s, alloc(s, 30_000), now);
    expect(plan.reason).toBe('safety');
  });

  it('inForceW: unconfirmed profiles count while valid, safe share after expiry, start default otherwise', () => {
    const base = session(1);
    expect(inForceW(base, now, 4000, 6000)).toBe(4000);
    expect(inForceW({ ...base, possibleW: 9000, possibleValidTo: validTo(10) }, now, 4000, 6000)).toBe(9000);
    expect(inForceW({ ...base, confirmedW: 9000, confirmedValidTo: validTo(-1) }, now, 4000, 6000)).toBe(6000);
    expect(inForceW({ ...base, confirmedW: 3000, confirmedValidTo: validTo(10), possibleW: 12_000, possibleValidTo: validTo(10) }, now, 4000, 6000)).toBe(12_000);
  });

  describe('two-phase apply (H1)', () => {
    // cap 40 kW, 8 connectors: startW 4140 (safe share 5000), reserve for 6 idle = 24 840, budget 15 160.
    const cap = 40_000;
    const startW = 4140;

    it('sends the decrease now and withholds an increase that only fits after the decrease is confirmed', () => {
      const s = site([runner(), session(2)], cap, 8);
      const plan = planProfiles(s, alloc(s, 7580), now);
      expect(plan.send.map((x) => [x.session.transactionId, x.kind, x.limitW])).toEqual([[1, 'decrease', 7580]]);
      expect(plan.deferred).toBe(1);
    });

    it('an unconfirmed decrease keeps counting at the old limit: the increase still waits', () => {
      const s = site([runner({ limitW: 7580, possibleW: 11_020, possibleValidTo: validTo(120) }), session(2)], cap, 8);
      const plan = planProfiles(s, alloc(s, 7580), now);
      expect(plan.deferred).toBe(1);
      expect(plan.send.filter((x) => x.kind === 'increase')).toEqual([]);
    });

    it('once the decrease is confirmed the increase fits exactly', () => {
      const s = site([runner({ limitW: 7580, confirmedW: 7580, confirmedValidTo: validTo(120) }), session(2)], cap, 8);
      const plan = planProfiles(s, alloc(s, 7580), now);
      expect(plan.send.map((x) => [x.session.transactionId, x.kind, x.limitW])).toContainEqual([2, 'increase', 7580]);
      expect(plan.deferred).toBe(0);
    });

    it('whatever is sent, sum(worst-case in force after the plan, increases applied, decreases NOT applied) + reserve <= cap', () => {
      let considered = 0;
      const violations: number[] = [];
      for (let seed = 1; seed <= 300; seed++) {
        let r = seed;
        const rnd = () => (r = (r * 1664525 + 1013904223) % 2 ** 32) / 2 ** 32;
        const n = 1 + Math.floor(rnd() * 6);
        const sessions = Array.from({ length: n }, (_, i) => {
          const lim = Math.floor(rnd() * 1500) + startW;
          return session(i + 1, { limitW: lim, limitValidTo: validTo(100), confirmedW: rnd() < 0.5 ? lim : null, confirmedValidTo: validTo(100), possibleW: rnd() < 0.5 ? lim + 500 : null, possibleValidTo: validTo(100) });
        });
        const st = site(sessions, cap, 8);
        const d = siteDefaults(st);
        const targets = sessions.map(() => Math.floor((cap - d.reserveW) / n * rnd()));
        const plan = planProfiles(st, alloc(st, (i) => targets[i] as number), now);
        const sent = new Map(plan.send.map((x) => [x.session.sessionId, x]));
        const cur = sessions.reduce((sum, x) => sum + inForceW(x, now, d.startW, d.safeShareW), 0);
        const worst = sessions.reduce((sum, x) => {
          const p = sent.get(x.sessionId);
          return sum + (p && p.kind === 'increase' ? p.limitW : inForceW(x, now, d.startW, d.safeShareW));
        }, 0);
        if (cur + d.reserveW <= cap) {
          considered++;
          if (worst + d.reserveW > cap) violations.push(seed);
        }
      }
      expect(violations).toEqual([]);
      expect(considered).toBeGreaterThan(100);
    });
  });
});

describe('setChargingProfilePayload', () => {
  it('is a valid OCPP 1.6 TxProfile at stackLevel 1 with validTo = computedAt + 180 s', () => {
    const s = session(1);
    const validToAt = new Date(now.getTime() + 180_000);
    const payload = setChargingProfilePayload(s, 7400, now, validToAt);
    const req = ocpp.CsmsToStation.SetChargingProfile.req.parse(payload);
    expect(req.connectorId).toBe(1);
    expect(req.csChargingProfiles).toMatchObject({ chargingProfilePurpose: 'TxProfile', stackLevel: 1, transactionId: 1, validTo: validToAt.toISOString() });
    expect(req.csChargingProfiles.chargingSchedule.chargingSchedulePeriod).toEqual([{ startPeriod: 0, limit: 7400 }]);
  });
});

describe('parseMeasured', () => {
  const t = Date.parse('2026-11-08T10:00:00Z');
  it('reads fresh readings in ms or s and ignores stale or malformed ones', () => {
    expect(parseMeasured({ w: '7400.9', ts: String(t - 1000) }, t)).toBe(7400);
    expect(parseMeasured({ w: '100', ts: String(Math.floor((t - 1000) / 1000)) }, t)).toBe(100);
    expect(parseMeasured({ w: '100', ts: new Date(t - 1000).toISOString() }, t)).toBe(100);
    expect(parseMeasured({ w: '100', ts: String(t - 300_000) }, t)).toBeUndefined();
    expect(parseMeasured({ w: 'abc' }, t)).toBeUndefined();
    expect(parseMeasured({ w: '-5' }, t)).toBeUndefined();
    expect(parseMeasured({}, t)).toBeUndefined();
    expect(parseMeasured(null, t)).toBeUndefined();
  });
});
