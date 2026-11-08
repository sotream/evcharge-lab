/**
 * Pure allocation planning (ADR 0006). No I/O: request building, response validation, fallback, and the
 * decision whether to send SetChargingProfile.
 */
import { api, fleet, ocpp } from '@evcharge/contracts';

export const VALIDITY_S = ocpp.ChargingProfiles.ALLOCATION_VALIDITY_S;
/** Refresh profiles this long before their validTo (ADR 0006). */
export const REFRESH_BEFORE_S = 60;
/** Minimum age before an unconfirmed profile is sent again. */
export const RESEND_UNCONFIRMED_S = 30;
export const MIN_CHANGE_W = 200;
export const MIN_CHANGE_RATIO = 0.02;

export interface ActiveSession {
  sessionId: string;
  transactionId: number;
  stationId: string;
  connectorId: number;
  maxW: number;
  minW: number;
  startedAt: Date;
  measuredW?: number;
  /** Last limit sent to the station (confirmed or not), null if none yet (station runs at startW). */
  limitW: number | null;
  limitValidTo: Date | null;
  /** When the current limit was last sent (null before the first send). */
  limitSentAt?: Date | null;
  /** Limit the station confirmed (SetChargingProfile Accepted) and its validTo. */
  confirmedW: number | null;
  confirmedValidTo: Date | null;
  /** Envelope (max limit, latest validTo) of sent-but-unconfirmed profiles that may be in force. */
  possibleW: number | null;
  possibleValidTo: Date | null;
}

export interface SiteState {
  siteId: string;
  capW: number;
  connectorsAtSite: number;
  sessions: ActiveSession[];
}

export type Strategy = 'optimal' | 'fallback-equal-share';

export interface Allocation {
  strategy: Strategy;
  /** sessionId -> limitW */
  limits: Map<string, number>;
}

export function siteDefaults(site: SiteState): { startW: number; safeShareW: number; idleConnectors: number; reserveW: number } {
  const d = fleet.chargingDefaults(site.capW, site.connectorsAtSite);
  const idleConnectors = Math.max(0, site.connectorsAtSite - site.sessions.length);
  return { ...d, idleConnectors, reserveW: idleConnectors * d.startW };
}

/** `computedAt` is not in the contract yet (smart-charging reads it when present): it makes replicas deterministic. */
export function buildAllocateReq(site: SiteState, computedAt: Date): api.AllocateReq & { computedAt: string } {
  const d = siteDefaults(site);
  return {
    computedAt: computedAt.toISOString(),
    siteId: site.siteId,
    siteCapW: site.capW,
    idleConnectors: d.idleConnectors,
    startW: d.startW,
    sessions: site.sessions.map((s) => ({
      sessionId: s.sessionId,
      stationId: s.stationId,
      connectorId: s.connectorId,
      maxW: s.maxW,
      minW: s.minW,
      ...(s.measuredW !== undefined ? { measuredW: s.measuredW } : {}),
      priority: 0,
      startedAt: s.startedAt.toISOString(),
    })),
  };
}

/**
 * Accepts an allocator response only if it is well-formed, covers exactly our sessions, and keeps
 * Σ limitW + idleConnectors × startW ≤ cap, computed from OUR state (ADR 0006 "never exceeded").
 */
export function validateAllocation(site: SiteState, body: unknown): { ok: true; allocation: Allocation } | { ok: false; reason: string; capViolation?: boolean } {
  const parsed = api.AllocateRes.safeParse(body);
  if (!parsed.success) return { ok: false, reason: 'invalid_response' };
  const res = parsed.data;
  if (res.siteId !== site.siteId) return { ok: false, reason: 'wrong_site' };
  const want = new Set(site.sessions.map((s) => s.sessionId));
  const limits = new Map<string, number>();
  for (const a of res.allocations) {
    if (!want.has(a.sessionId) || limits.has(a.sessionId) || a.limitW < 0) return { ok: false, reason: 'session_mismatch' };
    limits.set(a.sessionId, a.limitW);
  }
  if (limits.size !== want.size) return { ok: false, reason: 'session_mismatch' };
  const { reserveW } = siteDefaults(site);
  const sum = [...limits.values()].reduce((a, b) => a + b, 0);
  if (sum + reserveW > site.capW) return { ok: false, reason: 'cap_exceeded', capViolation: true };
  return { ok: true, allocation: { strategy: 'optimal', limits } };
}

/**
 * Fallback when the allocator is unavailable: every active session gets safeShareW = floor(cap / connectors).
 * Σ = active × safeShare + idle × startW ≤ connectors × safeShare ≤ cap.
 */
export function fallbackAllocation(site: SiteState): Allocation {
  const { safeShareW } = siteDefaults(site);
  return { strategy: 'fallback-equal-share', limits: new Map(site.sessions.map((s) => [s.sessionId, safeShareW])) };
}

/** Both thresholds must be reached (delta >= max(2 % of old, 200 W)), so measurement jitter does not re-send profiles. */
export function significantChange(oldW: number, newW: number): boolean {
  const delta = Math.abs(newW - oldW);
  return delta > 0 && delta >= Math.max(MIN_CHANGE_RATIO * oldW, MIN_CHANGE_W);
}

/**
 * Worst-case limit that may be in force at the station right now: the start default, any confirmed or possibly
 * applied (unconfirmed) profile while it is valid, and, once such a profile expired, the gateway's safe share
 * (ADR 0006 layer 3, applied by the gateway on its own).
 */
export function inForceW(s: ActiveSession, now: Date, startW: number, safeShareW: number): number {
  let w = startW;
  for (const [lim, validTo] of [
    [s.confirmedW, s.confirmedValidTo],
    [s.possibleW, s.possibleValidTo],
  ] as const) {
    if (lim === null || !validTo) continue;
    w = Math.max(w, validTo.getTime() > now.getTime() ? lim : safeShareW);
  }
  return w;
}

export type SendReason = 'new' | 'changed' | 'refresh' | 'retry' | 'safety';
export interface ProfileSend {
  session: ActiveSession;
  limitW: number;
  kind: 'decrease' | 'refresh' | 'increase';
}
export interface ProfilePlan {
  /** Why profiles are sent this round, or undefined when nothing needs sending. */
  reason: SendReason | undefined;
  /** Profiles to enqueue now: decreases and refreshes, plus the increases that fit the headroom. */
  send: ProfileSend[];
  /** Increases withheld because the headroom is not there yet; the next round (kicked by results) retries. */
  deferred: number;
}

/**
 * Two-phase apply (ADR 0006). Commands go to different stations and partitions, so Kafka gives NO ordering between
 * a decrease for one station and an increase for another, and a decrease may never be applied (offline, timeout,
 * rejected). Therefore:
 *  - decreases and refreshes are sent immediately;
 *  - an increase is sent only if the cap holds with every session at its worst-case limit in force
 *    (`inForceW`: confirmed or possibly applied), i.e. headroom = cap - idle reserve - sum(in force);
 *    the in-force value of a session only drops when its decrease is confirmed or its profile expires.
 * Increases that do not fit wait for the next round, which a SetChargingProfile result triggers.
 *
 * What starts a round of sends: a session whose target differs from what was last sent by max(2 %, 200 W), a profile
 * within 60 s of validTo, or limits in force that would not fit the cap (safety).
 */
export function planProfiles(site: SiteState, allocation: Allocation, now: Date): ProfilePlan {
  const { startW, safeShareW, reserveW } = siteDefaults(site);
  const target = (s: ActiveSession) => allocation.limits.get(s.sessionId) ?? startW;
  // Per session, not per site: a site-level trigger used to resend every session's profile, so one start or one result
  // cost one command per session at the site (measured: 17 refresh profiles/s for 73 sessions). A session is sent only
  // if it has no profile yet, its target moved by max(2 %, 200 W), its profile is near expiry, or the site is unsafe.
  const nearExpiry = (s: ActiveSession) => s.limitValidTo !== null && s.limitValidTo.getTime() - now.getTime() <= REFRESH_BEFORE_S * 1000;
  // A sent profile that is still unconfirmed (lost command, station offline) is retried, but at most every 30 s.
  const unconfirmed = (s: ActiveSession) => s.confirmedW !== s.limitW && (!s.limitSentAt || now.getTime() - s.limitSentAt.getTime() >= RESEND_UNCONFIRMED_S * 1000);
  const needs = (s: ActiveSession, t: number) => s.limitW === null || !s.limitValidTo || nearExpiry(s) || significantChange(s.limitW, t) || unconfirmed(s);
  let reason: SendReason | undefined;
  let sumInForce = 0;
  const cur = new Map<string, number>();
  for (const s of site.sessions) {
    const t = target(s);
    const c = inForceW(s, now, startW, safeShareW);
    cur.set(s.sessionId, c);
    sumInForce += c;
    if (s.limitW === null || !s.limitValidTo) {
      if (significantChange(startW, t)) reason ??= 'new';
    } else if (s.limitValidTo.getTime() - now.getTime() <= REFRESH_BEFORE_S * 1000) reason = reason === 'new' || reason === 'changed' ? reason : 'refresh';
    else if (significantChange(s.limitW, t)) reason = reason === 'new' ? reason : 'changed';
    else if (unconfirmed(s)) reason ??= 'retry';
  }
  if (!reason && sumInForce + reserveW > site.capW) reason = 'safety';
  if (!reason) return { reason, send: [], deferred: 0 };

  let headroom = site.capW - reserveW - sumInForce;
  const unsafe = reason === 'safety';
  const send: ProfileSend[] = [];
  const increases: { session: ActiveSession; limitW: number; delta: number }[] = [];
  for (const s of site.sessions) {
    const t = target(s);
    const c = cur.get(s.sessionId) as number;
    if (!unsafe && !needs(s, t)) continue;
    if (t < c) send.push({ session: s, limitW: t, kind: 'decrease' });
    else if (t === c) {
      if (s.limitW !== null) send.push({ session: s, limitW: t, kind: 'refresh' });
    } else increases.push({ session: s, limitW: t, delta: t - c });
  }
  let deferred = 0;
  for (const inc of increases.sort((a, b) => a.delta - b.delta)) {
    if (inc.delta <= headroom) {
      headroom -= inc.delta;
      send.push({ session: inc.session, limitW: inc.limitW, kind: 'increase' });
    } else deferred++;
  }
  return { reason, send, deferred };
}

/** OCPP SetChargingProfile payload for an allocation (TxProfile, stackLevel 1, validTo = computedAt + 180 s). */
export function setChargingProfilePayload(s: ActiveSession, limitW: number, computedAt: Date, validTo: Date): Record<string, unknown> {
  const profile: ocpp.ChargingProfile = {
    chargingProfileId: ocpp.txProfileId(s.connectorId),
    transactionId: s.transactionId,
    stackLevel: 1,
    chargingProfilePurpose: 'TxProfile',
    chargingProfileKind: 'Absolute',
    validTo: validTo.toISOString(),
    chargingSchedule: { startSchedule: computedAt.toISOString(), chargingRateUnit: 'W', chargingSchedulePeriod: [{ startPeriod: 0, limit: limitW }] },
  };
  return { connectorId: s.connectorId, csChargingProfiles: profile };
}

/** Parses the `power:{station}:{connector}` hash written by telemetry-ingest ({w, ts}); stale readings are ignored. */
export function parseMeasured(hash: Record<string, string> | null | undefined, now: number, maxAgeMs = 120_000): number | undefined {
  if (!hash?.['w']) return undefined;
  const w = Number(hash['w']);
  if (!Number.isFinite(w) || w < 0) return undefined;
  const rawTs = hash['ts'];
  if (rawTs) {
    const n = Number(rawTs);
    const t = Number.isFinite(n) ? (n < 1e12 ? n * 1000 : n) : Date.parse(rawTs);
    if (!Number.isFinite(t) || now - t > maxAgeMs) return undefined;
  }
  return Math.floor(w);
}
