import type { ocpp } from '@evcharge/contracts';
import { AC_VOLTAGE } from './physics';

export type Profile = ocpp.ChargingProfile;

interface Stored {
  connectorId: number;
  profile: Profile;
  /** Simulated time the profile was set (Relative kind counts from the transaction start instead when active). */
  receivedMs: number;
}

/**
 * The OCPP charging-profile stack of one station (ADR 0006). Purpose precedence follows OCPP:
 * while a transaction runs, a valid TxProfile beats TxDefaultProfile; inside a purpose the highest
 * stackLevel wins. A profile with the same id, or the same (purpose, stackLevel, connector), is replaced.
 */
export class ProfileStack {
  private readonly items: Stored[] = [];

  /** Applies SetChargingProfile. Returns the OCPP status. */
  set(connectorId: number, profile: Profile, nowMs: number, txActive: boolean): 'Accepted' | 'Rejected' {
    if (profile.chargingProfilePurpose === 'TxProfile' && (connectorId < 1 || !txActive)) return 'Rejected';
    if (profile.chargingProfilePurpose === 'ChargePointMaxProfile' && connectorId !== 0) return 'Rejected';
    const i = this.items.findIndex(
      (s) =>
        s.profile.chargingProfileId === profile.chargingProfileId ||
        (s.connectorId === connectorId && s.profile.chargingProfilePurpose === profile.chargingProfilePurpose && s.profile.stackLevel === profile.stackLevel),
    );
    const stored = { connectorId, profile, receivedMs: nowMs };
    if (i >= 0) this.items[i] = stored;
    else this.items.push(stored);
    return 'Accepted';
  }

  /** A transaction ended: its TxProfiles die with it. */
  clearTx(connectorId: number): void {
    for (let i = this.items.length - 1; i >= 0; i--) {
      const s = this.items[i];
      if (s && s.connectorId === connectorId && s.profile.chargingProfilePurpose === 'TxProfile') this.items.splice(i, 1);
    }
  }

  clearAll(): void {
    this.items.length = 0;
  }

  get size(): number {
    return this.items.length;
  }

  /**
   * Effective limit in W for `connectorId` at `nowMs` (Infinity when no profile applies).
   * `txStartMs` anchors Relative schedules; `phases` converts A limits.
   */
  limitW(connectorId: number, nowMs: number, txActive: boolean, txStartMs: number, phases: number): number {
    let best: Stored | undefined;
    let bestRank = -1;
    for (const s of this.items) {
      const p = s.profile;
      if (s.connectorId !== connectorId && s.connectorId !== 0) continue;
      if (p.chargingProfilePurpose === 'TxProfile' && !txActive) continue;
      if (p.chargingProfilePurpose === 'TxDefaultProfile' && !txActive) continue;
      if (p.validTo && Date.parse(p.validTo) <= nowMs) continue;
      // purpose precedence first (TxProfile > TxDefault > ChargePointMax handled separately below), then stack level
      const purposeRank = p.chargingProfilePurpose === 'TxProfile' ? 2 : p.chargingProfilePurpose === 'TxDefaultProfile' ? 1 : 0;
      if (purposeRank === 0) continue;
      const rank = purposeRank * 1000 + p.stackLevel * 2 + (s.connectorId === connectorId ? 1 : 0);
      if (rank > bestRank) {
        best = s;
        bestRank = rank;
      }
    }
    let limit = best ? scheduleLimitW(best, nowMs, txStartMs, phases) : Number.POSITIVE_INFINITY;
    // ChargePointMaxProfile always caps (connector 0).
    for (const s of this.items) {
      const p = s.profile;
      if (p.chargingProfilePurpose !== 'ChargePointMaxProfile') continue;
      if (p.validTo && Date.parse(p.validTo) <= nowMs) continue;
      limit = Math.min(limit, scheduleLimitW(s, nowMs, txStartMs, phases));
    }
    return limit;
  }
}

function scheduleLimitW(s: Stored, nowMs: number, txStartMs: number, phases: number): number {
  const sched = s.profile.chargingSchedule;
  // Relative schedules count from the transaction start; Absolute/Recurring from startSchedule.
  const absolute = sched.startSchedule ? Date.parse(sched.startSchedule) : s.receivedMs;
  const anchor = s.profile.chargingProfileKind === 'Relative' ? (txStartMs > 0 ? txStartMs : s.receivedMs) : absolute;
  const elapsedS = Math.max(0, (nowMs - anchor) / 1000);
  let limit = sched.chargingSchedulePeriod[0]?.limit ?? Number.POSITIVE_INFINITY;
  for (const period of sched.chargingSchedulePeriod) {
    if (period.startPeriod <= elapsedS) limit = period.limit;
    else break;
  }
  if (sched.duration !== undefined && elapsedS > sched.duration) return Number.POSITIVE_INFINITY;
  return sched.chargingRateUnit === 'A' ? limit * AC_VOLTAGE * phases : limit;
}
