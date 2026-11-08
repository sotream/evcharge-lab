/**
 * Gateway side of the three smart-charging layers (ADR 0006, contracts `ChargingProfiles`):
 *   - start default: TxDefaultProfile, stackLevel 0, limit startW, sent after an accepted Boot;
 *   - allocation:    TxProfile, stackLevel 1, validTo, sent by sessions through station.commands;
 *   - safe share:    TxProfile, stackLevel 1, no validTo, limit safeShareW, sent by the gateway
 *                    only when the whole site is stale (RedisKeys.siteAllocationValidTo + ALLOCATION_GRACE_S).
 */
import { ocpp } from '@evcharge/contracts';

const { ChargingProfiles } = ocpp;
type SetChargingProfileReq = ocpp.CsmsReq<'SetChargingProfile'>;

const flatProfile = (id: number, stackLevel: number, purpose: 'TxDefaultProfile' | 'TxProfile', limitW: number): SetChargingProfileReq['csChargingProfiles'] => ({
  chargingProfileId: id,
  stackLevel,
  chargingProfilePurpose: purpose,
  chargingProfileKind: 'Relative',
  chargingSchedule: { chargingRateUnit: 'W', chargingSchedulePeriod: [{ startPeriod: 0, limit: limitW }] },
});

/** TxDefaultProfile on connector 0 (all connectors). */
export function startDefaultProfile(startW: number): SetChargingProfileReq {
  return { connectorId: 0, csChargingProfiles: flatProfile(ChargingProfiles.DEFAULT_PROFILE_ID, 0, 'TxDefaultProfile', startW) };
}

/** Safe-share TxProfile for one connector. Same id, purpose and stack level as that connector's allocation, so it replaces it. */
export function safeShareProfile(connectorId: number, safeShareW: number): SetChargingProfileReq {
  return { connectorId, csChargingProfiles: flatProfile(ocpp.txProfileId(connectorId), 1, 'TxProfile', safeShareW) };
}

/** An allocation is a stackLevel-1 TxProfile with a validTo on a real connector. */
export function allocationValidTo(req: SetChargingProfileReq): number | undefined {
  const p = req.csChargingProfiles;
  if (req.connectorId < 1 || p.chargingProfilePurpose !== 'TxProfile' || p.stackLevel !== 1 || !p.validTo) return undefined;
  const t = Date.parse(p.validTo);
  return Number.isFinite(t) ? t : undefined;
}

/** True when the request replaces the stack-level-1 TxProfile with one that never expires. */
export function replacesAllocationWithoutExpiry(req: SetChargingProfileReq): boolean {
  const p = req.csChargingProfiles;
  return req.connectorId >= 1 && p.chargingProfilePurpose === 'TxProfile' && p.stackLevel === 1 && !p.validTo;
}

/**
 * Allocation TxProfiles this gateway delivered (and the station accepted), per connector:
 * stationId -> connectorId -> validTo (ms). `expired()` lists connectors whose allocation expired at
 * least `graceMs` ago. Entries stay until a newer profile replaces them or the station disconnects,
 * so the sweep can re-check them until the site-wide staleness test passes. `attempted()` throttles
 * resends of a safe share that was not accepted.
 */
export class AllocationTracker {
  private readonly byStation = new Map<string, Map<number, { validToMs: number; attemptedMs: number }>>();

  record(stationId: string, connectorId: number, validToMs: number): void {
    let m = this.byStation.get(stationId);
    if (!m) {
      m = new Map();
      this.byStation.set(stationId, m);
    }
    m.set(connectorId, { validToMs, attemptedMs: 0 });
  }

  clear(stationId: string, connectorId: number): void {
    const m = this.byStation.get(stationId);
    if (!m) return;
    m.delete(connectorId);
    if (m.size === 0) this.byStation.delete(stationId);
  }

  clearStation(stationId: string): void {
    this.byStation.delete(stationId);
  }

  /** Connectors whose allocation expired at least `graceMs` ago and that were not tried in the last `retryMs`. */
  expired(nowMs: number, graceMs: number, retryMs = 0): { stationId: string; connectorId: number }[] {
    const out: { stationId: string; connectorId: number }[] = [];
    for (const [stationId, m] of this.byStation) {
      for (const [connectorId, e] of m) {
        if (nowMs >= e.validToMs + graceMs && nowMs - e.attemptedMs >= retryMs) out.push({ stationId, connectorId });
      }
    }
    return out;
  }

  attempted(stationId: string, connectorId: number, nowMs: number): void {
    const e = this.byStation.get(stationId)?.get(connectorId);
    if (e) e.attemptedMs = nowMs;
  }

  get size(): number {
    let n = 0;
    for (const m of this.byStation.values()) n += m.size;
    return n;
  }
}
