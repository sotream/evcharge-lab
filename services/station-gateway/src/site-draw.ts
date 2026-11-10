import type { GatewayMetrics } from './metrics';
import type { Reading } from './ocpp/meter';

/** A reading older than this no longer counts: 2 x the 30 s MeterValues sample interval (a silent station is not drawing). */
export const MAX_READING_AGE_MS = 60_000;

interface Draw {
  watts: number;
  at: number;
  transactionId?: number;
}

/**
 * Site draw measured from the stations' own live Power.Active.Import (ADR 0006 cap check, M9): per site, the sum
 * over this instance's connected stations of each connector's latest reading. Replayed readings are ignored. A value
 * disappears when the connection closes, the transaction stops, the connector leaves Charging, or it is older than
 * MAX_READING_AGE_MS, so capacity the allocator hands to the next session is never counted twice. Keyed by connection
 * object, so a replaced connection closing late cannot wipe its successor's values.
 */
export class SiteDrawTracker {
  private readonly sites = new Map<string, Map<object, Map<number, Draw>>>();

  constructor(
    private readonly metrics: Pick<GatewayMetrics, 'siteDrawn' | 'siteCap'>,
    private readonly instance: string,
    private readonly now: () => number = Date.now,
  ) {}

  setCap(siteId: string, capW: number): void {
    this.metrics.siteCap.set({ gateway: this.instance, site_id: siteId }, capW);
  }

  /** Records the newest live Power.Active.Import (W or kW) of `readings` for the connector; per-phase values (L1-L3) are summed when no total is reported. */
  meterValues(conn: object, siteId: string, connectorId: number, readings: Reading[], replayed: boolean, transactionId?: number): void {
    if (replayed || connectorId === 0) return;
    let newestAt = -1;
    let watts: number | undefined;
    for (const r of readings) {
      const at = Date.parse(r.timestamp);
      if (at < newestAt) continue;
      let total: number | undefined;
      let phases: number | undefined;
      for (const s of r.samples) {
        if (s.measurand !== 'Power.Active.Import' || (s.unit !== 'W' && s.unit !== 'kW')) continue;
        const w = s.unit === 'kW' ? s.value * 1000 : s.value;
        if (!s.phase) total = w;
        else if (/^L[123]$/.test(s.phase)) phases = (phases ?? 0) + w;
      }
      const w = total ?? phases;
      if (w === undefined) continue;
      newestAt = at;
      watts = Math.max(0, Math.round(w));
    }
    if (watts === undefined) return;
    const site = this.sites.get(siteId) ?? new Map<object, Map<number, Draw>>();
    const conns = site.get(conn) ?? new Map<number, Draw>();
    conns.set(connectorId, { watts, at: this.now(), ...(transactionId !== undefined ? { transactionId } : {}) });
    site.set(conn, conns);
    this.sites.set(siteId, site);
    this.publish(siteId);
  }

  /** The connector stopped drawing (StatusNotification other than Charging). */
  clearConnector(conn: object, siteId: string, connectorId: number): void {
    if (this.sites.get(siteId)?.get(conn)?.delete(connectorId)) this.publish(siteId);
  }

  /** StopTransaction carries no connector id: drop the connector whose MeterValues named this transaction. */
  clearTransaction(conn: object, siteId: string, transactionId: number): void {
    const conns = this.sites.get(siteId)?.get(conn);
    if (!conns) return;
    let changed = false;
    for (const [cid, d] of conns) {
      if (d.transactionId === transactionId) {
        conns.delete(cid);
        changed = true;
      }
    }
    if (changed) this.publish(siteId);
  }

  clear(conn: object, siteId: string): void {
    const site = this.sites.get(siteId);
    if (!site?.delete(conn)) return;
    if (site.size === 0) this.sites.delete(siteId);
    this.publish(siteId);
  }

  /** Drops readings older than MAX_READING_AGE_MS; called periodically. */
  expire(): void {
    const cutoff = this.now() - MAX_READING_AGE_MS;
    for (const [siteId, site] of this.sites) {
      let changed = false;
      for (const conns of site.values()) {
        for (const [cid, d] of conns) {
          if (d.at < cutoff) {
            conns.delete(cid);
            changed = true;
          }
        }
      }
      if (changed) this.publish(siteId);
    }
  }

  private publish(siteId: string): void {
    let sum = 0;
    for (const conns of this.sites.get(siteId)?.values() ?? []) for (const d of conns.values()) sum += d.watts;
    this.metrics.siteDrawn.set({ gateway: this.instance, site_id: siteId }, sum);
  }
}
