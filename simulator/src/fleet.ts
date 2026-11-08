import { fleet } from '@evcharge/contracts';

/**
 * The fleet convention (ids, power ratings, site caps, secrets) lives in `@evcharge/contracts` `fleet`, shared
 * with `sessions` and the backfill. This file only turns it into the per-station view the simulator needs.
 */
export const CONNECTORS_PER_STATION = fleet.CONNECTORS_PER_STATION;

export interface StationSpec {
  stationId: string;
  siteId: string;
  /** 1-based site number and station number within the site. */
  siteNo: number;
  stationNo: number;
  /** 0-based fleet index. */
  index: number;
  kind: 'AC' | 'DC';
  /** Per-connector maximum power in W. */
  connectorMaxW: number;
  phases: number;
}

export function stationSpec(siteNo: number, stationNo: number, perSite: number, dcShare: number): StationSpec {
  const index = (siteNo - 1) * perSite + (stationNo - 1);
  const p = fleet.stationPower(index, dcShare);
  return {
    stationId: fleet.stationIdFor(siteNo, stationNo),
    siteId: fleet.siteIdFor(siteNo),
    siteNo,
    stationNo,
    index,
    kind: p.currentType,
    connectorMaxW: p.maxPowerW,
    phases: p.phases,
  };
}

export function buildFleet(sites: number, perSite: number, dcShare: number): StationSpec[] {
  const out: StationSpec[] = [];
  for (let s = 1; s <= sites; s++) for (let n = 1; n <= perSite; n++) out.push(stationSpec(s, n, perSite, dcShare));
  return out;
}

/** The cap `sessions` seeds for a site, computed from the shared convention (used until the real value is read from sessions). */
export function conventionSiteCapW(siteId: string, specs: StationSpec[]): number {
  return fleet.siteCap(siteId, specs.reduce((a, s) => a + s.connectorMaxW * CONNECTORS_PER_STATION, 0));
}
