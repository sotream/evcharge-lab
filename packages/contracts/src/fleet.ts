/**
 * Lab fleet convention (ADR 0013): the single source of station ids, power ratings, site caps and
 * secrets used by sessions (seed), the simulator and scripts/backfill-telemetry (Go mirror, checked
 * against schemas/fleet-fixture.json). Pure functions only: the same inputs always give the same fleet.
 */
import { createHash, createHmac } from 'node:crypto';

/** 6 A x 230 V x 3 phases: the AC charging minimum (ADR 0006). */
export const AC_MIN_W = 6 * 230 * 3;
/** Lowest useful DC power. */
export const DC_MIN_W = 5000;
export const CONNECTORS_PER_STATION = 2;
export const AC_POWERS_W = [11_000, 22_000] as const;
export const DC_POWERS_W = [50_000, 75_000, 100_000, 150_000] as const;
export const TARIFF_IDS = ['std', 'tou', 'fleet'] as const;
const BERLIN = { lat: 52.52, lng: 13.405 };

export interface SeedConnector {
  connectorId: number;
  currentType: 'AC' | 'DC';
  phases: number;
  maxPowerW: number;
  minPowerW: number;
  tariffId: string;
}
export interface SeedStation {
  stationId: string;
  siteId: string;
  lat: number;
  lng: number;
  secretSha256: string;
  connectors: SeedConnector[];
}
export interface SeedSite {
  siteId: string;
  name: string;
  lat: number;
  lng: number;
  capW: number;
  stations: SeedStation[];
}

const pad = (n: number, w: number) => String(n).padStart(w, '0');
export const siteIdFor = (site: number) => `site-${pad(site, 4)}`;
export const stationIdFor = (site: number, n: number) => `st-${pad(site, 4)}-${pad(n, 3)}`;

/** Uniform [0, 1) from a string, stable across runs and languages (first 32 bits of SHA-256). */
export function unit(s: string): number {
  return createHash('sha256').update(s).digest().readUInt32BE(0) / 2 ** 32;
}

/** ADR 0003: secret = HMAC-SHA256(seed, stationId) as hex, truncated to 32 chars (128 bits). */
export function stationSecret(seed: string, stationId: string): string {
  return createHmac('sha256', seed).update(stationId).digest('hex').slice(0, 32);
}
export function stationSecretSha256(seed: string, stationId: string): string {
  return createHash('sha256').update(stationSecret(seed, stationId)).digest('hex');
}

/** Station g (0-based over the whole fleet) is DC when the running DC count steps up: exactly `share` of stations. */
export function isDc(globalIndex: number, dcShare: number): boolean {
  return Math.floor((globalIndex + 1) * dcShare) > Math.floor(globalIndex * dcShare);
}

export function stationPower(globalIndex: number, dcShare: number): { currentType: 'AC' | 'DC'; maxPowerW: number; minPowerW: number; phases: number } {
  if (isDc(globalIndex, dcShare)) return { currentType: 'DC', maxPowerW: DC_POWERS_W[globalIndex % DC_POWERS_W.length] as number, minPowerW: DC_MIN_W, phases: 3 };
  return { currentType: 'AC', maxPowerW: AC_POWERS_W[globalIndex % AC_POWERS_W.length] as number, minPowerW: AC_MIN_W, phases: 3 };
}

/** Site cap = 40-60% of the connectors' summed maximum, rounded down to 1 kW, so smart charging matters. */
export function siteCap(siteId: string, sumMaxW: number): number {
  const fraction = 0.4 + 0.2 * unit(`${siteId}:cap`);
  return Math.floor((sumMaxW * fraction) / 1000) * 1000;
}

export interface SeedOptions {
  sites: number;
  stationsPerSite: number;
  dcShare: number;
  secretSeed: string;
}

export function buildSite(site: number, opts: SeedOptions): SeedSite {
  const siteId = siteIdFor(site);
  const lat = BERLIN.lat + (unit(`${siteId}:lat`) - 0.5) * 0.3;
  const lng = BERLIN.lng + (unit(`${siteId}:lng`) - 0.5) * 0.5;
  const stations: SeedStation[] = [];
  for (let n = 1; n <= opts.stationsPerSite; n++) {
    const g = (site - 1) * opts.stationsPerSite + (n - 1);
    const stationId = stationIdFor(site, n);
    const p = stationPower(g, opts.dcShare);
    const connectors: SeedConnector[] = [];
    for (let c = 1; c <= CONNECTORS_PER_STATION; c++) {
      connectors.push({ connectorId: c, ...p, tariffId: TARIFF_IDS[(2 * g + c - 1) % TARIFF_IDS.length] as string });
    }
    stations.push({
      stationId,
      siteId,
      lat: lat + ((n % 5) - 2) * 0.0002,
      lng: lng + (Math.floor(n / 5) % 5) * 0.0002,
      secretSha256: stationSecretSha256(opts.secretSeed, stationId),
      connectors,
    });
  }
  const sumMaxW = stations.reduce((s, st) => s + st.connectors.reduce((a, c) => a + c.maxPowerW, 0), 0);
  return { siteId, name: `Berlin site ${site}`, lat, lng, capW: siteCap(siteId, sumMaxW), stations };
}

export function* buildCatalog(opts: SeedOptions): Generator<SeedSite> {
  for (let site = 1; site <= opts.sites; site++) yield buildSite(site, opts);
}

/** ADR 0006 chargingDefaults for a site. */
export function chargingDefaults(capW: number, connectorsAtSite: number): { startW: number; safeShareW: number } {
  const safeShareW = connectorsAtSite > 0 ? Math.floor(capW / connectorsAtSite) : 0;
  return { startW: Math.min(AC_MIN_W, safeShareW), safeShareW };
}
