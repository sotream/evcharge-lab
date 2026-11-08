import { describe, expect, it } from 'vitest';
import { fleet } from '@evcharge/contracts';

const { AC_MIN_W, buildCatalog, buildSite, chargingDefaults, isDc, siteIdFor, stationIdFor, stationSecret, stationSecretSha256, unit } = fleet;

const opts = { sites: 50, stationsPerSite: 10, dcShare: 0.2, secretSeed: 'dev-station-seed-change-me' };

describe('seed convention shared with the simulator', () => {
  it('ids', () => {
    expect(siteIdFor(1)).toBe('site-0001');
    expect(stationIdFor(1, 1)).toBe('st-0001-001');
    expect(stationIdFor(123, 40)).toBe('st-0123-040');
  });

  it('station secret = HMAC-SHA256(seed, stationId) hex truncated to 32 chars; credential stores its SHA-256', () => {
    expect(stationSecret(opts.secretSeed, 'st-0001-001')).toBe('7cf02abaf04e892ece6e1e37740273b8');
    expect(stationSecretSha256(opts.secretSeed, 'st-0001-001')).toBe('8f612d01ec8233573461e560c410d0cede087623c2deae0a12dad67d8373181a');
  });

  it('is deterministic', () => {
    expect([...buildCatalog(opts)]).toEqual([...buildCatalog(opts)]);
    expect(unit('x')).toBe(unit('x'));
    expect(unit('x')).toBeGreaterThanOrEqual(0);
    expect(unit('x')).toBeLessThan(1);
  });

  it('DC share is exact over the fleet', () => {
    const n = 1000;
    expect(Array.from({ length: n }, (_, i) => isDc(i, 0.2)).filter(Boolean)).toHaveLength(200);
    expect(Array.from({ length: n }, (_, i) => isDc(i, 0)).filter(Boolean)).toHaveLength(0);
  });

  it('every station has 2 connectors; AC 11/22 kW, DC 50-150 kW; tariffs std/tou/fleet', () => {
    const tariffs = new Set<string>();
    for (const site of buildCatalog(opts)) {
      for (const st of site.stations) {
        expect(st.connectors).toHaveLength(2);
        expect(st.secretSha256).toMatch(/^[0-9a-f]{64}$/);
        for (const c of st.connectors) {
          tariffs.add(c.tariffId);
          const allowed = c.currentType === 'AC' ? [11_000, 22_000] : [50_000, 75_000, 100_000, 150_000];
          expect(allowed).toContain(c.maxPowerW);
          expect(c.minPowerW).toBeLessThan(c.maxPowerW);
        }
      }
    }
    expect([...tariffs].sort()).toEqual(['fleet', 'std', 'tou']);
  });

  it('site cap is 40-60% of the summed connector maximum, so smart charging matters, and spread around Berlin', () => {
    for (const site of buildCatalog(opts)) {
      const sum = site.stations.reduce((s, st) => s + st.connectors.reduce((a, c) => a + c.maxPowerW, 0), 0);
      expect(site.capW).toBeLessThan(sum * 0.6 + 1);
      expect(site.capW).toBeGreaterThan(sum * 0.4 - 1000);
      expect(Math.abs(site.lat - 52.52)).toBeLessThan(0.2);
      expect(Math.abs(site.lng - 13.405)).toBeLessThan(0.3);
    }
  });

  it('site 1 does not depend on how many sites are seeded', () => {
    expect(buildSite(1, opts)).toEqual(buildSite(1, { ...opts, sites: 3 }));
  });
});

describe('chargingDefaults (ADR 0006)', () => {
  it('startW = min(minW, floor(cap / connectors)), safeShareW = floor(cap / connectors)', () => {
    expect(chargingDefaults(100_000, 20)).toEqual({ startW: AC_MIN_W, safeShareW: 5000 });
    expect(chargingDefaults(40_000, 20)).toEqual({ startW: 2000, safeShareW: 2000 });
    expect(chargingDefaults(0, 4)).toEqual({ startW: 0, safeShareW: 0 });
    expect(chargingDefaults(1000, 0)).toEqual({ startW: 0, safeShareW: 0 });
  });

  it('start reserve never exceeds the cap whatever the site size', () => {
    for (const cap of [0, 1, 999, 12_345, 99_999]) {
      for (const n of [1, 2, 7, 80]) {
        const d = chargingDefaults(cap, n);
        expect(d.startW).toBeLessThanOrEqual(d.safeShareW);
        expect(d.startW * n).toBeLessThanOrEqual(cap);
        expect(d.safeShareW * n).toBeLessThanOrEqual(cap);
      }
    }
  });
});
