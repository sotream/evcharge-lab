import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { arrivalRatePerS, localHour, MEAN_UTILISATION, PEAK_UTILISATION, randomVehicle, sessionDurationMs, utilisation } from '../../src/demand';
import { fleet } from '@evcharge/contracts';
import { buildFleet, conventionSiteCapW, stationSpec } from '../../src/fleet';
import { loadSimConfig } from '../../src/config';
import { logNormal, mulberry32, poisson, SimClock } from '../../src/util';

describe('fleet comes from the shared contracts convention', () => {
  it('maps every station to fleet.stationPower and the shared id helpers', () => {
    const f = buildFleet(3, 10, 0.2);
    expect(f).toHaveLength(30);
    expect(f[0]?.stationId).toBe('st-0001-001');
    expect(f[0]?.siteId).toBe('site-0001');
    expect(f[29]?.stationId).toBe(fleet.stationIdFor(3, 10));
    f.forEach((s, i) => {
      const p = fleet.stationPower(i, 0.2);
      expect(s.kind).toBe(p.currentType);
      expect(s.connectorMaxW).toBe(p.maxPowerW);
    });
    expect(f.filter((s) => s.kind === 'DC')).toHaveLength(6);
    expect(JSON.stringify(buildFleet(3, 10, 0.2))).toBe(JSON.stringify(f));
    expect(stationSpec(1, 5, 10, 0.2).kind).toBe('DC'); // index 4
    expect(stationSpec(1, 4, 10, 0.2).kind).toBe('AC');
  });

  it('the convention cap equals the cap sessions seeds for the same site', () => {
    const specs = buildFleet(1, 10, 0.2);
    const seeded = fleet.buildSite(1, { sites: 1, stationsPerSite: 10, dcShare: 0.2, secretSeed: 'seed-123456' });
    expect(conventionSiteCapW('site-0001', specs)).toBe(seeded.capW);
  });

  it('station secret is the shared HMAC convention', () => {
    const s = fleet.stationSecret('seed-123456', 'st-0001-001');
    expect(s).toHaveLength(32);
    expect(s).toBe(createHmac('sha256', 'seed-123456').update('st-0001-001').digest('hex').slice(0, 32));
  });
});

describe('demand model (ADR 0000)', () => {
  it('daily utilisation averages 10% and peaks at 40% in the evening', () => {
    const hours = Array.from({ length: 1440 }, (_, i) => utilisation(i / 60));
    const mean = hours.reduce((a, b) => a + b, 0) / hours.length;
    expect(mean).toBeCloseTo(MEAN_UTILISATION, 3);
    expect(Math.max(...hours)).toBeCloseTo(PEAK_UTILISATION, 3);
    const peakHour = hours.indexOf(Math.max(...hours)) / 60;
    expect(peakHour).toBeGreaterThan(17.5);
    expect(peakHour).toBeLessThan(19.5);
    expect(Math.min(...hours)).toBeGreaterThan(0);
  });

  it('arrival rate gives the target busy fraction for blocking arrivals (Erlang loss)', () => {
    const u = 0.25;
    const lambda = arrivalRatePerS(u, 2700);
    expect((lambda * 2700) / (1 + lambda * 2700)).toBeCloseTo(u, 10);
  });

  it('simulating one connector reproduces ~10% utilisation and ~45 min mean AC sessions', () => {
    const rng = mulberry32(42);
    const u = 0.1;
    let busyUntil = 0;
    let busy = 0;
    let n = 0;
    const durs: number[] = [];
    const lambda = arrivalRatePerS(u, 45 * 60);
    const total = 400 * 86_400; // 400 simulated days, 1 s steps would be slow; use event jumps
    let t = 0;
    while (t < total) {
      t += -Math.log(1 - rng()) / lambda;
      if (t >= busyUntil) {
        const d = sessionDurationMs('AC', rng) / 1000;
        busyUntil = t + d;
        busy += d;
        n++;
        durs.push(d);
      }
    }
    expect(busy / total).toBeGreaterThan(0.085);
    expect(busy / total).toBeLessThan(0.115);
    expect(durs.reduce((a, b) => a + b, 0) / n / 60).toBeGreaterThan(40);
    expect(durs.reduce((a, b) => a + b, 0) / n / 60).toBeLessThan(50);
  });

  it('DC sessions are shorter than AC sessions', () => {
    const rng = mulberry32(3);
    const mean = (kind: 'AC' | 'DC') => Array.from({ length: 4000 }, () => sessionDurationMs(kind, rng)).reduce((a, b) => a + b, 0) / 4000;
    expect(mean('DC')).toBeLessThan(mean('AC') * 0.75);
  });

  it('vehicles are plausible', () => {
    const rng = mulberry32(5);
    for (let i = 0; i < 200; i++) {
      const { vehicle, soc0 } = randomVehicle(i % 2 ? 'AC' : 'DC', rng);
      expect(vehicle.capacityWh).toBeGreaterThanOrEqual(40_000);
      expect(soc0).toBeGreaterThanOrEqual(0.1);
      expect(soc0).toBeLessThan(0.6);
    }
  });

  it('local hour follows the UTC offset', () => {
    expect(localHour(Date.parse('2026-11-08T17:30:00Z'), 1)).toBeCloseTo(18.5, 6);
    expect(localHour(Date.parse('2026-11-08T23:30:00Z'), 2)).toBeCloseTo(1.5, 6);
  });
});

describe('util', () => {
  it('poisson and logNormal have the requested means', () => {
    const rng = mulberry32(9);
    expect(avg(() => poisson(3, rng))).toBeCloseTo(3, 0);
    expect(avg(() => poisson(200, rng))).toBeCloseTo(200, -1);
    expect(avg(() => logNormal(100, 0.5, rng))).toBeGreaterThan(95);
    expect(avg(() => logNormal(100, 0.5, rng))).toBeLessThan(105);
  });

  it('SimClock scales time', () => {
    let real = 1_000;
    const c = new SimClock(60, 10_000, 1_000, () => real);
    real += 1_000;
    expect(c.now()).toBe(10_000 + 60_000);
    expect(c.realMs(30_000)).toBe(500);
  });
});

const avg = (f: () => number) => Array.from({ length: 20_000 }, f).reduce((a, b) => a + b, 0) / 20_000;

describe('config', () => {
  it('validates and lists every problem', () => {
    expect(() => loadSimConfig({})).toThrow(/CSMS_URL/);
    expect(() => loadSimConfig({ CSMS_URL: 'http://x', STATION_SECRET_SEED: 'short' })).toThrow(/STATION_SECRET_SEED/);
    const c = loadSimConfig({ CSMS_URL: 'ws://lb:8080/ocpp', STATION_SECRET_SEED: 'long-enough-seed', SIM_SITES: '3', SIM_AUTOSTART: 'false' });
    expect(c.SIM_SITES).toBe(3);
    expect(c.SIM_AUTOSTART).toBe(false);
    expect(c.SIM_TIME_SCALE).toBe(1);
  });
});
