import { describe, expect, it } from 'vitest';
import { advanceCharge, curveW, dcTaper, drawnW, minChargeW, type ChargeState } from '../../src/physics';

const car = { capacityWh: 60_000, maxW: 22_000 };

describe('curves', () => {
  it('AC is flat at min(connector, vehicle) whatever the SoC', () => {
    expect(curveW('AC', 11_000, car, 0.1)).toBe(11_000);
    expect(curveW('AC', 11_000, car, 0.99)).toBe(11_000);
    expect(curveW('AC', 22_000, { capacityWh: 1, maxW: 7_400 }, 0.5)).toBe(7_400);
  });

  it('DC holds full power to 80% SoC, then tapers linearly to 10% at 100%', () => {
    expect(dcTaper(0.5)).toBe(1);
    expect(dcTaper(0.8)).toBe(1);
    expect(dcTaper(0.9)).toBeCloseTo(0.55, 10);
    expect(dcTaper(1)).toBeCloseTo(0.1, 10);
    expect(curveW('DC', 150_000, { capacityWh: 60_000, maxW: 120_000 }, 0.6)).toBe(120_000);
    expect(curveW('DC', 50_000, { capacityWh: 60_000, maxW: 120_000 }, 1)).toBeCloseTo(5_000, 6);
  });

  it('an OCPP limit caps the draw; below the minimum charge power the EVSE draws nothing', () => {
    expect(drawnW(11_000, 4_140, 4_140)).toBe(4_140);
    expect(drawnW(11_000, Number.POSITIVE_INFINITY, 4_140)).toBe(11_000);
    expect(drawnW(11_000, 3_000, minChargeW('AC'))).toBe(0);
    expect(minChargeW('AC')).toBe(4_140);
  });
});

const fresh = (soc: number, energyWh = 0): ChargeState => ({ tMs: 0, energyWh, soc, powerW: 0 });

describe('advanceCharge', () => {
  const base = { kind: 'AC' as const, connectorMaxW: 11_000, phases: 3, vehicle: car, limitAt: () => Number.POSITIVE_INFINITY };

  it('integrates energy from power: 11 kW for one hour is 11 kWh and SoC follows', () => {
    const s = advanceCharge(fresh(0.2), 3_600_000, base);
    expect(s.energyWh).toBeCloseTo(11_000, 3);
    expect(s.soc).toBeCloseTo(0.2 + 11_000 / 60_000, 6);
    expect(s.powerW).toBe(11_000);
    expect(s.tMs).toBe(3_600_000);
  });

  it('energy register is monotonic and consistent with sampled power on every step', () => {
    const s = fresh(0.3, 1_000);
    let last = s.energyWh;
    for (let t = 30_000; t <= 3_600_000; t += 30_000) {
      const before = s.energyWh;
      advanceCharge(s, t, { ...base, kind: 'DC', connectorMaxW: 100_000, vehicle: { capacityWh: 60_000, maxW: 100_000 } });
      expect(s.energyWh).toBeGreaterThanOrEqual(last);
      // energy added over the 30 s step is bounded by the step's start and end power
      expect(s.energyWh - before).toBeLessThanOrEqual((100_000 * 30) / 3600 + 1e-9);
      last = s.energyWh;
    }
    expect(s.soc).toBeLessThanOrEqual(1);
  });

  it('a DC session slows down in the taper: later minutes add less energy than earlier ones', () => {
    const dc = { ...base, kind: 'DC' as const, connectorMaxW: 100_000, vehicle: { capacityWh: 50_000, maxW: 100_000 } };
    const s = fresh(0.5);
    advanceCharge(s, 60_000, dc);
    const early = s.energyWh;
    // run until SoC ~ 0.9
    let t = 60_000;
    while (s.soc < 0.9) advanceCharge(s, (t += 10_000), dc);
    const e0 = s.energyWh;
    advanceCharge(s, t + 60_000, dc);
    expect(s.energyWh - e0).toBeLessThan(early);
    expect(s.powerW).toBeLessThan(100_000);
  });

  it('applies the limit in effect at each step (profile expiring mid-way)', () => {
    // 4 kW for the first 10 min, then no limit
    const s = advanceCharge(fresh(0.1), 1_200_000, { ...base, limitAt: (t) => (t < 600_000 ? 4_140 : Number.POSITIVE_INFINITY) });
    expect(s.energyWh).toBeCloseTo((4_140 * 600) / 3600 + (11_000 * 600) / 3600, 3);
  });

  it('stops drawing power when the battery is full', () => {
    const s = advanceCharge(fresh(0.99), 3_600_000, base);
    expect(s.soc).toBe(1);
    expect(s.energyWh).toBeCloseTo(600, 0);
    expect(s.powerW).toBe(0);
  });
});
