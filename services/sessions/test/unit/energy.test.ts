import { describe, expect, it } from 'vitest';
import { energyFlag } from '../../src/energy';
const t0 = new Date('2026-11-10T10:00:00Z');
const h = (n: number) => new Date(t0.getTime() + n * 36e5);
describe('energyFlag', () => {
  it('ok within max power x duration', () => expect(energyFlag(1000, 12000, t0, h(1), 11000)).toBe('ok'));
  it('meter_regressed when stop < start', () => expect(energyFlag(5000, 4000, t0, h(1), 11000)).toBe('meter_regressed'));
  it('implausible above 1.1 x max power x duration', () => expect(energyFlag(0, 12_200, t0, h(1), 11000)).toBe('implausible'));
  it('ok exactly at the 1.1 bound', () => expect(energyFlag(0, 12_100, t0, h(1), 11000)).toBe('ok'));
  it('zero-length session with energy is implausible', () => expect(energyFlag(0, 10, t0, t0, 11000)).toBe('implausible'));
});
