import { clamp, logNormal, type Rng } from './util';
import type { Kind, Vehicle } from './physics';

/**
 * Demand model (ADR 0000 / 0013): connector utilisation averages 10% over a day and peaks at 40%
 * in the evening; sessions last about 45 minutes (DC shorter).
 */
export const MEAN_UTILISATION = 0.1;
export const PEAK_UTILISATION = 0.4;
export const MEAN_DURATION_MIN = { AC: 45, DC: 25 } as const;

const gauss = (h: number, centre: number, sigma: number): number => {
  const d = Math.min(Math.abs(h - centre), 24 - Math.abs(h - centre));
  return Math.exp(-(d * d) / (2 * sigma * sigma));
};
/** 0..1 daily shape: a strong evening peak around 18:30 and a smaller morning bump. */
const shape = (h: number): number => Math.max(gauss(h, 18.5, 1.8), 0.3 * gauss(h, 8.5, 1.5));
const SHAPE_MEAN = (() => {
  let s = 0;
  for (let i = 0; i < 1440; i++) s += shape(i / 60);
  return s / 1440;
})();
const B = (PEAK_UTILISATION - MEAN_UTILISATION) / (1 - SHAPE_MEAN);
const A = PEAK_UTILISATION - B;

/** Target fraction of connectors in use at a local hour [0,24). Daily mean = 10%, maximum = 40%. */
export const utilisation = (localHour: number): number => A + B * shape(((localHour % 24) + 24) % 24);

export const localHour = (simMs: number, utcOffsetH: number): number => {
  const d = new Date(simMs);
  return (d.getUTCHours() + d.getUTCMinutes() / 60 + d.getUTCSeconds() / 3600 + utcOffsetH + 24) % 24;
};

/**
 * Arrival attempts per second on one connector such that blocking (an attempt on a busy connector
 * is lost) yields the target busy fraction u: p = lD / (1 + lD)  =>  l = u / ((1 - u) D).
 */
export const arrivalRatePerS = (u: number, meanDurationS: number): number => u / ((1 - u) * meanDurationS);

export function sessionDurationMs(kind: Kind, rng: Rng): number {
  const mean = MEAN_DURATION_MIN[kind] * 60_000;
  return clamp(logNormal(mean, 0.5, rng), 3 * 60_000, 4 * mean);
}

export function randomVehicle(kind: Kind, rng: Rng): { vehicle: Vehicle; soc0: number } {
  const capacityWh = 40_000 + Math.floor(rng() * 5) * 10_000; // 40..80 kWh
  const soc0 = 0.1 + rng() * 0.5;
  const maxW =
    kind === 'AC'
      ? [3_700, 7_400, 11_000, 11_000, 22_000][Math.floor(rng() * 5)] ?? 11_000
      : [50_000, 80_000, 120_000, 150_000][Math.floor(rng() * 4)] ?? 100_000;
  return { vehicle: { capacityWh, maxW }, soc0 };
}

/** A vehicle that never fills up and stays plugged in: used by probe stations so they emit continuously. */
export const probeVehicle = (kind: Kind): { vehicle: Vehicle; soc0: number } => ({
  vehicle: { capacityWh: 10_000_000, maxW: kind === 'AC' ? 22_000 : 150_000 },
  soc0: 0.2,
});
