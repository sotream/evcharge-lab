/** Small deterministic PRNG (mulberry32): the same seed gives the same fleet behaviour. */
export type Rng = () => number;

export function mulberry32(seed: number): Rng {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export const clamp = (v: number, lo: number, hi: number): number => Math.min(hi, Math.max(lo, v));

/** Poisson sample (Knuth for small means, normal approximation above 30). */
export function poisson(mean: number, rng: Rng): number {
  if (mean <= 0) return 0;
  if (mean > 30) {
    const u1 = Math.max(rng(), 1e-12);
    const n = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * rng());
    return Math.max(0, Math.round(mean + Math.sqrt(mean) * n));
  }
  const l = Math.exp(-mean);
  let k = 0;
  let p = 1;
  do {
    k++;
    p *= rng();
  } while (p > l);
  return k - 1;
}

/** Log-normal sample with the given arithmetic mean and sigma of the underlying normal. */
export function logNormal(mean: number, sigma: number, rng: Rng): number {
  const mu = Math.log(mean) - (sigma * sigma) / 2;
  const u1 = Math.max(rng(), 1e-12);
  const n = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * rng());
  return Math.exp(mu + sigma * n);
}

export const pad = (n: number, width: number): string => String(n).padStart(width, '0');

export const errorMessage = (err: unknown): string => (err instanceof Error ? err.message : String(err));

/** Simulated clock: `scale` simulated seconds pass per real second (ADR 0013 SIM_TIME_SCALE). */
export class SimClock {
  constructor(
    readonly scale: number,
    readonly startSimMs: number = Date.now(),
    private readonly startRealMs: number = Date.now(),
    private readonly realNow: () => number = Date.now,
  ) {}

  /** Simulated epoch milliseconds. */
  now(): number {
    return this.startSimMs + (this.realNow() - this.startRealMs) * this.scale;
  }

  iso(simMs: number = this.now()): string {
    return new Date(simMs).toISOString();
  }

  /** Real milliseconds a simulated duration takes. */
  realMs(simMs: number): number {
    return simMs / this.scale;
  }
}
