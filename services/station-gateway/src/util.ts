/** Token bucket: `ratePerS` tokens per second, burst = ratePerS (ADR 0003 admission control). */
export class TokenBucket {
  private tokens: number;
  private last: number;

  constructor(
    private readonly ratePerS: number,
    private readonly burst: number = ratePerS,
    private readonly now: () => number = () => performance.now(),
  ) {
    this.tokens = burst;
    this.last = now();
  }

  tryTake(): boolean {
    const t = this.now();
    this.tokens = Math.min(this.burst, this.tokens + ((t - this.last) / 1000) * this.ratePerS);
    this.last = t;
    if (this.tokens < 1) return false;
    this.tokens -= 1;
    return true;
  }
}

/** Bounded LRU with per-entry TTL (Map keeps insertion order; a hit re-inserts the key). */
export class TtlLru<V> {
  private readonly map = new Map<string, { value: V; expiresAt: number }>();

  constructor(
    private readonly max: number,
    private readonly now: () => number = Date.now,
  ) {}

  get(key: string): V | undefined {
    const e = this.map.get(key);
    if (!e) return undefined;
    if (e.expiresAt <= this.now()) {
      this.map.delete(key);
      return undefined;
    }
    this.map.delete(key);
    this.map.set(key, e);
    return e.value;
  }

  set(key: string, value: V, ttlMs: number): void {
    if (ttlMs <= 0) return;
    this.map.delete(key);
    this.map.set(key, { value, expiresAt: this.now() + ttlMs });
    while (this.map.size > this.max) {
      const oldest = this.map.keys().next().value;
      if (oldest === undefined) break;
      this.map.delete(oldest);
    }
  }

  delete(key: string): void {
    this.map.delete(key);
  }

  get size(): number {
    return this.map.size;
  }
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Walks an error tree (cause / AggregateError.errors) looking for a Kafka protocol error id. */
export function kafkaErrorReason(err: unknown): string {
  const seen = new Set<unknown>();
  const stack: unknown[] = [err];
  let timeout = false;
  while (stack.length > 0) {
    const e = stack.pop();
    if (!e || typeof e !== 'object' || seen.has(e)) continue;
    seen.add(e);
    const o = e as { apiId?: unknown; name?: unknown; message?: unknown; cause?: unknown; errors?: unknown };
    if (o.apiId === 'NOT_ENOUGH_REPLICAS' || o.apiId === 'NOT_ENOUGH_REPLICAS_AFTER_APPEND') return 'not_enough_replicas';
    if (o.name === 'TimeoutError' || (typeof o.message === 'string' && /timed? ?out/i.test(o.message))) timeout = true;
    if (o.cause) stack.push(o.cause);
    if (Array.isArray(o.errors)) stack.push(...(o.errors as unknown[]));
  }
  return timeout ? 'timeout' : 'other';
}

/** Uniform random integer in [min, max]. */
export const randInt = (min: number, max: number): number => min + Math.floor(Math.random() * (max - min + 1));
