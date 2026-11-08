const MAX_ENTRIES = 2000;

/**
 * In-process TTL cache with request coalescing: concurrent misses for one key share a single downstream call,
 * failures are not cached. shortcut: per instance (no shared cache), so staleness is at most ttlMs on each
 * instance; fine for map data, move to Redis if per-instance hit rates drop with many replicas.
 */
export class TtlCache<V> {
  private readonly entries = new Map<string, { expires: number; value: Promise<V> }>();

  constructor(
    private readonly ttlMs: number,
    private readonly now: () => number = () => Date.now(),
    private readonly onLookup: (hit: boolean) => void = () => {},
  ) {}

  get(key: string, load: () => Promise<V>): Promise<V> {
    if (this.ttlMs <= 0) return load();
    const t = this.now();
    const e = this.entries.get(key);
    if (e && e.expires > t) {
      this.onLookup(true);
      return e.value;
    }
    this.onLookup(false);
    if (this.entries.size >= MAX_ENTRIES) this.prune(t);
    const value = load();
    this.entries.set(key, { expires: t + this.ttlMs, value });
    value.catch(() => {
      if (this.entries.get(key)?.value === value) this.entries.delete(key);
    });
    return value;
  }

  private prune(t: number): void {
    for (const [k, e] of this.entries) if (e.expires <= t) this.entries.delete(k);
    // still full of live entries: drop the oldest inserted ones
    for (const k of this.entries.keys()) {
      if (this.entries.size < MAX_ENTRIES) break;
      this.entries.delete(k);
    }
  }
}
