/**
 * Remembers the commit LSN that sessions returned for a driver's last write, so the driver's next reads can send it back
 * as x-min-lsn and see their own write even when sessions reads from a replica (ADR 0015).
 *
 * shortcut: in-process map, so with several driver-api instances a read that lands on another instance carries no
 * header and sessions reads the primary (correct, just not offloaded). Upgrade: store the LSN in Redis under
 * `lsn:<driver>[:<resource>]` with the same TTL, or route a driver to one instance.
 */
const MAX_ENTRIES = 50_000;

const parts = (lsn: string): [bigint, bigint] | undefined => {
  const m = /^([0-9A-Fa-f]{1,8})\/([0-9A-Fa-f]{1,8})$/.exec(lsn);
  return m ? [BigInt(`0x${m[1]}`), BigInt(`0x${m[2]}`)] : undefined;
};
const later = (a: string, b: string): string => {
  const pa = parts(a);
  const pb = parts(b);
  if (!pa) return b;
  if (!pb) return a;
  return pa[0] > pb[0] || (pa[0] === pb[0] && pa[1] >= pb[1]) ? a : b;
};

export class LsnTracker {
  private readonly entries = new Map<string, { lsn: string; expires: number }>();

  constructor(
    private readonly ttlMs: number,
    private readonly now: () => number = Date.now,
  ) {}

  /** Records the LSN for the driver and, when given, for one resource such as `session:<id>`. Invalid LSNs are ignored. */
  record(driverId: string, lsn: string | null | undefined, resource?: string): void {
    if (this.ttlMs <= 0 || !lsn || !parts(lsn)) return;
    this.put(`d:${driverId}`, lsn);
    if (resource) this.put(`r:${driverId}:${resource}`, lsn);
  }

  /** The LSN to send as x-min-lsn: the later of the driver's last write and the resource's last write, if still fresh. */
  minLsn(driverId: string, resource?: string): string | undefined {
    const a = this.get(`d:${driverId}`);
    const b = resource ? this.get(`r:${driverId}:${resource}`) : undefined;
    return a === undefined ? b : b === undefined ? a : later(a, b);
  }

  private put(key: string, lsn: string): void {
    const prev = this.get(key);
    if (this.entries.size >= MAX_ENTRIES) this.sweep();
    if (this.entries.size >= MAX_ENTRIES) return; // still full of fresh entries: skip rather than grow without bound
    this.entries.set(key, { lsn: prev === undefined ? lsn : later(prev, lsn), expires: this.now() + this.ttlMs });
  }

  private get(key: string): string | undefined {
    const e = this.entries.get(key);
    if (!e) return undefined;
    if (e.expires <= this.now()) {
      this.entries.delete(key);
      return undefined;
    }
    return e.lsn;
  }

  private sweep(): void {
    const t = this.now();
    for (const [k, v] of this.entries) if (v.expires <= t) this.entries.delete(k);
  }
}
