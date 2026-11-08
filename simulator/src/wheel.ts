export interface WheelTimer {
  cancel(): void;
}

interface Entry {
  due: number;
  fn: () => void;
  cancelled: boolean;
}

/**
 * Hashed timing wheel: one interval drives every station timer (heartbeats, meter samples,
 * reconnects, call timeouts), so 10,000+ stations do not create per-station setInterval storms.
 * Resolution is `tickMs`; a timer fires at the first tick at or after its due time. Delays longer
 * than one revolution stay in their slot until due. `advance(now)` is public so tests can drive it.
 */
export class TimerWheel {
  private readonly slots: Entry[][];
  private cursor: number;
  private handle: NodeJS.Timeout | undefined;
  private size = 0;

  constructor(
    private readonly tickMs = 100,
    private readonly slotCount = 2048,
    private readonly now: () => number = Date.now,
    private readonly onError: (err: unknown) => void = () => {},
  ) {
    this.slots = Array.from({ length: slotCount }, () => []);
    this.cursor = Math.floor(now() / tickMs);
  }

  /** Number of scheduled, not yet fired or cancelled timers (cancelled ones are counted until swept). */
  get pending(): number {
    return this.size;
  }

  schedule(delayMs: number, fn: () => void): WheelTimer {
    const due = this.now() + Math.max(0, delayMs);
    const entry: Entry = { due, fn, cancelled: false };
    // Never into a slot the cursor already passed: the earliest firing tick is cursor + 1.
    const tick = Math.max(Math.floor(due / this.tickMs), this.cursor + 1);
    this.slots[tick % this.slotCount]?.push(entry);
    this.size++;
    return {
      cancel: () => {
        entry.cancelled = true;
      },
    };
  }

  /** Runs every timer due at or before `nowMs`. */
  advance(nowMs: number = this.now()): void {
    const target = Math.floor(nowMs / this.tickMs);
    while (this.cursor < target) {
      this.cursor++;
      const idx = this.cursor % this.slotCount;
      const slot = this.slots[idx];
      if (!slot || slot.length === 0) continue;
      this.slots[idx] = [];
      const keep: Entry[] = [];
      for (const e of slot) {
        if (e.cancelled) {
          this.size--;
          continue;
        }
        if (Math.floor(e.due / this.tickMs) > this.cursor) {
          keep.push(e);
          continue;
        }
        this.size--;
        try {
          e.fn();
        } catch (err) {
          this.onError(err);
        }
      }
      // Callbacks may have scheduled into this slot already; keep the survivors too.
      const now = this.slots[idx];
      this.slots[idx] = now && now.length > 0 ? [...keep, ...now] : keep;
    }
  }

  start(): void {
    if (this.handle) return;
    this.handle = setInterval(() => this.advance(), this.tickMs);
  }

  stop(): void {
    if (this.handle) clearInterval(this.handle);
    this.handle = undefined;
  }
}
