export type DurableAction = 'StartTransaction' | 'StopTransaction' | 'MeterValues';

export interface QueuedCall {
  /** Assigned once at enqueue time and reused on every resend (ADR 0003: original uniqueId). */
  uniqueId: string;
  action: DurableAction;
  payload: Record<string, unknown>;
  /** Called with the CALLRESULT payload when the CSMS acknowledged this call. */
  onResult?: (conf: Record<string, unknown>) => void;
  /** Called right before every (re)send, e.g. to patch in a transactionId learned meanwhile. */
  prepare?: (payload: Record<string, unknown>) => void;
}

/**
 * The station's persistent queue of transaction-related CALLs (StartTransaction, StopTransaction,
 * MeterValues). Strict FIFO: only the head may be in flight, and it leaves the queue only when its
 * CALLRESULT arrived. So after any disconnect the next send is the same head with the same
 * uniqueId, which is what a real station does and what makes duplicates possible (ADR 0003).
 */
export class CallQueue {
  private items: QueuedCall[] = [];
  /** Entries dropped to honour the capacity. */
  dropped = 0;

  constructor(private readonly max: number) {}

  get length(): number {
    return this.items.length;
  }

  peek(): QueuedCall | undefined {
    return this.items[0];
  }

  /** Appends; when over capacity, drops the oldest MeterValues (never the in-flight head), else the oldest entry after the head. */
  push(call: QueuedCall): number {
    this.items.push(call);
    let dropped = 0;
    while (this.items.length > this.max && this.items.length > 1) {
      const i = this.items.findIndex((c, idx) => idx > 0 && c.action === 'MeterValues');
      this.items.splice(i > 0 ? i : 1, 1);
      dropped++;
    }
    this.dropped += dropped;
    return dropped;
  }

  /** Removes the head if it carries `uniqueId`. Returns the removed call. */
  ack(uniqueId: string): QueuedCall | undefined {
    const h = this.items[0];
    if (!h || h.uniqueId !== uniqueId) return undefined;
    this.items.shift();
    return h;
  }

  /** Snapshot of queued uniqueIds in order (tests, diagnostics). */
  ids(): string[] {
    return this.items.map((c) => c.uniqueId);
  }
}
