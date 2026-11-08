/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (worker loops, ordering)
import { type Logger, sleep } from '@evcharge/platform';
import { errorMessage } from '../util';
import type { SagaEngine } from './saga-engine';

export interface SagaWorkerOptions {
  workers: number;
  batch: number;
  idlePollMs: number;
  gaugeIntervalMs?: number;
  /** Runs periodically (e.g. partition maintenance). */
  maintenance?: { intervalMs: number; run: () => Promise<void> };
}

/**
 * Runs `workers` claim loops. Each loop claims a batch of due sagas (FOR UPDATE SKIP LOCKED +
 * lease) and processes the batch concurrently; idle loops sleep `idlePollMs` or until woken by
 * the event consumer.
 */
export class SagaWorker {
  private running = false;
  private loops: Promise<void>[] = [];
  private timers: NodeJS.Timeout[] = [];
  private wakeups = new Set<() => void>();
  private readonly stopCtl = new AbortController();

  constructor(
    private readonly engine: SagaEngine,
    private readonly opts: SagaWorkerOptions,
    private readonly log: Logger,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loops = Array.from({ length: this.opts.workers }, () => this.loop());
    const gauge = setInterval(() => void this.engine.refreshPendingGauge().catch((err) => this.log.warn({ err: errorMessage(err) }, 'pending gauge refresh failed')), this.opts.gaugeIntervalMs ?? 5000);
    gauge.unref();
    this.timers.push(gauge);
    const m = this.opts.maintenance;
    if (m) {
      const t = setInterval(() => void m.run().catch((err) => this.log.warn({ err: errorMessage(err) }, 'maintenance failed')), m.intervalMs);
      t.unref();
      this.timers.push(t);
    }
  }

  /** New work arrived: wake idle loops. */
  wake(): void {
    for (const w of this.wakeups) w();
  }

  /** Lets in-flight steps finish, then stops. */
  async stop(): Promise<void> {
    this.running = false;
    this.stopCtl.abort();
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
    this.wake();
    await Promise.allSettled(this.loops);
  }

  /** Test helper: processes due sagas until none is due. Returns the number processed. */
  async drain(max = 1000): Promise<number> {
    let n = 0;
    while (n < max) {
      const rows = await this.engine.claim(this.opts.batch);
      if (rows.length === 0) return n;
      await Promise.all(rows.map((r) => this.engine.process(r)));
      n += rows.length;
    }
    return n;
  }

  private async loop(): Promise<void> {
    while (this.running) {
      let rows;
      try {
        rows = await this.engine.claim(this.opts.batch);
      } catch (err) {
        this.log.error({ err: errorMessage(err) }, 'saga claim failed');
        await sleep(1000, this.stopCtl.signal);
        continue;
      }
      if (rows.length === 0) {
        await this.idle();
        continue;
      }
      await Promise.all(rows.map((r) => this.engine.process(r)));
    }
  }

  private idle(): Promise<void> {
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(t);
        this.wakeups.delete(done);
        resolve();
      };
      const t = setTimeout(done, this.opts.idlePollMs);
      this.wakeups.add(done);
    });
  }
}
