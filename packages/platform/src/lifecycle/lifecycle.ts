/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (ordering, retries, backoff)
import type { Logger } from '../logging/logger';
import type { HealthRegistry } from '../health/health';
import { errorMessage, sleep } from '../util';

export interface ShutdownHook {
  name: string;
  run: () => unknown;
}

export interface LifecycleOptions {
  logger: Logger;
  health: HealthRegistry;
  /** Overall deadline for the whole shutdown sequence (SHUTDOWN_GRACE_MS). */
  graceMs: number;
  /** Time between "ready -> 503" and closing the HTTP server, so load balancers notice. */
  drainDelayMs: number;
}

export interface ShutdownResult {
  ok: boolean;
  timedOut: boolean;
  failed: string[];
}

/**
 * Graceful shutdown orchestration:
 *   1. mark draining (readiness -> 503)   2. wait drainDelayMs
 *   3. close HTTP (`setHttpCloser`)        4. run hooks in reverse registration order
 * all bounded by graceMs. Register long-lived dependencies (OTel, pools, producers, consumers)
 * in the order you create them; they stop in the reverse order.
 */
export class Lifecycle {
  private readonly hooks: ShutdownHook[] = [];
  private httpCloser: (() => Promise<unknown>) | undefined;
  private running: Promise<ShutdownResult> | undefined;
  private signalsInstalled = false;

  constructor(private readonly opts: LifecycleOptions) {}

  onShutdown(name: string, run: () => unknown): void {
    this.hooks.push({ name, run });
  }

  setHttpCloser(close: () => Promise<unknown>): void {
    this.httpCloser = close;
  }

  get shuttingDown(): boolean {
    return this.running !== undefined;
  }

  shutdown(reason = 'shutdown'): Promise<ShutdownResult> {
    return (this.running ??= this.run(reason));
  }

  /** SIGTERM/SIGINT -> shutdown -> exit(0 if clean else 1). A second signal exits immediately. */
  installSignalHandlers(exit: (code: number) => void = (c) => process.exit(c)): void {
    if (this.signalsInstalled) return;
    this.signalsInstalled = true;
    const onSignal = (sig: NodeJS.Signals) => {
      if (this.shuttingDown) {
        this.opts.logger.warn({ signal: sig }, 'second signal, exiting immediately');
        exit(1);
        return;
      }
      this.opts.logger.info({ signal: sig }, 'shutdown signal received');
      void this.shutdownAndExit(sig, exit);
    };
    process.on('SIGTERM', onSignal);
    process.on('SIGINT', onSignal);
  }

  private async shutdownAndExit(reason: string, exit: (code: number) => void): Promise<void> {
    const r = await this.shutdown(reason);
    this.opts.logger.flush?.();
    exit(r.ok ? 0 : 1);
  }

  private async run(reason: string): Promise<ShutdownResult> {
    const { logger, health, graceMs, drainDelayMs } = this.opts;
    const t0 = Date.now();
    const failed: string[] = [];
    const deadline = new AbortController();
    const timer = setTimeout(() => deadline.abort(), graceMs);
    timer.unref();
    const deadlineHit = new Promise<never>((_, reject) => deadline.signal.addEventListener('abort', () => reject(new Error('shutdown deadline exceeded')), { once: true }));
    deadlineHit.catch(() => {}); // rejected only when the deadline fires; never an unhandled rejection
    health.setDraining(true);
    logger.info({ reason, drainDelayMs, graceMs }, 'draining');

    const steps: ShutdownHook[] = [];
    if (this.httpCloser) steps.push({ name: 'http', run: this.httpCloser });
    steps.push(...this.hooks.toReversed());

    const sequence = (async () => {
      await sleep(Math.min(drainDelayMs, graceMs), deadline.signal);
      for (const step of steps) {
        if (deadline.signal.aborted) break;
        try {
          // Raced against the overall deadline itself, not against a second timer of the remaining time: two timers due
          // within a millisecond of each other can fire in either order, and when the step timer won, the loop saw no
          // abort yet and ran the next hook after the deadline.
          await Promise.race([Promise.resolve().then(step.run), deadlineHit]);
          logger.debug({ hook: step.name }, 'shutdown hook done');
        } catch (err) {
          failed.push(step.name);
          logger.error({ hook: step.name, err: errorMessage(err) }, 'shutdown hook failed');
        }
      }
    })();
    const timedOut = await Promise.race([
      sequence.then(() => false),
      new Promise<boolean>((resolve) => deadline.signal.addEventListener('abort', () => resolve(true), { once: true })),
    ]);
    clearTimeout(timer);
    if (timedOut) logger.error({ graceMs }, 'shutdown deadline exceeded');
    else logger.info({ durationMs: Date.now() - t0, failed }, 'shutdown complete');
    return { ok: !timedOut && failed.length === 0, timedOut, failed };
  }
}
