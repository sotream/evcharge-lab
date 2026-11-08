/**
 * Read routing for the sessions database (ADR 0015).
 *
 * - `stale()` reads go to the replica when one is configured (site map, catalog).
 * - `afterLsn(minLsn)` reads (read-your-writes) wait until the replica has replayed `minLsn`, polling for at most
 *   `waitMs`, then fall back to the primary.
 * - Any error from the replica falls back to the primary: reads are idempotent.
 */
import type { Logger, Pool } from '@evcharge/platform';
/* oxlint-disable no-await-in-loop */ // the read-your-writes wait polls sequentially
import { m } from './metrics';

/** Parses a Postgres LSN ("16/B374D848") to a comparable number. Throws on anything else. */
export function parseLsn(lsn: string): bigint {
  const match = /^([0-9A-Fa-f]{1,8})\/([0-9A-Fa-f]{1,8})$/.exec(lsn);
  if (!match) throw new RangeError(`invalid LSN: ${lsn}`);
  return (BigInt(`0x${match[1]}`) << 32n) | BigInt(`0x${match[2]}`);
}

export const isLsn = (s: string): boolean => /^[0-9A-Fa-f]{1,8}\/[0-9A-Fa-f]{1,8}$/.test(s);

/** True when the replica has replayed at least `min`. */
export const replayedAtLeast = (replayed: string, min: string): boolean => parseLsn(replayed) >= parseLsn(min);

/** The later of two LSNs; undefined stays undefined only when both are. */
export const maxLsn = (a: string | undefined, b: string | undefined): string | undefined => (a === undefined ? b : b === undefined ? a : parseLsn(a) >= parseLsn(b) ? a : b);

export type ReadTarget = 'primary' | 'replica';
export type RywOutcome = 'replica_caught_up' | 'fallback_primary';

export interface ReadRouterOptions {
  /** Total time a read-your-writes read may wait for the replica (default 200 ms). */
  waitMs: number;
  pollMs: number;
  logger?: Logger;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  /** Returns the replica's pg_last_wal_replay_lsn(); defaults to querying the replica pool. */
  replayLsn?: () => Promise<string | null>;
}

const realSleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export class ReadRouter {
  private readonly now: () => number;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly replayLsn: () => Promise<string | null>;

  constructor(
    readonly primary: Pool,
    readonly replica: Pool | undefined,
    private readonly opts: ReadRouterOptions,
  ) {
    this.now = opts.now ?? Date.now;
    this.sleep = opts.sleep ?? realSleep;
    this.replayLsn =
      opts.replayLsn ??
      (async () => {
        const r = await this.replica?.query<{ lsn: string | null }>('SELECT pg_last_wal_replay_lsn()::text AS lsn');
        return r?.rows[0]?.lsn ?? null;
      });
  }

  /** Decides the target for a read. `minLsn` undefined means stale-tolerant. */
  async choose(minLsn?: string): Promise<{ target: ReadTarget; pool: Pool }> {
    if (!this.replica) return { target: 'primary', pool: this.primary };
    if (minLsn === undefined) return { target: 'replica', pool: this.replica };
    const deadline = this.now() + this.opts.waitMs;
    for (;;) {
      let replayed: string | null = null;
      try {
        replayed = await this.replayLsn();
      } catch (err) {
        this.opts.logger?.warn({ err }, 'replica replay LSN unavailable, reading the primary');
        break;
      }
      if (replayed !== null && replayedAtLeast(replayed, minLsn)) {
        m.rywWaits.inc({ outcome: 'replica_caught_up' });
        return { target: 'replica', pool: this.replica };
      }
      if (this.now() >= deadline) break;
      await this.sleep(this.opts.pollMs);
    }
    m.rywWaits.inc({ outcome: 'fallback_primary' });
    return { target: 'primary', pool: this.primary };
  }

  /**
   * Runs a read. Without `minLsn` it is stale-tolerant; with it, read-your-writes. A failed replica read is retried once
   * on the primary.
   */
  async read<T>(fn: (pool: Pool) => Promise<T>, minLsn?: string): Promise<T> {
    const { target, pool } = await this.choose(minLsn);
    if (target === 'primary') {
      m.reads.inc({ target: 'primary' });
      return fn(pool);
    }
    try {
      const out = await fn(pool);
      m.reads.inc({ target: 'replica' });
      return out;
    } catch (err) {
      this.opts.logger?.warn({ err }, 'replica read failed, retrying on the primary');
      m.reads.inc({ target: 'primary' });
      return fn(this.primary);
    }
  }
}

/** Samples replica lag as sessions sees it, for the dashboards and the drill. */
export class ReplicaMonitor {
  private timer: NodeJS.Timeout | undefined;

  constructor(
    private readonly primary: Pool,
    private readonly replica: Pool,
    private readonly logger: Logger,
    private readonly intervalMs = 5000,
  ) {}

  start(): void {
    const tick = () => void this.sample().catch((err: unknown) => this.logger.warn({ err }, 'replica lag sample failed'));
    tick();
    this.timer = setInterval(tick, this.intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
  }

  async sample(): Promise<void> {
    // Seconds: zero when everything received is replayed (an idle primary is not lag), else age of the last replayed transaction.
    const [seconds, bytes] = await Promise.all([
      this.replica.query<{ s: string }>(
        `SELECT CASE WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
                     ELSE greatest(0, extract(epoch FROM now() - pg_last_xact_replay_timestamp())) END AS s`,
      ),
      this.primary.query<{ b: string | null }>(`SELECT max(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)) AS b FROM pg_stat_replication`),
    ]);
    m.replicaLagSeconds.set(Number(seconds.rows[0]?.s ?? 0));
    const b = bytes.rows[0]?.b;
    m.replicaLagBytes.set(b === null || b === undefined ? 0 : Number(b));
  }
}
