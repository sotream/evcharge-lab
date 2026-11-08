/**
 * Background housekeeping: reservation expiry sweeper (ADR 0005, every 5 s, SKIP LOCKED so replicas share the
 * work), monthly partition creation (ADR 0001 retention) and retention of dedup ledgers.
 */
/* oxlint-disable no-await-in-loop */ // sweep batches run one after another
import { type Logger, type Pool, purgeIdempotencyKeys, purgeProcessedEvents, withTx } from '@evcharge/platform';
import { m } from '../metrics';
import { emitSessionEvent, type TopicNames } from '../outbox-events';
import { errorMessage } from '../util';

const SWEEP_BATCH = 200;

/** Expires open reservations whose TTL passed; emits reservation.ended in the same transaction. Returns the count. */
export async function sweepExpiredReservations(pool: Pool, topics: TopicNames): Promise<number> {
  let total = 0;
  for (;;) {
    const n = await withTx(pool, async (c) => {
      const { rows } = await c.query<{ reservation_id: number; driver_id: string; site_id: string; station_id: string; connector_id: number }>(
        `WITH due AS (
           SELECT reservation_id, starts_at FROM reservations
            WHERE status IN ('Pending', 'Active') AND expires_at < now() AND starts_at > now() - interval '1 day'
            ORDER BY expires_at LIMIT ${SWEEP_BATCH} FOR UPDATE SKIP LOCKED)
         UPDATE reservations r SET status = 'Expired', ended_at = now()
           FROM due WHERE r.reservation_id = due.reservation_id AND r.starts_at = due.starts_at
         RETURNING r.reservation_id, r.driver_id, r.site_id, r.station_id, r.connector_id`,
      );
      for (const r of rows) {
        await emitSessionEvent(c, topics, 'reservation.ended', r.site_id, {
          reservationId: r.reservation_id,
          driverId: r.driver_id,
          siteId: r.site_id,
          stationId: r.station_id,
          connectorId: r.connector_id,
          reason: 'Expired',
        });
      }
      return rows.length;
    });
    total += n;
    if (n < SWEEP_BATCH) break;
  }
  if (total) {
    m.sweeperExpired.inc(total);
    m.reservationTransitions.inc({ to: 'Expired' }, total);
  }
  return total;
}

/** A Pending reservation whose ReserveNow got no result 10 s after the command expired is rejected, not held for 15 min. */
export const RESULT_GRACE_S = 10;

/**
 * Rejects Pending reservations whose ReserveNow command timed out without any result (gateway or broker down) and
 * marks the command Expired, so a late result is ignored. Returns the count.
 */
export async function rejectUnconfirmedReservations(pool: Pool, topics: TopicNames): Promise<number> {
  let total = 0;
  for (;;) {
    const n = await withTx(pool, async (c) => {
      const { rows } = await c.query<{ reservation_id: number; driver_id: string; site_id: string; station_id: string; connector_id: number }>(
        `WITH due AS (
           SELECT r.reservation_id, r.starts_at, r.command_id FROM reservations r JOIN commands cmd ON cmd.command_id = r.command_id
            WHERE r.status = 'Pending' AND r.starts_at > now() - interval '1 day' AND cmd.outcome IS NULL
              AND cmd.expires_at + make_interval(secs => ${RESULT_GRACE_S}) < now()
            ORDER BY cmd.expires_at LIMIT ${SWEEP_BATCH} FOR UPDATE OF r SKIP LOCKED),
         expired AS (UPDATE commands SET outcome = 'Expired', completed_at = now() WHERE command_id IN (SELECT command_id FROM due))
         UPDATE reservations r SET status = 'Rejected', ended_at = now()
           FROM due WHERE r.reservation_id = due.reservation_id AND r.starts_at = due.starts_at
         RETURNING r.reservation_id, r.driver_id, r.site_id, r.station_id, r.connector_id`,
      );
      for (const r of rows) {
        await emitSessionEvent(c, topics, 'reservation.ended', r.site_id, { reservationId: r.reservation_id, driverId: r.driver_id, siteId: r.site_id, stationId: r.station_id, connectorId: r.connector_id, reason: 'Rejected' });
      }
      return rows.length;
    });
    total += n;
    if (n < SWEEP_BATCH) break;
  }
  if (total) m.reservationTransitions.inc({ to: 'Rejected' }, total);
  return total;
}

/** Keeps monthly partitions of sessions/reservations ready for the next months. */
export async function ensurePartitions(pool: Pool, months = 4): Promise<number> {
  const { rows } = await pool.query<{ n: number }>('SELECT ensure_month_partitions(now(), $1) AS n', [months]);
  return rows[0]?.n ?? 0;
}

/** Retention for dedup/ledger tables (stations replay at most 24 h; ocpp_messages are kept 30 days). */
export async function purgeLedgers(pool: Pool): Promise<void> {
  await pool.query("DELETE FROM ocpp_messages WHERE created_at < now() - interval '30 days'");
  await pool.query("DELETE FROM commands WHERE created_at < now() - interval '30 days'");
  await purgeProcessedEvents(pool, 7 * 864e5);
  await purgeIdempotencyKeys(pool, 24 * 36e5);
}

export class Maintenance {
  private timers = new Set<NodeJS.Timeout>();
  private running = false;
  private readonly inflight = new Set<Promise<unknown>>();

  constructor(
    private readonly pool: Pool,
    private readonly topics: TopicNames,
    private readonly sweeperIntervalMs: number,
    private readonly logger: Logger,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.every('sweeper', this.sweeperIntervalMs, this.sweeperIntervalMs, async () => {
      await sweepExpiredReservations(this.pool, this.topics);
      await rejectUnconfirmedReservations(this.pool, this.topics);
    });
    this.every('partitions', 3_600_000, 1000, () => ensurePartitions(this.pool));
    this.every('purge', 3_600_000, 5000, () => purgeLedgers(this.pool));
  }

  private every(name: string, ms: number, firstMs: number, fn: () => Promise<unknown>): void {
    const schedule = (delay: number) => {
      const t = setTimeout(() => {
        this.timers.delete(t);
        if (!this.running) return;
        const p = fn()
          .catch((err) => this.logger.error({ worker: name, err: errorMessage(err) }, 'maintenance task failed'))
          .finally(() => {
            this.inflight.delete(p);
            if (this.running) schedule(ms);
          });
        this.inflight.add(p);
      }, delay);
      this.timers.add(t);
    };
    schedule(firstMs);
  }

  async stop(): Promise<void> {
    this.running = false;
    for (const t of this.timers) clearTimeout(t);
    this.timers.clear();
    await Promise.allSettled(this.inflight);
  }
}
