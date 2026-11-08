/**
 * Internal OCPP-facing operations called synchronously by station-gateway (ADR 0001 rule 1, ADR 0003).
 * Budget: CALLRESULT p99 < 500 ms, so each operation is one short transaction of indexed statements.
 * StartTransaction/StopTransaction are idempotent on (stationId, ocppMessageId, action, payload hash) via the
 * `ocpp_messages` primary key: a replay (same uniqueId, same payload) (or a concurrent duplicate, which blocks on the key until the first commits) gets the
 * stored response, i.e. the same transactionId.
 */
/* oxlint-disable no-await-in-loop */ // statements of one transaction run sequentially on one connection
import { createHash } from 'node:crypto';
import { type api, fleet } from '@evcharge/contracts';
import { type Logger, type Pool, type PoolClient, withTx } from '@evcharge/platform';
import type { z } from 'zod';
import { ProblemError } from '../http/problem';
import { m } from '../metrics';
import { emitSessionEvent, type TopicNames } from '../outbox-events';
import { markSiteDue, type SiteSignal } from '../site-signal';

type Internal = typeof api.SessionsInternal;
export type CredentialRes = z.infer<Internal['stationCredential']>;
export type AuthorizeReq = z.infer<Internal['authorizeReq']>;
export type AuthorizeRes = z.infer<Internal['authorizeRes']>;
export type StartTxReq = z.infer<Internal['startTxReq']>;
export type StartTxRes = z.infer<Internal['startTxRes']>;
export type StopTxReq = z.infer<Internal['stopTxReq']>;
export type StopTxRes = z.infer<Internal['stopTxRes']>;
export type RegisterDriverReq = z.infer<Internal['registerDriverReq']>;
type IdTagStatus = AuthorizeRes['idTagInfo']['status'];

/** Station clocks can be wrong; a timestamp more than this far from now is replaced by now (logged). */
const MAX_CLOCK_SKEW_MS = 7 * 864e5;

export function stationTime(ts: string, now = Date.now()): { at: Date; clamped: boolean } {
  const t = Date.parse(ts);
  if (!Number.isFinite(t) || Math.abs(t - now) > MAX_CLOCK_SKEW_MS) return { at: new Date(now), clamped: true };
  return { at: new Date(t), clamped: false };
}

/** Unknown idTags are ad-hoc charging (driverless session, invoiced unpaid per ADR 0007) only when the operator allows it. */
function idTagStatus(driver: { driver_id: string | null; blocked: boolean | null } | undefined, acceptUnknown: boolean): IdTagStatus {
  if (!driver?.driver_id) return acceptUnknown ? 'Accepted' : 'Invalid';
  return driver.blocked ? 'Blocked' : 'Accepted';
}

interface ActiveRow {
  transaction_id: number;
  session_id: string;
  started_at: Date;
  site_id: string;
  station_id: string;
  connector_id: number;
  limit_w: number | null;
}

export class OcppService {
  constructor(
    private readonly pool: Pool,
    private readonly topics: TopicNames,
    private readonly signal: SiteSignal,
    private readonly logger: Logger,
    private readonly acceptUnknownIdTags = false,
  ) {}

  async credential(stationId: string): Promise<CredentialRes> {
    const { rows } = await this.pool.query<{ station_id: string; site_id: string; secret_sha256: string; enabled: boolean; cap_w: number; n: string }>(
      `SELECT s.station_id, s.site_id, s.secret_sha256, s.enabled, si.cap_w,
              (SELECT count(*) FROM connectors c WHERE c.site_id = s.site_id) AS n
         FROM stations s JOIN sites si ON si.site_id = s.site_id
        WHERE s.station_id = $1`,
      [stationId],
    );
    const r = rows[0];
    if (!r) throw new ProblemError(404, 'unknown_station', 'Unknown station', `station ${stationId} is not registered`);
    return { stationId: r.station_id, siteId: r.site_id, secretSha256: r.secret_sha256, enabled: r.enabled, chargingDefaults: fleet.chargingDefaults(r.cap_w, Number(r.n)) };
  }

  async authorize(req: AuthorizeReq): Promise<AuthorizeRes> {
    const { rows } = await this.pool.query<{ driver_id: string; blocked: boolean }>('SELECT driver_id, blocked FROM drivers WHERE id_tag = $1', [req.idTag]);
    return { idTagInfo: { status: idTagStatus(rows[0], this.acceptUnknownIdTags) } };
  }

  async registerDriver(req: RegisterDriverReq): Promise<{ status: number; body: { driverId: string; idTag: string } }> {
    const ins = await this.pool.query('INSERT INTO drivers (driver_id, id_tag) VALUES ($1, $2) ON CONFLICT DO NOTHING', [req.driverId, req.idTag]);
    if (ins.rowCount === 1) return { status: 201, body: req };
    const { rows } = await this.pool.query<{ driver_id: string; id_tag: string }>('SELECT driver_id, id_tag FROM drivers WHERE driver_id = $1 OR id_tag = $2', [req.driverId, req.idTag]);
    if (rows.length === 1 && rows[0]?.driver_id === req.driverId && rows[0].id_tag === req.idTag) return { status: 200, body: req };
    throw new ProblemError(409, 'driver_conflict', 'Driver or idTag already registered', 'driverId or idTag is already registered with a different value');
  }

  async startTransaction(req: StartTxReq): Promise<StartTxRes> {
    return this.timed('start', async () => {
      const key = messageKey(req.stationId, req.ocppMessageId, 'StartTransaction', req);
      const out = await withTx(this.pool, async (c) => {
        const replay = await this.claim(c, key, 'StartTransaction');
        if (replay !== undefined) return { replay: true as const, res: replay as StartTxRes };
        const { rows } = await c.query<{ connector_pk: string; site_id: string; tariff_id: string; max_power_w: number; min_power_w: number; driver_id: string | null; blocked: boolean | null }>(
          `SELECT c.connector_pk, c.site_id, c.tariff_id, c.max_power_w, c.min_power_w, d.driver_id, d.blocked
             FROM connectors c LEFT JOIN drivers d ON d.id_tag = $3
            WHERE c.station_id = $1 AND c.connector_id = $2
              FOR NO KEY UPDATE OF c`, // serialises StartTransactions on one connector (the replace-then-insert below races otherwise); NO KEY so FK checks (KEY SHARE) do not queue behind it
          [req.stationId, req.connectorId, req.idTag],
        );
        const ctx = rows[0];
        if (!ctx) throw new ProblemError(422, 'unknown_connector', 'Unknown connector', `${req.stationId} has no connector ${req.connectorId}`);
        const status = idTagStatus(ctx, this.acceptUnknownIdTags);
        const { at: startedAt, clamped } = stationTime(req.timestamp);
        if (clamped) this.logger.warn({ stationId: req.stationId, timestamp: req.timestamp }, 'StartTransaction timestamp out of range, using now');

        // A station can only run one transaction per connector: an older one still open here was missed.
        const prev = await c.query<ActiveRow>('DELETE FROM active_sessions WHERE connector_pk = $1 RETURNING *', [ctx.connector_pk]);
        for (const p of prev.rows) await this.complete(c, p, req.meterStartWh, startedAt, 'Superseded', true);

        // ADR 0005 "Use": the reservation named by the station, or the reserving driver's own on this connector.
        const used = await c.query<{ reservation_id: number; driver_id: string; site_id: string; station_id: string; connector_id: number }>(
          `UPDATE reservations SET status = 'Used', ended_at = now()
            WHERE status IN ('Pending', 'Active') AND starts_at > now() - interval '1 day' AND connector_pk = $1
              AND (reservation_id = $2 OR driver_id = $3)
          RETURNING reservation_id, driver_id, site_id, station_id, connector_id`,
          [ctx.connector_pk, req.reservationId ?? null, ctx.driver_id],
        );
        const reservationId = used.rows[0]?.reservation_id ?? null;

        const ins = await c.query<{ session_id: string; transaction_id: number }>(
          `INSERT INTO sessions (session_id, transaction_id, driver_id, id_tag, site_id, station_id, connector_id, connector_pk, tariff_id, reservation_id, status, meter_start_wh, started_at)
           VALUES (uuidv7(), nextval('transaction_id_seq'), $1, $2, $3, $4, $5, $6, $7, $8, 'Active', $9, $10)
           RETURNING session_id, transaction_id`,
          [ctx.driver_id, req.idTag, ctx.site_id, req.stationId, req.connectorId, ctx.connector_pk, ctx.tariff_id, reservationId, req.meterStartWh, startedAt],
        );
        const s = ins.rows[0] as { session_id: string; transaction_id: number };
        await c.query(
          `INSERT INTO active_sessions (transaction_id, session_id, started_at, site_id, station_id, connector_id, connector_pk, driver_id, max_w, min_w)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
          [s.transaction_id, s.session_id, startedAt, ctx.site_id, req.stationId, req.connectorId, ctx.connector_pk, ctx.driver_id, ctx.max_power_w, ctx.min_power_w],
        );
        await emitSessionEvent(c, this.topics, 'session.started', ctx.site_id, {
          sessionId: s.session_id,
          transactionId: s.transaction_id,
          driverId: ctx.driver_id,
          idTag: req.idTag,
          siteId: ctx.site_id,
          stationId: req.stationId,
          connectorId: req.connectorId,
          tariffId: ctx.tariff_id,
          meterStartWh: req.meterStartWh,
          startedAt: startedAt.toISOString(),
        });
        for (const r of used.rows) {
          await emitSessionEvent(c, this.topics, 'reservation.ended', r.site_id, { reservationId: r.reservation_id, driverId: r.driver_id, siteId: r.site_id, stationId: r.station_id, connectorId: r.connector_id, reason: 'Used' });
          m.reservationTransitions.inc({ to: 'Used' });
        }
        await markSiteDue(c, ctx.site_id);
        const res: StartTxRes = { transactionId: s.transaction_id, sessionId: s.session_id, idTagInfo: { status } };
        await c.query('UPDATE ocpp_messages SET response = $2 WHERE message_key = $1', [key, JSON.stringify(res)]);
        return { replay: false as const, res, siteId: ctx.site_id };
      });
      if (out.replay) {
        m.ocppReplays.inc({ op: 'start' });
        return out.res;
      }
      m.sessionsStarted.inc();
      this.signal.changed(out.siteId);
      return out.res;
    });
  }

  async stopTransaction(req: StopTxReq): Promise<StopTxRes> {
    return this.timed('stop', async () => {
      const key = messageKey(req.stationId, req.ocppMessageId, 'StopTransaction', req);
      const out = await withTx(this.pool, async (c) => {
        const replay = await this.claim(c, key, 'StopTransaction');
        if (replay !== undefined) return { replay: true as const, res: replay as StopTxRes };
        const { at: stoppedAt } = stationTime(req.timestamp);
        const del = await c.query<ActiveRow>('DELETE FROM active_sessions WHERE transaction_id = $1 AND station_id = $2 RETURNING *', [req.transactionId, req.stationId]);
        let sessionId: string | null = null;
        let siteId: string | undefined;
        const active = del.rows[0];
        if (active) {
          await this.complete(c, active, req.meterStopWh, stoppedAt, req.reason ?? 'Local', false);
          sessionId = active.session_id;
          siteId = active.site_id;
          await markSiteDue(c, active.site_id);
        } else {
          // Already completed by an earlier (different) StopTransaction, or unknown: accept (ADR 0003).
          const { rows } = await c.query<{ session_id: string }>(
            "SELECT session_id FROM sessions WHERE transaction_id = $1 AND station_id = $2 AND started_at > now() - interval '62 days' LIMIT 1",
            [req.transactionId, req.stationId],
          );
          sessionId = rows[0]?.session_id ?? null;
          if (!sessionId) this.logger.warn({ stationId: req.stationId, transactionId: req.transactionId }, 'StopTransaction for unknown transaction accepted');
        }
        let res: StopTxRes = { sessionId };
        if (req.idTag) {
          const d = await c.query<{ driver_id: string; blocked: boolean }>('SELECT driver_id, blocked FROM drivers WHERE id_tag = $1', [req.idTag]);
          res = { sessionId, idTagInfo: { status: idTagStatus(d.rows[0], this.acceptUnknownIdTags) } };
        }
        await c.query('UPDATE ocpp_messages SET response = $2 WHERE message_key = $1', [key, JSON.stringify(res)]);
        return { replay: false as const, res, siteId };
      });
      if (out.replay) {
        m.ocppReplays.inc({ op: 'stop' });
        return out.res;
      }
      if (out.siteId) this.signal.changed(out.siteId);
      return out.res;
    });
  }

  /** Inserts the dedup key; returns the stored response if this message was already processed. */
  private async claim(c: PoolClient, key: string, action: string): Promise<unknown> {
    const ins = await c.query('INSERT INTO ocpp_messages (message_key, action) VALUES ($1, $2) ON CONFLICT DO NOTHING', [key, action]);
    if (ins.rowCount === 1) return undefined;
    const { rows } = await c.query<{ response: unknown }>('SELECT response FROM ocpp_messages WHERE message_key = $1', [key]);
    if (rows[0]?.response == null) throw new ProblemError(409, 'in_flight', 'Duplicate in flight', `${key} is being processed`);
    return rows[0].response;
  }

  /** Completes a session row (partition-pruned by started_at) and emits session.completed. */
  private async complete(c: PoolClient, a: ActiveRow, meterStopWh: number, stoppedAt: Date, reason: string, superseded: boolean): Promise<void> {
    const { rows } = await c.query<{
      session_id: string;
      transaction_id: number;
      driver_id: string | null;
      site_id: string;
      station_id: string;
      connector_id: number;
      tariff_id: string;
      meter_start_wh: string;
      meter_stop_wh: string;
      energy_wh: string;
      started_at: Date;
      stopped_at: Date;
      stop_reason: string;
    }>(
      `UPDATE sessions SET status = 'Completed',
              meter_stop_wh = greatest(meter_start_wh, $3::bigint),
              energy_wh = greatest(0, $3::bigint - meter_start_wh),
              stopped_at = greatest(started_at, $4::timestamptz),
              stop_reason = $5, final_limit_w = $6
        WHERE session_id = $1 AND started_at = $2
      RETURNING session_id, transaction_id, driver_id, site_id, station_id, connector_id, tariff_id, meter_start_wh, meter_stop_wh, energy_wh, started_at, stopped_at, stop_reason`,
      [a.session_id, a.started_at, meterStopWh, stoppedAt, reason, a.limit_w],
    );
    const s = rows[0];
    if (!s) return;
    await emitSessionEvent(c, this.topics, 'session.completed', s.site_id, {
      sessionId: s.session_id,
      transactionId: s.transaction_id,
      driverId: s.driver_id,
      siteId: s.site_id,
      stationId: s.station_id,
      connectorId: s.connector_id,
      tariffId: s.tariff_id,
      meterStartWh: Number(s.meter_start_wh),
      meterStopWh: Number(s.meter_stop_wh),
      energyWh: Number(s.energy_wh),
      startedAt: s.started_at.toISOString(),
      stoppedAt: s.stopped_at.toISOString(),
      stopReason: s.stop_reason,
    });
    m.sessionsCompleted.inc({ reason: superseded ? 'Superseded' : reason });
  }

  private async timed<T>(op: string, fn: () => Promise<T>): Promise<T> {
    const t0 = performance.now();
    try {
      const r = await fn();
      m.ocppSeconds.observe({ op, outcome: 'ok' }, (performance.now() - t0) / 1000);
      return r;
    } catch (err) {
      m.ocppSeconds.observe({ op, outcome: 'error' }, (performance.now() - t0) / 1000);
      throw err;
    }
  }
}

/**
 * Dedup key for station transaction messages. A station replays a message with the same uniqueId AND the same
 * payload, so both are part of the key. Firmware may restart its uniqueId counter after a reboot: keying on the
 * uniqueId alone made a new StartTransaction receive an old message's stored response (no transactionId), and the
 * station retried it forever (found in the Kafka drills; the simulator had the same counter reset).
 */
export function messageKey(stationId: string, ocppMessageId: string, action: string, req: object): string {
  const { ocppMessageId: _id, ...payload } = req as Record<string, unknown>;
  const digest = createHash('sha256').update(JSON.stringify(payload, Object.keys(payload).sort())).digest('hex').slice(0, 16);
  return `${stationId}:${ocppMessageId}:${action}:${digest}`;
}
