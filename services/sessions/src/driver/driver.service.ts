/**
 * Driver-facing reservations and sessions (proxied by driver-api with x-driver-id, ADR 0010).
 * Reservations follow ADR 0005: the database (exclusion constraint + partial unique index) decides who wins.
 */
import { randomUUID } from 'node:crypto';
import { hashRequest, type Pool, type PoolClient, type StoredResponse, withIdempotency, withTx } from '@evcharge/platform';
import { ProblemError, notFound } from '../http/problem';
import { m } from '../metrics';
import type { ReadRouter } from '../read-router';
import { emitSessionEvent, enqueueCommand, type ReservationDto, type SessionDto, type TopicNames } from '../outbox-events';

/** ADR 0000: reservation TTL is fixed at 15 minutes. */
export const RESERVATION_TTL_S = 15 * 60;
const BUSY_STATUSES = new Set(['Charging', 'Faulted']);

export const connectorUnavailable = (detail: string) => new ProblemError(409, 'connector_unavailable', 'Connector unavailable', detail);

interface PgError {
  code?: string;
  constraint?: string;
}

/** Maps the database's verdict on a reservation insert to HTTP (ADR 0005: 23P01 -> 409 connector_unavailable). */
export function mapReservationError(err: unknown): unknown {
  const e = err as PgError;
  if (e?.code === '23P01') return connectorUnavailable('connector is already reserved');
  if (e?.code === '23505' && e.constraint?.endsWith('one_open_per_driver')) return new ProblemError(409, 'reservation_exists', 'Open reservation exists', 'driver already holds an open reservation');
  return err;
}

interface ReservationRow {
  reservation_id: number;
  driver_id: string;
  site_id: string;
  station_id: string;
  connector_id: number;
  status: ReservationDto['status'];
  starts_at: Date;
  expires_at: Date;
}
export const reservationDto = (r: ReservationRow): ReservationDto => ({
  reservationId: r.reservation_id,
  driverId: r.driver_id,
  stationId: r.station_id,
  connectorId: r.connector_id,
  status: r.status,
  startsAt: r.starts_at.toISOString(),
  expiresAt: r.expires_at.toISOString(),
});

interface SessionRow {
  session_id: string;
  transaction_id: number;
  driver_id: string | null;
  station_id: string;
  connector_id: number;
  status: SessionDto['status'];
  started_at: Date;
  stopped_at: Date | null;
  energy_wh: string | null;
  limit_w: number | null;
  signed_meter_data: SessionDto['signedMeterData'];
}
const sessionDto = (r: SessionRow): SessionDto => ({
  sessionId: r.session_id,
  transactionId: r.transaction_id,
  driverId: r.driver_id,
  stationId: r.station_id,
  connectorId: r.connector_id,
  status: r.status,
  startedAt: r.started_at.toISOString(),
  stoppedAt: r.stopped_at?.toISOString() ?? null,
  energyWh: r.energy_wh === null ? null : Number(r.energy_wh),
  limitW: r.limit_w,
  ...(r.signed_meter_data ? { signedMeterData: r.signed_meter_data } : {}),
});

const SESSION_COLUMNS = `s.session_id, s.transaction_id, s.driver_id, s.station_id, s.connector_id, s.status, s.started_at, s.stopped_at, s.energy_wh,
  coalesce(a.limit_w, s.final_limit_w) AS limit_w, s.signed_meter_data`;
/** session_id is a UUIDv7 minted at insert; started_at is within 7 days of it (clamped), so this prunes partitions. */
const SESSION_BY_ID = `s.session_id = $1 AND s.started_at BETWEEN uuid_extract_timestamp($1::uuid) - interval '8 days' AND uuid_extract_timestamp($1::uuid) + interval '8 days'`;

interface ConnectorCtx {
  connector_pk: string;
  site_id: string;
  status: string;
  online: boolean;
  enabled: boolean;
  id_tag: string | null;
  blocked: boolean | null;
  occupied: boolean;
  reserved_by_other: boolean;
}

export class DriverService {
  constructor(
    private readonly pool: Pool,
    private readonly topics: TopicNames,
    private readonly router: ReadRouter,
  ) {}

  /** LSN after a committed write: pg_current_wal_lsn() is at or past the commit record (ADR 0015). Callers send it as x-commit-lsn. */
  async commitLsn(): Promise<string> {
    const { rows } = await this.pool.query<{ lsn: string }>('SELECT pg_current_wal_lsn()::text AS lsn');
    return (rows[0] as { lsn: string }).lsn;
  }

  /** Without x-min-lsn the read has no read-your-writes need for the replica to honour, so it uses the primary. */
  private read<T>(minLsn: string | undefined, fn: (pool: Pool) => Promise<T>): Promise<T> {
    if (minLsn !== undefined) return this.router.read(fn, minLsn);
    m.reads.inc({ target: 'primary' });
    return fn(this.pool);
  }

  private async connectorCtx(c: PoolClient, driverId: string, stationId: string, connectorId: number): Promise<ConnectorCtx> {
    const { rows } = await c.query<ConnectorCtx>(
      `SELECT c.connector_pk, c.site_id, c.status, st.online, st.enabled, d.id_tag, d.blocked,
              EXISTS (SELECT 1 FROM active_sessions a WHERE a.connector_pk = c.connector_pk) AS occupied,
              EXISTS (SELECT 1 FROM reservations r WHERE r.connector_pk = c.connector_pk AND r.status IN ('Pending', 'Active')
                         AND r.starts_at > now() - interval '1 day' AND r.period @> now() AND r.driver_id <> $3) AS reserved_by_other
         FROM connectors c JOIN stations st ON st.station_id = c.station_id
         LEFT JOIN drivers d ON d.driver_id = $3
        WHERE c.station_id = $1 AND c.connector_id = $2`,
      [stationId, connectorId, driverId],
    );
    const ctx = rows[0];
    if (!ctx || !ctx.enabled) throw new ProblemError(404, 'unknown_connector', 'Unknown connector', `${stationId} has no connector ${connectorId}`);
    if (!ctx.id_tag) throw new ProblemError(403, 'driver_not_registered', 'Driver not registered', 'driver has no idTag registered');
    if (ctx.blocked) throw new ProblemError(403, 'driver_blocked', 'Driver blocked');
    // ADR 0005: Charging/Faulted connectors are rejected up front (cheaper than a station round trip).
    if (BUSY_STATUSES.has(ctx.status) || ctx.occupied) throw connectorUnavailable(`connector is ${ctx.occupied ? 'in use' : ctx.status}`);
    return ctx;
  }

  /** POST /v1/reservations (idempotency-key required). */
  async createReservation(driverId: string, idempotencyKey: string, body: { stationId: string; connectorId: number }): Promise<{ replayed: boolean; response: StoredResponse }> {
    const req = { scope: `${driverId} POST /v1/reservations`, key: idempotencyKey, requestHash: hashRequest('POST', '/v1/reservations', body) };
    try {
      const r = await withIdempotency(this.pool, req, async (c) => {
        const ctx = await this.connectorCtx(c, driverId, body.stationId, body.connectorId);
        // Concurrent INSERTs into the exclusion-constrained table deadlock each other: every inserter finds the others'
        // uncommitted rows and waits for them (40P01 after deadlock_timeout, 1 s, and the waiters hold pool connections).
        // One transaction per connector at a time removes the cycle; the constraint stays as the backstop (ADR 0005).
        await c.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [`reservation:${ctx.connector_pk}`]);
        const commandId = randomUUID();
        let row: ReservationRow;
        try {
          const ins = await c.query<ReservationRow>(
            `INSERT INTO reservations (reservation_id, driver_id, id_tag, site_id, station_id, connector_id, connector_pk, status, starts_at, expires_at, command_id)
             SELECT nextval('reservation_id_seq'), $1, $2, $3, $4, $5, $6, 'Pending', t.ts, t.ts + make_interval(secs => $7), $8
               FROM (SELECT date_trunc('milliseconds', clock_timestamp()) AS ts) t
             RETURNING reservation_id, driver_id, site_id, station_id, connector_id, status, starts_at, expires_at`,
            [driverId, ctx.id_tag, ctx.site_id, body.stationId, body.connectorId, ctx.connector_pk, RESERVATION_TTL_S, commandId],
          );
          row = ins.rows[0] as ReservationRow;
        } catch (err) {
          throw mapReservationError(err);
        }
        await enqueueCommand(
          c,
          this.topics,
          body.stationId,
          'ReserveNow',
          { connectorId: body.connectorId, expiryDate: row.expires_at.toISOString(), idTag: ctx.id_tag, reservationId: row.reservation_id },
          { commandId, reservationId: row.reservation_id, reservationStartsAt: row.starts_at, driverId },
        );
        await emitSessionEvent(c, this.topics, 'reservation.created', row.site_id, {
          reservationId: row.reservation_id,
          driverId,
          siteId: row.site_id,
          stationId: row.station_id,
          connectorId: row.connector_id,
          expiresAt: row.expires_at.toISOString(),
        });
        return { status: 201, body: reservationDto(row) };
      });
      m.reservations.inc({ outcome: r.replayed ? 'replayed' : 'created' });
      return r;
    } catch (err) {
      m.reservations.inc({ outcome: err instanceof ProblemError ? err.code : 'error' });
      throw err;
    }
  }

  async getReservation(driverId: string, reservationId: number, minLsn?: string): Promise<ReservationDto> {
    const { rows } = await this.read(minLsn, (pool) => pool.query<ReservationRow>(
      `SELECT reservation_id, driver_id, site_id, station_id, connector_id, status, starts_at, expires_at FROM reservations
        WHERE reservation_id = $1 AND driver_id = $2 AND starts_at > now() - interval '62 days'`,
      [reservationId, driverId],
    ));
    if (!rows[0]) throw notFound('reservation');
    return reservationDto(rows[0]);
  }

  /** DELETE /v1/reservations/:id -> Cancelled + CancelReservation command. Idempotent: a closed one is returned as is. */
  async cancelReservation(driverId: string, reservationId: number): Promise<ReservationDto> {
    const dto = await withTx(this.pool, async (c) => {
      // Same per-connector lock as createReservation: a reserve waiting on this cancel then takes its starts_at after our
      // commit, so ended_at <= the next starts_at. Reservation timestamps use clock_timestamp(), not now() (transaction start).
      await c.query(
        `SELECT pg_advisory_xact_lock(hashtextextended('reservation:' || connector_pk, 0)) FROM reservations
          WHERE reservation_id = $1 AND driver_id = $2 AND starts_at > now() - interval '1 day' AND status IN ('Pending', 'Active')`,
        [reservationId, driverId],
      );
      const { rows } = await c.query<ReservationRow>(
        `UPDATE reservations SET status = 'Cancelled', ended_at = date_trunc('milliseconds', clock_timestamp())
          WHERE reservation_id = $1 AND driver_id = $2 AND starts_at > now() - interval '1 day' AND status IN ('Pending', 'Active')
        RETURNING reservation_id, driver_id, site_id, station_id, connector_id, status, starts_at, expires_at`,
        [reservationId, driverId],
      );
      const r = rows[0];
      if (!r) return undefined;
      await enqueueCommand(c, this.topics, r.station_id, 'CancelReservation', { reservationId }, { reservationId, reservationStartsAt: r.starts_at, driverId });
      await emitSessionEvent(c, this.topics, 'reservation.ended', r.site_id, { reservationId, driverId, siteId: r.site_id, stationId: r.station_id, connectorId: r.connector_id, reason: 'Cancelled' });
      m.reservationTransitions.inc({ to: 'Cancelled' });
      return reservationDto(r);
    });
    return dto ?? this.getReservation(driverId, reservationId);
  }

  /** POST /v1/sessions/remote-start (idempotency-key required): RemoteStartTransaction via the outbox. */
  async remoteStart(driverId: string, idempotencyKey: string, body: { stationId: string; connectorId: number }): Promise<{ replayed: boolean; response: StoredResponse }> {
    const req = { scope: `${driverId} POST /v1/sessions/remote-start`, key: idempotencyKey, requestHash: hashRequest('POST', '/v1/sessions/remote-start', body) };
    return withIdempotency(this.pool, req, async (c) => {
      const ctx = await this.connectorCtx(c, driverId, body.stationId, body.connectorId);
      if (ctx.reserved_by_other) throw new ProblemError(409, 'connector_reserved', 'Connector reserved', 'connector is reserved by another driver');
      if (!ctx.online) throw new ProblemError(409, 'station_offline', 'Station offline', `${body.stationId} is not connected`);
      const commandId = await enqueueCommand(c, this.topics, body.stationId, 'RemoteStartTransaction', { connectorId: body.connectorId, idTag: ctx.id_tag }, { driverId });
      return { status: 202, body: { commandId, status: 'Requested' } };
    });
  }

  async getSession(driverId: string, sessionId: string, minLsn?: string): Promise<SessionDto> {
    const { rows } = await this.read(minLsn, (pool) =>
      pool.query<SessionRow>(`SELECT ${SESSION_COLUMNS} FROM sessions s LEFT JOIN active_sessions a ON a.session_id = s.session_id WHERE ${SESSION_BY_ID} AND s.driver_id = $2`, [sessionId, driverId]),
    );
    if (!rows[0]) throw notFound('session');
    return sessionDto(rows[0]);
  }

  /** POST /v1/sessions/:id/stop -> Stopping + RemoteStopTransaction. Idempotent: repeated calls send one command. */
  async stopSession(driverId: string, sessionId: string): Promise<SessionDto> {
    await withTx(this.pool, async (c) => {
      const { rows } = await c.query<{ transaction_id: number; station_id: string; started_at: Date }>(
        `UPDATE sessions s SET status = 'Stopping' WHERE ${SESSION_BY_ID} AND s.driver_id = $2 AND s.status = 'Active'
         RETURNING s.transaction_id, s.station_id, s.started_at`,
        [sessionId, driverId],
      );
      const r = rows[0];
      if (!r) return;
      await enqueueCommand(c, this.topics, r.station_id, 'RemoteStopTransaction', { transactionId: r.transaction_id }, { sessionId, sessionStartedAt: r.started_at, driverId });
    });
    return this.getSession(driverId, sessionId);
  }

  /** GET /v1/drivers/:id/sessions, newest first; `before` (ISO time) pages backwards. */
  async listDriverSessions(driverId: string, limit: number, before?: string, minLsn?: string): Promise<{ sessions: SessionDto[] }> {
    const { rows } = await this.read(minLsn, (pool) =>
      pool.query<SessionRow>(
        `SELECT ${SESSION_COLUMNS} FROM sessions s LEFT JOIN active_sessions a ON a.session_id = s.session_id
          WHERE s.driver_id = $1 AND ($3::timestamptz IS NULL OR s.started_at < $3::timestamptz)
          ORDER BY s.started_at DESC LIMIT $2`,
        [driverId, limit, before ?? null],
      ),
    );
    return { sessions: rows.map(sessionDto) };
  }
}
