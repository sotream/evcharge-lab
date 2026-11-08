/** Typed helpers that write domain events and station commands to the transactional outbox (ADR 0002). */
import { randomUUID } from 'node:crypto';
import type { api, events } from '@evcharge/contracts';
import { enqueueOutbox, makeEnvelope, type Queryable } from '@evcharge/platform';
import type { z } from 'zod';
import { m } from './metrics';

export const PRODUCER = 'sessions';
/** ADR 0005: ReserveNow (and other driver commands) expire after 30 s if not delivered. */
export const COMMAND_TTL_S = 30;

export interface TopicNames {
  sessionEvents: string;
  stationCommands: string;
}

type SessionEvent = z.infer<typeof events.SessionEvent>;
type DataOf<T extends SessionEvent['type']> = Extract<SessionEvent, { type: T }>['data'];
export type CsmsAction = 'RemoteStartTransaction' | 'RemoteStopTransaction' | 'ReserveNow' | 'CancelReservation' | 'SetChargingProfile' | 'TriggerMessage';

/** session.events.v1, key = siteId. */
export async function emitSessionEvent<T extends SessionEvent['type']>(db: Queryable, topics: TopicNames, type: T, siteId: string, data: DataOf<T>): Promise<void> {
  await enqueueOutbox(db, { topic: topics.sessionEvents, event: makeEnvelope(type, siteId, data, PRODUCER) });
}

export interface CommandLink {
  /** Pre-generated id (when the caller stores it elsewhere in the same transaction). */
  commandId?: string;
  reservationId?: number;
  reservationStartsAt?: Date;
  sessionId?: string;
  sessionStartedAt?: Date;
  driverId?: string | null;
  /** SetChargingProfile: limit and validTo, so the result can confirm exactly what was sent. */
  limitW?: number;
  validTo?: Date;
}

/**
 * station.commands.v1 (key = stationId) StationCommandRequested + a `commands` row so the result can be
 * correlated. Returns the commandId.
 */
export async function enqueueCommand(
  db: Queryable,
  topics: TopicNames,
  stationId: string,
  action: CsmsAction,
  payload: Record<string, unknown>,
  link: CommandLink = {},
  ttlS = COMMAND_TTL_S,
): Promise<string> {
  const commandId = link.commandId ?? randomUUID();
  const expiresAt = new Date(Date.now() + ttlS * 1000);
  await db.query(
    `INSERT INTO commands (command_id, station_id, action, reservation_id, reservation_starts_at, session_id, session_started_at, driver_id, expires_at, limit_w, valid_to)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
    [commandId, stationId, action, link.reservationId ?? null, link.reservationStartsAt ?? null, link.sessionId ?? null, link.sessionStartedAt ?? null, link.driverId ?? null, expiresAt, link.limitW ?? null, link.validTo ?? null],
  );
  const data = { commandId, stationId, action, payload, expiresAt: expiresAt.toISOString(), requestedBy: PRODUCER };
  await enqueueOutbox(db, { topic: topics.stationCommands, event: makeEnvelope('station.command-requested', stationId, data, PRODUCER) });
  m.commands.inc({ action });
  return commandId;
}

export type ReservationDto = z.infer<typeof api.SessionsPublic.reservation>;
export type SessionDto = z.infer<typeof api.SessionsPublic.session>;
