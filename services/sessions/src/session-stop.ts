import type { PoolClient } from '@evcharge/platform';
import { enqueueCommand, type TopicNames } from './outbox-events';

/** Sessions with a remembered system stop reason that are Active again, at this station (or all stations when omitted). */
export async function activeSessionsWithStopReason(c: PoolClient, stationId?: string, minAgeS = 30): Promise<string[]> {
  const { rows } = await c.query<{ session_id: string }>(
    `SELECT a.session_id FROM active_sessions a JOIN sessions s ON s.session_id = a.session_id AND s.started_at = a.started_at
      WHERE s.status = 'Active' AND s.stop_requested_reason IS NOT NULL AND ($1::text IS NULL OR a.station_id = $1)
        AND NOT EXISTS (SELECT 1 FROM commands cmd WHERE cmd.session_id = a.session_id AND cmd.action = 'RemoteStopTransaction'
                           AND (cmd.outcome IS NULL OR cmd.created_at > now() - make_interval(secs => $2::float8)))
      LIMIT 200 FOR UPDATE OF a SKIP LOCKED`,
    [stationId ?? null, minAgeS],
  );
  return rows.map((r) => r.session_id);
}

/**
 * Active -> Stopping plus a RemoteStopTransaction through the outbox, exactly like DriverService.stopSession but
 * without the driver check (system-initiated stops). `reason` is remembered on the session (stop_requested_reason): a stop
 * whose command fails (station offline, timeout) goes back to Active and is re-issued by reissueRequestedStops and on
 * station.connected until the session completes. Returns false when the session is not Active (nothing to do).
 */
export async function requestRemoteStop(c: PoolClient, topics: TopicNames, sessionId: string, reason?: string): Promise<boolean> {
  const { rows } = await c.query<{ transaction_id: number; station_id: string; started_at: Date; driver_id: string | null }>(
    `UPDATE sessions s SET status = 'Stopping', stop_requested_reason = coalesce($2, s.stop_requested_reason)
      WHERE s.session_id = $1 AND s.started_at BETWEEN uuid_extract_timestamp($1::uuid) - interval '8 days' AND uuid_extract_timestamp($1::uuid) + interval '8 days'
        AND s.status = 'Active'
    RETURNING s.transaction_id, s.station_id, s.started_at, s.driver_id`,
    [sessionId, reason ?? null],
  );
  const r = rows[0];
  if (!r) return false;
  await enqueueCommand(c, topics, r.station_id, 'RemoteStopTransaction', { transactionId: r.transaction_id }, { sessionId, sessionStartedAt: r.started_at, driverId: r.driver_id ?? undefined });
  return true;
}
