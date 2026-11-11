/**
 * Idempotent handlers for station.events.v1 and station.command-results.v1 (ADR 0002 inbox on eventId).
 * Each handler runs in one transaction with its `processed_events` marker.
 */
/* oxlint-disable no-await-in-loop */ // statements of one transaction run sequentially on one connection
import type { events } from '@evcharge/contracts';
import { type Logger, type Pool, withInbox, withTx } from '@evcharge/platform';
import { m } from '../metrics';
import { emitSessionEvent, enqueueCommand, PRODUCER, type TopicNames } from '../outbox-events';
import { activeSessionsWithStopReason, requestRemoteStop } from '../session-stop';
import type { SiteSignal } from '../site-signal';

export const STATION_EVENTS_CONSUMER = 'sessions.station-events';
export const COMMAND_RESULTS_CONSUMER = 'sessions.command-results';
/** Close code the gateway uses when a newer connection replaced this one (ADR 0003). */
const REPLACED_CLOSE_CODE = 4000;

export class StationEventHandlers {
  constructor(
    private readonly pool: Pool,
    private readonly topics: TopicNames,
    private readonly logger: Logger,
    private readonly signal?: SiteSignal,
    private readonly profileFailures: { alertAfter: number; stopAfter: number } = { alertAfter: 3, stopAfter: 0 },
  ) {}

  /** Connector status last-write-wins on the station timestamp; online flag from connect/disconnect. */
  async stationEvent(e: events.StationEvent): Promise<void> {
    await withTx(this.pool, (c) =>
      withInbox(c, STATION_EVENTS_CONSUMER, e.eventId, async () => {
        switch (e.type) {
          case 'station.connector-status-changed': {
            const d = e.data;
            if (d.connectorId === 0) return; // the charge point as a whole: no connector row
            const at = d.stationTimestamp ?? e.occurredAt;
            await c.query(
              `UPDATE connectors SET status = $3, error_code = $4, status_at = $5
                WHERE station_id = $1 AND connector_id = $2 AND (status_at IS NULL OR status_at <= $5::timestamptz)`,
              [d.stationId, d.connectorId, d.status, d.errorCode, at],
            );
            return;
          }
          case 'station.connected':
            // A system-requested stop (declined payment, rejected profiles) that could not reach the station is re-issued now.
            for (const id of await activeSessionsWithStopReason(c, e.data.stationId, 0)) await requestRemoteStop(c, this.topics, id);
            await c.query(
              `UPDATE stations SET online = true, gateway_instance = $2, online_changed_at = $3
                WHERE station_id = $1 AND (online_changed_at IS NULL OR online_changed_at <= $3::timestamptz)`,
              [e.data.stationId, e.data.gatewayInstance, e.occurredAt],
            );
            return;
          case 'station.disconnected':
            if (e.data.code === REPLACED_CLOSE_CODE) return; // a newer connection already owns the station
            await c.query(
              `UPDATE stations SET online = false, online_changed_at = $3
                WHERE station_id = $1 AND (gateway_instance IS NULL OR gateway_instance = $2)
                  AND (online_changed_at IS NULL OR online_changed_at <= $3::timestamptz)`,
              [e.data.stationId, e.data.gatewayInstance, e.occurredAt],
            );
            return;
          case 'station.booted':
            await c.query('UPDATE stations SET vendor = $2, model = $3, firmware_version = $4, booted_at = $5 WHERE station_id = $1', [
              e.data.stationId,
              e.data.vendor,
              e.data.model,
              e.data.firmwareVersion ?? null,
              e.occurredAt,
            ]);
            return;
        }
      }),
    );
  }

  /** Command outcomes for commands this service requested: reservation transitions, stop/profile rollbacks. */
  async commandResult(e: events.StationCommandCompleted): Promise<void> {
    const d = e.data;
    if (d.requestedBy !== PRODUCER) return;
    const out = await withTx(this.pool, (c) =>
      withInbox(c, COMMAND_RESULTS_CONSUMER, e.eventId, async (): Promise<string | undefined> => {
        const { rows } = await c.query<{ action: string; reservation_id: number | null; reservation_starts_at: Date | null; session_id: string | null; session_started_at: Date | null; limit_w: number | null; valid_to: Date | null }>(
          `UPDATE commands SET outcome = $2, response = $3, completed_at = now() WHERE command_id = $1 AND outcome IS NULL
           RETURNING action, reservation_id, reservation_starts_at, session_id, session_started_at, limit_w, valid_to`,
          [d.commandId, d.outcome, d.response ? JSON.stringify(d.response) : null],
        );
        const cmd = rows[0];
        if (!cmd) {
          // Unknown or already completed. One case needs an action: the station accepted a ReserveNow only after the
          // sweeper gave up on it (command Expired, reservation Rejected). The station now holds the connector for a
          // reservation this service no longer knows, so tell it to drop it.
          if (d.outcome === 'Accepted') await this.cancelLateReservation(c, d.commandId);
          return;
        }
        m.commandResults.inc({ action: cmd.action, outcome: d.outcome });
        const accepted = d.outcome === 'Accepted';
        switch (cmd.action) {
          case 'ReserveNow': {
            if (accepted) {
              const r = await c.query(
                "UPDATE reservations SET status = 'Active' WHERE reservation_id = $1 AND starts_at = $2 AND status = 'Pending'",
                [cmd.reservation_id, cmd.reservation_starts_at],
              );
              if (r.rowCount) m.reservationTransitions.inc({ to: 'Active' });
              return;
            }
            const r = await c.query<{ reservation_id: number; driver_id: string; site_id: string; station_id: string; connector_id: number }>(
              `UPDATE reservations SET status = 'Rejected', ended_at = date_trunc('milliseconds', clock_timestamp()) WHERE reservation_id = $1 AND starts_at = $2 AND status IN ('Pending', 'Active')
               RETURNING reservation_id, driver_id, site_id, station_id, connector_id`,
              [cmd.reservation_id, cmd.reservation_starts_at],
            );
            for (const x of r.rows) {
              await emitSessionEvent(c, this.topics, 'reservation.ended', x.site_id, { reservationId: x.reservation_id, driverId: x.driver_id, siteId: x.site_id, stationId: x.station_id, connectorId: x.connector_id, reason: 'Rejected' });
              m.reservationTransitions.inc({ to: 'Rejected' });
            }
            return;
          }
          case 'RemoteStopTransaction':
            if (!accepted) {
              await c.query("UPDATE sessions SET status = 'Active' WHERE session_id = $1 AND started_at = $2 AND status = 'Stopping'", [cmd.session_id, cmd.session_started_at]);
            }
            return;
          case 'SetChargingProfile':
            return this.profileResult(c, cmd, d.commandId, accepted, d.outcome);
          default:
            if (!accepted) this.logger.info({ commandId: d.commandId, action: cmd.action, outcome: d.outcome }, 'station command not accepted');
        }
      }),
    );
    if (!out.duplicate && out.result) this.signal?.changed(out.result);
  }

  /** A station that keeps rejecting profiles ignores the site cap: alert once, optionally stop the session. */
  private async countProfileRejection(c: Parameters<Parameters<typeof withTx>[1]>[0], sessionId: string): Promise<void> {
    const { rows } = await c.query<{ site_id: string; profile_failures: number }>('UPDATE active_sessions SET profile_failures = profile_failures + 1 WHERE session_id = $1 RETURNING site_id, profile_failures', [sessionId]);
    const r = rows[0];
    if (!r) return;
    if (r.profile_failures === this.profileFailures.alertAfter) m.profileNoncompliant.inc({ site_id: r.site_id });
    if (this.profileFailures.stopAfter > 0 && r.profile_failures === this.profileFailures.stopAfter && (await requestRemoteStop(c, this.topics, sessionId, 'profile_noncompliant'))) {
      this.logger.warn({ sessionId, failures: r.profile_failures }, 'station keeps rejecting charging profiles: stopping the session');
    }
  }

  private async cancelLateReservation(c: Parameters<Parameters<typeof withTx>[1]>[0], commandId: string): Promise<void> {
    const { rows } = await c.query<{ station_id: string; reservation_id: number; reservation_starts_at: Date; driver_id: string }>(
      `SELECT cmd.station_id, cmd.reservation_id, cmd.reservation_starts_at, r.driver_id
         FROM commands cmd JOIN reservations r ON r.reservation_id = cmd.reservation_id AND r.starts_at = cmd.reservation_starts_at
        WHERE cmd.command_id = $1 AND cmd.action = 'ReserveNow' AND cmd.outcome = 'Expired' AND r.status = 'Rejected'`,
      [commandId],
    );
    const late = rows[0];
    if (!late) return;
    await enqueueCommand(c, this.topics, late.station_id, 'CancelReservation', { reservationId: late.reservation_id }, { reservationId: late.reservation_id, reservationStartsAt: late.reservation_starts_at, driverId: late.driver_id });
    this.logger.warn({ commandId, stationId: late.station_id, reservationId: late.reservation_id }, 'station accepted ReserveNow after it expired; sent CancelReservation');
  }

  /**
   * SetChargingProfile outcome (ADR 0006 two-phase apply). Accepted: the station applies exactly what was sent, so it
   * becomes the confirmed limit; commands to one station are ordered, so if this was the latest one nothing
   * unconfirmed remains. Any other outcome never lowers what we assume is in force: the previous limits stay
   * counted until their validTo. A site that withheld increases is re-triggered on Accepted only (returns its id to kick).
   */
  private async profileResult(
    c: Parameters<Parameters<typeof withTx>[1]>[0],
    cmd: { session_id: string | null; limit_w: number | null; valid_to: Date | null },
    commandId: string,
    accepted: boolean,
    outcome: string,
  ): Promise<string | undefined> {
    if (!cmd.session_id) return undefined;
    // M10: only an explicit Rejected counts (StationOffline/Timeout are connectivity or backend side, ADR 0000 degradation).
    if (outcome === 'Rejected') await this.countProfileRejection(c, cmd.session_id);
    let siteId: string | undefined;
    if (accepted && cmd.limit_w !== null && cmd.valid_to) {
      const { rows } = await c.query<{ site_id: string }>(
        `UPDATE active_sessions SET
            confirmed_limit_w = CASE WHEN confirmed_valid_to IS NULL OR confirmed_valid_to <= $3 THEN $2 ELSE confirmed_limit_w END,
            confirmed_valid_to = CASE WHEN confirmed_valid_to IS NULL OR confirmed_valid_to <= $3 THEN $3 ELSE confirmed_valid_to END,
            possible_limit_w = CASE WHEN pending_command_id = $4 THEN NULL ELSE possible_limit_w END,
            possible_valid_to = CASE WHEN pending_command_id = $4 THEN NULL ELSE possible_valid_to END,
            pending_command_id = CASE WHEN pending_command_id = $4 THEN NULL ELSE pending_command_id END,
            profile_failures = 0
          WHERE session_id = $1 RETURNING site_id`,
        [cmd.session_id, cmd.limit_w, cmd.valid_to, commandId],
      );
      siteId = rows[0]?.site_id;
    }
    // Only an Accepted result changes what is in force, so only it can free headroom for withheld increases. Re-running
    // the round on StationOffline/Timeout/Rejected would resend the same profiles to a station that cannot take them,
    // a command storm (seen with the fleet offline). Those sites are retried by the periodic round instead.
    if (!siteId) return undefined;
    // Debounced, not kicked: every Accepted result of a round used to trigger its own round at once, and a round that
    // sends new profiles produces new results, a self-sustaining loop (measured: about 140 SetChargingProfile/s for 20
    // sites). The site becomes due in 2 s, so a round's results share one rerun, picked up by the periodic tick.
    // Starts and stops still kick immediately (reallocation p99 < 5 s, ADR 0000).
    await c.query(
      `UPDATE sites SET alloc_due_at = LEAST(alloc_due_at, now() + interval '2 seconds'), alloc_requested_at = coalesce(alloc_requested_at, clock_timestamp())
        WHERE site_id = $1 AND alloc_deferred`,
      [siteId],
    );
    return undefined;
  }
}
