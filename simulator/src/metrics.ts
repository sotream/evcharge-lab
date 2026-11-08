import { counter, gauge, histogram } from '@evcharge/platform';
import type { Counter, Gauge, Histogram } from 'prom-client';

export interface SimMetrics {
  stations: Gauge;
  connected: Gauge;
  sessions: Gauge;
  sessionsStarted: Counter;
  messages: Counter<'direction' | 'action'>;
  callRtt: Histogram<'action'>;
  siteDrawn: Gauge<'site'>;
  siteCap: Gauge<'site'>;
  siteCapFromSessions: Gauge<'site'>;
  replayQueue: Gauge;
  reconnects: Counter<'kind'>;
  connectFailures: Counter<'reason'>;
  queueDropped: Counter;
  probeReadings: Counter;
  driverRequests: Counter<'op' | 'outcome'>;
  callErrors: Counter<'action' | 'code'>;
}

/** Get-or-create on the shared prom-client registry (ADR 0013 metrics). */
export const simMetrics = (): SimMetrics => ({
  stations: gauge({ name: 'sim_stations', help: 'Stations in the simulated fleet' }),
  connected: gauge({ name: 'sim_connected_stations', help: 'Stations with an accepted BootNotification on an open socket' }),
  sessions: gauge({ name: 'sim_active_sessions', help: 'Connectors with a running transaction' }),
  sessionsStarted: counter({ name: 'sim_sessions_started_total', help: 'Charging sessions started' }),
  messages: counter({ name: 'sim_ocpp_messages_total', help: 'OCPP frames by direction (sent/received) and action; rate() gives msgs/s', labelNames: ['direction', 'action'] as const }),
  callRtt: histogram({ name: 'sim_call_rtt_seconds', help: 'Station CALL sent -> CALLRESULT received', labelNames: ['action'] as const, buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30] }),
  siteDrawn: gauge({ name: 'sim_site_drawn_watts', help: 'Sum of power drawn by all connectors of a site right now (after profile limits)', labelNames: ['site'] as const }),
  siteCap: gauge({ name: 'sim_site_cap_watts', help: 'Site power cap the drawn sum is compared against', labelNames: ['site'] as const }),
  siteCapFromSessions: gauge({ name: 'sim_site_cap_from_sessions', help: '1 when the site cap was read from sessions, 0 when it comes from the shared fleet convention', labelNames: ['site'] as const }),
  replayQueue: gauge({ name: 'sim_replay_queue_depth', help: 'Queued Start/Stop/MeterValues CALLs waiting for a CALLRESULT, all stations' }),
  reconnects: counter({ name: 'sim_reconnect_attempts_total', help: 'Connection attempts by kind (initial, retry, after_drop)', labelNames: ['kind'] as const }),
  connectFailures: counter({ name: 'sim_connect_failures_total', help: 'Failed connection attempts by reason', labelNames: ['reason'] as const }),
  queueDropped: counter({ name: 'sim_queue_dropped_total', help: 'Queued messages dropped because the offline queue was full' }),
  probeReadings: counter({ name: 'sim_probe_readings_total', help: 'Readings written to the probe JSONL file' }),
  driverRequests: counter({ name: 'sim_driver_requests_total', help: 'driver-api requests by operation and outcome', labelNames: ['op', 'outcome'] as const }),
  callErrors: counter({ name: 'sim_call_errors_total', help: 'CALLERRORs received for station CALLs', labelNames: ['action', 'code'] as const }),
});
