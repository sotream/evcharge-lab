import { counter, gauge, histogram, LATENCY_BUCKETS } from '@evcharge/platform';
import type { Counter, Gauge, Histogram } from 'prom-client';

/**
 * Gateway metrics (ADR 0003, 0004, 0012). Gauges carry a `gateway` label (the instance id) so
 * several instances in one process (integration tests) do not overwrite each other.
 */
export const gatewayMetrics = (): GatewayMetrics => ({
  connected: gauge({ name: 'gateway_connected_stations', help: 'Open, authenticated station WebSockets', labelNames: ['gateway'] as const }),
  upgrades: counter({ name: 'gateway_upgrades_total', help: 'WebSocket upgrade attempts by outcome and reason', labelNames: ['outcome', 'reason'] as const }),
  messages: counter({ name: 'ocpp_messages_total', help: 'OCPP frames by direction, message type and action', labelNames: ['direction', 'type', 'action'] as const }),
  callDuration: histogram({
    name: 'ocpp_call_duration_seconds',
    help: 'Station CALL received -> CALLRESULT/CALLERROR sent (p99 target 500 ms for transaction calls)',
    labelNames: ['action', 'outcome'] as const,
    buckets: LATENCY_BUCKETS,
  }),
  produceBuffer: gauge({ name: 'gateway_produce_buffer_depth', help: 'Records waiting for a broker ack (bounded by GATEWAY_PRODUCE_BUFFER)', labelNames: ['gateway'] as const }),
  produceRejected: counter({ name: 'gateway_produce_rejected_total', help: 'CALLs answered InternalError because the produce buffer was full', labelNames: ['topic'] as const }),
  produceErrors: counter({ name: 'gateway_produce_errors_total', help: 'Records the broker did not acknowledge, by reason', labelNames: ['topic', 'reason'] as const }),
  rawLogDropped: counter({ name: 'ocpp_raw_log_dropped_total', help: 'Raw OCPP frames not logged (buffer full or broker error)', labelNames: ['reason'] as const }),
  commands: counter({ name: 'gateway_commands_total', help: 'CSMS -> station commands by action and outcome', labelNames: ['action', 'outcome'] as const }),
  commandDuration: histogram({ name: 'gateway_command_duration_seconds', help: 'Command received by the owning gateway -> station response', labelNames: ['action'] as const, buckets: LATENCY_BUCKETS }),
  dispatch: counter({ name: 'gateway_dispatch_total', help: 'Dispatcher decisions for station.commands records', labelNames: ['result'] as const }),
  credentials: counter({ name: 'gateway_credential_lookups_total', help: 'Station credential lookups by source', labelNames: ['source'] as const }),
  routeRefresh: counter({ name: 'gateway_route_refresh_total', help: 'Route TTL refreshes by result', labelNames: ['result'] as const }),
  safeShare: counter({ name: 'gateway_safe_share_applied_total', help: 'Safe-share TxProfiles the gateway sent after an allocation expired (ADR 0006)', labelNames: ['outcome'] as const }),
  replaced: counter({ name: 'gateway_connections_replaced_total', help: 'Connections closed with 4000 because the station connected again', labelNames: ['where'] as const }),
});

export interface GatewayMetrics {
  connected: Gauge<'gateway'>;
  upgrades: Counter<'outcome' | 'reason'>;
  messages: Counter<'direction' | 'type' | 'action'>;
  callDuration: Histogram<'action' | 'outcome'>;
  produceBuffer: Gauge<'gateway'>;
  produceRejected: Counter<'topic'>;
  produceErrors: Counter<'topic' | 'reason'>;
  rawLogDropped: Counter<'reason'>;
  commands: Counter<'action' | 'outcome'>;
  commandDuration: Histogram<'action'>;
  dispatch: Counter<'result'>;
  credentials: Counter<'source'>;
  routeRefresh: Counter<'result'>;
  safeShare: Counter<'outcome'>;
  replaced: Counter<'where'>;
}
