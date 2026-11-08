import { counter, gauge, histogram, LATENCY_BUCKETS } from '@evcharge/platform';

/** Service metrics named by ADR 0000 / 0003 / 0005 / 0006. */
export const m = {
  /** OCPP hot path (CALLRESULT p99 < 500 ms): handler latency per operation. */
  ocppSeconds: histogram({ name: 'sessions_ocpp_request_seconds', help: 'Internal OCPP handler latency', labelNames: ['op', 'outcome'] as const, buckets: LATENCY_BUCKETS }),
  ocppReplays: counter({ name: 'sessions_ocpp_replays_total', help: 'Start/StopTransaction replays answered from the dedup ledger', labelNames: ['op'] as const }),
  reservations: counter({ name: 'sessions_reservations_total', help: 'Reservation attempts by outcome', labelNames: ['outcome'] as const }),
  reservationTransitions: counter({ name: 'sessions_reservation_transitions_total', help: 'Reservation status transitions', labelNames: ['to'] as const }),
  sessionsStarted: counter({ name: 'sessions_started_total', help: 'Sessions created from StartTransaction' }),
  sessionsCompleted: counter({ name: 'sessions_completed_total', help: 'Sessions completed', labelNames: ['reason'] as const }),
  activeSessions: gauge({ name: 'sessions_active', help: 'Active sessions (refreshed periodically)' }),
  commands: counter({ name: 'sessions_station_commands_total', help: 'Station commands enqueued via outbox', labelNames: ['action'] as const }),
  commandResults: counter({ name: 'sessions_command_results_total', help: 'Station command results consumed', labelNames: ['action', 'outcome'] as const }),
  /** ADR 0006: reallocation p99 < 5 s from session start/stop to the profile command being enqueued. */
  reallocationLatency: histogram({
    name: 'sessions_reallocation_latency_seconds',
    help: 'Session start/stop trigger -> allocation round committed (SetChargingProfile enqueued)',
    buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 30, 60],
  }),
  allocationRounds: counter({ name: 'sessions_allocation_rounds_total', help: 'Allocation rounds by strategy and reason', labelNames: ['strategy', 'reason'] as const }),
  allocationSeconds: histogram({ name: 'sessions_allocation_round_seconds', help: 'Allocation round duration', buckets: LATENCY_BUCKETS }),
  allocatorFailures: counter({ name: 'sessions_allocator_failures_total', help: 'smart-charging calls that failed and fell back', labelNames: ['reason'] as const }),
  profilesSent: counter({ name: 'sessions_charging_profiles_sent_total', help: 'SetChargingProfile commands enqueued', labelNames: ['reason'] as const }),
  /** Must stay 0: allocation results whose sum would exceed the site cap are rejected. */
  capViolationsRejected: counter({ name: 'sessions_allocation_cap_violations_rejected_total', help: 'Allocator responses rejected for exceeding the site cap' }),
  allocationBacklog: gauge({ name: 'sessions_allocation_backlog_seconds', help: 'Age of the oldest site whose allocation round is due and not leased' }),
  increasesDeferred: counter({ name: 'sessions_allocation_increases_deferred_total', help: 'Limit increases withheld until decreases are confirmed' }),
  /** ADR 0015: where driver-facing reads were served. */
  reads: counter({ name: 'sessions_reads_total', help: 'Driver-facing reads by target database', labelNames: ['target'] as const }),
  /** ADR 0015: read-your-writes waits; fallback_primary means the replica did not reach min-lsn within the timeout. */
  rywWaits: counter({ name: 'sessions_ryw_waits_total', help: 'Read-your-writes waits by outcome', labelNames: ['outcome'] as const }),
  replicaLagSeconds: gauge({ name: 'sessions_replica_lag_seconds', help: 'Replica replay lag in seconds as sampled by sessions (0 when fully replayed)' }),
  replicaLagBytes: gauge({ name: 'sessions_replica_lag_bytes', help: 'Primary pg_current_wal_lsn() minus the replica replay_lsn from pg_stat_replication' }),
  sweeperExpired: counter({ name: 'sessions_reservations_expired_total', help: 'Reservations expired by the sweeper' }),
};
