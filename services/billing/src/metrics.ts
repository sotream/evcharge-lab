import { counter, gauge, histogram } from '@evcharge/platform';

/** Metrics are get-or-create on the shared registry, so this module can be imported anywhere. */
export const metrics = {
  sagasPending: gauge({ name: 'billing_sagas_pending', help: 'Sagas not yet in a terminal state, by state', labelNames: ['state'] as const }),
  /** Session stoppedAt -> invoice issued. SLO p99 < 5 min (ADR 0000). */
  invoiceLatency: histogram({
    name: 'billing_invoice_latency_seconds',
    help: 'Time from session stoppedAt to invoice issued',
    buckets: [0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 21600, 86400],
  }),
  invoices: counter({ name: 'billing_invoices_total', help: 'Invoices issued' }),
  captures: counter({ name: 'billing_captures_total', help: 'Capture attempts by outcome', labelNames: ['outcome'] as const }),
  pspCalls: counter({ name: 'billing_psp_calls_total', help: 'PSP calls by operation and outcome', labelNames: ['operation', 'outcome'] as const }),
  sagaSteps: counter({ name: 'billing_saga_steps_total', help: 'Saga steps executed by step and outcome', labelNames: ['step', 'outcome'] as const }),
  sagasParked: counter({ name: 'billing_sagas_parked_total', help: 'Sagas given up on automatic retry (deadline or permanent error)', labelNames: ['step'] as const }),
  pspIdempotencyMismatch: counter({ name: 'billing_psp_idempotency_mismatch_total', help: 'PSP rejected a retry because the idempotency key had a different request (saga parked)', labelNames: ['step'] as const }),
  events: counter({ name: 'billing_session_events_total', help: 'Session events consumed by type and result', labelNames: ['type', 'result'] as const }),
};
