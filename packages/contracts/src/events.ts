/**
 * Kafka event contracts (ADR 0002). Every record value is an Envelope<T>.
 * Record key = Envelope.key (documented per topic in topics.ts).
 * Kafka header "traceparent" carries W3C trace context.
 * Consumers MUST be idempotent on eventId (at-least-once delivery).
 */
import { z } from 'zod';
import { ChargePointStatus, ChargingProfile, CsmsAction, CSMS_ACTIONS } from './ocpp';

const uuid = z.uuid();
const ts = z.iso.datetime({ offset: true });

export function envelope<T extends string, D extends z.ZodType>(type: T, data: D) {
  return z.object({
    eventId: uuid,
    type: z.literal(type),
    version: z.literal(1),
    occurredAt: ts,
    producer: z.string(),
    key: z.string(),
    data,
  });
}

// ---------------- station.events.v1 (producer: station-gateway, key: stationId)
export const StationConnected = envelope(
  'station.connected',
  z.object({ stationId: z.string(), gatewayInstance: z.string(), remoteAddr: z.string().optional() }),
);
export const StationDisconnected = envelope(
  'station.disconnected',
  z.object({ stationId: z.string(), gatewayInstance: z.string(), code: z.number().int(), reason: z.string() }),
);
export const StationBooted = envelope(
  'station.booted',
  z.object({ stationId: z.string(), vendor: z.string(), model: z.string(), firmwareVersion: z.string().optional() }),
);
export const ConnectorStatusChanged = envelope(
  'station.connector-status-changed',
  z.object({ stationId: z.string(), connectorId: z.number().int(), status: ChargePointStatus, errorCode: z.string(), stationTimestamp: ts.optional() }),
);
export const StationEvent = z.discriminatedUnion('type', [StationConnected, StationDisconnected, StationBooted, ConnectorStatusChanged]);
export type StationEvent = z.infer<typeof StationEvent>;

// ---------------- telemetry.meter-values.v1 (producer: station-gateway, key: stationId)
export const MeterSample = z.object({
  measurand: z.string(),
  value: z.number(),
  unit: z.string(),
  context: z.string().optional(),
  phase: z.string().optional(),
});
export const MeterValuesReceived = envelope(
  'telemetry.meter-values',
  z.object({
    stationId: z.string(),
    connectorId: z.number().int(),
    transactionId: z.number().int().optional(),
    /** OCPP uniqueId of the CALL; with stationId forms the dedup key. */
    ocppMessageId: z.string(),
    /** true when the gateway saw this as an offline replay (station timestamp older than 60s). */
    replayed: z.boolean(),
    readings: z.array(z.object({ timestamp: ts, samples: z.array(MeterSample).min(1) })).min(1),
  }),
);
export type MeterValuesReceived = z.infer<typeof MeterValuesReceived>;

// ---------------- station.commands.v1 (producers: sessions, smart-charging fallback; key: stationId)
export const StationCommandRequested = envelope(
  'station.command-requested',
  z.object({
    commandId: uuid,
    stationId: z.string(),
    action: z.enum(CSMS_ACTIONS as [CsmsAction, ...CsmsAction[]]),
    payload: z.record(z.string(), z.unknown()),
    /** Drop the command if not delivered by this time. */
    expiresAt: ts,
    /** Who wants the result (topic consumers filter on this). */
    requestedBy: z.string(),
  }),
);
export type StationCommandRequested = z.infer<typeof StationCommandRequested>;

// ---------------- station.command-results.v1 (producer: station-gateway, key: stationId)
export const StationCommandCompleted = envelope(
  'station.command-completed',
  z.object({
    commandId: uuid,
    stationId: z.string(),
    action: z.string(),
    outcome: z.enum(['Accepted', 'Rejected', 'Error', 'Timeout', 'StationOffline', 'Expired']),
    response: z.record(z.string(), z.unknown()).optional(),
    requestedBy: z.string(),
  }),
);
export type StationCommandCompleted = z.infer<typeof StationCommandCompleted>;

// ---------------- session.events.v1 (producer: sessions via outbox, key: siteId)
const money = z.object({ amountMinor: z.number().int(), currency: z.string().length(3) });
export const ReservationCreated = envelope(
  'reservation.created',
  z.object({ reservationId: z.number().int(), driverId: uuid, siteId: z.string(), stationId: z.string(), connectorId: z.number().int(), expiresAt: ts }),
);
export const ReservationEnded = envelope(
  'reservation.ended',
  z.object({ reservationId: z.number().int(), driverId: uuid, siteId: z.string(), stationId: z.string(), connectorId: z.number().int(), reason: z.enum(['Used', 'Cancelled', 'Expired', 'Rejected']) }),
);
export const SessionStarted = envelope(
  'session.started',
  z.object({
    sessionId: uuid,
    transactionId: z.number().int(),
    driverId: uuid.nullable(),
    idTag: z.string(),
    siteId: z.string(),
    stationId: z.string(),
    connectorId: z.number().int(),
    tariffId: z.string(),
    meterStartWh: z.number().int(),
    startedAt: ts,
  }),
);
export const SessionCompleted = envelope(
  'session.completed',
  z.object({
    sessionId: uuid,
    transactionId: z.number().int(),
    driverId: uuid.nullable(),
    siteId: z.string(),
    stationId: z.string(),
    connectorId: z.number().int(),
    tariffId: z.string(),
    meterStartWh: z.number().int(),
    meterStopWh: z.number().int(),
    energyWh: z.number().int(),
    startedAt: ts,
    stoppedAt: ts,
    stopReason: z.string(),
    energyFlag: z.enum(['ok', 'meter_regressed', 'implausible', 'stale']).optional(),
  }),
);
export const PowerAllocated = envelope(
  'site.power-allocated',
  z.object({
    siteId: z.string(),
    siteCapW: z.number().int(),
    strategy: z.enum(['optimal', 'fallback-equal-share']),
    allocations: z.array(z.object({ sessionId: uuid, stationId: z.string(), connectorId: z.number().int(), limitW: z.number().int() })),
    computedAt: ts,
  }),
);
export const SessionEvent = z.discriminatedUnion('type', [ReservationCreated, ReservationEnded, SessionStarted, SessionCompleted, PowerAllocated]);
export type SessionEvent = z.infer<typeof SessionEvent>;
export type SessionStarted = z.infer<typeof SessionStarted>;
export type SessionCompleted = z.infer<typeof SessionCompleted>;

// ---------------- billing.events.v1 (producer: billing via outbox, key: sessionId)
export const PaymentAuthorized = envelope(
  'billing.payment-authorized',
  z.object({ sessionId: uuid, driverId: uuid, authorizationId: z.string(), hold: money }),
);
export const InvoiceIssued = envelope(
  'billing.invoice-issued',
  z.object({
    invoiceId: uuid,
    invoiceNumber: z.string().optional(),
    sessionId: uuid,
    driverId: uuid.nullable(),
    total: money,
    net: money.optional(),
    vat: money.optional(),
    vatRateBp: z.number().int().min(0).optional(),
    lines: z.array(z.object({ kind: z.string(), description: z.string(), quantity: z.number(), unit: z.string(), amountMinor: z.number().int() })),
  }),
);
export const PaymentCaptured = envelope('billing.payment-captured', z.object({ invoiceId: uuid, sessionId: uuid, captured: money, pspReference: z.string() }));
export const PaymentFailed = envelope('billing.payment-failed', z.object({ invoiceId: uuid.nullable(), sessionId: uuid, step: z.string(), reason: z.string() }));
export const CreditNoteIssued = envelope(
  'billing.credit-note-issued',
  z.object({
    creditNoteId: uuid,
    creditNumber: z.string(),
    invoiceId: uuid,
    invoiceNumber: z.string().optional(),
    sessionId: uuid,
    driverId: uuid.nullable(),
    amount: money,
    net: money,
    vat: money,
    vatRateBp: z.number().int().min(0),
    reason: z.string(),
  }),
);
export const BillingEvent = z.discriminatedUnion('type', [PaymentAuthorized, InvoiceIssued, PaymentCaptured, PaymentFailed, CreditNoteIssued]);
export type BillingEvent = z.infer<typeof BillingEvent>;

// ---------------- ocpp.raw-log.v1 (producer: station-gateway, key: stationId)
/** Compact record, NOT an Envelope: highest-volume topic, audit/debug only, 14d retention (ADR 0003). */
export const OcppRawLogRecord = z.object({
  stationId: z.string(),
  gatewayInstance: z.string(),
  direction: z.enum(['in', 'out']),
  at: ts,
  /** The exact JSON text of the frame as sent/received. */
  frame: z.string(),
});
export type OcppRawLogRecord = z.infer<typeof OcppRawLogRecord>;

export { ChargingProfile };

/** Every event schema, for JSON Schema generation and cross-language contract tests. */
export const ALL_EVENTS = {
  StationConnected,
  StationDisconnected,
  StationBooted,
  ConnectorStatusChanged,
  MeterValuesReceived,
  StationCommandRequested,
  StationCommandCompleted,
  ReservationCreated,
  ReservationEnded,
  SessionStarted,
  SessionCompleted,
  PowerAllocated,
  PaymentAuthorized,
  InvoiceIssued,
  PaymentCaptured,
  PaymentFailed,
  CreditNoteIssued,
  OcppRawLogRecord,
} as const;
