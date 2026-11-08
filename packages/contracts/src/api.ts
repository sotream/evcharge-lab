/**
 * Internal synchronous HTTP contracts between services (ADR 0002).
 * All internal calls carry:
 *   x-internal-token: shared secret (INTERNAL_TOKEN)
 *   traceparent:      W3C trace context
 *   idempotency-key:  on every mutating call (retries are safe)
 * Errors use application/problem+json: {type, title, status, detail, code}.
 */
import { z } from 'zod';
import { IdTagInfo } from './ocpp';

const ts = z.iso.datetime({ offset: true });
const uuid = z.uuid();

export const Problem = z.object({ type: z.string(), title: z.string(), status: z.number().int(), detail: z.string().optional(), code: z.string().optional() });
export type Problem = z.infer<typeof Problem>;

export const Money = z.object({ amountMinor: z.number().int(), currency: z.string().length(3) });

// ========== sessions: internal OCPP-facing API (called by station-gateway) ==========
export const SessionsInternal = {
  /** GET /internal/stations/:stationId/credential */
  stationCredential: z.object({
    stationId: z.string(),
    siteId: z.string(),
    /** hex SHA-256 of the station's 128-bit random secret (ADR 0003: no slow KDF for high-entropy secrets). */
    secretSha256: z.string().length(64),
    enabled: z.boolean(),
    /** Static profiles the gateway applies on its own (ADR 0006). */
    chargingDefaults: z.object({
      /** TxDefaultProfile limit: min(minW, safeShareW). Every new session starts here. */
      startW: z.number().int().min(0),
      /** floor(siteCapW / connectorsAtSite): applied by the gateway when allocations expire. */
      safeShareW: z.number().int().min(0),
    }),
  }),
  /** POST /internal/ocpp/authorize */
  authorizeReq: z.object({ stationId: z.string(), idTag: z.string() }),
  authorizeRes: z.object({ idTagInfo: IdTagInfo }),
  /** POST /internal/ocpp/start-transaction  (idempotency-key = `${stationId}:${ocppMessageId}`) */
  startTxReq: z.object({
    stationId: z.string(),
    ocppMessageId: z.string(),
    connectorId: z.number().int(),
    idTag: z.string(),
    meterStartWh: z.number().int(),
    timestamp: ts,
    reservationId: z.number().int().optional(),
  }),
  startTxRes: z.object({ transactionId: z.number().int(), sessionId: uuid, idTagInfo: IdTagInfo }),
  /** POST /internal/ocpp/stop-transaction  (idempotency-key = `${stationId}:${ocppMessageId}`) */
  stopTxReq: z.object({
    stationId: z.string(),
    ocppMessageId: z.string(),
    transactionId: z.number().int(),
    meterStopWh: z.number().int(),
    timestamp: ts,
    reason: z.string().optional(),
    idTag: z.string().optional(),
  }),
  stopTxRes: z.object({ sessionId: uuid.nullable(), idTagInfo: IdTagInfo.optional() }),
  /** POST /internal/drivers  (called by driver-api on registration) */
  registerDriverReq: z.object({ driverId: uuid, idTag: z.string().min(1).max(20) }),
} as const;

// ========== sessions: driver-facing API (proxied by driver-api; header x-driver-id) ==========
export const Connector = z.object({
  connectorId: z.number().int(),
  status: z.string(),
  maxPowerW: z.number().int(),
  tariffId: z.string(),
});
export const Station = z.object({
  stationId: z.string(),
  siteId: z.string(),
  online: z.boolean(),
  lat: z.number(),
  lng: z.number(),
  connectors: z.array(Connector),
});
export const Site = z.object({ siteId: z.string(), name: z.string(), lat: z.number(), lng: z.number(), capW: z.number().int(), stations: z.array(Station) });
export const SessionsPublic = {
  /** GET /v1/sites?lat=&lng=&radiusKm= */
  listSitesRes: z.object({ sites: z.array(Site) }),
  /** POST /v1/reservations  (idempotency-key required) */
  /** Reservation TTL is fixed at 15 min (ADR 0000 SLO "Reservations"). */
  createReservationReq: z.object({ stationId: z.string(), connectorId: z.number().int().min(1) }),
  reservation: z.object({
    reservationId: z.number().int(),
    driverId: uuid,
    stationId: z.string(),
    connectorId: z.number().int(),
    status: z.enum(['Pending', 'Active', 'Used', 'Cancelled', 'Expired', 'Rejected']),
    startsAt: ts,
    expiresAt: ts,
  }),
  /** POST /v1/sessions/remote-start  (idempotency-key required) */
  remoteStartReq: z.object({ stationId: z.string(), connectorId: z.number().int().min(1) }),
  remoteStartRes: z.object({ commandId: uuid, status: z.literal('Requested') }),
  /** GET /v1/sessions/:sessionId, POST /v1/sessions/:sessionId/stop */
  session: z.object({
    sessionId: uuid,
    transactionId: z.number().int(),
    driverId: uuid.nullable(),
    stationId: z.string(),
    connectorId: z.number().int(),
    status: z.enum(['Active', 'Stopping', 'Completed']),
    startedAt: ts,
    stoppedAt: ts.nullable(),
    energyWh: z.number().int().nullable(),
    limitW: z.number().int().nullable(),
  }),
} as const;

// ========== smart-charging (Go): POST /v1/allocate ==========
export const AllocateReq = z.object({
  siteId: z.string(),
  siteCapW: z.number().int().min(0),
  /** connectors at the site with no active session; each reserves startW (ADR 0006). */
  idleConnectors: z.number().int().min(0),
  /** chargingDefaults.startW for this site. */
  startW: z.number().int().min(0),
  /** reference time for the 60 s measured-demand rule; makes the answer a pure function of the request (default: server clock). */
  computedAt: ts.optional(),
  sessions: z.array(
    z.object({
      sessionId: uuid,
      stationId: z.string(),
      connectorId: z.number().int(),
      /** connector/vehicle maximum */
      maxW: z.number().int().min(0),
      /** below this the EV cannot charge (6A * 230V * phases); 0 = no minimum */
      minW: z.number().int().min(0),
      /** latest measured draw, if known (telemetry) */
      measuredW: z.number().int().min(0).optional(),
      priority: z.number().int().min(0).max(10).default(0),
      startedAt: ts,
    }),
  ),
});
export const AllocateRes = z.object({
  siteId: z.string(),
  strategy: z.literal('optimal'),
  totalAllocatedW: z.number().int(),
  /** idleConnectors x startW held back for sessions that may start before the next round. */
  reserveW: z.number().int(),
  allocations: z.array(z.object({ sessionId: uuid, limitW: z.number().int(), suspended: z.boolean() })),
});
export type AllocateReq = z.infer<typeof AllocateReq>;
export type AllocateRes = z.infer<typeof AllocateRes>;

// ========== tariff (Go): POST /v1/quote ==========
export const QuoteReq = z.object({ tariffId: z.string(), startedAt: ts, stoppedAt: ts, energyWh: z.number().int().min(0) });
export const QuoteLine = z.object({ kind: z.enum(['energy', 'time', 'session', 'idle']), description: z.string(), quantity: z.number(), unit: z.string(), amountMinor: z.number().int() });
export const QuoteRes = z.object({ tariffId: z.string(), currency: z.string().length(3), lines: z.array(QuoteLine), totalMinor: z.number().int() });
export type QuoteReq = z.infer<typeof QuoteReq>;
export type QuoteRes = z.infer<typeof QuoteRes>;

// ========== billing: driver-facing ==========
export const BillingPublic = {
  /** PUT /v1/drivers/:driverId/payment-method */
  paymentMethodReq: z.object({ token: z.string().min(4) }),
  /** GET /v1/invoices?driverId=, GET /v1/invoices/:invoiceId */
  invoice: z.object({
    invoiceId: uuid,
    sessionId: uuid,
    driverId: uuid.nullable(),
    status: z.enum(['Issued', 'Paid', 'PaymentFailed']),
    total: Money,
    issuedAt: ts,
  }),
} as const;

// ========== telemetry-ingest (Go): GET /v1/stations/:id/connectors/:cid/power?from=&to=&step=
export const PowerSeriesRes = z.object({
  stationId: z.string(),
  connectorId: z.number().int(),
  points: z.array(z.object({ t: ts, avgW: z.number(), maxW: z.number() })),
});
