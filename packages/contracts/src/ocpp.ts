/**
 * OCPP 1.6J subset used by evcharge-lab (see ADR 0003).
 *
 * Frames on the wire are JSON arrays:
 *   CALL       [2, uniqueId, action, payload]
 *   CALLRESULT [3, uniqueId, payload]
 *   CALLERROR  [4, uniqueId, errorCode, errorDescription, errorDetails]
 *
 * WebSocket subprotocol: "ocpp1.6". Station identity is the last path segment
 * of the connection URL: ws://host/ocpp/{stationId}. Credentials: HTTP Basic
 * on the upgrade request (OCPP security profile 1), username = stationId.
 */
import { z } from 'zod';

export const OCPP_SUBPROTOCOL = 'ocpp1.6';

export const MessageTypeId = { Call: 2, CallResult: 3, CallError: 4 } as const;

export const CallFrame = z.tuple([z.literal(2), z.string().min(1).max(36), z.string().min(1), z.record(z.string(), z.unknown())]);
export const CallResultFrame = z.tuple([z.literal(3), z.string().min(1).max(36), z.record(z.string(), z.unknown())]);
export const CallErrorFrame = z.tuple([
  z.literal(4),
  z.string().min(1).max(36),
  z.string(),
  z.string(),
  z.record(z.string(), z.unknown()),
]);
export const Frame = z.union([CallFrame, CallResultFrame, CallErrorFrame]);
export type CallFrame = z.infer<typeof CallFrame>;
export type CallResultFrame = z.infer<typeof CallResultFrame>;
export type CallErrorFrame = z.infer<typeof CallErrorFrame>;
export type Frame = z.infer<typeof Frame>;

export const OcppErrorCode = z.enum([
  'NotImplemented',
  'NotSupported',
  'InternalError',
  'ProtocolError',
  'SecurityError',
  'FormationViolation',
  'PropertyConstraintViolation',
  'OccurenceConstraintViolation',
  'TypeConstraintViolation',
  'GenericError',
]);
export type OcppErrorCode = z.infer<typeof OcppErrorCode>;

const dateTime = z.iso.datetime({ offset: true });
const idToken = z.string().min(1).max(20);
const connectorId = z.number().int().min(0);

export const IdTagInfo = z.object({
  status: z.enum(['Accepted', 'Blocked', 'Expired', 'Invalid', 'ConcurrentTx']),
  expiryDate: dateTime.optional(),
  parentIdTag: idToken.optional(),
});

export const RegistrationStatus = z.enum(['Accepted', 'Pending', 'Rejected']);

export const ChargePointStatus = z.enum([
  'Available',
  'Preparing',
  'Charging',
  'SuspendedEVSE',
  'SuspendedEV',
  'Finishing',
  'Reserved',
  'Unavailable',
  'Faulted',
]);
export type ChargePointStatus = z.infer<typeof ChargePointStatus>;

export const Measurand = z.enum([
  'Energy.Active.Import.Register',
  'Power.Active.Import',
  'Current.Import',
  'Voltage',
  'SoC',
]);
export type Measurand = z.infer<typeof Measurand>;

export const SampledValue = z.object({
  value: z.string(),
  context: z.enum(['Interruption.Begin', 'Interruption.End', 'Sample.Clock', 'Sample.Periodic', 'Transaction.Begin', 'Transaction.End', 'Trigger', 'Other']).optional(),
  measurand: Measurand.optional(),
  unit: z.enum(['Wh', 'kWh', 'W', 'kW', 'A', 'V', 'Percent']).optional(),
  phase: z.string().optional(),
});
export const MeterValue = z.object({ timestamp: dateTime, sampledValue: z.array(SampledValue).min(1) });

// ---------- Station -> CSMS (CALL) ----------
export const StationToCsms = {
  BootNotification: {
    req: z.object({
      chargePointVendor: z.string().max(20),
      chargePointModel: z.string().max(20),
      chargePointSerialNumber: z.string().max(25).optional(),
      firmwareVersion: z.string().max(50).optional(),
    }),
    conf: z.object({ status: RegistrationStatus, currentTime: dateTime, interval: z.number().int().min(0) }),
  },
  Heartbeat: {
    req: z.object({}),
    conf: z.object({ currentTime: dateTime }),
  },
  StatusNotification: {
    req: z.object({
      connectorId,
      errorCode: z.string(),
      status: ChargePointStatus,
      timestamp: dateTime.optional(),
      info: z.string().max(50).optional(),
    }),
    conf: z.object({}),
  },
  Authorize: {
    req: z.object({ idTag: idToken }),
    conf: z.object({ idTagInfo: IdTagInfo }),
  },
  StartTransaction: {
    req: z.object({
      connectorId: connectorId.min(1),
      idTag: idToken,
      meterStart: z.number().int(),
      reservationId: z.number().int().optional(),
      timestamp: dateTime,
    }),
    conf: z.object({ idTagInfo: IdTagInfo, transactionId: z.number().int() }),
  },
  StopTransaction: {
    req: z.object({
      transactionId: z.number().int(),
      idTag: idToken.optional(),
      meterStop: z.number().int(),
      timestamp: dateTime,
      reason: z.enum(['EmergencyStop', 'EVDisconnected', 'HardReset', 'Local', 'Other', 'PowerLoss', 'Reboot', 'Remote', 'SoftReset', 'UnlockCommand', 'DeAuthorized']).optional(),
      transactionData: z.array(MeterValue).optional(),
    }),
    conf: z.object({ idTagInfo: IdTagInfo.optional() }),
  },
  MeterValues: {
    req: z.object({ connectorId, transactionId: z.number().int().optional(), meterValue: z.array(MeterValue).min(1) }),
    conf: z.object({}),
  },
} as const;
export type StationAction = keyof typeof StationToCsms;
export const STATION_ACTIONS = Object.keys(StationToCsms) as StationAction[];

// ---------- CSMS -> Station (CALL) ----------
export const ChargingSchedulePeriod = z.object({
  startPeriod: z.number().int().min(0),
  limit: z.number().min(0),
  numberPhases: z.number().int().min(1).max(3).optional(),
});
export const ChargingProfile = z.object({
  chargingProfileId: z.number().int(),
  transactionId: z.number().int().optional(),
  stackLevel: z.number().int().min(0),
  chargingProfilePurpose: z.enum(['ChargePointMaxProfile', 'TxDefaultProfile', 'TxProfile']),
  chargingProfileKind: z.enum(['Absolute', 'Recurring', 'Relative']),
  validTo: dateTime.optional(),
  chargingSchedule: z.object({
    duration: z.number().int().optional(),
    startSchedule: dateTime.optional(),
    /** evcharge-lab always uses "W" (ADR 0006). */
    chargingRateUnit: z.enum(['W', 'A']),
    chargingSchedulePeriod: z.array(ChargingSchedulePeriod).min(1),
  }),
});
export type ChargingProfile = z.infer<typeof ChargingProfile>;

export const CsmsToStation = {
  RemoteStartTransaction: {
    req: z.object({ connectorId: connectorId.min(1).optional(), idTag: idToken, chargingProfile: ChargingProfile.optional() }),
    conf: z.object({ status: z.enum(['Accepted', 'Rejected']) }),
  },
  RemoteStopTransaction: {
    req: z.object({ transactionId: z.number().int() }),
    conf: z.object({ status: z.enum(['Accepted', 'Rejected']) }),
  },
  ReserveNow: {
    req: z.object({ connectorId, expiryDate: dateTime, idTag: idToken, reservationId: z.number().int() }),
    conf: z.object({ status: z.enum(['Accepted', 'Faulted', 'Occupied', 'Rejected', 'Unavailable']) }),
  },
  CancelReservation: {
    req: z.object({ reservationId: z.number().int() }),
    conf: z.object({ status: z.enum(['Accepted', 'Rejected']) }),
  },
  SetChargingProfile: {
    req: z.object({ connectorId, csChargingProfiles: ChargingProfile }),
    conf: z.object({ status: z.enum(['Accepted', 'Rejected', 'NotSupported']) }),
  },
  TriggerMessage: {
    req: z.object({ requestedMessage: z.enum(['BootNotification', 'Heartbeat', 'MeterValues', 'StatusNotification']), connectorId: connectorId.optional() }),
    conf: z.object({ status: z.enum(['Accepted', 'Rejected', 'NotImplemented']) }),
  },
} as const;
export type CsmsAction = keyof typeof CsmsToStation;
export const CSMS_ACTIONS = Object.keys(CsmsToStation) as CsmsAction[];

export type StationReq<A extends StationAction> = z.infer<(typeof StationToCsms)[A]['req']>;
export type StationConf<A extends StationAction> = z.infer<(typeof StationToCsms)[A]['conf']>;
export type CsmsReq<A extends CsmsAction> = z.infer<(typeof CsmsToStation)[A]['req']>;
export type CsmsConf<A extends CsmsAction> = z.infer<(typeof CsmsToStation)[A]['conf']>;

/**
 * Offline replay (ADR 0003): a station that was offline (up to 24h) replays
 * queued transaction-related CALLs (StartTransaction, StopTransaction,
 * MeterValues) in original order with their ORIGINAL uniqueId. Duplicates are
 * harmless by construction: Start/Stop are idempotent in `sessions` on
 * `${stationId}:${uniqueId}`, meter values are deduplicated by telemetry-ingest
 * on (station, connector, timestamp). The gateway keeps no dedup cache.
 */
export const REPLAYABLE_ACTIONS: readonly StationAction[] = ['StartTransaction', 'StopTransaction', 'MeterValues'];

/** Timing conventions (ADR 0000 traffic model). */
export const Timing = {
  /** BootNotification.conf interval. */
  HEARTBEAT_INTERVAL_S: 300,
  /** MeterValueSampleInterval for active connectors. */
  METER_INTERVAL_S: 30,
  /** One outstanding CSMS->station CALL; response timeout. */
  CALL_TIMEOUT_S: 30,
} as const;

/**
 * Smart-charging profile conventions (ADR 0006). All limits in W.
 *  - TxDefaultProfile, stackLevel 0, no validTo, limit = chargingDefaults.startW (sent by the gateway after Boot).
 *  - Allocation TxProfile, stackLevel 1, validTo = computedAt + ALLOCATION_VALIDITY_S (sent via station.commands).
 *  - Gateway safe-share TxProfile, stackLevel 1, no validTo, limit = chargingDefaults.safeShareW,
 *    sent by the owning gateway when an allocation profile expired ALLOCATION_GRACE_S ago with no newer one.
 */
export const ChargingProfiles = {
  DEFAULT_PROFILE_ID: 1,
  ALLOCATION_PROFILE_ID: 2,
  SAFE_SHARE_PROFILE_ID: 3,
  ALLOCATION_VALIDITY_S: 180,
  PERIODIC_REALLOCATION_S: 60,
  ALLOCATION_GRACE_S: 5,
} as const;
