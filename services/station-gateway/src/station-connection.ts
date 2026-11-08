import { SpanKind, SpanStatusCode, trace } from '@opentelemetry/api';
import { ocpp } from '@evcharge/contracts';
import { HttpError, inSpan, makeEnvelope, sleep, type Logger } from '@evcharge/platform';
import { WebSocket, type RawData } from 'ws';
import { allocationValidTo, AllocationTracker, replacesAllocationWithoutExpiry, startDefaultProfile } from './charging';
import { CommandChannel, type CommandOutcome, type CsmsAction, type OutboundCommand } from './command-channel';
import type { GatewayMetrics } from './metrics';
import { encodeCall, encodeError, encodeResult, parseFrame, validateStationCall, type OcppErrorCode } from './ocpp/framing';
import { isReplayed, toReadings } from './ocpp/meter';
import { BufferFullError, type EventPublisher, type GatewayTopics } from './publisher';
import type { SessionsClient, StationCredential } from './sessions-client';
import { errorMessage } from './util';

export const PRODUCER = 'station-gateway';

export interface ConnectionContext {
  instance: string;
  publisher: EventPublisher;
  sessions: SessionsClient;
  topics: GatewayTopics;
  metrics: GatewayMetrics;
  logger: Logger;
  allocations: AllocationTracker;
  commandQueue: number;
  callTimeoutMs: number;
  replayThresholdS: number;
}

/** Thrown inside a CALL handler to answer with a specific CALLERROR. */
class OcppError extends Error {
  constructor(
    readonly code: OcppErrorCode,
    message: string,
  ) {
    super(message);
  }
}

/**
 * One authenticated station socket: answers station CALLs (ADR 0003), runs the outbound command
 * channel (one outstanding CSMS CALL, queue 32, 30 s timeout) and applies the ADR 0006 profiles.
 */
export class StationConnection {
  readonly connectedAt = new Date().toISOString();
  /** Registry epoch; 0 until the route registration completed. */
  epoch = 0;
  alive = true;
  closed = false;
  readonly channel: CommandChannel;

  constructor(
    readonly stationId: string,
    readonly ws: WebSocket,
    readonly credential: StationCredential,
    private readonly ctx: ConnectionContext,
  ) {
    this.channel = new CommandChannel((id, action, payload) => this.sendCall(id, action, payload), { capacity: ctx.commandQueue, timeoutMs: ctx.callTimeoutMs });
  }

  /** Closes the socket (idempotent). The `close` event performs the cleanup. */
  close(code: number, reason: string): void {
    if (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING) this.ws.close(code, reason);
  }

  /** Called once from the socket's close event. */
  onClosed(): void {
    if (this.closed) return;
    this.closed = true;
    this.channel.close();
    this.ctx.allocations.clearStation(this.stationId);
  }

  /** Queues a CSMS -> station command; `done` receives the outcome. */
  command(action: CsmsAction, payload: Record<string, unknown>, done: OutboundCommand['onDone'], expiresAtMs?: number): void {
    const t0 = performance.now();
    // Child of the active span: the `command <action>` consumer span (which carries the command's traceparent) or the Boot CALL span.
    // shortcut: starts at enqueue, so it includes time queued behind the one outstanding CALL; fine while queues are short.
    const span = trace.getTracer('@evcharge/station-gateway').startSpan(`ocpp ${action} call`, {
      kind: SpanKind.CLIENT,
      attributes: { 'ocpp.action': action, 'ocpp.station_id': this.stationId, 'ocpp.gateway': this.ctx.instance },
    });
    this.channel.enqueue({
      action,
      payload,
      ...(expiresAtMs !== undefined ? { expiresAtMs } : {}),
      onDone: (outcome, response) => {
        span.setAttribute('ocpp.result', outcome);
        if (outcome === 'Error' || outcome === 'Timeout') span.setStatus({ code: SpanStatusCode.ERROR, message: outcome });
        span.end();
        this.ctx.metrics.commands.inc({ action, outcome });
        if (outcome === 'Accepted' || outcome === 'Rejected' || outcome === 'Error' || outcome === 'Timeout') this.ctx.metrics.commandDuration.observe({ action }, (performance.now() - t0) / 1000);
        if (action === 'SetChargingProfile' && outcome === 'Accepted') this.trackProfile(payload);
        done(outcome, response);
      },
    });
  }

  onMessage(data: RawData, isBinary: boolean): void {
    const text = isBinary ? '' : rawToString(data);
    this.ctx.publisher.rawLog(this.stationId, 'in', text);
    const frame = isBinary ? ({ kind: 'invalid', uniqueId: '-1', code: 'FormationViolation', description: 'binary frames are not OCPP-J' } as const) : parseFrame(text);
    switch (frame.kind) {
      case 'call':
        this.ctx.metrics.messages.inc({ direction: 'in', type: 'call', action: knownAction(frame.action) });
        void this.handleCall(frame.uniqueId, frame.action, frame.payload);
        return;
      case 'result':
        this.ctx.metrics.messages.inc({ direction: 'in', type: 'result', action: '' });
        if (!this.channel.handleResult(frame.uniqueId, frame.payload)) this.ctx.logger.debug({ stationId: this.stationId, uniqueId: frame.uniqueId }, 'CALLRESULT for no outstanding CALL');
        return;
      case 'error':
        this.ctx.metrics.messages.inc({ direction: 'in', type: 'error', action: '' });
        if (!this.channel.handleError(frame.uniqueId, frame.code, frame.description)) this.ctx.logger.debug({ stationId: this.stationId, uniqueId: frame.uniqueId }, 'CALLERROR for no outstanding CALL');
        return;
      case 'invalid':
        this.ctx.metrics.messages.inc({ direction: 'in', type: 'invalid', action: '' });
        this.sendError(frame.uniqueId, frame.code, frame.description, '');
    }
  }

  private async handleCall(uniqueId: string, action: string, payload: Record<string, unknown>): Promise<void> {
    const t0 = performance.now();
    const label = knownAction(action);
    const observe = (outcome: string) => this.ctx.metrics.callDuration.observe({ action: label, outcome }, (performance.now() - t0) / 1000);
    await inSpan(
      `ocpp ${label}`,
      { kind: SpanKind.SERVER, attributes: { 'ocpp.action': label, 'ocpp.unique_id': uniqueId, 'ocpp.station_id': this.stationId, 'ocpp.gateway': this.ctx.instance } },
      async (span) => {
        const fail = (code: OcppErrorCode, description: string) => {
          span.setAttributes({ 'ocpp.result': code, 'ocpp.error_code': code });
          if (code === 'InternalError') span.setStatus({ code: SpanStatusCode.ERROR, message: description });
          this.sendError(uniqueId, code, description, label);
          observe(code);
        };
        const v = validateStationCall(action, payload);
        if (!v.ok) return fail(v.code, v.description);
        try {
          const conf = await this.dispatch(action as ocpp.StationAction, uniqueId, v.value);
          this.sendResult(uniqueId, conf, label);
          span.setAttribute('ocpp.result', 'ok');
          observe('ok');
          if (action === 'BootNotification') this.afterBoot();
        } catch (err) {
          if (!(err instanceof OcppError)) this.ctx.logger.warn({ stationId: this.stationId, uniqueId, action, err: errorMessage(err) }, 'CALL failed');
          fail(err instanceof OcppError ? err.code : 'InternalError', err instanceof OcppError ? err.message : err instanceof BufferFullError ? 'backend busy' : 'backend unavailable');
        }
      },
    );
  }

  private async dispatch(action: ocpp.StationAction, uniqueId: string, req: unknown): Promise<Record<string, unknown>> {
    const { publisher, topics, sessions } = this.ctx;
    const stationId = this.stationId;
    switch (action) {
      case 'Heartbeat':
        return { currentTime: nowIso() };
      case 'BootNotification': {
        const r = req as ocpp.StationReq<'BootNotification'>;
        await publisher.publish(
          topics.stationEvents,
          makeEnvelope('station.booted', stationId, { stationId, vendor: r.chargePointVendor, model: r.chargePointModel, ...(r.firmwareVersion ? { firmwareVersion: r.firmwareVersion } : {}) }, PRODUCER),
        );
        return { status: 'Accepted', currentTime: nowIso(), interval: ocpp.Timing.HEARTBEAT_INTERVAL_S };
      }
      case 'StatusNotification': {
        const r = req as ocpp.StationReq<'StatusNotification'>;
        await publisher.publish(
          topics.stationEvents,
          makeEnvelope(
            'station.connector-status-changed',
            stationId,
            { stationId, connectorId: r.connectorId, status: r.status, errorCode: r.errorCode, ...(r.timestamp ? { stationTimestamp: new Date(r.timestamp).toISOString() } : {}) },
            PRODUCER,
          ),
        );
        return {};
      }
      case 'MeterValues': {
        const r = req as ocpp.StationReq<'MeterValues'>;
        let readings;
        try {
          readings = toReadings(r.meterValue);
        } catch (err) {
          throw new OcppError('TypeConstraintViolation', errorMessage(err));
        }
        const replayed = isReplayed(readings, Date.now(), this.ctx.replayThresholdS);
        trace.getActiveSpan()?.setAttribute('ocpp.replayed', replayed);
        await publisher.publish(
          topics.meterValues,
          makeEnvelope(
            'telemetry.meter-values',
            stationId,
            {
              stationId,
              connectorId: r.connectorId,
              ...(r.transactionId !== undefined ? { transactionId: r.transactionId } : {}),
              ocppMessageId: uniqueId,
              replayed,
              readings,
            },
            PRODUCER,
          ),
        );
        return {};
      }
      case 'Authorize': {
        const r = req as ocpp.StationReq<'Authorize'>;
        try {
          return (await this.withRetryOnInFlight(() => sessions.authorize(stationId, r.idTag))) as unknown as Record<string, unknown>;
        } catch (err) {
          if (isPermanentClientError(err)) return { idTagInfo: { status: 'Invalid' } };
          throw err;
        }
      }
      case 'StartTransaction': {
        const r = req as ocpp.StationReq<'StartTransaction'>;
        let res;
        try {
          res = await this.withRetryOnInFlight(() =>
            sessions.startTransaction({
              stationId,
              ocppMessageId: uniqueId,
              connectorId: r.connectorId,
              idTag: r.idTag,
              meterStartWh: r.meterStart,
              timestamp: new Date(r.timestamp).toISOString(),
              ...(r.reservationId !== undefined ? { reservationId: r.reservationId } : {}),
            }),
          );
        } catch (err) {
          // A permanent rejection must not make the station retry forever: answer with an Invalid tag and no transaction.
          if (isPermanentClientError(err)) return { idTagInfo: { status: 'Invalid' }, transactionId: 0 };
          throw err;
        }
        return { idTagInfo: res.idTagInfo, transactionId: res.transactionId };
      }
      case 'StopTransaction': {
        const r = req as ocpp.StationReq<'StopTransaction'>;
        let res;
        try {
          res = await this.withRetryOnInFlight(() =>
            sessions.stopTransaction({
              stationId,
              ocppMessageId: uniqueId,
              transactionId: r.transactionId,
              meterStopWh: r.meterStop,
              timestamp: new Date(r.timestamp).toISOString(),
              ...(r.reason ? { reason: r.reason } : {}),
              ...(r.idTag ? { idTag: r.idTag } : {}),
            }),
          );
        } catch (err) {
          // ADR 0003: an unknown or unacceptable stop is accepted, rejecting it would make the station retry forever.
          if (isPermanentClientError(err)) {
            this.ctx.logger.warn({ stationId, transactionId: r.transactionId, status: err instanceof HttpError ? err.status : 0 }, 'StopTransaction rejected by sessions; accepted towards the station');
            return {};
          }
          throw err;
        }
        if (res.sessionId === null) this.ctx.logger.info({ stationId, transactionId: r.transactionId }, 'StopTransaction for unknown transaction accepted');
        return res.idTagInfo ? { idTagInfo: res.idTagInfo } : {};
      }
      default:
        throw new OcppError('NotImplemented', `action ${String(action)} is not implemented`);
    }
  }

  /** 409 means the same idempotency key is still being processed: wait briefly and retry once, then give up (InternalError). */
  private async withRetryOnInFlight<T>(fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (err) {
      if (!(err instanceof HttpError) || err.status !== 409) throw err;
      await sleep(IN_FLIGHT_RETRY_MS);
      return fn();
    }
  }

  /** ADR 0006: TxDefaultProfile stackLevel 0 at startW right after an accepted Boot. */
  private afterBoot(): void {
    const startW = this.credential.chargingDefaults.startW;
    this.command('SetChargingProfile', startDefaultProfile(startW) as unknown as Record<string, unknown>, (outcome: CommandOutcome) => {
      if (outcome !== 'Accepted') this.ctx.logger.warn({ stationId: this.stationId, outcome }, 'start default profile not accepted');
    });
  }

  private trackProfile(payload: Record<string, unknown>): void {
    const req = ocpp.CsmsToStation.SetChargingProfile.req.safeParse(payload);
    if (!req.success) return;
    const validTo = allocationValidTo(req.data);
    if (validTo !== undefined) this.ctx.allocations.record(this.stationId, req.data.connectorId, validTo);
    else if (replacesAllocationWithoutExpiry(req.data)) this.ctx.allocations.clear(this.stationId, req.data.connectorId);
  }

  private sendCall(uniqueId: string, action: CsmsAction, payload: Record<string, unknown>): boolean {
    if (this.ws.readyState !== WebSocket.OPEN) return false;
    this.write(encodeCall(uniqueId, action, payload), 'call', action);
    return true;
  }

  private sendResult(uniqueId: string, payload: Record<string, unknown>, action: string): void {
    this.write(encodeResult(uniqueId, payload), 'result', action);
  }

  private sendError(uniqueId: string, code: OcppErrorCode, description: string, action: string): void {
    this.write(encodeError(uniqueId, code, description), 'error', action);
  }

  private write(text: string, type: string, action: string): void {
    if (this.ws.readyState !== WebSocket.OPEN) return;
    this.ws.send(text);
    this.ctx.metrics.messages.inc({ direction: 'out', type, action });
    this.ctx.publisher.rawLog(this.stationId, 'out', text);
  }
}

const IN_FLIGHT_RETRY_MS = 200;

/** 4xx that will never succeed on retry. 408, 409 and 429 are transient and stay InternalError. */
const isPermanentClientError = (err: unknown): boolean => err instanceof HttpError && err.status >= 400 && err.status < 500 && ![408, 409, 429].includes(err.status);

const nowIso = (): string => new Date().toISOString();

const KNOWN = new Set<string>([...ocpp.STATION_ACTIONS, ...ocpp.CSMS_ACTIONS]);
/** Bounded metric label cardinality: unknown actions are reported as "other". */
const knownAction = (a: string): string => (KNOWN.has(a) ? a : 'other');

function rawToString(data: RawData): string {
  if (typeof data === 'string') return data;
  if (Buffer.isBuffer(data)) return data.toString('utf8');
  if (Array.isArray(data)) return Buffer.concat(data).toString('utf8');
  return Buffer.from(data).toString('utf8');
}
