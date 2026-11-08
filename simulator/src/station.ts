import { ocpp } from '@evcharge/contracts';
import type { Logger } from '@evcharge/platform';
import { WebSocket, type RawData } from 'ws';
import { CallQueue, type DurableAction } from './call-queue';
import { probeVehicle, randomVehicle, sessionDurationMs } from './demand';
import { fleet } from '@evcharge/contracts';
import { CONNECTORS_PER_STATION, type StationSpec } from './fleet';
import type { SimMetrics } from './metrics';
import { advanceCharge, curveW, drawnW, minChargeW, sampleElectrics, type ChargeState, type Vehicle } from './physics';
import type { ProbeLog } from './probe';
import { ProfileStack } from './profiles';
import { reconnectDelayMs } from './reconnect';
import { errorMessage, type Rng, type SimClock } from './util';
import type { TimerWheel, WheelTimer } from './wheel';

type Status = ocpp.ChargePointStatus;
type Obj = Record<string, unknown>;

/** Everything a station needs from the simulator process. */
export interface SimContext {
  clock: SimClock;
  wheel: TimerWheel;
  rng: Rng;
  metrics: SimMetrics;
  log: Logger;
  csmsUrl: string;
  secretSeed: string;
  probe?: ProbeLog;
  queueMax: number;
  /** Simulated seconds between MeterValues of a charging connector. */
  meterIntervalS: number;
  /** idTag for a random arrival; undefined = anonymous. */
  pickIdTag(rng: Rng): string;
  /** CALL response timeout in real ms (OCPP: the station gives up and reconnects). */
  callTimeoutMs: number;
  /** Station-side heartbeat period override (real s); default = BootNotification.conf.interval. */
  heartbeatS?: number;
  /** Notified when a station becomes ready (accepted Boot) or drops. */
  onConnectionChange(station: Station, connected: boolean): void;
}

export interface Tx {
  idTag: string;
  startMs: number;
  plannedEndMs: number;
  vehicle: Vehicle;
  charge: ChargeState;
  /** Assigned by the CSMS in the StartTransaction CALLRESULT; undefined until then (offline start). */
  transactionId?: number;
  meterStartWh: number;
  reservationId?: number;
  stopping: boolean;
  probe: boolean;
}

export class Connector {
  status: Status = 'Available';
  tx: Tx | undefined;
  /** Energy register (Wh), continues across sessions. */
  registerWh = 0;
  reservation: { reservationId: number; idTag: string; expiresMs: number; timer: WheelTimer } | undefined;
  meterTimer: WheelTimer | undefined;
  endTimer: WheelTimer | undefined;
  probeSeq = 0;
  /** A session is being set up (authorizing) or torn down (Finishing). */
  busy = false;

  constructor(readonly id: number) {}

  get idle(): boolean {
    return !this.tx && !this.busy && this.status === 'Available';
  }
}

type PendingCall = { uniqueId: string; action: string; payload: Obj; onResult?: (conf: Obj) => void; onError?: (code: string) => void; durable: boolean; sentAt: number };

const OPEN = WebSocket.OPEN;

/**
 * One virtual charge point: two connectors, the OCPP client with its offline queue and replay,
 * the reconnect policy, and the CSMS-initiated commands (ADR 0003, 0006, 0013).
 */
export class Station {
  readonly connectors: Connector[];
  readonly profiles = new ProfileStack();
  readonly secret: string;
  readonly queue: CallQueue;
  probe = false;

  /** The simulator wants this station connected (false: not started, or stopped). */
  wantOnline = false;
  ready = false;
  /** No connection attempts before this real time (offline mode). */
  blockedUntilMs = 0;
  attempt = 0;

  private ws: WebSocket | undefined;
  private generation = 0;
  private counter = 0;
  private volatileQ: PendingCall[] = [];
  private inflight: PendingCall | undefined;
  private inflightTimer: WheelTimer | undefined;
  private retryTimer: WheelTimer | undefined;
  private reconnectTimer: WheelTimer | undefined;
  private heartbeatTimer: WheelTimer | undefined;
  private waiters: ((ok: boolean) => void)[] = [];

  constructor(
    readonly spec: StationSpec,
    private readonly ctx: SimContext,
  ) {
    this.secret = fleet.stationSecret(ctx.secretSeed, spec.stationId);
    this.connectors = Array.from({ length: CONNECTORS_PER_STATION }, (_, i) => new Connector(i + 1));
    this.queue = new CallQueue(ctx.queueMax);
  }

  get id(): string {
    return this.spec.stationId;
  }

  get online(): boolean {
    return this.ready;
  }

  // ---------------------------------------------------------------- connection management

  /** Start (or continue) trying to stay connected. `delayMs` 0 connects now. */
  start(delayMs = 0, kind: 'initial' | 'retry' | 'after_drop' = 'initial'): void {
    this.wantOnline = true;
    this.scheduleConnect(delayMs, kind);
  }

  /** Resolves true when a connection became ready, false when this attempt failed. */
  whenSettled(): Promise<boolean> {
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  stop(): void {
    this.wantOnline = false;
    this.reconnectTimer?.cancel();
    this.dropSocket(1001, 'simulator shutdown');
  }

  /** Offline drill: close abruptly and stay away until `untilRealMs`; sessions keep running and queue messages. */
  goOffline(untilRealMs: number): void {
    this.blockedUntilMs = untilRealMs;
    this.reconnectTimer?.cancel();
    this.ws?.terminate();
    if (!this.ws) this.scheduleReconnect(0);
  }

  /** Storm drill: abrupt disconnect, then the normal reconnect policy. */
  drop(): void {
    this.ws?.terminate();
  }

  private scheduleConnect(delayMs: number, kind: 'initial' | 'retry' | 'after_drop'): void {
    this.reconnectTimer?.cancel();
    if (delayMs <= 0 && Date.now() >= this.blockedUntilMs) {
      this.reconnectTimer = undefined;
      this.connect(kind);
      return;
    }
    // After an offline block the policy delay counts from the end of the block (no synchronised reconnect).
    const wait = Math.max(0, this.blockedUntilMs - Date.now()) + delayMs;
    this.reconnectTimer = this.ctx.wheel.schedule(wait, () => this.connect(kind));
  }

  private scheduleReconnect(retryAfterMs: number): void {
    if (!this.wantOnline) return;
    const kind = this.attempt === 0 ? 'after_drop' : 'retry';
    const delay = reconnectDelayMs(this.attempt, this.ctx.rng, retryAfterMs);
    this.attempt++;
    this.scheduleConnect(delay, kind);
  }

  private connect(kind: 'initial' | 'retry' | 'after_drop'): void {
    this.reconnectTimer = undefined;
    if (!this.wantOnline || this.ws) return;
    const { metrics } = this.ctx;
    metrics.reconnects.inc({ kind });
    const gen = ++this.generation;
    const auth = Buffer.from(`${this.id}:${this.secret}`).toString('base64');
    let ws: WebSocket;
    try {
      ws = new WebSocket(`${this.ctx.csmsUrl}/${this.id}`, ocpp.OCPP_SUBPROTOCOL, { headers: { authorization: `Basic ${auth}` }, perMessageDeflate: false, handshakeTimeout: 15_000, maxPayload: 64 * 1024 });
    } catch (err) {
      metrics.connectFailures.inc({ reason: 'invalid' });
      this.ctx.log.warn({ stationId: this.id, err: errorMessage(err) }, 'cannot create socket');
      this.settle(false);
      this.scheduleReconnect(0);
      return;
    }
    this.ws = ws;
    let finished = false;
    let opened = false;
    const failed = (reason: string, retryAfterMs = 0) => {
      if (finished || gen !== this.generation) return;
      finished = true;
      metrics.connectFailures.inc({ reason });
      this.ws = undefined;
      this.settle(false);
      this.scheduleReconnect(retryAfterMs);
    };
    ws.on('open', () => {
      if (gen !== this.generation) return;
      opened = true;
      this.sendBoot();
    });
    ws.on('unexpected-response', (_req, res) => {
      const ra = Number(res.headers['retry-after']);
      res.resume();
      failed(`http_${res.statusCode ?? 0}`, Number.isFinite(ra) ? ra * 1000 : 0);
      ws.terminate();
    });
    ws.on('error', () => {
      // `close` follows for an open socket; a failed handshake needs handling here.
      if (!opened) failed('network');
    });
    ws.on('message', (data) => {
      if (gen === this.generation) this.onMessage(data);
    });
    ws.on('close', (code) => {
      if (gen !== this.generation || finished) return;
      if (!opened) return failed('network');
      finished = true;
      this.onClosed(code);
    });
  }

  private settle(ok: boolean): void {
    const w = this.waiters;
    this.waiters = [];
    for (const f of w) f(ok);
  }

  private dropSocket(code: number, reason: string): void {
    const ws = this.ws;
    if (!ws) return;
    if (ws.readyState === OPEN) ws.close(code, reason);
    else ws.terminate();
  }

  private onClosed(code: number): void {
    const wasReady = this.ready;
    this.ready = false;
    this.ws = undefined;
    this.inflightTimer?.cancel();
    this.heartbeatTimer?.cancel();
    this.retryTimer?.cancel();
    const inflight = this.inflight;
    this.inflight = undefined;
    // Volatile calls die with the connection; durable ones stay at the head of the queue and are resent.
    if (inflight && !inflight.durable) inflight.onError?.('Disconnected');
    for (const v of this.volatileQ.splice(0)) v.onError?.('Disconnected');
    if (wasReady) this.ctx.onConnectionChange(this, false);
    this.settle(false);
    if (code === 4000) {
      this.ctx.log.warn({ stationId: this.id }, 'connection replaced by the CSMS (4000); not reconnecting');
      this.wantOnline = false;
      return;
    }
    this.scheduleReconnect(0);
  }

  // ---------------------------------------------------------------- OCPP transport

  private nextId(): string {
    return `${this.id}-${(++this.counter).toString(36)}`;
  }

  private rawSend(frame: unknown[], action: string): boolean {
    const ws = this.ws;
    if (!ws || ws.readyState !== OPEN) return false;
    ws.send(JSON.stringify(frame));
    this.ctx.metrics.messages.inc({ direction: 'sent', action });
    return true;
  }

  /** A CALL that does not survive a disconnect (Authorize, Status, Heartbeat, Trigger replies). */
  private callVolatile(action: string, payload: Obj, onResult?: (conf: Obj) => void, onError?: (code: string) => void): void {
    if (!this.ready) {
      onError?.('Offline');
      return;
    }
    this.volatileQ.push({ uniqueId: this.nextId(), action, payload, durable: false, sentAt: 0, ...(onResult ? { onResult } : {}), ...(onError ? { onError } : {}) });
    this.pump();
  }

  private enqueueDurable(action: DurableAction, payload: Obj, extra: { onResult?: (c: Obj) => void; prepare?: (p: Obj) => void } = {}): void {
    const dropped = this.queue.push({ uniqueId: this.nextId(), action, payload, ...extra });
    if (dropped > 0) this.ctx.metrics.queueDropped.inc(dropped);
    this.pump();
  }

  private pump(): void {
    if (!this.ready || this.inflight) return;
    const v = this.volatileQ.shift();
    if (v) {
      this.transmit(v);
      return;
    }
    const head = this.queue.peek();
    if (!head) return;
    head.prepare?.(head.payload);
    this.transmit({ uniqueId: head.uniqueId, action: head.action, payload: head.payload, durable: true, sentAt: 0, ...(head.onResult ? { onResult: head.onResult } : {}) });
  }

  private transmit(call: PendingCall): void {
    call.sentAt = performance.now();
    if (!this.rawSend([ocpp.MessageTypeId.Call, call.uniqueId, call.action, call.payload], call.action)) {
      if (!call.durable) call.onError?.('Offline');
      return;
    }
    this.inflight = call;
    this.inflightTimer?.cancel();
    this.inflightTimer = this.ctx.wheel.schedule(this.ctx.callTimeoutMs, () => {
      if (this.inflight?.uniqueId !== call.uniqueId) return;
      // No answer: OCPP-J says the call failed; reconnecting resends durable calls with the same uniqueId.
      this.ctx.log.debug({ stationId: this.id, action: call.action }, 'CALL timed out; reconnecting');
      this.ws?.terminate();
    });
  }

  private onMessage(data: RawData): void {
    let msg: unknown;
    try {
      msg = JSON.parse(Buffer.isBuffer(data) ? data.toString('utf8') : String(data));
    } catch {
      return;
    }
    if (!Array.isArray(msg)) return;
    const type = msg[0];
    if (type === ocpp.MessageTypeId.Call) {
      const [, uniqueId, action, payload] = msg as [number, string, string, Obj];
      this.ctx.metrics.messages.inc({ direction: 'received', action: String(action) });
      this.onCsmsCall(uniqueId, action, payload ?? {});
      return;
    }
    const call = this.inflight;
    const uniqueId = msg[1];
    if (!call || call.uniqueId !== uniqueId) return;
    this.inflightTimer?.cancel();
    this.inflight = undefined;
    this.ctx.metrics.messages.inc({ direction: 'received', action: `${call.action}.${type === ocpp.MessageTypeId.CallResult ? 'conf' : 'error'}` });
    if (type === ocpp.MessageTypeId.CallResult) {
      this.ctx.metrics.callRtt.observe({ action: call.action }, (performance.now() - call.sentAt) / 1000);
      if (call.durable) this.queue.ack(call.uniqueId);
      call.onResult?.((msg[2] ?? {}) as Obj);
      this.pump();
      return;
    }
    const code = String(msg[2]);
    this.ctx.metrics.callErrors.inc({ action: call.action, code });
    if (call.durable) {
      if (code === 'InternalError' || code === 'GenericError') {
        // Transient backend trouble: keep the head and resend the same uniqueId shortly (ADR 0003, 0012).
        this.retryTimer?.cancel();
        this.retryTimer = this.ctx.wheel.schedule(1000 + this.ctx.rng() * 2000, () => this.pump());
        return;
      }
      this.queue.ack(call.uniqueId); // a permanent rejection must not wedge the queue
    } else {
      call.onError?.(code);
    }
    this.pump();
  }

  private sendBoot(): void {
    const payload = { chargePointVendor: 'evcharge-lab', chargePointModel: this.spec.kind === 'DC' ? 'SIM-DC' : 'SIM-AC', chargePointSerialNumber: this.id, firmwareVersion: 'sim-1.0' };
    const boot: PendingCall = {
      uniqueId: this.nextId(),
      action: 'BootNotification',
      payload,
      durable: false,
      sentAt: 0,
      onResult: (conf) => this.onBootConf(conf),
      onError: () => this.ws?.terminate(),
    };
    this.transmit(boot);
  }

  private onBootConf(conf: Obj): void {
    const status = conf['status'];
    if (status !== 'Accepted') {
      this.ctx.metrics.connectFailures.inc({ reason: `boot_${String(status)}` });
      const interval = Number(conf['interval']);
      this.ws?.terminate();
      this.scheduleReconnect(Number.isFinite(interval) ? interval * 1000 : 0);
      return;
    }
    this.ready = true;
    this.attempt = 0;
    this.ctx.onConnectionChange(this, true);
    this.settle(true);
    const interval = this.ctx.heartbeatS ?? (Number(conf['interval']) || ocpp.Timing.HEARTBEAT_INTERVAL_S);
    this.scheduleHeartbeat(interval, true);
    // Current state first, then the queued transaction messages in order.
    this.callVolatile('StatusNotification', this.statusPayload(0, 'Available'));
    for (const c of this.connectors) this.callVolatile('StatusNotification', this.statusPayload(c.id, c.status));
    this.pump();
  }

  private scheduleHeartbeat(intervalS: number, first = false): void {
    this.heartbeatTimer?.cancel();
    const delay = (first ? this.ctx.rng() : 1) * intervalS * 1000;
    this.heartbeatTimer = this.ctx.wheel.schedule(delay, () => {
      this.callVolatile('Heartbeat', {});
      this.scheduleHeartbeat(intervalS);
    });
  }

  private statusPayload(connectorId: number, status: Status): Obj {
    return { connectorId, errorCode: 'NoError', status, timestamp: this.ctx.clock.iso() };
  }

  // ---------------------------------------------------------------- CSMS -> station commands

  private reply(uniqueId: string, payload: Obj): void {
    this.rawSend([ocpp.MessageTypeId.CallResult, uniqueId, payload], 'conf');
  }

  private callError(uniqueId: string, code: string, description: string): void {
    this.rawSend([ocpp.MessageTypeId.CallError, uniqueId, code, description.slice(0, 255), {}], 'error');
  }

  private onCsmsCall(uniqueId: string, action: string, payload: Obj): void {
    const now = this.ctx.clock.now();
    switch (action) {
      case 'SetChargingProfile': {
        const req = ocpp.CsmsToStation.SetChargingProfile.req.safeParse(payload);
        if (!req.success) return this.callError(uniqueId, 'TypeConstraintViolation', req.error.issues[0]?.message ?? 'invalid');
        // Integrate up to now with the old limits before the new profile changes them.
        for (const c of this.connectors) if (c.tx) this.advance(c, now);
        const cid = req.data.connectorId;
        const target = this.connectors.find((c) => c.id === cid);
        const status = this.profiles.set(cid, req.data.csChargingProfiles, now, Boolean(target?.tx));
        this.reply(uniqueId, { status });
        for (const c of this.connectors) if (c.tx) this.updateChargingStatus(c, now);
        return;
      }
      case 'RemoteStartTransaction': {
        const req = ocpp.CsmsToStation.RemoteStartTransaction.req.safeParse(payload);
        if (!req.success) return this.callError(uniqueId, 'TypeConstraintViolation', req.error.issues[0]?.message ?? 'invalid');
        const c = req.data.connectorId ? this.connectors.find((x) => x.id === req.data.connectorId) : this.connectors.find((x) => x.idle || (x.status === 'Reserved' && x.reservation?.idTag === req.data.idTag));
        const usable = c && !c.tx && !c.busy && (c.status === 'Available' || (c.status === 'Reserved' && c.reservation?.idTag === req.data.idTag));
        if (!c || !usable) return this.reply(uniqueId, { status: 'Rejected' });
        c.busy = true;
        this.reply(uniqueId, { status: 'Accepted' });
        const profile = req.data.chargingProfile;
        this.ctx.wheel.schedule(200 + this.ctx.rng() * 800, () => {
          c.busy = false;
          this.startSession(c, req.data.idTag, { remote: true, ...(profile ? { profile } : {}) });
        });
        return;
      }
      case 'RemoteStopTransaction': {
        const req = ocpp.CsmsToStation.RemoteStopTransaction.req.safeParse(payload);
        if (!req.success) return this.callError(uniqueId, 'TypeConstraintViolation', req.error.issues[0]?.message ?? 'invalid');
        const c = this.connectors.find((x) => x.tx && x.tx.transactionId === req.data.transactionId);
        if (!c) return this.reply(uniqueId, { status: 'Rejected' });
        this.reply(uniqueId, { status: 'Accepted' });
        this.endSession(c, 'Remote');
        return;
      }
      case 'ReserveNow': {
        const req = ocpp.CsmsToStation.ReserveNow.req.safeParse(payload);
        if (!req.success) return this.callError(uniqueId, 'TypeConstraintViolation', req.error.issues[0]?.message ?? 'invalid');
        return this.reply(uniqueId, { status: this.reserve(req.data) });
      }
      case 'CancelReservation': {
        const req = ocpp.CsmsToStation.CancelReservation.req.safeParse(payload);
        if (!req.success) return this.callError(uniqueId, 'TypeConstraintViolation', req.error.issues[0]?.message ?? 'invalid');
        const c = this.connectors.find((x) => x.reservation?.reservationId === req.data.reservationId);
        if (!c) return this.reply(uniqueId, { status: 'Rejected' });
        this.clearReservation(c);
        return this.reply(uniqueId, { status: 'Accepted' });
      }
      case 'TriggerMessage': {
        const req = ocpp.CsmsToStation.TriggerMessage.req.safeParse(payload);
        if (!req.success) return this.callError(uniqueId, 'TypeConstraintViolation', req.error.issues[0]?.message ?? 'invalid');
        this.reply(uniqueId, { status: 'Accepted' });
        this.trigger(req.data.requestedMessage, req.data.connectorId);
        return;
      }
      default:
        this.callError(uniqueId, 'NotImplemented', `${action} is not implemented`);
    }
  }

  private trigger(message: string, connectorId: number | undefined): void {
    const targets = connectorId ? this.connectors.filter((c) => c.id === connectorId) : this.connectors;
    switch (message) {
      case 'BootNotification':
        this.callVolatile('BootNotification', { chargePointVendor: 'evcharge-lab', chargePointModel: this.spec.kind === 'DC' ? 'SIM-DC' : 'SIM-AC' });
        break;
      case 'Heartbeat':
        this.callVolatile('Heartbeat', {});
        break;
      case 'StatusNotification':
        for (const c of targets) this.callVolatile('StatusNotification', this.statusPayload(c.id, c.status));
        break;
      case 'MeterValues':
        for (const c of targets) if (c.tx) this.sampleMeter(c, 'Trigger');
        break;
    }
  }

  private reserve(req: { connectorId: number; expiryDate: string; idTag: string; reservationId: number }): 'Accepted' | 'Occupied' | 'Rejected' | 'Unavailable' | 'Faulted' {
    const c = this.connectors.find((x) => x.id === req.connectorId);
    if (!c) return 'Rejected';
    if (c.status === 'Unavailable') return 'Unavailable';
    if (c.status === 'Faulted') return 'Faulted';
    if (c.reservation?.reservationId === req.reservationId) {
      this.clearReservation(c, false); // same reservation again: extend
    } else if (c.tx || c.busy || c.status !== 'Available') {
      return 'Occupied';
    }
    const expiresMs = Date.parse(req.expiryDate);
    const timer = this.ctx.wheel.schedule(Math.max(0, this.ctx.clock.realMs(expiresMs - this.ctx.clock.now())), () => {
      if (c.reservation?.reservationId === req.reservationId) this.clearReservation(c);
    });
    c.reservation = { reservationId: req.reservationId, idTag: req.idTag, expiresMs, timer };
    this.setStatus(c, 'Reserved');
    return 'Accepted';
  }

  private clearReservation(c: Connector, notify = true): void {
    c.reservation?.timer.cancel();
    c.reservation = undefined;
    if (notify && c.status === 'Reserved') this.setStatus(c, 'Available');
  }

  // ---------------------------------------------------------------- sessions and physics

  private setStatus(c: Connector, status: Status): void {
    if (c.status === status) return;
    c.status = status;
    this.callVolatile('StatusNotification', this.statusPayload(c.id, status));
  }

  /** A vehicle arrives (demand model). Returns false when the connector cannot take it. */
  plugIn(connectorId: number, idTag: string): boolean {
    const c = this.connectors.find((x) => x.id === connectorId);
    if (!c || !c.idle) return false;
    this.startSession(c, idTag, { remote: false });
    return true;
  }

  /** Probe stations charge continuously (huge battery, 24 h) so drills always see readings. */
  startProbeSession(c: Connector): void {
    if (c.tx || c.busy || c.status !== 'Available') return;
    this.startSession(c, `PROBE${this.spec.index}`.slice(0, 20), { remote: false, probe: true });
  }

  private startSession(c: Connector, idTag: string, o: { remote: boolean; probe?: boolean; profile?: ocpp.ChargingProfile }): void {
    const reservedFor = c.status === 'Reserved' ? c.reservation : undefined;
    if (reservedFor && reservedFor.idTag !== idTag) return;
    c.busy = true;
    this.setStatus(c, 'Preparing');
    const begin = (accepted: boolean) => {
      c.busy = false;
      if (!accepted) {
        this.setStatus(c, reservedFor ? 'Reserved' : 'Available');
        return;
      }
      const t0 = this.ctx.clock.now();
      const { vehicle, soc0 } = o.probe ? probeVehicle(this.spec.kind) : randomVehicle(this.spec.kind, this.ctx.rng);
      const durationMs = o.probe ? 24 * 3_600_000 : sessionDurationMs(this.spec.kind, this.ctx.rng);
      const tx: Tx = {
        idTag,
        startMs: t0,
        plannedEndMs: t0 + durationMs,
        vehicle,
        charge: { tMs: t0, energyWh: c.registerWh, soc: soc0, powerW: 0 },
        meterStartWh: Math.round(c.registerWh),
        stopping: false,
        probe: Boolean(o.probe),
        ...(reservedFor ? { reservationId: reservedFor.reservationId } : {}),
      };
      if (reservedFor) this.clearReservation(c, false);
      c.tx = tx;
      this.ctx.metrics.sessionsStarted.inc();
      this.ctx.metrics.sessions.inc();
      if (o.profile) this.profiles.set(c.id, o.profile, t0, true);
      this.enqueueDurable(
        'StartTransaction',
        { connectorId: c.id, idTag, meterStart: tx.meterStartWh, timestamp: this.ctx.clock.iso(t0), ...(tx.reservationId !== undefined ? { reservationId: tx.reservationId } : {}) },
        {
          onResult: (conf) => {
            const id = Number(conf['transactionId']);
            if (Number.isFinite(id)) tx.transactionId = id;
            const info = conf['idTagInfo'] as { status?: string } | undefined;
            if (info?.status && info.status !== 'Accepted' && c.tx === tx) this.endSession(c, 'DeAuthorized');
          },
        },
      );
      this.updateChargingStatus(c, t0);
      this.scheduleMeter(c);
      this.endTimerFor(c, tx);
    };
    if (o.remote || !this.ready) {
      begin(true); // remote start is pre-authorised; offline stations use their local list (accept)
      return;
    }
    this.callVolatile(
      'Authorize',
      { idTag },
      (conf) => begin((conf['idTagInfo'] as { status?: string } | undefined)?.status === 'Accepted'),
      (code) => begin(code === 'Disconnected' || code === 'Offline'), // authorize unanswered: local acceptance; a CALLERROR rejects
    );
  }

  private endTimerFor(c: Connector, tx: Tx): void {
    c.endTimer?.cancel();
    const delay = Math.max(0, this.ctx.clock.realMs(tx.plannedEndMs - this.ctx.clock.now()));
    c.endTimer = this.ctx.wheel.schedule(delay, () => {
      if (c.tx === tx) this.endSession(c, 'Local');
    });
  }

  private scheduleMeter(c: Connector): void {
    c.meterTimer?.cancel();
    const tx = c.tx;
    if (!tx) return;
    c.meterTimer = this.ctx.wheel.schedule(this.ctx.clock.realMs(this.ctx.meterIntervalS * 1000), () => {
      if (c.tx !== tx) return;
      this.sampleMeter(c, 'Sample.Periodic');
      if (c.tx === tx) this.scheduleMeter(c);
    });
  }

  private limitFor(c: Connector, t: number): number {
    const tx = c.tx;
    return this.profiles.limitW(c.id, t, Boolean(tx), tx?.startMs ?? 0, this.spec.phases);
  }

  private advance(c: Connector, toMs: number): void {
    const tx = c.tx;
    if (!tx) return;
    advanceCharge(tx.charge, toMs, {
      kind: this.spec.kind,
      connectorMaxW: this.spec.connectorMaxW,
      phases: this.spec.phases,
      vehicle: tx.vehicle,
      limitAt: (t) => this.limitFor(c, t),
    });
    c.registerWh = tx.charge.energyWh;
  }

  /** Power this connector draws at simulated time `t` (profile limits applied), without integrating. */
  powerNowW(c: Connector, t: number): number {
    const tx = c.tx;
    if (!tx || tx.charge.soc >= 1) return 0;
    return drawnW(curveW(this.spec.kind, this.spec.connectorMaxW, tx.vehicle, tx.charge.soc), this.limitFor(c, t), minChargeW(this.spec.kind));
  }

  private updateChargingStatus(c: Connector, t: number): void {
    const tx = c.tx;
    if (!tx) return;
    const p = this.powerNowW(c, t);
    this.setStatus(c, p > 0 ? 'Charging' : tx.charge.soc >= 1 ? 'SuspendedEV' : 'SuspendedEVSE');
  }

  /** Integrates to now and emits one MeterValues CALL (queued: it survives disconnects). */
  private sampleMeter(c: Connector, context: 'Sample.Periodic' | 'Trigger'): void {
    const tx = c.tx;
    if (!tx) return;
    const now = this.ctx.clock.now();
    this.advance(c, now);
    const { kind, phases } = this.spec;
    const powerW = tx.charge.powerW;
    const { voltageV, currentA } = sampleElectrics(kind, phases, powerW);
    const ts = this.ctx.clock.iso(now);
    const sampledValue: Obj[] = [
      { value: String(Math.round(tx.charge.energyWh)), context, measurand: 'Energy.Active.Import.Register', unit: 'Wh' },
      { value: String(Math.round(powerW)), context, measurand: 'Power.Active.Import', unit: 'W' },
      { value: currentA.toFixed(2), context, measurand: 'Current.Import', unit: 'A' },
      { value: voltageV.toFixed(1), context, measurand: 'Voltage', unit: 'V' },
    ];
    if (kind === 'DC') sampledValue.push({ value: String(Math.round(tx.charge.soc * 100)), context, measurand: 'SoC', unit: 'Percent' });
    const payload: Obj = { connectorId: c.id, meterValue: [{ timestamp: ts, sampledValue }] };
    this.enqueueDurable('MeterValues', payload, {
      prepare: (p) => {
        if (tx.transactionId !== undefined) p['transactionId'] = tx.transactionId;
      },
    });
    if (this.probe && this.ctx.probe) {
      this.ctx.probe.write({ stationId: this.id, connectorId: c.id, seq: ++c.probeSeq, ts, ...(tx.transactionId !== undefined ? { transactionId: tx.transactionId } : {}), energyWh: Math.round(tx.charge.energyWh), powerW: Math.round(powerW) });
      this.ctx.metrics.probeReadings.inc();
    }
    this.updateChargingStatus(c, now);
    if (tx.charge.soc >= 1 && !tx.stopping) this.endSession(c, 'EVDisconnected');
  }

  endSession(c: Connector, reason: 'Local' | 'Remote' | 'EVDisconnected' | 'DeAuthorized'): void {
    const tx = c.tx;
    if (!tx || tx.stopping) return;
    tx.stopping = true;
    const now = this.ctx.clock.now();
    this.advance(c, now);
    c.meterTimer?.cancel();
    c.endTimer?.cancel();
    c.tx = undefined;
    c.busy = true;
    this.profiles.clearTx(c.id);
    this.ctx.metrics.sessions.dec();
    this.enqueueDurable(
      'StopTransaction',
      { meterStop: Math.round(tx.charge.energyWh), timestamp: this.ctx.clock.iso(now), reason, idTag: tx.idTag },
      {
        prepare: (p) => {
          if (tx.transactionId !== undefined) p['transactionId'] = tx.transactionId;
        },
      },
    );
    this.setStatus(c, 'Finishing');
    this.ctx.wheel.schedule(Math.max(50, this.ctx.clock.realMs(5000)), () => {
      c.busy = false;
      this.setStatus(c, 'Available');
      if (this.probe && this.wantOnline) this.startProbeSession(c);
    });
  }

  /** Sum of the power all connectors draw right now (for the site gauge). */
  drawnNowW(t: number): number {
    let sum = 0;
    for (const c of this.connectors) if (c.tx) sum += this.powerNowW(c, t);
    return sum;
  }
}
