import { createLogger } from '@evcharge/platform';
import { WebSocketServer, type WebSocket as ServerSocket } from 'ws';
import { afterEach, describe, expect, it } from 'vitest';
import { stationSpec } from '../../src/fleet';
import { simMetrics } from '../../src/metrics';
import { Station, type SimContext } from '../../src/station';
import { SimClock } from '../../src/util';
import { TimerWheel } from '../../src/wheel';

type Msg = { conn: number; id: string; action: string; payload: Record<string, unknown> };

const servers: WebSocketServer[] = [];
const stations: Station[] = [];
const wheels: TimerWheel[] = [];
afterEach(() => {
  for (const s of stations.splice(0)) s.stop();
  for (const w of wheels.splice(0)) w.stop();
  for (const s of servers.splice(0)) s.close();
});

const waitFor = async (fn: () => boolean, ms = 8000) => {
  const t0 = Date.now();
  while (!fn()) {
    if (Date.now() - t0 > ms) throw new Error('timeout');
    // oxlint-disable-next-line no-await-in-loop -- polling
    await new Promise((r) => setTimeout(r, 10));
  }
};

/** A scripted CSMS: `onCall` decides per CALL; returning undefined means "do not answer". */
function csms(onCall: (m: Msg, ws: ServerSocket) => Record<string, unknown> | undefined) {
  const wss = new WebSocketServer({ port: 0, handleProtocols: (p) => (p.has('ocpp1.6') ? 'ocpp1.6' : false) });
  servers.push(wss);
  const seen: Msg[] = [];
  let conns = 0;
  wss.on('connection', (ws) => {
    const conn = ++conns;
    ws.on('message', (d) => {
      const f = JSON.parse(d.toString()) as [number, string, string, Record<string, unknown>];
      if (f[0] !== 2) return;
      const m: Msg = { conn, id: f[1], action: f[2], payload: f[3] };
      seen.push(m);
      const res = onCall(m, ws);
      if (res) ws.send(JSON.stringify([3, m.id, res]));
    });
  });
  const addr = wss.address();
  return { seen, url: `ws://127.0.0.1:${typeof addr === 'object' && addr ? addr.port : 0}/ocpp` };
}

function station(url: string, over: Partial<SimContext> = {}): Station {
  const wheel = new TimerWheel(10);
  wheel.start();
  wheels.push(wheel);
  const ctx: SimContext = {
    clock: new SimClock(60), // 30 s sample interval = 0.5 s real; 1 s sample interval = 17 ms
    wheel,
    rng: () => 0, // zero reconnect delay, deterministic vehicle
    metrics: simMetrics(),
    log: createLogger({ service: 'sim-unit', level: 'silent' }),
    csmsUrl: url,
    secretSeed: 'unit-test-seed',
    queueMax: 1000,
    meterIntervalS: 1,
    callTimeoutMs: 5000,
    pickIdTag: () => 'TAG',
    onConnectionChange: () => {},
    ...over,
  };
  const st = new Station(stationSpec(1, 1, 10, 0), ctx);
  stations.push(st);
  return st;
}

const boot = { status: 'Accepted', currentTime: new Date().toISOString(), interval: 300 };

describe('Station OCPP client', () => {
  it('resends an unanswered MeterValues with the SAME uniqueId after reconnecting, then continues in order', async () => {
    let firstMv: string | undefined;
    const { seen, url } = csms((m, ws) => {
      if (m.action === 'BootNotification') return boot;
      if (m.action === 'Authorize') return { idTagInfo: { status: 'Accepted' } };
      if (m.action === 'StartTransaction') return { transactionId: 77, idTagInfo: { status: 'Accepted' } };
      if (m.action === 'MeterValues' && firstMv === undefined) {
        firstMv = m.id; // lose the CALLRESULT: drop the socket without answering
        ws.terminate();
        return undefined;
      }
      return {};
    });
    const st = station(url);
    st.start();
    await waitFor(() => st.online);
    expect(st.plugIn(1, 'TAG')).toBe(true);
    await waitFor(() => seen.filter((m) => m.action === 'MeterValues').length >= 4);
    const mvs = seen.filter((m) => m.action === 'MeterValues');
    expect(mvs[0]?.id).toBe(firstMv);
    expect(mvs[0]?.conn).toBe(1);
    expect(mvs[1]?.id).toBe(firstMv); // resent on connection 2 with the original uniqueId
    expect(mvs[1]?.conn).toBeGreaterThanOrEqual(2);
    const rest = mvs.slice(2).map((m) => m.id); // acknowledged now: later readings are new CALLs with fresh ids
    expect(new Set(rest).size).toBe(rest.length);
    expect(rest).not.toContain(firstMv);
    // the transactionId learned from StartTransaction is filled into the readings
    expect(mvs.at(-1)?.payload['transactionId']).toBe(77);
  });

  it('while offline, charging continues and Start/MeterValues/Stop queue up; after Boot they replay strictly in order', async () => {
    let accepting = false;
    const { seen, url } = csms((m) => {
      if (m.action === 'BootNotification') return boot;
      if (m.action === 'Authorize') return { idTagInfo: { status: 'Accepted' } };
      if (m.action === 'StartTransaction') return { transactionId: 5, idTagInfo: { status: 'Accepted' } };
      return accepting ? {} : undefined;
    });
    const st = station(url);
    st.start();
    await waitFor(() => st.online);
    // lose the network before the session starts, so StartTransaction is queued with an unknown transactionId
    st.goOffline(Date.now() + 600);
    await waitFor(() => !st.online);
    expect(st.plugIn(1, 'TAG')).toBe(true); // offline: local acceptance
    await waitFor(() => st.queue.length >= 5);
    const conn = st.connectors[0];
    st.endSession(conn as NonNullable<typeof conn>, 'Local');
    const queued = st.queue.ids();
    accepting = true;
    await waitFor(() => st.online && st.queue.length === 0, 15_000);
    const replay = seen.filter((m) => queued.includes(m.id));
    const order = replay.map((m) => m.id);
    // queued ids appear in the original order, each at least once, with the original uniqueId
    expect(order.filter((id, i) => order.indexOf(id) === i)).toEqual(queued.filter((id) => order.includes(id)));
    expect(replay[0]?.action).toBe('StartTransaction');
    expect(replay.at(-1)?.action).toBe('StopTransaction');
    expect(replay.at(-1)?.payload['transactionId']).toBe(5); // patched in after the Start was acknowledged
    const meterStart = replay[0]?.payload['meterStart'] as number;
    const meterStop = replay.at(-1)?.payload['meterStop'] as number;
    expect(meterStop).toBeGreaterThanOrEqual(meterStart);
  });

  it('answers CSMS commands: ReserveNow / CancelReservation / SetChargingProfile / TriggerMessage / unknown', async () => {
    const replies = new Map<string, unknown[]>();
    const wss = new WebSocketServer({ port: 0, handleProtocols: (p) => (p.has('ocpp1.6') ? 'ocpp1.6' : false) });
    servers.push(wss);
    wss.on('connection', (ws) => {
      ws.on('message', (d) => {
        const f = JSON.parse(d.toString()) as unknown[];
        if (f[0] === 2 && f[2] === 'BootNotification') ws.send(JSON.stringify([3, f[1], boot]));
        else if (f[0] === 2) ws.send(JSON.stringify([3, f[1], {}]));
        else replies.set(String(f[1]), f);
      });
      const call = (id: string, action: string, payload: unknown) => ws.send(JSON.stringify([2, id, action, payload]));
      setTimeout(() => {
        const exp = new Date(Date.now() + 600_000).toISOString();
        call('r1', 'ReserveNow', { connectorId: 1, expiryDate: exp, idTag: 'T', reservationId: 5 });
        call('r2', 'ReserveNow', { connectorId: 1, expiryDate: exp, idTag: 'U', reservationId: 6 });
        call('r3', 'CancelReservation', { reservationId: 5 });
        call('r4', 'CancelReservation', { reservationId: 99 });
        call('r5', 'SetChargingProfile', { connectorId: 0, csChargingProfiles: { chargingProfileId: 1, stackLevel: 0, chargingProfilePurpose: 'TxDefaultProfile', chargingProfileKind: 'Relative', chargingSchedule: { chargingRateUnit: 'W', chargingSchedulePeriod: [{ startPeriod: 0, limit: 4140 }] } } });
        call('r6', 'SetChargingProfile', { connectorId: 1, csChargingProfiles: { chargingProfileId: 2, stackLevel: 1, chargingProfilePurpose: 'TxProfile', chargingProfileKind: 'Relative', chargingSchedule: { chargingRateUnit: 'W', chargingSchedulePeriod: [{ startPeriod: 0, limit: 1 }] } } });
        call('r7', 'TriggerMessage', { requestedMessage: 'Heartbeat' });
        call('r8', 'DataTransfer', {});
        call('r9', 'ReserveNow', { connectorId: 'x' });
      }, 200);
    });
    const addr = wss.address();
    const st = station(`ws://127.0.0.1:${typeof addr === 'object' && addr ? addr.port : 0}/ocpp`);
    st.start();
    await waitFor(() => replies.size >= 9);
    const status = (id: string) => (replies.get(id)?.[2] as { status?: string } | undefined)?.status;
    expect(status('r1')).toBe('Accepted');
    expect(status('r2')).toBe('Occupied'); // connector 1 is already reserved for another reservation
    expect(status('r3')).toBe('Accepted');
    expect(status('r4')).toBe('Rejected');
    expect(status('r5')).toBe('Accepted');
    expect(status('r6')).toBe('Rejected'); // TxProfile without a running transaction
    expect(status('r7')).toBe('Accepted');
    expect(replies.get('r8')?.[2]).toBe('NotImplemented');
    expect(replies.get('r9')?.[2]).toBe('TypeConstraintViolation');
    expect(st.connectors[0]?.status).toBe('Available'); // reservation cancelled
  });
});
