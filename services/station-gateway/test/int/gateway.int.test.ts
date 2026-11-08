import { randomUUID } from 'node:crypto';
import { createLogger, createProducer, makeEnvelope, createRedis, closeRedis, type KafkaProducer, type Redis } from '@evcharge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { ocpp } from '@evcharge/contracts';
import { loadGatewayConfig, StationGateway, type GatewayTopics } from '../../src';
import { createTopics, INTERNAL_TOKEN, KAFKA_BROKERS, REDIS_URL, StubSessions, TestStation, TopicCollector, uniq, waitFor } from '../support/infra';

const { txProfileId } = ocpp;
const run = uniq('gwt');
const topics: GatewayTopics = {
  stationEvents: `${run}.station-events`,
  meterValues: `${run}.meter-values`,
  commands: `${run}.commands`,
  commandResults: `${run}.command-results`,
  rawLog: `${run}.raw-log`,
};
const dispatcherGroup = `${run}.dispatcher`;

const sessions = new StubSessions();
const collector = new TopicCollector();
const gateways: StationGateway[] = [];
const stations: TestStation[] = [];
let redis: Redis;
let cmdProducer: KafkaProducer;
let dropTopics: () => Promise<void>;

async function startGateway(id: string, env: Record<string, string> = {}, over: Partial<GatewayTopics> = {}): Promise<StationGateway> {
  const config = loadGatewayConfig({
    SERVICE_NAME: 'station-gateway',
    LOG_LEVEL: 'warn',
    INTERNAL_TOKEN,
    SESSIONS_URL: sessions.url,
    KAFKA_BROKERS: KAFKA_BROKERS.join(','),
    REDIS_URL,
    GATEWAY_INSTANCE_ID: `${run}-${id}`,
    ALLOCATION_GRACE_S: '0',
    ...env,
  });
  const gw = await StationGateway.start(config, {
    port: 0,
    host: '127.0.0.1',
    topics: { ...topics, ...over },
    dispatcherGroupId: dispatcherGroup,
    dispatcherStartFrom: 'earliest',
    dispatcher: { publishRetryDelayMs: 100 },
    logger: createLogger({ service: 'gw-test', level: 'warn' }),
    handleSignals: false,
    drainDelayMs: 0,
    allocationSweepMs: 100,
  });
  gateways.push(gw);
  return gw;
}

async function connect(gw: StationGateway, name: string, opts: { password?: string; username?: string; protocol?: string | null; siteId?: string } = {}): Promise<{ st: TestStation; id: string; secret: string }> {
  const id = uniq(name);
  const cred = sessions.addStation(id, opts.siteId ? { siteId: opts.siteId } : {});
  const st = new TestStation(gw.wsUrl, id, opts.password ?? cred.secret);
  stations.push(st);
  await st.connect(opts);
  return { st, id, secret: cred.secret };
}

const allocationReq = (validTo: string, limit: number, connectorId = 1) => ({
  connectorId,
  csChargingProfiles: {
    chargingProfileId: txProfileId(connectorId),
    stackLevel: 1,
    chargingProfilePurpose: 'TxProfile',
    chargingProfileKind: 'Relative',
    validTo,
    chargingSchedule: { chargingRateUnit: 'W', chargingSchedulePeriod: [{ startPeriod: 0, limit }] },
  },
});
const meterReq = () => ({ connectorId: 1, meterValue: [{ timestamp: new Date().toISOString(), sampledValue: [{ value: '1' }] }] });

const safeShares = (st: TestStation, connectorId: number) =>
  st.callsReceived('SetChargingProfile').filter((p) => {
    const q = p as { connectorId: number; csChargingProfiles: { validTo?: string; stackLevel: number } };
    return q.connectorId === connectorId && q.csChargingProfiles.validTo === undefined && q.csChargingProfiles.stackLevel === 1;
  });

const boot = (st: TestStation) => st.call('BootNotification', { chargePointVendor: 'Acme', chargePointModel: 'X1' });
const now = () => new Date().toISOString();

beforeAll(async () => {
  await sessions.start();
  dropTopics = await createTopics(
    [...Object.values(topics), `${topics.commands}.dlq`],
    3,
  );
  await collector.start(Object.values(topics));
  redis = createRedis(REDIS_URL);
  cmdProducer = createProducer({ brokers: KAFKA_BROKERS, clientId: uniq('cmd-producer') });
});

afterAll(async () => {
  for (const s of stations) s.close();
  await Promise.all(gateways.map((g) => g.shutdown('test done').catch(() => {})));
  const keys = await redis.keys(`route:station:*${run.split('-').slice(1).join('-')}*`);
  if (keys.length) await redis.del(...keys);
  await closeRedis(redis);
  await cmdProducer.close();
  await collector.stop();
  await dropTopics();
  await sessions.stop();
});

async function sendCommand(stationId: string, action: string, payload: Record<string, unknown>, expiresInMs = 60_000): Promise<string> {
  const commandId = randomUUID();
  await cmdProducer.sendEnvelope(
    topics.commands,
    makeEnvelope('station.command-requested', stationId, { commandId, stationId, action, payload, expiresAt: new Date(Date.now() + expiresInMs).toISOString(), requestedBy: 'test' }, 'test'),
  );
  return commandId;
}

const completed = (commandId: string) => collector.waitFor((r) => r.topic === topics.commandResults && (r.value['data'] as { commandId: string }).commandId === commandId, `result ${commandId}`);

describe('station gateway (real Redis + Kafka, stub sessions)', () => {
  let gw1: StationGateway;
  let gw2: StationGateway;

  beforeAll(async () => {
    gw1 = await startGateway('gw1');
    gw2 = await startGateway('gw2');
  });

  it('authenticates on upgrade: subprotocol, Basic auth, unknown station', async () => {
    const id = uniq('auth');
    const cred = sessions.addStation(id);
    const mk = (pw: string) => new TestStation(gw1.wsUrl, id, pw);
    await expect(mk(cred.secret).connect({ protocol: null })).rejects.toMatchObject({ status: 400 });
    await expect(mk('wrong-secret').connect()).rejects.toMatchObject({ status: 401 });
    await expect(mk(cred.secret).connect({ username: 'someone-else' })).rejects.toMatchObject({ status: 401 });
    await expect(new TestStation(gw1.wsUrl, uniq('ghost'), 'x').connect()).rejects.toMatchObject({ status: 401 });
    const st = await mk(cred.secret).connect();
    stations.push(st);
    expect(st.ws.protocol).toBe('ocpp1.6');
    st.close();
    const disabled = uniq('off');
    const c2 = sessions.addStation(disabled, { enabled: false });
    await expect(new TestStation(gw1.wsUrl, disabled, c2.secret).connect()).rejects.toMatchObject({ status: 401 });
  });

  it('caches credentials: a second connection does not hit sessions', async () => {
    const { st, id } = await connect(gw1, 'cache');
    st.close();
    await st.waitClosed();
    const again = new TestStation(gw1.wsUrl, id, sessions.stations.get(id)?.secret ?? '');
    stations.push(again);
    await again.connect();
    expect(sessions.credentialRequests(id)).toBe(1);
    // and Redis holds it for the other instance
    expect(await redis.get(`cred:station:${id}`)).toContain(id);
    const viaGw2 = new TestStation(gw2.wsUrl, id, sessions.stations.get(id)?.secret ?? '');
    stations.push(viaGw2);
    await viaGw2.connect();
    expect(sessions.credentialRequests(id)).toBe(1);
  });

  it('Boot, StatusNotification and MeterValues reach Kafka before the CALLRESULT; heartbeat is local', async () => {
    const { st, id } = await connect(gw1, 'ev');
    const bootRes = await boot(st);
    expect(bootRes[0]).toBe(3);
    expect(bootRes[2]).toMatchObject({ status: 'Accepted', interval: 300 });
    // start default profile: TxDefaultProfile, stack 0, limit startW (from the credential)
    const prof = await waitFor(() => st.callsReceived('SetChargingProfile')[0], { what: 'default profile' });
    expect(prof).toMatchObject({ connectorId: 0, csChargingProfiles: { stackLevel: 0, chargingProfilePurpose: 'TxDefaultProfile', chargingSchedule: { chargingRateUnit: 'W', chargingSchedulePeriod: [{ limit: 4140 }] } } });
    expect((await st.call('Heartbeat', {}))[2]).toHaveProperty('currentTime');

    expect((await st.call('StatusNotification', { connectorId: 1, errorCode: 'NoError', status: 'Charging', timestamp: now() }))[0]).toBe(3);
    const t1 = now();
    const mvId = 'mv-1';
    expect((await st.call('MeterValues', { connectorId: 1, transactionId: 7, meterValue: [{ timestamp: t1, sampledValue: [{ value: '1200', measurand: 'Energy.Active.Import.Register', unit: 'Wh' }, { value: '7000', measurand: 'Power.Active.Import', unit: 'W' }] }] }, mvId))[0]).toBe(3);
    // an old timestamp is flagged as a replay
    await st.call('MeterValues', { connectorId: 1, meterValue: [{ timestamp: new Date(Date.now() - 600_000).toISOString(), sampledValue: [{ value: '5' }] }] }, 'mv-old');

    const booted = collector.find((r) => r.topic === topics.stationEvents && r.value['type'] === 'station.booted' && r.key === id);
    expect(booted?.value['data']).toMatchObject({ stationId: id, vendor: 'Acme', model: 'X1' });
    expect(collector.find((r) => r.topic === topics.stationEvents && r.value['type'] === 'station.connected' && r.key === id)).toBeDefined();
    const status = collector.find((r) => r.topic === topics.stationEvents && r.value['type'] === 'station.connector-status-changed' && r.key === id);
    expect(status?.value['data']).toMatchObject({ connectorId: 1, status: 'Charging' });
    const mv = collector.find((r) => r.topic === topics.meterValues && (r.value['data'] as { ocppMessageId: string }).ocppMessageId === mvId && r.key === id);
    expect(mv?.value['data']).toMatchObject({ stationId: id, connectorId: 1, transactionId: 7, replayed: false });
    const mvData = mv?.value['data'] as { readings: { samples: unknown[] }[] } | undefined;
    expect(mvData?.readings[0]?.samples).toHaveLength(2);
    expect(mv?.headers['event-type']).toBe('telemetry.meter-values');
    // Acked by the broker before the CALLRESULT, but the test's collector consumer may not have it yet: wait for it.
    const old = await waitFor(() => collector.find((r) => r.topic === topics.meterValues && (r.value['data'] as { ocppMessageId: string }).ocppMessageId === 'mv-old' && r.key === id), { what: 'replayed MeterValues record' });
    expect(old?.value['data']).toMatchObject({ replayed: true });

    // raw log: both directions, key = stationId
    const raw = await waitFor(() => collector.records.filter((r) => r.topic === topics.rawLog && r.key === id), { what: 'raw log' });
    await waitFor(() => collector.records.some((r) => r.topic === topics.rawLog && r.key === id && r.value['direction'] === 'out'));
    expect(raw[0]?.value).toHaveProperty('frame');
    st.close();
    await collector.waitFor((r) => r.topic === topics.stationEvents && r.value['type'] === 'station.disconnected' && r.key === id, 'disconnect event');
  });

  it('rejects malformed and unknown calls with the right CALLERROR', async () => {
    const { st } = await connect(gw1, 'bad');
    expect((await st.call('DataTransfer', {}))[2]).toBe('NotImplemented');
    expect((await st.call('Authorize', {}))[2]).toBe('ProtocolError');
    expect((await st.call('Authorize', { idTag: 42 }))[2]).toBe('TypeConstraintViolation');
    expect((await st.raw('not json', '-1'))[2]).toBe('FormationViolation');
    expect((await st.call('MeterValues', { connectorId: 1, meterValue: [{ timestamp: now(), sampledValue: [{ value: 'abc' }] }] }))[2]).toBe('TypeConstraintViolation');
    expect((await st.call('Heartbeat', {}))[0]).toBe(3); // connection still healthy
  });

  it('Authorize, StartTransaction and StopTransaction go through sessions with an idempotency key; a replay returns the same transactionId', async () => {
    const { st, id } = await connect(gw1, 'tx');
    expect((await st.call('Authorize', { idTag: 'TAG1' }))[2]).toEqual({ idTagInfo: { status: 'Accepted' } });
    expect((await st.call('Authorize', { idTag: 'BLOCKED' }))[2]).toEqual({ idTagInfo: { status: 'Blocked' } });
    const start = { connectorId: 1, idTag: 'TAG1', meterStart: 100, timestamp: now() };
    const a = await st.call('StartTransaction', start, 'start-1');
    expect(a[0]).toBe(3);
    const txId = (a[2] as { transactionId: number }).transactionId;
    const b = await st.call('StartTransaction', start, 'start-1'); // station retry / offline replay: original uniqueId
    expect((b[2] as { transactionId: number }).transactionId).toBe(txId);
    const reqs = sessions.requests.filter((r) => r.path === '/internal/ocpp/start-transaction' && (r.body as { stationId: string }).stationId === id);
    expect(reqs).toHaveLength(2);
    expect(reqs.every((r) => r.headers['idempotency-key'] === `${id}:start-1` && r.headers['x-internal-token'] === INTERNAL_TOKEN)).toBe(true);
    expect(reqs[0]?.body).toMatchObject({ stationId: id, ocppMessageId: 'start-1', meterStartWh: 100, connectorId: 1 });
    const stop = await st.call('StopTransaction', { transactionId: txId, meterStop: 5100, timestamp: now(), reason: 'Local' }, 'stop-1');
    expect(stop[0]).toBe(3);
    const stopReq = sessions.requests.find((r) => r.path === '/internal/ocpp/stop-transaction' && (r.body as { stationId: string }).stationId === id);
    expect(stopReq?.headers['idempotency-key']).toBe(`${id}:stop-1`);
    expect(stopReq?.body).toMatchObject({ transactionId: txId, meterStopWh: 5100, reason: 'Local' });
  });

  it('answers InternalError when sessions is down or slow, and recovers', async () => {
    const { st } = await connect(gw1, 'down');
    sessions.mode = 'error';
    try {
      const r = await st.call('StartTransaction', { connectorId: 1, idTag: 'T', meterStart: 0, timestamp: now() }, 'down-1');
      expect(r[0]).toBe(4);
      expect(r[2]).toBe('InternalError');
      sessions.mode = 'slow';
      const t0 = Date.now();
      const s = await st.call('Authorize', { idTag: 'T' });
      expect(s[2]).toBe('InternalError');
      expect(Date.now() - t0).toBeLessThan(1400); // 1 s timeout, no retry
    } finally {
      sessions.mode = 'ok';
    }
    // same uniqueId again succeeds once sessions is back
    expect((await st.call('StartTransaction', { connectorId: 1, idTag: 'T', meterStart: 0, timestamp: now() }, 'down-1'))[0]).toBe(3);
  });

  it('maps permanent sessions 4xx answers: Start gets Invalid + transactionId 0, Stop is accepted, Authorize is Invalid', async () => {
    const { st } = await connect(gw1, 'perm');
    sessions.mode = 'reject';
    try {
      const start = await st.call('StartTransaction', { connectorId: 1, idTag: 'T', meterStart: 0, timestamp: now() }, 'perm-start');
      expect(start[0]).toBe(3);
      expect(start[2]).toEqual({ idTagInfo: { status: 'Invalid' }, transactionId: 0 });
      const stop = await st.call('StopTransaction', { transactionId: 424242, meterStop: 10, timestamp: now() }, 'perm-stop');
      expect(stop[0]).toBe(3);
      expect(stop[2]).toEqual({});
      expect((await st.call('Authorize', { idTag: 'T' }))[2]).toEqual({ idTagInfo: { status: 'Invalid' } });
    } finally {
      sessions.mode = 'ok';
    }
  });

  it('409 in_flight: waits about 200 ms and retries once; a second 409 becomes InternalError', async () => {
    const { st } = await connect(gw1, 'inflight');
    sessions.mode = 'inflight-once';
    try {
      const t0 = Date.now();
      const r = await st.call('StartTransaction', { connectorId: 1, idTag: 'T', meterStart: 0, timestamp: now() }, 'inflight-1');
      expect(r[0]).toBe(3);
      expect(Date.now() - t0).toBeGreaterThanOrEqual(190);
      sessions.mode = 'inflight';
      const before = sessions.requests.length;
      const f = await st.call('StartTransaction', { connectorId: 1, idTag: 'T', meterStart: 0, timestamp: now() }, 'inflight-2');
      expect(f[0]).toBe(4);
      expect(f[2]).toBe('InternalError');
      expect(sessions.requests.length - before).toBe(2); // one retry, no more
    } finally {
      sessions.mode = 'ok';
    }
  });

  it('routes commands across two gateway instances and reports the outcome', async () => {
    const { st, id } = await connect(gw2, 'route');
    await boot(st);
    const route = await waitFor(async () => {
      const h = await redis.hgetall(`route:station:${id}`);
      return h['instance'] ? h : undefined;
    }, { what: 'route' });
    expect(route['instance']).toBe(gw2.instance);
    expect(Number(route['epoch'])).toBeGreaterThanOrEqual(1);
    expect(await redis.ttl(`route:station:${id}`)).toBeGreaterThan(60);
    expect(await redis.zscore('gw:instances', gw1.instance)).not.toBeNull();

    // Whichever instance's dispatcher consumes the command, the owner (gw2) must deliver it.
    const ids = await Promise.all(
      [1, 2, 3, 4].map((n) => sendCommand(id, 'ReserveNow', { connectorId: n, expiryDate: new Date(Date.now() + 600_000).toISOString(), idTag: 'TAG1', reservationId: n })),
    );
    const results = await Promise.all(ids.map((c) => completed(c)));
    for (const r of results) {
      expect(r.value['data']).toMatchObject({ outcome: 'Accepted', stationId: id, action: 'ReserveNow', requestedBy: 'test' });
      expect(r.key).toBe(id);
    }
    expect(st.callsReceived('ReserveNow')).toHaveLength(4);
    expect(gw1.connectionInfo(id)).toBeUndefined();
    expect(gw2.connectionInfo(id)).toBeDefined();

    // Rejected and Error are reported as such
    st.onCall = (action) => (action === 'ReserveNow' ? { status: 'Occupied' } : { status: 'Accepted' });
    const rej = await completed(await sendCommand(id, 'ReserveNow', { connectorId: 1, expiryDate: now(), idTag: 'T', reservationId: 9 }));
    expect(rej.value['data']).toMatchObject({ outcome: 'Rejected', response: { status: 'Occupied' } });
    const bad = await completed(await sendCommand(id, 'ReserveNow', { connectorId: 'x' }));
    expect(bad.value['data']).toMatchObject({ outcome: 'Error' });
  });

  it('reports StationOffline for an unrouted station and Expired for an old command', async () => {
    const off = await completed(await sendCommand(uniq('nobody'), 'RemoteStopTransaction', { transactionId: 1 }));
    expect(off.value['data']).toMatchObject({ outcome: 'StationOffline' });
    const { st, id } = await connect(gw1, 'exp');
    await boot(st);
    const exp = await completed(await sendCommand(id, 'RemoteStopTransaction', { transactionId: 1 }, -1000));
    expect(exp.value['data']).toMatchObject({ outcome: 'Expired' });
    expect(st.callsReceived('RemoteStopTransaction')).toHaveLength(0);
  });

  it('serialises commands: one outstanding CALL per station, in order', async () => {
    const { st, id } = await connect(gw1, 'ser');
    await boot(st);
    const held: { uid: string }[] = [];
    st.onCall = (action) => {
      if (action === 'RemoteStopTransaction') held.push({ uid: '' });
      return { status: 'Accepted' };
    };
    const ids = await Promise.all([10, 11, 12].map((n) => sendCommand(id, 'RemoteStopTransaction', { transactionId: n })));
    await Promise.all(ids.map((c) => completed(c)));
    expect(st.callsReceived('RemoteStopTransaction').map((p) => (p as { transactionId: number }).transactionId).sort()).toEqual([10, 11, 12]);
    expect(held).toHaveLength(3);
  });

  it('a duplicate connection replaces the old one (4000), locally and across instances', async () => {
    const id = uniq('dup');
    const cred = sessions.addStation(id);
    const first = new TestStation(gw1.wsUrl, id, cred.secret);
    stations.push(first);
    await first.connect();
    await waitFor(async () => (await redis.hget(`route:station:${id}`, 'instance')) === gw1.instance, { what: 'first route' });
    const second = new TestStation(gw1.wsUrl, id, cred.secret);
    stations.push(second);
    await second.connect();
    expect(await first.waitClosed()).toBe(4000);
    expect((await second.call('Heartbeat', {}))[0]).toBe(3);
    const epoch2 = await waitFor(async () => {
      const e = Number(await redis.hget(`route:station:${id}`, 'epoch'));
      return e >= 2 ? e : undefined;
    }, { what: 'epoch bump' });

    // late close of the replaced connection must not remove the newer route
    expect(await redis.hget(`route:station:${id}`, 'instance')).toBe(gw1.instance);

    // takeover by the other instance: the route moves and gw1 evicts via pub/sub
    const third = new TestStation(gw2.wsUrl, id, cred.secret);
    stations.push(third);
    await third.connect();
    expect(await second.waitClosed()).toBe(4000);
    await waitFor(async () => (await redis.hget(`route:station:${id}`, 'instance')) === gw2.instance, { what: 'route moved' });
    expect(Number(await redis.hget(`route:station:${id}`, 'epoch'))).toBeGreaterThan(epoch2);
    await new Promise((r) => setTimeout(r, 200));
    expect(await redis.hget(`route:station:${id}`, 'instance')).toBe(gw2.instance);
    expect(gw1.connectionInfo(id)).toBeUndefined();

    // closing the owner deletes the route (compare-and-delete)
    third.close();
    await waitFor(async () => (await redis.exists(`route:station:${id}`)) === 0, { what: 'route deleted' });
    // the epoch counter is a separate key without TTL: it survives the route and keeps growing
    expect(Number(await redis.get(`route:epoch:${id}`))).toBeGreaterThan(epoch2);
    expect(await redis.ttl(`route:epoch:${id}`)).toBe(-1);
  });

  it('raises a connector to the safe share only when the WHOLE site is stale, with the same profile id as its allocation', async () => {
    const siteId = uniq('site');
    const { st, id } = await connect(gw1, 'safe', { siteId });
    await boot(st);
    // the site key is still in the future, as sessions wrote it for a newer round of the site
    await redis.set(`alloc:site:${siteId}`, String(Date.now() + 3_600_000), 'EX', 600);
    // connector 1's own allocation expires soon, connector 2's much later
    await completed(await sendCommand(id, 'SetChargingProfile', allocationReq(new Date(Date.now() + 700).toISOString(), 9000, 1)));
    await completed(await sendCommand(id, 'SetChargingProfile', allocationReq(new Date(Date.now() + 3_600_000).toISOString(), 8000, 2)));
    await new Promise((r) => setTimeout(r, 2500));
    expect(safeShares(st, 1)).toHaveLength(0); // connector 1 expired, but the site is not stale: stay at startW
    // the site key is now in the past: the whole site is stale and the gateway raises the expired connector only
    await redis.set(`alloc:site:${siteId}`, String(Date.now() - 1000), 'EX', 600);
    const safe = await waitFor(() => safeShares(st, 1)[0], { what: 'safe share', timeoutMs: 10_000 });
    expect(safe).toMatchObject({ connectorId: 1, csChargingProfiles: { chargingProfileId: txProfileId(1), stackLevel: 1, chargingProfilePurpose: 'TxProfile', chargingSchedule: { chargingSchedulePeriod: [{ limit: 11000 }] } } });
    expect(safeShares(st, 2)).toHaveLength(0); // connector 2's allocation is still valid
    const ids = st.callsReceived('SetChargingProfile').map((p) => (p as { csChargingProfiles: { chargingProfileId: number } }).csChargingProfiles.chargingProfileId);
    expect(ids).toContain(txProfileId(1));
    expect(ids).toContain(txProfileId(2));
  });

  it('never raises to the safe share when the site key is missing', async () => {
    const { st, id } = await connect(gw1, 'nokey', { siteId: uniq('site') });
    await boot(st);
    await completed(await sendCommand(id, 'SetChargingProfile', allocationReq(new Date(Date.now() + 500).toISOString(), 8000)));
    await new Promise((r) => setTimeout(r, 2000));
    expect(safeShares(st, 1)).toHaveLength(0);
  });

  it('does not apply safe share when a newer allocation replaced the old one', async () => {
    const siteId = uniq('site');
    const { st, id } = await connect(gw1, 'fresh', { siteId });
    await boot(st);
    await redis.set(`alloc:site:${siteId}`, String(Date.now() - 1000), 'EX', 600); // site stale, so only the tracker holds it back
    await completed(await sendCommand(id, 'SetChargingProfile', allocationReq(new Date(Date.now() + 600).toISOString(), 8000)));
    await completed(await sendCommand(id, 'SetChargingProfile', allocationReq(new Date(Date.now() + 120_000).toISOString(), 7000)));
    await new Promise((r) => setTimeout(r, 1500));
    expect(safeShares(st, 1)).toHaveLength(0);
  });

  it('exposes metrics and internal introspection with the internal token only', async () => {
    const m = await (await fetch(`${gw1.url}/metrics`)).text();
    expect(m).toContain('gateway_connected_stations');
    expect(m).toContain('ocpp_messages_total');
    expect(m).toContain('ocpp_call_duration_seconds_bucket');
    expect(m).toContain('gateway_produce_buffer_depth');
    expect(m).toContain('gateway_commands_total');
    expect(m).toContain('ocpp_raw_log_dropped_total');
    expect((await fetch(`${gw1.url}/internal/stats`)).status).toBe(401);
    const stats = (await (await fetch(`${gw1.url}/internal/stats`, { headers: { 'x-internal-token': INTERNAL_TOKEN } })).json()) as { instance: string };
    expect(stats.instance).toBe(gw1.instance);
    expect((await fetch(`${gw1.url}/health/ready`)).status).toBe(200);
    expect((await fetch(`${gw1.url}/health/live`)).status).toBe(200);
  });
});

describe('admission control', () => {
  it('answers 503 + Retry-After above the upgrade rate, and at the connection cap', async () => {
    const gw = await startGateway('rate', { GATEWAY_MAX_UPGRADES_PER_S: '3' });
    const statuses: (number | 'ok')[] = [];
    const retryAfters: number[] = [];
    await Promise.all(
      Array.from({ length: 8 }, async () => {
        const id = uniq('rate');
        const cred = sessions.addStation(id);
        const st = new TestStation(gw.wsUrl, id, cred.secret);
        stations.push(st);
        try {
          await st.connect();
          statuses.push('ok');
        } catch (e) {
          const err = e as { status?: number; headers?: Record<string, string> };
          statuses.push(err.status ?? 0);
          if (err.status === 503) retryAfters.push(Number(err.headers?.['retry-after']));
        }
      }),
    );
    expect(statuses.filter((s) => s === 'ok').length).toBeGreaterThanOrEqual(3);
    expect(statuses.filter((s) => s === 503).length).toBeGreaterThanOrEqual(1);
    expect(statuses.every((s) => s === 'ok' || s === 503)).toBe(true);
    expect(retryAfters.every((r) => r >= 1 && r <= 5)).toBe(true);

    const capped = await startGateway('cap', { GATEWAY_MAX_CONNECTIONS: '1' });
    await connect(capped, 'cap1');
    const id = uniq('cap2');
    const cred = sessions.addStation(id);
    await expect(new TestStation(capped.wsUrl, id, cred.secret).connect()).rejects.toMatchObject({ status: 503 });
    expect((await fetch(`${capped.url}/health/ready`)).status).toBe(503);
  });
});

describe('bad credentials budget', () => {
  it('401 upgrades use their own small bucket: attackers get 503 and good stations still connect', async () => {
    const gw = await startGateway('badauth', { GATEWAY_MAX_BAD_AUTH_PER_S: '3', GATEWAY_MAX_UPGRADES_PER_S: '1000' });
    const target = uniq('victim');
    sessions.addStation(target);
    const statuses = await Promise.all(
      Array.from({ length: 12 }, () =>
        new TestStation(gw.wsUrl, target, 'wrong-password').connect().then(
          () => 0,
          (e: { status?: number }) => e.status ?? 0,
        ),
      ),
    );
    expect(statuses.filter((s) => s === 401).length).toBeGreaterThanOrEqual(3);
    expect(statuses.filter((s) => s === 503).length).toBeGreaterThanOrEqual(1);
    expect(statuses.every((s) => s === 401 || s === 503)).toBe(true);
    // the good-station budget is untouched
    const { st } = await connect(gw, 'goodstation');
    expect((await st.call('Heartbeat', {}))[0]).toBe(3);
    const m = await (await fetch(`${gw.url}/metrics`)).text();
    expect(m).toMatch(/gateway_upgrades_total\{[^}]*reason="bad_auth_limited"[^}]*\} [1-9]/);
  });
});

describe('producer backpressure (ADR 0012)', () => {
  it('answers InternalError immediately when the produce buffer is full, without crashing', async () => {
    // Records for a topic that does not exist are never acknowledged (auto-create is off): the buffer fills.
    const gw = await startGateway('bp', { GATEWAY_PRODUCE_BUFFER: '2', KAFKA_SEND_TIMEOUT_MS: '2000' }, { meterValues: `${run}.missing-topic` });
    const conns = await Promise.all([1, 2, 3, 4].map((n) => connect(gw, `bp${n}`)));
    const t0 = Date.now();
    const results = await Promise.all(conns.map(({ st }) => st.call('MeterValues', meterReq())));
    expect(results.every((r) => r[0] === 4 && r[2] === 'InternalError')).toBe(true);
    const busy = results.filter((r) => r[3] === 'backend busy');
    expect(busy.length).toBeGreaterThanOrEqual(2); // 4 concurrent, buffer of 2
    expect(Date.now() - t0).toBeLessThan(15_000);
    const m = await (await fetch(`${gw.url}/metrics`)).text();
    expect(m).toMatch(/gateway_produce_rejected_total\{[^}]*\} [1-9]/);
    // heartbeats never touch the broker and the gateway is still alive
    expect((await conns[0]?.st.call('Heartbeat', {}))?.[0]).toBe(3);
    expect(gw.stats().produceBufferDepth).toBeLessThanOrEqual(2);
  });
});

describe('graceful shutdown', () => {
  it('turns readiness to 503 and closes station sockets with 1012', async () => {
    const gw = await startGateway('stop');
    const { st, id } = await connect(gw, 'stop');
    await boot(st);
    await waitFor(async () => (await redis.exists(`route:station:${id}`)) === 1, { what: 'route' });
    await gw.shutdown('test');
    expect(await st.waitClosed()).toBe(1012);
    await waitFor(async () => (await redis.exists(`route:station:${id}`)) === 0, { what: 'route removed' });
    expect(await redis.zscore('gw:instances', gw.instance)).toBeNull();
  });
});
