import { randomUUID } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { closeRedis, createLogger, createProducer, createRedis, makeEnvelope, type KafkaProducer, type Redis } from '@evcharge/platform';
import { loadGatewayConfig, StationGateway, type GatewayTopics } from '@evcharge/station-gateway';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { fleet, ocpp } from '@evcharge/contracts';
import { createApi, loadSimConfig, Simulator } from '../../src';
import { createTopics, INTERNAL_TOKEN, KAFKA_BROKERS, REDIS_URL, StubSessions, TopicCollector, uniq, waitFor } from '../support/infra';

const SEED = 'integration-seed-0123';
const run = uniq('simt');
const topics: GatewayTopics = {
  stationEvents: `${run}.station-events`,
  meterValues: `${run}.meter-values`,
  commands: `${run}.commands`,
  commandResults: `${run}.command-results`,
  rawLog: `${run}.raw-log`,
};

const sessions = new StubSessions();
const collector = new TopicCollector();
const probeDir = mkdtempSync(join(tmpdir(), 'sim-probes-'));
let gateway: StationGateway;
let sim: Simulator;
let flood: Simulator;
let redis: Redis;
let cmdProducer: KafkaProducer;
let dropTopics: () => Promise<void>;
let apiBase = '';
let apiServer: ReturnType<typeof createApi>;

const log = createLogger({ service: 'sim-test', level: 'warn' });
const simEnv = (o: Record<string, string>) =>
  loadSimConfig({
    LOG_LEVEL: 'warn',
    CSMS_URL: gateway.wsUrl,
    STATION_SECRET_SEED: SEED,
    SIM_AUTOSTART: 'false',
    SIM_HTTP_PORT: '0',
    SIM_PROBE_DIR: probeDir,
    ...o,
  });

const meterRecords = (stationId: string) => collector.records.filter((r) => r.topic === topics.meterValues && r.key === stationId);
type MvData = { ocppMessageId: string; replayed: boolean; connectorId: number; transactionId?: number; readings: { timestamp: string; samples: { measurand: string; value: number; unit: string }[] }[] };
const sample = (d: MvData, measurand: string) => d.readings[0]?.samples.find((s) => s.measurand === measurand)?.value;

beforeAll(async () => {
  await sessions.start();
  dropTopics = await createTopics([...Object.values(topics), `${topics.commands}.dlq`], 3);
  await collector.start(Object.values(topics));
  redis = createRedis(REDIS_URL);
  cmdProducer = createProducer({ brokers: KAFKA_BROKERS, clientId: uniq('sim-cmd') });
  gateway = await StationGateway.start(
    loadGatewayConfig({
      SERVICE_NAME: 'station-gateway',
      LOG_LEVEL: 'warn',
      INTERNAL_TOKEN,
      SESSIONS_URL: sessions.url,
      KAFKA_BROKERS: KAFKA_BROKERS.join(','),
      REDIS_URL,
      GATEWAY_INSTANCE_ID: `${run}-gw`,
      GATEWAY_MAX_UPGRADES_PER_S: '1000',
    }),
    { port: 0, host: '127.0.0.1', topics, dispatcherGroupId: `${run}.dispatcher`, dispatcherStartFrom: 'earliest', logger: log, handleSignals: false, drainDelayMs: 0 },
  );
  // 1 site x 4 stations (the 4th is DC with DC share 0.25). Secrets follow the shared convention.
  for (let n = 1; n <= 4; n++) {
    const id = `st-0001-${String(n).padStart(3, '0')}`;
    sessions.addStation(id, { secret: fleet.stationSecret(SEED, id), startW: 4140, safeShareW: 11_000 });
  }
  for (let n = 1; n <= 6; n++) {
    const id = `st-0002-${String(n).padStart(3, '0')}`;
    sessions.addStation(id, { secret: fleet.stationSecret(SEED, id), siteId: 'site-0002', startW: 4140, safeShareW: 11_000 });
  }
  // station st-0001-001 is the probe: continuous sessions, every reading logged
  sim = new Simulator(simEnv({ SIM_SITES: '1', SIM_STATIONS_PER_SITE: '4', SIM_DC_SHARE: '0.25', SIM_METER_INTERVAL_S: '1', SIM_PROBE_STATIONS: '1', SIM_SEED: '3' }), log);
  apiServer = createApi(sim, { shuttingDown: false });
  await new Promise<void>((r) => apiServer.listen(0, '127.0.0.1', () => r()));
  const addr = apiServer.address();
  apiBase = `http://127.0.0.1:${typeof addr === 'object' && addr ? addr.port : 0}`;
  sim.start();
});

afterAll(async () => {
  await sim?.stop();
  await flood?.stop();
  await new Promise<void>((r) => apiServer?.close(() => r()));
  await gateway?.shutdown('test done').catch(() => {});
  const keys = await redis.keys(`route:station:st-000*`);
  if (keys.length) await redis.del(...keys);
  await closeRedis(redis);
  await cmdProducer.close();
  await collector.stop();
  await dropTopics();
  await sessions.stop();
  rmSync(probeDir, { recursive: true, force: true });
});

const api = async (method: string, path: string, body?: unknown) => {
  const res = await fetch(`${apiBase}${path}`, { method, headers: { 'content-type': 'application/json' }, ...(body ? { body: JSON.stringify(body) } : {}) });
  const text = await res.text();
  return { status: res.status, json: res.headers.get('content-type')?.includes('json') ? JSON.parse(text) : text };
};

async function sendCommand(stationId: string, action: string, payload: Record<string, unknown>): Promise<void> {
  await cmdProducer.sendEnvelope(
    topics.commands,
    makeEnvelope('station.command-requested', stationId, { commandId: randomUUID(), stationId, action, payload, expiresAt: new Date(Date.now() + 60_000).toISOString(), requestedBy: 'test' }, 'test'),
  );
}

describe('simulator against the real gateway (Redis + Kafka, stub sessions)', () => {
  it('connects the fleet with Basic auth, boots, and the gateway applies the start default profile', async () => {
    expect((await api('POST', '/fleet/start', { ratePerS: 1000 })).status).toBe(202);
    await waitFor(() => sim.status().connected === 4, { what: '4 stations connected', timeoutMs: 30_000 });
    expect(gateway.connections.size).toBe(4);
    await collector.waitFor((r) => r.topic === topics.stationEvents && r.value['type'] === 'station.booted' && r.key === 'st-0001-004', 'DC station boot');
    const booted = collector.find((r) => r.topic === topics.stationEvents && r.key === 'st-0001-004' && r.value['type'] === 'station.booted');
    expect(booted?.value['data']).toMatchObject({ vendor: 'evcharge-lab', model: 'SIM-DC' });
    // the gateway sent TxDefaultProfile at startW; the station stack now holds it
    await waitFor(() => sim.byId.get('st-0001-001')?.profiles.size === 1, { what: 'default profile applied' });
  });

  it('probe station draws power limited by the TxDefaultProfile, with energy and power consistent; every reading is logged', async () => {
    const probeId = 'st-0001-001';
    expect(sim.probeStations()).toEqual([probeId]);
    await waitFor(() => meterRecords(probeId).length >= 6, { what: 'probe meter values on Kafka', timeoutMs: 30_000 });
    const mvs = meterRecords(probeId).map((r) => r.value['data'] as MvData);
    const powers = mvs.map((d) => sample(d, 'Power.Active.Import') ?? 0);
    // the very first readings can precede the Boot-time profile; afterwards the limit is startW
    expect(Math.max(...powers.slice(2))).toBeLessThanOrEqual(4140);
    expect(powers.filter((p) => p === 4140).length).toBeGreaterThan(0); // limited to startW, not the 11 kW the connector could do
    const c1 = mvs.filter((d) => d.connectorId === 1).map((d) => sample(d, 'Energy.Active.Import.Register') ?? 0);
    for (let i = 1; i < c1.length; i++) expect(c1[i] as number).toBeGreaterThanOrEqual(c1[i - 1] as number);
    expect(mvs[0]?.readings[0]?.samples.map((s) => s.measurand).sort()).toEqual(['Current.Import', 'Energy.Active.Import.Register', 'Power.Active.Import', 'Voltage']);
    expect(sessions.requests.some((r) => r.path === '/internal/ocpp/start-transaction' && (r.body as { stationId: string }).stationId === probeId)).toBe(true);
    const lines = readFileSync(sim.probeLog?.path ?? '', 'utf8').trim().split('\n').map((l) => JSON.parse(l) as { stationId: string; connectorId: number; seq: number });
    expect(lines.length).toBeGreaterThanOrEqual(6);
    for (const cid of [1, 2]) {
      const seqs = lines.filter((l) => l.connectorId === cid).map((l) => l.seq);
      expect(seqs).toEqual(seqs.map((_, i) => i + 1)); // gap-free, 1-based
    }
  });

  it('exposes site power vs cap, connected stations and message rates on /metrics', async () => {
    const m = (await api('GET', '/metrics')).json as string;
    expect(m).toMatch(/sim_connected_stations(\{[^}]*\})? 4/);
    expect(m).toMatch(/sim_site_drawn_watts\{[^}]*site="site-0001"[^}]*\} [1-9]/);
    expect(m).toMatch(/sim_site_cap_watts\{[^}]*site="site-0001"[^}]*\} [1-9]/);
    expect(m).toContain('sim_ocpp_messages_total');
    expect(m).toContain('sim_replay_queue_depth');
    expect(m).toContain('sim_reconnect_attempts_total');
    const status = (await api('GET', '/status')).json as { connected: number; probeStations: number };
    expect(status.connected).toBe(4);
    expect(status.probeStations).toBe(1);
    expect((await api('POST', '/offline', { minutes: -1 })).status).toBe(400);
  });

  it('a TxProfile (stack 1) sent through the command path raises the limit, and the draw follows it', async () => {
    const probeId = 'st-0001-001';
    const validTo = new Date(Date.now() + 120_000).toISOString();
    await sendCommand(probeId, 'SetChargingProfile', {
      connectorId: 1,
      csChargingProfiles: { chargingProfileId: ocpp.txProfileId(1), stackLevel: 1, chargingProfilePurpose: 'TxProfile', chargingProfileKind: 'Relative', validTo, chargingSchedule: { chargingRateUnit: 'W', chargingSchedulePeriod: [{ startPeriod: 0, limit: 9000 }] } },
    });
    await waitFor(() => meterRecords(probeId).some((r) => (r.value['data'] as MvData).connectorId === 1 && sample(r.value['data'] as MvData, 'Power.Active.Import') === 9000), { what: 'power follows the TxProfile', timeoutMs: 30_000 });
    // connector 2 still runs at the default
    const c2 = meterRecords(probeId).map((r) => r.value['data'] as MvData).filter((d) => d.connectorId === 2);
    expect(Math.max(...c2.slice(2).map((d) => sample(d, 'Power.Active.Import') ?? 0))).toBeLessThanOrEqual(4140);
  });

  it('RemoteStartTransaction starts a session on an idle connector (Authorize skipped, StartTransaction queued)', async () => {
    const id = 'st-0001-002';
    const before = sessions.requests.filter((r) => r.path === '/internal/ocpp/start-transaction' && (r.body as { stationId: string }).stationId === id).length;
    await sendCommand(id, 'RemoteStartTransaction', { connectorId: 1, idTag: 'DRIVER-TAG-1' });
    await waitFor(() => sessions.requests.filter((r) => r.path === '/internal/ocpp/start-transaction' && (r.body as { stationId: string; idTag: string }).stationId === id && (r.body as { idTag: string }).idTag === 'DRIVER-TAG-1').length > 0, { what: 'StartTransaction for remote start', timeoutMs: 30_000 });
    expect(before).toBe(0);
    const res = await collector.waitFor((r) => r.topic === topics.commandResults && (r.value['data'] as { stationId: string; action: string }).stationId === id && (r.value['data'] as { action: string }).action === 'RemoteStartTransaction', 'remote start result');
    expect(res.value['data']).toMatchObject({ outcome: 'Accepted' });
    const conn = sim.byId.get(id)?.connectors[0];
    await waitFor(() => conn?.tx?.transactionId !== undefined, { what: 'transactionId assigned' });
    // a second remote start on the same connector is rejected
    await sendCommand(id, 'RemoteStartTransaction', { connectorId: 1, idTag: 'DRIVER-TAG-2' });
    const rej = await collector.waitFor((r) => r.topic === topics.commandResults && (r.value['data'] as { stationId: string; outcome: string; action: string }).stationId === id && (r.value['data'] as { outcome: string }).outcome === 'Rejected', 'rejected remote start');
    expect(rej).toBeDefined();
    // RemoteStop ends it and a StopTransaction reaches sessions with the assigned transactionId
    const txId = conn?.tx?.transactionId;
    await sendCommand(id, 'RemoteStopTransaction', { transactionId: txId });
    await waitFor(() => sessions.requests.some((r) => r.path === '/internal/ocpp/stop-transaction' && (r.body as { stationId: string; transactionId: number }).stationId === id && (r.body as { transactionId: number }).transactionId === txId), { what: 'StopTransaction', timeoutMs: 30_000 });
    const stop = sessions.requests.find((r) => r.path === '/internal/ocpp/stop-transaction' && (r.body as { stationId: string }).stationId === id);
    expect(stop?.body).toMatchObject({ reason: 'Remote' });
    expect(conn?.tx).toBeUndefined();
  });

  it('offline drill: a station loses the network, keeps charging, queues readings, then replays them in order with original uniqueIds; the probe log shows zero gaps', async () => {
    const probeId = 'st-0001-001';
    const st = sim.byId.get(probeId);
    expect(st).toBeDefined();
    const offlineAt = new Date().toISOString();
    const res = await api('POST', '/offline', { ids: [probeId], minutes: 5 / 60 }); // 5 s at scale 1
    expect(res.status).toBe(202);
    expect(res.json.stations).toBe(1);
    await waitFor(() => !st?.online, { what: 'station offline' });
    await waitFor(() => (st?.queue.length ?? 0) >= 4, { what: 'readings queued while offline', timeoutMs: 20_000 });
    const queuedIds = st?.queue.ids() ?? [];
    expect(new Set(queuedIds).size).toBe(queuedIds.length);
    // after the block and the 0-10 s reconnect delay the station is back and the queue drains in order
    await waitFor(() => st?.online, { what: 'station back online', timeoutMs: 45_000 });
    await waitFor(() => st?.queue.length === 0, { what: 'queue replayed', timeoutMs: 45_000 });
    // the replayed CALLs carry their original uniqueIds and reach Kafka in the order they were queued
    const positions = await waitFor(
      () => {
        const kafkaOrder = meterRecords(probeId).map((r) => (r.value['data'] as MvData).ocppMessageId);
        const p = queuedIds.map((id) => kafkaOrder.indexOf(id)).filter((i) => i >= 0);
        return p.length >= 4 ? p : undefined;
      },
      { what: 'queued uniqueIds on Kafka', timeoutMs: 30_000 },
    );
    expect(positions).toEqual([...positions].sort((x, y) => x - y));
    // zero gaps: every probe reading since the drill began reached Kafka (dedup is downstream)
    await waitFor(() => {
      const lines = readFileSync(sim.probeLog?.path ?? '', 'utf8').trim().split('\n').map((l) => JSON.parse(l) as { stationId: string; connectorId: number; seq: number; ts: string });
      const kafkaTs = new Set(meterRecords(probeId).flatMap((r) => (r.value['data'] as MvData & { connectorId: number }).readings.map((x) => `${(r.value['data'] as MvData).connectorId}|${x.timestamp}`)));
      const missing = lines.filter((l) => l.stationId === probeId && l.ts >= offlineAt && !kafkaTs.has(`${l.connectorId}|${l.ts}`));
      return missing.length === 0;
    }, { what: 'no probe reading missing in Kafka', timeoutMs: 30_000 });
    const m = (await api('GET', '/metrics')).json as string;
    expect(m).toMatch(/sim_reconnect_attempts_total\{[^}]*kind="after_drop"[^}]*\} [1-9]/);
  });

  it('storm drill: every station disconnects at once and reconnects under the backoff policy', async () => {
    const res = await api('POST', '/storm', {});
    expect(res.status).toBe(202);
    expect(res.json.disconnected).toBeGreaterThanOrEqual(3);
    await waitFor(() => sim.status().connected < res.json.disconnected, { what: 'stations dropped' });
    await waitFor(() => sim.status().connected === 4, { what: 'fleet reconnected', timeoutMs: 60_000 });
    expect(gateway.connections.size).toBe(4);
  });

  it('flood drill connects stations as fast as possible and reports the job', async () => {
    // 2 sites x 6 stations: the stub knows st-0001-001..004 and st-0002-001..006, so two stations are refused (401)
    flood = new Simulator(simEnv({ SIM_SITES: '2', SIM_STATIONS_PER_SITE: '6', SIM_DC_SHARE: '0', SIM_PROBE_STATIONS: '0' }), log);
    flood.start();
    const job = flood.flood(12, 100);
    expect(job.requested).toBe(12);
    await waitFor(() => job.finishedAt, { what: 'flood finished', timeoutMs: 30_000 });
    // st-0001-005/006 do not exist in the stub (only 4 stations at site 1): 401 -> failed; the 6 known st-0002 stations connect
    expect(job.connected + job.failed).toBe(12);
    expect(job.connected).toBeGreaterThanOrEqual(6);
    await flood.stop();
  });
});
