/* oxlint-disable no-await-in-loop, typescript/no-explicit-any */ // helpers poll sequentially; HTTP bodies are untyped JSON
import { createServer, type Server } from 'node:http';
import { randomBytes, randomUUID } from 'node:crypto';
import type { AddressInfo } from 'node:net';
import { events } from '@evcharge/contracts';
import { createConsumer, createLogger, createPgPool, createProducer, makeEnvelope, type KafkaConsumer, type KafkaProducer, type Pool } from '@evcharge/platform';
import { Admin } from '@platformatic/kafka';
import { Client } from 'pg';
import { z } from 'zod';
import { createSessionsApp, type SessionsApp } from '../../src/app';
import { readConfig } from '../../src/config';

export const PG_URL = process.env['TEST_PG_URL'] ?? 'postgres://evcharge:evcharge@localhost:25432';
export const REDIS_URL = process.env['TEST_REDIS_URL'] ?? 'redis://localhost:26379';
export const KAFKA_BROKERS = (process.env['TEST_KAFKA_BROKERS'] ?? 'localhost:29091,localhost:29092,localhost:29093')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
export const TOKEN = 'test-internal-token-0123456789';
export const SEED = 'test-station-seed';

export const uniq = (prefix: string) => `${prefix}_${Date.now().toString(36)}_${randomBytes(3).toString('hex')}`;

function dbUrl(name: string): string {
  const u = new URL(PG_URL);
  u.pathname = `/${name}`;
  return u.toString();
}

export async function tempDatabase(prefix = 'sessions_test'): Promise<{ name: string; url: string; pool: Pool; drop: () => Promise<void> }> {
  const name = uniq(prefix).toLowerCase();
  const admin = new Client({ connectionString: dbUrl('postgres') });
  await admin.connect();
  await admin.query(`CREATE DATABASE "${name}"`);
  await admin.end();
  const url = dbUrl(name);
  const pool = createPgPool(url, { max: 5 });
  return {
    name,
    url,
    pool,
    drop: async () => {
      await pool.end().catch(() => {});
      const a = new Client({ connectionString: dbUrl('postgres') });
      await a.connect();
      await a.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
      await a.end();
    },
  };
}

export async function createTopics(names: string[], partitions = 3): Promise<() => Promise<void>> {
  const admin = new Admin({ clientId: uniq('admin'), bootstrapBrokers: KAFKA_BROKERS });
  try {
    const meta = await admin.metadata({ topics: [] });
    await admin.createTopics({ topics: names, partitions, replicas: Math.min(3, meta.brokers.size) });
  } finally {
    await admin.close();
  }
  return async () => {
    const a = new Admin({ clientId: uniq('admin'), bootstrapBrokers: KAFKA_BROKERS });
    await a.deleteTopics({ topics: names }).catch(() => {});
    await a.close();
  };
}

export async function waitFor<T>(fn: () => T | Promise<T>, { timeoutMs = 15_000, intervalMs = 50 } = {}): Promise<NonNullable<T>> {
  const t0 = Date.now();
  let last: unknown;
  for (;;) {
    try {
      const v = await fn();
      if (v) return v as NonNullable<T>;
    } catch (err) {
      last = err;
    }
    if (Date.now() - t0 > timeoutMs) throw new Error(`waitFor timed out${last ? `: ${String(last)}` : ''}`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

/** A stub of smart-charging POST /v1/allocate whose behaviour a test can switch. */
export interface Allocator {
  url: string;
  mode: 'ok' | 'down' | 'over-cap' | 'hang';
  calls: number;
  /** Body of the last allocate request. */
  lastReq: Record<string, unknown> | undefined;
  close(): Promise<void>;
}

type AllocReq = { siteId: string; siteCapW: number; idleConnectors: number; startW: number; sessions: { sessionId: string; minW: number; maxW: number }[] };

/** Equal-share water-filling stand-in (the real allocator is a separate service); respects reserve and bounds. */
export function stubAllocate(req: AllocReq): { siteId: string; strategy: 'optimal'; totalAllocatedW: number; reserveW: number; allocations: { sessionId: string; limitW: number; suspended: boolean }[] } {
  const reserveW = req.idleConnectors * req.startW;
  const budget = Math.max(0, req.siteCapW - reserveW);
  const share = req.sessions.length ? Math.floor(budget / req.sessions.length) : 0;
  const allocations = req.sessions.map((s) => ({ sessionId: s.sessionId, limitW: Math.min(s.maxW, Math.max(share, 0)), suspended: false }));
  return { siteId: req.siteId, strategy: 'optimal', totalAllocatedW: allocations.reduce((a, b) => a + b.limitW, 0), reserveW, allocations };
}

export async function startAllocator(): Promise<Allocator> {
  const state = { mode: 'ok' as Allocator['mode'], calls: 0, lastReq: undefined as Record<string, unknown> | undefined };
  const server: Server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      state.calls++;
      if (req.headers['x-internal-token'] !== TOKEN) {
        res.writeHead(401).end();
        return;
      }
      if (state.mode === 'down') {
        res.writeHead(503, { 'content-type': 'application/problem+json' }).end('{"title":"down","status":503,"type":"about:blank"}');
        return;
      }
      if (state.mode === 'hang') return; // never answers: the caller's 300 ms timeout fires
      const parsed = JSON.parse(Buffer.concat(chunks).toString()) as AllocReq;
      state.lastReq = parsed as unknown as Record<string, unknown>;
      const body = stubAllocate(parsed);
      if (state.mode === 'over-cap') for (const a of body.allocations) a.limitW = 1_000_000;
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(body));
    });
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const port = (server.address() as AddressInfo).port;
  return {
    url: `http://127.0.0.1:${port}`,
    get mode() {
      return state.mode;
    },
    set mode(v) {
      state.mode = v;
    },
    get calls() {
      return state.calls;
    },
    get lastReq() {
      return state.lastReq;
    },
    close: () => new Promise<void>((r) => server.close(() => r())),
  } as Allocator;
}

export interface Harness {
  app: SessionsApp;
  db: Awaited<ReturnType<typeof tempDatabase>>;
  allocator: Allocator;
  topics: { stationEvents: string; commandResults: string; stationCommands: string; sessionEvents: string };
  producer: KafkaProducer;
  /** Stub gateway: answers every SetChargingProfile with Accepted unless `skip` says to leave it unconfirmed. */
  ack: { enabled: boolean; skip?: (cmd: events.StationCommandRequested) => boolean };
  /** Records seen on session.events.v1 / station.commands.v1 (decoded with the contract schemas). */
  sessionEvents: events.SessionEvent[];
  commands: events.StationCommandRequested[];
  http(method: string, path: string, init?: { headers?: Record<string, string | undefined>; body?: unknown; token?: string | null; driverId?: string | null }): Promise<{ status: number; body: any; headers: Headers }>;
  driver(driverId: string): (method: string, path: string, init?: { body?: unknown; key?: string; headers?: Record<string, string> }) => Promise<{ status: number; body: any; headers: Headers }>;
  registerDriver(idTag?: string): Promise<{ driverId: string; idTag: string }>;
  startTx(stationId: string, connectorId: number, idTag: string, msgId?: string): Promise<{ status: number; body: any }>;
  stopTx(stationId: string, transactionId: number, msgId?: string, meterStopWh?: number): Promise<{ status: number; body: any }>;
  publishStationEvent(type: 'station.connector-status-changed' | 'station.connected' | 'station.disconnected', stationId: string, data: Record<string, unknown>, occurredAt?: string): Promise<void>;
  publishCommandResult(commandId: string, stationId: string, action: string, outcome: events.StationCommandCompleted['data']['outcome'], requestedBy?: string): Promise<void>;
  close(): Promise<void>;
}

export interface HarnessOptions {
  sites?: number;
  stationsPerSite?: number;
  env?: Record<string, string>;
  workers?: boolean;
}

/** Whole service against real Postgres (temp database), Kafka (unique topics), Redis and a stub allocator. */
export async function startHarness(opts: HarnessOptions = {}): Promise<Harness> {
  const db = await tempDatabase();
  const base = uniq('sess').replace(/_/g, '-');
  const topics = { stationEvents: `${base}.station-events`, commandResults: `${base}.command-results`, stationCommands: `${base}.station-commands`, sessionEvents: `${base}.session-events` };
  const names = [topics.stationEvents, topics.commandResults, topics.stationCommands, topics.sessionEvents];
  const dropTopics = await createTopics([...names, `${topics.stationEvents}.dlq`, `${topics.commandResults}.dlq`]);
  const allocator = await startAllocator();
  const config = readConfig({
    SERVICE_NAME: 'sessions',
    LOG_LEVEL: 'warn',
    INTERNAL_TOKEN: TOKEN,
    DATABASE_URL: db.url,
    KAFKA_BROKERS: KAFKA_BROKERS.join(','),
    REDIS_URL,
    SMART_CHARGING_URL: allocator.url,
    STATION_SECRET_SEED: SEED,
    SEED_SITES: String(opts.sites ?? 2),
    SEED_STATIONS_PER_SITE: String(opts.stationsPerSite ?? 3),
    SEED_DC_SHARE: '0.2',
    ALLOCATION_TICK_MS: '100',
    ALLOCATION_PERIOD_S: '3600',
    SWEEPER_INTERVAL_MS: '200',
    HTTP_PORT: '0',
    SHUTDOWN_GRACE_MS: '5000',
    ...opts.env,
  });
  const logger = createLogger({ service: 'sessions-test', level: 'warn' });
  const groups = { stationEvents: uniq('g-st'), commandResults: uniq('g-cr') };
  const app = await createSessionsApp({ config, logger, topics, groups, port: 0, handleSignals: false, workers: opts.workers ?? true });

  const producer = createProducer({ brokers: KAFKA_BROKERS, clientId: uniq('test-producer') });
  const sessionEvents: events.SessionEvent[] = [];
  const commands: events.StationCommandRequested[] = [];
  const ack: Harness['ack'] = { enabled: false };
  let self: Harness | undefined;
  const consumers: KafkaConsumer[] = [
    createConsumer({ brokers: KAFKA_BROKERS, groupId: uniq('tap-se'), topics: [topics.sessionEvents], schema: events.SessionEvent, lagIntervalMs: 0, logger, handler: async (e) => void sessionEvents.push(e) }),
    createConsumer({ brokers: KAFKA_BROKERS, groupId: uniq('tap-cmd'), topics: [topics.stationCommands], schema: events.StationCommandRequested, lagIntervalMs: 0, logger, handler: async (e) => {
        commands.push(e);
        if (ack.enabled && e.data.action === 'SetChargingProfile' && !ack.skip?.(e)) await self?.publishCommandResult(e.data.commandId, e.data.stationId, 'SetChargingProfile', 'Accepted');
      },
    }),
  ];
  for (const c of consumers) await c.start();

  const http: Harness['http'] = async (method, path, init = {}) => {
    const headers: Record<string, string> = { 'content-type': 'application/json' };
    if (init.token !== null) headers['x-internal-token'] = init.token ?? TOKEN;
    if (init.driverId) headers['x-driver-id'] = init.driverId;
    for (const [k, v] of Object.entries(init.headers ?? {})) if (v !== undefined) headers[k] = v;
    const res = await fetch(`${app.url}${path}`, { method, headers, body: init.body === undefined ? undefined : JSON.stringify(init.body) });
    const text = await res.text();
    let body: unknown = text;
    try {
      body = text ? JSON.parse(text) : undefined;
    } catch {
      /* keep text */
    }
    return { status: res.status, body, headers: res.headers };
  };

  const send = async (topic: string, key: string, env: ReturnType<typeof makeEnvelope>) => producer.sendEnvelope(topic, env);
  const h: Harness = {
    app,
    db,
    allocator,
    topics,
    producer,
    ack,
    sessionEvents,
    commands,
    http,
    driver: (driverId) => (method, path, init = {}) => http(method, path, { driverId, body: init.body, headers: { ...(init.key ? { 'idempotency-key': init.key } : {}), ...init.headers } }),
    registerDriver: async (idTag = randomBytes(5).toString('hex')) => {
      const driverId = randomUUID();
      const r = await http('POST', '/internal/drivers', { body: { driverId, idTag } });
      if (r.status !== 201) throw new Error(`register failed ${r.status} ${JSON.stringify(r.body)}`);
      return { driverId, idTag };
    },
    startTx: (stationId, connectorId, idTag, msgId = randomUUID()) =>
      http('POST', '/internal/ocpp/start-transaction', { body: { stationId, ocppMessageId: msgId, connectorId, idTag, meterStartWh: 1000, timestamp: new Date().toISOString() } }),
    stopTx: (stationId, transactionId, msgId = randomUUID(), meterStopWh = 6000) =>
      http('POST', '/internal/ocpp/stop-transaction', { body: { stationId, ocppMessageId: msgId, transactionId, meterStopWh, timestamp: new Date().toISOString() } }),
    publishStationEvent: async (type, stationId, data, occurredAt) => {
      const env = makeEnvelope(type, stationId, { stationId, ...data }, 'test-gateway', occurredAt ? { occurredAt: new Date(occurredAt) } : undefined);
      await send(topics.stationEvents, stationId, env);
    },
    publishCommandResult: async (commandId, stationId, action, outcome, requestedBy = 'sessions') => {
      await send(topics.commandResults, stationId, makeEnvelope('station.command-completed', stationId, { commandId, stationId, action, outcome, requestedBy }, 'test-gateway'));
    },
    close: async () => {
      await Promise.allSettled(consumers.map((c) => c.stop()));
      await app.shutdown('test');
      await producer.close();
      await allocator.close();
      await dropTopics();
      await db.drop();
    },
  };
  self = h;
  return h;
}

export const bodySchemaOk = <S extends z.ZodType>(schema: S, value: unknown): z.infer<S> => schema.parse(value);
