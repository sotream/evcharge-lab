/* oxlint-disable typescript/no-explicit-any */ // HTTP bodies in tests are untyped JSON
import { randomBytes } from 'node:crypto';
import { createServer, type IncomingMessage } from 'node:http';
import type { AddressInfo } from 'node:net';
import { createLogger } from '@evcharge/platform';
import { Client } from 'pg';
import { createDriverApi, type DriverApp } from '../../src/app';
import { readConfig } from '../../src/config';

export const PG_URL = process.env['TEST_PG_URL'] ?? 'postgres://evcharge:evcharge@localhost:25432';
export const REDIS_URL = process.env['TEST_REDIS_URL'] ?? 'redis://localhost:26379';
export const INTERNAL_TOKEN = 'test-internal-token-0123456789';
export const JWT_SECRET = 'test-jwt-secret-0123456789-0123456789';

export const uniq = (prefix: string) => `${prefix}_${Date.now().toString(36)}_${randomBytes(3).toString('hex')}`;

function dbUrl(name: string): string {
  const u = new URL(PG_URL);
  u.pathname = `/${name}`;
  return u.toString();
}

export async function tempDatabase(): Promise<{ url: string; drop: () => Promise<void> }> {
  const name = uniq('drivers_test').toLowerCase();
  const admin = new Client({ connectionString: dbUrl('postgres') });
  await admin.connect();
  await admin.query(`CREATE DATABASE "${name}"`);
  await admin.end();
  return {
    url: dbUrl(name),
    drop: async () => {
      const a = new Client({ connectionString: dbUrl('postgres') });
      await a.connect();
      await a.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
      await a.end();
    },
  };
}

export interface Recorded {
  method: string;
  path: string;
  query: URLSearchParams;
  headers: IncomingMessage['headers'];
  body: any;
}
export interface StubReply {
  status: number;
  body?: unknown;
  headers?: Record<string, string>;
  /** Answer only after this many ms (timeout tests). */
  delayMs?: number;
}
export interface Stub {
  url: string;
  calls: Recorded[];
  /** Replace to script the stub. It already rejects calls without the internal token. */
  handle: (req: Recorded) => StubReply;
  close(): Promise<void>;
}

/** An HTTP server standing in for sessions, billing or telemetry-ingest. */
export async function startStub(handle: Stub['handle'] = () => ({ status: 404 })): Promise<Stub> {
  const stub: Stub = { url: '', calls: [], handle, close: async () => {} };
  const server = createServer((req, res) => {
    void (async () => {
      const chunks: Buffer[] = [];
      for await (const c of req) chunks.push(c as Buffer);
      const text = Buffer.concat(chunks).toString();
      const u = new URL(req.url ?? '/', 'http://stub');
      const rec: Recorded = { method: req.method ?? 'GET', path: u.pathname, query: u.searchParams, headers: req.headers, body: text ? JSON.parse(text) : undefined };
      stub.calls.push(rec);
      const reply: StubReply = req.headers['x-internal-token'] === INTERNAL_TOKEN ? stub.handle(rec) : { status: 401, body: { title: 'Unauthorized', status: 401 } };
      if (reply.delayMs) await new Promise((r) => setTimeout(r, reply.delayMs));
      const headers: Record<string, string> = { ...reply.headers };
      if (reply.body !== undefined) headers['content-type'] = reply.status >= 400 ? 'application/problem+json' : 'application/json';
      res.writeHead(reply.status, headers).end(reply.body === undefined ? undefined : JSON.stringify(reply.body));
    })();
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  stub.url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  stub.close = () =>
    new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeAllConnections();
    });
  return stub;
}

export interface TestEnv {
  app: DriverApp;
  url: string;
  sessions: Stub;
  billing: Stub;
  telemetry: Stub;
  redisPrefix: string;
  close(): Promise<void>;
}

/** driver-api against a fresh database, real Redis (unique key prefix) and three stub downstreams. */
export async function startEnv(env: Record<string, string> = {}): Promise<TestEnv> {
  const db = await tempDatabase();
  const [sessions, billing, telemetry] = await Promise.all([startStub(), startStub(), startStub()]);
  const redisPrefix = `${uniq('t')}:`;
  const config = readConfig({
    SERVICE_NAME: 'driver-api',
    LOG_LEVEL: 'silent',
    HTTP_PORT: '0',
    INTERNAL_TOKEN,
    DATABASE_URL: db.url,
    REDIS_URL,
    REDIS_KEY_PREFIX: redisPrefix,
    SESSIONS_URL: sessions.url,
    BILLING_URL: billing.url,
    TELEMETRY_URL: telemetry.url,
    JWT_SECRET,
    LOGIN_RATE_LIMIT: '1000',
    READ_RETRIES: '1',
    REGISTER_RATE_LIMIT: '1000',
    SHUTDOWN_GRACE_MS: '3000',
    ...env,
  });
  const app = await createDriverApi({ config, logger: createLogger({ service: 'driver-api-test', level: 'silent' }), port: 0, handleSignals: false });
  return {
    app,
    url: app.url,
    sessions,
    billing,
    telemetry,
    redisPrefix,
    close: async () => {
      await app.shutdown('test');
      await Promise.all([sessions.close(), billing.close(), telemetry.close()]);
      await db.drop();
    },
  };
}

export interface Res {
  status: number;
  headers: Headers;
  body: any;
}

export async function call(base: string, method: string, path: string, opts: { token?: string; body?: unknown; headers?: Record<string, string> } = {}): Promise<Res> {
  const res = await fetch(`${base}${path}`, {
    method,
    headers: { ...(opts.body !== undefined ? { 'content-type': 'application/json' } : {}), ...(opts.token ? { authorization: `Bearer ${opts.token}` } : {}), ...opts.headers },
    ...(opts.body !== undefined ? { body: JSON.stringify(opts.body) } : {}),
  });
  const text = await res.text();
  return { status: res.status, headers: res.headers, body: text ? JSON.parse(text) : undefined };
}

/** Registers (sessions stub must accept /internal/drivers) and logs in; returns the token pair. */
export async function signUp(env: TestEnv, email = `${uniq('d')}@example.test`, password = 'correct-horse-battery') {
  const prev = env.sessions.handle;
  env.sessions.handle = (r) => (r.method === 'POST' && r.path === '/internal/drivers' ? { status: 201, body: {} } : prev(r));
  const reg = await call(env.url, 'POST', '/v1/auth/register', { body: { email, password } });
  env.sessions.handle = prev;
  if (reg.status !== 201) throw new Error(`register failed: ${reg.status} ${JSON.stringify(reg.body)}`);
  const login = await call(env.url, 'POST', '/v1/auth/login', { body: { email, password } });
  return { email, password, driverId: reg.body.driverId as string, idTag: reg.body.idTag as string, access: login.body.accessToken as string, refresh: login.body.refreshToken as string };
}
