/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (worker loops, ordering)
import { randomBytes } from 'node:crypto';
import { createServer, type IncomingMessage, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { createLogger, createPgPool, type Pool, runMigrations } from '@evcharge/platform';
import { Admin } from '@platformatic/kafka';
import { Client } from 'pg';

export const PG_URL = process.env['TEST_PG_URL'] ?? 'postgres://evcharge:evcharge@localhost:25432';
export const KAFKA_BROKERS = (process.env['TEST_KAFKA_BROKERS'] ?? 'localhost:29091,localhost:29092,localhost:29093')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
export const INTERNAL_TOKEN = 'test-internal-token-0123456789';

export const uniq = (prefix: string) => `${prefix}_${Date.now().toString(36)}_${randomBytes(3).toString('hex')}`;
export const silentLogger = createLogger({ service: 'billing-test', level: 'silent' });

function dbUrl(name: string): string {
  const u = new URL(PG_URL);
  u.pathname = `/${name}`;
  return u.toString();
}

/** Fresh database with the billing migrations applied; `drop()` removes it. */
export async function tempBillingDb(): Promise<{ name: string; url: string; pool: Pool; drop: () => Promise<void> }> {
  const name = uniq('billing_test').toLowerCase();
  const admin = new Client({ connectionString: dbUrl('postgres') });
  await admin.connect();
  await admin.query(`CREATE DATABASE "${name}"`);
  await admin.end();
  const url = dbUrl(name);
  const pool = createPgPool(url, { max: 20 });
  await runMigrations(pool, `${__dirname}/../../migrations`);
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

/** Topics are not auto-created on the test cluster: create them (RF 3) and return a cleanup. */
export async function createTopics(names: string[], partitions = 3): Promise<() => Promise<void>> {
  const admin = new Admin({ clientId: uniq('admin'), bootstrapBrokers: KAFKA_BROKERS });
  try {
    await admin.createTopics({ topics: names, partitions, replicas: 3 });
  } finally {
    await admin.close();
  }
  return async () => {
    const a = new Admin({ clientId: uniq('admin'), bootstrapBrokers: KAFKA_BROKERS });
    await a.deleteTopics({ topics: names }).catch(() => {});
    await a.close();
  };
}

export async function waitFor<T>(fn: () => T | Promise<T>, { timeoutMs = 60_000, intervalMs = 50, what = 'condition' } = {}): Promise<NonNullable<T>> {
  const t0 = Date.now();
  let last: unknown;
  for (;;) {
    try {
      const v = await fn();
      if (v) return v as NonNullable<T>;
    } catch (err) {
      last = err;
    }
    if (Date.now() - t0 > timeoutMs) throw new Error(`waitFor(${what}) timed out${last ? `: ${String(last)}` : ''}`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

export interface StubTariff {
  url: string;
  calls: number;
  down: boolean;
  /** When set, every quote answers with this 4xx status (a permanent rejection). */
  reject?: number;
  close(): Promise<void>;
}

const readBody = async (req: IncomingMessage) => {
  const chunks: Buffer[] = [];
  for await (const c of req) chunks.push(c as Buffer);
  return JSON.parse(Buffer.concat(chunks).toString() || '{}') as { tariffId: string; energyWh: number; startedAt: string; stoppedAt: string };
};

/** Minimal tariff service: 0.40 EUR/kWh (40 minor per kWh) plus a 0.50 EUR session fee. `down` makes it answer 503. */
export async function startStubTariff(): Promise<StubTariff> {
  const stub: StubTariff = { url: '', calls: 0, down: false, close: async () => {} };
  const server: Server = createServer((req, res) => {
    void (async () => {
      if (req.method !== 'POST' || req.url !== '/v1/quote') {
        res.writeHead(404).end();
        return;
      }
      stub.calls++;
      const body = await readBody(req);
      if (req.headers['x-internal-token'] !== INTERNAL_TOKEN) {
        res.writeHead(401, { 'content-type': 'application/problem+json' }).end(JSON.stringify({ title: 'Unauthorized', status: 401 }));
        return;
      }
      if (stub.down) {
        res.writeHead(503, { 'content-type': 'application/problem+json' }).end(JSON.stringify({ title: 'Service Unavailable', status: 503 }));
        return;
      }
      if (stub.reject) {
        res.writeHead(stub.reject, { 'content-type': 'application/problem+json' }).end(JSON.stringify({ title: 'Rejected', status: stub.reject }));
        return;
      }
      const energy = Math.ceil((body.energyWh / 1000) * 40);
      res.writeHead(200, { 'content-type': 'application/json' }).end(
        JSON.stringify({
          tariffId: body.tariffId,
          currency: 'EUR',
          lines: [
            { kind: 'energy', description: 'Energy', quantity: body.energyWh / 1000, unit: 'kWh', amountMinor: energy },
            { kind: 'session', description: 'Session fee', quantity: 1, unit: 'session', amountMinor: 50 },
          ],
          totalMinor: energy + 50,
        }),
      );
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
