/* oxlint-disable no-await-in-loop */
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Admin, Consumer, deserializersFrom, stringDeserializer, type MessagesStream } from '@platformatic/kafka';

export const REDIS_URL = process.env['TEST_REDIS_URL'] ?? 'redis://localhost:26379';
export const KAFKA_BROKERS = (process.env['TEST_KAFKA_BROKERS'] ?? 'localhost:29091,localhost:29092,localhost:29093')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
export const INTERNAL_TOKEN = 'integration-internal-token';

export const uniq = (prefix: string) => `${prefix}-${Date.now().toString(36)}-${randomBytes(3).toString('hex')}`;

export async function waitFor<T>(fn: () => T | Promise<T>, { timeoutMs = 20_000, intervalMs = 25, what = 'condition' } = {}): Promise<NonNullable<T>> {
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

/** Creates topics (RF 3) and returns a cleanup that deletes them. Auto-create is off on the cluster. */
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

export interface Record_ {
  topic: string;
  key: string | undefined;
  value: Record<string, unknown>;
  headers: Record<string, string>;
}

/** Collects every record of the given topics from the beginning. */
export class TopicCollector {
  readonly records: Record_[] = [];
  private consumer: Consumer<string, string, string, string> | undefined;
  private stream: MessagesStream<string, string, string, string> | undefined;

  async start(topics: string[]): Promise<this> {
    this.consumer = new Consumer({ clientId: uniq('collect'), groupId: uniq('collect'), bootstrapBrokers: KAFKA_BROKERS, deserializers: deserializersFrom(stringDeserializer) });
    this.stream = await this.consumer.consume({ topics, mode: 'earliest', autocommit: false, maxWaitTime: 200 });
    this.stream.on('data', (m) => {
      this.records.push({ topic: m.topic, key: m.key, value: JSON.parse(m.value ?? 'null') as Record<string, unknown>, headers: Object.fromEntries([...m.headers].map(([k, v]) => [String(k), String(v)])) });
    });
    return this;
  }

  find(pred: (r: Record_) => boolean): Record_ | undefined {
    return this.records.find(pred);
  }

  waitFor(pred: (r: Record_) => boolean, what = 'record', timeoutMs = 30_000): Promise<Record_> {
    return waitFor(() => this.find(pred), { what, timeoutMs });
  }

  async stop(): Promise<void> {
    await this.stream?.close().catch(() => {});
    await this.consumer?.close().catch(() => {});
  }
}

function sendJson(res: ServerResponse, status: number, payload: unknown): void {
  res.writeHead(status, { 'content-type': status >= 400 ? 'application/problem+json' : 'application/json' });
  res.end(JSON.stringify(payload));
}

export interface StubStation {
  secret: string;
  siteId: string;
  startW: number;
  safeShareW: number;
  enabled: boolean;
}

/** A stand-in for `sessions`' internal OCPP API (SessionsInternal in contracts). */
export class StubSessions {
  readonly stations = new Map<string, StubStation>();
  readonly requests: { method: string; path: string; headers: IncomingMessage['headers']; body: unknown }[] = [];
  mode: 'ok' | 'error' | 'slow' = 'ok';
  private nextTx = 1000;
  private readonly byKey = new Map<string, unknown>();
  private server: Server | undefined;
  url = '';

  addStation(stationId: string, s: Partial<StubStation> = {}): StubStation {
    const st = { secret: randomBytes(16).toString('hex'), siteId: 'site-0001', startW: 4140, safeShareW: 11000, enabled: true, ...s };
    this.stations.set(stationId, st);
    return st;
  }

  credentialRequests(stationId: string): number {
    return this.requests.filter((r) => r.path === `/internal/stations/${stationId}/credential`).length;
  }

  async start(): Promise<this> {
    this.server = createServer((req, res) => {
      let raw = '';
      req.on('data', (c: Buffer) => (raw += c.toString()));
      req.on('end', () => {
        const body = raw ? (JSON.parse(raw) as Record<string, unknown>) : undefined;
        const path = req.url ?? '';
        this.requests.push({ method: req.method ?? '', path, headers: req.headers, body });
        if (req.headers['x-internal-token'] !== INTERNAL_TOKEN) return sendJson(res, 401, { type: 'about:blank', title: 'Unauthorized', status: 401 });
        const respond = () => {
          if (this.mode === 'error') return sendJson(res, 503, { type: 'about:blank', title: 'down', status: 503 });
          const cred = /^\/internal\/stations\/([^/]+)\/credential$/.exec(path);
          if (cred?.[1]) {
            const id = decodeURIComponent(cred[1]);
            const st = this.stations.get(id);
            if (!st) return sendJson(res, 404, { type: 'about:blank', title: 'Not Found', status: 404 });
            return sendJson(res, 200, {
              stationId: id,
              siteId: st.siteId,
              secretSha256: createHash('sha256').update(st.secret).digest('hex'),
              enabled: st.enabled,
              chargingDefaults: { startW: st.startW, safeShareW: st.safeShareW },
            });
          }
          const b = body as Record<string, unknown>;
          if (path === '/internal/ocpp/authorize') return sendJson(res, 200, { idTagInfo: { status: b['idTag'] === 'BLOCKED' ? 'Blocked' : 'Accepted' } });
          const key = String(req.headers['idempotency-key'] ?? '');
          if (path === '/internal/ocpp/start-transaction') {
            const prev = this.byKey.get(key);
            if (prev) return sendJson(res, 200, prev);
            const r = { transactionId: this.nextTx++, sessionId: randomUUID(), idTagInfo: { status: 'Accepted' } };
            this.byKey.set(key, r);
            return sendJson(res, 200, r);
          }
          if (path === '/internal/ocpp/stop-transaction') return sendJson(res, 200, { sessionId: randomUUID(), idTagInfo: { status: 'Accepted' } });
          return sendJson(res, 404, { type: 'about:blank', title: 'Not Found', status: 404 });
        };
        if (this.mode === 'slow') setTimeout(respond, 1500);
        else respond();
      });
    });
    await new Promise<void>((r) => this.server?.listen(0, '127.0.0.1', () => r()));
    this.url = `http://127.0.0.1:${(this.server.address() as AddressInfo).port}`;
    return this;
  }

  async stop(): Promise<void> {
    this.server?.closeAllConnections();
    await new Promise<void>((r) => this.server?.close(() => r()));
  }
}
