/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (ordering, retries, backoff)
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { CircuitOpenError, HttpError, HttpNetworkError, HttpTimeoutError } from '../../src/http-client/errors';
import { createHttpClient } from '../../src/http-client/http-client';
import { registry } from '../../src/metrics/metrics';
import { sample } from '../helpers';

type Handler = (req: IncomingMessage, res: ServerResponse, body: string) => void;
let server: Server;
let base: string;
let handler: Handler;
let calls: { method: string; url: string; headers: IncomingMessage['headers']; body: string }[] = [];

const json = (res: ServerResponse, status: number, body: unknown, ct = 'application/json') => {
  res.writeHead(status, { 'content-type': ct });
  res.end(JSON.stringify(body));
};
/** Fails `n` times with `status`, then answers 200. */
const failThenOk = (n: number, status: number): Handler => {
  let i = 0;
  return (_req, res) => (i++ < n ? json(res, status, { title: 'nope' }, 'application/problem+json') : json(res, 200, { ok: true }));
};

beforeAll(async () => {
  server = createServer((req, res) => {
    let body = '';
    req.on('data', (c: Buffer) => (body += c.toString()));
    req.on('end', () => {
      calls.push({ method: req.method ?? '', url: req.url ?? '', headers: req.headers, body });
      handler(req, res, body);
    });
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
afterAll(() => new Promise<void>((r) => server.close(() => r())));
beforeEach(() => {
  calls = [];
});

const client = (o: Partial<Parameters<typeof createHttpClient>[0]> = {}) =>
  createHttpClient({ name: 'unit-target', baseUrl: base, timeoutMs: 500, retries: 2, backoffBaseMs: 1, backoffMaxMs: 5, ...o });

describe('createHttpClient', () => {
  it('sends JSON, query, internal token and parses JSON', async () => {
    handler = (_req, res, body) => json(res, 201, { echo: JSON.parse(body) });
    const r = await client({ internalToken: 'tok-1234567890abcd' }).post<{ echo: unknown }>('/things', { a: 1 }, { query: { q: 'x', skip: undefined } });
    expect(r).toMatchObject({ status: 201, body: { echo: { a: 1 } } });
    expect(calls[0]).toMatchObject({ method: 'POST', url: '/things?q=x' });
    expect(calls[0]?.headers['x-internal-token']).toBe('tok-1234567890abcd');
    expect(calls[0]?.headers['content-type']).toBe('application/json');
  });

  it('returns undefined body for 204', async () => {
    handler = (_req, res) => {
      res.writeHead(204);
      res.end();
    };
    expect((await client().delete('/x', { idempotencyKey: 'k' })).body).toBeUndefined();
  });

  it('parses problem+json into HttpError', async () => {
    handler = (_req, res) => json(res, 409, { type: 'https://errors/conflict', title: 'Conflict', status: 409, detail: 'already exists', code: 'DUP' }, 'application/problem+json');
    const err = await client().get('/x').catch((e: unknown) => e);
    expect(err).toBeInstanceOf(HttpError);
    const e = err as HttpError;
    expect(e.status).toBe(409);
    expect(e.problem).toMatchObject({ type: 'https://errors/conflict', title: 'Conflict', detail: 'already exists', code: 'DUP' });
    expect(e.request).toMatchObject({ target: 'unit-target', method: 'GET' });
    expect(calls).toHaveLength(1); // 4xx is not retried
  });

  it('wraps non-JSON error bodies', async () => {
    handler = (_req, res) => {
      res.writeHead(400, { 'content-type': 'text/plain' });
      res.end('bad input');
    };
    const e = (await client().get('/x').catch((x: unknown) => x)) as HttpError;
    expect(e.problem).toMatchObject({ type: 'about:blank', status: 400, detail: 'bad input' });
  });

  it('retries GET on 5xx and 429', async () => {
    handler = failThenOk(2, 503);
    expect((await client().get('/x')).status).toBe(200);
    expect(calls).toHaveLength(3);
    calls = [];
    handler = failThenOk(1, 429);
    expect((await client().get('/x')).status).toBe(200);
    expect(calls).toHaveLength(2);
  });

  it('gives up after retries with the last HttpError', async () => {
    handler = failThenOk(10, 500);
    const e = (await client().get('/x').catch((x: unknown) => x)) as HttpError;
    expect(e).toBeInstanceOf(HttpError);
    expect(e.attempts).toBe(3);
    expect(calls).toHaveLength(3);
  });

  it('does not retry POST without idempotency-key, retries with one', async () => {
    handler = failThenOk(1, 503);
    await expect(client().post('/x', {})).rejects.toBeInstanceOf(HttpError);
    expect(calls).toHaveLength(1);
    calls = [];
    handler = failThenOk(1, 503);
    expect((await client().post('/x', { a: 1 }, { idempotencyKey: 'key-1' })).status).toBe(200);
    expect(calls).toHaveLength(2);
    expect(calls.map((c) => c.headers['idempotency-key'])).toEqual(['key-1', 'key-1']);
  });

  it('per-attempt timeout -> HttpTimeoutError after retries', async () => {
    handler = (_req, res) => setTimeout(() => json(res, 200, {}), 300);
    const t0 = Date.now();
    const err = await client({ timeoutMs: 50, retries: 1 }).get('/slow').catch((e: unknown) => e);
    expect(err).toBeInstanceOf(HttpTimeoutError);
    expect(calls.length).toBeLessThanOrEqual(2);
    expect(Date.now() - t0).toBeLessThan(290);
  });

  it('network errors -> HttpNetworkError', async () => {
    const c = createHttpClient({ name: 'dead', baseUrl: 'http://127.0.0.1:1', retries: 1, backoffBaseMs: 1 });
    await expect(c.get('/x')).rejects.toBeInstanceOf(HttpNetworkError);
  });

  it('opens the circuit after consecutive failures and exposes it', async () => {
    handler = failThenOk(100, 500);
    const c = client({ retries: 0, circuitBreaker: { failureThreshold: 3, openMs: 100 } });
    for (let i = 0; i < 3; i++) await expect(c.get('/x')).rejects.toBeInstanceOf(HttpError);
    expect(c.circuit.isOpen).toBe(true);
    const n = calls.length;
    await expect(c.get('/x')).rejects.toBeInstanceOf(CircuitOpenError);
    expect(calls.length).toBe(n); // short-circuited
    await new Promise((r) => setTimeout(r, 110));
    handler = (_req, res) => json(res, 200, { ok: true });
    expect((await c.get('/x')).status).toBe(200);
    expect(c.circuit.state).toBe('closed');
  });

  it('records http_client_requests_seconds by target and outcome', async () => {
    handler = (_req, res) => json(res, 200, {});
    await client({ name: 'metrics-target' }).get('/x');
    const text = await registry.metrics();
    expect(sample(text, 'http_client_requests_seconds_count', { target: 'metrics-target', outcome: 'success' })).toBe(1);
  });
});
