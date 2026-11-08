/* oxlint-disable typescript/no-explicit-any, no-await-in-loop */ // stub bodies are untyped JSON; requests are sequential on purpose
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { call, INTERNAL_TOKEN, type Recorded, type StubReply, startEnv, signUp, type TestEnv } from './harness';

let env: TestEnv;
let a: Awaited<ReturnType<typeof signUp>>;
let b: Awaited<ReturnType<typeof signUp>>;

const SESSION_ID = randomUUID();
const startedAt = '2026-11-08T10:00:00.000Z';
const invoices: { invoiceId: string; driverId: string }[] = [];

function sessionsStub(r: Recorded): StubReply {
  const driver = r.headers['x-driver-id'];
  if (r.method === 'POST' && r.path === '/internal/drivers') return { status: 201, body: {} };
  if (r.method === 'GET' && r.path === '/v1/sites') return { status: 200, body: { sites: [{ siteId: 's1' }] } };
  if (r.method === 'POST' && r.path === '/v1/reservations') {
    if (r.body.connectorId === 9) return { status: 409, body: { title: 'Conflict', status: 409, code: 'connector_unavailable', detail: 'connector is reserved' } };
    const replayed = r.headers['idempotency-key'] === 'replay-me';
    return { status: 201, body: { reservationId: 7, driverId: driver, status: 'Active' }, headers: { 'x-commit-lsn': '0/1A2B', ...(replayed ? { 'idempotent-replayed': 'true' } : {}) } };
  }
  if (r.method === 'DELETE' && r.path === '/v1/reservations/7') return { status: 200, body: { reservationId: 7, status: 'Cancelled' } };
  if (r.method === 'POST' && r.path === '/v1/sessions/remote-start') return { status: 202, body: { commandId: randomUUID(), status: 'Requested' } };
  if (r.method === 'GET' && r.path === `/v1/drivers/${driver}/sessions`) return { status: 200, body: { sessions: [] } };
  if (r.method === 'GET' && r.path === `/v1/sessions/${SESSION_ID}`) {
    // sessions enforces ownership: only driver A owns this session
    if (driver !== a.driverId) return { status: 404, body: { title: 'Not found', status: 404, code: 'not_found' } };
    return { status: 200, body: { sessionId: SESSION_ID, stationId: 'st-1', connectorId: 2, status: 'Active', startedAt, stoppedAt: null } };
  }
  if (r.method === 'POST' && r.path === `/v1/sessions/${SESSION_ID}/stop`) return { status: 202, body: { sessionId: SESSION_ID, status: 'Stopping' } };
  return { status: 404, body: { title: 'Not found', status: 404 } };
}

function billingStub(r: Recorded): StubReply {
  const driver = r.headers['x-driver-id'] as string;
  if (r.method === 'PUT' && r.path === `/v1/drivers/${driver}/payment-method`) return { status: 204 };
  if (r.method === 'GET' && r.path === '/v1/invoices') {
    if (r.query.get('driverId') !== driver) return { status: 403, body: { title: 'Forbidden', status: 403 } };
    return { status: 200, body: { items: invoices.filter((i) => i.driverId === driver), nextBefore: null } };
  }
  const m = /^\/v1\/invoices\/(.+)$/.exec(r.path);
  if (r.method === 'GET' && m) {
    const inv = invoices.find((i) => i.invoiceId === m[1]);
    return inv && inv.driverId === driver ? { status: 200, body: inv } : { status: 404, body: { title: 'Not Found', status: 404 } };
  }
  return { status: 404 };
}

function telemetryStub(r: Recorded): StubReply {
  if (r.path === '/v1/stations/st-1/connectors/2/latest') return { status: 200, body: { stationId: 'st-1', connectorId: 2, powerW: 7200, at: startedAt, source: 'redis' } };
  if (r.path === '/v1/stations/st-1/connectors/2/power') return { status: 200, body: { stationId: 'st-1', connectorId: 2, points: [{ t: startedAt, avgW: 7000, maxW: 7300 }] } };
  return { status: 404, body: { title: 'Not found', status: 404 } };
}

beforeAll(async () => {
  env = await startEnv({ SITES_CACHE_TTL_MS: '60000' });
  env.sessions.handle = sessionsStub;
  env.billing.handle = billingStub;
  env.telemetry.handle = telemetryStub;
  a = await signUp(env);
  b = await signUp(env);
  invoices.push({ invoiceId: randomUUID(), driverId: a.driverId }, { invoiceId: randomUUID(), driverId: b.driverId });
});
afterAll(async () => {
  await env?.close();
});

const last = (stub: { calls: Recorded[] }) => stub.calls[stub.calls.length - 1] as Recorded;

describe('identity', () => {
  it('requires a valid access token on every /v1 route except auth', async () => {
    for (const [m, p] of [['GET', '/v1/sites?lat=1&lng=1'], ['GET', '/v1/sessions'], ['POST', '/v1/reservations'], ['GET', '/v1/invoices'], ['GET', '/v1/me']] as const) {
      expect((await call(env.url, m, p)).status, `${m} ${p}`).toBe(401);
    }
  });

  it('ignores a spoofed x-driver-id / x-internal-token and forwards only the JWT subject', async () => {
    const res = await call(env.url, 'GET', '/v1/sessions', { token: a.access, headers: { 'x-driver-id': b.driverId, 'x-internal-token': 'evil' } });
    expect(res.status).toBe(200);
    const c = last(env.sessions);
    expect(c.path).toBe(`/v1/drivers/${a.driverId}/sessions`);
    expect(c.headers['x-driver-id']).toBe(a.driverId);
    expect(c.headers['x-internal-token']).toBe(INTERNAL_TOKEN);
  });

  it('does not let spoofed headers authenticate anything without a token', async () => {
    const res = await call(env.url, 'GET', '/v1/sessions', { headers: { 'x-driver-id': a.driverId, 'x-internal-token': INTERNAL_TOKEN } });
    expect(res.status).toBe(401);
  });
});

describe('payment method', () => {
  it('goes to billing for the token owner with the idempotency key', async () => {
    const res = await call(env.url, 'PUT', '/v1/me/payment-method', { token: a.access, body: { token: 'tok_visa_1234' }, headers: { 'idempotency-key': 'pm-1' } });
    expect(res.status).toBe(204);
    const c = last(env.billing);
    expect(c.path).toBe(`/v1/drivers/${a.driverId}/payment-method`);
    expect(c.body).toEqual({ token: 'tok_visa_1234' });
    expect(c.headers['idempotency-key']).toBe('pm-1');
    expect(c.headers['x-driver-id']).toBe(a.driverId);
  });

  it('requires an idempotency key and a valid body', async () => {
    expect((await call(env.url, 'PUT', '/v1/me/payment-method', { token: a.access, body: { token: 'tok_visa_1234' } })).status).toBe(400);
    expect((await call(env.url, 'PUT', '/v1/me/payment-method', { token: a.access, body: { token: 'x' }, headers: { 'idempotency-key': 'pm-2' } })).status).toBe(400);
  });
});

describe('sites', () => {
  it('validates the query, caches briefly and coalesces concurrent misses', async () => {
    expect((await call(env.url, 'GET', '/v1/sites?lat=52.5&lng=13.4&radiusKm=101', { token: a.access })).status).toBe(400);
    expect((await call(env.url, 'GET', '/v1/sites?lat=abc&lng=13.4', { token: a.access })).status).toBe(400);
    const before = env.sessions.calls.filter((c) => c.path === '/v1/sites').length;
    const results = await Promise.all(Array.from({ length: 20 }, () => call(env.url, 'GET', '/v1/sites?lat=52.5201&lng=13.4001&radiusKm=25', { token: a.access })));
    expect(results.every((r) => r.status === 200 && r.body.sites[0].siteId === 's1')).toBe(true);
    // a second driver asking for nearly the same spot shares the cached answer (~110 m grid)
    expect((await call(env.url, 'GET', '/v1/sites?lat=52.5204&lng=13.4003&radiusKm=25', { token: b.access })).status).toBe(200);
    const calls = env.sessions.calls.filter((c) => c.path === '/v1/sites');
    expect(calls.length - before).toBe(1);
    expect(last({ calls }).query.get('radiusKm')).toBe('25');
    expect(last({ calls }).headers['x-driver-id']).toBeUndefined();
    await call(env.url, 'GET', '/v1/sites?lat=48.1&lng=11.5', { token: a.access });
    expect(env.sessions.calls.filter((c) => c.path === '/v1/sites').length - before).toBe(2);
    // radii snap up to 1, 2, 5, 10, 25, 50, 100 km: 3 km and 4.9 km share one cache entry (5 km)
    for (const [asked, sent] of [['0.2', '1'], ['3', '5'], ['4.9', '5'], ['26', '50'], ['100', '100']] as const) {
      await call(env.url, 'GET', `/v1/sites?lat=40&lng=10&radiusKm=${asked}`, { token: a.access });
      expect(last({ calls: env.sessions.calls.filter((c) => c.path === '/v1/sites') }).query.get('radiusKm') ?? '', `radius ${asked}`).toBeTruthy();
      expect(env.sessions.calls.filter((c) => c.path === '/v1/sites' && c.query.get('lat') === '40.000' && c.query.get('radiusKm') === sent).length).toBe(1);
    }
  });
});

describe('reservations', () => {
  it('requires an idempotency key and does not call sessions without one', async () => {
    const n = env.sessions.calls.length;
    const res = await call(env.url, 'POST', '/v1/reservations', { token: a.access, body: { stationId: 'st-1', connectorId: 1 } });
    expect(res.status).toBe(400);
    expect(res.body.code).toBe('idempotency_key_required');
    expect(env.sessions.calls.length).toBe(n);
  });

  it('forwards the key and the driver, relays status, body and the replay marker', async () => {
    const res = await call(env.url, 'POST', '/v1/reservations', { token: a.access, body: { stationId: 'st-1', connectorId: 1 }, headers: { 'idempotency-key': 'replay-me' } });
    expect(res.status).toBe(201);
    expect(res.body).toMatchObject({ reservationId: 7, driverId: a.driverId });
    expect(res.headers.get('idempotent-replayed')).toBe('true');
    const c = last(env.sessions);
    expect(c.headers['idempotency-key']).toBe('replay-me');
    expect(c.headers['x-driver-id']).toBe(a.driverId);
    expect(c.body).toEqual({ stationId: 'st-1', connectorId: 1 });
  });

  it('passes a 409 from sessions through as problem+json with its code', async () => {
    const res = await call(env.url, 'POST', '/v1/reservations', { token: a.access, body: { stationId: 'st-1', connectorId: 9 }, headers: { 'idempotency-key': 'k-409' } });
    expect(res.status).toBe(409);
    expect(res.headers.get('content-type')).toContain('application/problem+json');
    expect(res.body).toMatchObject({ status: 409, code: 'connector_unavailable', detail: 'connector is reserved' });
  });

  it('validates the body', async () => {
    expect((await call(env.url, 'POST', '/v1/reservations', { token: a.access, body: { stationId: 'st-1' }, headers: { 'idempotency-key': 'k' } })).status).toBe(400);
  });

  it('cancels with a key, rejects bad ids', async () => {
    expect((await call(env.url, 'DELETE', '/v1/reservations/7', { token: a.access })).status).toBe(400);
    const res = await call(env.url, 'DELETE', '/v1/reservations/7', { token: a.access, headers: { 'idempotency-key': 'del-1' } });
    expect(res.status).toBe(200);
    expect(last(env.sessions).headers['idempotency-key']).toBe('del-1');
    expect(last(env.sessions).headers['x-driver-id']).toBe(a.driverId);
    expect((await call(env.url, 'DELETE', '/v1/reservations/abc', { token: a.access, headers: { 'idempotency-key': 'del-2' } })).status).toBe(400);
  });
});

describe('sessions', () => {
  it('remote start goes to sessions with the key and the JWT driver', async () => {
    expect((await call(env.url, 'POST', '/v1/sessions', { token: a.access, body: { stationId: 'st-1', connectorId: 2 } })).status).toBe(400);
    const res = await call(env.url, 'POST', '/v1/sessions', { token: a.access, body: { stationId: 'st-1', connectorId: 2 }, headers: { 'idempotency-key': 'rs-1', 'x-driver-id': b.driverId } });
    expect(res.status).toBe(202);
    expect(res.body.status).toBe('Requested');
    const c = last(env.sessions);
    expect(c.path).toBe('/v1/sessions/remote-start');
    expect(c.headers['idempotency-key']).toBe('rs-1');
    expect(c.headers['x-driver-id']).toBe(a.driverId);
  });

  it('stops a session (key required) and reads it; other drivers get sessions\' 404', async () => {
    expect((await call(env.url, 'POST', `/v1/sessions/${SESSION_ID}/stop`, { token: a.access })).status).toBe(400);
    const stop = await call(env.url, 'POST', `/v1/sessions/${SESSION_ID}/stop`, { token: a.access, headers: { 'idempotency-key': 'stop-1' } });
    expect(stop.status).toBe(202);
    expect(last(env.sessions).headers['idempotency-key']).toBe('stop-1');
    expect((await call(env.url, 'GET', `/v1/sessions/${SESSION_ID}`, { token: a.access })).body.sessionId).toBe(SESSION_ID);
    expect((await call(env.url, 'GET', `/v1/sessions/${SESSION_ID}`, { token: b.access })).status).toBe(404);
    expect((await call(env.url, 'GET', '/v1/sessions/not-a-uuid', { token: a.access })).status).toBe(400);
  });

  it('lists the caller\'s sessions with paging parameters', async () => {
    const res = await call(env.url, 'GET', '/v1/sessions?limit=5&before=2026-11-08T12:00:00Z', { token: b.access });
    expect(res.status).toBe(200);
    expect(last(env.sessions).path).toBe(`/v1/drivers/${b.driverId}/sessions`);
    expect(last(env.sessions).query.get('limit')).toBe('5');
    expect((await call(env.url, 'GET', '/v1/sessions?limit=0', { token: b.access })).status).toBe(400);
  });

  it('serves latest power plus the series from telemetry-ingest', async () => {
    const res = await call(env.url, 'GET', `/v1/sessions/${SESSION_ID}/power?step=5m`, { token: a.access });
    expect(res.status).toBe(200);
    expect(res.body.sessionId).toBe(SESSION_ID);
    expect(res.body.latest.powerW).toBe(7200);
    expect(res.body.series.points).toHaveLength(1);
    const c = last(env.telemetry);
    expect(c.query.get('from')).toBe(startedAt);
    expect(c.query.get('step')).toBe('5m');
    expect(c.headers['x-driver-id']).toBeUndefined();
    // someone else's session is a 404 from sessions, so telemetry is never asked
    const n = env.telemetry.calls.length;
    expect((await call(env.url, 'GET', `/v1/sessions/${SESSION_ID}/power`, { token: b.access })).status).toBe(404);
    expect(env.telemetry.calls.length).toBe(n);
  });

  it('returns latest: null when telemetry has no reading yet', async () => {
    const before = env.telemetry.handle;
    env.telemetry.handle = (r) => (r.path.endsWith('/latest') ? { status: 404, body: { title: 'Not found', status: 404 } } : before(r));
    const res = await call(env.url, 'GET', `/v1/sessions/${SESSION_ID}/power`, { token: a.access });
    env.telemetry.handle = before;
    expect(res.status).toBe(200);
    expect(res.body.latest).toBeNull();
  });
});

describe('invoices', () => {
  it('lists only the caller\'s invoices, ignoring a client-supplied driverId', async () => {
    const mine = await call(env.url, 'GET', '/v1/invoices', { token: a.access });
    expect(mine.status).toBe(200);
    expect(mine.body.items.map((i: any) => i.driverId)).toEqual([a.driverId]);
    const spoof = await call(env.url, 'GET', `/v1/invoices?driverId=${b.driverId}`, { token: a.access, headers: { 'x-driver-id': b.driverId } });
    expect(spoof.status).toBe(200);
    expect(spoof.body.items.map((i: any) => i.driverId)).toEqual([a.driverId]);
    expect(last(env.billing).query.get('driverId')).toBe(a.driverId);
    expect(last(env.billing).headers['x-driver-id']).toBe(a.driverId);
  });

  it('shows an own invoice and hides another driver\'s as 404', async () => {
    const own = invoices.find((i) => i.driverId === a.driverId)!;
    const other = invoices.find((i) => i.driverId === b.driverId)!;
    expect((await call(env.url, 'GET', `/v1/invoices/${own.invoiceId}`, { token: a.access })).status).toBe(200);
    expect((await call(env.url, 'GET', `/v1/invoices/${other.invoiceId}`, { token: a.access })).status).toBe(404);
    expect((await call(env.url, 'GET', '/v1/invoices/nope', { token: a.access })).status).toBe(400);
  });
});

describe('downstream failures', () => {
  it('maps 5xx to 502 and a downstream 401 (bad service credentials) to 502, not 401', async () => {
    const before = env.sessions.handle;
    env.sessions.handle = () => ({ status: 500, body: { title: 'boom', status: 500 } });
    const r500 = await call(env.url, 'GET', '/v1/sessions', { token: a.access });
    expect(r500.status).toBe(502);
    expect(r500.body.code).toBe('bad_gateway');
    env.sessions.handle = () => ({ status: 401, body: { title: 'Unauthorized', status: 401 } });
    expect((await call(env.url, 'GET', '/v1/sessions', { token: a.access })).status).toBe(502);
    env.sessions.handle = before;
  });

  it('retries a read once, but never retries a write on timeout (504, one call)', async () => {
    const before = env.sessions.handle;
    let n = 0;
    env.sessions.handle = (r) => (r.path === '/v1/drivers/' + a.driverId + '/sessions' && ++n === 1 ? { status: 500, body: { title: 'boom', status: 500 } } : before(r));
    expect((await call(env.url, 'GET', '/v1/sessions', { token: a.access })).status).toBe(200);
    expect(n).toBe(2);
    env.sessions.handle = before;

    const e = await startEnv({ DOWNSTREAM_TIMEOUT_MS: '150' });
    try {
      const d = await signUp(e);
      e.sessions.handle = () => ({ status: 201, body: {}, delayMs: 600 });
      const res = await call(e.url, 'POST', '/v1/reservations', { token: d.access, body: { stationId: 'st-1', connectorId: 1 }, headers: { 'idempotency-key': 'w-1' } });
      expect(res.status).toBe(504);
      expect(e.sessions.calls.filter((c) => c.path === '/v1/reservations')).toHaveLength(1);
    } finally {
      await e.close();
    }
  });

  it('shows only allow-listed downstream problem codes and hides downstream detail otherwise', async () => {
    const before = env.sessions.handle;
    env.sessions.handle = () => ({ status: 409, body: { title: 'relation "secret_table" violated', status: 409, code: 'pg_internal', detail: 'constraint secret_table_pk on host db-7' } });
    const res = await call(env.url, 'POST', '/v1/reservations', { token: a.access, body: { stationId: 'st-1', connectorId: 1 }, headers: { 'idempotency-key': 'k-leak' } });
    env.sessions.handle = before;
    expect(res.status).toBe(409);
    expect(res.body.code).toBe('downstream_rejected');
    expect(JSON.stringify(res.body)).not.toMatch(/secret_table|db-7|pg_internal/);
  });

  it('maps a slow downstream to 504 and an open circuit to 503', async () => {
    const e = await startEnv({ DOWNSTREAM_TIMEOUT_MS: '150', READ_TIMEOUT_MS: '150', READ_RETRIES: '0' });
    try {
      e.sessions.handle = (r) => (r.path === '/internal/drivers' ? { status: 201, body: {} } : { status: 200, body: {}, delayMs: 600 });
      const d = await signUp(e);
      const slow = await call(e.url, 'GET', '/v1/sessions', { token: d.access });
      expect(slow.status).toBe(504);
      expect(slow.body.code).toBe('downstream_timeout');
      e.sessions.handle = () => ({ status: 500, body: { title: 'boom', status: 500 } });
      for (let i = 0; i < 6; i++) await call(e.url, 'GET', '/v1/sessions', { token: d.access });
      const n = e.sessions.calls.length;
      const open = await call(e.url, 'GET', '/v1/sessions', { token: d.access });
      expect(open.status).toBe(503);
      expect(open.body.code).toBe('downstream_unavailable');
      expect(e.sessions.calls.length).toBe(n); // short-circuited
    } finally {
      await e.close();
    }
  });
});

describe('read-your-writes (ADR 0015)', () => {
  it('forwards the commit LSN of a write as x-min-lsn on that driver only', async () => {
    const c = await signUp(env);
    await call(env.url, 'GET', '/v1/sessions', { token: c.access });
    expect(last(env.sessions).headers['x-min-lsn']).toBeUndefined();
    const res = await call(env.url, 'POST', '/v1/reservations', { token: c.access, headers: { 'idempotency-key': randomUUID() }, body: { stationId: 'st-1', connectorId: 1 } });
    expect(res.status).toBe(201);
    expect(res.headers.get('x-commit-lsn')).toBeNull(); // internal detail, not sent to drivers
    await call(env.url, 'GET', '/v1/sessions', { token: c.access });
    expect(last(env.sessions).headers['x-min-lsn']).toBe('0/1A2B');
    await call(env.url, 'GET', '/v1/sessions', { token: b.access });
    expect(last(env.sessions).headers['x-min-lsn']).toBeUndefined();
  });
});

describe('operations', () => {
  it('serves liveness, readiness (postgres only) and metrics', async () => {
    expect((await call(env.url, 'GET', '/health/live')).status).toBe(200);
    const ready = await call(env.url, 'GET', '/health/ready');
    expect(ready.status).toBe(200);
    expect(Object.keys(ready.body.checks)).toEqual(['postgres']); // redis is not a readiness dependency (the limiter fails open)
    const text = await (await fetch(`${env.url}/metrics`)).text();
    expect(text).toMatch(/http_server_requests_seconds_count\{[^}]*route="\/v1\/sessions"/);
    expect(text).toMatch(/driver_api_token_verify_total\{[^}]*outcome="ok"/);
    expect(text).toMatch(/driver_api_sites_cache_total\{[^}]*result="hit"/);
  });
});
