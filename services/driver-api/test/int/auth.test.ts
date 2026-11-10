/* oxlint-disable no-await-in-loop */ // sequential requests are the point of these tests
import { createHash } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { JwtService } from '../../src/jwt';
import { call, JWT_SECRET, signUp, startEnv, type TestEnv, uniq } from './harness';

const PREVIOUS = 'previous-jwt-secret-0123456789-0123456789';
const accept = (r: { path: string }) => (r.path === '/internal/drivers' ? { status: 201, body: {} } : { status: 404 });
let env: TestEnv;
let limited: TestEnv;

beforeAll(async () => {
  env = await startEnv({ JWT_PREVIOUS_SECRETS: PREVIOUS });
  limited = await startEnv({ LOGIN_RATE_LIMIT: '3', LOGIN_RATE_WINDOW_S: '3600', REGISTER_RATE_LIMIT: '3', REGISTER_RATE_WINDOW_S: '3600', TRUST_PROXY: '1' });
  env.sessions.handle = accept;
  limited.sessions.handle = accept;
});
afterAll(async () => {
  await Promise.all([env?.close(), limited?.close()]);
});

const xffEmail = () => `${uniq('xff')}@example.test`;
const login = (e: TestEnv, email: string, password: string, headers: Record<string, string> = {}) => call(e.url, 'POST', '/v1/auth/login', { body: { email, password }, headers });
/** Moves rotations out of the 5 s grace window, so a replay counts as reuse. */
const leaveGrace = (e: TestEnv) => e.app.pool.query("UPDATE refresh_tokens SET rotated_at = rotated_at - interval '10 seconds' WHERE rotated_at IS NOT NULL");
const refresh = (refreshToken: string) => call(env.url, 'POST', '/v1/auth/refresh', { body: { refreshToken } });

describe('register', () => {
  it('creates the account, a 20 char idTag and registers it in sessions with an idempotency key', async () => {
    const email = `${uniq('reg')}@Example.test`;
    const res = await call(env.url, 'POST', '/v1/auth/register', { body: { email, password: 'correct-horse-battery' } });
    expect(res.status).toBe(201);
    expect(res.body.idTag).toHaveLength(20);
    expect(res.body.email).toBe(email.toLowerCase());
    const call1 = env.sessions.calls.findLast((c) => c.path === '/internal/drivers');
    expect(call1?.body).toEqual({ driverId: res.body.driverId, idTag: res.body.idTag });
    expect(call1?.headers['idempotency-key']).toBe(res.body.driverId);
    const { rows } = await env.app.pool.query('SELECT password_hash, id_tag FROM drivers WHERE driver_id = $1', [res.body.driverId]);
    expect(rows[0].password_hash).toMatch(/^scrypt\$16384\$8\$1\$/);
    expect(rows[0].id_tag).toBe(res.body.idTag);
  });

  it('rejects a duplicate email (case-insensitively) and weak input', async () => {
    const email = `${uniq('dup')}@example.test`;
    expect((await call(env.url, 'POST', '/v1/auth/register', { body: { email, password: 'correct-horse-battery' } })).status).toBe(201);
    const dup = await call(env.url, 'POST', '/v1/auth/register', { body: { email: email.toUpperCase(), password: 'another-password' } });
    expect(dup.status).toBe(409);
    expect(dup.body.code).toBe('email_taken');
    expect((await call(env.url, 'POST', '/v1/auth/register', { body: { email: 'not-an-email', password: 'correct-horse-battery' } })).status).toBe(400);
    expect((await call(env.url, 'POST', '/v1/auth/register', { body: { email: `${uniq('w')}@example.test`, password: 'short' } })).status).toBe(400);
  });

  it('leaves no orphan account when sessions fails, and a retry succeeds', async () => {
    const email = `${uniq('orph')}@example.test`;
    const before = env.sessions.handle;
    env.sessions.handle = () => ({ status: 503, body: { title: 'down', status: 503 } });
    const failed = await call(env.url, 'POST', '/v1/auth/register', { body: { email, password: 'correct-horse-battery' } });
    env.sessions.handle = before;
    expect(failed.status).toBe(502);
    expect(env.sessions.calls.filter((c) => c.headers['idempotency-key'] && c.path === '/internal/drivers').length).toBeGreaterThanOrEqual(2); // one attempt plus one retry
    expect((await env.app.pool.query('SELECT 1 FROM drivers WHERE email = $1', [email])).rowCount).toBe(0);
    expect((await call(env.url, 'POST', '/v1/auth/register', { body: { email, password: 'correct-horse-battery' } })).status).toBe(201);
  });

  it('reports 503 when sessions is unreachable', async () => {
    const e = await startEnv();
    try {
      await e.sessions.close();
      const res = await call(e.url, 'POST', '/v1/auth/register', { body: { email: `${uniq('u')}@example.test`, password: 'correct-horse-battery' } });
      expect(res.status).toBe(503);
      expect(res.body.code).toBe('downstream_unavailable');
      expect((await e.app.pool.query('SELECT 1 FROM drivers')).rowCount).toBe(0);
    } finally {
      await e.close();
    }
  });
});

describe('login and tokens', () => {
  it('issues an access JWT and an opaque refresh token stored only as SHA-256', async () => {
    const d = await signUp(env);
    const res = await login(env, d.email.toUpperCase(), d.password);
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ tokenType: 'Bearer', expiresIn: 900, driverId: d.driverId });
    const [h, p] = (res.body.accessToken as string).split('.') as [string, string];
    expect(JSON.parse(Buffer.from(h, 'base64url').toString())).toMatchObject({ alg: 'HS256', kid: expect.any(String) });
    expect(JSON.parse(Buffer.from(p, 'base64url').toString())).toMatchObject({ sub: d.driverId, iss: 'evcharge', aud: 'driver-api' });
    expect(Buffer.from(res.body.refreshToken, 'base64url')).toHaveLength(32);
    const hash = createHash('sha256').update(res.body.refreshToken).digest();
    const { rows } = await env.app.pool.query('SELECT expires_at, created_at FROM refresh_tokens WHERE token_hash = $1', [hash]);
    expect(rows).toHaveLength(1);
    expect((rows[0].expires_at.getTime() - rows[0].created_at.getTime()) / 86_400_000).toBeCloseTo(30, 0);
    const me = await call(env.url, 'GET', '/v1/me', { token: res.body.accessToken });
    expect(me.body).toMatchObject({ driverId: d.driverId, email: d.email, idTag: d.idTag });
  });

  it('answers wrong password and unknown email identically', async () => {
    const d = await signUp(env);
    const wrong = await login(env, d.email, 'wrong-password');
    const unknown = await login(env, `${uniq('nobody')}@example.test`, 'wrong-password');
    expect(wrong.status).toBe(401);
    expect(unknown.status).toBe(401);
    expect(wrong.body).toEqual(unknown.body);
  });

  it('exposes auth outcome metrics', async () => {
    const text = await (await fetch(`${env.url}/metrics`)).text();
    expect(text).toMatch(/driver_api_auth_total\{[^}]*op="login"[^}]*outcome="ok"/);
    expect(text).toMatch(/driver_api_auth_total\{[^}]*op="login"[^}]*outcome="invalid_credentials"/);
  });
});

describe('refresh rotation', () => {
  it('rotates on every use and revokes the whole family when a rotated token is replayed', async () => {
    const d = await signUp(env);
    const r1 = d.refresh;
    const second = await refresh(r1);
    expect(second.status).toBe(200);
    const r2 = second.body.refreshToken as string;
    expect(r2).not.toBe(r1);
    expect(await call(env.url, 'GET', '/v1/me', { token: second.body.accessToken })).toMatchObject({ status: 200 });
    const third = await refresh(r2);
    expect(third.status).toBe(200);
    const r3 = third.body.refreshToken as string;

    // Replaying r1 (rotated long ago) is theft: everything in the family dies, including the live r3.
    await leaveGrace(env);
    const replay = await refresh(r1);
    expect(replay.status).toBe(401);
    expect((await refresh(r3)).status).toBe(401);
    const { rows } = await env.app.pool.query('SELECT count(*)::int AS live FROM refresh_tokens WHERE driver_id = $1 AND revoked_at IS NULL', [d.driverId]);
    expect(rows[0].live).toBe(0);
    expect(await (await fetch(`${env.url}/metrics`)).text()).toMatch(/outcome="reuse_detected"/);
  });

  it('does not touch other families of the same driver', async () => {
    const d = await signUp(env);
    const other = await login(env, d.email, d.password);
    expect((await refresh(d.refresh)).status).toBe(200);
    await leaveGrace(env);
    expect((await refresh(d.refresh)).status).toBe(401); // reuse in family A
    expect((await refresh(other.body.refreshToken)).status).toBe(200); // family B unaffected
  });

  it('rejects unknown, malformed and expired refresh tokens', async () => {
    const d = await signUp(env);
    expect((await refresh('x'.repeat(43))).status).toBe(401);
    expect((await refresh('short')).status).toBe(400);
    await env.app.pool.query("UPDATE refresh_tokens SET expires_at = now() - interval '1 second' WHERE driver_id = $1", [d.driverId]);
    const res = await refresh(d.refresh);
    expect(res.status).toBe(401);
    expect(res.body.code).toBe('refresh_token_expired');
  });

  it('treats an immediate retry of the previous token as a client retry, not theft (5 s grace)', async () => {
    const d = await signUp(env);
    const first = await refresh(d.refresh);
    const retry = await refresh(d.refresh); // the response was "lost": same old token again
    expect(first.status).toBe(200);
    expect(retry.status).toBe(200);
    expect(retry.body.refreshToken).not.toBe(first.body.refreshToken);
    expect((await refresh(first.body.refreshToken)).status).toBe(200); // the family is intact
    expect(await (await fetch(`${env.url}/metrics`)).text()).toMatch(/outcome="grace_retry"/);
    await leaveGrace(env);
    expect((await refresh(d.refresh)).status).toBe(401); // after the grace window it is reuse
    expect((await refresh(retry.body.refreshToken)).status).toBe(401); // and the family is revoked
  });

  it('lets two concurrent uses of one token both succeed inside the grace window', async () => {
    const d = await signUp(env);
    const results = await Promise.all([refresh(d.refresh), refresh(d.refresh)]);
    expect(results.map((r) => r.status)).toEqual([200, 200]);
    expect(results[0]?.body.refreshToken).not.toBe(results[1]?.body.refreshToken);
  });
});

describe('logout', () => {
  it('revokes the family; logout is idempotent and ignores unknown tokens', async () => {
    const d = await signUp(env);
    expect((await call(env.url, 'POST', '/v1/auth/logout', { body: { refreshToken: d.refresh } })).status).toBe(204);
    expect((await refresh(d.refresh)).status).toBe(401);
    expect((await call(env.url, 'POST', '/v1/auth/logout', { body: { refreshToken: d.refresh } })).status).toBe(204);
    expect((await call(env.url, 'POST', '/v1/auth/logout', { body: { refreshToken: 'y'.repeat(43) } })).status).toBe(204);
  });
});

describe('access token verification', () => {
  it('rejects missing, malformed, expired, wrongly signed and wrongly scoped tokens', async () => {
    const d = await signUp(env);
    const me = (token?: string) => call(env.url, 'GET', '/v1/me', token ? { token } : {});
    expect((await me()).status).toBe(401);
    expect((await me('garbage')).status).toBe(401);
    const expired = new JwtService(JWT_SECRET, [], 900, () => Date.now() - 3_600_000).issue(d.driverId);
    const expiredRes = await me(expired);
    expect(expiredRes.status).toBe(401);
    expect(expiredRes.body.code).toBe('token_expired');
    expect(expiredRes.headers.get('www-authenticate')).toBe('Bearer');
    expect((await me(new JwtService('another-secret-0123456789-0123456789-01').issue(d.driverId))).status).toBe(401);
    const [h, p, s] = d.access.split('.') as [string, string, string];
    const forgedPayload = Buffer.from(JSON.stringify({ ...JSON.parse(Buffer.from(p, 'base64url').toString()), sub: '22222222-2222-4222-8222-222222222222' })).toString('base64url');
    expect((await me(`${h}.${forgedPayload}.${s}`)).status).toBe(401);
    expect((await me(`${Buffer.from('{"alg":"none"}').toString('base64url')}.${p}.`)).status).toBe(401);
    expect((await me(d.access)).status).toBe(200);
  });

  it('accepts tokens signed with a previous secret (key rotation)', async () => {
    const d = await signUp(env);
    expect((await call(env.url, 'GET', '/v1/me', { token: new JwtService(PREVIOUS).issue(d.driverId) })).status).toBe(200);
  });
});

describe('login rate limit', () => {
  it('limits per email across IPs with 429 and retry-after', async () => {
    const email = `${uniq('rl')}@example.test`;
    for (let i = 0; i < 3; i++) expect((await login(limited, email, 'nope', { 'x-forwarded-for': `10.0.0.${i + 1}` })).status).toBe(401);
    const blocked = await login(limited, email, 'nope', { 'x-forwarded-for': '10.0.0.99' });
    expect(blocked.status).toBe(429);
    expect(Number(blocked.headers.get('retry-after'))).toBeGreaterThan(0);
    expect(blocked.body.code).toBe('rate_limited');
    // another email from a fresh IP is unaffected
    expect((await login(limited, `${uniq('rl2')}@example.test`, 'nope', { 'x-forwarded-for': '10.0.1.1' })).status).toBe(401);
  });

  it('limits per IP across emails', async () => {
    const ip = { 'x-forwarded-for': '10.9.9.9' };
    for (let i = 0; i < 3; i++) expect((await login(limited, `${uniq('ip')}@example.test`, 'nope', ip)).status).toBe(401);
    expect((await login(limited, `${uniq('ip')}@example.test`, 'nope', ip)).status).toBe(429);
  });
});

describe('register', () => {
  it('does not hold database connections while sessions is slow', async () => {
    const e = await startEnv({ PG_POOL_MAX: '3', REGISTER_SESSIONS_TIMEOUT_MS: '400' });
    try {
      const d = await signUp(e);
      e.sessions.handle = () => ({ status: 201, body: {}, delayMs: 1500 });
      const slow = Array.from({ length: 12 }, () => call(e.url, 'POST', '/v1/auth/register', { body: { email: `${uniq('slow')}@example.test`, password: 'correct-horse-battery' } }));
      await new Promise((r) => setTimeout(r, 200));
      const t0 = Date.now();
      expect((await call(e.url, 'GET', '/v1/me', { token: d.access })).status).toBe(200); // needs a pool connection
      expect(Date.now() - t0).toBeLessThan(500);
      const results = await Promise.all(slow);
      expect(results.every((r) => r.status === 504 || r.status === 503)).toBe(true); // timeouts, then the breaker opens
      expect((await e.app.pool.query('SELECT 1 FROM drivers')).rowCount).toBe(1);
    } finally {
      await e.close();
    }
  });

  it('is rate limited per IP', async () => {
    const ip = { 'x-forwarded-for': '10.7.7.7' };
    const reg = () => call(limited.url, 'POST', '/v1/auth/register', { body: { email: `${uniq('rr')}@example.test`, password: 'correct-horse-battery' }, headers: ip });
    for (let i = 0; i < 3; i++) expect((await reg()).status).toBe(201);
    const blocked = await reg();
    expect(blocked.status).toBe(429);
    expect(Number(blocked.headers.get('retry-after'))).toBeGreaterThan(0);
  });
});

describe('trusted proxy hops', () => {
  it('with one hop, spoofed leading x-forwarded-for entries cannot steer the limit', async () => {
    // The proxy appends the real peer (10.5.5.5); the client controls everything before it.
    for (let i = 0; i < 3; i++) expect((await login(limited, xffEmail(), 'nope', { 'x-forwarded-for': `203.0.113.${i}, 10.5.5.5` })).status).toBe(401);
    expect((await login(limited, xffEmail(), 'nope', { 'x-forwarded-for': '198.51.100.77, 10.5.5.5' })).status).toBe(429);
  });

  it('with no trusted hop (default) x-forwarded-for is ignored entirely', async () => {
    const e = await startEnv({ LOGIN_RATE_LIMIT: '2', LOGIN_RATE_WINDOW_S: '3600' });
    try {
      const statuses: number[] = [];
      for (let i = 0; i < 3; i++) statuses.push((await login(e, `${uniq('x')}@example.test`, 'nope', { 'x-forwarded-for': `1.2.3.${i}` })).status);
      expect(statuses).toEqual([401, 401, 429]); // all three share the socket address
    } finally {
      await e.close();
    }
  });
});

const del = (token: string, key: string | null = 'del-1') => call(env.url, 'DELETE', '/v1/me', { token, headers: key ? { 'idempotency-key': key } : {} });
describe('account deletion (DELETE /v1/me)', () => {
  it('needs an idempotency key, deletes through sessions and billing, then login fails; repeating gives 204', async () => {
    const u = await signUp(env);
    expect((await del(u.access, null)).status).toBe(400);
    env.sessions.handle = (r) => (r.method === 'DELETE' ? { status: 204 } : accept(r));
    env.billing.handle = (r) => (r.method === 'DELETE' ? { status: 204 } : { status: 404 });
    expect((await del(u.access)).status).toBe(204);
    expect(env.sessions.calls.findLast((c) => c.method === 'DELETE')?.path).toBe(`/internal/drivers/${u.driverId}`);
    expect(env.billing.calls.findLast((c) => c.method === 'DELETE')?.path).toBe(`/internal/drivers/${u.driverId}/payment-data`);
    expect((await login(env, u.email, u.password)).status).toBe(401);
    expect((await env.app.pool.query('SELECT 1 FROM refresh_tokens WHERE driver_id = $1', [u.driverId])).rowCount).toBe(0);
    expect((await del(u.access)).status).toBe(204);
  });
  it('is refused with 409 active_session while the driver charges', async () => {
    const u = await signUp(env);
    env.sessions.handle = (r) => (r.method === 'DELETE' ? { status: 409, body: { type: 'about:blank', title: 'Active session', status: 409, code: 'active_session' } } : accept(r));
    const res = await del(u.access);
    expect(res.status).toBe(409);
    expect(res.body.code).toBe('active_session');
    expect((await login(env, u.email, u.password)).status).toBe(200);
  });

  it('rejects writes that would recreate data while the access token is still valid', async () => {
    const u = await signUp(env);
    env.sessions.handle = (r) => (r.method === 'DELETE' ? { status: 204 } : accept(r));
    env.billing.handle = (r) => (r.method === 'DELETE' ? { status: 204 } : { status: 204 });
    expect((await del(u.access)).status).toBe(204);
    const before = env.billing.calls.length;
    const put = await call(env.url, 'PUT', '/v1/me/payment-method', { token: u.access, headers: { 'idempotency-key': 'pm-1' }, body: { token: 'tok_again' } });
    expect(put.status).toBe(403);
    expect(put.body.code).toBe('account_deleted');
    expect(env.billing.calls.length).toBe(before); // never reached billing
    expect((await call(env.url, 'POST', '/v1/sessions', { token: u.access, headers: { 'idempotency-key': 'rs-1' }, body: { stationId: 'st-1', connectorId: 1 } })).status).toBe(403);
    expect((await call(env.url, 'POST', '/v1/reservations', { token: u.access, headers: { 'idempotency-key': 'rv-1' }, body: { stationId: 'st-1', connectorId: 1 } })).status).toBe(403);
  });
});
