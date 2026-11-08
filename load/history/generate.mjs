/* oxlint-disable no-await-in-loop, no-unmodified-loop-condition */ // virtual drivers are deliberately sequential loops
// Reservation-history generator: contended reserve/cancel traffic against driver-api, one JSON line per operation.
// Env: DRIVERS=40, CONNECTORS=6 (pool taken from GET /v1/sites), DURATION_S=120, OUT=. , API=http://127.0.0.1:28082
// Writes $OUT/history.jsonl (flushed per op) and $OUT/summary.json. Feed history.jsonl to check.mjs.
// Outcomes: ok = definitely accepted (2xx), rejected = definite 4xx refusal, error = timeout/5xx/429/network (unknown, never rejected).
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';

const API = process.env.API ?? 'http://127.0.0.1:28082';
const DRIVERS = Number(process.env.DRIVERS ?? 12);
const POOL = Number(process.env.CONNECTORS ?? 6);
const DURATION_S = Number(process.env.DURATION_S ?? 120);
const OUT = process.env.OUT ?? '.';
const TIMEOUT_MS = 5000;
const HISTORY = `${OUT}/history.jsonl`;
let deadline = Date.now() + DURATION_S * 1000;
for (const sig of ["SIGTERM", "SIGINT"]) process.on(sig, () => { deadline = 0; });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const counts = { total: 0, byOutcome: {}, byType: {} };

/** Never throws: network errors and timeouts come back as status 0. */
async function api(method, path, { token, body, key } = {}) {
  try {
    const res = await fetch(`${API}${path}`, {
      method,
      headers: { ...(body ? { 'content-type': 'application/json' } : {}), ...(token ? { authorization: `Bearer ${token}` } : {}), ...(key ? { 'idempotency-key': key } : {}) },
      ...(body ? { body: JSON.stringify(body) } : {}),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    const text = await res.text();
    let parsed;
    try {
      parsed = text ? JSON.parse(text) : undefined;
    } catch {
      /* non-JSON body: treated as no body */
    }
    return { status: res.status, body: parsed };
  } catch {
    return { status: 0 };
  }
}

async function login(creds) {
  const res = await api('POST', '/v1/auth/login', { body: creds });
  return res.body?.accessToken;
}

async function newDriver() {
  for (;;) {
    const creds = { email: `history-${randomUUID()}@load.test`, password: `pw-${randomUUID()}` };
    await api('POST', '/v1/auth/register', { body: creds, key: randomUUID() });
    const token = await login(creds);
    if (token) return { creds, token };
    if (Date.now() > deadline) return undefined;
    await sleep(500);
  }
}

const classify = (status, okCodes) => (okCodes.includes(status) ? 'ok' : status >= 400 && status < 500 && status !== 408 && status !== 429 ? 'rejected' : 'error');

function record(op) {
  appendFileSync(HISTORY, `${JSON.stringify(op)}\n`);
  counts.total++;
  counts.byOutcome[op.outcome] = (counts.byOutcome[op.outcome] ?? 0) + 1;
  counts.byType[op.type] = (counts.byType[op.type] ?? 0) + 1;
}

async function timed(method, path, opts) {
  const invokeMs = Date.now();
  const res = await api(method, path, opts);
  return { res, invokeMs, completeMs: Date.now() };
}

async function driverLoop(n, pool) {
  const me = await newDriver();
  if (!me) return;
  const driverId = `d${n}`;
  while (Date.now() < deadline) {
    const c = pool[Math.floor(Math.random() * pool.length)];
    const { res, invokeMs, completeMs } = await timed('POST', '/v1/reservations', { token: me.token, body: c, key: randomUUID() });
    const outcome = classify(res.status, [200, 201]);
    const rid = res.body?.reservationId;
    const op = { opId: randomUUID(), driverId, type: 'reserve', ...c, invokeMs, completeMs, outcome, status: res.status };
    if (outcome === 'ok') Object.assign(op, { reservationId: rid, startsAt: res.body.startsAt, expiresAt: res.body.expiresAt });
    record(op);
    if (res.status === 401) me.token = (await login(me.creds)) ?? me.token; // re-login on auth failure
    if (outcome !== 'ok') {
      await sleep(400 + Math.random() * 800); // think time: a tight retry loop on a few connectors only measures lock contention
      continue;
    }
    await sleep(500 + Math.random() * 2500);
    // A driver whose cancel failed (outage) keeps retrying, otherwise it would hold its reservation for the full 15 minutes.
    for (let attempt = 0; attempt < 60; attempt++) {
      const del = await timed('DELETE', `/v1/reservations/${rid}`, { token: me.token, key: randomUUID() });
      const cancelOutcome = classify(del.res.status, [200, 202, 204]);
      record({ opId: randomUUID(), driverId, type: 'cancel', ...c, reservationId: rid, invokeMs: del.invokeMs, completeMs: del.completeMs, outcome: cancelOutcome, status: del.res.status });
      if (del.res.status === 401) me.token = (await login(me.creds)) ?? me.token;
      if (cancelOutcome !== 'error') break;
      await sleep(500);
    }
  }
}

async function connectorPool() {
  const me = await newDriver();
  for (let i = 0; i < 60 && me; i++) {
    const r = await api('GET', '/v1/sites?lat=52.52&lng=13.405&radiusKm=100&limit=100', { token: me.token });
    const all = (r.body?.sites ?? []).flatMap((s) => s.stations.flatMap((st) => st.connectors.map((c) => ({ stationId: st.stationId, connectorId: c.connectorId, status: c.status }))));
    // Idle connectors first: a station rejects ReserveNow on a busy connector, which would turn most 201s into Rejected.
    const idle = all.filter((c) => c.status === 'Available');
    if (all.length) return (idle.length >= POOL ? idle : all).slice(0, POOL).map(({ stationId, connectorId }) => ({ stationId, connectorId }));
    await sleep(1000);
  }
  throw new Error('no connectors found via GET /v1/sites');
}

mkdirSync(OUT, { recursive: true });
writeFileSync(HISTORY, '');
const pool = await connectorPool();
await Promise.all(Array.from({ length: DRIVERS }, (_, i) => driverLoop(i, pool)));
writeFileSync(`${OUT}/summary.json`, `${JSON.stringify({ ...counts, connectors: pool }, null, 2)}\n`);
console.log(JSON.stringify(counts));
