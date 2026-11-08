/* oxlint-disable no-await-in-loop */ // probes are deliberately sequential
// Driver traffic for drills/replica-lag.sh (ADR 0015). Plain Node 20+, no dependencies.
//
// 1. Read load: LOAD_RPS of GET /v1/sites (served by the sessions replica) with client-side latency per phase.
// 2. Probe drivers (PROBES of them, one connector each). Each cycle:
//      remote start -> wait until the driver's session list shows it Active on the primary (the truth)
//      -> poll the site map until that connector shows Charging, SuspendedEVSE or SuspendedEV; the delay is the staleness of the replica read
//      -> stop the session and read the session back at once: it must not be "Active" any more
//         (a read-your-writes violation otherwise; the replica may be paused, so a stale read would show Active).
//
// Env: URL (driver-api), DURATION_S, LOAD_RPS, PROBES, OUT (directory), PHASE_FILE (drill phase, optional).
// Writes OUT/probe-events.jsonl and, on exit, OUT/probe-summary.json.
import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';

const BASE_URL = process.env.URL ?? 'http://localhost:28082';
const DURATION_S = Number(process.env.DURATION_S ?? 300);
const LOAD_RPS = Number(process.env.LOAD_RPS ?? 200);
const PROBES = Number(process.env.PROBES ?? 4);
const OUT = process.env.OUT ?? '.';
const PHASE_FILE = process.env.PHASE_FILE;
const STALE_TIMEOUT_MS = Number(process.env.STALE_TIMEOUT_MS ?? 240_000);
const EVENTS = `${OUT}/probe-events.jsonl`;
const deadline = Date.now() + DURATION_S * 1000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const phase = () => {
  try {
    return PHASE_FILE ? readFileSync(PHASE_FILE, 'utf8').trim() : 'run';
  } catch {
    return 'after';
  }
};
const emit = (e) => appendFileSync(EVENTS, `${JSON.stringify({ t: new Date().toISOString(), phase: phase(), ...e })}\n`);

async function api(method, path, { token, body, key } = {}) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method,
    headers: { ...(body ? { 'content-type': 'application/json' } : {}), ...(token ? { authorization: `Bearer ${token}` } : {}), ...(key ? { 'idempotency-key': key } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
    signal: AbortSignal.timeout(10_000),
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : undefined };
}

async function newDriver() {
  const email = `replica-probe-${randomUUID()}@load.test`;
  const password = `pw-${randomUUID()}`;
  await api('POST', '/v1/auth/register', { body: { email, password }, key: randomUUID() });
  const login = await api('POST', '/v1/auth/login', { body: { email, password } });
  if (!login.body?.accessToken) throw new Error(`login failed: ${login.status}`);
  return login.body.accessToken;
}

const summary = { ryw_checks: 0, ryw_violations: 0, stale_probes: 0, stale_max_s: 0, stale_unresolved: 0, probe_errors: 0, reads: 0, read_errors: 0, read_p99_ms: { before: null, during: null, after: null } };
const latencies = { before: [], during: [], after: [] };

async function readLoad(token) {
  const tick = 1000 / LOAD_RPS;
  let i = 0;
  const inflight = new Set();
  while (Date.now() < deadline) {
    const lat = (52.4 + (i++ % 30) * 0.01).toFixed(3);
    const ph = phase();
    const t0 = performance.now();
    const p = fetch(`${BASE_URL}/v1/sites?lat=${lat}&lng=13.4&radiusKm=10`, { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(10_000) })
      .then(async (r) => {
        await r.arrayBuffer();
        summary.reads++;
        if (r.status !== 200) summary.read_errors++;
        (latencies[ph] ??= []).push(performance.now() - t0);
        return undefined;
      })
      .catch(() => void summary.read_errors++)
      .finally(() => inflight.delete(p));
    inflight.add(p);
    if (inflight.size > 400) await Promise.race(inflight);
    await sleep(tick);
  }
  await Promise.allSettled(inflight);
}

// A started session shows as one of these in the site map; SuspendedEVSE is a session the allocator holds at the site cap (never Charging).
const SESSION_STATUSES = new Set(['Charging', 'SuspendedEVSE', 'SuspendedEV']);
const connectorStatus = (sites, stationId, connectorId) => {
  for (const s of sites.sites ?? []) for (const st of s.stations) if (st.stationId === stationId) return st.connectors.find((c) => c.connectorId === connectorId)?.status;
  return undefined;
};

/** An online station's idle connector no other probe uses; location comes from the site map itself. */
async function pickConnector(token, taken) {
  const res = await api('GET', '/v1/sites?lat=52.52&lng=13.405&radiusKm=100&limit=100', { token });
  for (const s of res.body?.sites ?? [])
    for (const st of s.stations)
      if (st.online)
        for (const c of st.connectors) {
          const id = `${st.stationId}/${c.connectorId}`;
          if (c.status === 'Available' && !taken.has(id)) return { stationId: st.stationId, connectorId: c.connectorId, lat: s.lat, lng: s.lng, id };
        }
  return undefined;
}

async function probe(n, taken) {
  const token = await newDriver();
  while (Date.now() < deadline) {
    const c = await pickConnector(token, taken);
    if (!c) {
      await sleep(2000);
      continue;
    }
    taken.add(c.id);
    try {
      const started = await api('POST', '/v1/sessions', { token, body: { stationId: c.stationId, connectorId: c.connectorId }, key: randomUUID() });
      if (started.status !== 202) throw new Error(`remote start ${started.status}`);
      // Truth: the driver's own session list, read with x-min-lsn, so it is current even when the replica is paused.
      let session;
      for (let i = 0; i < 120 && !session; i++) {
        const list = await api('GET', '/v1/sessions?limit=5', { token });
        session = list.body?.sessions?.find((s) => s.status === 'Active' && s.stationId === c.stationId && s.connectorId === c.connectorId);
        if (!session) await sleep(250);
      }
      if (!session) throw new Error('session never became Active');
      const tActive = Date.now();
      // Staleness of the stale-tolerant read: how long until the site map (replica) shows the connector as Charging, SuspendedEVSE or SuspendedEV.
      let seenAt;
      let lastStatus;
      while (seenAt === undefined && Date.now() - tActive < STALE_TIMEOUT_MS) {
        const r = await api('GET', `/v1/sites?lat=${c.lat}&lng=${c.lng}&radiusKm=5&limit=5`, { token });
        lastStatus = connectorStatus(r.body ?? {}, c.stationId, c.connectorId);
        if (SESSION_STATUSES.has(lastStatus)) seenAt = Date.now();
        else await sleep(500);
      }
      if (seenAt === undefined) {
        summary.stale_unresolved++;
        emit({ kind: 'stale', probe: n, connector: c.id, unresolved: true, lastStatus, waitedS: (Date.now() - tActive) / 1000 });
      } else {
        const staleS = (seenAt - tActive) / 1000;
        summary.stale_probes++;
        summary.stale_max_s = Math.max(summary.stale_max_s, staleS);
        emit({ kind: 'stale', probe: n, connector: c.id, staleS });
      }
      // Read-your-writes: stop, then read the session straight back (and twice more, it must never return to Active).
      const stop = await api('POST', `/v1/sessions/${session.sessionId}/stop`, { token, key: randomUUID() });
      if (stop.status !== 202) throw new Error(`stop ${stop.status}`);
      for (let i = 0; i < 3; i++) {
        const got = await api('GET', `/v1/sessions/${session.sessionId}`, { token });
        const violation = got.status !== 200 || got.body?.status === 'Active';
        summary.ryw_checks++;
        if (violation) summary.ryw_violations++;
        emit({ kind: 'ryw', probe: n, read: i, status: got.status, sessionStatus: got.body?.status, violation });
      }
      for (let i = 0; i < 120; i++) {
        const got = await api('GET', `/v1/sessions/${session.sessionId}`, { token });
        if (got.body?.status === 'Completed') break;
        await sleep(500);
      }
    } catch (err) {
      summary.probe_errors++;
      emit({ kind: 'error', probe: n, message: String(err) });
      await sleep(2000);
    } finally {
      taken.delete(c.id);
    }
  }
}

const p99 = (xs) => (xs.length ? [...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.ceil(xs.length * 0.99) - 1)] : null);
function finish() {
  for (const k of Object.keys(summary.read_p99_ms)) summary.read_p99_ms[k] = p99(latencies[k] ?? []);
  writeFileSync(`${OUT}/probe-summary.json`, `${JSON.stringify(summary, null, 2)}\n`);
}
process.on('SIGTERM', () => {
  finish();
  process.exit(0);
});

const taken = new Set();
const loadToken = await newDriver();
await Promise.all([readLoad(loadToken), ...Array.from({ length: PROBES }, (_, i) => probe(i, taken))]);
finish();
