/* oxlint-disable no-await-in-loop */ // each virtual driver is a sequential story
import { randomUUID } from 'node:crypto';
import type { Logger } from '@evcharge/platform';
import type { SimMetrics } from './metrics';
import { errorMessage, type Rng } from './util';

/** driver-api paths (services/driver-api README). Change them here only. */
export const DRIVER_API = {
  register: '/v1/auth/register',
  login: '/v1/auth/login',
  paymentMethod: '/v1/me/payment-method',
  sites: '/v1/sites',
  reserve: '/v1/reservations',
  remoteStart: '/v1/sessions',
  sessions: '/v1/sessions',
  stop: (sessionId: string) => `/v1/sessions/${sessionId}/stop`,
} as const;

interface Json {
  [k: string]: unknown;
}

export interface DriverTrafficOptions {
  baseUrl: string;
  metrics: SimMetrics;
  log: Logger;
  rng: Rng;
  /** Mean real seconds between a driver's actions. */
  thinkS: number;
  /** Receives idTags returned at registration so stations can authorise them. */
  onIdTag?: (idTag: string) => void;
  fetch?: typeof fetch;
}

const sleepMs = (ms: number, signal: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const t = setTimeout(resolve, ms);
    signal.addEventListener('abort', () => {
      clearTimeout(t);
      resolve();
    });
  });

/** Virtual drivers talking to driver-api: register, log in, find a charger, reserve, remote start, remote stop. */
export class DriverTraffic {
  private ctl: AbortController | undefined;
  private tasks: Promise<void>[] = [];
  private lastWarnMs = 0;
  active = 0;

  constructor(private readonly o: DriverTrafficOptions) {}

  get running(): boolean {
    return this.ctl !== undefined && !this.ctl.signal.aborted;
  }

  start(count: number, ratePerS = 5): void {
    if (this.running) return;
    const ctl = new AbortController();
    this.ctl = ctl;
    this.tasks = Array.from({ length: count }, (_, i) => this.driver(i, (i / ratePerS) * 1000, ctl.signal));
  }

  async stop(): Promise<void> {
    this.ctl?.abort();
    await Promise.allSettled(this.tasks);
    this.tasks = [];
    this.active = 0;
  }

  private async req(op: string, method: string, path: string, token: string | undefined, body?: unknown, extra: Record<string, string> = {}): Promise<{ status: number; body: Json; retryAfterMs?: number } | undefined> {
    const f = this.o.fetch ?? fetch;
    try {
      const res = await f(`${this.o.baseUrl}${path}`, {
        method,
        headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}), ...extra },
        ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
        signal: AbortSignal.timeout(5000),
      });
      const text = await res.text();
      let json: Json = {};
      try {
        json = text ? (JSON.parse(text) as Json) : {};
      } catch {
        json = {};
      }
      this.o.metrics.driverRequests.inc({ op, outcome: res.ok ? 'ok' : res.status >= 500 ? 'http_5xx' : 'http_4xx' });
      const ra = Number(res.headers.get('retry-after'));
      return { status: res.status, body: json, ...(Number.isFinite(ra) && ra > 0 ? { retryAfterMs: ra * 1000 } : {}) };
    } catch (err) {
      this.o.metrics.driverRequests.inc({ op, outcome: 'unreachable' });
      const now = Date.now();
      if (now - this.lastWarnMs > 60_000) {
        this.lastWarnMs = now;
        this.o.log.warn({ op, err: errorMessage(err) }, 'driver-api not reachable (tolerated; retrying)');
      }
      return undefined;
    }
  }

  private async driver(n: number, startDelayMs: number, signal: AbortSignal): Promise<void> {
    const think = () => sleepMs((0.5 + this.o.rng()) * this.o.thinkS * 1000, signal);
    await sleepMs(startDelayMs, signal);
    const email = `sim-${randomUUID().slice(0, 8)}-${n}@example.test`;
    const password = `Sim-${randomUUID()}`;
    let token: string | undefined;
    const login = async (): Promise<{ ok: boolean; retryAfterMs?: number }> => {
      const r = await this.req('login', 'POST', DRIVER_API.login, undefined, { email, password });
      const t = r?.body['accessToken'] ?? r?.body['access_token'] ?? r?.body['token'];
      if ((r?.status === 200 || r?.status === 201) && typeof t === 'string') {
        token = t;
        return { ok: true };
      }
      return { ok: false, ...(r?.retryAfterMs ? { retryAfterMs: r.retryAfterMs } : {}) };
    };
    // register once (409 means it already exists, so go straight to login), then log in; a 429 waits for Retry-After
    let registered = false;
    for (;;) {
      if (signal.aborted) break;
      let waitMs = 5000 + this.o.rng() * 10_000;
      if (!registered) {
        const r = await this.req('register', 'POST', DRIVER_API.register, undefined, { email, password, name: `Sim Driver ${n}` });
        if (r && (r.status === 200 || r.status === 201 || r.status === 409)) {
          registered = true;
          const tag = r.body['idTag'];
          if (typeof tag === 'string') this.o.onIdTag?.(tag);
        } else if (r?.status === 429 && r.retryAfterMs) {
          waitMs = r.retryAfterMs + this.o.rng() * 1000;
        }
      }
      if (registered) {
        const l = await login();
        if (l.ok) break;
        if (l.retryAfterMs) waitMs = l.retryAfterMs + this.o.rng() * 1000;
      }
      await sleepMs(waitMs, signal);
    }
    // A card on file, so billing can authorise a hold and capture (ADR 0007). One in ten drivers uses the mock PSP's
    // declining card when PSP_DECLINE_TOKENS lists it, which exercises the compensation path.
    const card = n % 10 === 9 ? 'tok_decline' : `tok_sim_${n}`;
    await this.req('payment_method', 'PUT', DRIVER_API.paymentMethod, token, { token: card }, { 'idempotency-key': randomUUID() });
    this.active++;
    try {
      while (!signal.aborted) {
        await think();
        if (signal.aborted) break;
        await this.oneVisit(() => token, async () => (await login()).ok, think, signal);
      }
    } finally {
      this.active--;
    }
  }

  private async oneVisit(getToken: () => string | undefined, relogin: () => Promise<boolean>, think: () => Promise<void>, signal: AbortSignal): Promise<void> {
    const call = async (op: string, method: string, path: string, body?: unknown, extra: Record<string, string> = {}) => {
      let r = await this.req(op, method, path, getToken(), body, extra);
      if (r?.status === 401 && (await relogin())) r = await this.req(op, method, path, getToken(), body, extra);
      return r;
    };
    const lat = 52.52 + (this.o.rng() - 0.5) * 0.2;
    const lng = 13.4 + (this.o.rng() - 0.5) * 0.3;
    const sites = await call('sites', 'GET', `${DRIVER_API.sites}?lat=${lat.toFixed(4)}&lng=${lng.toFixed(4)}&radiusKm=25`);
    const list = Array.isArray(sites?.body['sites']) ? (sites?.body['sites'] as Json[]) : [];
    const free: { stationId: string; connectorId: number }[] = [];
    for (const s of list) {
      for (const st of (Array.isArray(s['stations']) ? s['stations'] : []) as Json[]) {
        if (st['online'] !== true) continue;
        for (const c of (Array.isArray(st['connectors']) ? st['connectors'] : []) as Json[]) {
          if (c['status'] === 'Available') free.push({ stationId: String(st['stationId']), connectorId: Number(c['connectorId']) });
        }
      }
    }
    const pick = free[Math.floor(this.o.rng() * free.length)];
    if (!pick) return;
    if (this.o.rng() < 0.5) {
      const r = await call('reserve', 'POST', DRIVER_API.reserve, pick, { 'idempotency-key': randomUUID() });
      if (!r || r.status >= 300) return; // 409 connector_unavailable is a normal outcome under contention
      const id = r.body['reservationId'];
      if (this.o.rng() < 0.2 && typeof id === 'number') {
        // some drivers change their mind
        await think();
        await call('cancel', 'DELETE', `${DRIVER_API.reserve}/${id}`, undefined, { 'idempotency-key': randomUUID() });
        return;
      }
      await sleepMs(1000 + this.o.rng() * 3000, signal);
    }
    const started = await call('remote_start', 'POST', DRIVER_API.remoteStart, pick, { 'idempotency-key': randomUUID() });
    if (!started || started.status >= 300) return;
    // find the session and stop it after a while
    for (let i = 0; i < 20 && !signal.aborted; i++) {
      await sleepMs(1500, signal);
      const r = await call('sessions', 'GET', DRIVER_API.sessions);
      const items = Array.isArray(r?.body['sessions']) ? (r?.body['sessions'] as Json[]) : Array.isArray(r?.body) ? (r?.body as unknown as Json[]) : [];
      const s = items.find((x) => x['stationId'] === pick.stationId && x['connectorId'] === pick.connectorId && x['status'] === 'Active');
      if (!s) continue;
      await sleepMs((0.3 + this.o.rng()) * this.o.thinkS * 1000, signal);
      await call('remote_stop', 'POST', DRIVER_API.stop(String(s['sessionId'])), {}, { 'idempotency-key': randomUUID() });
      return;
    }
  }
}
