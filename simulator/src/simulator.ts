import { api } from '@evcharge/contracts';
import type { Logger } from '@evcharge/platform';
import { randomUUID } from 'node:crypto';
import type { SimConfig } from './config';
import { arrivalRatePerS, localHour, MEAN_DURATION_MIN, utilisation } from './demand';
import { DriverTraffic } from './drivers';
import { buildFleet, CONNECTORS_PER_STATION, conventionSiteCapW, type StationSpec } from './fleet';
import { simMetrics, type SimMetrics } from './metrics';
import { ProbeLog } from './probe';
import { Station, type SimContext } from './station';
import { errorMessage, mulberry32, poisson, SimClock, type Rng } from './util';
import { TimerWheel } from './wheel';

interface Site {
  siteId: string;
  capW: number;
  /** True once the cap was read from sessions; false while it is the shared-convention value. */
  capFromSessions: boolean;
  stations: Station[];
}

export interface FloodJob {
  id: string;
  requested: number;
  connected: number;
  failed: number;
  startedAt: string;
  finishedAt?: string;
}

/**
 * The fleet and its controls: demand generation, the control modes (offline, storm, flood,
 * probes, drivers) and the gauges. One process, one event loop, one timing wheel (ADR 0013).
 */
export class Simulator {
  readonly metrics: SimMetrics;
  readonly clock: SimClock;
  readonly wheel: TimerWheel;
  readonly stations: Station[];
  readonly byId = new Map<string, Station>();
  readonly sites = new Map<string, Site>();
  readonly floods: FloodJob[] = [];
  drivers: DriverTraffic | undefined;
  probeLog: ProbeLog | undefined;
  readonly runId = `${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}-${randomUUID().slice(0, 6)}`;

  private readonly rng: Rng;
  private readonly ctx: SimContext;
  private readonly idTags: string[] = [];
  private connected = 0;
  private arrivalTimer: NodeJS.Timeout | undefined;
  private capTimer: NodeJS.Timeout | undefined;
  private lastArrivalMs: number;
  private readonly acConnectors: { st: Station; cid: number }[] = [];
  private readonly dcConnectors: { st: Station; cid: number }[] = [];
  private stopped = false;

  constructor(
    readonly config: SimConfig,
    private readonly log: Logger,
    opts: { fetch?: typeof fetch } = {},
  ) {
    this.metrics = simMetrics();
    this.rng = mulberry32(config.SIM_SEED);
    this.clock = new SimClock(config.SIM_TIME_SCALE, config.SIM_START ? Date.parse(config.SIM_START) : Date.now());
    this.wheel = new TimerWheel(100, 4096, Date.now, (err) => this.log.error({ err: errorMessage(err) }, 'timer callback failed'));
    this.lastArrivalMs = this.clock.now();
    if (config.SIM_PROBE_STATIONS > 0) this.probeLog = new ProbeLog(config.SIM_PROBE_DIR, this.runId);
    this.ctx = {
      clock: this.clock,
      wheel: this.wheel,
      rng: this.rng,
      metrics: this.metrics,
      log: this.log,
      csmsUrl: config.CSMS_URL.replace(/\/+$/, ''),
      secretSeed: config.STATION_SECRET_SEED,
      ...(this.probeLog ? { probe: this.probeLog } : {}),
      queueMax: config.SIM_QUEUE_MAX,
      meterIntervalS: config.SIM_METER_INTERVAL_S,
      callTimeoutMs: config.SIM_CALL_TIMEOUT_S * 1000,
      ...(config.SIM_HEARTBEAT_S !== undefined ? { heartbeatS: config.SIM_HEARTBEAT_S } : {}),
      pickIdTag: (rng) => (this.idTags.length > 0 && rng() < 0.4 ? (this.idTags[Math.floor(rng() * this.idTags.length)] ?? 'SIMANON') : `SIMANON${Math.floor(rng() * 500)}`),
      onConnectionChange: (_st, up) => {
        this.connected += up ? 1 : -1;
        this.metrics.connected.set(this.connected);
      },
    };
    const specs = buildFleet(config.SIM_SITES, config.SIM_STATIONS_PER_SITE, config.SIM_DC_SHARE);
    this.stations = specs.map((s: StationSpec) => new Station(s, this.ctx));
    const bySite = new Map<string, StationSpec[]>();
    for (const st of this.stations) {
      this.byId.set(st.id, st);
      let site = this.sites.get(st.spec.siteId);
      if (!site) {
        site = { siteId: st.spec.siteId, capW: 0, capFromSessions: false, stations: [] };
        this.sites.set(st.spec.siteId, site);
        bySite.set(st.spec.siteId, []);
      }
      site.stations.push(st);
      bySite.get(st.spec.siteId)?.push(st.spec);
      for (let c = 1; c <= CONNECTORS_PER_STATION; c++) (st.spec.kind === 'AC' ? this.acConnectors : this.dcConnectors).push({ st, cid: c });
    }
    for (const [siteId, list] of bySite) {
      const site = this.sites.get(siteId);
      if (site) site.capW = conventionSiteCapW(siteId, list);
    }
    this.metrics.stations.set(this.stations.length);
    this.drivers = config.DRIVER_API_URL
      ? new DriverTraffic({ baseUrl: config.DRIVER_API_URL.replace(/\/+$/, ''), metrics: this.metrics, log, rng: this.rng, thinkS: config.SIM_DRIVER_THINK_S, onIdTag: (t) => this.addIdTag(t), ...(opts.fetch ? { fetch: opts.fetch } : {}) })
      : undefined;
  }

  addIdTag(tag: string): void {
    if (this.idTags.length < 5000) this.idTags.push(tag);
  }

  // ---------------------------------------------------------------- lifecycle

  /** Starts the wheel, the demand generator and (SIM_AUTOSTART) the fleet. */
  start(): void {
    this.wheel.start();
    if (this.config.SIM_PROBE_STATIONS > 0) this.setProbes(this.config.SIM_PROBE_STATIONS);
    const tickMs = Math.min(1000, Math.max(100, 1000 / this.config.SIM_TIME_SCALE));
    this.arrivalTimer = setInterval(() => this.arrivals(), tickMs);
    void this.refreshCaps();
    this.capTimer = setInterval(() => void this.refreshCaps(), 300_000);
    if (this.config.SIM_AUTOSTART) {
      this.connectFleet(this.stations.length, this.config.SIM_CONNECT_RATE);
      if (this.config.SIM_WARM_START) this.wheel.schedule(Math.ceil((this.stations.length / this.config.SIM_CONNECT_RATE) * 1000) + 5000, () => this.warmStart());
    }
    if (this.drivers && this.config.SIM_DRIVERS > 0 && this.config.SIM_DRIVERS_AUTOSTART) this.drivers.start(this.config.SIM_DRIVERS);
    this.log.info({ stations: this.stations.length, sites: this.sites.size, scale: this.config.SIM_TIME_SCALE, probes: this.config.SIM_PROBE_STATIONS, runId: this.runId }, 'simulator started');
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.arrivalTimer) clearInterval(this.arrivalTimer);
    if (this.capTimer) clearInterval(this.capTimer);
    await this.drivers?.stop();
    for (const s of this.stations) s.stop();
    this.wheel.stop();
    await this.probeLog?.close();
  }

  // ---------------------------------------------------------------- demand

  /** Spawns vehicles: arrivals ~ Poisson with the daily utilisation profile, blocked on busy connectors. */
  private arrivals(): void {
    const now = this.clock.now();
    const dtS = Math.max(0, (now - this.lastArrivalMs) / 1000);
    this.lastArrivalMs = now;
    const u = utilisation(localHour(now, this.config.SIM_UTC_OFFSET_H));
    for (const [list, kind] of [
      [this.acConnectors, 'AC'],
      [this.dcConnectors, 'DC'],
    ] as const) {
      if (list.length === 0) continue;
      const rate = arrivalRatePerS(u, MEAN_DURATION_MIN[kind] * 60);
      const n = poisson(list.length * rate * dtS, this.rng);
      for (let i = 0; i < n; i++) {
        const pick = list[Math.floor(this.rng() * list.length)];
        if (pick?.st.wantOnline && !pick.st.probe) pick.st.plugIn(pick.cid, this.ctx.pickIdTag(this.rng));
      }
    }
  }

  /** Plugs in vehicles on idle connectors with probability = current utilisation (steady-state occupancy). */
  private warmStart(): void {
    if (this.stopped) return;
    const u = utilisation(localHour(this.clock.now(), this.config.SIM_UTC_OFFSET_H));
    let n = 0;
    for (const pick of [...this.acConnectors, ...this.dcConnectors]) {
      if (this.rng() < u && pick.st.wantOnline && !pick.st.probe && pick.st.plugIn(pick.cid, this.ctx.pickIdTag(this.rng))) n++;
    }
    this.log.info({ utilisation: u, plugged: n }, 'warm start');
  }

  // ---------------------------------------------------------------- modes

  /** Connects the first `count` stations not yet started, `ratePerS` per second (ramp). */
  connectFleet(count: number, ratePerS: number): number {
    const todo = this.stations.filter((s) => !s.wantOnline).slice(0, count);
    const perTick = Math.max(1, Math.ceil(ratePerS / 10));
    let i = 0;
    const step = () => {
      if (this.stopped) return;
      for (let k = 0; k < perTick && i < todo.length; k++, i++) this.startStation(todo[i] as Station);
      if (i < todo.length) this.wheel.schedule(100, step);
    };
    step();
    return todo.length;
  }

  private startStation(st: Station, kind: 'initial' | 'retry' | 'after_drop' = 'initial'): void {
    st.start(0, kind);
    if (st.probe) for (const c of st.connectors) st.startProbeSession(c);
  }

  /** Stops `count` connected stations for good (no reconnect). */
  disconnectFleet(count: number): number {
    const targets = this.stations.filter((s) => s.wantOnline).slice(0, count);
    for (const s of targets) s.stop();
    return targets.length;
  }

  /** Offline drill: `count` connected stations (or the listed ids) lose the network for `minutes` simulated minutes (max 24 h). */
  offline(o: { stations?: number; ids?: string[]; minutes: number }): { stations: string[]; untilRealMs: number; realSeconds: number } {
    const minutes = Math.min(1440, Math.max(0.01, o.minutes));
    const realMs = this.clock.realMs(minutes * 60_000);
    const untilRealMs = Date.now() + realMs;
    const pool = o.ids ? o.ids.map((id) => this.byId.get(id)).filter((s): s is Station => Boolean(s)) : this.shuffle(this.stations.filter((s) => s.online)).slice(0, o.stations ?? 1);
    for (const s of pool) s.goOffline(untilRealMs);
    this.log.info({ stations: pool.length, minutes, realSeconds: Math.round(realMs / 1000) }, 'offline drill started');
    return { stations: pool.map((s) => s.id), untilRealMs, realSeconds: realMs / 1000 };
  }

  /** Storm drill: every connected station (or `count` of them) disconnects at once; each reconnects per the ADR 0003 policy. */
  storm(count?: number): number {
    const pool = this.stations.filter((s) => s.online);
    const targets = count === undefined ? pool : this.shuffle(pool).slice(0, count);
    for (const s of targets) s.drop();
    this.log.info({ stations: targets.length }, 'storm drill: stations disconnected');
    return targets.length;
  }

  /** Flood drill: connect `count` stations as fast as possible (bounded only by `concurrency` handshakes in flight). */
  flood(count: number, concurrency = 1000): FloodJob {
    const job: FloodJob = { id: randomUUID().slice(0, 8), requested: 0, connected: 0, failed: 0, startedAt: new Date().toISOString() };
    this.floods.push(job);
    if (this.floods.length > 20) this.floods.shift();
    const targets = [...this.stations.filter((s) => !s.wantOnline), ...this.stations.filter((s) => s.wantOnline && !s.online)].slice(0, count);
    job.requested = targets.length;
    let next = 0;
    // `concurrency` workers each take the next station and wait until its first attempt settled.
    const worker = async (): Promise<void> => {
      while (next < targets.length) {
        const st = targets[next++] as Station;
        const settled = st.whenSettled();
        if (st.wantOnline) st.start(0, 'retry');
        else this.startStation(st);
        // oxlint-disable-next-line no-await-in-loop -- sequential per worker by design
        if (await settled) job.connected++;
        else job.failed++;
      }
    };
    const finish = async (): Promise<void> => {
      await Promise.all(Array.from({ length: Math.min(concurrency, targets.length) }, worker));
      job.finishedAt = new Date().toISOString();
    };
    void finish();
    if (targets.length === 0) job.finishedAt = new Date().toISOString();
    return job;
  }

  /** Marks `count` evenly spread stations as probes; they charge continuously and log every reading. */
  setProbes(count: number): string[] {
    if (!this.probeLog) this.probeLog = new ProbeLog(this.config.SIM_PROBE_DIR, this.runId);
    this.ctx.probe = this.probeLog;
    for (const s of this.stations) s.probe = false;
    const n = Math.min(count, this.stations.length);
    const ids: string[] = [];
    for (let i = 0; i < n; i++) {
      const st = this.stations[Math.floor((i * this.stations.length) / n)];
      if (!st) continue;
      st.probe = true;
      ids.push(st.id);
      if (st.wantOnline) for (const c of st.connectors) st.startProbeSession(c);
    }
    return ids;
  }

  probeStations(): string[] {
    return this.stations.filter((s) => s.probe).map((s) => s.id);
  }

  startDrivers(count: number, ratePerS?: number): boolean {
    if (!this.drivers) return false;
    this.drivers.start(count, ratePerS);
    return true;
  }

  // ---------------------------------------------------------------- reading state

  /** Real site caps from `sessions` when configured; otherwise the fallback fraction stays. */
  async refreshCaps(): Promise<void> {
    const { SESSIONS_URL, INTERNAL_TOKEN } = this.config;
    if (!SESSIONS_URL || !INTERNAL_TOKEN) return;
    try {
      const res = await fetch(`${SESSIONS_URL.replace(/\/+$/, '')}/v1/sites?lat=52.52&lng=13.4&radiusKm=100`, {
        headers: { 'x-internal-token': INTERNAL_TOKEN, 'x-driver-id': randomUUID() },
        signal: AbortSignal.timeout(15_000),
      });
      if (!res.ok) return;
      const parsed = api.SessionsPublic.listSitesRes.safeParse(await res.json());
      if (!parsed.success) return;
      for (const s of parsed.data.sites) {
        const site = this.sites.get(s.siteId);
        if (site) {
          site.capW = s.capW;
          site.capFromSessions = true;
        }
      }
    } catch (err) {
      this.log.debug({ err: errorMessage(err) }, 'site caps not refreshed from sessions');
    }
  }

  /** Refreshes the gauges that are computed from the whole fleet (called before every scrape). */
  updateGauges(): void {
    const now = this.clock.now();
    let queue = 0;
    for (const s of this.stations) queue += s.queue.length;
    this.metrics.replayQueue.set(queue);
    for (const site of this.sites.values()) {
      let drawn = 0;
      for (const s of site.stations) drawn += s.drawnNowW(now);
      this.metrics.siteDrawn.set({ site: site.siteId }, drawn);
      this.metrics.siteCap.set({ site: site.siteId }, site.capW);
      this.metrics.siteCapFromSessions.set({ site: site.siteId }, site.capFromSessions ? 1 : 0);
    }
  }

  status() {
    let active = 0;
    let queue = 0;
    let started = 0;
    for (const s of this.stations) {
      if (s.wantOnline) started++;
      queue += s.queue.length;
      for (const c of s.connectors) if (c.tx) active++;
    }
    return {
      runId: this.runId,
      stations: this.stations.length,
      sites: this.sites.size,
      siteCapsFromSessions: [...this.sites.values()].filter((x) => x.capFromSessions).length,
      started,
      connected: this.connected,
      activeSessions: active,
      replayQueueDepth: queue,
      timeScale: this.config.SIM_TIME_SCALE,
      simTime: this.clock.iso(),
      probeStations: this.probeStations().length,
      probeFile: this.probeLog?.path ?? null,
      drivers: this.drivers ? { running: this.drivers.running, active: this.drivers.active } : null,
      floods: this.floods,
    };
  }

  private shuffle<T>(a: T[]): T[] {
    for (let i = a.length - 1; i > 0; i--) {
      const j = Math.floor(this.rng() * (i + 1));
      [a[i], a[j]] = [a[j] as T, a[i] as T];
    }
    return a;
  }
}
