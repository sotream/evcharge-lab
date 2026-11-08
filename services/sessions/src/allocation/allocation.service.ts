/**
 * Allocation orchestration (ADR 0006). `sessions` owns site state and runs one allocation round per site:
 * on every StartTransaction/StopTransaction (sites.alloc_due_at = now) and every 60 s per site (each site has
 * its own phase, so rounds spread over the minute).
 *
 * Replica safety: a round claims its site with `FOR UPDATE SKIP LOCKED` and a short lease (token + expiry),
 * so two instances never run the same site concurrently; the claim transaction is tiny, so the OCPP hot path
 * (which touches the same row to mark it due) never waits on an allocator call.
 *
 * Load: periodic and trigger-driven rounds share one concurrency limit (ALLOCATION_CONCURRENCY). Start/Stop kicks go
 * through a deduplicated per-site queue, so a replay storm cannot flood the pool the OCPP hot path uses. The tick
 * loops again at once when it claimed a full batch, so a backlog drains at full speed.
 */
/* oxlint-disable no-await-in-loop */ // per-round statements are sequential inside one transaction
import { randomUUID } from 'node:crypto';
import { RedisKeys } from '@evcharge/contracts';
import { CircuitOpenError, HttpTimeoutError, type HttpClient, type Logger, type Pool, type Redis, withTx } from '@evcharge/platform';
import { errorMessage } from '../util';
import { m } from '../metrics';
import { emitSessionEvent, enqueueCommand, type TopicNames } from '../outbox-events';
import type { SiteSignal } from '../site-signal';
import { type ActiveSession, type Allocation, buildAllocateReq, fallbackAllocation, parseMeasured, planProfiles, setChargingProfilePayload, type SiteState, validateAllocation, VALIDITY_S } from './plan';

const LEASE_S = 15;
const PROFILE_COMMAND_TTL_S = 30;
// Only ever raises the stored value (several rounds or replicas may race).
const RAISE_SITE_VALID_TO = "local v = tonumber(redis.call('GET', KEYS[1]) or '0') if tonumber(ARGV[1]) > v then redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]) end return 1";
const SITE_VALID_TO_TTL_MS = 24 * 3600 * 1000;
/** After a failed round the site is retried this soon (not after the whole period). */
const FAILURE_BACKOFF_S = 5;
const BACKLOG_GAUGE_EVERY_MS = 5000;

export interface AllocationOptions {
  periodS: number;
  tickMs: number;
  concurrency: number;
}

interface Claimed {
  site_id: string;
  cap_w: number;
  requested_at: Date | null;
  token: string;
}

/** Thrown inside a round's transaction when the site lease expired: the whole round is rolled back. */
class LeaseLostError extends Error {}

export interface RoundResult {
  siteId: string;
  strategy: Allocation['strategy'] | 'none';
  sent: number;
  deferred: number;
  limits: Map<string, number>;
}

class Semaphore {
  private free: number;
  private readonly waiters: (() => void)[] = [];

  constructor(n: number) {
    this.free = n;
  }

  async run<T>(fn: () => Promise<T>): Promise<T> {
    if (this.free > 0) this.free--;
    else await new Promise<void>((r) => this.waiters.push(r));
    try {
      return await fn();
    } finally {
      const next = this.waiters.shift();
      if (next) next();
      else this.free++;
    }
  }
}

export class AllocationService {
  private timer: NodeJS.Timeout | undefined;
  private running = false;
  private ticking: Promise<void> | undefined;
  private readonly inFlight = new Map<string, Promise<RoundResult | undefined>>();
  private readonly rerun = new Set<string>();
  private readonly kickQueue = new Set<string>();
  private readonly pending = new Set<Promise<unknown>>();
  private readonly limit: Semaphore;
  private lastGauge = 0;
  private lastBacklog = 0;

  constructor(
    private readonly pool: Pool,
    private readonly redis: Redis | undefined,
    private readonly allocator: HttpClient,
    private readonly topics: TopicNames,
    private readonly opts: AllocationOptions,
    private readonly logger: Logger,
    signal?: SiteSignal,
  ) {
    this.limit = new Semaphore(opts.concurrency);
    signal?.onChanged((siteId) => this.kick(siteId));
  }

  private get batch(): number {
    return this.opts.concurrency * 4;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    const loop = () => {
      if (!this.running) return;
      let full = false;
      this.ticking = this.tick()
        .then((n) => void (full = n >= this.batch))
        .catch((err) => this.logger.error({ err: errorMessage(err) }, 'allocation tick failed'))
        .finally(() => {
          if (this.running) this.timer = setTimeout(loop, full ? 0 : this.opts.tickMs);
        });
    };
    this.timer = setTimeout(loop, 0);
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) clearTimeout(this.timer);
    await this.ticking;
    await Promise.allSettled(this.inFlight.values());
    await Promise.allSettled(this.pending);
  }

  /** Queues a site to run now if it is due and not leased (called after a start/stop commits). Deduplicated. */
  kick(siteId: string): void {
    if (!this.running) return;
    if (this.inFlight.has(siteId)) {
      this.rerun.add(siteId);
      return;
    }
    if (this.kickQueue.has(siteId)) return;
    this.kickQueue.add(siteId);
    const p = this.limit
      .run(async () => {
        this.kickQueue.delete(siteId);
        await this.runClaimed(siteId);
      })
      .catch((err) => this.logger.warn({ siteId, err: errorMessage(err) }, 'allocation kick failed'))
      .finally(() => this.pending.delete(p));
    this.pending.add(p);
  }

  /** Claims due sites (up to concurrency x 4) and runs them under the shared concurrency limit. Returns how many. */
  async tick(): Promise<number> {
    const claimed = await this.claim(undefined, this.batch);
    await Promise.all(claimed.map((c) => this.limit.run(() => this.track(c, () => this.round(c)))));
    if (Date.now() - this.lastGauge > 15_000) {
      this.lastGauge = Date.now();
      const { rows } = await this.pool.query<{ n: string }>('SELECT count(*) AS n FROM active_sessions');
      m.activeSessions.set(Number(rows[0]?.n ?? 0));
    }
    if (Date.now() - this.lastBacklog > BACKLOG_GAUGE_EVERY_MS) {
      this.lastBacklog = Date.now();
      const { rows } = await this.pool.query<{ s: number }>(
        `SELECT coalesce(extract(epoch FROM now() - min(alloc_due_at)), 0)::float8 AS s FROM sites
          WHERE alloc_due_at <= now() AND (alloc_lease_until IS NULL OR alloc_lease_until < now())`,
      );
      m.allocationBacklog.set(rows[0]?.s ?? 0);
    }
    return claimed.length;
  }

  /** Claims one specific site if due, and runs it. Exposed for tests. */
  async runClaimed(siteId: string): Promise<RoundResult | undefined> {
    const [c] = await this.claim(siteId, 1);
    if (!c) return undefined;
    return this.track(c, () => this.round(c));
  }

  private async track(c: Claimed, fn: () => Promise<RoundResult>): Promise<RoundResult | undefined> {
    const p = fn()
      .catch(async (err): Promise<undefined> => {
        this.logger.error({ siteId: c.site_id, err: errorMessage(err) }, 'allocation round failed');
        await this.restore(c).catch((e) => this.logger.error({ siteId: c.site_id, err: errorMessage(e) }, 'could not reschedule site'));
        return undefined;
      })
      .finally(() => {
        this.inFlight.delete(c.site_id);
        if (this.rerun.delete(c.site_id)) this.kick(c.site_id);
      });
    this.inFlight.set(c.site_id, p);
    return p;
  }

  /** A failed round must not lose its trigger: retry soon and keep the oldest pending trigger time. */
  private async restore(c: Claimed): Promise<void> {
    await this.pool.query(
      `UPDATE sites SET alloc_due_at = now() + make_interval(secs => $3), alloc_requested_at = coalesce(alloc_requested_at, $4::timestamptz),
              alloc_lease_token = NULL, alloc_lease_until = NULL
        WHERE site_id = $1 AND alloc_lease_token = $2`,
      [c.site_id, c.token, FAILURE_BACKOFF_S, c.requested_at],
    );
  }

  private async claim(siteId: string | undefined, limit: number): Promise<Claimed[]> {
    const token = randomUUID();
    const { rows } = await this.pool.query<Claimed>(
      `WITH due AS (
         SELECT site_id, alloc_requested_at FROM sites
          WHERE alloc_due_at <= now() AND (alloc_lease_until IS NULL OR alloc_lease_until < now()) AND ($1::text IS NULL OR site_id = $1)
          ORDER BY alloc_due_at LIMIT $2 FOR UPDATE SKIP LOCKED)
       UPDATE sites s SET alloc_lease_token = $3, alloc_lease_until = now() + make_interval(secs => $4),
              alloc_due_at = now() + make_interval(secs => $5), alloc_requested_at = NULL
         FROM due WHERE s.site_id = due.site_id
       RETURNING s.site_id, s.cap_w, due.alloc_requested_at AS requested_at, s.alloc_lease_token AS token`,
      [siteId ?? null, limit, token, LEASE_S, this.opts.periodS],
    );
    return rows;
  }

  async loadSite(siteId: string, capW: number): Promise<SiteState> {
    const { rows } = await this.pool.query<{
      session_id: string;
      transaction_id: number;
      station_id: string;
      connector_id: number;
      max_w: number;
      min_w: number;
      started_at: Date;
      limit_w: number | null;
      limit_valid_to: Date | null;
      confirmed_limit_w: number | null;
      confirmed_valid_to: Date | null;
      possible_limit_w: number | null;
      possible_valid_to: Date | null;
    }>(
      `SELECT a.session_id, a.transaction_id, a.station_id, a.connector_id, a.max_w, a.min_w, a.started_at, a.limit_w, a.limit_valid_to,
              a.confirmed_limit_w, a.confirmed_valid_to, a.possible_limit_w, a.possible_valid_to
         FROM active_sessions a WHERE a.site_id = $1 ORDER BY a.started_at, a.transaction_id`,
      [siteId],
    );
    const r = await this.pool.query<{ n: string }>('SELECT count(*) AS n FROM connectors WHERE site_id = $1', [siteId]);
    const connectorsAtSite = Number(r.rows[0]?.n ?? 0);
    const sessions: ActiveSession[] = rows.map((x) => ({
      sessionId: x.session_id,
      transactionId: x.transaction_id,
      stationId: x.station_id,
      connectorId: x.connector_id,
      maxW: x.max_w,
      minW: x.min_w,
      startedAt: x.started_at,
      limitW: x.limit_w,
      limitValidTo: x.limit_valid_to,
      confirmedW: x.confirmed_limit_w,
      confirmedValidTo: x.confirmed_valid_to,
      possibleW: x.possible_limit_w,
      possibleValidTo: x.possible_valid_to,
    }));
    await this.addMeasured(sessions);
    return { siteId, capW, connectorsAtSite, sessions };
  }

  /** Latest measured draw from Redis (written by telemetry-ingest). Redis trouble only drops this input. */
  private async addMeasured(sessions: ActiveSession[]): Promise<void> {
    if (!this.redis || sessions.length === 0) return;
    try {
      const pipe = this.redis.pipeline();
      for (const s of sessions) pipe.hgetall(RedisKeys.connectorPower(s.stationId, s.connectorId));
      const res = (await pipe.exec()) ?? [];
      const now = Date.now();
      res.forEach(([err, value], i) => {
        const s = sessions[i];
        if (err || !s) return;
        const w = parseMeasured(value as Record<string, string>, now);
        if (w !== undefined) s.measuredW = w;
      });
    } catch (err) {
      this.logger.debug({ err: errorMessage(err) }, 'measured power unavailable');
    }
  }

  /** Calls smart-charging (300 ms timeout, circuit breaker); any failure falls back to the static safe share. */
  async allocate(site: SiteState, computedAt: Date = new Date()): Promise<Allocation> {
    try {
      const res = await this.allocator.post('/v1/allocate', buildAllocateReq(site, computedAt), { retries: 0 });
      const v = validateAllocation(site, res.body);
      if (v.ok) return v.allocation;
      if (v.capViolation) m.capViolationsRejected.inc();
      m.allocatorFailures.inc({ reason: v.reason });
      this.logger.warn({ siteId: site.siteId, reason: v.reason }, 'allocator response rejected, using fallback');
    } catch (err) {
      const reason = err instanceof CircuitOpenError ? 'circuit_open' : err instanceof HttpTimeoutError ? 'timeout' : 'error';
      m.allocatorFailures.inc({ reason });
      if (reason !== 'circuit_open') this.logger.warn({ siteId: site.siteId, err: errorMessage(err) }, 'allocator call failed, using fallback');
    }
    return fallbackAllocation(site);
  }

  private async round(c: Claimed): Promise<RoundResult> {
    const t0 = performance.now();
    const site = await this.loadSite(c.site_id, c.cap_w);
    const triggered = c.requested_at !== null;
    if (site.sessions.length === 0) {
      let owned = true;
      await withTx(this.pool, async (tx) => {
        owned = await this.release(tx, c, 'none', 0);
        if (owned && triggered) await emitSessionEvent(tx, this.topics, 'site.power-allocated', site.siteId, { siteId: site.siteId, siteCapW: site.capW, strategy: 'optimal', allocations: [], computedAt: new Date().toISOString() });
      });
      if (owned) this.observe(c, 'none', 'idle', t0);
      return { siteId: site.siteId, strategy: 'none', sent: 0, deferred: 0, limits: new Map() };
    }
    const computedAt = new Date();
    const allocation = await this.allocate(site, computedAt);
    const validTo = new Date(computedAt.getTime() + VALIDITY_S * 1000);
    const plan = planProfiles(site, allocation, computedAt);
    let sent = 0;
    let owned = true;
    // Lock order matters: StartTransaction/StopTransaction touch active_sessions first and `sites` last (markSiteDue),
    // so a round must do the same (profiles first, lease release last) or the two deadlock.
    await withTx(this.pool, async (tx) => {
      if (plan.reason && plan.send.length) {
        const ids = plan.send.map((p) => p.session.sessionId);
        const cmdIds = ids.map(() => randomUUID());
        // Unconfirmed profiles that may still be in force stay in the envelope until they expire or are superseded.
        const keep = plan.send.map((p) => p.session.possibleW !== null && p.session.possibleValidTo !== null && p.session.possibleValidTo > computedAt);
        const possibleW = plan.send.map((p, i) => (keep[i] ? Math.max(p.session.possibleW as number, p.limitW) : p.limitW));
        const possibleTo = plan.send.map((p, i) => (keep[i] ? new Date(Math.max((p.session.possibleValidTo as Date).getTime(), validTo.getTime())) : validTo));
        const { rows } = await tx.query<{ session_id: string }>(
          `UPDATE active_sessions a SET limit_w = u.limit_w, limit_valid_to = $6, limit_sent_at = now(),
                  pending_command_id = u.cmd, possible_limit_w = u.plim, possible_valid_to = u.pto
             FROM unnest($1::uuid[], $2::int[], $3::uuid[], $4::int[], $5::timestamptz[]) AS u(session_id, limit_w, cmd, plim, pto)
            WHERE a.session_id = u.session_id
          RETURNING a.session_id`,
          [ids, plan.send.map((p) => p.limitW), cmdIds, possibleW, possibleTo, validTo],
        );
        const still = new Set(rows.map((r) => r.session_id));
        for (const [i, p] of plan.send.entries()) {
          if (!still.has(p.session.sessionId)) continue; // stopped meanwhile
          await enqueueCommand(
            tx,
            this.topics,
            p.session.stationId,
            'SetChargingProfile',
            setChargingProfilePayload(p.session, p.limitW, computedAt, validTo),
            { commandId: cmdIds[i], sessionId: p.session.sessionId, sessionStartedAt: p.session.startedAt, limitW: p.limitW, validTo },
            PROFILE_COMMAND_TTL_S,
          );
          sent++;
        }
      }
      await emitSessionEvent(tx, this.topics, 'site.power-allocated', site.siteId, {
        siteId: site.siteId,
        siteCapW: site.capW,
        strategy: allocation.strategy,
        allocations: site.sessions.map((s) => ({ sessionId: s.sessionId, stationId: s.stationId, connectorId: s.connectorId, limitW: allocation.limits.get(s.sessionId) ?? 0 })),
        computedAt: computedAt.toISOString(),
      });
      // ADR 0006: the gateway raises a connector to the safe share only after EVERY allocation profile of the
      // site has expired. Publish the latest validTo that may be in force before commit: a rollback leaves it too
      // high (safe, it only delays the raise); a Redis failure aborts the round so nothing is sent unannounced.
      const latest = Math.max(sent ? validTo.getTime() : 0, ...site.sessions.map((s) => Math.max(s.possibleValidTo?.getTime() ?? 0, s.confirmedValidTo?.getTime() ?? 0)));
      if (latest > 0 && this.redis) await this.redis.eval(RAISE_SITE_VALID_TO, 1, RedisKeys.siteAllocationValidTo(site.siteId), String(latest), String(SITE_VALID_TO_TTL_MS));
      if (!(await this.release(tx, c, allocation.strategy, plan.deferred))) throw new LeaseLostError();
    }).catch((err: unknown) => {
      if (!(err instanceof LeaseLostError)) throw err;
      owned = false; // rolled back: nothing was enqueued
    });
    if (!owned) return { siteId: site.siteId, strategy: 'none', sent: 0, deferred: 0, limits: new Map() };
    if (sent && plan.reason) m.profilesSent.inc({ reason: plan.reason }, sent);
    if (plan.deferred) m.increasesDeferred.inc(plan.deferred);
    this.observe(c, allocation.strategy, triggered ? 'trigger' : 'periodic', t0);
    return { siteId: site.siteId, strategy: allocation.strategy, sent, deferred: plan.deferred, limits: allocation.limits };
  }

  /** Ends the lease in the round's transaction; false if the lease was lost (round took longer than LEASE_S). */
  private async release(tx: { query: Pool['query'] }, c: Claimed, strategy: string, deferred: number): Promise<boolean> {
    const r = await tx.query(
      'UPDATE sites SET alloc_lease_token = NULL, alloc_lease_until = NULL, last_alloc_at = now(), last_alloc_strategy = $3, alloc_deferred = $4 WHERE site_id = $1 AND alloc_lease_token = $2',
      [c.site_id, c.token, strategy, deferred > 0],
    );
    if (r.rowCount === 1) return true;
    this.logger.warn({ siteId: c.site_id }, 'allocation lease lost, round discarded');
    return false;
  }

  private observe(c: Claimed, strategy: string, reason: string, t0: number): void {
    m.allocationRounds.inc({ strategy, reason });
    m.allocationSeconds.observe((performance.now() - t0) / 1000);
    if (c.requested_at) m.reallocationLatency.observe(Math.max(0, Date.now() - c.requested_at.getTime()) / 1000);
  }
}
