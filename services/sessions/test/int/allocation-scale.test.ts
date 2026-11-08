/* oxlint-disable no-await-in-loop */
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { registry } from '@evcharge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createSessionsApp, type SessionsApp } from '../../src/app';
import { readConfig } from '../../src/config';
import { KAFKA_BROKERS, REDIS_URL, SEED, startAllocator, tempDatabase, TOKEN, waitFor, type Allocator } from './harness';

// ADR 0000: 5,000 sites must be cycled once per 60 s. Two replicas, one shared database, stub allocator.
const SITES = 5000;
const SESSIONS_PER_SITE = 3;
const PERIOD_S = 60;

let db: Awaited<ReturnType<typeof tempDatabase>>;
let allocator: Allocator;
const apps: SessionsApp[] = [];

beforeAll(async () => {
  db = await tempDatabase('sessions_scale');
  allocator = await startAllocator();
  const config = readConfig({
    SERVICE_NAME: 'sessions',
    LOG_LEVEL: 'error',
    INTERNAL_TOKEN: TOKEN,
    DATABASE_URL: db.url,
    KAFKA_BROKERS: KAFKA_BROKERS.join(','),
    REDIS_URL,
    SMART_CHARGING_URL: allocator.url,
    STATION_SECRET_SEED: SEED,
    SEED_SITES: String(SITES),
    SEED_STATIONS_PER_SITE: '10',
    SEED_DC_SHARE: '0.2',
    ALLOCATION_PERIOD_S: String(PERIOD_S),
    ALLOCATION_TICK_MS: '100',
    PG_POOL_MAX: '12',
    HTTP_PORT: '0',
    SHUTDOWN_GRACE_MS: '5000',
  });
  // workers are off: allocation is started explicitly, and nothing relays the outbox
  for (let i = 0; i < 2; i++) apps.push(await createSessionsApp({ config, port: 0, handleSignals: false, workers: false }));
}, 240_000);

afterAll(async () => {
  for (const a of apps) await a.allocation.stop().catch(() => {});
  for (const a of apps) await a.shutdown('test').catch(() => {});
  await allocator?.close();
  await db?.drop();
});

// Wall-clock benchmark: only meaningful on an otherwise idle machine, so it runs on demand (BENCH=1).
describe.skipIf(process.env['BENCH'] !== '1')('allocation throughput (H2)', () => {
  it(`cycles ${SITES} sites with ${SITES * SESSIONS_PER_SITE} active sessions within ${PERIOD_S} s using 2 instances`, async () => {
    await db.pool.query(
      `INSERT INTO active_sessions (transaction_id, session_id, started_at, site_id, station_id, connector_id, connector_pk, max_w, min_w)
       SELECT nextval('transaction_id_seq'), uuidv7(), now(), c.site_id, c.station_id, c.connector_id, c.connector_pk, c.max_power_w, c.min_power_w
         FROM connectors c WHERE c.connector_id = 1 AND right(c.station_id, 3) IN ('001', '002', '003')`,
    );
    const sessions = Number((await db.pool.query('SELECT count(*) AS n FROM active_sessions')).rows[0].n);
    expect(sessions).toBe(SITES * SESSIONS_PER_SITE);
    // every site is due now, as after an outage
    await db.pool.query("UPDATE sites SET alloc_due_at = now() - interval '1 second', alloc_requested_at = NULL, last_alloc_at = NULL");
    const callsBefore = allocator.calls;
    const t0 = Date.now();
    for (const a of apps) a.allocation.start();
    await waitFor(async () => Number((await db.pool.query('SELECT count(*) AS n FROM sites WHERE last_alloc_at IS NULL')).rows[0].n) === 0, { timeoutMs: 120_000, intervalMs: 250 });
    const elapsedS = (Date.now() - t0) / 1000;
    const commands = Number((await db.pool.query("SELECT count(*) AS n FROM commands WHERE action = 'SetChargingProfile'")).rows[0].n);
    const backlog = /sessions_allocation_backlog_seconds\{[^}]*\} ([\d.e+-]+)/.exec(await registry.metrics())?.[1];
    for (const a of apps) await a.allocation.stop();

    const result = {
      measured: {
        sites: SITES,
        activeSessions: sessions,
        instances: apps.length,
        concurrencyPerInstance: 8,
        elapsedSeconds: elapsedS,
        roundsPerSecond: SITES / elapsedS,
        allocatorCalls: allocator.calls - callsBefore,
        setChargingProfileCommands: commands,
        backlogGaugeSecondsLastSample: backlog === undefined ? null : Number(backlog),
      },
      budgetSeconds: PERIOD_S,
      note: 'stub allocator on localhost, Postgres/Redis on the compose infra, workers off (no outbox relay); wall clock from start() to every site having completed a round',
      measuredAt: new Date().toISOString(),
    };
    if (process.env['WRITE_BENCH']) {
      const dir = join(__dirname, '..', '..', '..', '..', 'docs', 'load-output');
      mkdirSync(dir, { recursive: true });
      writeFileSync(join(dir, 'sessions-allocation-rounds.json'), `${JSON.stringify(result, null, 2)}\n`);
    }
    expect(elapsedS).toBeLessThan(PERIOD_S);
    expect(allocator.calls - callsBefore).toBeGreaterThanOrEqual(SITES);
  }, 240_000);
});
