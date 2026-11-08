/**
 * ADR 0015 against a real hot standby. Skipped unless TEST_PG_REPLICA_URL is set (make sets it for `make test-int`).
 * The temporary database created on the primary is physically replicated, so the same database name exists on the replica.
 */
import { createPgPool, type Pool } from '@evcharge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { ReadRouter } from '../../src/read-router';
import { tempDatabase, waitFor } from './harness';

const REPLICA_URL = process.env['TEST_PG_REPLICA_URL'];
const WAIT_MS = 200;

const replicaUrl = (name: string) => {
  const u = new URL(REPLICA_URL as string);
  u.pathname = `/${name}`;
  return u.toString();
};
const readRow = (id: string) => (pool: Pool) =>
  pool.query<{ status: string; replica: boolean }>('SELECT status, pg_is_in_recovery() AS replica FROM session_probe WHERE session_id = $1', [id]).then((r) => r.rows[0]);

describe.skipIf(!REPLICA_URL)('sessions read replica and read-your-writes (ADR 0015)', () => {
  let db: Awaited<ReturnType<typeof tempDatabase>>;
  let replica: Pool;
  let adminReplica: Pool;
  let router: ReadRouter;

  /** Inserts a row on the primary and returns the LSN a write response would carry. */
  async function write(id: string): Promise<string> {
    await db.pool.query('INSERT INTO session_probe (session_id, status) VALUES ($1, $2)', [id, 'Active']);
    return (await db.pool.query<{ lsn: string }>('SELECT pg_current_wal_lsn()::text AS lsn')).rows[0]?.lsn as string;
  }
  beforeAll(async () => {
    db = await tempDatabase('replica_ryw');
    await db.pool.query('CREATE TABLE session_probe (session_id text PRIMARY KEY, status text NOT NULL)');
    adminReplica = createPgPool(replicaUrl('postgres'), { max: 2 });
    // Wait for CREATE DATABASE and CREATE TABLE to be replayed before connecting to the temporary database.
    replica = createPgPool(replicaUrl(db.name), { max: 5 });
    await waitFor(async () => (await replica.query("SELECT to_regclass('session_probe') AS t")).rows[0]?.t, { timeoutMs: 30_000, intervalMs: 200 });
    router = new ReadRouter(db.pool, replica, { waitMs: WAIT_MS, pollMs: 10 });
  });

  afterAll(async () => {
    await adminReplica?.query('SELECT pg_wal_replay_resume()').catch(() => {});
    await replica?.end().catch(() => {});
    await adminReplica?.end().catch(() => {});
    await db?.drop();
  });

  it('serves a read-your-writes read from the replica once it has caught up', async () => {
    const lsn = await write('s-caught-up');
    const row = await router.read(readRow('s-caught-up'), lsn);
    expect(row).toMatchObject({ status: 'Active', replica: true });
  });

  it('falls back to the primary within the timeout while replay is paused, and a stale read misses the row', async () => {
    await adminReplica.query('SELECT pg_wal_replay_pause()');
    try {
      const lsn = await write('s-paused');
      const t0 = Date.now();
      const row = await router.read(readRow('s-paused'), lsn);
      const elapsed = Date.now() - t0;
      expect(row).toMatchObject({ status: 'Active', replica: false });
      expect(elapsed).toBeGreaterThanOrEqual(WAIT_MS - 20);
      expect(elapsed).toBeLessThan(WAIT_MS + 800);
      // No x-min-lsn: stale-tolerant, served by the paused replica, which has not seen the row.
      expect(await router.read(readRow('s-paused'))).toBeUndefined();
    } finally {
      await adminReplica.query('SELECT pg_wal_replay_resume()');
    }
  });

  it('serves reads from the replica again after replay resumes', async () => {
    const lsn = await write('s-resumed');
    const row = await router.read(readRow('s-resumed'), lsn);
    expect(row).toMatchObject({ status: 'Active', replica: true });
  });
});
