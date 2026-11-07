import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createPgPool, pgReadinessCheck, runMigrations, withTx } from '../../src/postgres/postgres';
import { tempDatabase } from './infra';

let db: Awaited<ReturnType<typeof tempDatabase>>;
let dir: string;

beforeAll(async () => {
  db = await tempDatabase();
  dir = await mkdtemp(join(tmpdir(), 'migr-'));
  await writeFile(join(dir, '0001_create_a.sql'), 'CREATE TABLE a (id int PRIMARY KEY, v text);');
  await writeFile(join(dir, '0002_seed.sql'), "INSERT INTO a VALUES (1, 'one');");
});
afterAll(async () => {
  await db.drop();
  await rm(dir, { recursive: true, force: true });
});

describe('postgres', () => {
  it('pool readiness check passes, and fails for a bad server', async () => {
    await expect(pgReadinessCheck(db.pool)()).resolves.toBeUndefined();
    const bad = createPgPool('postgres://x:y@127.0.0.1:1/none', { connectTimeoutMs: 500 });
    await expect(pgReadinessCheck(bad)()).rejects.toThrow(/ECONNREFUSED|timeout|connect/i);
    await bad.end();
  });

  it('sets statement_timeout on sessions', async () => {
    const p = createPgPool(db.url, { statementTimeoutMs: 100 });
    const { rows } = await p.query<{ statement_timeout: string }>('SHOW statement_timeout');
    expect(rows[0]?.statement_timeout).toBe('100ms');
    await expect(p.query('SELECT pg_sleep(1)')).rejects.toThrow(/statement timeout/);
    await p.end();
  });

  it('applies migrations in order, once, even when run concurrently', async () => {
    const other = createPgPool(db.url);
    const [x, y] = await Promise.all([runMigrations(db.pool, dir), runMigrations(other, dir)]);
    expect([...x, ...y].sort()).toEqual(['0001_create_a.sql', '0002_seed.sql']);
    expect(await runMigrations(db.pool, dir)).toEqual([]);
    const { rows } = await db.pool.query('SELECT version, name FROM schema_migrations ORDER BY version');
    expect(rows).toEqual([
      { version: 1, name: 'create_a' },
      { version: 2, name: 'seed' },
    ]);
    await other.end();
  });

  it('a failing migration rolls back entirely and stops the run', async () => {
    await writeFile(join(dir, '0003_bad.sql'), "INSERT INTO a VALUES (3, 'three'); SELECT * FROM missing_table;");
    await expect(runMigrations(db.pool, dir)).rejects.toThrow(/0003_bad.sql failed/);
    expect((await db.pool.query('SELECT 1 FROM a WHERE id = 3')).rowCount).toBe(0);
    await writeFile(join(dir, '0003_bad.sql'), "INSERT INTO a VALUES (3, 'three');");
    expect(await runMigrations(db.pool, dir)).toEqual(['0003_bad.sql']);
  });

  it('detects edited migrations and bad file names', async () => {
    await writeFile(join(dir, '0001_create_a.sql'), 'CREATE TABLE a (id bigint PRIMARY KEY);');
    await expect(runMigrations(db.pool, dir)).rejects.toThrow(/checksum mismatch/);
    await writeFile(join(dir, '0001_create_a.sql'), 'CREATE TABLE a (id int PRIMARY KEY, v text);');
    await writeFile(join(dir, 'oops.sql'), 'SELECT 1');
    await expect(runMigrations(db.pool, dir)).rejects.toThrow(/bad migration file name/);
    await rm(join(dir, 'oops.sql'));
  });

  it('withTx commits on success and rolls back on error', async () => {
    const r = await withTx(db.pool, async (c) => {
      await c.query("INSERT INTO a VALUES (10, 'ten')");
      return 'done';
    });
    expect(r).toBe('done');
    await expect(
      withTx(db.pool, async (c) => {
        await c.query("INSERT INTO a VALUES (11, 'eleven')");
        throw new Error('boom');
      }),
    ).rejects.toThrow('boom');
    const { rows } = await db.pool.query('SELECT id FROM a WHERE id >= 10 ORDER BY id');
    expect(rows).toEqual([{ id: 10 }]);
    const iso = await withTx(db.pool, async (c) => (await c.query<{ transaction_isolation: string }>('SHOW transaction_isolation')).rows[0], { isolation: 'SERIALIZABLE' });
    expect(iso?.transaction_isolation).toBe('serializable');
  });
});
