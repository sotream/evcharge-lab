/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (ordering, retries, backoff)
import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { type ClientBase, Pool as PgPool, type PoolClient as PgPoolClient } from 'pg';
import type { Logger } from '../logging/logger';

export type Pool = PgPool;
export type PoolClient = PgPoolClient;
/** Anything with `query` — a Pool, a PoolClient or a Client. */
export type Queryable = Pick<ClientBase, 'query'>;

export interface PgPoolOptions {
  max?: number;
  /** Server-side statement_timeout for every session. Default 5000ms. */
  statementTimeoutMs?: number;
  /** Time to wait for a new connection. Default 3000ms. */
  connectTimeoutMs?: number;
  idleTimeoutMs?: number;
  /** Server-side idle_in_transaction_session_timeout. Default 30000ms. */
  idleInTxTimeoutMs?: number;
  applicationName?: string;
  logger?: Logger;
}

/**
 * pg Pool with statement_timeout, connect timeout and idle-in-transaction timeout set, and an
 * 'error' listener so a dropped idle connection doesn't crash the process.
 */
export function createPgPool(url: string, opts: PgPoolOptions = {}): Pool {
  const pool = new PgPool({
    connectionString: url,
    max: opts.max ?? 10,
    statement_timeout: opts.statementTimeoutMs ?? 5000,
    idle_in_transaction_session_timeout: opts.idleInTxTimeoutMs ?? 30_000,
    connectionTimeoutMillis: opts.connectTimeoutMs ?? 3000,
    idleTimeoutMillis: opts.idleTimeoutMs ?? 30_000,
    ...(opts.applicationName ? { application_name: opts.applicationName } : {}),
  });
  pool.on('error', (err) => opts.logger?.error({ err: err.message }, 'idle pg client error'));
  return pool;
}

/** Readiness check: `SELECT 1` through the pool. */
export function pgReadinessCheck(pool: Pool): () => Promise<void> {
  return async () => {
    await pool.query('SELECT 1');
  };
}

export type IsolationLevel = 'READ COMMITTED' | 'REPEATABLE READ' | 'SERIALIZABLE';

/** Runs fn in BEGIN/COMMIT on one pooled client; rolls back and rethrows on error. */
export async function withTx<T>(pool: Pool, fn: (client: PoolClient) => Promise<T>, opts: { isolation?: IsolationLevel } = {}): Promise<T> {
  const client = await pool.connect();
  let broken: Error | undefined;
  // A checked-out client that dies (e.g. idle_in_transaction_session_timeout) emits 'error';
  // without a listener that would be an uncaught exception. The pending query rejects anyway.
  const onError = (err: Error) => {
    broken = err;
  };
  client.on('error', onError);
  try {
    await client.query(opts.isolation ? `BEGIN ISOLATION LEVEL ${opts.isolation}` : 'BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    if (!broken) {
      try {
        await client.query('ROLLBACK');
      } catch (rbErr) {
        broken = rbErr as Error;
      }
    }
    throw err;
  } finally {
    client.removeListener('error', onError);
    client.release(broken);
  }
}

const MIGRATION_RE = /^(\d{4})_([\w.-]+)\.sql$/;
/** Advisory lock key used while migrating (any 64-bit constant; shared by all services is fine per-DB). */
export const MIGRATION_LOCK_KEY = 72_616_001n;

export interface Migration {
  version: number;
  name: string;
  file: string;
  sql: string;
  checksum: string;
}

export async function readMigrations(dir: string): Promise<Migration[]> {
  const files = (await readdir(dir)).filter((f) => f.endsWith('.sql')).sort();
  const out: Migration[] = [];
  for (const file of files) {
    const m = MIGRATION_RE.exec(file);
    if (!m) throw new Error(`bad migration file name "${file}" (expected NNNN_name.sql)`);
    const sql = await readFile(join(dir, file), 'utf8');
    const version = Number(m[1]);
    if (out.some((x) => x.version === version)) throw new Error(`duplicate migration version ${m[1]}`);
    out.push({ version, name: m[2] ?? file, file, sql, checksum: createHash('sha256').update(sql).digest('hex') });
  }
  return out;
}

/**
 * Applies pending `NNNN_name.sql` files from `dir` in version order. Each file runs in its own
 * transaction together with its `schema_migrations` row; the whole run holds a pg advisory lock
 * so concurrent service instances migrate one at a time. A changed checksum of an already-applied
 * file is an error. Returns the files applied by this call.
 * (Statements that cannot run in a transaction, e.g. CREATE INDEX CONCURRENTLY, are not supported.)
 */
export async function runMigrations(pool: Pool, dir: string, opts: { logger?: Logger } = {}): Promise<string[]> {
  const migrations = await readMigrations(dir);
  const client = await pool.connect();
  const applied: string[] = [];
  try {
    await client.query('SET statement_timeout = 0');
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_KEY.toString()]);
    try {
      await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
        version integer PRIMARY KEY,
        name text NOT NULL,
        checksum text NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now()
      )`);
      const { rows } = await client.query<{ version: number; checksum: string }>('SELECT version, checksum FROM schema_migrations');
      const done = new Map(rows.map((r) => [r.version, r.checksum]));
      for (const m of migrations) {
        const prev = done.get(m.version);
        if (prev !== undefined) {
          if (prev !== m.checksum) throw new Error(`migration ${m.file} was modified after being applied (checksum mismatch)`);
          continue;
        }
        await client.query('BEGIN');
        try {
          await client.query(m.sql);
          await client.query('INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)', [m.version, m.name, m.checksum]);
          await client.query('COMMIT');
        } catch (err) {
          await client.query('ROLLBACK');
          throw new Error(`migration ${m.file} failed: ${(err as Error).message}`, { cause: err });
        }
        applied.push(m.file);
        opts.logger?.info({ migration: m.file }, 'migration applied');
      }
    } finally {
      await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_KEY.toString()]);
    }
  } finally {
    await client.query('RESET statement_timeout').catch(() => {});
    client.release();
  }
  return applied;
}
