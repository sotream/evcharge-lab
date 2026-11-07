import { randomBytes } from 'node:crypto';
import { Admin } from '@platformatic/kafka';
import { Client } from 'pg';
import { createPgPool, type Pool } from '../../src/postgres/postgres';

export const PG_URL = process.env['TEST_PG_URL'] ?? 'postgres://evcharge:evcharge@localhost:25432';
export const REDIS_URL = process.env['TEST_REDIS_URL'] ?? 'redis://localhost:26379';
/** Comma-separated list; any one broker is enough to bootstrap. */
export const KAFKA_BROKERS = (process.env['TEST_KAFKA_BROKERS'] ?? 'localhost:29091,localhost:29092,localhost:29093')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

export const uniq = (prefix: string) => `${prefix}_${Date.now().toString(36)}_${randomBytes(3).toString('hex')}`;

function dbUrl(name: string): string {
  const u = new URL(PG_URL);
  u.pathname = `/${name}`;
  return u.toString();
}

/** Creates a fresh database; `drop()` terminates connections and drops it. */
export async function tempDatabase(prefix = 'platform_test'): Promise<{ name: string; url: string; pool: Pool; drop: () => Promise<void> }> {
  const name = uniq(prefix).toLowerCase();
  const admin = new Client({ connectionString: dbUrl('postgres') });
  await admin.connect();
  await admin.query(`CREATE DATABASE "${name}"`);
  await admin.end();
  const url = dbUrl(name);
  const pool = createPgPool(url, { max: 10 });
  return {
    name,
    url,
    pool,
    drop: async () => {
      await pool.end().catch(() => {});
      const a = new Client({ connectionString: dbUrl('postgres') });
      await a.connect();
      await a.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
      await a.end();
    },
  };
}

/** Topics are not auto-created on the test cluster: create them explicitly (RF = min(3, brokers)). */
export async function createTopics(names: string[], partitions = 3): Promise<() => Promise<void>> {
  const admin = new Admin({ clientId: uniq('admin'), bootstrapBrokers: KAFKA_BROKERS });
  try {
    const meta = await admin.metadata({ topics: [] });
    const replicas = Math.min(3, meta.brokers.size);
    await admin.createTopics({ topics: names, partitions, replicas });
  } finally {
    await admin.close();
  }
  return async () => {
    const a = new Admin({ clientId: uniq('admin'), bootstrapBrokers: KAFKA_BROKERS });
    await a.deleteTopics({ topics: names }).catch(() => {});
    await a.close();
  };
}
