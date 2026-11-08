import { createConsumer } from '@evcharge/platform';
import { Client } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { z } from 'zod';
import { completedEvent, createHarness, driverIdOf, type Harness, sessionIdOf, startedEvent } from './harness';
import { KAFKA_BROKERS, PG_URL, startStubTariff, type StubTariff, uniq, waitFor } from './infra';

const count = async (h: Harness, sql: string, params: unknown[] = []) => Number((await h.db.pool.query<{ n: string }>(sql, params)).rows[0]?.n);

describe('Postgres outage', () => {
  let tariff: StubTariff;
  let h: Harness;
  beforeAll(async () => {
    tariff = await startStubTariff();
    h = await createHarness(tariff.url, 3);
  });
  afterAll(async () => {
    await h?.close();
    await tariff?.close();
  });

  it('keeps retrying a session.completed handled during a 10 s outage: invoiced afterwards, nothing dead-lettered', async () => {
    const n = 1;
    await h.db.pool.query('INSERT INTO payment_methods (driver_id, token) VALUES ($1, $2)', [driverIdOf(n), 'tok_outage']);
    const dlq: unknown[] = [];
    const dlqReader = createConsumer({
      brokers: KAFKA_BROKERS,
      groupId: uniq('dlq-reader'),
      topics: [`${h.sessionTopic}.dlq`],
      schema: z.unknown(),
      lagIntervalMs: 0,
      handler: async (e) => void dlq.push(e),
    });
    await dlqReader.start();
    await h.startInstance();
    await h.publishSession(startedEvent(n, driverIdOf(n)));
    await waitFor(async () => (await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE state = 'AUTHORIZED'")) === 1, { what: 'session started and authorized' });

    // Outage without touching the shared container: refuse new connections to this database and
    // keep killing the existing ones. The consumer's handler fails against the unreachable database.
    const url = new URL(PG_URL);
    url.pathname = '/postgres';
    const admin = new Client({ connectionString: url.toString() });
    await admin.connect();
    await admin.query(`ALTER DATABASE "${h.db.name}" WITH ALLOW_CONNECTIONS false`);
    const killer = setInterval(() => void admin.query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1', [h.db.name]).catch(() => {}), 200);
    try {
      await h.publishSession(completedEvent(n, driverIdOf(n), 8_000));
      await new Promise((r) => setTimeout(r, 10_000));
    } finally {
      clearInterval(killer);
      await admin.query(`ALTER DATABASE "${h.db.name}" WITH ALLOW_CONNECTIONS true`);
      await admin.end();
    }

    await waitFor(async () => (await count(h, "SELECT count(*) AS n FROM billing_sagas WHERE state = 'PAID'")) === 1, { what: 'PAID after the outage', timeoutMs: 60_000 });
    expect(await count(h, 'SELECT count(*) AS n FROM invoices WHERE session_id = $1', [sessionIdOf(n)])).toBe(1);
    expect(await count(h, 'SELECT count(*) AS n FROM payments')).toBe(1);
    await new Promise((r) => setTimeout(r, 1000));
    expect(dlq).toHaveLength(0);
    await dlqReader.stop();
  });
});
