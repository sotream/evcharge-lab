/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (ordering, retries, backoff)
import { events } from '@evcharge/contracts';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createConsumer, type KafkaConsumer } from '../../src/kafka/consumer';
import { makeEnvelope } from '../../src/kafka/envelope';
import { createProducer, type KafkaProducer } from '../../src/kafka/producer';
import { registry } from '../../src/metrics/metrics';
import { enqueueOutbox, OUTBOX_DDL, OUTBOX_RELAY_LOCK_KEY, OutboxRelay } from '../../src/outbox/outbox';
import { withTx } from '../../src/postgres/postgres';
import { injectTraceHeaders } from '../../src/telemetry/propagation';
import { captureLogger, sample, spanContext, TRACE_ID, waitFor } from '../helpers';
import { createTopics, KAFKA_BROKERS, tempDatabase, uniq } from './infra';

let db: Awaited<ReturnType<typeof tempDatabase>>;
let producer: KafkaProducer;
let topic: string;
let dropTopics: () => Promise<void>;
let consumer: KafkaConsumer;
const received: { eventId: string; gw: string; traceparent: string | undefined }[] = [];
const relays: OutboxRelay[] = [];
const { logger } = captureLogger('outbox-test', 'warn');

const env = (n: number) => makeEnvelope('station.connected', `CP-${n % 5}`, { stationId: `CP-${n % 5}`, gatewayInstance: `gw-${n}` }, 'test');
const pending = async () => Number((await db.pool.query<{ n: string }>('SELECT count(*) AS n FROM outbox WHERE published_at IS NULL')).rows[0]?.n);

beforeAll(async () => {
  db = await tempDatabase();
  await db.pool.query(OUTBOX_DDL);
  topic = uniq('platform.outbox').replace(/_/g, '-');
  dropTopics = await createTopics([topic, `${topic}.dlq`]);
  producer = createProducer({ brokers: KAFKA_BROKERS, clientId: uniq('outbox-producer') });
  consumer = createConsumer({
    brokers: KAFKA_BROKERS,
    groupId: uniq('outbox-reader'),
    topics: [topic],
    schema: events.StationConnected,
    lagIntervalMs: 0,
    handler: async (e, ctx) => void received.push({ eventId: e.eventId, gw: e.data.gatewayInstance, traceparent: ctx.headers['traceparent'] }),
  });
  await consumer.start();
});
afterAll(async () => {
  await Promise.allSettled(relays.map((r) => r.stop()));
  await consumer.stop();
  await producer.close();
  await dropTopics();
  await db.drop();
});

const relay = (o: Partial<ConstructorParameters<typeof OutboxRelay>[0]> = {}) => {
  const r = new OutboxRelay({ pool: db.pool, producer, logger, cleanupIntervalMs: 0, ...o });
  relays.push(r);
  return r;
};

describe('outbox', () => {
  it('enqueues only when the business transaction commits; captures traceparent', async () => {
    const committed = env(1);
    await withTx(db.pool, (c) => enqueueOutbox(c, { topic, event: committed, headers: injectTraceHeaders({}, spanContext()) }));
    await expect(
      withTx(db.pool, async (c) => {
        await enqueueOutbox(c, { topic, event: env(2) });
        throw new Error('rollback');
      }),
    ).rejects.toThrow('rollback');
    const { rows } = await db.pool.query('SELECT topic, key, payload, headers, published_at FROM outbox');
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ topic, key: 'CP-1', payload: committed, published_at: null });
    expect(rows[0].headers['traceparent']).toContain(TRACE_ID);
    expect(rows[0].headers['event-id']).toBe(committed.eventId);

    expect(await relay({ listen: false }).runOnce()).toBe(1);
    await waitFor(() => received.find((r) => r.eventId === committed.eventId));
    expect(received.find((r) => r.eventId === committed.eventId)?.traceparent).toContain(TRACE_ID);
    expect(await pending()).toBe(0);
  });

  it('several relays publish every row exactly once, in per-key order (single active relay)', async () => {
    received.length = 0;
    const N = 300;
    for (let i = 0; i < N; i += 50) {
      await withTx(db.pool, async (c) => {
        for (let n = i; n < i + 50; n++) await enqueueOutbox(c, { topic, event: env(n) });
      });
    }
    const rs = [relay({ batchSize: 20, listen: false, pollIntervalMs: 50 }), relay({ batchSize: 20, listen: false, pollIntervalMs: 50 }), relay({ batchSize: 20, listen: false, pollIntervalMs: 50 })];
    await Promise.all(rs.map((r) => r.start()));
    await waitFor(async () => (await pending()) === 0, { timeoutMs: 30_000 });
    await waitFor(() => received.length >= N, { timeoutMs: 30_000 });
    await new Promise((r) => setTimeout(r, 500));
    expect(received).toHaveLength(N);
    expect(new Set(received.map((r) => r.eventId)).size).toBe(N);
    const byKey = new Map<string, number[]>();
    for (const r of received) {
      const n = Number(r.gw.slice(3));
      const k = `CP-${n % 5}`;
      byKey.set(k, [...(byKey.get(k) ?? []), n]);
    }
    for (const seq of byKey.values()) expect(seq).toEqual([...seq].sort((a, b) => a - b));
    await Promise.all(rs.map((r) => r.stop()));
    const text = await registry.metrics();
    expect(sample(text, 'outbox_published_total', { topic })).toBe(N + 1);
    expect(sample(text, 'outbox_publish_lag_seconds_count', {})).toBeGreaterThanOrEqual(N);
  });

  it('a committed row is published within a few poll intervals (no NOTIFY in the commit path)', async () => {
    const { rows } = await db.pool.query("SELECT count(*)::int AS n FROM pg_trigger WHERE tgname = 'outbox_notify_trg'");
    expect(rows[0].n).toBe(0);
    const r = relay({ pollIntervalMs: 50 });
    await r.start();
    await new Promise((res) => setTimeout(res, 300)); // let the first cycle go idle
    const e = env(999);
    const t0 = Date.now();
    await withTx(db.pool, (c) => enqueueOutbox(c, { topic, event: e }));
    await waitFor(() => received.find((x) => x.eventId === e.eventId), { timeoutMs: 10_000 });
    expect(Date.now() - t0).toBeLessThan(1000);
    await r.stop();
  });

  it('keeps rows when Kafka rejects the batch, and cleans up old published rows', async () => {
    const before = await pending();
    await withTx(db.pool, (c) => enqueueOutbox(c, { topic: uniq('missing-topic'), event: env(5) }));
    const badProducer = createProducer({ brokers: KAFKA_BROKERS, clientId: uniq('p'), timeoutMs: 1000, sendTimeoutMs: 1500 });
    const failing = relay({ listen: false, producer: badProducer });
    await expect(failing.runOnce()).rejects.toThrow(/timed out|topic|unknown/i);
    expect(await pending()).toBe(before + 1);
    await badProducer.close();
    await db.pool.query('DELETE FROM outbox WHERE published_at IS NULL');

    await db.pool.query("UPDATE outbox SET published_at = now() - interval '2 days' WHERE id IN (SELECT id FROM outbox ORDER BY id LIMIT 10)");
    expect(await relay({ retentionMs: 86_400_000 }).cleanup()).toBe(10);
    expect(await relay().refreshPending()).toBe(0);
  });
});

describe('outbox commit order', () => {
  it('does not publish a later-committed row before an earlier-started one with the same key', async () => {
    received.length = 0;
    const early = env(10_005); // key CP-0
    const late = env(10_010); // key CP-0
    const t1 = await db.pool.connect();
    try {
      await t1.query('BEGIN');
      await t1.query('SELECT pg_current_xact_id()'); // T1 takes its transaction id first
      await withTx(db.pool, (c) => enqueueOutbox(c, { topic, event: late })); // T2 commits first
      await enqueueOutbox(t1, { topic, event: early });
      const r = relay({ listen: false });
      expect(await r.runOnce()).toBe(0); // T2's row waits: T1 (older) is still running
      await t1.query('COMMIT');
      expect(await r.runOnce()).toBe(2);
    } finally {
      t1.release();
    }
    await waitFor(() => received.length >= 2);
    expect(received.map((x) => x.eventId)).toEqual([early.eventId, late.eventId]);
  });

  it('a second relay is a no-op while the first holds the lock', async () => {
    await withTx(db.pool, (c) => enqueueOutbox(c, { topic, event: env(20_000) }));
    const holder = await db.pool.connect();
    try {
      await holder.query('BEGIN');
      await holder.query('SELECT pg_advisory_xact_lock($1)', [OUTBOX_RELAY_LOCK_KEY]);
      expect(await relay({ listen: false }).runOnce()).toBe(0);
      await holder.query('COMMIT');
    } finally {
      holder.release();
    }
    expect(await relay({ listen: false }).runOnce()).toBe(1);
  });
});
