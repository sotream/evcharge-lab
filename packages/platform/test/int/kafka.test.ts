/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (ordering, retries, backoff)
import { events } from '@evcharge/contracts';
import { Consumer, deserializersFrom, stringDeserializer } from '@platformatic/kafka';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createConsumer, type KafkaConsumer } from '../../src/kafka/consumer';
import { makeEnvelope } from '../../src/kafka/envelope';
import { createProducer, type KafkaProducer } from '../../src/kafka/producer';
import { registry } from '../../src/metrics/metrics';
import { injectTraceHeaders } from '../../src/telemetry/propagation';
import { captureLogger, sample, spanContext, TRACE_ID, waitFor } from '../helpers';
import { createTopics, KAFKA_BROKERS, uniq } from './infra';

type StationConnected = typeof events.StationConnected._output;

const connected = (stationId: string, n: number) => makeEnvelope('station.connected', stationId, { stationId, gatewayInstance: `gw-${n}` }, 'test');

let producer: KafkaProducer;
const cleanups: (() => Promise<void>)[] = [];
const consumers: KafkaConsumer[] = [];
const { logger } = captureLogger('kafka-test', 'warn');

async function topic(partitions = 3): Promise<string> {
  const t = uniq('platform.test').replace(/_/g, '-');
  cleanups.push(await createTopics([t, `${t}.dlq`], partitions));
  return t;
}

function consumer(opts: Partial<Parameters<typeof createConsumer<typeof events.StationConnected>>[0]> & { topics: string[]; handler: (e: StationConnected, ctx: { partition: number; headers: Record<string, string>; attempt: number }) => Promise<void> }) {
  const c = createConsumer({
    brokers: KAFKA_BROKERS,
    groupId: uniq('grp'),
    schema: events.StationConnected,
    retryBackoffMs: 10,
    retryBackoffMaxMs: 50,
    lagIntervalMs: 0,
    logger,
    ...opts,
  });
  consumers.push(c);
  return c;
}

/** Reads the first `n` records of a topic with a bare client (no platform logic involved). */
async function readRaw(t: string, n: number): Promise<{ key: string | undefined; value: string | undefined; headers: Record<string, string> }[]> {
  const raw = new Consumer<string, string, string, string>({ clientId: uniq('raw'), groupId: uniq('raw'), bootstrapBrokers: KAFKA_BROKERS, deserializers: deserializersFrom(stringDeserializer) });
  const stream = await raw.consume({ topics: [t], mode: 'earliest', autocommit: false, maxWaitTime: 300 });
  const out: { key: string | undefined; value: string | undefined; headers: Record<string, string> }[] = [];
  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`read ${out.length}/${n} records from ${t}`)), 30_000);
      stream.on('data', (m) => {
        out.push({ key: m.key, value: m.value, headers: Object.fromEntries(m.headers) });
        if (out.length >= n) {
          clearTimeout(timer);
          resolve();
        }
      });
    });
  } finally {
    await stream.close();
    await raw.close();
  }
  return out;
}

beforeAll(() => {
  producer = createProducer({ brokers: KAFKA_BROKERS, clientId: uniq('producer') });
});
afterAll(async () => {
  await Promise.allSettled(consumers.map((c) => c.stop()));
  await producer.close();
  await Promise.allSettled(cleanups.map((c) => c()));
});

describe('kafka producer + consumer', () => {
  it('producer readiness reaches the cluster', async () => {
    await expect(producer.readinessCheck()()).resolves.toBeUndefined();
  });

  it('delivers validated envelopes with key and traceparent, preserving per-key order', async () => {
    const t = await topic();
    const got: { key: string; n: number; traceparent?: string }[] = [];
    const c = consumer({
      topics: [t],
      concurrency: 3,
      handler: async (e, ctx) => {
        await new Promise((r) => setTimeout(r, Math.random() * 5));
        got.push({ key: e.key, n: Number(e.data.gatewayInstance.slice(3)), ...(ctx.headers['traceparent'] ? { traceparent: ctx.headers['traceparent'] } : {}) });
      },
    });
    await c.start();
    const sends: Promise<void>[] = [];
    for (let n = 0; n < 30; n++) {
      const env = connected(`CP-${n % 4}`, n);
      sends.push(producer.sendEnvelope(t, env, { headers: injectTraceHeaders({}, spanContext()) }));
    }
    await Promise.all(sends);
    await waitFor(() => got.length >= 30, { timeoutMs: 30_000 });
    for (const k of ['CP-0', 'CP-1', 'CP-2', 'CP-3']) {
      const ns = got.filter((g) => g.key === k).map((g) => g.n);
      expect(ns).toEqual([...ns].sort((a, b) => a - b));
    }
    expect(got.every((g) => g.traceparent?.includes(TRACE_ID))).toBe(true);
    await waitFor(() => c.readinessCheck()());
    await c.refreshLag();
    const text = await registry.metrics();
    expect(sample(text, 'kafka_consumer_messages_total', { topic: t, outcome: 'ok' })).toBe(30);
    expect(text).toContain('kafka_consumer_lag');
    await c.stop();
    expect(c.readinessCheck()()).toBe(false);
  });

  it('retries a failing handler, then dead-letters it and moves on', async () => {
    const t = await topic(1);
    const attempts: number[] = [];
    const ok: string[] = [];
    const c = consumer({
      topics: [t],
      maxAttempts: 3,
      handler: async (e, ctx) => {
        if (e.data.gatewayInstance === 'gw-poison') {
          attempts.push(ctx.attempt);
          throw new Error('cannot handle');
        }
        ok.push(e.data.gatewayInstance);
      },
    });
    await c.start();
    await producer.sendEnvelope(t, makeEnvelope('station.connected', 'CP', { stationId: 'CP', gatewayInstance: 'gw-poison' }, 'test'));
    await producer.send([{ topic: t, key: 'CP', value: '{"not":"an envelope"}' }]);
    await producer.send([{ topic: t, key: 'CP', value: 'not json' }]);
    await producer.sendEnvelope(t, connected('CP', 1));
    await waitFor(() => ok.length === 1, { timeoutMs: 30_000 });
    expect(attempts).toEqual([1, 2, 3]);

    const dlq = await readRaw(`${t}.dlq`, 3);
    expect(dlq.map((d) => d.headers['dlq-reason'])).toEqual(['handler', 'decode', 'decode']);
    expect(dlq[0]?.headers).toMatchObject({ 'dlq-attempts': '3', 'dlq-source-topic': t, 'dlq-source-partition': '0', 'dlq-error': 'cannot handle', 'dlq-source-offset': '0' });
    expect(dlq[0]?.key).toBe('CP');
    expect(JSON.parse(dlq[0]?.value ?? '{}')).toMatchObject({ data: { gatewayInstance: 'gw-poison' } });
    expect(dlq[1]?.headers['dlq-error']).toContain('schema validation failed');
    expect(dlq[2]?.value).toBe('not json');
    const text = await registry.metrics();
    expect(sample(text, 'kafka_consumer_messages_total', { topic: t, outcome: 'dlq' })).toBe(3);
  });

  it('commits only after success: unfinished work is redelivered to the next member', async () => {
    const t = await topic(1);
    const groupId = uniq('grp-redeliver');
    const seen1: string[] = [];
    const c1 = consumer({
      topics: [t],
      groupId,
      maxAttempts: 1000,
      handler: async (e) => {
        seen1.push(e.data.gatewayInstance);
        if (e.data.gatewayInstance === 'gw-2') throw new Error('stuck');
      },
    });
    await c1.start();
    for (let n = 1; n <= 3; n++) await producer.sendEnvelope(t, connected('CP', n));
    await waitFor(() => seen1.includes('gw-2'), { timeoutMs: 30_000 });
    await c1.stop();
    expect(seen1).not.toContain('gw-3');

    const seen2: string[] = [];
    const c2 = consumer({ topics: [t], groupId, handler: async (e) => void seen2.push(e.data.gatewayInstance) });
    await c2.start();
    await waitFor(() => seen2.length >= 2, { timeoutMs: 30_000 });
    expect(seen2).toEqual(['gw-2', 'gw-3']); // gw-1 was committed, gw-2 never was
    await c2.stop();
  });
});

describe('kafka consumer without reachable brokers (ADR 0012)', () => {
  it('start() returns instead of failing the service, reports not ready, and stops cleanly', async () => {
    const { logger: quiet } = captureLogger('kafka-unreachable', 'error');
    const c = createConsumer({
      brokers: ['127.0.0.1:1'],
      groupId: uniq('unreachable'),
      topics: ['nowhere.v1'],
      schema: events.StationConnected,
      lagIntervalMs: 0,
      logger: quiet,
      dlqProducer: createProducer({ brokers: ['127.0.0.1:1'], clientId: uniq('dlq') }),
      handler: async () => {},
    });
    const t0 = Date.now();
    await c.start(); // must not throw: the service keeps serving what does not need Kafka
    expect(Date.now() - t0).toBeLessThan(30_000);
    expect(c.readinessCheck()()).toBe(false);
    await c.stop();
  }, 60_000);
});
