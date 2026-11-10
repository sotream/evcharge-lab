/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (worker loops, ordering)
import { randomBytes } from 'node:crypto';
import { events } from '@evcharge/contracts';
import { createConsumer, createPgPool, createProducer, loadConfig, makeEnvelope, type KafkaConsumer, type KafkaProducer, type Pool } from '@evcharge/platform';
import type { PaymentProvider } from '../../src/psp/payment-provider';
import { billingConfigSchema, type BillingConfig } from '../../src/config';
import { createBillingRuntime, type BillingRuntime } from '../../src/runtime';
import { createTopics, INTERNAL_TOKEN, KAFKA_BROKERS, silentLogger, tempBillingDb, uniq } from './infra';

/** Per test process, never a literal in the source. */
const TEST_TOKEN_ENC_KEY = randomBytes(32).toString('base64');

export interface Harness {
  db: Awaited<ReturnType<typeof tempBillingDb>>;
  sessionTopic: string;
  billingTopic: string;
  group: string;
  tariffUrl: string;
  /** Starts a billing instance (own pool, consumer in the shared group) on the harness database. */
  startInstance(env?: Record<string, string>, psp?: (pool: Pool) => PaymentProvider): Promise<{ rt: BillingRuntime; config: BillingConfig; pool: Pool }>;
  publishSession(event: SessionEventEnvelope): Promise<void>;
  billingEvents: events.BillingEvent[];
  close(): Promise<void>;
}

type SessionEventEnvelope = ReturnType<typeof makeEnvelope>;

export async function createHarness(tariffUrl: string, partitions = 6): Promise<Harness> {
  const db = await tempBillingDb();
  const sessionTopic = uniq('t.session-events').replace(/_/g, '-');
  const billingTopic = uniq('t.billing-events').replace(/_/g, '-');
  const group = uniq('billing-test-group');
  const dropTopics = await createTopics([sessionTopic, `${sessionTopic}.dlq`, billingTopic, `${billingTopic}.dlq`], partitions);
  const producer: KafkaProducer = createProducer({ brokers: KAFKA_BROKERS, clientId: uniq('test-producer'), lingerMs: 1 });
  const instances: { rt: BillingRuntime; pool: Pool }[] = [];
  const billingEvents: events.BillingEvent[] = [];
  const reader: KafkaConsumer = createConsumer({
    brokers: KAFKA_BROKERS,
    groupId: uniq('billing-test-reader'),
    topics: [billingTopic],
    schema: events.BillingEvent,
    lagIntervalMs: 0,
    handler: async (e) => void billingEvents.push(e),
  });
  await reader.start();

  return {
    db,
    sessionTopic,
    billingTopic,
    group,
    tariffUrl,
    billingEvents,
    async startInstance(env = {}, makePsp) {
      const config = loadConfig(billingConfigSchema, {
        SERVICE_NAME: 'billing',
        LOG_LEVEL: 'silent',
        TOKEN_ENC_KEY: TEST_TOKEN_ENC_KEY,
        HTTP_PORT: '0',
        INTERNAL_TOKEN,
        DATABASE_URL: db.url,
        KAFKA_BROKERS: KAFKA_BROKERS.join(','),
        TARIFF_URL: tariffUrl,
        SESSION_EVENTS_TOPIC: sessionTopic,
        BILLING_EVENTS_TOPIC: billingTopic,
        CONSUMER_GROUP: group,
        SAGA_BACKOFF_BASE_MS: '20',
        SAGA_BACKOFF_MAX_MS: '150',
        SAGA_IDLE_POLL_MS: '50',
        TARIFF_BREAKER_OPEN_MS: '200',
        PSP_CALL_TIMEOUT_MS: '3000',
        ...env,
      });
      const pool = createPgPool(db.url, { max: 10 });
      const rt = createBillingRuntime(config, pool, silentLogger, makePsp ? { psp: makePsp(pool) } : {});
      instances.push({ rt, pool });
      await rt.start();
      return { rt, config, pool };
    },
    publishSession: (event) => producer.sendEnvelope(sessionTopic, event),
    async close() {
      await reader.stop().catch(() => {});
      for (const i of instances) await i.rt.stop().catch(() => {});
      for (const i of instances) await i.pool.end().catch(() => {});
      await producer.close().catch(() => {});
      await dropTopics();
      await db.drop();
    },
  };
}

export const driverIdOf = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
export const sessionIdOf = (n: number) => `10000000-0000-4000-8000-${String(n).padStart(12, '0')}`;

export function startedEvent(n: number, driverId: string | null, over: Partial<events.SessionStarted['data']> = {}) {
  const startedAt = new Date(Date.now() - 3_600_000).toISOString();
  return makeEnvelope(
    'session.started',
    `site-${n % 7}`,
    { sessionId: sessionIdOf(n), transactionId: n, driverId, idTag: `TAG${n}`, siteId: `site-${n % 7}`, stationId: `st-${n}`, connectorId: 1, tariffId: 'std', meterStartWh: 0, startedAt, ...over },
    'sessions',
  );
}

export function completedEvent(n: number, driverId: string | null, energyWh: number, opts: { eventId?: string } = {}) {
  return makeEnvelope(
    'session.completed',
    `site-${n % 7}`,
    {
      sessionId: sessionIdOf(n),
      transactionId: n,
      driverId,
      siteId: `site-${n % 7}`,
      stationId: `st-${n}`,
      connectorId: 1,
      tariffId: 'std',
      meterStartWh: 0,
      meterStopWh: energyWh,
      energyWh,
      startedAt: new Date(Date.now() - 3_600_000).toISOString(),
      stoppedAt: new Date().toISOString(),
      stopReason: 'Local',
    },
    'sessions',
    opts,
  );
}
