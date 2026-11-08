import { events } from '@evcharge/contracts';
import {
  createConsumer,
  createProducer,
  type KafkaConsumer,
  type KafkaProducer,
  type Logger,
  OutboxRelay,
  type Pool,
  withInbox,
  withTx,
} from '@evcharge/platform';
import type { BillingDeps } from './api/billing.controller';
import type { BillingConfig } from './config';
import { errorMessage } from './util';
import { InvoiceReader } from './invoices/invoice-reader';
import { metrics } from './metrics';
import type { PaymentProvider } from './psp/payment-provider';
import { MockPsp } from './psp/mock-psp';
import { SagaEngine } from './sagas/saga-engine';
import { SagaWorker } from './sagas/saga-worker';
import { TariffClient, type TariffQuoter } from './tariff/tariff-client';

export interface BillingRuntime {
  engine: SagaEngine;
  worker: SagaWorker;
  consumer: KafkaConsumer;
  relay: OutboxRelay;
  producer: KafkaProducer;
  psp: PaymentProvider;
  deps: BillingDeps;
  /** Creates invoice partitions, then starts the consumer, outbox relay and saga workers. */
  start(): Promise<void>;
  /** Stops intake first, then the workers, the relay and the producer. */
  stop(): Promise<void>;
}

export interface RuntimeOverrides {
  psp?: PaymentProvider;
  tariff?: TariffQuoter;
}

const CONSUMER_NAME = 'billing.session-events';

/** Monthly invoice partitions: last month through three months ahead. */
export async function ensureInvoicePartitions(pool: Pool): Promise<number> {
  const { rows } = await pool.query<{ n: number }>("SELECT billing_ensure_invoice_partitions((date_trunc('month', now()) - interval '1 month')::date, 5) AS n");
  return rows[0]?.n ?? 0;
}

export function createBillingRuntime(config: BillingConfig, pool: Pool, log: Logger, overrides: RuntimeOverrides = {}): BillingRuntime {
  const psp =
    overrides.psp ??
    new MockPsp(pool, {
      timeoutRate: config.PSP_TIMEOUT_RATE,
      declineTokens: new Set(config.PSP_DECLINE_TOKENS),
      authDeclineTokens: new Set(config.PSP_AUTH_DECLINE_TOKENS),
      latencyMs: config.PSP_LATENCY_MS,
    });
  const tariff = overrides.tariff ?? new TariffClient(config);
  const engine = new SagaEngine(pool, psp, tariff, config, log);
  const worker: SagaWorker = new SagaWorker(
    engine,
    {
      workers: config.SAGA_WORKERS,
      batch: config.SAGA_BATCH,
      idlePollMs: config.SAGA_IDLE_POLL_MS,
      maintenance: { intervalMs: 6 * 3600 * 1000, run: async () => void (await ensureInvoicePartitions(pool)) },
    },
    log,
  );
  const producer = createProducer({ brokers: config.KAFKA_BROKERS, clientId: `${config.SERVICE_NAME}-producer` });
  const relay = new OutboxRelay({ pool, producer, logger: log });

  const consumer = createConsumer({
    brokers: config.KAFKA_BROKERS,
    groupId: config.CONSUMER_GROUP,
    clientId: config.SERVICE_NAME,
    topics: [config.SESSION_EVENTS_TOPIC],
    schema: events.SessionEvent,
    logger: log,
    handler: async (event) => {
      if (event.type !== 'session.started' && event.type !== 'session.completed') {
        metrics.events.inc({ type: event.type, result: 'ignored' });
        return;
      }
      // Inbox marker and saga write commit together: a redelivery is a no-op.
      const res = await withTx(pool, (c) =>
        withInbox(c, CONSUMER_NAME, event.eventId, () => (event.type === 'session.started' ? engine.onSessionStarted(c, event.data) : engine.onSessionCompleted(c, event.data))),
      );
      metrics.events.inc({ type: event.type, result: res.duplicate ? 'duplicate' : 'applied' });
      if (!res.duplicate) worker.wake();
    },
  });

  return {
    engine,
    worker,
    consumer,
    relay,
    producer,
    psp,
    deps: { pool, invoices: new InvoiceReader(pool) },
    async start() {
      const created = await ensureInvoicePartitions(pool);
      if (created > 0) log.info({ created }, 'created invoice partitions');
      await engine.refreshPendingGauge().catch((err) => log.warn({ err: errorMessage(err) }, 'initial gauge refresh failed'));
      worker.start();
      await relay.start();
      await consumer.start();
    },
    async stop() {
      await consumer.stop();
      await worker.stop();
      await relay.stop();
      await producer.close();
    },
  };
}
