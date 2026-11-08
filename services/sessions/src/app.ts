/**
 * Builds and starts the sessions service: Nest app (health, metrics, internal auth from @evcharge/platform),
 * Postgres + migrations + seed, outbox relay, Kafka consumers, allocation orchestration and housekeeping.
 * `main.ts` calls this with the process environment; integration tests call it with their own topics/groups.
 */
/* oxlint-disable no-await-in-loop */ // consumers are started one after another
import { join } from 'node:path';
import { ConsumerGroups, events, Topics } from '@evcharge/contracts';
import {
  bootstrapService,
  closeRedis,
  createConsumer,
  createHttpClient,
  createLogger,
  createPgPool,
  createProducer,
  createRedis,
  type KafkaConsumer,
  type Logger,
  OutboxRelay,
  type Pool,
  pgReadinessCheck,
  type RunningService,
  redisReadinessCheck,
  registerReadinessCheck,
  runMigrations,
} from '@evcharge/platform';
import type { Telemetry } from '@evcharge/platform/telemetry';
import { type DynamicModule, Inject, Injectable, Module, type OnModuleInit } from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import type { FastifyInstance } from 'fastify';
import { AllocationService } from './allocation/allocation.service';
import { seedCatalogIfEmpty } from './catalog/seeder';
import type { Config } from './config';
import { StationEventHandlers } from './consumers/handlers';
import { CatalogService } from './driver/catalog.service';
import { DriverService } from './driver/driver.service';
import { DriverController } from './http/driver.controller';
import { DriverGuard } from './http/driver.guard';
import { InternalController } from './http/internal.controller';
import { ProblemFilter } from './http/problem';
import { OcppService } from './ocpp/ocpp.service';
import { ReadRouter, ReplicaMonitor } from './read-router';
import { SiteSignal } from './site-signal';
import { APP_CONFIG, LOGGER } from './tokens';
import { ensurePartitions, Maintenance } from './workers/maintenance';

export interface AppTopics {
  stationEvents: string;
  commandResults: string;
  stationCommands: string;
  sessionEvents: string;
}

export interface AppOptions {
  config: Config;
  telemetry?: Telemetry;
  logger?: Logger;
  /** Topic names; tests use unique topics. */
  topics?: Partial<AppTopics>;
  /** Consumer group ids; tests use unique groups. */
  groups?: { stationEvents: string; commandResults: string };
  /** 0 picks a free port (tests). */
  port?: number;
  handleSignals?: boolean;
  /** Overrides config.WORKERS_ENABLED when set. */
  workers?: boolean;
}

export interface SessionsApp extends RunningService {
  pool: Pool;
  allocation: AllocationService;
  ocpp: OcppService;
}

/**
 * Driver-api sends `content-type: application/json` on bodiless POST/DELETE calls (stop, cancel). Fastify's default
 * parser rejects an empty JSON body with 400 before any guard runs; treat it as "no body" instead.
 */
@Injectable()
class EmptyJsonBodyParser implements OnModuleInit {
  constructor(@Inject(HttpAdapterHost) private readonly host: HttpAdapterHost) {}

  onModuleInit(): void {
    const fastify = this.host.httpAdapter.getInstance() as FastifyInstance;
    const json = fastify.getDefaultJsonParser('error', 'error');
    fastify.removeContentTypeParser('application/json');
    fastify.addContentTypeParser('application/json', { parseAs: 'string' }, (req, body, done) => {
      if (body === '') done(null, undefined);
      else json(req, body as string, done);
    });
  }
}

@Module({})
class SessionsModule {
  static forRoot(deps: { config: Config; logger: Logger; catalog: CatalogService; drivers: DriverService; ocpp: OcppService }): DynamicModule {
    return {
      module: SessionsModule,
      controllers: [InternalController, DriverController],
      providers: [
        { provide: LOGGER, useValue: deps.logger },
        { provide: APP_CONFIG, useValue: deps.config },
        { provide: CatalogService, useValue: deps.catalog },
        { provide: DriverService, useValue: deps.drivers },
        { provide: OcppService, useValue: deps.ocpp },
        ProblemFilter,
        DriverGuard,
        EmptyJsonBodyParser,
      ],
    };
  }
}

export async function createSessionsApp(opts: AppOptions): Promise<SessionsApp> {
  const { config } = opts;
  const logger = opts.logger ?? createLogger({ service: config.SERVICE_NAME, level: config.LOG_LEVEL });
  const topics: AppTopics = {
    stationEvents: opts.topics?.stationEvents ?? Topics.StationEvents,
    commandResults: opts.topics?.commandResults ?? Topics.StationCommandResults,
    stationCommands: opts.topics?.stationCommands ?? Topics.StationCommands,
    sessionEvents: opts.topics?.sessionEvents ?? Topics.SessionEvents,
  };
  const groups = opts.groups ?? { stationEvents: ConsumerGroups.SessionsStationEvents, commandResults: ConsumerGroups.SessionsCommandResults };
  const workers = opts.workers ?? config.WORKERS_ENABLED;

  const pool = createPgPool(config.DATABASE_URL, { max: config.PG_POOL_MAX, applicationName: config.SERVICE_NAME, logger });
  // Bulkhead: allocation rounds (one per start/stop plus periodic) get their own pool, so a burst of rounds can never
  // starve the OCPP hot path (CALLRESULT p99 < 500 ms, ADR 0000). Found by the sessions k6 run (pool waits of ~0.8 s).
  const allocationPool = createPgPool(config.DATABASE_URL, { max: config.ALLOCATION_PG_POOL_MAX, applicationName: `${config.SERVICE_NAME}-allocation`, logger });
  await runMigrations(pool, join(__dirname, '..', 'migrations'), { logger });
  await ensurePartitions(pool);
  await seedCatalogIfEmpty(
    pool,
    { sites: config.SEED_SITES, stationsPerSite: config.SEED_STATIONS_PER_SITE, dcShare: config.SEED_DC_SHARE, secretSeed: config.STATION_SECRET_SEED, allocationPeriodS: config.ALLOCATION_PERIOD_S },
    logger,
  );

  const redis = createRedis(config.REDIS_URL, { logger });
  const producer = createProducer({ brokers: config.KAFKA_BROKERS, clientId: `${config.SERVICE_NAME}-producer` });
  const allocator = createHttpClient({
    name: 'smart-charging',
    baseUrl: config.SMART_CHARGING_URL,
    timeoutMs: config.SMART_CHARGING_TIMEOUT_MS,
    retries: 0,
    internalToken: config.INTERNAL_TOKEN,
    circuitBreaker: { failureThreshold: 5, openMs: 10_000 },
  });

  const signal = new SiteSignal();
  const ocpp = new OcppService(pool, topics, signal, logger, config.ACCEPT_UNKNOWN_IDTAGS);
  // Optional hot standby (ADR 0015); without it the router sends every read to the primary.
  const replicaPool = config.REPLICA_DATABASE_URL
    ? createPgPool(config.REPLICA_DATABASE_URL, { max: config.REPLICA_PG_POOL_MAX, applicationName: `${config.SERVICE_NAME}-replica`, logger })
    : undefined;
  const router = new ReadRouter(pool, replicaPool, { waitMs: config.REPLICA_RYW_TIMEOUT_MS, pollMs: config.REPLICA_RYW_POLL_MS, logger });
  const replicaMonitor = replicaPool ? new ReplicaMonitor(pool, replicaPool, logger) : undefined;
  const drivers = new DriverService(pool, topics, router);
  const catalog = new CatalogService(router);
  const allocation = new AllocationService(allocationPool, redis, allocator, topics, { periodS: config.ALLOCATION_PERIOD_S, tickMs: config.ALLOCATION_TICK_MS, concurrency: config.ALLOCATION_CONCURRENCY }, logger, signal);
  const handlers = new StationEventHandlers(pool, topics, logger, signal);
  const maintenance = new Maintenance(pool, topics, config.SWEEPER_INTERVAL_MS, logger);
  const relay = new OutboxRelay({ pool, producer, logger });

  registerReadinessCheck('postgres', pgReadinessCheck(pool));
  registerReadinessCheck('redis', redisReadinessCheck(redis));
  registerReadinessCheck('kafka-producer', () => producer.readinessCheck());

  const svc = await bootstrapService({
    module: SessionsModule.forRoot({ config, logger, catalog, drivers, ocpp }),
    config,
    logger,
    port: opts.port,
    handleSignals: opts.handleSignals,
    telemetry: opts.telemetry,
    // Hooks run in reverse registration order: these connections close last.
    onShutdown: [
      { name: 'pg', run: () => pool.end() },
      { name: 'pg-allocation', run: () => allocationPool.end() },
      ...(replicaPool ? [{ name: 'pg-replica', run: () => replicaPool.end() }] : []),
      { name: 'replica-monitor', run: () => replicaMonitor?.stop() },
      { name: 'redis', run: () => closeRedis(redis) },
      { name: 'kafka-producer', run: () => producer.close() },
    ],
  });
  replicaMonitor?.start();
  if (workers) {
    const consumers: { name: string; consumer: KafkaConsumer }[] = [
      {
        name: 'station-events',
        consumer: createConsumer({ brokers: config.KAFKA_BROKERS, groupId: groups.stationEvents, topics: [topics.stationEvents], schema: events.StationEvent, handler: (e) => handlers.stationEvent(e), logger }),
      },
      {
        name: 'command-results',
        consumer: createConsumer({ brokers: config.KAFKA_BROKERS, groupId: groups.commandResults, topics: [topics.commandResults], schema: events.StationCommandCompleted, handler: (e) => handlers.commandResult(e), logger }),
      },
    ];
    svc.lifecycle.onShutdown('outbox-relay', () => relay.stop());
    svc.lifecycle.onShutdown('consumers', async () => void (await Promise.allSettled(consumers.map((c) => c.consumer.stop()))));
    svc.lifecycle.onShutdown('workers', async () => {
      await allocation.stop();
      await maintenance.stop();
    });
    await relay.start();
    for (const c of consumers) {
      await c.consumer.start();
      registerReadinessCheck(`kafka-consumer:${c.name}`, () => c.consumer.readinessCheck());
    }
    maintenance.start();
    allocation.start();
  }
  return Object.assign(svc, { pool, allocation, ocpp });
}
