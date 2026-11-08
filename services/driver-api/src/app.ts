/**
 * Builds and starts driver-api: Postgres (drivers DB, migrations), Redis (login rate limit), JWT service,
 * downstream clients, and the Nest app with health, metrics and graceful shutdown from @evcharge/platform.
 * `main.ts` calls this with the process environment; integration tests call it with their own database and keys.
 */
import { join } from 'node:path';
import {
  bootstrapService,
  closeRedis,
  createLogger,
  createPgPool,
  createRedis,
  HealthRegistry,
  type Logger,
  type Pool,
  pgReadinessCheck,
  type RunningService,
  runMigrations,
} from '@evcharge/platform';
import type { Telemetry } from '@evcharge/platform/telemetry';
import { type DynamicModule, Module } from '@nestjs/common';
import { AuthController } from './auth.controller';
import { AuthGuard } from './auth.guard';
import { AuthService } from './auth.service';
import type { Config } from './config';
import { DriverController } from './driver.controller';
import { createDownstream, type Downstream } from './downstream';
import { JwtService } from './jwt';
import { metrics } from './metrics';
import { RateLimiter } from './rate-limit';
import { APP_CONFIG, DOWNSTREAM, JWT, LOGGER, PG_POOL, RATE_LIMITER } from './tokens';

/** proxy-addr hop semantics: trust the n nearest hops (the socket peer is hop 0). */
const hops = (n: number) => (_addr: string, i: number) => i < n;

@Module({})
class AppModule {
  static register(values: Record<symbol, unknown>): DynamicModule {
    return {
      module: AppModule,
      controllers: [AuthController, DriverController],
      providers: [...Reflect.ownKeys(values).map((k) => ({ provide: k, useValue: values[k as symbol] })), AuthService, AuthGuard],
    };
  }
}

export interface DriverApiOptions {
  config: Config;
  telemetry?: Telemetry;
  logger?: Logger;
  /** 0 picks a free port (tests). */
  port?: number;
  handleSignals?: boolean;
}

export interface DriverApp extends RunningService {
  pool: Pool;
  downstream: Downstream;
}

export async function createDriverApi(opts: DriverApiOptions): Promise<DriverApp> {
  const { config } = opts;
  const logger = opts.logger ?? createLogger({ service: config.SERVICE_NAME, level: config.LOG_LEVEL });
  const pool = createPgPool(config.DATABASE_URL, { max: config.PG_POOL_MAX, applicationName: config.SERVICE_NAME, logger });
  await runMigrations(pool, join(__dirname, '..', 'migrations'), { logger });
  const redis = createRedis(config.REDIS_URL, { logger });
  // One registry per app instance (not the process-wide default), so several apps in one process stay independent.
  const health = new HealthRegistry();
  health.register('postgres', pgReadinessCheck(pool));
  // Redis is deliberately not a readiness check: the rate limiter fails open, so Redis down must not take the API out.

  const downstream = createDownstream(config);
  const jwt = new JwtService(config.JWT_SECRET, config.JWT_PREVIOUS_SECRETS, config.ACCESS_TOKEN_TTL_S);
  const limiter = new RateLimiter(redis, config.REDIS_KEY_PREFIX, config.LOGIN_RATE_LIMIT, config.LOGIN_RATE_WINDOW_S, Date.now, (err) => {
    metrics.rateLimiterErrors.inc();
    logger.warn({ err: err instanceof Error ? err.message : String(err) }, 'rate limiter unavailable; allowing request');
  });
  const module = AppModule.register({ [PG_POOL]: pool, [JWT]: jwt, [RATE_LIMITER]: limiter, [DOWNSTREAM]: downstream, [APP_CONFIG]: config, [LOGGER]: logger });

  // Expired refresh tokens are dead weight; purge them once an hour.
  const purge = setInterval(() => {
    pool.query("DELETE FROM refresh_tokens WHERE expires_at < now() - interval '1 day'").catch((err: unknown) => logger.warn({ err: String(err) }, 'refresh token purge failed'));
  }, 3_600_000);
  purge.unref();

  const svc = await bootstrapService({
    module,
    config,
    logger,
    health,
    ...(opts.telemetry ? { telemetry: opts.telemetry } : {}),
    ...(opts.port !== undefined ? { port: opts.port } : {}),
    ...(opts.handleSignals !== undefined ? { handleSignals: opts.handleSignals } : {}),
    fastify: { trustProxy: typeof config.TRUST_PROXY === 'number' ? hops(config.TRUST_PROXY) : config.TRUST_PROXY, bodyLimit: 64 * 1024 },
    // Hooks run in reverse order: redis closes first, then the pool.
    onShutdown: [
      { name: 'postgres', run: () => pool.end() },
      { name: 'redis', run: () => closeRedis(redis) },
      { name: 'purge-timer', run: () => clearInterval(purge) },
    ],
  });
  return Object.assign(svc, { pool, downstream });
}
