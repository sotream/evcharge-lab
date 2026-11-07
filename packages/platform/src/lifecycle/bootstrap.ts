import { type DynamicModule, Module, type Type } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import type { FastifyInstance } from 'fastify';
import { InternalAuthModule } from '../auth/internal-token.guard';
import type { BaseConfig } from '../config/config';
import { defaultHealth, HealthModule, type HealthRegistry } from '../health/health';
import { createLogger, type Logger, NestLoggerAdapter } from '../logging/logger';
import { initMetrics, registerHttpMetrics } from '../metrics/metrics';
import type { Telemetry } from '../telemetry/telemetry';
import { Lifecycle, type ShutdownHook, type ShutdownResult } from './lifecycle';

export type ServiceConfig = Pick<BaseConfig, 'SERVICE_NAME' | 'LOG_LEVEL' | 'HTTP_PORT' | 'INTERNAL_TOKEN' | 'SHUTDOWN_GRACE_MS'>;

export interface BootstrapOptions {
  /** The service's root Nest module. Health, metrics and internal auth are added around it. */
  module: Type<unknown> | DynamicModule;
  config: ServiceConfig;
  /** Defaults to config.HTTP_PORT. 0 picks a free port (tests). */
  port?: number;
  host?: string;
  logger?: Logger;
  /** Handle from startTelemetry(); its shutdown runs last. */
  telemetry?: Telemetry;
  /** Hooks run (in reverse order) after HTTP is closed. More can be added via `lifecycle.onShutdown`. */
  onShutdown?: ShutdownHook | ShutdownHook[];
  /** Default 2000ms, capped to a third of SHUTDOWN_GRACE_MS. */
  drainDelayMs?: number;
  health?: HealthRegistry;
  /** Default true. Tests set false and call `service.shutdown()` themselves. */
  handleSignals?: boolean;
  /** Called after a signal-triggered shutdown. Default process.exit. */
  exit?: (code: number) => void;
  /** Extra Fastify adapter options (bodyLimit, trustProxy, ...). */
  fastify?: ConstructorParameters<typeof FastifyAdapter>[0];
}

export interface RunningService {
  app: NestFastifyApplication;
  fastify: FastifyInstance;
  url: string;
  logger: Logger;
  lifecycle: Lifecycle;
  health: HealthRegistry;
  shutdown(reason?: string): Promise<ShutdownResult>;
}

/**
 * Creates a Fastify-based Nest app with pino logging, /metrics + http_server_requests_seconds,
 * /health/live + /health/ready, a global INTERNAL_TOKEN provider for InternalTokenGuard, and
 * graceful SIGTERM/SIGINT handling, then starts listening.
 */
export async function bootstrapService(opts: BootstrapOptions): Promise<RunningService> {
  const { config } = opts;
  const logger = opts.logger ?? createLogger({ service: config.SERVICE_NAME, level: config.LOG_LEVEL });
  const health = opts.health ?? defaultHealth;
  initMetrics({ service: config.SERVICE_NAME });

  const lifecycle = new Lifecycle({
    logger,
    health,
    graceMs: config.SHUTDOWN_GRACE_MS,
    drainDelayMs: opts.drainDelayMs ?? Math.min(2000, Math.floor(config.SHUTDOWN_GRACE_MS / 3)),
  });
  if (opts.telemetry) {
    const t = opts.telemetry;
    lifecycle.onShutdown('telemetry', () => t.shutdown());
  }
  const hooks = opts.onShutdown === undefined ? [] : Array.isArray(opts.onShutdown) ? opts.onShutdown : [opts.onShutdown];
  for (const h of hooks) lifecycle.onShutdown(h.name, h.run);

  @Module({ imports: [InternalAuthModule.forRoot(config.INTERNAL_TOKEN), HealthModule.forRoot(health), opts.module] })
  class PlatformRootModule {}

  const adapter = new FastifyAdapter({ return503OnClosing: true, forceCloseConnections: 'idle', ...opts.fastify });
  const app = await NestFactory.create<NestFastifyApplication>(PlatformRootModule, adapter, {
    logger: new NestLoggerAdapter(logger),
    abortOnError: false,
  });
  const fastify = app.getHttpAdapter().getInstance() as unknown as FastifyInstance;
  registerHttpMetrics(fastify);
  lifecycle.setHttpCloser(() => app.close());

  const port = opts.port ?? config.HTTP_PORT;
  await app.listen(port, opts.host ?? '0.0.0.0');
  const addr = fastify.server.address();
  const actualPort = typeof addr === 'object' && addr ? addr.port : port;
  const url = `http://127.0.0.1:${actualPort}`;
  if (opts.handleSignals !== false) lifecycle.installSignalHandlers(opts.exit);
  logger.info({ port: actualPort }, 'service listening');

  return { app, fastify, url, logger, lifecycle, health, shutdown: (reason) => lifecycle.shutdown(reason) };
}
