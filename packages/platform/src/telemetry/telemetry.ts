/**
 * OpenTelemetry bootstrap. This file must only import @opentelemetry/* packages so it can run
 * before pg/ioredis/fastify/Nest are loaded (instrumentations patch modules on first require).
 * Use the `@evcharge/platform/telemetry` entry point from the very top of a service's main.ts.
 */
import type { Instrumentation } from '@opentelemetry/instrumentation';

export interface TelemetryOptions {
  serviceName: string;
  serviceVersion?: string;
  /** OTLP/HTTP base URL, e.g. http://jaeger:4318. Defaults to env OTEL_EXPORTER_OTLP_ENDPOINT. Unset = no-op. */
  endpoint?: string | undefined;
  /** Extra instrumentations on top of the default set. */
  instrumentations?: Instrumentation[];
  /** URL path prefixes that never produce server spans (default: /health, /metrics). */
  ignorePaths?: string[];
}

export interface Telemetry {
  readonly enabled: boolean;
  /** Flushes pending spans and stops the SDK. Safe to call more than once. */
  shutdown(): Promise<void>;
}

let current: Telemetry | undefined;

/**
 * Starts the OTel NodeSDK with an OTLP/HTTP trace exporter and instrumentations for
 * http, undici (global fetch), fastify (@fastify/otel), pg and ioredis.
 * Returns a no-op handle when no endpoint is configured. Idempotent per process.
 */
export function startTelemetry(opts: TelemetryOptions): Telemetry {
  if (current) return current;
  const endpoint = opts.endpoint ?? process.env['OTEL_EXPORTER_OTLP_ENDPOINT'];
  if (!endpoint) {
    current = { enabled: false, shutdown: async () => {} };
    return current;
  }
  const ignore = opts.ignorePaths ?? ['/health', '/metrics'];
  const ignored = (url: string | undefined) => !!url && ignore.some((p) => url.startsWith(p));

  /* Loaded lazily so the no-op path costs nothing. */
  const { NodeSDK } = require('@opentelemetry/sdk-node') as typeof import('@opentelemetry/sdk-node');
  const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http') as typeof import('@opentelemetry/exporter-trace-otlp-http');
  const { HttpInstrumentation } = require('@opentelemetry/instrumentation-http') as typeof import('@opentelemetry/instrumentation-http');
  const { UndiciInstrumentation } = require('@opentelemetry/instrumentation-undici') as typeof import('@opentelemetry/instrumentation-undici');
  const { PgInstrumentation } = require('@opentelemetry/instrumentation-pg') as typeof import('@opentelemetry/instrumentation-pg');
  const { IORedisInstrumentation } = require('@opentelemetry/instrumentation-ioredis') as typeof import('@opentelemetry/instrumentation-ioredis');
  const { FastifyOtelInstrumentation } = require('@fastify/otel') as typeof import('@fastify/otel');
  const { W3CTraceContextPropagator } = require('@opentelemetry/core') as typeof import('@opentelemetry/core');
  const { resourceFromAttributes } = require('@opentelemetry/resources') as typeof import('@opentelemetry/resources');

  const sdk = new NodeSDK({
    serviceName: opts.serviceName,
    resource: resourceFromAttributes({
      'service.name': opts.serviceName,
      ...(opts.serviceVersion ? { 'service.version': opts.serviceVersion } : {}),
    }),
    traceExporter: new OTLPTraceExporter({ url: `${endpoint.replace(/\/$/, '')}/v1/traces` }),
    textMapPropagator: new W3CTraceContextPropagator(),
    instrumentations: [
      new HttpInstrumentation({ ignoreIncomingRequestHook: (req) => ignored(req.url) }),
      new UndiciInstrumentation(),
      new FastifyOtelInstrumentation({ registerOnInitialization: true, ignorePaths: (r) => ignored(r.url) }),
      new PgInstrumentation({ enhancedDatabaseReporting: false }),
      new IORedisInstrumentation(),
      ...(opts.instrumentations ?? []),
    ],
  });
  sdk.start();
  let stopping: Promise<void> | undefined;
  current = {
    enabled: true,
    shutdown: () => (stopping ??= sdk.shutdown().catch(() => {})),
  };
  return current;
}

/** Test hook: forget the started SDK so startTelemetry can be called again. */
export function resetTelemetryForTests(): void {
  current = undefined;
}
