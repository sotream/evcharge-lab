import type { FastifyInstance, FastifyRequest } from 'fastify';
import { collectDefaultMetrics, Counter, Gauge, Histogram, Registry, type CounterConfiguration, type GaugeConfiguration, type HistogramConfiguration } from 'prom-client';

/** The one registry every platform metric (and service metric created via helpers) lives on. */
export const registry = new Registry();

let defaultsCollected = false;

/** Sets the `service` default label and (once) starts default process/runtime metrics. */
export function initMetrics(opts: { service: string; defaultMetrics?: boolean }): Registry {
  registry.setDefaultLabels({ service: opts.service });
  if (opts.defaultMetrics !== false && !defaultsCollected) {
    collectDefaultMetrics({ register: registry });
    defaultsCollected = true;
  }
  return registry;
}

type Cfg<C> = Omit<C, 'registers'>;

/** Get-or-create a counter on the shared registry (safe to call repeatedly with the same name). */
export function counter<T extends string = string>(cfg: Cfg<CounterConfiguration<T>>): Counter<T> {
  return (registry.getSingleMetric(cfg.name) as Counter<T> | undefined) ?? new Counter<T>({ ...cfg, registers: [registry] });
}

/** Get-or-create a histogram on the shared registry. */
export function histogram<T extends string = string>(cfg: Cfg<HistogramConfiguration<T>>): Histogram<T> {
  return (registry.getSingleMetric(cfg.name) as Histogram<T> | undefined) ?? new Histogram<T>({ ...cfg, registers: [registry] });
}

/** Get-or-create a gauge on the shared registry. */
export function gauge<T extends string = string>(cfg: Cfg<GaugeConfiguration<T>>): Gauge<T> {
  return (registry.getSingleMetric(cfg.name) as Gauge<T> | undefined) ?? new Gauge<T>({ ...cfg, registers: [registry] });
}

export const LATENCY_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

const startTimes = new WeakMap<FastifyRequest, bigint>();

/**
 * Adds `http_server_requests_seconds{method,route,status}` (route = route template, or
 * "unmatched" for 404s, so cardinality stays bounded) and `GET /metrics` to a Fastify instance.
 */
export function registerHttpMetrics(app: FastifyInstance, opts: { metricsPath?: string } = {}): void {
  const path = opts.metricsPath ?? '/metrics';
  const h = histogram({
    name: 'http_server_requests_seconds',
    help: 'HTTP server request duration in seconds',
    labelNames: ['method', 'route', 'status'] as const,
    buckets: LATENCY_BUCKETS,
  });
  app.addHook('onRequest', async (req) => {
    startTimes.set(req, process.hrtime.bigint());
  });
  app.addHook('onResponse', async (req, reply) => {
    const start = startTimes.get(req);
    if (start === undefined) return;
    const route = req.routeOptions?.url ?? 'unmatched';
    if (route === path) return;
    h.observe({ method: req.method, route, status: String(reply.statusCode) }, Number(process.hrtime.bigint() - start) / 1e9);
  });
  app.get(path, async (_req, reply) => {
    reply.header('content-type', registry.contentType);
    return registry.metrics();
  });
}
