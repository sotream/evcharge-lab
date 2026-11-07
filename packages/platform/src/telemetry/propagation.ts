import { context as otelContext, type Context, type TextMapGetter, trace, SpanKind, type Span, SpanStatusCode } from '@opentelemetry/api';
import { W3CTraceContextPropagator } from '@opentelemetry/core';

/**
 * W3C traceparent propagation over Kafka headers. Uses a local W3C propagator (not the global
 * one) so it works the same whether or not the SDK is started.
 */
const w3c = new W3CTraceContextPropagator();

/** Kafka headers as produced/consumed: Map or plain object, values Buffer or string. */
export type KafkaHeaders = Map<string | Buffer, string | Buffer> | Record<string, string | Buffer | undefined>;

const headerGetter: TextMapGetter<KafkaHeaders> = {
  keys(carrier) {
    return carrier instanceof Map ? [...carrier.keys()].map(String) : Object.keys(carrier);
  },
  get(carrier, key) {
    let v: string | Buffer | undefined;
    if (carrier instanceof Map) {
      for (const [k, val] of carrier) if (String(k).toLowerCase() === key) v = val;
    } else {
      v = carrier[key] ?? Object.entries(carrier).find(([k]) => k.toLowerCase() === key)?.[1];
    }
    return v === undefined ? undefined : String(v);
  },
};

/** Writes `traceparent` (and `tracestate`) for the given/active context into `headers`. */
export function injectTraceHeaders(headers: Record<string, string> = {}, ctx: Context = otelContext.active()): Record<string, string> {
  w3c.inject(ctx, headers, { set: (c, k, v) => (c[k] = v) });
  return headers;
}

/** Returns a context whose parent span is the one described by the headers' traceparent. */
export function extractTraceContext(headers: KafkaHeaders, ctx: Context = otelContext.active()): Context {
  return w3c.extract(ctx, headers, headerGetter);
}

/** The traceparent value for the active span, if any. */
export function currentTraceparent(ctx: Context = otelContext.active()): string | undefined {
  return injectTraceHeaders({}, ctx)['traceparent'];
}

const tracer = () => trace.getTracer('@evcharge/platform');

/** Runs fn inside a span (CONSUMER/PRODUCER/INTERNAL); records exceptions and ends the span. */
export async function inSpan<T>(
  name: string,
  opts: { kind?: SpanKind; parent?: Context; attributes?: Record<string, string | number | boolean> },
  fn: (span: Span) => Promise<T>,
): Promise<T> {
  const parent = opts.parent ?? otelContext.active();
  const span = tracer().startSpan(name, { kind: opts.kind ?? SpanKind.INTERNAL, attributes: opts.attributes ?? {} }, parent);
  return otelContext.with(trace.setSpan(parent, span), async () => {
    try {
      return await fn(span);
    } catch (err) {
      span.recordException(err as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error)?.message });
      throw err;
    } finally {
      span.end();
    }
  });
}
