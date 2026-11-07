/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (ordering, retries, backoff)
import { Writable } from 'node:stream';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { context, trace, TraceFlags, type Context } from '@opentelemetry/api';
import { createLogger, type Logger } from '../src/logging/logger';

/** A logger whose JSON lines are captured in `lines`. */
export function captureLogger(service = 'test', level = 'trace'): { logger: Logger; lines: Record<string, unknown>[] } {
  const lines: Record<string, unknown>[] = [];
  const destination = new Writable({
    write(chunk: Buffer, _enc, cb) {
      for (const l of chunk.toString().split('\n')) if (l.trim()) lines.push(JSON.parse(l) as Record<string, unknown>);
      cb();
    },
  });
  return { logger: createLogger({ service, level, destination }), lines };
}

/** Value of the first Prometheus sample `name{...}` whose labels include all of `labels`, else undefined. */
export function sample(text: string, name: string, labels: Record<string, string>): number | undefined {
  for (const line of text.split('\n')) {
    const m = /^([a-zA-Z_:][\w:]*)(?:\{(.*)\})? (\S+)$/.exec(line);
    if (!m || m[1] !== name) continue;
    const ls = m[2] ?? '';
    if (Object.entries(labels).every(([k, v]) => ls.includes(`${k}="${v}"`))) return Number(m[3]);
  }
  return undefined;
}

let cm:AsyncLocalStorageContextManager | undefined;
export function enableContext(): void {
  if (cm) return;
  cm = new AsyncLocalStorageContextManager();
  cm.enable();
  context.setGlobalContextManager(cm);
}

export const TRACE_ID = '0af7651916cd43dd8448eb211c80319c';
export const SPAN_ID = 'b7ad6b7169203331';

/** A context carrying a (non-recording) sampled span with fixed ids. */
export function spanContext(): Context {
  return trace.setSpanContext(context.active(), { traceId: TRACE_ID, spanId: SPAN_ID, traceFlags: TraceFlags.SAMPLED, isRemote: false });
}

export async function waitFor<T>(fn: () => T | Promise<T>, { timeoutMs = 10_000, intervalMs = 25 } = {}): Promise<NonNullable<T>> {
  const t0 = Date.now();
  let last: unknown;
  for (;;) {
    try {
      const v = await fn();
      if (v) return v as NonNullable<T>;
    } catch (err) {
      last = err;
    }
    if (Date.now() - t0 > timeoutMs) throw new Error(`waitFor timed out${last ? `: ${String(last)}` : ''}`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}
