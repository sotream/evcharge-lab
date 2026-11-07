import { context, trace } from '@opentelemetry/api';
import { afterEach, describe, expect, it } from 'vitest';
import { currentTraceparent, extractTraceContext, injectTraceHeaders, inSpan } from '../../src/telemetry/propagation';
import { resetTelemetryForTests, startTelemetry } from '../../src/telemetry/telemetry';
import { enableContext, SPAN_ID, spanContext, TRACE_ID } from '../helpers';

describe('startTelemetry', () => {
  afterEach(() => resetTelemetryForTests());

  it('is a no-op without an endpoint', async () => {
    const saved = process.env['OTEL_EXPORTER_OTLP_ENDPOINT'];
    delete process.env['OTEL_EXPORTER_OTLP_ENDPOINT'];
    const t = startTelemetry({ serviceName: 'x' });
    expect(t.enabled).toBe(false);
    await t.shutdown();
    if (saved !== undefined) process.env['OTEL_EXPORTER_OTLP_ENDPOINT'] = saved;
  });

  it('starts the SDK when an endpoint is given and is idempotent', async () => {
    const t = startTelemetry({ serviceName: 'x', endpoint: 'http://127.0.0.1:1' });
    expect(t.enabled).toBe(true);
    expect(startTelemetry({ serviceName: 'y' })).toBe(t);
    await t.shutdown();
    await t.shutdown();
  });
});

describe('kafka trace propagation', () => {
  it('injects W3C traceparent from a context', () => {
    const h = injectTraceHeaders({ other: '1' }, spanContext());
    expect(h['traceparent']).toBe(`00-${TRACE_ID}-${SPAN_ID}-01`);
    expect(h['other']).toBe('1');
    expect(currentTraceparent(spanContext())).toBe(h['traceparent']);
  });

  it('injects nothing without an active span', () => {
    expect(injectTraceHeaders({}, context.active())).toEqual({});
  });

  it('extracts from Map<Buffer,Buffer> and plain-object headers', () => {
    const tp = `00-${TRACE_ID}-${SPAN_ID}-01`;
    for (const headers of [new Map([[Buffer.from('traceparent'), Buffer.from(tp)]]), { traceparent: tp }, new Map([['Traceparent', tp]])]) {
      const sc = trace.getSpanContext(extractTraceContext(headers, context.active()));
      expect(sc).toMatchObject({ traceId: TRACE_ID, spanId: SPAN_ID, isRemote: true });
    }
    expect(trace.getSpanContext(extractTraceContext({}, context.active()))).toBeUndefined();
  });

  it('inSpan runs under the parent context and rethrows', async () => {
    enableContext();
    const tp = await inSpan('op', { parent: spanContext() }, async () => currentTraceparent());
    // With the no-op tracer the child span carries the parent's span context.
    expect(tp).toContain(TRACE_ID);
    await expect(inSpan('op', {}, async () => Promise.reject(new Error('x')))).rejects.toThrow('x');
  });
});
