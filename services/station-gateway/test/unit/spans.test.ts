import { context, SpanKind, trace } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { InMemorySpanExporter, SimpleSpanProcessor, TracerProvider, type ReadableSpan } from '@opentelemetry/sdk-trace';
import { createLogger, type KafkaProducer } from '@evcharge/platform';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import type { WebSocket } from 'ws';
import { AllocationTracker } from '../../src/charging';
import { gatewayMetrics } from '../../src/metrics';
import { defaultTopics, EventPublisher } from '../../src/publisher';
import { SiteDrawTracker } from '../../src/site-draw';
import type { StationCredential } from '../../src/sessions-client';
import { StationConnection } from '../../src/station-connection';

const exporter = new InMemorySpanExporter();

beforeAll(() => {
  context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  trace.setGlobalTracerProvider(new TracerProvider({ spanProcessors: [new SimpleSpanProcessor({ exporter })] }));
});

const sent: { topic: string; key: string; headers?: Record<string, string> }[] = [];
const producer = { send: (m: { topic: string; key: string; headers?: Record<string, string> }[]) => (sent.push(...m), Promise.resolve()) } as unknown as KafkaProducer;
const logger = createLogger({ service: 'u', level: 'silent' });
const metrics = gatewayMetrics();
const frames: string[] = [];
const ws = { readyState: 1, send: (t: string) => frames.push(t) } as unknown as WebSocket;
const conn = new StationConnection('ST-1', ws, {} as StationCredential, {
  instance: 'gw-test',
  publisher: new EventPublisher(producer, undefined, { instance: 'gw-test', capacity: 10, rawCapacity: 0, rawEnabled: false, topics: defaultTopics(), logger, metrics }),
  sessions: {} as never,
  topics: defaultTopics(),
  metrics,
  logger,
  allocations: new AllocationTracker(),
  siteDraw: new SiteDrawTracker(metrics, 'test'),
  commandQueue: 4,
  callTimeoutMs: 1000,
  replayThresholdS: 60,
});

const call = async (uniqueId: string, action: string, payload: object): Promise<ReadableSpan> => {
  conn.onMessage(Buffer.from(JSON.stringify([2, uniqueId, action, payload])), false);
  await vi.waitFor(() => expect(exporter.getFinishedSpans().some((s) => s.name === `ocpp ${action}`)).toBe(true));
  return exporter.getFinishedSpans().find((s) => s.name === `ocpp ${action}`) as ReadableSpan;
};

describe('station-gateway spans', () => {
  it('Heartbeat: one SERVER span with call attributes and no children', async () => {
    const span = await call('u-hb', 'Heartbeat', {});
    expect(span.kind).toBe(SpanKind.SERVER);
    expect(span.attributes).toMatchObject({ 'ocpp.action': 'Heartbeat', 'ocpp.unique_id': 'u-hb', 'ocpp.station_id': 'ST-1', 'ocpp.gateway': 'gw-test', 'ocpp.result': 'ok' });
    expect(exporter.getFinishedSpans().filter((s) => s.parentSpanContext?.spanId === span.spanContext().spanId)).toHaveLength(0);
  });

  it('MeterValues: PRODUCER child per awaited produce, traceparent in headers, ocpp.replayed set', async () => {
    const old = new Date(Date.now() - 3600_000).toISOString();
    const span = await call('u-mv', 'MeterValues', { connectorId: 1, meterValue: [{ timestamp: old, sampledValue: [{ value: '10' }] }] });
    expect(span.attributes).toMatchObject({ 'ocpp.action': 'MeterValues', 'ocpp.result': 'ok', 'ocpp.replayed': true });
    const topic = defaultTopics().meterValues;
    const child = exporter.getFinishedSpans().find((s) => s.name === `${topic} publish`) as ReadableSpan;
    expect(child.kind).toBe(SpanKind.PRODUCER);
    expect(child.parentSpanContext?.spanId).toBe(span.spanContext().spanId);
    expect(child.spanContext().traceId).toBe(span.spanContext().traceId);
    expect(child.attributes).toMatchObject({ 'messaging.destination.name': topic, 'messaging.kafka.message.key': 'ST-1', 'kafka.outcome': 'ok' });
    expect(sent.at(-1)?.headers?.['traceparent']).toContain(child.spanContext().spanId);
  });

  it('outbound command: CLIENT span with the outcome', () => {
    conn.command('RemoteStopTransaction', { transactionId: 7 }, () => {});
    const uniqueId = (JSON.parse(frames.at(-1) as string) as unknown[])[1] as string;
    conn.onMessage(Buffer.from(JSON.stringify([3, uniqueId, { status: 'Accepted' }])), false);
    const span = exporter.getFinishedSpans().find((s) => s.name === 'ocpp RemoteStopTransaction call') as ReadableSpan;
    expect(span.kind).toBe(SpanKind.CLIENT);
    expect(span.attributes).toMatchObject({ 'ocpp.action': 'RemoteStopTransaction', 'ocpp.station_id': 'ST-1', 'ocpp.result': 'Accepted' });
  });
});
