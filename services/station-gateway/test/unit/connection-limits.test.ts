import { createLogger, type KafkaProducer } from '@evcharge/platform';
import { describe, expect, it, vi } from 'vitest';
import type { WebSocket } from 'ws';
import { AllocationTracker } from '../../src/charging';
import { gatewayMetrics } from '../../src/metrics';
import { defaultTopics, EventPublisher } from '../../src/publisher';
import type { StationCredential } from '../../src/sessions-client';
import { StationConnection } from '../../src/station-connection';

const logger = createLogger({ service: 'u', level: 'silent' });
const metrics = gatewayMetrics();

/** A connection whose Kafka produces never finish, so every MeterValues CALL stays unanswered. */
function connection(ws: Partial<WebSocket> & { frames?: string[] }) {
  const producer = { send: () => new Promise<void>(() => {}) } as unknown as KafkaProducer;
  return new StationConnection('ST-1', { readyState: 1, bufferedAmount: 0, send: (t: string) => ws.frames?.push(t), terminate: () => {}, ...ws } as unknown as WebSocket, {} as StationCredential, {
    instance: 'gw-test',
    publisher: new EventPublisher(producer, undefined, { instance: 'gw-test', capacity: 1000, rawCapacity: 0, rawEnabled: false, topics: defaultTopics(), logger, metrics }),
    sessions: {} as never,
    topics: defaultTopics(),
    metrics,
    logger,
    allocations: new AllocationTracker(),
    commandQueue: 4,
    callTimeoutMs: 1000,
    replayThresholdS: 60,
  });
}
const meter = (id: string) => Buffer.from(JSON.stringify([2, id, 'MeterValues', { connectorId: 1, meterValue: [{ timestamp: new Date().toISOString(), sampledValue: [{ value: '1' }] }] }]));

describe('per-station limits', () => {
  it('answers CALLs beyond 16 unanswered ones with a CALLERROR instead of queueing them', () => {
    const frames: string[] = [];
    const conn = connection({ frames });
    for (let i = 0; i < 20; i++) conn.onMessage(meter(`m-${i}`), false);
    const errors = frames.map((f) => JSON.parse(f) as unknown[]).filter((f) => f[0] === 4);
    expect(errors.map((e) => e[1])).toEqual(['m-16', 'm-17', 'm-18', 'm-19']);
    expect(errors[0]?.[2]).toBe('InternalError');
  });

  it('closes a socket whose unsent bytes exceed 1 MiB instead of buffering more', async () => {
    const terminate = vi.fn<() => void>();
    const frames: string[] = [];
    const conn = connection({ frames, bufferedAmount: 2 * 1024 * 1024, terminate });
    conn.onMessage(Buffer.from(JSON.stringify([2, 'h-1', 'Heartbeat', {}])), false);
    await vi.waitFor(() => expect(terminate).toHaveBeenCalledTimes(1));
    expect(frames).toHaveLength(0);
  });
});
