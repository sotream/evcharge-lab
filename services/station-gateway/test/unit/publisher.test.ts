import { createLogger, registry, type Envelope, type KafkaProducer } from '@evcharge/platform';
import { describe, expect, it } from 'vitest';
import { gatewayMetrics } from '../../src/metrics';
import { BufferFullError, defaultTopics, EventPublisher } from '../../src/publisher';
import { kafkaErrorReason } from '../../src/util';

const env = { eventId: 'e', type: 't', version: 1, occurredAt: new Date().toISOString(), producer: 'p', key: 'k', data: {} } as unknown as Envelope;
const logger = createLogger({ service: 'u', level: 'silent' });
const notEnough = Object.assign(new Error('Produce failed'), { cause: { errors: [{ apiId: 'NOT_ENOUGH_REPLICAS' }] } });

const publisher = (producer: KafkaProducer, instance: string, capacity = 2) =>
  new EventPublisher(producer, undefined, { instance, capacity, rawCapacity: 0, rawEnabled: false, topics: defaultTopics(), logger, metrics: gatewayMetrics() });

/**
 * Real NOT_ENOUGH_REPLICAS cannot be provoked on the compose cluster: Kafka 4.3 accepts a topic with
 * min.insync.replicas=4 and replication factor 3 but does not enforce it above the replication factor
 * (the produce succeeds). So the error path is covered with a producer that fails the way the client does.
 */
describe('EventPublisher failure handling (ADR 0012)', () => {
  it('classifies NOT_ENOUGH_REPLICAS inside nested client errors, and timeouts', () => {
    expect(kafkaErrorReason(notEnough)).toBe('not_enough_replicas');
    expect(kafkaErrorReason(new Error('request timed out'))).toBe('timeout');
    expect(kafkaErrorReason(new Error('boom'))).toBe('other');
  });

  it('rethrows the producer error, counts the reason, frees the buffer slot and keeps working', async () => {
    let fail = true;
    const producer = { send: () => (fail ? Promise.reject(notEnough) : Promise.resolve()) } as unknown as KafkaProducer;
    const pub = publisher(producer, 'unit');
    await expect(pub.publish('t', env)).rejects.toBe(notEnough);
    expect(pub.depth).toBe(0);
    expect(await registry.metrics()).toMatch(/gateway_produce_errors_total\{[^}]*reason="not_enough_replicas"[^}]*\} [1-9]/);
    fail = false;
    await expect(pub.publish('t', env)).resolves.toBeUndefined();
  });

  it('rejects with BufferFullError when every slot is waiting for an ack', async () => {
    const producer = { send: () => new Promise(() => {}) } as unknown as KafkaProducer;
    const pub = publisher(producer, 'unit2');
    void pub.publish('t', env);
    void pub.publish('t', env);
    await expect(pub.publish('t', env)).rejects.toBeInstanceOf(BufferFullError);
    expect(pub.depth).toBe(2);
  });
});
