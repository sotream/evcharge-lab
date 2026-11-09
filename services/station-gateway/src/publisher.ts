import { SpanKind } from '@opentelemetry/api';
import { Topics } from '@evcharge/contracts';
import { injectTraceHeaders, inSpan, type Envelope, type KafkaProducer, type Logger } from '@evcharge/platform';
import type { GatewayMetrics } from './metrics';
import { errorMessage, kafkaErrorReason } from './util';

export interface GatewayTopics {
  stationEvents: string;
  meterValues: string;
  commands: string;
  commandResults: string;
  rawLog: string;
}

export const defaultTopics = (): GatewayTopics => ({
  stationEvents: Topics.StationEvents,
  meterValues: Topics.MeterValues,
  commands: Topics.StationCommands,
  commandResults: Topics.StationCommandResults,
  rawLog: Topics.OcppRawLog,
});

/** Longest raw-log frame kept in full, in characters. */
const RAW_FRAME_MAX_CHARS = 4096;

/** The bounded produce buffer is full: answer the CALL with InternalError (ADR 0012). */
export class BufferFullError extends Error {
  override readonly name = 'BufferFullError';
}

export interface PublisherOptions {
  instance: string;
  capacity: number;
  rawCapacity: number;
  rawEnabled: boolean;
  topics: GatewayTopics;
  logger: Logger;
  metrics: GatewayMetrics;
}

/**
 * Kafka publishing with ADR 0012 backpressure. Every awaited publish holds one slot of a bounded
 * buffer until the broker acknowledges (acks=all); with the buffer full, publish throws
 * BufferFullError instead of queueing more. Raw-log records are fire-and-forget on their own
 * producer and are dropped first: when their own buffer is full or the main buffer is half full.
 */
export class EventPublisher {
  private inFlight = 0;
  private rawInFlight = 0;

  constructor(
    private readonly producer: KafkaProducer,
    private readonly rawProducer: KafkaProducer | undefined,
    private readonly opts: PublisherOptions,
  ) {}

  get depth(): number {
    return this.inFlight;
  }

  /** Resolves after the broker ack. Throws BufferFullError or the producer's error. */
  async publish(topic: string, envelope: Envelope): Promise<void> {
    const { metrics } = this.opts;
    if (this.inFlight >= this.opts.capacity) {
      metrics.produceRejected.inc({ topic });
      throw new BufferFullError(`produce buffer full (${this.opts.capacity})`);
    }
    this.inFlight++;
    metrics.produceBuffer.set({ gateway: this.opts.instance }, this.inFlight);
    try {
      // Own PRODUCER span (not sendEnvelope's) so the outcome is recorded; same record shape as sendEnvelope.
      await inSpan(
        `${topic} publish`,
        { kind: SpanKind.PRODUCER, attributes: { 'messaging.system': 'kafka', 'messaging.destination.name': topic, 'messaging.kafka.message.key': envelope.key, 'messaging.message.id': envelope.eventId } },
        async (span) => {
          try {
            await this.producer.send([{ topic, key: envelope.key, value: JSON.stringify(envelope), headers: { ...injectTraceHeaders({}), 'event-type': envelope.type, 'event-id': envelope.eventId } }]);
            span.setAttribute('kafka.outcome', 'ok');
          } catch (err) {
            const reason = kafkaErrorReason(err);
            span.setAttribute('kafka.outcome', reason === 'other' ? 'error' : reason);
            throw err;
          }
        },
      );
    } catch (err) {
      metrics.produceErrors.inc({ topic, reason: kafkaErrorReason(err) });
      throw err;
    } finally {
      this.inFlight--;
      metrics.produceBuffer.set({ gateway: this.opts.instance }, this.inFlight);
    }
  }

  /** For events nobody waits on (connect/disconnect): failures are logged, never thrown. */
  publishQuietly(topic: string, envelope: Envelope): void {
    this.publish(topic, envelope).catch((err: unknown) => this.opts.logger.warn({ topic, type: envelope.type, key: envelope.key, err: errorMessage(err) }, 'event not published'));
  }

  /** OcppRawLogRecord, key stationId, no envelope (ADR 0003). Never delays the caller. */
  rawLog(stationId: string, direction: 'in' | 'out', frame: string): void {
    if (!this.opts.rawEnabled || !this.rawProducer) return;
    const { metrics } = this.opts;
    if (this.rawInFlight >= this.opts.rawCapacity || this.inFlight >= this.opts.capacity / 2) {
      metrics.rawLogDropped.inc({ reason: 'buffer_full' });
      return;
    }
    this.rawInFlight++;
    // Frames up to 64 KB were copied whole; with rawCapacity of them in flight that is hundreds of MB. Keep the head only.
    const truncated = frame.length > RAW_FRAME_MAX_CHARS;
    const value = JSON.stringify({ stationId, gatewayInstance: this.opts.instance, direction, at: new Date().toISOString(), frame: truncated ? frame.slice(0, RAW_FRAME_MAX_CHARS) : frame, ...(truncated ? { truncated: true, originalChars: frame.length } : {}) });
    this.rawProducer
      .send([{ topic: this.opts.topics.rawLog, key: stationId, value }])
      .catch(() => metrics.rawLogDropped.inc({ reason: 'broker_error' }))
      .finally(() => this.rawInFlight--);
  }
}
