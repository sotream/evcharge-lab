import { SpanKind } from '@opentelemetry/api';
import { Producer, ProduceAcks, serializersFrom, stringSerializer } from '@platformatic/kafka';
import { counter } from '../metrics/metrics';
import { injectTraceHeaders, inSpan } from '../telemetry/propagation';
import { withTimeout } from '../util';
import type { Envelope } from './envelope';

export type Compression = 'lz4' | 'snappy' | 'gzip' | 'zstd' | 'none';

export interface ProducerOptions {
  /** Attempts per request on retriable errors such as NOT_LEADER_OR_FOLLOWER. Default 10. */
  retries?: number;
  /** Delay between those attempts. Default 100 ms (the client default of 1000 ms stalled produces during leader moves). */
  retryDelayMs?: number;
  /** How long cached cluster metadata is trusted. Client default (5000 ms); 1000 ms did not shorten leader-move stalls. */
  metadataMaxAgeMs?: number;
  brokers: string[];
  clientId: string;
  /** Default lz4. */
  compression?: Compression;
  /** Max time a message waits to be batched with others before the produce request. Default 5ms. */
  lingerMs?: number;
  /** Flush immediately once this many messages are queued. Default 500. */
  maxBatchSize?: number;
  /** Per-request timeout. Default 10s. */
  timeoutMs?: number;
  /**
   * Overall deadline for a send incl. the client's internal metadata/retry loops (e.g. an unknown
   * topic retries indefinitely). Default 15s, below the outbox's idle-in-transaction timeout.
   */
  sendTimeoutMs?: number;
  /** Let the broker auto-create missing topics (dev / DLQ topics). Default false. */
  autocreateTopics?: boolean;
}

export interface OutgoingMessage {
  topic: string;
  key: string;
  /** Already-serialised value (JSON for envelopes). */
  value: string;
  headers?: Record<string, string>;
}

interface Pending {
  msg: OutgoingMessage;
  resolve: () => void;
  reject: (err: unknown) => void;
}

/**
 * Idempotent (acks=all) producer with lz4 compression and a small linger buffer.
 * Every send() promise resolves only after the broker acknowledged the batch it was part of.
 */
export class KafkaProducer {
  readonly client: Producer<string, string, string, string>;
  private queue: Pending[] = [];
  private timer: NodeJS.Timeout | undefined;
  private inFlight = new Set<Promise<void>>();
  private closed = false;
  private readonly lingerMs: number;
  private readonly maxBatchSize: number;
  private readonly sendTimeoutMs: number;
  private readonly sent = counter({ name: 'kafka_producer_messages_total', help: 'Kafka messages produced', labelNames: ['topic', 'outcome'] as const });

  constructor(opts: ProducerOptions) {
    this.lingerMs = opts.lingerMs ?? 5;
    this.maxBatchSize = opts.maxBatchSize ?? 500;
    this.sendTimeoutMs = opts.sendTimeoutMs ?? 15_000;
    this.client = new Producer<string, string, string, string>({
      clientId: opts.clientId,
      bootstrapBrokers: opts.brokers,
      idempotent: true,
      acks: ProduceAcks.ALL,
      compression: opts.compression ?? 'lz4',
      timeout: opts.timeoutMs ?? 10_000,
      // The client defaults (retries 3, retryDelay 1000 ms) stalled every produce for about 4.4 s when partition leadership
      // moved (load/repro/leader-move.sh). A short delay with more attempts recovers within a few hundred ms.
      retries: opts.retries ?? 10,
      retryDelay: opts.retryDelayMs ?? 100,
      ...(opts.metadataMaxAgeMs !== undefined ? { metadataMaxAge: opts.metadataMaxAgeMs } : {}),
      autocreateTopics: opts.autocreateTopics ?? false,
      serializers: serializersFrom(stringSerializer),
    });
  }

  /** Queue messages; resolves when all are acknowledged (rejects if their batch failed). */
  send(messages: OutgoingMessage[]): Promise<void> {
    if (this.closed) return Promise.reject(new Error('producer is closed'));
    if (messages.length === 0) return Promise.resolve();
    const all = messages.map((msg) => new Promise<void>((resolve, reject) => this.queue.push({ msg, resolve, reject })));
    if (this.queue.length >= this.maxBatchSize) this.flush();
    else this.timer ??= setTimeout(() => this.flush(), this.lingerMs);
    return Promise.all(all).then(() => undefined);
  }

  /** JSON-encodes the envelope; key = envelope.key; adds `traceparent` from the active span. */
  sendEnvelope(topic: string, envelope: Envelope, opts: { headers?: Record<string, string> } = {}): Promise<void> {
    return inSpan(`${topic} publish`, { kind: SpanKind.PRODUCER, attributes: { 'messaging.system': 'kafka', 'messaging.destination.name': topic, 'messaging.message.id': envelope.eventId } }, () =>
      this.send([
        {
          topic,
          key: envelope.key,
          value: JSON.stringify(envelope),
          headers: { ...opts.headers, ...injectTraceHeaders({}), 'event-type': envelope.type, 'event-id': envelope.eventId },
        },
      ]),
    );
  }

  /** Readiness: fetch cluster metadata (fails if no broker is reachable). */
  readinessCheck(): () => Promise<void> {
    return async () => {
      await this.client.metadata({ topics: [] });
    };
  }

  /** Flushes queued messages, waits for in-flight batches, then closes connections. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.flush();
    this.closed = true;
    await Promise.allSettled(this.inFlight);
    // A send abandoned by sendTimeoutMs may still be looping inside the client: force after 5s.
    await withTimeout(this.client.close(), 5000, 'kafka producer close').catch(() => this.client.close(true));
  }

  private flush(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    while (this.queue.length > 0) {
      const batch = this.queue.splice(0, this.maxBatchSize);
      const p: Promise<void> = this.sendBatch(batch).finally(() => this.inFlight.delete(p));
      this.inFlight.add(p);
    }
  }

  /** Never rejects: settles each queued message's own promise instead. */
  private async sendBatch(batch: Pending[]): Promise<void> {
    try {
      const send = this.client.send({ messages: batch.map((b) => ({ topic: b.msg.topic, key: b.msg.key, value: b.msg.value, headers: b.msg.headers ?? {} })) });
      await withTimeout(send, this.sendTimeoutMs, 'kafka send');
    } catch (failure) {
      for (const b of batch) {
        this.sent.inc({ topic: b.msg.topic, outcome: 'error' });
        b.reject(failure);
      }
      return;
    }
    for (const b of batch) {
      this.sent.inc({ topic: b.msg.topic, outcome: 'ok' });
      b.resolve();
    }
  }
}

export function createProducer(opts: ProducerOptions): KafkaProducer {
  return new KafkaProducer(opts);
}
