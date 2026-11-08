/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (ordering, retries, backoff)
import { SpanKind } from '@opentelemetry/api';
import { Consumer, deserializersFrom, type Message, type MessagesStream, stringDeserializer } from '@platformatic/kafka';
import type { z } from 'zod';
import type { Logger } from '../logging/logger';
import { counter, gauge, histogram, LATENCY_BUCKETS } from '../metrics/metrics';
import { extractTraceContext, inSpan } from '../telemetry/propagation';
import { errorMessage, fullJitterBackoff, sleep } from '../util';
import { createProducer, type KafkaProducer } from './producer';

export interface MessageContext {
  topic: string;
  partition: number;
  offset: bigint;
  key: string | undefined;
  headers: Record<string, string>;
  /** 1-based attempt number for this delivery. */
  attempt: number;
}

export interface ConsumerOptions<S extends z.ZodType> {
  brokers: string[];
  groupId: string;
  clientId?: string;
  topics: string[];
  /** Envelope schema (e.g. `events.StationEvent` from @evcharge/contracts) every value is decoded with. */
  schema: S;
  handler: (event: z.infer<S>, ctx: MessageContext) => Promise<void>;
  /** Partitions processed in parallel (each partition is strictly sequential). Default 8. */
  concurrency?: number;
  /**
   * Handler attempts before the message goes to `<topic>.dlq`. Default: unlimited. A handler error is
   * usually a dependency outage (DB, broker), so the partition blocks and retries with backoff
   * (the lag alert fires) instead of dead-lettering real events. Undecodable messages always go to
   * the DLQ at once.
   */
  maxAttempts?: number;
  retryBackoffMs?: number;
  retryBackoffMaxMs?: number;
  /** Producer for the DLQ; by default one is created (with topic auto-creation) and owned by the consumer. */
  dlqProducer?: KafkaProducer;
  /** Where to start when the group has no committed offset. Default 'earliest'. */
  startFrom?: 'earliest' | 'latest';
  sessionTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  /** How often kafka_consumer_lag is refreshed. Default 15s; 0 disables. */
  lagIntervalMs?: number;
  /** Buffered (fetched, not yet handled) messages above which fetching pauses. Default 1000. */
  maxBuffered?: number;
  /** How often processed offsets are committed (latest per partition). Default 250 ms. */
  commitIntervalMs?: number;
  logger?: Logger;
}

interface PartitionQueue {
  items: Message<string, string, string, string>[];
  running: boolean;
}

/**
 * At-least-once consumer: an offset is committed only after its handler succeeded (or the
 * message was dead-lettered). Ordering is preserved per partition; up to `concurrency`
 * partitions are processed at once.
 */
export class KafkaConsumer<S extends z.ZodType = z.ZodType> {
  client: Consumer<string, string, string, string>;
  private reconnecting = false;
  private starting = false;
  private stream: MessagesStream<string, string, string, string> | undefined;
  private readonly queues = new Map<string, PartitionQueue>();
  private readonly highestSeen = new Map<string, bigint>();
  private readonly ready: string[] = [];
  private active = 0;
  private buffered = 0;
  private paused = false;
  private stopping = false;
  private readonly stopCtl = new AbortController();
  private readonly idle = new Set<() => void>();
  private lagTimer: NodeJS.Timeout | undefined;
  private dlq: KafkaProducer | undefined;
  private readonly ownsDlq: boolean;
  private readonly concurrency: number;
  private readonly maxAttempts: number;
  private readonly maxBuffered: number;
  private readonly commitIntervalMs: number;
  private readonly pendingCommits = new Map<string, Message<string, string, string, string>>();
  private commitTimer: NodeJS.Timeout | undefined;

  private readonly mMessages = counter({ name: 'kafka_consumer_messages_total', help: 'Kafka messages consumed by outcome', labelNames: ['group', 'topic', 'outcome'] as const });
  private readonly mHandler = histogram({ name: 'kafka_consumer_handler_seconds', help: 'Kafka handler duration incl. retries', labelNames: ['group', 'topic'] as const, buckets: LATENCY_BUCKETS });
  private readonly mLag = gauge({ name: 'kafka_consumer_lag', help: 'Messages between the committed offset and the log end', labelNames: ['group', 'topic', 'partition'] as const });

  constructor(private readonly opts: ConsumerOptions<S>) {
    this.concurrency = Math.max(1, opts.concurrency ?? 8);
    this.maxAttempts = Math.max(1, opts.maxAttempts ?? Number.POSITIVE_INFINITY);
    this.maxBuffered = opts.maxBuffered ?? 1000;
    this.commitIntervalMs = opts.commitIntervalMs ?? 250;
    this.client = this.newClient();
    this.dlq = opts.dlqProducer;
    this.ownsDlq = !opts.dlqProducer;
  }

  private newClient(): Consumer<string, string, string, string> {
    const opts = this.opts;
    const client = new Consumer<string, string, string, string>({
      clientId: opts.clientId ?? opts.groupId,
      bootstrapBrokers: opts.brokers,
      groupId: opts.groupId,
      sessionTimeout: opts.sessionTimeoutMs ?? 30_000,
      heartbeatInterval: opts.heartbeatIntervalMs ?? 3000,
      autocommit: false,
      deserializers: deserializersFrom(stringDeserializer),
    });
    // The client emits 'error' itself when it gives up (for example joinGroup failed 4 times while brokers are down).
    // Unhandled, that event crashed the whole service in the two-broker drill. Log it and rebuild the client instead.
    client.on('error', (err: unknown) => {
      this.opts.logger?.error({ err: errorMessage(err), group: this.opts.groupId }, 'kafka consumer client error, reconnecting');
      void this.reconnect();
    });
    return client;
  }

  /** Drops the current client and stream and joins again with backoff, until it works or the consumer stops. */
  private async reconnect(): Promise<void> {
    if (this.reconnecting || this.stopping) return;
    this.reconnecting = true;
    try {
      const old = this.stream;
      this.stream = undefined;
      this.pendingCommits.clear(); // offsets of the old membership cannot be committed; redelivery is harmless
      await old?.close().catch(() => {});
      await this.client.close(true).catch(() => {});
      this.client = this.newClient();
      await this.joinWithRetry();
    } finally {
      this.reconnecting = false;
    }
  }

  private async joinWithRetry(): Promise<boolean> {
    for (let attempt = 1; !this.stopping; attempt++) {
      try {
        await this.join();
        if (attempt > 1) this.opts.logger?.info({ group: this.opts.groupId, attempt }, 'kafka consumer joined');
        return true;
      } catch (err) {
        this.opts.logger?.warn({ err: errorMessage(err), group: this.opts.groupId, attempt }, 'kafka consumer join failed, retrying');
        await sleep(fullJitterBackoff(attempt, 500, 15_000) + 250, this.stopCtl.signal);
      }
    }
    return false;
  }

  get running(): boolean {
    return this.stream !== undefined && !this.stopping;
  }

  /**
   * Joins the group. If the brokers are unreachable the first attempt fails and start() returns anyway: joining continues
   * in the background (readinessCheck reports it), so a service still starts and serves what does not need Kafka. This
   * is what ADR 0012 requires of the gateway while two brokers are down.
   */
  async start(): Promise<void> {
    if (this.stream || this.starting) return;
    this.starting = true;
    this.dlq ??= createProducer({ brokers: this.opts.brokers, clientId: `${this.opts.groupId}.dlq`, autocreateTopics: true });
    try {
      await this.join();
    } catch (err) {
      this.opts.logger?.warn({ err: errorMessage(err), group: this.opts.groupId }, 'kafka consumer could not join yet; retrying in the background');
      void this.joinWithRetry();
    }
  }

  private async join(): Promise<void> {
    const stream = await this.client.consume({
      topics: this.opts.topics,
      mode: 'committed',
      fallbackMode: this.opts.startFrom ?? 'earliest',
      autocommit: false,
      maxWaitTime: 500,
    });
    this.stream = stream;
    stream.on('data', (m) => this.enqueue(m));
    stream.on('error', (err) => this.opts.logger?.error({ err: errorMessage(err), group: this.opts.groupId }, 'kafka consumer stream error'));
    const lagMs = this.opts.lagIntervalMs ?? 15_000;
    if (lagMs > 0 && !this.lagTimer) {
      this.lagTimer = setInterval(() => void this.refreshLag(), lagMs);
      this.lagTimer.unref();
    }
  }

  /** Readiness: started, not stopping, and an active member of its group. */
  readinessCheck(): () => boolean {
    return () => this.running && this.client.isActive();
  }

  /**
   * Stops fetching, drops messages not yet started (they are redelivered later), waits for
   * in-flight handlers, flushes commits and leaves the group.
   */
  async stop(): Promise<void> {
    if (this.stopping) return;
    this.stopping = true;
    if (this.lagTimer) clearInterval(this.lagTimer);
    this.stream?.pause();
    for (const q of this.queues.values()) {
      this.buffered -= q.items.length;
      q.items.length = 0;
    }
    this.ready.length = 0;
    this.stopCtl.abort();
    if (this.active > 0) await new Promise<void>((resolve) => this.idle.add(resolve));
    await this.flushCommits();
    await this.stream?.close().catch(() => {});
    await this.client.close().catch(() => {});
    if (this.ownsDlq) await this.dlq?.close().catch(() => {});
  }

  /** Recomputes kafka_consumer_lag now (also runs on an interval). */
  async refreshLag(): Promise<void> {
    if (!this.running || !this.client.isActive()) return;
    try {
      const lag = await this.client.getLag({ topics: this.opts.topics });
      for (const [topic, parts] of lag) {
        parts.forEach((l, partition) => {
          const labels = { group: this.opts.groupId, topic, partition: String(partition) };
          if (l >= 0n) this.mLag.set(labels, Number(l));
          else this.mLag.remove(labels);
        });
      }
    } catch (err) {
      this.opts.logger?.debug({ err: errorMessage(err) }, 'lag refresh failed');
    }
  }

  private enqueue(m: Message<string, string, string, string>): void {
    if (this.stopping) return;
    const key = `${m.topic}:${m.partition}`;
    // After a rebalance the stream may refetch from the committed offset; skip what is already queued.
    const seen = this.highestSeen.get(key);
    if (seen !== undefined && m.offset <= seen) return;
    this.highestSeen.set(key, m.offset);
    let q = this.queues.get(key);
    if (!q) {
      q = { items: [], running: false };
      this.queues.set(key, q);
    }
    q.items.push(m);
    this.buffered++;
    if (!q.running && !this.ready.includes(key)) this.ready.push(key);
    if (this.buffered >= this.maxBuffered && !this.paused) {
      this.paused = true;
      this.stream?.pause();
    }
    this.pump();
  }

  private pump(): void {
    while (this.active < this.concurrency && this.ready.length > 0 && !this.stopping) {
      const key = this.ready.shift() as string;
      const q = this.queues.get(key);
      if (!q || q.running || q.items.length === 0) continue;
      q.running = true;
      this.active++;
      void this.drain(key, q);
    }
  }

  /** Processes one partition queue until empty (sequentially), then frees the concurrency slot. */
  private async drain(key: string, q: PartitionQueue): Promise<void> {
    try {
      while (q.items.length > 0 && !this.stopping) {
        const m = q.items.shift() as Message<string, string, string, string>;
        this.buffered--;
        if (this.paused && this.buffered < this.maxBuffered / 2) {
          this.paused = false;
          this.stream?.resume();
        }
        await this.process(m);
      }
    } finally {
      q.running = false;
      this.active--;
      if (q.items.length > 0 && !this.stopping) this.ready.push(key);
      if (this.active === 0) for (const r of this.idle) r();
      this.pump();
    }
  }

  private async process(m: Message<string, string, string, string>): Promise<void> {
    const { topic, partition, offset } = m;
    const group = this.opts.groupId;
    const headers: Record<string, string> = {};
    for (const [k, v] of m.headers) headers[String(k)] = String(v);
    const t0 = performance.now();

    let event: z.infer<S>;
    try {
      const parsed = this.opts.schema.safeParse(JSON.parse(m.value ?? ''));
      if (!parsed.success) throw new Error(`schema validation failed: ${parsed.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; ')}`);
      event = parsed.data;
    } catch (err) {
      this.opts.logger?.warn({ topic, partition, offset: String(offset), err: errorMessage(err) }, 'undecodable message, dead-lettering');
      if (await this.deadLetter(m, headers, 'decode', err, 0)) {
        this.mMessages.inc({ group, topic, outcome: 'dlq' });
        await this.commit(m);
      }
      return;
    }

    const parent = extractTraceContext(m.headers);
    let lastErr: unknown;
    for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
      if (this.stopping && attempt > 1) return; // not committed: redelivered after restart
      try {
        await inSpan(`${topic} process`, { kind: SpanKind.CONSUMER, parent, attributes: { 'messaging.system': 'kafka', 'messaging.destination.name': topic, 'messaging.consumer.group.name': group, 'messaging.kafka.offset': Number(offset), attempt } }, () =>
          this.opts.handler(event, { topic, partition, offset, key: m.key, headers, attempt }),
        );
        this.mHandler.observe({ group, topic }, (performance.now() - t0) / 1000);
        this.mMessages.inc({ group, topic, outcome: 'ok' });
        await this.commit(m);
        return;
      } catch (err) {
        lastErr = err;
        this.mMessages.inc({ group, topic, outcome: 'error' });
        this.opts.logger?.warn({ topic, partition, offset: String(offset), attempt, err: errorMessage(err) }, 'kafka handler failed');
        if (attempt < this.maxAttempts) await sleep(fullJitterBackoff(attempt, this.opts.retryBackoffMs ?? 100, this.opts.retryBackoffMaxMs ?? 30_000), this.stopCtl.signal);
      }
    }
    if (await this.deadLetter(m, headers, 'handler', lastErr, this.maxAttempts)) {
      this.mMessages.inc({ group, topic, outcome: 'dlq' });
      await this.commit(m);
    }
  }

  /** Publishes the original record to `<topic>.dlq`, retrying until acked or stopping. */
  private async deadLetter(m: Message<string, string, string, string>, headers: Record<string, string>, reason: string, err: unknown, attempts: number): Promise<boolean> {
    const dlqTopic = `${m.topic}.dlq`;
    for (let i = 0; ; i++) {
      try {
        await (this.dlq as KafkaProducer).send([
          {
            topic: dlqTopic,
            key: m.key ?? '',
            value: m.value ?? '',
            headers: {
              ...headers,
              'dlq-reason': reason,
              'dlq-error': errorMessage(err).slice(0, 1000),
              'dlq-attempts': String(attempts),
              'dlq-source-topic': m.topic,
              'dlq-source-partition': String(m.partition),
              'dlq-source-offset': String(m.offset),
              'dlq-consumer-group': this.opts.groupId,
            },
          },
        ]);
        this.opts.logger?.error({ topic: m.topic, partition: m.partition, offset: String(m.offset), dlqTopic, reason }, 'message dead-lettered');
        return true;
      } catch (e) {
        this.opts.logger?.error({ err: errorMessage(e), dlqTopic }, 'DLQ publish failed, retrying');
        if (this.stopping) return false;
        await sleep(fullJitterBackoff(i, 200, 5000), this.stopCtl.signal);
      }
    }
  }

  /**
   * Marks m as processed. Offsets are committed in batches (the latest processed message per partition, every
   * commitIntervalMs and on stop) instead of one OffsetCommit per message, which at a few hundred messages/s cost the
   * brokers more CPU than the messages themselves. A crash redelivers at most one interval of messages; handlers are
   * idempotent (ADR 0002).
   */
  private async commit(m: Message<string, string, string, string>): Promise<void> {
    this.pendingCommits.set(`${m.topic}:${m.partition}`, m);
    this.commitTimer ??= setTimeout(() => void this.flushCommits(), this.commitIntervalMs);
  }

  private async flushCommits(): Promise<void> {
    if (this.commitTimer) clearTimeout(this.commitTimer);
    this.commitTimer = undefined;
    const batch = [...this.pendingCommits.values()];
    this.pendingCommits.clear();
    for (const m of batch) {
      try {
        await m.commit();
      } catch (err) {
        // Typically a rebalance: the partition moved; the new owner re-processes (at-least-once).
        this.opts.logger?.warn({ topic: m.topic, partition: m.partition, offset: String(m.offset), err: errorMessage(err) }, 'offset commit failed');
      }
    }
  }
}

export function createConsumer<S extends z.ZodType>(opts: ConsumerOptions<S>): KafkaConsumer<S> {
  return new KafkaConsumer(opts);
}
