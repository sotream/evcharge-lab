/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (ordering, retries, backoff)
import { Client } from 'pg';
import type { Envelope } from '../kafka/envelope';
import type { KafkaProducer } from '../kafka/producer';
import type { Logger } from '../logging/logger';
import { counter, gauge, histogram } from '../metrics/metrics';
import { type Pool, type Queryable, withTx } from '../postgres/postgres';
import { injectTraceHeaders } from '../telemetry/propagation';
import { errorMessage, fullJitterBackoff, sleep } from '../util';

export const OUTBOX_CHANNEL = 'outbox';

/**
 * Commit-order column (ADR 0002). `tx` is the inserting transaction id. The relay publishes only rows
 * whose transaction finished before every transaction still running, ordered by (tx, id). Per-key
 * order then follows causal order even when ids are allocated out of commit order. Apply in
 * a migration of every service whose outbox table predates it (idempotent).
 */
export const OUTBOX_TX_ORDER_DDL = `
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS tx xid8 NOT NULL DEFAULT pg_current_xact_id();
DROP INDEX IF EXISTS outbox_unpublished_idx;
CREATE INDEX IF NOT EXISTS outbox_unpublished_tx_idx ON outbox (tx, id) WHERE published_at IS NULL;
`;

/** Advisory lock key (any constant): only one relay per database publishes at a time. */
export const OUTBOX_RELAY_LOCK_KEY = 7_201_811_080;

/** Include in a service migration. The trigger NOTIFYs relays on commit of any insert. */
export const OUTBOX_DDL = `
CREATE TABLE IF NOT EXISTS outbox (
  id bigserial PRIMARY KEY,
  topic text NOT NULL,
  key text NOT NULL,
  payload jsonb NOT NULL,
  headers jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz NULL
);
CREATE INDEX IF NOT EXISTS outbox_published_at_idx ON outbox (published_at) WHERE published_at IS NOT NULL;
${OUTBOX_TX_ORDER_DDL}
CREATE OR REPLACE FUNCTION outbox_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('${OUTBOX_CHANNEL}', '');
  RETURN NULL;
END $$;
CREATE OR REPLACE TRIGGER outbox_notify_trg AFTER INSERT ON outbox FOR EACH STATEMENT EXECUTE FUNCTION outbox_notify();
`;

export interface OutboxMessage {
  topic: string;
  /** Defaults to event.key. */
  key?: string;
  event: Envelope;
  headers?: Record<string, string>;
}

/**
 * Inserts an event into the outbox using the business transaction's client, so the event is
 * published if and only if the transaction commits. Captures the active traceparent.
 * Returns the outbox row id.
 */
export async function enqueueOutbox(client: Queryable, msg: OutboxMessage): Promise<string> {
  const headers = { ...msg.headers, ...injectTraceHeaders({}), 'event-type': msg.event.type, 'event-id': msg.event.eventId };
  const { rows } = await client.query<{ id: string }>('INSERT INTO outbox (topic, key, payload, headers) VALUES ($1, $2, $3::jsonb, $4::jsonb) RETURNING id', [
    msg.topic,
    msg.key ?? msg.event.key,
    JSON.stringify(msg.event),
    JSON.stringify(headers),
  ]);
  return (rows[0] as { id: string }).id;
}

export interface OutboxRelayOptions {
  pool: Pool;
  producer: KafkaProducer;
  logger?: Logger;
  /** Rows claimed per cycle. Default 100. */
  batchSize?: number;
  /** Fallback poll interval when no NOTIFY arrives. Default 1000ms. */
  pollIntervalMs?: number;
  /** Use LISTEN/NOTIFY for low-latency wake-up (one dedicated connection). Default true. */
  listen?: boolean;
  /** Connection string for the LISTEN connection. Defaults to the pool's connection string. */
  listenConnectionString?: string;
  /** Published rows older than this are deleted. Default 24h. */
  retentionMs?: number;
  /** How often cleanup runs. Default 60s; 0 disables. */
  cleanupIntervalMs?: number;
}

interface Row {
  id: string;
  topic: string;
  key: string;
  payload: unknown;
  headers: Record<string, string>;
  created_at: Date;
}

/**
 * Publishes outbox rows to Kafka. Each cycle claims up to batchSize unpublished rows with
 * `FOR UPDATE SKIP LOCKED` (so several relay instances split the work and never publish the same
 * row concurrently), produces them, waits for the acks, marks them published and commits.
 * If producing fails the transaction rolls back and the rows are retried (at-least-once).
 */
export class OutboxRelay {
  private running = false;
  private loop: Promise<void> | undefined;
  private wake: (() => void) | undefined;
  private listener: Client | undefined;
  private cleanupTimer: NodeJS.Timeout | undefined;
  private stopCtl = new AbortController();
  private lastPendingAt = 0;
  private readonly batchSize: number;
  private readonly pollIntervalMs: number;

  private readonly mPending = gauge({ name: 'outbox_pending', help: 'Unpublished outbox rows' });
  private readonly mPublished = counter({ name: 'outbox_published_total', help: 'Outbox rows published to Kafka', labelNames: ['topic'] as const });
  private readonly mLag = histogram({ name: 'outbox_publish_lag_seconds', help: 'Time from outbox insert to Kafka ack', buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60] });

  constructor(private readonly opts: OutboxRelayOptions) {
    this.batchSize = opts.batchSize ?? 100;
    this.pollIntervalMs = opts.pollIntervalMs ?? 1000;
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.stopCtl = new AbortController();
    if (this.opts.listen !== false) await this.connectListener();
    const cleanupMs = this.opts.cleanupIntervalMs ?? 60_000;
    if (cleanupMs > 0) {
      this.cleanupTimer = setInterval(() => void this.cleanup().catch((err) => this.opts.logger?.warn({ err: errorMessage(err) }, 'outbox cleanup failed')), cleanupMs);
      this.cleanupTimer.unref();
    }
    this.loop = this.run();
  }

  /** Finishes the current cycle, then stops. */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.stopCtl.abort();
    this.wake?.();
    if (this.cleanupTimer) clearInterval(this.cleanupTimer);
    await this.loop;
    const l = this.listener;
    this.listener = undefined;
    await l?.end().catch(() => {});
  }

  /** One claim/publish/mark cycle. Returns the number of rows published. */
  async runOnce(): Promise<number> {
    const published = await withTx(this.opts.pool, async (client) => {
      // Single active relay per database: a second replica's cycle is a no-op (keeps per-key order).
      const lock = await client.query<{ ok: boolean }>('SELECT pg_try_advisory_xact_lock($1) AS ok', [OUTBOX_RELAY_LOCK_KEY]);
      if (!lock.rows[0]?.ok) return [];
      const { rows } = await client.query<Row>(
        `SELECT id, topic, key, payload, headers, created_at FROM outbox
          WHERE published_at IS NULL AND tx < pg_snapshot_xmin(pg_current_snapshot())
          ORDER BY tx, id LIMIT $1 FOR UPDATE`,
        [this.batchSize],
      );
      if (rows.length === 0) return [];
      await this.opts.producer.send(rows.map((r) => ({ topic: r.topic, key: r.key, value: JSON.stringify(r.payload), headers: r.headers ?? {} })));
      await client.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])', [rows.map((r) => r.id)]);
      return rows;
    });
    const now = Date.now();
    for (const r of published) {
      this.mPublished.inc({ topic: r.topic });
      this.mLag.observe(Math.max(0, now - r.created_at.getTime()) / 1000);
    }
    return published.length;
  }

  /** Deletes published rows older than retentionMs (in chunks). Returns rows deleted. */
  async cleanup(): Promise<number> {
    const secs = (this.opts.retentionMs ?? 864e5) / 1000;
    let total = 0;
    for (;;) {
      const res = await this.opts.pool.query(
        `DELETE FROM outbox WHERE id IN (
           SELECT id FROM outbox WHERE published_at IS NOT NULL AND published_at < now() - make_interval(secs => $1::double precision) LIMIT 5000)`,
        [secs],
      );
      total += res.rowCount ?? 0;
      if ((res.rowCount ?? 0) < 5000) return total;
    }
  }

  async refreshPending(): Promise<number> {
    const { rows } = await this.opts.pool.query<{ n: string }>('SELECT count(*) AS n FROM outbox WHERE published_at IS NULL');
    const n = Number(rows[0]?.n ?? 0);
    this.mPending.set(n);
    this.lastPendingAt = Date.now();
    return n;
  }

  private async run(): Promise<void> {
    let failures = 0;
    while (this.running) {
      try {
        const n = await this.runOnce();
        failures = 0;
        if (Date.now() - this.lastPendingAt >= this.pollIntervalMs) await this.refreshPending();
        if (n >= this.batchSize) continue; // more waiting: go again immediately
        await this.waitForWork();
      } catch (err) {
        failures++;
        this.opts.logger?.error({ err: errorMessage(err), failures }, 'outbox relay cycle failed');
        await sleep(fullJitterBackoff(failures, 100, 10_000) + 50, this.stopCtl.signal);
      }
    }
  }

  private async waitForWork(): Promise<void> {
    await new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(t);
        resolve();
      };
      const t = setTimeout(done, this.pollIntervalMs);
      this.wake = done;
    });
    this.wake = undefined;
  }

  private scheduleReconnect(ms: number): void {
    if (!this.running) return;
    setTimeout(() => void this.connectListener(), ms).unref();
  }

  private async onListenerLost(client: Client, cause: Error): Promise<void> {
    this.opts.logger?.warn({ err: cause.message }, 'outbox LISTEN connection lost, reconnecting');
    if (this.listener === client) this.listener = undefined;
    await client.end().catch(() => {});
    this.scheduleReconnect(1000);
  }

  private async connectListener(): Promise<void> {
    const connectionString = this.opts.listenConnectionString ?? (this.opts.pool.options as { connectionString?: string }).connectionString;
    if (!connectionString) return;
    const client = new Client({ connectionString, connectionTimeoutMillis: 3000 });
    client.on('notification', () => this.wake?.());
    client.on('error', (cause: Error) => void this.onListenerLost(client, cause));
    try {
      await client.connect();
      await client.query(`LISTEN ${OUTBOX_CHANNEL}`);
      this.listener = client;
      if (!this.running) await client.end();
    } catch (err) {
      this.opts.logger?.warn({ err: errorMessage(err) }, 'outbox LISTEN unavailable, polling only');
      await client.end().catch(() => {});
      this.scheduleReconnect(5000);
    }
  }
}
