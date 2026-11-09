import type { Queryable } from '../postgres/postgres';

/** Include in a service migration (e.g. `0002_inbox.sql`), or run it from code. */
export const PROCESSED_EVENTS_DDL = `
CREATE TABLE IF NOT EXISTS processed_events (
  consumer text NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE INDEX IF NOT EXISTS processed_events_processed_at_idx ON processed_events (processed_at);
`;

export type InboxResult<T> = { duplicate: true } | { duplicate: false; result: T };

/**
 * Idempotent consumption: inserts (consumer, eventId) and runs fn only if the row was new.
 * `client` MUST be inside a transaction (see withTx) that also holds fn's writes, so the marker
 * and the side effects commit or roll back together. A concurrent duplicate blocks on the
 * primary key until the first transaction finishes, then is skipped (or runs, if that rolled back).
 */
export async function withInbox<T>(client: Queryable, consumerName: string, eventId: string, fn: () => Promise<T>): Promise<InboxResult<T>> {
  const res = await client.query('INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING', [consumerName, eventId]);
  if (res.rowCount === 0) return { duplicate: true };
  return { duplicate: false, result: await fn() };
}

/** The dead-letter topics keep records 14 days (infra/kafka/topics.sh): a republished record must still be recognised as a duplicate. */
export const PROCESSED_EVENTS_RETENTION_MS = 14 * 864e5;

/** Deletes markers older than `olderThanMs` (keep longer than any possible redelivery window). */
export async function purgeProcessedEvents(db: Queryable, olderThanMs: number): Promise<number> {
  const res = await db.query('DELETE FROM processed_events WHERE processed_at < now() - make_interval(secs => $1::double precision)', [olderThanMs / 1000]);
  return res.rowCount ?? 0;
}
