/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (ordering, retries, backoff)
import { createHash } from 'node:crypto';
import { type Pool, type PoolClient, type Queryable, withTx } from '../postgres/postgres';

/** Include in a service migration. */
export const IDEMPOTENCY_KEYS_DDL = `
CREATE TABLE IF NOT EXISTS idempotency_keys (
  scope text NOT NULL,
  key text NOT NULL,
  request_hash text NOT NULL,
  status_code integer NULL,
  response_body jsonb NULL,
  locked_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz NULL,
  PRIMARY KEY (scope, key)
);
CREATE INDEX IF NOT EXISTS idempotency_keys_created_at_idx ON idempotency_keys (created_at);
`;

export interface StoredResponse {
  status: number;
  body: unknown;
}

export interface IdempotencyRequest {
  /** Namespace for keys, e.g. "POST /sessions" (keys are unique per scope). */
  scope: string;
  key: string;
  /** hashRequest(...) of the request; a different hash under the same key is a 422. */
  requestHash: string;
  /** An unfinished claim older than this is considered abandoned (crashed holder) and is taken over. Default 60s. */
  lockTtlMs?: number;
}

/** Same key is currently being processed by another request (HTTP 409). */
export class IdempotencyInFlightError extends Error {
  override readonly name = 'IdempotencyInFlightError';
  readonly status = 409;
}

/** Same key was used with a different request body (HTTP 422). */
export class IdempotencyMismatchError extends Error {
  override readonly name = 'IdempotencyMismatchError';
  readonly status = 422;
}

function canonical(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(canonical);
  if (v && typeof v === 'object') {
    return Object.fromEntries(
      Object.keys(v as Record<string, unknown>)
        .sort()
        .map((k) => [k, canonical((v as Record<string, unknown>)[k])]),
    );
  }
  return v;
}

/** sha256 over method, path and the body as canonical JSON (key order does not matter). */
export function hashRequest(method: string, path: string, body: unknown): string {
  return createHash('sha256')
    .update(`${method.toUpperCase()} ${path}\n${JSON.stringify(canonical(body ?? null))}`)
    .digest('hex');
}

export type BeginResult = { kind: 'new' } | { kind: 'replay'; response: StoredResponse };

/**
 * Claims the key (autocommitted, so concurrent duplicates can see it). Returns 'new' when the
 * caller should process the request, 'replay' with the stored response for a completed
 * duplicate; throws IdempotencyMismatchError / IdempotencyInFlightError otherwise.
 */
export async function beginIdempotent(db: Queryable, req: IdempotencyRequest): Promise<BeginResult> {
  const ttlSecs = (req.lockTtlMs ?? 60_000) / 1000;
  for (let i = 0; i < 3; i++) {
    const ins = await db.query('INSERT INTO idempotency_keys (scope, key, request_hash) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING', [req.scope, req.key, req.requestHash]);
    if (ins.rowCount === 1) return { kind: 'new' };
    const { rows } = await db.query<{ request_hash: string; status_code: number | null; response_body: unknown; completed: boolean; stale: boolean; locked_at: string }>(
      `SELECT request_hash, status_code, response_body, completed_at IS NOT NULL AS completed,
              locked_at < clock_timestamp() - make_interval(secs => $3::double precision) AS stale, locked_at::text AS locked_at
         FROM idempotency_keys WHERE scope = $1 AND key = $2`,
      [req.scope, req.key, ttlSecs],
    );
    const row = rows[0];
    if (!row) continue; // released between our INSERT and SELECT: try again
    if (row.request_hash !== req.requestHash) throw new IdempotencyMismatchError(`idempotency key "${req.key}" was already used with a different request`);
    if (row.completed) return { kind: 'replay', response: { status: row.status_code ?? 200, body: row.response_body } };
    if (row.stale) {
      const took = await db.query('UPDATE idempotency_keys SET locked_at = clock_timestamp() WHERE scope = $1 AND key = $2 AND completed_at IS NULL AND locked_at = $3::timestamptz', [req.scope, req.key, row.locked_at]);
      if (took.rowCount === 1) return { kind: 'new' };
    }
    throw new IdempotencyInFlightError(`a request with idempotency key "${req.key}" is still in progress`);
  }
  throw new IdempotencyInFlightError(`a request with idempotency key "${req.key}" is still in progress`);
}

/** Stores the response; call inside the business transaction for atomicity. */
export async function completeIdempotent(db: Queryable, scope: string, key: string, response: StoredResponse): Promise<void> {
  await db.query('UPDATE idempotency_keys SET status_code = $3, response_body = $4::jsonb, completed_at = now() WHERE scope = $1 AND key = $2', [
    scope,
    key,
    response.status,
    JSON.stringify(response.body ?? null),
  ]);
}

/** Drops an unfinished claim so the client may retry (used when processing threw). */
export async function releaseIdempotent(db: Queryable, scope: string, key: string): Promise<void> {
  await db.query('DELETE FROM idempotency_keys WHERE scope = $1 AND key = $2 AND completed_at IS NULL', [scope, key]);
}

/**
 * Full idempotent execution: claim -> run fn in a transaction -> store its response in the same
 * transaction (so side effects and the stored response commit together). A thrown error releases
 * the claim. Replays return the stored response with `replayed: true`.
 */
export async function withIdempotency(pool: Pool, req: IdempotencyRequest, fn: (client: PoolClient) => Promise<StoredResponse>): Promise<{ replayed: boolean; response: StoredResponse }> {
  const begin = await beginIdempotent(pool, req);
  if (begin.kind === 'replay') return { replayed: true, response: begin.response };
  try {
    const response = await withTx(pool, async (client) => {
      const r = await fn(client);
      await completeIdempotent(client, req.scope, req.key, r);
      return r;
    });
    return { replayed: false, response };
  } catch (err) {
    await releaseIdempotent(pool, req.scope, req.key).catch(() => {});
    throw err;
  }
}

/** Deletes keys older than `olderThanMs` (the replay window you promise clients, e.g. 24h). */
export async function purgeIdempotencyKeys(db: Queryable, olderThanMs: number): Promise<number> {
  const res = await db.query('DELETE FROM idempotency_keys WHERE created_at < now() - make_interval(secs => $1::double precision)', [olderThanMs / 1000]);
  return res.rowCount ?? 0;
}
