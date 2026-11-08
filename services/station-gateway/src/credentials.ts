import { createHash, timingSafeEqual } from 'node:crypto';
import { api, RedisKeys } from '@evcharge/contracts';
import type { Logger, Redis } from '@evcharge/platform';
import type { GatewayMetrics } from './metrics';
import type { SessionsClient, StationCredential } from './sessions-client';
import { errorMessage, TtlLru } from './util';

export interface CredentialStoreOptions {
  lruTtlMs: number;
  lruMax: number;
  redisTtlS: number;
  /** Unknown stations are remembered briefly so a flood of bad ids does not hammer sessions. */
  negativeTtlMs?: number;
}

/**
 * Station credential lookup (ADR 0003): 30 s in-process LRU -> Redis `cred:station:{id}` (5 min)
 * -> sessions `GET /internal/stations/:id/credential`. Concurrent lookups of one station share a
 * single request (single flight), so a reconnect storm costs one sessions read per station.
 */
export class CredentialStore {
  private readonly lru: TtlLru<StationCredential | null>;
  private readonly inflight = new Map<string, Promise<StationCredential | null>>();

  constructor(
    private readonly redis: Redis,
    private readonly sessions: SessionsClient,
    private readonly opts: CredentialStoreOptions,
    private readonly logger: Logger,
    private readonly metrics: GatewayMetrics,
  ) {
    this.lru = new TtlLru(opts.lruMax);
  }

  /** null = unknown station. Throws when neither Redis nor sessions can answer. */
  get(stationId: string): Promise<StationCredential | null> {
    const cached = this.lru.get(stationId);
    if (cached !== undefined) {
      this.metrics.credentials.inc({ source: 'lru' });
      return Promise.resolve(cached);
    }
    let p = this.inflight.get(stationId);
    if (!p) {
      p = this.load(stationId).finally(() => this.inflight.delete(stationId));
      this.inflight.set(stationId, p);
    }
    return p;
  }

  private async load(stationId: string): Promise<StationCredential | null> {
    const key = RedisKeys.stationCredential(stationId);
    try {
      const raw = await this.redis.get(key);
      if (raw) {
        const parsed = api.SessionsInternal.stationCredential.safeParse(JSON.parse(raw));
        if (parsed.success) {
          this.metrics.credentials.inc({ source: 'redis' });
          this.lru.set(stationId, parsed.data, this.opts.lruTtlMs);
          return parsed.data;
        }
      }
    } catch (err) {
      this.logger.warn({ stationId, err: errorMessage(err) }, 'credential cache read failed; asking sessions');
    }
    const cred = await this.sessions.credential(stationId);
    this.metrics.credentials.inc({ source: cred ? 'sessions' : 'sessions_unknown' });
    if (!cred) {
      this.lru.set(stationId, null, this.opts.negativeTtlMs ?? 5000);
      return null;
    }
    this.lru.set(stationId, cred, this.opts.lruTtlMs);
    this.redis.set(key, JSON.stringify(cred), 'EX', this.opts.redisTtlS).catch((err: unknown) => this.logger.warn({ stationId, err: errorMessage(err) }, 'credential cache write failed'));
    return cred;
  }
}

/** Constant-time check of SHA-256(password) against the stored hex digest (ADR 0003). */
export function verifySecret(password: string, secretSha256: string): boolean {
  const expected = Buffer.from(secretSha256, 'hex');
  const given = createHash('sha256').update(password, 'utf8').digest();
  return expected.length === given.length && timingSafeEqual(expected, given);
}

/** Parses `Authorization: Basic base64(user:pass)`; undefined when absent or malformed. */
export function parseBasicAuth(header: string | undefined): { username: string; password: string } | undefined {
  if (!header) return undefined;
  const m = /^Basic\s+([A-Za-z0-9+/=]+)\s*$/i.exec(header);
  if (!m?.[1]) return undefined;
  const decoded = Buffer.from(m[1], 'base64').toString('utf8');
  const idx = decoded.indexOf(':');
  if (idx < 0) return undefined;
  return { username: decoded.slice(0, idx), password: decoded.slice(idx + 1) };
}
