import { baseConfigSchema, loadConfig, type Env } from '@evcharge/platform';
import { z } from 'zod';

const secret = z.string().refine((s) => Buffer.byteLength(s) >= 32, 'must be at least 32 bytes');
const posInt = (def: number) => z.coerce.number().int().positive().default(def);

export const configSchema = baseConfigSchema.extend({
  SERVICE_NAME: z.string().min(1).default('driver-api'),
  DATABASE_URL: z.string().regex(/^postgres(ql)?:\/\//, 'must be a postgres:// URL'),
  REDIS_URL: z.string().regex(/^rediss?:\/\//, 'must be a redis:// URL'),
  SESSIONS_URL: z.url(),
  BILLING_URL: z.url(),
  TELEMETRY_URL: z.url(),
  /** HS256 signing key (ADR 0010). */
  JWT_SECRET: secret,
  /** Comma-separated retired keys, accepted for verification only (rotation window). */
  JWT_PREVIOUS_SECRETS: z
    .string()
    .default('')
    .transform((s) =>
      s
        .split(',')
        .map((x) => x.trim())
        .filter(Boolean),
    )
    .pipe(z.array(secret)),
  ACCESS_TOKEN_TTL_S: posInt(15 * 60),
  REFRESH_TOKEN_TTL_S: posInt(30 * 24 * 3600),
  LOGIN_RATE_LIMIT: posInt(10),
  LOGIN_RATE_WINDOW_S: posInt(60),
  /** Prefix for Redis keys (tests use a unique one). */
  REDIS_KEY_PREFIX: z.string().default('driver-api:'),
  REGISTER_RATE_LIMIT: posInt(10),
  REGISTER_RATE_WINDOW_S: posInt(60),
  /**
   * How many proxy hops in front of the service to trust for x-forwarded-for: a number (0 = none, use the socket
   * address) or a comma-separated list of proxy IPs/CIDRs. "true"/"all" is rejected on purpose: trusting every
   * hop lets a client choose its own rate-limit identity.
   */
  TRUST_PROXY: z
    .string()
    .default('0')
    .transform((v, ctx): number | string[] => {
      if (/^\d+$/.test(v)) return Number(v);
      const list = v.split(',').map((x) => x.trim()).filter(Boolean);
      if (list.length > 0 && list.every((x) => /^[0-9a-fA-F:.]+(\/\d{1,3})?$/.test(x))) return list;
      ctx.addIssue({ code: 'custom', message: 'must be a hop count (e.g. 1) or a comma-separated list of IPs/CIDRs' });
      return z.NEVER;
    }),
  /** ADR 0002: 3 s ceiling for writes. Writes are never retried on timeout: the client retries with the same key. */
  DOWNSTREAM_TIMEOUT_MS: posInt(3000),
  /** Reads: short per-attempt timeout and at most one retry, so the whole call stays within ~3 s. */
  READ_TIMEOUT_MS: posInt(1200),
  READ_RETRIES: z.coerce.number().int().min(0).max(2).default(1),
  /** Registration calls sessions before touching the database: one attempt plus one retry, 1 s each. */
  REGISTER_SESSIONS_TIMEOUT_MS: posInt(1000),
  /** A refresh token that was rotated less than this long ago is a client retry, not theft. */
  REFRESH_GRACE_S: posInt(5),
  /** Short cache for GET /v1/sites (read SLO: 1,000 RPS per instance). 0 disables it. */
  SITES_CACHE_TTL_MS: z.coerce.number().int().min(0).default(3000),
  /** ADR 0015: how long a write's commit LSN is forwarded as x-min-lsn on the driver's reads. 0 disables it. */
  MIN_LSN_TTL_MS: z.coerce.number().int().min(0).default(30_000),
  PG_POOL_MAX: z.coerce.number().int().min(2).max(200).default(20),
});
export type Config = z.infer<typeof configSchema>;

export const readConfig = (env: Env = process.env): Config => loadConfig(configSchema, env);
