import { baseConfigSchema, csv, envBool, loadConfig, type Env } from '@evcharge/platform';
import { z } from 'zod';

export const configSchema = baseConfigSchema.extend({
  SERVICE_NAME: z.string().min(1).default('sessions'),
  DATABASE_URL: z.string().regex(/^postgres(ql)?:\/\//, 'must be a postgres:// URL'),
  /** Optional hot standby (ADR 0015). Unset or empty: every read uses the primary. */
  REPLICA_DATABASE_URL: z.string().regex(/^postgres(ql)?:\/\//, 'must be a postgres:// URL').or(z.literal('')).optional(),
  REPLICA_PG_POOL_MAX: z.coerce.number().int().min(2).max(200).default(20),
  /** Read-your-writes: longest wait for the replica to replay x-min-lsn before reading the primary. */
  REPLICA_RYW_TIMEOUT_MS: z.coerce.number().int().min(0).max(5000).default(200),
  REPLICA_RYW_POLL_MS: z.coerce.number().int().min(1).max(1000).default(20),
  PG_POOL_MAX: z.coerce.number().int().min(2).max(200).default(20),
  KAFKA_BROKERS: csv(),
  REDIS_URL: z.string().regex(/^rediss?:\/\//, 'must be a redis:// URL'),
  SMART_CHARGING_URL: z.url(),
  /** ADR 0002: 300 ms caller timeout for the allocator. */
  SMART_CHARGING_TIMEOUT_MS: z.coerce.number().int().positive().default(300),
  /** Deterministic lab credentials (ADR 0003). */
  STATION_SECRET_SEED: z.string().min(8),
  SEED_SITES: z.coerce.number().int().min(0).max(10_000).default(20),
  SEED_STATIONS_PER_SITE: z.coerce.number().int().min(1).max(40).default(10),
  SEED_DC_SHARE: z.coerce.number().min(0).max(1).default(0.2),
  /** Accept idTags with no registered driver as ad-hoc charging (driverless, invoiced unpaid). Default off. */
  ACCEPT_UNKNOWN_IDTAGS: envBool().default(false),
  /** How often each instance looks for sites due for allocation. */
  ALLOCATION_TICK_MS: z.coerce.number().int().positive().default(1000),
  /** Periodic reallocation per site (ADR 0006: 60 s). */
  ALLOCATION_PERIOD_S: z.coerce.number().int().positive().default(60),
  ALLOCATION_CONCURRENCY: z.coerce.number().int().min(1).max(64).default(8),
  /** Separate pool for allocation rounds (bulkhead for the OCPP hot path). */
  ALLOCATION_PG_POOL_MAX: z.coerce.number().int().min(2).max(100).default(10),
  /** Reservation sweeper (ADR 0005: every 5 s). */
  SWEEPER_INTERVAL_MS: z.coerce.number().int().positive().default(5000),
  /** Run Kafka consumers and background workers (tests may disable). */
  WORKERS_ENABLED: envBool().default(true),
});
export type Config = z.infer<typeof configSchema>;

export function readConfig(env: Env = process.env): Config {
  return loadConfig(configSchema, env);
}
