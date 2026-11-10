import { baseConfigSchema, csv } from '@evcharge/platform';
import { z } from 'zod';
import { parseRetryDelaysH } from './domain';

const posInt = (def: number) => z.coerce.number().int().positive().default(def);

export const billingConfigSchema = baseConfigSchema.extend({
  DATABASE_URL: z.string().min(1),
  KAFKA_BROKERS: csv(),
  TARIFF_URL: z.url(),
  SESSION_EVENTS_TOPIC: z.string().default('session.events.v1'),
  BILLING_EVENTS_TOPIC: z.string().default('billing.events.v1'),
  CONSUMER_GROUP: z.string().default('billing.session-events'),

  /** Authorization hold placed at session start, in minor units (50.00 EUR). */
  HOLD_AMOUNT: posInt(5000),
  HOLD_CURRENCY: z.string().length(3).default('EUR'),

  /** Mock PSP failure injection (ADR 0007). */
  PSP_TIMEOUT_RATE: z.coerce.number().min(0).max(1).default(0),
  PSP_DECLINE_TOKENS: csv().default([]),
  PSP_AUTH_DECLINE_TOKENS: csv().default([]),
  PSP_LATENCY_MS: z.coerce.number().int().min(0).default(0),
  /** Client-side timeout for one PSP call. A timeout is an unknown outcome and is retried with the same key. */
  PSP_CALL_TIMEOUT_MS: posInt(5000),

  TARIFF_TIMEOUT_MS: posInt(1000),
  TARIFF_ATTEMPTS: posInt(3),
  TARIFF_BREAKER_OPEN_MS: posInt(10_000),

  SAGA_WORKERS: posInt(4),
  SAGA_BATCH: posInt(20),
  /** Retry backoff for a failing step: base * 2^(attempt-1), capped (1 s -> 30 s). */
  SAGA_BACKOFF_BASE_MS: posInt(1000),
  SAGA_BACKOFF_MAX_MS: posInt(30_000),
  /** How long a session.completed is retried while tariff is unavailable (24 h). */
  SAGA_PRICING_DEADLINE_MS: posInt(24 * 3600 * 1000),
  SAGA_LEASE_MS: posInt(60_000),
  SAGA_IDLE_POLL_MS: posInt(250),

  DB_POOL_MAX: posInt(20),

  /** Dunning: retry a declined capture this many hours after the FIRST decline ("24,72"; "none" = fail at the first decline). */
  CAPTURE_RETRY_DELAYS_H: z
    .string()
    .default('24,72')
    .refine((v) => {
      try {
        parseRetryDelaysH(v);
        return true;
      } catch {
        return false;
      }
    }, 'CAPTURE_RETRY_DELAYS_H must be positive, strictly increasing hours, e.g. "24,72"')
    .transform(parseRetryDelaysH),

  /** VAT rate applied to invoices, in basis points (1900 = 19%, Germany). Tariff prices are gross. */
  VAT_RATE_BP: z.coerce.number().int().min(0).max(10_000).default(1900),
  /** Prefix of invoice numbers (EVC-2026-000042); credit notes use <prefix>-CN. */
  INVOICE_PREFIX: z.string().regex(/^[A-Z0-9]{2,10}$/).default('EVC'),
  /** telemetry-ingest base URL; absent = quotes are sent without a meter profile (time-weighted pricing). */
  TELEMETRY_URL: z.url().optional(),
  /**
   * AES-256 key (32 bytes, base64) for payment tokens at rest. Always required: there is no built-in key, so no
   * deployment can run on a key that is public in the source. docker-compose.yml sets a lab value; generate a real one
   * with `openssl rand -base64 32`.
   */
  TOKEN_ENC_KEY: z.string().refine((v) => Buffer.from(v, 'base64').length === 32, 'TOKEN_ENC_KEY must be 32 bytes, base64 encoded'),
});

export type BillingConfig = z.infer<typeof billingConfigSchema>;
