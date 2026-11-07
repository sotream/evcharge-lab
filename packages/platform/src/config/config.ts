import { z } from 'zod';

export type Env = Record<string, string | undefined>;

/** Thrown by loadConfig; `issues` lists every invalid variable as "NAME: reason". */
export class ConfigError extends Error {
  override readonly name = 'ConfigError';
  constructor(readonly issues: string[]) {
    super(`Invalid configuration (${issues.length} problem${issues.length === 1 ? '' : 's'}):\n${issues.map((i) => `  - ${i}`).join('\n')}`);
  }
}

/**
 * Validates and coerces `env` against a zod object schema. Empty strings are treated as unset,
 * so `FOO=` falls back to the schema default. Throws ConfigError listing *every* bad variable.
 */
export function loadConfig<S extends z.ZodType>(schema: S, env: Env = process.env): z.infer<S> {
  const cleaned: Env = {};
  for (const [k, v] of Object.entries(env)) if (v !== undefined && v !== '') cleaned[k] = v;
  const res = schema.safeParse(cleaned);
  if (res.success) return res.data;
  const issues = res.error.issues.map((i) => {
    const name = i.path.length ? i.path.join('.') : '(root)';
    const missing = i.code === 'invalid_type' && cleaned[String(i.path[0])] === undefined;
    return `${name}: ${missing ? 'required but not set' : i.message}`;
  });
  throw new ConfigError(issues);
}

/** Comma-separated list, e.g. KAFKA_BROKERS=a:9092,b:9092 -> ['a:9092','b:9092']. */
export const csv = () =>
  z
    .string()
    .transform((s) => s.split(',').map((x) => x.trim()).filter(Boolean))
    .pipe(z.array(z.string()).min(1));

/** "true"/"false"/"1"/"0"/"yes"/"no" -> boolean (z.coerce.boolean treats "false" as true). */
export const envBool = () =>
  z.enum(['true', 'false', '1', '0', 'yes', 'no']).transform((v) => v === 'true' || v === '1' || v === 'yes');

export const LOG_LEVELS = ['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'] as const;
export type LogLevel = (typeof LOG_LEVELS)[number];

/** Variables every service has. Extend with `baseConfigSchema.extend({...})`. */
export const baseConfigSchema = z.object({
  SERVICE_NAME: z.string().min(1),
  LOG_LEVEL: z.enum(LOG_LEVELS).default('info'),
  HTTP_PORT: z.coerce.number().int().min(0).max(65535).default(8080),
  INTERNAL_TOKEN: z.string().min(16),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.url().optional(),
  SHUTDOWN_GRACE_MS: z.coerce.number().int().positive().default(15000),
});
export type BaseConfig = z.infer<typeof baseConfigSchema>;
