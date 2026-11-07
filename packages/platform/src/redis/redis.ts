import { Redis, type RedisOptions } from 'ioredis';
import type { Logger } from '../logging/logger';

export type { Redis };

export interface CreateRedisOptions {
  /** TCP connect timeout. Default 3000ms. */
  connectTimeoutMs?: number;
  /** Per-command timeout (rejects the command, connection stays). Default 2000ms. */
  commandTimeoutMs?: number;
  /** Retries per command while reconnecting. Default 2 (ioredis default is 20). */
  maxRetriesPerRequest?: number;
  logger?: Logger;
  /** Anything else passed through to ioredis. */
  options?: RedisOptions;
}

/**
 * ioredis client with connect/command timeouts, a bounded reconnect backoff and an error
 * listener (so connection errors are logged instead of crashing the process).
 */
export function createRedis(url: string, opts: CreateRedisOptions = {}): Redis {
  const client = new Redis(url, {
    connectTimeout: opts.connectTimeoutMs ?? 3000,
    commandTimeout: opts.commandTimeoutMs ?? 2000,
    maxRetriesPerRequest: opts.maxRetriesPerRequest ?? 2,
    retryStrategy: (times) => Math.min(50 * 2 ** times, 2000),
    enableOfflineQueue: true,
    ...opts.options,
  });
  let lastLogged = 0;
  client.on('error', (err: Error) => {
    if (Date.now() - lastLogged > 5000) {
      lastLogged = Date.now();
      opts.logger?.warn({ err: err.message }, 'redis error');
    }
  });
  return client;
}

/** Readiness check: PING must answer PONG. */
export function redisReadinessCheck(client: Redis): () => Promise<boolean> {
  return async () => (await client.ping()) === 'PONG';
}

/** Graceful close: QUIT (waits for pending replies), falling back to disconnect. */
export async function closeRedis(client: Redis): Promise<void> {
  try {
    await client.quit();
  } catch {
    client.disconnect();
  }
}
