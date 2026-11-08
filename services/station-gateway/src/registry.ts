import { RedisKeys } from '@evcharge/contracts';
import type { Redis } from '@evcharge/platform';

/** The epoch counter lives in its own key without TTL, so it keeps growing when the route key expires or is deleted. */
export const routeEpochKey = (stationId: string): string => `route:epoch:${stationId}`;

/**
 * Registration: INCR the epoch key (KEYS[2], no TTL), take ownership, set the TTL.
 * Returns {epoch, previous owner or ''} so a takeover from another instance can evict the old socket.
 */
const REGISTER = `
local prev = redis.call('HGET', KEYS[1], 'instance')
local epoch = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1], 'instance', ARGV[1], 'connectedAt', ARGV[2], 'epoch', epoch)
redis.call('EXPIRE', KEYS[1], ARGV[3])
return {epoch, prev or ''}
`;

/**
 * Refresh: extend the TTL only while this instance still holds its epoch (1). A key that vanished
 * (TTL, Redis restart) is recreated with the same epoch (2). Another owner means this socket was
 * replaced elsewhere (0).
 */
const REFRESH = `
local inst = redis.call('HGET', KEYS[1], 'instance')
if not inst then
  redis.call('HSET', KEYS[1], 'instance', ARGV[1], 'epoch', ARGV[2], 'connectedAt', ARGV[3])
  redis.call('EXPIRE', KEYS[1], ARGV[4])
  return 2
end
if inst == ARGV[1] and redis.call('HGET', KEYS[1], 'epoch') == ARGV[2] then
  redis.call('EXPIRE', KEYS[1], ARGV[4])
  return 1
end
return 0
`;

/** Compare-and-delete: a late close of a replaced connection cannot remove the newer route (ADR 0004). */
const UNREGISTER = `
if redis.call('HGET', KEYS[1], 'instance') == ARGV[1] and redis.call('HGET', KEYS[1], 'epoch') == ARGV[2] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

interface Scripts {
  gwRouteRegister(key: string, epochKey: string, instance: string, connectedAt: string, ttlS: number): Promise<[number, string]>;
  gwRouteRefresh(key: string, instance: string, epoch: number, connectedAt: string, ttlS: number): Promise<number>;
  gwRouteUnregister(key: string, instance: string, epoch: number): Promise<number>;
}
interface ScriptPipeline {
  gwRouteRefresh(key: string, instance: string, epoch: number, connectedAt: string, ttlS: number): ScriptPipeline;
  exec(): Promise<[Error | null, unknown][] | null>;
}

export interface Route {
  instance: string;
  epoch: number;
  connectedAt: string;
}

export interface OwnedRoute {
  stationId: string;
  epoch: number;
  connectedAt: string;
}

export class RouteRegistry {
  private readonly r: Redis & Scripts;

  constructor(
    redis: Redis,
    private readonly instance: string,
    private readonly opts: { ttlS: number; deadAfterS: number; batchSize?: number },
  ) {
    for (const [name, lua, keys] of [
      ['gwRouteRegister', REGISTER, 2],
      ['gwRouteRefresh', REFRESH, 1],
      ['gwRouteUnregister', UNREGISTER, 1],
    ] as const) {
      if (!(name in redis)) redis.defineCommand(name, { numberOfKeys: keys, lua });
    }
    this.r = redis as Redis & Scripts;
  }

  async register(stationId: string, connectedAt: string): Promise<{ epoch: number; previousInstance: string | undefined }> {
    const [epoch, prev] = await this.r.gwRouteRegister(RedisKeys.stationRoute(stationId), routeEpochKey(stationId), this.instance, connectedAt, this.opts.ttlS);
    return { epoch: Number(epoch), previousInstance: prev ? prev : undefined };
  }

  async unregister(stationId: string, epoch: number): Promise<boolean> {
    return (await this.r.gwRouteUnregister(RedisKeys.stationRoute(stationId), this.instance, epoch)) === 1;
  }

  /**
   * Pipelined TTL refresh for every owned connection (ADR 0004: every 30 s, in batches).
   * Returns the station ids whose route now belongs to someone else.
   */
  async refresh(routes: OwnedRoute[]): Promise<{ refreshed: number; recreated: number; lost: string[]; failed: number }> {
    const size = this.opts.batchSize ?? 500;
    const out = { refreshed: 0, recreated: 0, lost: [] as string[], failed: 0 };
    for (let i = 0; i < routes.length; i += size) {
      const batch = routes.slice(i, i + size);
      const p = this.r.pipeline() as unknown as ScriptPipeline;
      for (const rt of batch) p.gwRouteRefresh(RedisKeys.stationRoute(rt.stationId), this.instance, rt.epoch, rt.connectedAt, this.opts.ttlS);
      // oxlint-disable-next-line no-await-in-loop -- batches are sequential on purpose (bounded Redis burst)
      const res = await p.exec();
      batch.forEach((rt, j) => {
        const [err, v] = res?.[j] ?? [new Error('no reply'), undefined];
        if (err) out.failed++;
        else if (v === 1) out.refreshed++;
        else if (v === 2) out.recreated++;
        else out.lost.push(rt.stationId);
      });
    }
    return out;
  }

  async lookup(stationId: string): Promise<Route | null> {
    const h = await this.r.hgetall(RedisKeys.stationRoute(stationId));
    if (!h['instance']) return null;
    return { instance: h['instance'], epoch: Number(h['epoch'] ?? 0), connectedAt: h['connectedAt'] ?? '' };
  }

  /** ZADD gw:instances <now> <id> (ADR 0004: every 5 s). */
  async heartbeat(nowMs = Date.now()): Promise<void> {
    await this.r.zadd(RedisKeys.gatewayInstances, nowMs, this.instance);
  }

  async removeInstance(): Promise<void> {
    await this.r.zrem(RedisKeys.gatewayInstances, this.instance);
  }

  /** An instance is alive if its heartbeat is newer than INSTANCE_DEAD_AFTER_S (15 s). */
  async isInstanceAlive(instance: string, nowMs = Date.now()): Promise<boolean> {
    const score = await this.r.zscore(RedisKeys.gatewayInstances, instance);
    return score !== null && nowMs - Number(score) <= this.opts.deadAfterS * 1000;
  }
}
