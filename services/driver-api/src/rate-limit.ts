import type { Redis } from '@evcharge/platform';

export interface RateLimitResult {
  allowed: boolean;
  retryAfterS: number;
}

/**
 * Fixed-window counter in Redis: INCR on `<prefix>rl:<name>:<id>:<window>` with an expiry set on first use.
 * Fails open when Redis is unavailable (login availability beats brute-force protection for a short outage;
 * the scrypt cost still bounds guessing). shortcut: fixed windows allow up to 2x the limit across a window
 * boundary; switch to a sliding window if that matters.
 */
export class RateLimiter {
  constructor(
    private readonly redis: Redis,
    private readonly prefix: string,
    private readonly limit: number,
    private readonly windowS: number,
    private readonly now: () => number = () => Date.now(),
    private readonly onError: (err: unknown) => void = () => {},
  ) {}

  /** `over` replaces the default limit/window for this call (register has its own). */
  async hit(name: string, id: string, over: { limit: number; windowS: number } = { limit: this.limit, windowS: this.windowS }): Promise<RateLimitResult> {
    const { limit, windowS } = over;
    const nowS = Math.floor(this.now() / 1000);
    const window = Math.floor(nowS / windowS);
    const key = `${this.prefix}rl:${name}:${id}:${window}`;
    try {
      const res = await this.redis.multi().incr(key).expire(key, windowS, 'NX').exec();
      const count = Number(res?.[0]?.[1] ?? 0);
      return { allowed: count <= limit, retryAfterS: (window + 1) * windowS - nowS };
    } catch (err) {
      this.onError(err);
      return { allowed: true, retryAfterS: 0 };
    }
  }
}
