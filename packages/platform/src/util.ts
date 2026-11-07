/** Small internal helpers shared by several modules (not part of the public API). */

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const t = setTimeout(done, ms);
    function done() {
      clearTimeout(t);
      signal?.removeEventListener('abort', done);
      resolve();
    }
    signal?.addEventListener('abort', done, { once: true });
  });
}

export class TimeoutError extends Error {
  override readonly name = 'TimeoutError';
}

/** Rejects with TimeoutError if `p` does not settle within `ms`. */
export function withTimeout<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new TimeoutError(`${what} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([p, timeout]).finally(() => clearTimeout(timer));
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Exponential backoff with full jitter: random in [0, min(max, base * 2^attempt)). */
export function fullJitterBackoff(attempt: number, baseMs: number, maxMs: number, random: () => number = Math.random): number {
  const cap = Math.min(maxMs, baseMs * 2 ** attempt);
  return Math.floor(random() * cap);
}
