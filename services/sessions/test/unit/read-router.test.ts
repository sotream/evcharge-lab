import type { Pool } from '@evcharge/platform';
import { describe, expect, it } from 'vitest';
import { isLsn, maxLsn, parseLsn, ReadRouter, replayedAtLeast } from '../../src/read-router';

const pool = (name: string) => ({ name }) as unknown as Pool;

describe('LSN comparison', () => {
  it('parses high and low words', () => {
    expect(parseLsn('0/0')).toBe(0n);
    expect(parseLsn('16/B374D848')).toBe((0x16n << 32n) | 0xb374d848n);
  });
  it('compares numerically, not as strings', () => {
    expect(replayedAtLeast('0/9', '0/A')).toBe(false);
    expect(replayedAtLeast('0/A', '0/A')).toBe(true);
    expect(replayedAtLeast('1/0', '0/FFFFFFFF')).toBe(true);
    expect(replayedAtLeast('A/0', '9/FFFFFFFF')).toBe(true);
  });
  it('rejects malformed input', () => {
    expect(() => parseLsn('nope')).toThrow(RangeError);
    expect(isLsn('16/B374D848')).toBe(true);
    expect(isLsn('16/B374D848; DROP')).toBe(false);
    expect(isLsn('')).toBe(false);
  });
  it('maxLsn picks the later one and tolerates undefined', () => {
    expect(maxLsn('0/9', '0/A')).toBe('0/A');
    expect(maxLsn(undefined, '0/A')).toBe('0/A');
    expect(maxLsn('0/9', undefined)).toBe('0/9');
    expect(maxLsn(undefined, undefined)).toBeUndefined();
  });
});

const clock = () => {
  let t = 0;
  return { now: () => t, sleep: async (ms: number) => void (t += ms) };
};

describe('ReadRouter.choose', () => {
  const primary = pool('primary');
  const replica = pool('replica');

  it('uses the primary for everything without a replica', async () => {
    const r = new ReadRouter(primary, undefined, { waitMs: 200, pollMs: 20 });
    expect((await r.choose()).target).toBe('primary');
    expect((await r.choose('0/10')).target).toBe('primary');
  });

  it('stale-tolerant reads use the replica without checking its position', async () => {
    let calls = 0;
    const r = new ReadRouter(primary, replica, { waitMs: 200, pollMs: 20, replayLsn: async () => (calls++, '0/0') });
    expect((await r.choose()).target).toBe('replica');
    expect(calls).toBe(0);
  });

  it('read-your-writes uses the replica once it has replayed min-lsn', async () => {
    const c = clock();
    const replayed = ['0/5', '0/8', '0/10'];
    const r = new ReadRouter(primary, replica, { waitMs: 200, pollMs: 20, ...c, replayLsn: async () => replayed.shift() ?? '0/10' });
    expect((await r.choose('0/10')).target).toBe('replica');
    expect(c.now()).toBe(40);
  });

  it('read-your-writes falls back to the primary after the timeout', async () => {
    const c = clock();
    const r = new ReadRouter(primary, replica, { waitMs: 200, pollMs: 20, ...c, replayLsn: async () => '0/5' });
    expect((await r.choose('0/10')).target).toBe('primary');
    expect(c.now()).toBe(200);
  });

  it('falls back immediately when the replica cannot be asked, or is not in recovery', async () => {
    const c = clock();
    const failing = new ReadRouter(primary, replica, {
      waitMs: 200,
      pollMs: 20,
      ...c,
      replayLsn: async () => {
        throw new Error('connection refused');
      },
    });
    expect((await failing.choose('0/10')).target).toBe('primary');
    expect(c.now()).toBe(0);
    const nullLsn = new ReadRouter(primary, replica, { waitMs: 60, pollMs: 20, ...clock(), replayLsn: async () => null });
    expect((await nullLsn.choose('0/10')).target).toBe('primary');
  });

  it('read() retries a failed replica read on the primary', async () => {
    const r = new ReadRouter(primary, replica, { waitMs: 200, pollMs: 20 });
    const seen: unknown[] = [];
    const out = await r.read(async (p) => {
      seen.push(p);
      if (p === replica) throw new Error('replica down');
      return 'ok';
    });
    expect(out).toBe('ok');
    expect(seen).toEqual([replica, primary]);
  });
});
