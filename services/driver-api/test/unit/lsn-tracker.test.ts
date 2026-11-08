import { describe, expect, it } from 'vitest';
import { LsnTracker } from '../../src/lsn-tracker';

const clock = () => {
  let t = 1000;
  return { now: () => t, advance: (ms: number) => void (t += ms) };
};

describe('LsnTracker (ADR 0015)', () => {
  it('returns nothing before any write', () => {
    expect(new LsnTracker(30_000).minLsn('d1', 'session:a')).toBeUndefined();
  });

  it('returns the later of the driver and the resource LSN', () => {
    const t = new LsnTracker(30_000);
    t.record('d1', '0/20', 'session:a');
    t.record('d1', '0/30'); // a later write without a resource, for example a remote start
    expect(t.minLsn('d1', 'session:a')).toBe('0/30');
    expect(t.minLsn('d1')).toBe('0/30');
    expect(t.minLsn('d2', 'session:a')).toBeUndefined();
  });

  it('never moves backwards when responses arrive out of order', () => {
    const t = new LsnTracker(30_000);
    t.record('d1', '1/0');
    t.record('d1', '0/FFFFFFFF');
    expect(t.minLsn('d1')).toBe('1/0');
  });

  it('expires after the TTL', () => {
    const c = clock();
    const t = new LsnTracker(5000, c.now);
    t.record('d1', '0/20', 'session:a');
    c.advance(4999);
    expect(t.minLsn('d1', 'session:a')).toBe('0/20');
    c.advance(2);
    expect(t.minLsn('d1', 'session:a')).toBeUndefined();
  });

  it('ignores missing or malformed LSNs, and does nothing when disabled', () => {
    const t = new LsnTracker(30_000);
    t.record('d1', null);
    t.record('d1', 'garbage');
    expect(t.minLsn('d1')).toBeUndefined();
    const off = new LsnTracker(0);
    off.record('d1', '0/20');
    expect(off.minLsn('d1')).toBeUndefined();
  });
});
