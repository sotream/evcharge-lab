import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { CommandChannel, type CommandOutcome } from '../../src/command-channel';

function make(opts: { capacity?: number; timeoutMs?: number; open?: boolean } = {}) {
  const sent: { id: string; action: string }[] = [];
  let n = 0;
  const state = { open: opts.open ?? true };
  const ch = new CommandChannel(
    (id, action) => {
      if (!state.open) return false;
      sent.push({ id, action });
      return true;
    },
    { capacity: opts.capacity ?? 32, timeoutMs: opts.timeoutMs ?? 30_000, newId: () => `c${++n}` },
  );
  const outcomes: CommandOutcome[] = [];
  const cmd = (action: 'SetChargingProfile' | 'ReserveNow' = 'SetChargingProfile', extra: { expiresAtMs?: number } = {}) => ({
    action,
    payload: {},
    ...extra,
    onDone: (o: CommandOutcome) => void outcomes.push(o),
  });
  return { ch, sent, outcomes, cmd, state };
}

describe('CommandChannel', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('keeps one outstanding CALL and pumps the queue in order', () => {
    const { ch, sent, outcomes, cmd } = make();
    ch.enqueue(cmd());
    ch.enqueue(cmd('ReserveNow'));
    expect(sent.map((s) => s.action)).toEqual(['SetChargingProfile']);
    ch.handleResult('c1', { status: 'Accepted' });
    expect(sent.map((s) => s.action)).toEqual(['SetChargingProfile', 'ReserveNow']);
    ch.handleResult('c2', { status: 'Occupied' });
    expect(outcomes).toEqual(['Accepted', 'Rejected']);
    expect(ch.pending).toBe(0);
  });

  it('times out after the response deadline and moves on', () => {
    const { ch, sent, outcomes, cmd } = make({ timeoutMs: 30_000 });
    ch.enqueue(cmd());
    ch.enqueue(cmd());
    vi.advanceTimersByTime(29_999);
    expect(outcomes).toEqual([]);
    vi.advanceTimersByTime(2);
    expect(outcomes).toEqual(['Timeout']);
    expect(sent).toHaveLength(2);
  });

  it('ignores responses with an unknown uniqueId and maps CALLERROR to Error', () => {
    const { ch, outcomes, cmd } = make();
    ch.enqueue(cmd());
    expect(ch.handleResult('nope', {})).toBe(false);
    expect(ch.handleError('c1', 'NotSupported', 'x')).toBe(true);
    expect(outcomes).toEqual(['Error']);
  });

  it('reports an invalid CALLRESULT payload as Error', () => {
    const { ch, outcomes, cmd } = make();
    ch.enqueue(cmd());
    ch.handleResult('c1', { status: 'Maybe' });
    expect(outcomes).toEqual(['Error']);
  });

  it('overflows the oldest waiting command with Error when the queue is full', () => {
    const { ch, outcomes, cmd } = make({ capacity: 2 });
    ch.enqueue(cmd()); // outstanding
    ch.enqueue(cmd());
    ch.enqueue(cmd());
    expect(outcomes).toEqual([]);
    ch.enqueue(cmd());
    expect(outcomes).toEqual(['Error']);
    expect(ch.pending).toBe(3);
  });

  it('expires commands whose expiresAt passed before sending', () => {
    const { ch, sent, outcomes, cmd } = make();
    ch.enqueue(cmd());
    ch.enqueue(cmd('ReserveNow', { expiresAtMs: Date.now() + 1000 }));
    vi.advanceTimersByTime(2000);
    ch.handleResult('c1', { status: 'Accepted' });
    expect(outcomes).toEqual(['Accepted', 'Expired']);
    expect(sent).toHaveLength(1);
  });

  it('completes everything StationOffline on close and afterwards', () => {
    const { ch, outcomes, cmd } = make();
    ch.enqueue(cmd());
    ch.enqueue(cmd());
    ch.close();
    expect(outcomes).toEqual(['StationOffline', 'StationOffline']);
    ch.enqueue(cmd());
    expect(outcomes).toHaveLength(3);
  });

  it('completes StationOffline when the socket is not writable', () => {
    const { ch, outcomes, cmd } = make({ open: false });
    ch.enqueue(cmd());
    expect(outcomes).toEqual(['StationOffline']);
  });
});
