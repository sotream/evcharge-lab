import { randomUUID } from 'node:crypto';
import { ocpp } from '@evcharge/contracts';

export type CommandOutcome = 'Accepted' | 'Rejected' | 'Error' | 'Timeout' | 'StationOffline' | 'Expired';
export type CsmsAction = ocpp.CsmsAction;

export interface OutboundCommand {
  action: CsmsAction;
  payload: Record<string, unknown>;
  /** Not sent after this instant: completes `Expired` instead. */
  expiresAtMs?: number;
  /** Called exactly once. */
  onDone(outcome: CommandOutcome, response?: Record<string, unknown>): void;
}

export interface CommandChannelOptions {
  /** Waiting commands per station (ADR 0003: 32); on overflow the oldest completes `Error`. */
  capacity: number;
  /** Response timeout for the one outstanding CALL (ADR 0003: 30 s). */
  timeoutMs: number;
  now?: () => number;
  newId?: () => string;
}

interface Outstanding {
  uniqueId: string;
  cmd: OutboundCommand;
  timer: NodeJS.Timeout;
}

/**
 * CSMS -> station CALL sequencing for one connection: one outstanding CALL at a time (OCPP),
 * a bounded FIFO behind it, a response timeout, and an outcome per command.
 */
export class CommandChannel {
  private readonly queue: OutboundCommand[] = [];
  private outstanding: Outstanding | undefined;
  private closed = false;
  private readonly now: () => number;
  private readonly newId: () => string;

  constructor(
    /** Writes the CALL frame; false when the socket is not open. */
    private readonly send: (uniqueId: string, action: CsmsAction, payload: Record<string, unknown>) => boolean,
    private readonly opts: CommandChannelOptions,
  ) {
    this.now = opts.now ?? Date.now;
    this.newId = opts.newId ?? randomUUID;
  }

  get pending(): number {
    return this.queue.length + (this.outstanding ? 1 : 0);
  }

  enqueue(cmd: OutboundCommand): void {
    if (this.closed) {
      cmd.onDone('StationOffline');
      return;
    }
    if (this.queue.length >= this.opts.capacity) this.queue.shift()?.onDone('Error', { reason: 'command queue overflow' });
    this.queue.push(cmd);
    this.pump();
  }

  /** CALLRESULT from the station. Returns false if it matches no outstanding CALL. */
  handleResult(uniqueId: string, payload: Record<string, unknown>): boolean {
    const o = this.take(uniqueId);
    if (!o) return false;
    const conf = ocpp.CsmsToStation[o.cmd.action].conf.safeParse(payload);
    if (!conf.success) o.cmd.onDone('Error', { reason: 'invalid response', payload });
    else o.cmd.onDone(payload['status'] === 'Accepted' ? 'Accepted' : 'Rejected', payload);
    this.pump();
    return true;
  }

  /** CALLERROR from the station. */
  handleError(uniqueId: string, errorCode: string, errorDescription: string): boolean {
    const o = this.take(uniqueId);
    if (!o) return false;
    o.cmd.onDone('Error', { errorCode, errorDescription });
    this.pump();
    return true;
  }

  /** The socket closed: everything not answered completes `StationOffline`. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.outstanding) {
      clearTimeout(this.outstanding.timer);
      const o = this.outstanding;
      this.outstanding = undefined;
      o.cmd.onDone('StationOffline');
    }
    for (const c of this.queue.splice(0)) c.onDone('StationOffline');
  }

  private take(uniqueId: string): Outstanding | undefined {
    const o = this.outstanding;
    if (!o || o.uniqueId !== uniqueId) return undefined;
    clearTimeout(o.timer);
    this.outstanding = undefined;
    return o;
  }

  private pump(): void {
    while (!this.closed && !this.outstanding && this.queue.length > 0) {
      const cmd = this.queue.shift() as OutboundCommand;
      if (cmd.expiresAtMs !== undefined && cmd.expiresAtMs <= this.now()) {
        cmd.onDone('Expired');
        continue;
      }
      const uniqueId = this.newId();
      if (!this.send(uniqueId, cmd.action, cmd.payload)) {
        cmd.onDone('StationOffline');
        continue;
      }
      const timer = setTimeout(() => {
        if (this.outstanding?.uniqueId !== uniqueId) return;
        this.outstanding = undefined;
        cmd.onDone('Timeout');
        this.pump();
      }, this.opts.timeoutMs);
      timer.unref();
      this.outstanding = { uniqueId, cmd, timer };
    }
  }
}
