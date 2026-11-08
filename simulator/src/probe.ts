import { createWriteStream, mkdirSync, type WriteStream } from 'node:fs';
import { join } from 'node:path';

export interface ProbeReading {
  stationId: string;
  connectorId: number;
  /** Per (station, connector) sequence number, 1-based, gap-free in generation order. */
  seq: number;
  /** Station (simulated) timestamp of the reading, ISO. */
  ts: string;
  transactionId?: number;
  energyWh: number;
  powerW: number;
}

/**
 * JSONL log of every reading a probe station generates, written at generation time. Drills compare
 * it with what reached Timescale to prove zero loss or zero gaps (ADR 0012, 0013).
 */
export class ProbeLog {
  private readonly stream: WriteStream;
  readonly path: string;

  constructor(dir: string, runId: string) {
    mkdirSync(dir, { recursive: true });
    this.path = join(dir, `probe-${runId}.jsonl`);
    this.stream = createWriteStream(this.path, { flags: 'a' });
  }

  write(r: ProbeReading): void {
    this.stream.write(`${JSON.stringify(r)}\n`);
  }

  close(): Promise<void> {
    return new Promise((resolve) => this.stream.end(() => resolve()));
  }
}
