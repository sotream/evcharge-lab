/* oxlint-disable no-await-in-loop, no-unmodified-loop-condition */ // one sequential writer, stopped by a signal
// Durability probe for drills/postgres-failover.sh (ADR 0018): one committed INSERT at a time, as fast as the database
// acknowledges, and every acknowledged sequence number is written to a file BEFORE the next one is attempted.
// After a failover, every acknowledged seq must exist on the new primary; a missing one is an acknowledged write that was lost.
//
// Env: HOSTS (comma list of host:port to try in order, default postgres:5432, so run it on the compose network where the alias moves with the failover), DB (default evcharge),
//      OUT (directory, required), INTERVAL_MS (pause between writes, default 10), PHASE_FILE (optional drill phase).
// Writes OUT/acks.jsonl  {seq, startMs, ackMs, host}        one line per acknowledged commit
//        OUT/errors.jsonl {seq, atMs, host, code, message}   one line per failed attempt (the commit outcome is UNKNOWN)
// On SIGTERM/SIGINT writes OUT/ack-summary.json.
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';

const require = createRequire(new URL('../services/sessions/package.json', import.meta.url));
const { Client } = require('pg');

const OUT = process.env.OUT;
if (!OUT) {
  console.error('OUT is required');
  process.exit(2);
}
mkdirSync(OUT, { recursive: true });
const hosts = (process.env.HOSTS ?? 'postgres:5432').split(',').map((h) => {
  const [host, port] = h.split(':');
  return { host, port: Number(port), label: h };
});
const DB = process.env.DB ?? 'evcharge';
const INTERVAL_MS = Number(process.env.INTERVAL_MS ?? 10);
const PHASE_FILE = process.env.PHASE_FILE;

const summary = { acked: 0, errors: 0, firstAckMs: null, lastAckMs: null, maxSeq: 0, hosts: {} };
let stop = false;
const writeSummary = () => writeFileSync(`${OUT}/ack-summary.json`, `${JSON.stringify(summary, null, 2)}\n`);
const finish = () => {
  stop = true;
  // a query stuck on a half-open connection must not keep the probe alive
  setTimeout(() => {
    writeSummary();
    process.exit(0);
  }, 4000).unref();
};
process.on('SIGTERM', finish);
process.on('SIGINT', finish);

async function connect(h) {
  const c = new Client({ host: h.host, port: h.port, user: 'evcharge', password: 'evcharge', database: DB, connectionTimeoutMillis: 1000, statement_timeout: 2000, query_timeout: 3000 });
  c.on('error', () => {});
  await c.connect();
  return c;
}

let client = null;
let hostIdx = 0;
let seq = 0;

async function ensureTable() {
  // Created on whichever host is the primary at the start.
  for (const h of hosts) {
    try {
      const c = await connect(h);
      try {
        await c.query('CREATE TABLE IF NOT EXISTS drill_acks (seq bigint PRIMARY KEY, writer text NOT NULL, ts timestamptz NOT NULL DEFAULT now())');
        await c.end();
        return;
      } catch {
        await c.end().catch(() => {});
      }
    } catch {}
  }
  console.error('no writable host to create drill_acks');
  process.exit(3);
}
await ensureTable();
const WRITER = `probe-${Date.now()}`;

while (!stop) {
  const h = hosts[hostIdx % hosts.length];
  seq += 1;
  try {
    client ??= await connect(h);
    const startMs = Date.now();
    await client.query('INSERT INTO drill_acks (seq, writer) VALUES ($1, $2)', [seq, WRITER]);
    const ackMs = Date.now();
    appendFileSync(`${OUT}/acks.jsonl`, `${JSON.stringify({ seq, startMs, ackMs, host: h.label })}\n`);
    summary.acked += 1;
    summary.maxSeq = seq;
    summary.firstAckMs ??= ackMs;
    summary.lastAckMs = ackMs;
    summary.hosts[h.label] = (summary.hosts[h.label] ?? 0) + 1;
    if (INTERVAL_MS > 0) await new Promise((r) => setTimeout(r, INTERVAL_MS));
  } catch (err) {
    summary.errors += 1;
    appendFileSync(`${OUT}/errors.jsonl`, `${JSON.stringify({ seq, atMs: Date.now(), host: h.label, code: err?.code ?? null, message: String(err?.message ?? err).slice(0, 160) })}\n`);
    await client?.end().catch(() => {});
    client = null;
    // A standby ("read-only transaction") or a dead host: try the next one, without a hot loop.
    hostIdx += 1;
    await new Promise((r) => setTimeout(r, 100));
  }
}
await client?.end().catch(() => {});
if (PHASE_FILE) summary.lastPhaseFile = PHASE_FILE;
writeFileSync(`${OUT}/ack-summary.json`, `${JSON.stringify(summary, null, 2)}\n`);
