/* oxlint-disable no-await-in-loop, no-unmodified-loop-condition */ // a sequential once-per-second loop, stopped by a signal handler
// Samples Prometheus instant queries once per second into a JSONL timeline.
// Usage: node drills/lib/sample.mjs <queries.json> <out.jsonl> <phase-file>
// queries.json: {"name": "promql", ...}. The current phase is read from <phase-file> (written by the
// drill script: before / during / after), so every sample is labelled with the phase it belongs to.
import { appendFileSync, readFileSync } from 'node:fs';

const [queriesPath, outPath, phasePath] = process.argv.slice(2);
if (!queriesPath || !outPath || !phasePath) {
  console.error('usage: sample.mjs <queries.json> <out.jsonl> <phase-file>');
  process.exit(2);
}
const PROM = process.env.PROM_URL ?? 'http://localhost:29090';
const queries = JSON.parse(readFileSync(queriesPath, 'utf8'));
const t0 = Date.now();

async function value(q) {
  try {
    const res = await fetch(`${PROM}/api/v1/query?query=${encodeURIComponent(q)}`, { signal: AbortSignal.timeout(2000) });
    const body = await res.json();
    const r = body?.data?.result ?? [];
    if (r.length === 0) return null;
    // Sum vectors so every query yields one number per tick.
    return r.reduce((s, x) => s + Number(x.value[1]), 0);
  } catch {
    return null;
  }
}

let stop = false;
process.on('SIGTERM', () => (stop = true));
process.on('SIGINT', () => (stop = true));
while (!stop) {
  const tick = Date.now();
  let phase = 'unknown';
  try {
    phase = readFileSync(phasePath, 'utf8').trim() || 'unknown';
  } catch {}
  const entries = await Promise.all(Object.entries(queries).map(async ([k, q]) => [k, await value(q)]));
  appendFileSync(outPath, JSON.stringify({ t: new Date(tick).toISOString(), s: Math.round((tick - t0) / 100) / 10, phase, ...Object.fromEntries(entries) }) + '\n');
  await new Promise((r) => setTimeout(r, Math.max(0, 1000 - (Date.now() - tick))));
}
