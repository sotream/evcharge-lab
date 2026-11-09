/* oxlint-disable no-await-in-loop, no-unmodified-loop-condition */ // one sequential once-per-second loop, stopped by a signal
// Polls /metrics of every telemetry-ingest instance once per second over its published host port, because the second
// instance is started outside compose and Prometheus does not scrape it (the image is distroless: no docker exec).
// Usage: node drills/lib/rebalance-sample.mjs <out.jsonl> <phase-file> <name=http://localhost:port> [...]
// Line: {t, c, up, partitions, lagTotal, rows, dedup, rebalanceDrops, phase}
import { appendFileSync, readFileSync } from 'node:fs';

const [out, phaseFile, ...targets] = process.argv.slice(2);
if (!out || !phaseFile || targets.length === 0) {
  console.error('usage: rebalance-sample.mjs <out.jsonl> <phase-file> <name=url>...');
  process.exit(2);
}
let stop = false;
process.on('SIGTERM', () => (stop = true));
process.on('SIGINT', () => (stop = true));

const num = (text, re) => {
  let sum = 0;
  let n = 0;
  for (const m of text.matchAll(re)) {
    sum += Number(m[1]);
    n += 1;
  }
  return { sum, n };
};

async function sample(url) {
  try {
    const stdout = await (await fetch(`${url}/metrics`, { signal: AbortSignal.timeout(2000) })).text();
    const lag = num(stdout, /^kafka_consumer_lag\{[^}]*\}\s+([0-9.e+-]+)$/gm);
    return {
      up: true,
      partitions: lag.n,
      lagTotal: lag.sum,
      rows: num(stdout, /^telemetry_rows_ingested_total\s+([0-9.e+-]+)$/gm).sum,
      dedup: num(stdout, /^telemetry_dedup_dropped_total\s+([0-9.e+-]+)$/gm).sum,
      rebalanceDrops: num(stdout, /^telemetry_rebalance_drops_total\s+([0-9.e+-]+)$/gm).sum,
    };
  } catch {
    return { up: false, partitions: 0, lagTotal: null, rows: null, dedup: null, rebalanceDrops: null };
  }
}

while (!stop) {
  const tick = Date.now();
  let phase = 'unknown';
  try {
    phase = readFileSync(phaseFile, 'utf8').trim() || 'unknown';
  } catch {}
  const rows = await Promise.all(targets.map(async (tg) => { const [c, url] = tg.split('='); return { t: new Date(tick).toISOString(), ms: tick, c, phase, ...(await sample(url)) }; }));
  for (const r of rows) appendFileSync(out, `${JSON.stringify(r)}\n`);
  const wait = 1000 - (Date.now() - tick);
  if (wait > 0) await new Promise((r) => setTimeout(r, wait));
}
