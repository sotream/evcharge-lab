/* oxlint-disable no-await-in-loop */ // a paced send loop: one send every 20 ms
// Reproduces produce latency during a partition leader move, outside any drill.
// It produces one acks=all record every 20 ms with the platform producer (the gateway's client stack) to a private
// topic, and records per-send latency and errors as JSONL. Another shell triggers the leader move (see leader-move.sh).
//   node load/repro/leader-move.mjs <topic> <seconds> <out.jsonl>
import { createRequire } from 'node:module';
import { writeFileSync, appendFileSync } from 'node:fs';
const require = createRequire(import.meta.url);
const { createProducer } = require('../../packages/platform/dist/index.js');

const [topic, seconds = '90', out = 'leader-move.jsonl'] = process.argv.slice(2);
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:29091,localhost:29092,localhost:29093').split(',');
const producer = createProducer({ brokers, clientId: 'leader-move-repro', lingerMs: 0, sendTimeoutMs: Number(process.env.SEND_TIMEOUT_MS ?? 30_000) });
writeFileSync(out, '');
const end = Date.now() + Number(seconds) * 1000;
let i = 0;
const inflight = new Set();
while (Date.now() < end) {
  const n = i++;
  const t0 = Date.now();
  const p = producer
    .send([{ topic, key: `k${n % 12}`, value: JSON.stringify({ n, t0 }) }])
    .then(() => appendFileSync(out, JSON.stringify({ n, t: new Date(t0).toISOString(), ms: Date.now() - t0, ok: true }) + '\n'))
    .catch((err) => appendFileSync(out, JSON.stringify({ n, t: new Date(t0).toISOString(), ms: Date.now() - t0, ok: false, err: String(err?.message ?? err).slice(0, 200) }) + '\n'))
    .finally(() => inflight.delete(p));
  inflight.add(p);
  await new Promise((r) => setTimeout(r, 20));
}
await Promise.allSettled(inflight);
await producer.close();
