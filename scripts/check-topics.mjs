// Fails if infra/kafka/topics.sh drifts from packages/contracts TOPIC_SPECS.
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { TOPIC_SPECS } = require('../packages/contracts/dist/index.js');
const sh = readFileSync(new URL('../infra/kafka/topics.sh', import.meta.url), 'utf8');
const lines = [...sh.matchAll(/^create (\S+) (\d+) (\d+)$/gm)].map((m) => `${m[1]} ${m[2]} ${m[3]}`);
const want = TOPIC_SPECS.map((t) => `${t.name} ${t.partitions} ${t.retentionMs}`);
const missing = want.filter((w) => !lines.includes(w));
const extra = lines.filter((l) => !want.includes(l));
if (missing.length || extra.length) {
  console.error('topics.sh out of sync with TOPIC_SPECS', { missing, extra });
  process.exit(1);
}
console.log(`topics in sync (${want.length})`);
