/* oxlint-disable no-await-in-loop, no-unmodified-loop-condition */ // one sequential polling loop, stopped by a signal
// Asks the group coordinator who owns how many partitions: `kafka-consumer-groups --describe --members --verbose`.
// This is the ground truth for a rebalance (the lag series an instance exports can trail the assignment).
// Usage: node drills/lib/group-sample.mjs <group> <out.jsonl> [kafka-service]   (one line per poll: {ms, members, assigned})
import { execFile } from 'node:child_process';
import { appendFileSync } from 'node:fs';
import { promisify } from 'node:util';

const run = promisify(execFile);
const [group, out, service = 'kafka-1'] = process.argv.slice(2);
if (!group || !out) {
  console.error('usage: group-sample.mjs <group> <out.jsonl> [kafka-service]');
  process.exit(2);
}
let stop = false;
process.on('SIGTERM', () => (stop = true));
process.on('SIGINT', () => (stop = true));
while (!stop) {
  const ms = Date.now();
  let members = 0;
  let assigned = 0;
  try {
    const { stdout } = await run('docker', ['compose', 'exec', '-T', service, '/opt/kafka/bin/kafka-consumer-groups.sh', '--bootstrap-server', `${service}:9092`, '--describe', '--members', '--verbose', '--group', group], { timeout: 15000 });
    for (const line of stdout.split('\n')) {
      const cols = line.trim().split(/\s+/);
      if (/^\d+$/.test(cols[4] ?? '')) {
        members += 1;
        assigned += Number(cols[4]);
      }
    }
  } catch {}
  appendFileSync(out, `${JSON.stringify({ ms, members, assigned })}\n`);
}
