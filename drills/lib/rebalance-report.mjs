// Derives the rebalance timings of drills/consumer-rebalance.sh from its raw files and writes timings.json.
// Usage: node drills/lib/rebalance-report.mjs <out-dir>
// kill/start instants come from events.log (1 s resolution), so every time below is +-1 s.
// Reads: events.log, group.jsonl ({ms, members, assigned} from the group coordinator), timeline.jsonl (group lag from Prometheus).
import { readFileSync, writeFileSync } from 'node:fs';

const dir = process.argv[2];
const lines = (f) => readFileSync(`${dir}/${f}`, 'utf8').split('\n').filter(Boolean);
const ev = lines('events.log');
const at = (re) => {
  const l = ev.find((x) => re.test(x));
  return l ? Date.parse(l.split(' ')[0]) : null;
};
const tKill = at(/RUN docker kill evcharge-telemetry-ingest-2/);
const tStart = at(/RUN docker start evcharge-telemetry-ingest-2/);
const group = lines('group.jsonl').map((l) => JSON.parse(l));
const tl = lines('timeline.jsonl').map((l) => JSON.parse(l));
const rel = (ms, base) => (ms === null || ms === undefined ? null : Math.round((ms - base) / 100) / 10);
const first = (f, from) => group.find((g) => g.ms > from && f(g));
const detect = first((g) => g.members === 1, tKill);
const done = first((g) => g.members === 1 && g.assigned === 24, tKill);
const back = first((g) => g.members === 2 && g.assigned === 24, tStart);
// The forced-redelivery phase ('replay') moves the offsets back on purpose and creates its own lag, so it is analysed separately.
const lagAfter = tl.filter((r) => r.group_lag_total != null && r.phase !== 'replay' && Date.parse(r.t) > tKill - 2000);
const replayLag = tl.filter((r) => r.group_lag_total != null && r.phase === 'replay');
const peak = lagAfter.reduce((m, r) => (r.group_lag_total > m.v ? { v: r.group_lag_total, ms: Date.parse(r.t) } : m), { v: 0, ms: null });
const drained = lagAfter.find((r) => Date.parse(r.t) > (peak.ms ?? 0) && r.group_lag_total < 100);
const out = {
  note: 'times in seconds, resolution +-1 s (kill/start instants come from events.log)',
  killToCoordinatorDropsMemberS: rel(detect?.ms, tKill),
  killToSurvivorOwnsAll24S: rel(done?.ms, tKill),
  restartToTwoMembersOwnAll24S: rel(back?.ms, tStart),
  lagPeakRecords: peak.v,
  lagPeakAtSAfterKill: rel(peak.ms, tKill),
  killToLagBelow100S: drained ? rel(Date.parse(drained.t), tKill) : null,
  forcedRedeliveryLagPeakRecords: Math.max(0, ...replayLag.map((r) => r.group_lag_total)),
  lagBeforeMaxRecords: Math.max(0, ...tl.filter((r) => r.phase === 'before' && r.group_lag_total != null).map((r) => r.group_lag_total)),
};
writeFileSync(`${dir}/timings.json`, `${JSON.stringify(out, null, 2)}\n`);
console.log(JSON.stringify(out));
