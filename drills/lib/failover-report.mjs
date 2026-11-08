// Turns the raw files of drills/postgres-failover.sh into summary.json and a readable timeline.txt.
// Usage: node drills/lib/failover-report.mjs <out-dir>
// Reads:  marks.json (epoch ms of the drill's own actions), ack/acks.jsonl + ack/errors.jsonl (load/pg-ack-probe.mjs),
//         ack/db-seqs.txt (every seq found on the NEW primary), history/history.jsonl (load/history/generate.mjs),
//         timeline.jsonl (Prometheus samples, one per second).
// Writes: summary.json, timeline.txt. Prints timeline.txt.
import { readFileSync, writeFileSync, existsSync } from 'node:fs';

const dir = process.argv[2];
if (!dir) {
  console.error('usage: failover-report.mjs <out-dir>');
  process.exit(2);
}
const lines = (p) => (existsSync(p) ? readFileSync(p, 'utf8').split('\n').filter(Boolean) : []);
const jsonl = (p) => lines(p).map((l) => JSON.parse(l));
const marks = JSON.parse(readFileSync(`${dir}/marks.json`, 'utf8'));
const acks = jsonl(`${dir}/ack/acks.jsonl`);
const probeErrors = jsonl(`${dir}/ack/errors.jsonl`);
const dbSeqs = new Set(lines(`${dir}/ack/db-seqs.txt`).map(Number));
const ops = jsonl(`${dir}/history/history.jsonl`);
const prom = jsonl(`${dir}/timeline.jsonl`);
const kill = marks.kill;
const sec = (ms) => (ms === null || ms === undefined ? null : Math.round((ms - kill) / 100) / 10);

// Acknowledged writes lost: the probe was told "committed", the new primary does not have the row.
const lostAcks = acks.filter((a) => !dbSeqs.has(a.seq));
// RTO counts only writes that STARTED after the kill: an acknowledgement that was already in flight says nothing about recovery.
const firstAckAfter = acks.find((a) => a.startMs > kill);
const lastAckBefore = acks.toReversed().find((a) => a.ackMs <= kill);
const reserveOkAfter = ops.filter((o) => o.type === 'reserve' && o.outcome === 'ok' && o.invokeMs > kill).sort((a, b) => a.completeMs - b.completeMs)[0];
const rtoDb = firstAckAfter ? (firstAckAfter.ackMs - kill) / 1000 : null;
// The longest silence between two acknowledged probe commits is the write outage as the database writer saw it.
const gaps = acks.slice(1).map((a, i) => ({ gapMs: a.ackMs - acks[i].ackMs, from: acks[i].ackMs }));
const longestGap = gaps.reduce((m, g) => (g.gapMs > m.gapMs ? g : m), { gapMs: 0, from: null });
const rtoApp = reserveOkAfter ? (reserveOkAfter.completeMs - kill) / 1000 : null;
const windowEnd = kill + (Math.max(rtoApp ?? 0, rtoDb ?? 0) + 10) * 1000;
const inWindow = ops.filter((o) => o.completeMs >= kill && o.invokeMs <= windowEnd);
const count = (arr, f) => arr.filter(f).length;

// Cumulative error counters from Prometheus: growth between the last 'before' sample and the last sample.
const lastBefore = prom.toReversed().find((s) => s.phase === 'before');
const last = prom[prom.length - 1];
// A counter series that never existed (no error yet) reads as null: that is 0.
const delta = (k) => (lastBefore && last ? Math.round((last[k] ?? 0) - (lastBefore[k] ?? 0)) : null);

const summary = {
  mode: marks.mode,
  synchronous: marks.sync,
  stallS: marks.stallS,
  synchronousStandbyNames: marks.synchronousStandbyNames,
  marksSecondsFromKill: { stall: sec(marks.stallStart), kill: 0, promoteStart: sec(marks.promoteStart), promoted: sec(marks.promoted), repointed: sec(marks.repointed) },
  rtoDbS: rtoDb,
  longestWriteGapS: longestGap.gapMs / 1000,
  rtoAppS: rtoApp,
  probe: {
    acked: acks.length,
    ackedBeforeKill: count(acks, (a) => a.ackMs <= kill),
    lost: lostAcks.length,
    lostFirstSeq: lostAcks[0]?.seq ?? null,
    lostLastSeq: lostAcks[lostAcks.length - 1]?.seq ?? null,
    lostSpanMs: lostAcks.length ? lostAcks[lostAcks.length - 1].ackMs - lostAcks[0].ackMs : 0,
    lastAckBeforeKillMsBeforeKill: lastBefore && lastAckBefore ? kill - lastAckBefore.ackMs : null,
    failedAttempts: probeErrors.length,
    errorKinds: Object.fromEntries([...new Set(probeErrors.map((e) => e.code ?? 'none'))].map((c) => [c, count(probeErrors, (e) => (e.code ?? 'none') === c)])),
  },
  driversInWindow: {
    windowS: Math.round((windowEnd - kill) / 100) / 10,
    ok: count(inWindow, (o) => o.outcome === 'ok'),
    rejected: count(inWindow, (o) => o.outcome === 'rejected'),
    error: count(inWindow, (o) => o.outcome === 'error'),
  },
  stationsAndServices: {
    ocppCallErrorsSent: delta('ocpp_error_out_total'),
    sessions5xx: delta('sessions_5xx_total'),
    driverApi5xx: delta('driver_api_5xx_total'),
  },
};
writeFileSync(`${dir}/summary.json`, `${JSON.stringify(summary, null, 2)}\n`);

// ---- timeline.txt: 2 s buckets from kill-10 s to the end of the recovery window, with the drill's own actions as markers.
const BUCKET = 2000;
const from = kill - 10_000;
const to = Math.max(windowEnd + 10_000, (marks.repointed ?? kill) + 20_000);
const rows = [];
const bar = (n, scale) => '#'.repeat(Math.min(40, Math.round(n / scale)));
const maxAcks = Math.max(1, ...Array.from({ length: Math.ceil((to - from) / BUCKET) }, (_, i) => count(acks, (a) => a.ackMs >= from + i * BUCKET && a.ackMs < from + (i + 1) * BUCKET)));
const markList = [
  [marks.stallStart, 'replica cut off from the primary (network partition)'],
  [marks.kill, 'PRIMARY KILLED (docker kill)'],
  [marks.promoteStart, 'promote started'],
  [marks.promoted, 'replica promoted (pg_promote returned)'],
  [marks.repointed, 'alias postgres now points at the new primary'],
].filter(([t]) => t != null);
rows.push('Failover timeline (seconds relative to the kill; 2 s buckets)');
rows.push(`mode=${marks.mode}  synchronous_standby_names='${marks.synchronousStandbyNames}'  stall=${marks.stallS}s`);
rows.push('');
rows.push('   t(s)  db-acks  drv ok/rej/err  db-acks bar                                 events');
for (let t = from; t < to; t += BUCKET) {
  const a = count(acks, (x) => x.ackMs >= t && x.ackMs < t + BUCKET);
  const o = ops.filter((x) => x.completeMs >= t && x.completeMs < t + BUCKET);
  const ev = markList.filter(([m]) => m >= t && m < t + BUCKET).map(([, label]) => label).join('; ');
  rows.push(`${String(Math.round((t - kill) / 100) / 10).padStart(7)}  ${String(a).padStart(7)}  ${String(count(o, (x) => x.outcome === 'ok')).padStart(4)}/${String(count(o, (x) => x.outcome === 'rejected')).padStart(3)}/${String(count(o, (x) => x.outcome === 'error')).padStart(3)}  ${bar(a, maxAcks / 40).padEnd(40)}  ${ev}`);
}
rows.push('');
rows.push(`Longest gap between two acknowledged DB commits: ${(longestGap.gapMs / 1000).toFixed(1)} s`);
rows.push(`RTO, first acknowledged DB write that started after the kill: ${rtoDb === null ? 'none' : `${rtoDb.toFixed(1)} s`}`);
rows.push(`RTO, first accepted driver reservation invoked after the kill: ${rtoApp === null ? 'none' : `${rtoApp.toFixed(1)} s`}`);
rows.push(`Acknowledged probe commits: ${acks.length}, lost on the new primary: ${lostAcks.length}${lostAcks.length ? ` (seq ${summary.probe.lostFirstSeq}..${summary.probe.lostLastSeq}, spanning ${summary.probe.lostSpanMs} ms)` : ''}`);
rows.push(`Drivers during the window (${summary.driversInWindow.windowS} s): ${summary.driversInWindow.ok} ok, ${summary.driversInWindow.rejected} rejected, ${summary.driversInWindow.error} errors/timeouts`);
rows.push(`Stations and services (counter growth): OCPP CALLERROR sent ${summary.stationsAndServices.ocppCallErrorsSent}, sessions HTTP 5xx ${summary.stationsAndServices.sessions5xx}, driver-api HTTP 5xx ${summary.stationsAndServices.driverApi5xx}`);
writeFileSync(`${dir}/timeline.txt`, `${rows.join('\n')}\n`);
console.log(rows.join('\n'));
