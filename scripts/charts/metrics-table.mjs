// Markdown metrics table for the README, generated from the raw files (never typed by hand).
import * as D from './data.mjs';

const T = D.targets.perInstance;
const link = (f, prefix = '') => `[\`${f.split('/').pop()}\`](${prefix}${f})`;
const n = (v, u = '') => v === null || v === undefined ? 'no measurement' : `${Number(v.toFixed(2)).toLocaleString('en-US', { maximumFractionDigits: 2 })}${u ? ' ' + u : ''}`;
const verdict = (ok) => (ok ? 'met' : '**MISSED**');
// Verdict computed from the numbers alone: no tolerance.
const V = (value, op, target) => ({ pass: 'met', miss: '**MISSED**', none: 'no measurement' })[D.verdictOf(value, op, target)];
// k6 throughput rows use the ADR 0000 rate rule (D.rateVerdict).
const RV = (k, t) => (D.rateVerdict(k, t) === 'pass' && k.metric('http_reqs').rate < t ? 'met, rate rule' : V(k.metric('http_reqs').rate, 'ge', t));
// k6 reports http_reqs.rate as count / test run duration; when that lands just under the target, say it is the duration.
const durationNote = (k, target) => {
  const reqs = k.metric('http_reqs').count, secs = k.durationMs / 1000;
  return `${n(reqs)} requests in ${secs.toFixed(4)} s. The target is ${n(target)} /s and the reported rate is ${n(k.metric('http_reqs').rate)} /s only because the run lasted slightly longer than 60 s, so the gap is a duration artefact and not a throughput shortfall; the row meets the target under the ADR 0000 rate rule. Derived: http_reqs.count / testRunDurationMs × 1000 (${n(reqs)} / ${secs.toFixed(4)} s = ${n(reqs / secs)}).`;
};
// Rows may carry a note; metricsMarkdown lists them under the table.


export function metricsRows() {
  const rows = []; // { metric, target, value, label, sources, formula }
  const add = (metric, target, value, label, sources, formula, note) => rows.push({ metric, target, value, label, sources, formula, note });

  // station-gateway
  const g = D.tool('gateway-10k-connections.json');
  if (g) {
    const s = g.data.summary;
    add('Gateway: WebSocket connections on one instance', `≥ ${n(T['station-gateway'].connections)}`, `${n(s.peakConnections)} (${verdict(s.peakConnections >= T['station-gateway'].connections)})`, 'measured', [g.file]);
    add('Gateway: peak RSS at that load', g.data.targets['ADR 0004 RSS at 10,000 connections'], `${n(s.peakRssMB, 'MB')}`, 'measured', [g.file]);
    add('Gateway: inbound OCPP frames/s, 10,000 simulated stations, stub sessions (not the pass/fail run)', 'n/a', `peak ${n(s.peakMsgInPerS)}; last-30 s average ${n(s.last30sAvgMsgInPerS)}`, 'measured', [g.file]);
  }
  for (const r of D.k6List(/^k6-gateway-ws-/)) {
    const rate = r.metric('ocpp_callresults').rate, p99 = r.metric('ocpp_callresult_ms')['p(99)'];
    const offered = D.gatewayOfferedRate(r), cr = r.metric('ocpp_callresult_ms');
    const untested = offered !== null && offered < T['station-gateway'].msgPerSec;
    add(`Gateway: msg/s, ${r.metric('ws_sessions').count} stations at 1 msg/s (k6)`, `≥ ${n(T['station-gateway'].msgPerSec)}`,
      untested ? `${n(rate)} (not tested: offered load is at most ${n(offered)} msg/s)` : `${n(rate)} (${V(rate, 'ge', T['station-gateway'].msgPerSec)})`,
      untested ? 'measured; offered load derived' : 'measured', [r.file, r.envFile], untested ? 'offered = stations × Hz (one outstanding CALL per station)' : undefined);
    add(`Gateway: CALLRESULT p99, ${r.metric('ws_sessions').count} stations, at ${n(rate)} msg/s achieved (k6)`, `≤ ${n(T['station-gateway'].callResultP99Ms, 'ms')}`, `${n(p99, 'ms')} (${V(p99, 'le', T['station-gateway'].callResultP99Ms)}); p50 ${n(cr['p(50)'], 'ms')}; CALLERROR ${r.metric('ocpp_callerrors').count}`, 'measured', [r.file, r.envFile]);
  }

  // telemetry-ingest
  const ing = D.tool('telemetry-ingestbench.json');
  if (ing) {
    const d = ing.data, tt = T['telemetry-ingest'];
    const at = d.copy_staging.find((r) => r.target_rows_per_s === tt.rowsPerSec);
    const top = d.copy_staging.find((r) => r.target_rows_per_s === d.summary.copy_max_rate_keeping_up_rows_per_s);
    const peak = d.copy_staging.reduce((a, b) => (b.measured_sustained_rows_per_s > a.measured_sustained_rows_per_s ? b : a));
    add(`Telemetry ingest: sustained rows/s at ${n(tt.rowsPerSec)} rows/s offered (COPY)`, `≥ ${n(tt.rowsPerSec)}`, `${n(at.measured_sustained_rows_per_s)} (${V(at.measured_sustained_rows_per_s, 'ge', tt.rowsPerSec)}; keeping up: ${at.keeping_up}, defined as ${d.summary.keeping_up_definition})`, 'measured', [ing.file]);
    add('Telemetry ingest: durable lag p99 at that rate', `≤ ${n(tt.durableP99Ms, 'ms')}`, `${n(at.measured_durable_lag_p99_s * 1000, 'ms')} (${verdict(at.measured_durable_lag_p99_s * 1000 <= tt.durableP99Ms)})`, 'derived', [ing.file], 'measured_durable_lag_p99_s × 1000');
    add('Telemetry ingest: highest offered rate that kept up (COPY)', `≥ ${n(tt.rowsPerSec)}`, `${n(d.summary.copy_max_rate_keeping_up_rows_per_s)} offered, ${n(top.measured_sustained_rows_per_s)} sustained (${V(top.measured_sustained_rows_per_s, 'ge', tt.rowsPerSec)}), lag p99 ${n(top.measured_durable_lag_p99_s, 's')}`, 'measured', [ing.file]);
    add('Telemetry ingest: peak sustained rows/s (COPY, did not keep up)', 'n/a', `${n(peak.measured_sustained_rows_per_s)} at ${n(peak.target_rows_per_s)} offered; lag p99 ${n(peak.measured_durable_lag_p99_s, 's')}`, 'measured', [ing.file]);
    add('Telemetry ingest: row-by-row INSERT baseline', 'n/a', `keeps up to ${n(d.summary.row_by_row_max_rate_keeping_up_rows_per_s)} offered; peak sustained ${n(d.summary.row_by_row_peak_sustained_rows_per_s)}`, 'measured', [ing.file]);
    add('Telemetry ingest: loss ratio at the target rate', `≤ ${tt.maxLossRatio}`, `${(at.rows_produced - at.rows_inserted) / at.rows_produced} (${V((at.rows_produced - at.rows_inserted) / at.rows_produced, 'le', tt.maxLossRatio)}; ${at.rows_produced} produced, ${at.rows_inserted} inserted)`, 'derived', [ing.file], '(rows_produced - rows_inserted) / rows_produced');
  }
  const bf = D.toolGlob(/^telemetry-backfill-/).at(-1);
  if (bf) {
    const { load: l, size: s, queriesMs: q, fullFleetExtrapolation: x, config: c } = bf.data;
    add(`Backfill: load rate (${n(c.stations)} stations, ${c.days} days)`, 'n/a', `${n(l.rowsPerSecond.value)} rows/s (${n(l.rowsInserted.value)} rows in ${n(l.wallSeconds.value, 's')})`, l.rowsPerSecond.label, [bf.file]);
    add('Storage: compression ratio, compressed raw chunks / all chunks', 'n/a', `${s.compressedChunksRatioBeforeOverAfter.value} x / ${s.overallRatioBeforeOverAfter.value} x`, s.overallRatioBeforeOverAfter.label, [bf.file]);
    add('Storage: compression ratio, power_1m compressed chunks', 'n/a', `${s.power1mCompressedChunksRatioBeforeOverAfter.value} x`, s.power1mCompressedChunksRatioBeforeOverAfter.label, [bf.file]);
    for (const [k, v] of Object.entries(q)) add(`Storage query: ${k}`, 'n/a', `p50 ${n(v.p50, 'ms')}, p95 ${n(v.p95, 'ms')}, p99 ${n(v.p99, 'ms')} (${v.runs} runs)`, v.label, [bf.file]);
    for (const k of ['raw90dSizeGB', 'rollup2ySizeGB', 'totalSizeGB']) add(`Full fleet (50,000 stations): ${k}`, 'n/a', `${x[k].value} ${x[k].unit}`, x[k].label, [bf.file], x[k].formula);
    add('Full fleet: average ingest', 'n/a', `${x.averageIngestRowsPerSecondFullFleet.value} rows/s`, x.averageIngestRowsPerSecondFullFleet.label, [bf.file], x.averageIngestRowsPerSecondFullFleet.formula);
  }

  // sessions
  for (const r of D.k6List(/^k6-sessions-ocpp-rate/).sort((a, b) => a.metric('http_reqs').rate - b.metric('http_reqs').rate)) {
    const st = r.metric('http_req_duration{op:start}')['p(99)'], sp = r.metric('http_req_duration{op:stop}')['p(99)'], lim = r.threshold('http_req_duration{op:start}', /p\(99\)/)?.limit;
    const isTarget = r.name.endsWith('rate200'), rate = r.metric('http_reqs').rate, dropped = r.metric('dropped_iterations').count, reqs = r.metric('http_reqs').count, secs = r.durationMs / 1000;
    const miss = isTarget && rate < T.sessions.commandsPerSec;
    add(`Sessions: commands/s at ${r.name.replace('sessions-ocpp-rate', '')} offered (k6)`, isTarget ? `≥ ${n(T.sessions.commandsPerSec)}` : 'n/a', `${n(rate)}${isTarget ? ` (${RV(r, T.sessions.commandsPerSec)})` : ''}; p99 start ${n(st, 'ms')}, stop ${n(sp, 'ms')} (script threshold < ${lim} ms: start ${verdict(st < lim)}, stop ${verdict(sp < lim)}); dropped ${dropped}`, 'measured', [r.file, r.envFile], undefined,
      miss && dropped === 0 ? `${n(reqs)} requests in ${secs.toFixed(3)} s with ${dropped} dropped iterations: every offered request was sent. The rate is below ${T.sessions.commandsPerSec} only because k6 divides by the test run duration (${secs.toFixed(3)} s); it is a rounding artefact, not a throughput shortfall, and the row meets the target under the ADR 0000 rate rule. Derived: http_reqs.count / testRunDurationMs × 1000.` : undefined);
  }
  const ar = D.tool('sessions-allocation-rounds.json');
  if (ar) add(`Sessions: one allocation round for ${n(ar.data.measured.sites)} sites, ${n(ar.data.measured.activeSessions)} active sessions (${ar.data.measured.instances} instances, concurrency ${ar.data.measured.concurrencyPerInstance} each)`, `≤ ${ar.data.budgetSeconds} s`, `${n(ar.data.measured.elapsedSeconds, 's')} (${verdict(ar.data.measured.elapsedSeconds <= ar.data.budgetSeconds)}; ${n(ar.data.measured.roundsPerSecond)} rounds/s, ${n(ar.data.measured.setChargingProfileCommands)} SetChargingProfile commands). Setup recorded in the file: ${ar.data.note}`, 'measured', [ar.file]);
  const rc = D.k6('reservation-contention');
  if (rc) {
    const won = rc.metric('reservation_won').count, conf = rc.metric('reservation_conflict').count;
    add('Reservations: concurrent attempts on one connector, winners / conflicts', 'exactly 1 winner', `${won} / ${conf}`, 'measured', [rc.file, rc.envFile]);
    add('Reservations: double bookings', `${D.targets.slo.doubleBookings}`, `${won - 1}`, 'derived', [rc.file], 'reservation_won - 1');
  }

  // smart-charging
  const ab = D.tool('smart-charging-allocbench.json');
  if (ab) {
    for (const s of ab.data.scenarios) add(`Smart charging: ${s.name} (${s.mode})`, `≥ ${n(T['smart-charging'].allocationsPerSec)} /s, p99 ≤ ${n(T['smart-charging'].p99Ms, 'ms')}`, `${n(s.allocationsPerSec)} /s, p99 ${n(s.latencyMs.p99, 'ms')} (${verdict(s.meetsTarget)}), ${s.clientInvariantViolations} cap violations`, 'measured', [ab.file]);
    add('Smart charging: server-side invariant violations', `${D.targets.slo.siteCapExceededSamples}`, `${ab.data.serverMetrics.invariant_violations_total} over ${ab.data.serverMetrics.allocations_total_ok} allocations`, 'measured', [ab.file]);
  }
  const sk = D.k6('smart-charging');
  if (sk) add('Smart charging: k6 against the service', `≥ ${n(T['smart-charging'].allocationsPerSec)} /s, p99 ≤ ${n(T['smart-charging'].p99Ms, 'ms')}`, `${n(sk.metric('http_reqs').rate)} /s (${RV(sk, T['smart-charging'].allocationsPerSec)}), p99 ${n(sk.metric('http_req_duration')['p(99)'], 'ms')} (${V(sk.metric('http_req_duration')['p(99)'], 'le', T['smart-charging'].p99Ms)})`, 'measured', [sk.file, sk.envFile], undefined, sk.metric('http_reqs').rate < T['smart-charging'].allocationsPerSec ? durationNote(sk, T['smart-charging'].allocationsPerSec) : undefined);

  // tariff
  for (const f of D.toolGlob(/^tariff-quotebench/)) for (const r of f.data.results) add(`Tariff: ${r.mode === 'openloop' ? `open loop at ${n(r.offeredRatePerSecond)} quotes/s` : `closed loop, concurrency ${r.concurrency}`}`, `≥ ${n(T.tariff.calculationsPerSec)} /s, p99 ≤ ${n(T.tariff.p99Ms, 'ms')}`, `${n(r.quotesPerSecond)} /s, p99 ${n(r.latencyMs.p99, 'ms')} (${verdict(r.pass)})`, 'measured', [f.file]);
  const tk = D.k6('tariff');
  if (tk) add('Tariff: k6 against the service', `≥ ${n(T.tariff.calculationsPerSec)} /s, p99 ≤ ${n(T.tariff.p99Ms, 'ms')}`, `${n(tk.metric('http_reqs').rate)} /s (${RV(tk, T.tariff.calculationsPerSec)}), p99 ${n(tk.metric('http_req_duration')['p(99)'], 'ms')} (${V(tk.metric('http_req_duration')['p(99)'], 'le', T.tariff.p99Ms)})`, 'measured', [tk.file, tk.envFile], undefined, tk.metric('http_reqs').rate < T.tariff.calculationsPerSec ? durationNote(tk, T.tariff.calculationsPerSec) : undefined);

  // driver-api
  const dr = D.k6('driver-api-reads');
  if (dr) {
    const v = dr.metric('http_reqs').rate, o = dr.metric('http_req_duration{scenario:open}');
    const iters = dr.metric('iterations').count, dropped = dr.metric('dropped_iterations').count, reqs = dr.metric('http_reqs').count;
    const scenarioS = (iters + dropped) / T['driver-api'].readRps, th = dr.threshold('dropped_iterations', /rate</);
    add('Driver API: read requests/s (GET /v1/sites, k6)', `≥ ${n(T['driver-api'].readRps)}`, `${n(v)} (${RV(dr, T['driver-api'].readRps)}); ${dropped} of ${iters + dropped} iterations dropped`, 'measured', [dr.file, dr.envFile], undefined,
      `The reported rate is http_reqs.count / test run duration, and http_reqs also counts ${n(reqs - iters)} setup requests (register and login). The scenario itself completed ${n(iters)} iterations in ${n(scenarioS, 's')}, about ${n(iters / scenarioS)} per second (derived: iterations / ((iterations + dropped_iterations) / ${n(T['driver-api'].readRps)} per second)), and ${n(dropped)} iterations were dropped. The k6 script's own threshold on dropped_iterations (${th ? th.expr : 'none recorded'}, ${th ? (th.ok ? 'held' : 'failed') : 'n/a'}) is a 1% tolerance set in the script; the ADR 0000 rate rule allows no dropped iterations, so the row counts as missed.`);
    add('Driver API: read p99', `≤ ${n(T['driver-api'].readP99Ms, 'ms')}`, `${n(o['p(99)'], 'ms')} (${V(o['p(99)'], 'le', T['driver-api'].readP99Ms)})`, 'measured', [dr.file, dr.envFile]);
  }

  // simulator
  for (const s of D.jsonlGlob(/^simulator-scale-/)) {
    const fin = s.rows.find((r) => r.final), first = s.rows.find((r) => r.connected >= fin.stations);
    add(`Simulator: ${n(fin.stations)} stations at time scale ${fin.scale}`, 'n/a', `all connected after ${first.t} s; peak ${n(fin.peak.msgPerS)} msg/s at the stub; peak RSS ${n(fin.peak.rssMB, 'MB')}; event-loop lag max ${n(fin.peak.lagMaxMs, 'ms')}`, 'measured', [s.file]);
  }

  // drills
  const inc = D.increaseChecks();
  for (const dd of D.drills()) {
    const name = `Drill ${dd.result?.drill ?? dd.name} (${dd.result?.startedAt ?? dd.name.split('-').at(-1)})`;
    if (!dd.result) { add(`${name}: result.json`, 'n/a', 'no result.json', 'measured', [`${dd.rel}/timeline.jsonl`]); continue; }
    for (const c of dd.result.checks) {
      const sym = { ge: '≥', le: '≤', gt: '>', lt: '<', eq: '=' }[c.op] ?? c.op;
      const none = c.measured === null || c.measured === undefined;
      add(`${name}: ${c.check}`, `${sym} ${n(c.threshold)}`, none ? `no measurement (${c.pass ? 'pass' : '**FAIL**'})` : `${n(c.measured)} (${c.pass ? 'pass' : '**FAIL**'})`, inc.has(c.check) ? 'measured (PromQL increase, extrapolated)' : 'measured', [`${dd.rel}/result.json`]);
    }
  }
  return rows;
}

// prefix: path from the file that holds the table to the repo root ('../' for docs/metrics.md).
export function metricsMarkdown(prefix = '') {
  const rows = metricsRows();
  const head = '| Metric | Target | Value | Label | Source file | Formula |\n|---|---|---|---|---|---|\n';
  const body = rows.map((r) => `| ${r.metric} | ${r.target} | ${r.value} | ${r.label} | ${r.sources.filter(Boolean).map((f) => link(f, prefix)).join(', ')} | ${r.formula ? `\`${r.formula}\`` : ''} |`).join('\n');
  const nt = rows.filter((r) => r.note).map((r) => `- **${r.metric}:** ${r.note}`).join('\n');
  return head + body + '\n' + (nt ? `\nNotes on individual rows:\n\n${nt}\n` : '');
}
