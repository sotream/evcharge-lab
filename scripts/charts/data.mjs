// Raw-file readers and the metric model. Every number that reaches a chart or the README table is read here.
import fs from 'node:fs';
import path from 'node:path';

export const ROOT = path.resolve(import.meta.dirname, '../..');
export const LOAD_DIR = path.join(ROOT, 'docs/load-output');
// The drill output directory can be redirected (for example to a checkout that has newer drills).
export const DRILL_DIR = process.env.DRILL_OUTPUT_DIR ? path.resolve(process.env.DRILL_OUTPUT_DIR) : path.join(ROOT, 'docs/drill-output');

const readJson = (p) => JSON.parse(fs.readFileSync(p, 'utf8'));
// Every skipped or damaged input is reported here and on stderr; build.mjs prints the list again at the end.
export const warnings = [];
export function warn(msg) { if (warnings.includes(msg)) return; warnings.push(msg); console.warn(`WARNING: ${msg}`); }
// Reads a JSON file that may be malformed (a drill harness bug can leave invalid JSON); null plus a warning, never silent.
function readJsonSafe(p) {
  try { return readJson(p); } catch (e) { warn(`${path.relative(ROOT, p)} is not valid JSON (${e.message}); the file is skipped`); return null; }
}
export const targets = readJson(path.join(ROOT, 'load/targets.json'));
export const TARGETS_FILE = 'load/targets.json';
const exists = (p) => fs.existsSync(p);
const rel = (f) => `docs/load-output/${f}`;

// k6 ------------------------------------------------------------------------------------------------
export function k6(name) {
  const f = path.join(LOAD_DIR, `k6-${name}.json`);
  if (!exists(f)) return null;
  const sum = readJsonSafe(f);
  if (!sum?.metrics || !sum.state) { if (sum) warn(`${rel(`k6-${name}.json`)} has no metrics or state block; the file is skipped`); return null; }
  const ef = path.join(LOAD_DIR, `k6-${name}.env.json`);
  const env = exists(ef) ? readJsonSafe(ef) : null;
  const metric = (key) => sum.metrics[key]?.values;
  const threshold = (key, pat) => {
    const th = sum.metrics[key]?.thresholds ?? {};
    const k = Object.keys(th).find((t) => pat.test(t));
    return k ? { expr: k, ok: th[k].ok, limit: Number(k.match(/[<>=]+\s*([\d.]+)/)?.[1]) } : null;
  };
  return { name, file: rel(`k6-${name}.json`), envFile: env ? rel(`k6-${name}.env.json`) : null, sum, env, metric, threshold, durationMs: sum.state.testRunDurationMs };
}
export function k6List(re) {
  return fs.readdirSync(LOAD_DIR).filter((f) => re.test(f) && f.endsWith('.json') && !f.endsWith('.env.json')).sort().map((f) => k6(f.replace(/^k6-/, '').replace(/\.json$/, ''))).filter(Boolean);
}
export function envLine(run) {
  if (!run?.env) return `${run?.file}: no .env.json setup record`;
  const e = run.env;
  return `${run.name}: ${e.generator}, measured ${e.measuredAt.slice(0, 19)}Z, ${e.host.os}/${e.host.arch}, Docker VM ${e.dockerCpus} CPUs / ${(e.dockerMemBytes / 1e9).toFixed(1)} GB, ${e.note}.`;
}

// Other tool outputs --------------------------------------------------------------------------------
export function tool(file) {
  const p = path.join(LOAD_DIR, file);
  const data = exists(p) ? readJsonSafe(p) : null;
  return data ? { file: rel(file), data } : null;
}
export function toolGlob(re) {
  return fs.readdirSync(LOAD_DIR).filter((f) => re.test(f)).sort().map((f) => ({ file: rel(f), data: readJsonSafe(path.join(LOAD_DIR, f)) })).filter((x) => x.data);
}
export function jsonl(file) {
  const p = path.join(LOAD_DIR, file);
  const rows = [];
  for (const l of fs.readFileSync(p, 'utf8').split('\n').filter(Boolean)) { try { rows.push(JSON.parse(l)); } catch { warn(`${rel(file)}: an unreadable line was skipped`); } }
  return { file: rel(file), rows };
}
export function jsonlGlob(re) {
  return fs.readdirSync(LOAD_DIR).filter((f) => re.test(f)).sort().map(jsonl);
}

// Drills --------------------------------------------------------------------------------------------
// Drill checks whose value is a PromQL increase() over a window; the result is extrapolated, so it is a float.
export function increaseChecks() {
  const dir = path.join(ROOT, 'drills');
  const names = new Set();
  if (!exists(dir)) return names;
  for (const f of fs.readdirSync(dir).filter((x) => x.endsWith('.sh'))) {
    for (const m of fs.readFileSync(path.join(dir, f), 'utf8').matchAll(/check "([\w-]+)" "\$\(prom '[^']*increase\(/g)) names.add(m[1]);
  }
  return names;
}
export function drills() {
  if (!exists(DRILL_DIR)) return [];
  const out = [];
  for (const d of fs.readdirSync(DRILL_DIR, { withFileTypes: true })) {
    if (!d.isDirectory()) continue;
    const dir = path.join(DRILL_DIR, d.name);
    const tl = path.join(dir, 'timeline.jsonl');
    if (!exists(tl)) { warn(`docs/drill-output/${d.name} has no timeline.jsonl; the directory is skipped`); continue; }
    const rows = [];
    let bad = 0;
    for (const l of fs.readFileSync(tl, 'utf8').split('\n').filter(Boolean)) {
      try { const r = JSON.parse(l); if (r && typeof r === 'object' && typeof r.s === 'number') rows.push(r); else bad++; } catch { bad++; }
    }
    if (bad) warn(`docs/drill-output/${d.name}/timeline.jsonl: ${bad} unreadable line(s) skipped`);
    if (rows.length < 2) { warn(`docs/drill-output/${d.name}/timeline.jsonl has fewer than two usable samples; the directory is skipped`); continue; }
    const opt = (f) => (exists(path.join(dir, f)) ? readJsonSafe(path.join(dir, f)) : null);
    let result = opt('result.json');
    if (result && !Array.isArray(result.checks)) { warn(`docs/drill-output/${d.name}/result.json has no checks array; the file is skipped`); result = null; }
    out.push({ name: d.name, rel: `docs/drill-output/${d.name}`, rows, result, queries: opt('queries.json') ?? opt('q.json'), hasEvents: exists(path.join(dir, 'events.log')) });
  }
  return out.sort((a, b) => a.name.localeCompare(b.name));
}
export const latestDrill = (prefix) => drills().findLast((d) => d.name.startsWith(prefix)) ?? null;

const tariffSetup = (r) => `${r.data.server.loadGeneratorHost}; server ${r.data.server.concurrency}, CPU limit ${r.data.server.cpuLimit}; Go ${r.data.host.go}, ${r.data.host.numCPU} CPUs.`;
const envSig = (e) => JSON.stringify([e.generator, e.host, e.dockerCpus, e.dockerMemBytes, e.note]);

// Metric model ---------------------------------------------------------------------------------------
// Verdict: computed from the numbers alone, no tolerance. 'none' = the raw file has no value.
export function verdictOf(value, op, target) {
  if (typeof value !== 'number' || Number.isNaN(value)) return 'none';
  return (op === 'ge' ? value >= target : value <= target) ? 'pass' : 'miss';
}
export function targetMetrics() {
  const T = targets.perInstance;
  const out = [];
  const add = (m) => out.push({ label: 'measured', ...m, status: verdictOf(m.value, m.op, m.target) });

  // station-gateway
  const g10k = tool('gateway-10k-connections.json');
  const gwRuns = k6List(/^k6-gateway-ws-/);
  // Rule: rate and p99 both come from the run with the highest achieved rate, whether or not its p99 met the target.
  const gwBest = gwRuns.length && gwRuns.reduce((a, b) => (b.metric('ocpp_callresults').rate > a.metric('ocpp_callresults').rate ? b : a));
  if (g10k) add({ key: 'connections', service: 'station-gateway', metric: 'WebSocket connections', unit: '', target: T['station-gateway'].connections, op: 'ge', value: g10k.data.summary.peakConnections, sources: [g10k.file, TARGETS_FILE], setup: g10k.data.setup.gateway });
  if (gwBest) add({ key: 'msgPerSec', service: 'station-gateway', metric: 'Messages per second', unit: '/s', target: T['station-gateway'].msgPerSec, op: 'ge', value: gwBest.metric('ocpp_callresults').rate, sources: [gwBest.file, TARGETS_FILE], setup: envLine(gwBest), note: gwBest.name });
  if (gwBest) add({ key: 'callResultP99Ms', service: 'station-gateway', metric: 'CALLRESULT p99', unit: 'ms', target: T['station-gateway'].callResultP99Ms, op: 'le', value: gwBest.metric('ocpp_callresult_ms')['p(99)'], sources: [gwBest.file, TARGETS_FILE], setup: envLine(gwBest), note: gwBest.name });

  // telemetry-ingest
  const ing = tool('telemetry-ingestbench.json');
  if (ing) {
    const tt = T['telemetry-ingest'];
    const atTarget = ing.data.copy_staging.find((r) => r.target_rows_per_s === tt.rowsPerSec);
    const set = `${ing.data.note} Go ${ing.data.environment.go}, ${ing.data.environment.num_cpu} CPUs, ${ing.data.environment.partitions} partitions, flush ${ing.data.environment.flush}.`;
    // Same run and value as the README metrics table: the run offered at the target rate, not the highest rate that kept up.
    add({ key: 'rowsPerSec', service: 'telemetry-ingest', metric: `Sustained rows per second (${tt.rowsPerSec.toLocaleString('en-US')} offered)`, unit: '/s', target: tt.rowsPerSec, op: 'ge', value: atTarget.measured_sustained_rows_per_s, sources: [ing.file, TARGETS_FILE], setup: set, note: `offered ${tt.rowsPerSec}/s` });
    add({ key: 'durableP99Ms', service: 'telemetry-ingest', metric: 'Durable lag p99 (at the target rate)', unit: 'ms', target: tt.durableP99Ms, op: 'le', value: atTarget.measured_durable_lag_p99_s * 1000, sources: [ing.file, TARGETS_FILE], setup: set, note: `offered ${tt.rowsPerSec}/s` });
    add({ key: 'maxLossRatio', service: 'telemetry-ingest', metric: 'Loss ratio at the target rate', unit: '', target: tt.maxLossRatio, op: 'le', value: (atTarget.rows_produced - atTarget.rows_inserted) / atTarget.rows_produced, label: 'derived', formula: '(rows_produced - rows_inserted) / rows_produced', sources: [ing.file, TARGETS_FILE], setup: set, note: `offered ${tt.rowsPerSec}/s` });
  }

  // sessions
  const sess = k6List(/^k6-sessions-ocpp-rate/).sort((a, b) => a.metric('http_reqs').rate - b.metric('http_reqs').rate);
  const sBest = sess.at(-1);
  if (sBest) {
    const v = sBest.metric('http_reqs').rate;
    add({ key: 'commandsPerSec', service: 'sessions', metric: 'Commands per second (StartTransaction + StopTransaction)', unit: '/s', target: T.sessions.commandsPerSec, op: 'ge', value: v, sources: [sBest.file, TARGETS_FILE], setup: envLine(sBest), note: `no iterations dropped: ${sBest.metric('dropped_iterations').count}` });
  }

  // smart-charging
  const ab = tool('smart-charging-allocbench.json');
  if (ab) {
    const fixed = ab.data.scenarios.find((s) => s.mode === 'open-loop');
    const set = `${ab.data.note} Go ${ab.data.goVersion}, ${ab.data.platform}, ${ab.data.clientNumCPU} CPUs, ${ab.data.options.sites} synthetic sites (mean ${ab.data.siteShape.sessionsMean} sessions each).`;
    add({ key: 'p99Ms', service: 'smart-charging', metric: 'Allocation p99 (open loop at the target rate)', unit: 'ms', target: T['smart-charging'].p99Ms, op: 'le', value: fixed.latencyMs.p99, sources: [ab.file, TARGETS_FILE], setup: set });
  }
  // Rate: the k6 run against the service, the same value and verdict as the README metrics table.
  const sk = k6('smart-charging');
  if (sk) add({ key: 'allocationsPerSec', service: 'smart-charging', metric: 'Allocations per second (k6, service)', unit: '/s', target: T['smart-charging'].allocationsPerSec, op: 'ge', value: sk.metric('http_reqs').rate, sources: [sk.file, TARGETS_FILE], setup: envLine(sk), note: 'k6 http_reqs.count / run duration' });

  // tariff
  const qb = toolGlob(/^tariff-quotebench/).flatMap((f) => f.data.results.map((r) => ({ ...r, file: f.file, data: f.data })));
  if (qb.length) {
    const open = qb.find((r) => r.mode === 'openloop');
    if (open) add({ key: 'p99Ms', service: 'tariff', metric: 'Quote p99 (open loop at the target rate)', unit: 'ms', target: T.tariff.p99Ms, op: 'le', value: open.latencyMs.p99, sources: [open.file, TARGETS_FILE], setup: tariffSetup(open) });
  }
  const tk = k6('tariff');
  if (tk) add({ key: 'calculationsPerSec', service: 'tariff', metric: 'Quotes per second (k6, service)', unit: '/s', target: T.tariff.calculationsPerSec, op: 'ge', value: tk.metric('http_reqs').rate, sources: [tk.file, TARGETS_FILE], setup: envLine(tk), note: 'k6 http_reqs.count / run duration' });

  // driver-api
  const dr = k6('driver-api-reads');
  if (dr) {
    const v = dr.metric('http_reqs').rate;
    const open = dr.metric('http_req_duration{scenario:open}') ?? dr.metric('http_req_duration');
    add({ key: 'readRps', service: 'driver-api', metric: 'Read requests per second (GET /v1/sites)', unit: '/s', target: T['driver-api'].readRps, op: 'ge', value: v, sources: [dr.file, TARGETS_FILE], setup: envLine(dr), note: `${dr.metric('dropped_iterations').count} of ${dr.metric('iterations').count + dr.metric('dropped_iterations').count} iterations dropped` });
    add({ key: 'readP99Ms', service: 'driver-api', metric: 'Read p99', unit: 'ms', target: T['driver-api'].readP99Ms, op: 'le', value: open['p(99)'], sources: [dr.file, TARGETS_FILE], setup: envLine(dr) });
  }
  return out;
}

// One footer sentence for several k6 runs that share a setup; flags it when their setup records differ.
export function envSummary(runs) {
  const withEnv = runs.filter((r) => r.env);
  if (!withEnv.length) return 'no .env.json setup record';
  const e = withEnv[0].env, times = withEnv.map((r) => r.env.measuredAt).sort();
  const same = new Set(withEnv.map((r) => envSig(r.env))).size === 1;
  return `${e.generator}; ${e.host.os}/${e.host.arch}; Docker VM ${e.dockerCpus} CPUs and ${(e.dockerMemBytes / 1e9).toFixed(1)} GB; ${e.note}. Measured ${times[0].slice(0, 19)}Z to ${times.at(-1).slice(0, 19)}Z${same ? '' : ' (setups differ between runs)'}; the containers running during each run are listed in ${withEnv.map((r) => r.envFile.split('/').pop()).join(', ')}.`;
}

// Offered load of a gateway k6 run: each station keeps one CALL outstanding and sends at its set rate, so the offered
// rate is stations x Hz (ws_sessions and the Hz in the file name, for example gateway-ws-1000st-1hz). Derived.
export function gatewayOfferedRate(run) {
  const hz = Number(run.name.match(/(\d+)hz/)?.[1]);
  const stations = run.metric('ws_sessions')?.count;
  return Number.isFinite(hz) && typeof stations === 'number' ? stations * hz : null;
}

export const SERVICE_ORDER =['station-gateway', 'telemetry-ingest', 'sessions', 'smart-charging', 'tariff', 'driver-api'];
