// Generated README and docs blocks: stat tiles, targets at a glance, drills table, findings table, docs/drills.md.
// Every number comes from the repository files or from the raw measurement files; none is typed here.
import fs from 'node:fs';
import path from 'node:path';
import * as D from './data.mjs';
import { ROOT } from './data.mjs';

const T = D.targets.perInstance;
const SYM = { ge: '≥', le: '≤' };
const num = (v) => (v !== 0 && Math.abs(v) < 0.01 ? String(Number(v.toPrecision(3))) : Number(v.toFixed(2)).toLocaleString('en-US', { maximumFractionDigits: 2 }));
const slug = (s) => s.toLowerCase();
const exists = (rel) => fs.existsSync(path.join(ROOT, rel));
const runBase = (name) => name.replace(/-\d{8}T\d{6}Z$/, '');
const link = (f, prefix = '') => `[\`${f.split('/').pop()}\`](${prefix}${f})`;

// Findings ------------------------------------------------------------------------------------------
// Each finding in the README is <details id="finding-N"><summary>N. title</summary> ... </details>.
export function parseFindings(md) {
  const out = [];
  for (const m of md.matchAll(/<details id="finding-(\d+)">\n<summary>(.*?)<\/summary>\n([\s\S]*?)<\/details>/g)) {
    const body = m[3], foundBy = body.match(/^- \*\*Found by:\*\* (.*)$/m)?.[1] ?? '';
    out.push({ n: Number(m[1]), title: m[2].replace(/^\d+\.\s*/, ''), foundBy, fixed: /^- \*\*(?:Process f|F)ix:\*\*/m.test(body) });
  }
  return out;
}
export function foundByKind(text) {
  if (/review|audit/i.test(text)) return 'review';
  if (/\bk6\b/.test(text)) return 'k6';
  if (/drill|\bre?runs?\b|result\.json/i.test(text)) return 'drill';
  return 'manual check';
}
export function findingsTable(md) {
  const rows = parseFindings(md).map((f) => `| ${f.n} | ${f.title} | ${foundByKind(f.foundBy)} | [details](#finding-${f.n}) |`);
  return '| # | Finding | Found by | Item |\n|---:|---|---|---|\n' + rows.join('\n') + '\n';
}

// Stats ---------------------------------------------------------------------------------------------
const ls = (rel, f) => fs.readdirSync(path.join(ROOT, rel)).filter(f);
export function counts(md) {
  const ds = D.drills(), found = parseFindings(md);
  return {
    services: ls('services', (d) => exists(`services/${d}/package.json`) || exists(`services/${d}/go.mod`)).length,
    adrs: ls('docs/adr', (f) => /^\d{4}-.*\.md$/.test(f)).length,
    drillScripts: ls('drills', (f) => f.endsWith('.sh')).length,
    runsPassed: ds.filter((d) => d.result?.pass === true).length,
    runsTotal: ds.length,
    drillsLatestPass: drillGroups().filter((g) => g.latest.result?.pass === true).length,
    drillsTotal: drillGroups().length,
    findingsFixed: found.filter((f) => f.fixed).length,
    findingsTotal: found.length,
  };
}
export function statsMarkdown(md) {
  const c = counts(md);
  return `| ${c.services} services | ${c.adrs} ADRs | ${c.drillScripts} drill scripts | ${c.drillsLatestPass} of ${c.drillsTotal} drills pass on the latest run | ${c.findingsFixed} of ${c.findingsTotal} findings fixed |
|:---:|:---:|:---:|:---:|:---:|
| [NestJS and Go, plus a station simulator](#the-system-design-question-each-service-answers) | [one decision per file](docs/adr/README.md) | [failure drills](#drills) | [${c.runsTotal - c.runsPassed} failed runs kept as evidence](docs/drills.md) | [found by drills, k6 and reviews](#what-the-drills-and-reviews-caught) |
`;
}

// Targets at a glance -------------------------------------------------------------------------------
const UNMEASURED = { visibleP99Ms: ['Telemetry visible in driver-api p99', 'ms', 'le'], writeP99Ms: ['Write p99', 'ms', 'le'] };
export function glanceRows() {
  const ms = D.targetMetrics(), rows = [];
  for (const service of D.SERVICE_ORDER) for (const [key, target] of Object.entries(T[service] ?? {})) {
    const m = ms.find((x) => x.service === service && x.key === key);
    const [metric, unit, op] = m ? [m.metric, m.unit, m.op] : UNMEASURED[key] ?? [key, '', 'le'];
    const offered = m?.note && D.k6(m.note) ? D.gatewayOfferedRate(D.k6(m.note)) : null;
    const status = !m || m.status === 'none' || (service === 'station-gateway' && key === 'msgPerSec' && offered !== null && offered < target) ? 'none' : m.status;
    rows.push({ service, metric, target: `${SYM[op]} ${num(target)}${unit ? ' ' + unit : ''}`, measured: m && m.value !== null && m.value !== undefined ? `${num(m.value)}${unit ? ' ' + unit : ''}` : 'no measurement', status, verdict: status === 'pass' && m.op === 'ge' && m.value < target ? '✅ met (rate rule)' : { pass: '✅ met', miss: '❌ missed', none: 'not tested' }[status], source: m ? link(m.sources[0]) : 'no saved run' });
  }
  return rows;
}
export function glanceMarkdown() {
  return '| Service | Metric | Target | Measured | Verdict | Source |\n|---|---|---|---|---|---|\n' +
    glanceRows().map((r) => `| ${r.service} | ${r.metric} | ${r.target} | ${r.measured} | ${r.verdict} | ${r.source} |`).join('\n') + '\n';
}

// Drills --------------------------------------------------------------------------------------------
// "What it breaks": the first sentence of the comment header of drills/<script>.sh; for a numbered variant
// (kafka-broker-loss-1) the header line that documents that argument.
export function drillWhat(name) {
  const base = runBase(name), script = exists(`drills/${base}.sh`) ? base : base.replace(/-\d+$/, ''), arg = base.slice(script.length + 1);
  const file = path.join(ROOT, `drills/${script}.sh`);
  if (!fs.existsSync(file)) return 'no drill script';
  const head = [];
  for (const l of fs.readFileSync(file, 'utf8').split('\n').slice(1)) { if (!l.startsWith('#')) break; head.push(l.replace(/^#\s?/, '')); }
  if (arg) {
    const m = head.map((l) => l.match(new RegExp(`\\.sh ${arg}\\s+(.+)`))?.[1]).find(Boolean);
    if (m) return m.split(':')[0].trim();
  }
  const text = head.join(' ').replace(/^Drill:\s*/, '');
  return (text.match(/^(.*?)\.(\s|$)/)?.[1] ?? text).replace(/\s*\(ADR[^)]*\)$/, '').replace(/ for DOWN_S seconds/, '').replace(/,\s*optionally.*$/, '').trim();
}
export function drillGroups() {
  const g = new Map();
  for (const d of D.drills()) { const k = d.result?.drill ?? runBase(d.name); if (!g.has(k)) g.set(k, []); g.get(k).push(d); }
  return [...g].map(([name, runs]) => ({ name, runs, latest: runs.at(-1), passed: runs.filter((r) => r.result?.pass === true).length }));
}
const verdictCell = (r) => (!r.result ? 'no result.json' : r.result.pass ? '✅ passed' : '❌ failed');
export function drillsTable() {
  return '| Drill | What it breaks | Runs passed | Latest run | Timeline |\n|---|---|:---:|---|---|\n' +
    drillGroups().map((g) => `| \`${g.name}\` | ${drillWhat(g.name)} | ${g.passed} of ${g.runs.length} | ${verdictCell(g.latest)} | [${g.latest.name.match(/\d{8}T\d{6}Z$/)?.[0] ?? g.latest.name}](docs/drills.md#${slug(g.latest.name)}) |`).join('\n') + '\n';
}

// docs/drills.md ------------------------------------------------------------------------------------
export function drillsPage() {
  const out = ['# Drill runs', '',
    'Generated by `node scripts/charts/build.mjs` from `docs/drill-output/`. Every run is listed, including the failed and inconclusive ones: they stay as evidence next to their notes. Each timeline shows the key series with before, during and after phases and the checks from `result.json`. The summary table is in the [README](../README.md#drills).', ''];
  for (const g of drillGroups()) {
    out.push(`## ${g.name}`, '', `${drillWhat(g.name)}. ${g.passed} of ${g.runs.length} runs passed.`, '');
    for (const r of g.runs) {
      const failed = r.result?.checks.filter((c) => !c.pass).map((c) => `\`${c.check}\``) ?? [];
      out.push(`### ${r.name}`, '', `Result: ${verdictCell(r)}${failed.length ? `; failed checks: ${failed.join(', ')}` : ''}. Files: ${[r.result ? `[result.json](drill-output/${r.name}/result.json)` : null, exists(`${r.rel}/NOTE.md`) ? `[NOTE.md](drill-output/${r.name}/NOTE.md)` : null, `[timeline.jsonl](drill-output/${r.name}/timeline.jsonl)`].filter(Boolean).join(', ')}.`, '');
      if (exists(`docs/img/drill-${r.name}.svg`)) out.push(`![Drill timeline: ${r.name}](img/drill-${r.name}.svg)`, '');
    }
  }
  return out.join('\n');
}
