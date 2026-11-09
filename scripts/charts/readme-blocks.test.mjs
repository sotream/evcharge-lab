// node --test scripts/charts/   (no docker needed)
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import * as D from './data.mjs';
import { counts, glanceRows, glanceMarkdown, drillGroups, drillsTable, drillsPage, drillWhat, parseFindings, findingsTable, foundByKind, statsMarkdown } from './readme-blocks.mjs';

const read = (f) => fs.readFileSync(path.join(D.ROOT, f), 'utf8');
const README = read('README.md');
const FINDINGS = read('docs/findings.md');
const block = (md, name) => md.slice(md.indexOf(`<!-- ${name}:start`)).split(/-->\n/).slice(1).join('-->\n').split(`<!-- ${name}:end -->`)[0];

test('counts match the files on disk', () => {
  const c = counts(FINDINGS);
  assert.equal(c.runsTotal, fs.readdirSync(D.DRILL_DIR).length);
  assert.equal(c.runsPassed, D.drills().filter((d) => d.result?.pass).length);
  assert.ok(c.drillsLatestPass <= c.drillsTotal && c.drillsTotal <= c.runsTotal);
  assert.equal(c.drillScripts, fs.readdirSync(path.join(D.ROOT, 'drills')).filter((f) => f.endsWith('.sh')).length);
  assert.equal(c.findingsTotal, FINDINGS.match(/<details id="finding-\d+">/g).length);
});

test('glance table has one row per target in load/targets.json and verdicts follow the numbers', () => {
  const rows = glanceRows();
  assert.equal(rows.length, Object.values(D.targets.perInstance).flatMap(Object.keys).length);
  for (const m of D.targetMetrics()) assert.ok(rows.some((r) => r.service === m.service && r.metric === m.metric), `${m.service} ${m.metric}`);
  for (const r of rows) assert.ok(['pass', 'miss', 'none'].includes(r.status));
  assert.ok(rows.every((r) => r.status !== 'none' || /not tested/.test(r.verdict)));
});

test('drills table covers every run and links to a heading in docs/drills.md', () => {
  const groups = drillGroups(), page = drillsPage();
  assert.equal(groups.reduce((n, g) => n + g.runs.length, 0), D.drills().length);
  for (const d of D.drills()) assert.ok(page.includes(`### ${d.name}\n`), d.name);
  for (const g of groups) assert.ok(drillsTable().includes(`docs/drills.md#${g.latest.name.toLowerCase()}`) && page.includes(`(drills.md#${g.latest.name.toLowerCase()})`));
  assert.match(drillWhat('kafka-broker-loss-2-20261108T123144Z'), /two brokers down/);
});

test('findings are parsed from the items in docs/findings.md', () => {
  const f = parseFindings(FINDINGS);
  assert.deepEqual(f.map((x) => x.n), f.map((_, i) => i + 1));
  assert.equal(foundByKind('the Opus review of `billing`.'), 'review');
  assert.equal(foundByKind('a k6 sessions run'), 'k6');
  assert.equal(foundByKind('checking the running compose stack'), 'manual check');
  assert.equal(findingsTable(FINDINGS).split('\n').filter((l) => /^\| \d+ \|/.test(l)).length, f.length);
});

test('generated README blocks are not stale', () => {
  assert.equal(block(README, 'stats'), statsMarkdown(FINDINGS));
  assert.equal(block(README, 'glance'), glanceMarkdown());
  assert.equal(block(FINDINGS, 'findings'), findingsTable(FINDINGS));
  assert.equal(fs.readFileSync(path.join(D.ROOT, 'docs/drills.md'), 'utf8'), drillsPage());
});

const run = (rate, dropped, failed) => ({ metric: (k) => ({ http_reqs: { rate }, dropped_iterations: { count: dropped }, http_req_failed: { rate: failed } })[k] });
test('rate rule: a clean k6 run within 0.1% of the target meets it, a run with drops does not', () => {
  assert.equal(D.rateVerdict(run(199.99, 0, 0), 200), 'pass');
  assert.equal(D.rateVerdict(run(199.7, 0, 0), 200), 'miss');
  assert.equal(D.rateVerdict(run(199.99, 1, 0), 200), 'miss');
  assert.equal(D.rateVerdict(run(199.99, 0, 0.01), 200), 'miss');
  assert.equal(D.rateVerdict(run(200, 5, 0), 200), 'pass');
});
