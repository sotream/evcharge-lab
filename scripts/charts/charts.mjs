// Chart builders. Each returns { file, svg } (or null when its raw input is missing). All values come from data.mjs.
import { doc, text, line, rect, path, circle, legend, plotFrame, linear, logScale, niceTicks, logTicks, fmt, si, bytesFmt, wrap } from './lib.mjs';
import * as D from './data.mjs';

const SERIES = ['blue', 'orange', 'purple', 'teal', 'green'];
const median = (a) => { const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length / 2)]; };
const FILL = { pass: 'f-green', miss: 'f-red', none: 'f-muted' };
const WORD = { pass: 'PASS', miss: 'MISS', none: 'NO DATA' };
const cmp = (m) => (m.op === 'ge' ? '≥' : '≤');
const val = (v, unit) => `${fmt(v, 2)}${unit ? (unit === '/s' ? ' /s' : ' ' + unit) : ''}`;

function badge(x, y, status) {
  const w = status === 'none' ? 70 : 54;
  return rect(x, y - 15, w, 22, FILL[status], 'rx="5"') + text(x + w / 2, y + 1, WORD[status], { anchor: 'middle', cls: 'b on', size: 12 });
}

// 1. Target vs measured ---------------------------------------------------------------------------
export function targetVsMeasured() {
  const ms = D.targetMetrics();
  const W = 980, x0 = 420, bw = 330, max = 2;
  const sx = linear(0, max, x0, x0 + bw);
  let y = 100;
  const out = [];
  out.push(text(x0 + bw / 2, 68, 'measured ÷ target (bars clip at 2×; the line marks 1× = the target)', { anchor: 'middle', cls: 'm' }));
  const rowsStart = y;
  for (const svc of D.SERVICE_ORDER) {
    const rows = ms.filter((m) => m.service === svc);
    if (!rows.length) continue;
    y += 14;
    out.push(text(20, y, svc, { cls: 'h' }));
    y += 8;
    for (const m of rows) {
      const ratio = typeof m.value === 'number' ? m.value / m.target : 0;
      const barW = Math.max(sx(Math.min(ratio, max)) - x0, ratio === 0 ? 0 : 2);
      out.push(text(34, y + 15, m.metric, { cls: 'b' }));
      out.push(text(34, y + 33, `${typeof m.value === 'number' ? val(m.value, m.unit) : 'no measurement'} measured, target ${cmp(m)} ${val(m.target, m.unit)}${m.label === 'derived' ? ' (derived)' : ''}`, { cls: 'm' }));
      out.push(rect(x0, y + 8, barW, 22, FILL[m.status], 'fill-opacity=".85"'));
      if (ratio > max) out.push(text(x0 + bw - 4, y + 24, `${fmt(ratio)}×  ▸`, { anchor: 'end', cls: 'b on' }));
      else out.push(text(x0 + barW + 8, y + 24, ratio === 0 ? '0' : `${fmt(ratio)}×`, { cls: 'm' }));
      out.push(badge(x0 + bw + 70, y + 24, m.status));
      y += 46;
    }
    y += 6;
  }
  const h = y + 4;
  // target line and axis
  out.unshift(line(sx(1), rowsStart, sx(1), h - 14, 'axis', 'stroke-dasharray="4 3" style="stroke:var(--fg)"'));
  for (const t of [0, 0.5, 1, 1.5, 2]) out.unshift(text(sx(t), rowsStart - 2, `${t}×`, { anchor: 'middle', cls: 'm' }));
  // Verdicts are value versus target; k6 throughput rows use the ADR 0000 rate rule. Say which misses are near misses.
  const near = ms.filter((m) => m.status === 'miss' && typeof m.value === 'number' && Math.abs(m.value / m.target - 1) <= 0.01).map((m) => `${m.service} ${m.metric} (${fmt(m.value / m.target)}× of target)`);
  const foot = `Verdicts compare the measured value with the ADR 0000 target; k6 throughput rows within 0.1% with no dropped iteration and no failed request count as met (ADR 0000 rate rule).${near.length ? ` Misses within 1% of the target: ${near.join('; ')}; the README metrics table explains each.` : ''}`;
  out.push(...wrap(foot, 120).map((t, i) => text(20, h + 8 + i * 17, t, { cls: 'm' })));
  const srcs = [...new Set(ms.flatMap((m) => m.sources))].sort();
  const setups = [...new Map(ms.map((m) => [m.setup, `Setup (${m.sources[0].split('/').pop()}): ${m.setup}`])).values()];
  return {
    file: 'targets-vs-measured.svg',
    svg: doc({
      w: W, h: h + 40, title: 'Per-instance targets versus measured values', subtitle: 'Per-instance targets from ADR 0000; every bar is the measured value divided by its target.', body: out.join(''),
      footer: [`Sources: ${srcs.join(', ')}.`, ...setups],
    }),
  };
}

// 2. Throughput and p99 per service -----------------------------------------------------------------
export function throughputAndP99() {
  const rows = [];
  const add = (label, unit, rate, p99, limit, run) => rows.push({ label, unit, rate, p99, limit, run });
  for (const r of D.k6List(/^k6-gateway-ws-/)) add(`station-gateway, ${r.metric('ws_sessions').count} stations`, 'msg/s', D.gatewayRate(r), r.metric('ocpp_callresult_ms')['p(99)'], r.threshold('ocpp_callresult_ms', /p\(99\)/)?.limit, r);
  const sess = D.k6List(/^k6-sessions-ocpp-rate/).sort((a, b) => a.metric('http_reqs').rate - b.metric('http_reqs').rate).at(-1);
  if (sess) add('sessions (highest rate of the sweep)', 'commands/s', sess.metric('http_reqs').rate, sess.metric('http_req_duration')['p(99)'], sess.threshold('http_req_duration{op:start}', /p\(99\)/)?.limit, sess);
  const dr = D.k6('driver-api-reads');
  if (dr) add('driver-api reads', 'req/s', dr.metric('http_reqs').rate, (dr.metric('http_req_duration{scenario:open}') ?? dr.metric('http_req_duration'))['p(99)'], dr.threshold('http_req_duration{scenario:open}', /p\(99\)/)?.limit, dr);
  const sc = D.k6('smart-charging');
  if (sc) add('smart-charging (k6 against the service)', 'alloc/s', sc.metric('http_reqs').rate, sc.metric('http_req_duration')['p(99)'], sc.threshold('http_req_duration', /p\(99\)/)?.limit, sc);
  const ta = D.k6('tariff');
  if (ta) add('tariff (k6 against the service)', 'quotes/s', ta.metric('http_reqs').rate, ta.metric('http_req_duration')['p(99)'], ta.threshold('http_req_duration', /p\(99\)/)?.limit, ta);

  const W = 980, rh = 40, top = 96, lx = 20, ax = 290, aw = 300, bx = 650, bw = 250;
  const h = top + rows.length * rh + 40;
  const xa = logScale(10, 10000, ax, ax + aw), xb = logScale(1, 10000, bx, bx + bw);
  const out = [];
  out.push(text(ax + aw / 2, 68, 'Achieved rate (log scale)', { anchor: 'middle', cls: 'h' }), text(bx + bw / 2, 68, 'p99 latency, ms (log scale)', { anchor: 'middle', cls: 'h' }));
  for (const t of logTicks(10, 10000)) { out.push(line(xa(t), top - 10, xa(t), h - 30), text(xa(t), top - 14, fmt(t, 0), { anchor: 'middle', cls: 'm' })); }
  for (const t of logTicks(1, 10000)) { out.push(line(xb(t), top - 10, xb(t), h - 30), text(xb(t), top - 14, fmt(t, 0), { anchor: 'middle', cls: 'm' })); }
  rows.forEach((r, i) => {
    const y = top + i * rh;
    const ok = r.limit === undefined || r.p99 < r.limit; // same comparison as the k6 threshold p(99)<limit
    out.push(text(lx, y + 21, r.label, { cls: 'b' }));
    out.push(rect(ax, y + 8, xa(r.rate) - ax, 20, 'f-blue', 'fill-opacity=".85"'), text(xa(r.rate) + 6, y + 23, `${fmt(r.rate, 2)} ${r.unit}`, { cls: 'm' }));
    out.push(rect(bx, y + 8, xb(r.p99) - bx, 20, ok ? 'f-green' : 'f-red', 'fill-opacity=".85"'), text(Math.max(xb(r.p99), r.limit ? xb(r.limit) : 0) + 8, y + 23, `${fmt(Math.round(r.p99 * 10) / 10)} ms`, { cls: 'm' }));
    if (r.limit !== undefined) out.push(line(xb(r.limit), y + 3, xb(r.limit), y + 33, 'axis', 'style="stroke:var(--fg);stroke-width:2.5"'));
  });
  out.push(legend(bx - 150 + 150, h - 8, [{ label: 'p99 within threshold', cls: 'f-green' }, { label: 'p99 over threshold', cls: 'f-red' }]), text(20, h - 8, 'Vertical tick = the p99 threshold in the k6 script.', { cls: 'm' }));
  const runs = rows.map((r) => r.run);
  return {
    file: 'throughput-p99.svg',
    svg: doc({
      w: W, h, title: 'Throughput and p99 latency per service (k6)', subtitle: 'Latency metric: ocpp_callresult_ms for the gateway, http_req_duration{scenario:open} for driver-api, http_req_duration otherwise.', body: out.join(''),
      footer: [`Sources: ${runs.map((r) => r.file).join(', ')}.`, 'Rates are k6 http_reqs.count divided by the test run duration, so a run that lasts slightly over 60 s reports a rate slightly under its offered rate.', `Setup: k6, ${D.envSummary(runs)}`],
    }),
  };
}

// 3. Sessions rate sweep ----------------------------------------------------------------------------
export function sessionsSweep() {
  const runs = D.k6List(/^k6-sessions-ocpp-rate/).sort((a, b) => a.metric('http_reqs').rate - b.metric('http_reqs').rate);
  if (!runs.length) return null;
  const pts = runs.map((r) => ({ x: r.metric('http_reqs').rate, start: r.metric('http_req_duration{op:start}')['p(99)'], stop: r.metric('http_req_duration{op:stop}')['p(99)'], p50: r.metric('http_req_duration')['p(50)'], run: r }));
  const limit = runs[0].threshold('http_req_duration{op:start}', /p\(99\)/)?.limit;
  const W = 760, H = 400, px = 80, py = 80, pw = 640, ph = 250;
  const ymax = Math.max(...pts.flatMap((p) => [p.start, p.stop]), limit ?? 0) * 1.12;
  const xs = linear(0, Math.max(...pts.map((p) => p.x)) * 1.1, px, px + pw), ys = linear(0, ymax, py + ph, py);
  const out = [plotFrame({ x: px, y: py, w: pw, h: ph, xs, ys, xTicks: pts.map((p) => p.x), yTicks: niceTicks(0, ymax, 5), xFmt: (v) => fmt(v, 0), xLabel: 'offered commands per second (StartTransaction + StopTransaction, measured http_reqs rate)', yLabel: 'latency, ms' })];
  if (limit !== undefined) out.push(line(px, ys(limit), px + pw, ys(limit), 'ln s-red dash'), text(px + pw - 4, ys(limit) - 6, `p99 threshold ${limit} ms`, { anchor: 'end', cls: 'f-red' }));
  const series = [['start', 'blue', 'p99 StartTransaction'], ['stop', 'orange', 'p99 StopTransaction'], ['p50', 'teal', 'p50 all commands']];
  for (const [k, c] of series) {
    out.push(path(pts.map((p) => [xs(p.x), ys(p[k])]), `s-${c}`));
    for (const p of pts) { out.push(circle(xs(p.x), ys(p[k]), 4, `f-${c}`)); if (k !== 'p50') out.push(text(xs(p.x), ys(p[k]) + (k === 'start' ? -9 : 17), fmt(p[k]), { anchor: 'middle', cls: 'm' })); }
  }
  out.push(legend(px, 68, series.map(([, c, l]) => ({ label: l, cls: `f-${c}`, kind: 'line' }))));
  const dropped = pts.map((p) => p.run.metric('dropped_iterations')?.count ?? 0);
  return {
    file: 'sessions-rate-sweep.svg',
    svg: doc({
      w: W, h: H, title: 'sessions: latency versus offered rate', subtitle: `${runs.length} constant-arrival-rate runs; dropped iterations per run: ${dropped.join(', ')}. Each point is one run.`, body: out.join(''),
      footer: [`Sources: ${runs.map((r) => r.file).join(', ')}.`, `Setup: k6, ${D.envSummary(runs)}`],
    }),
  };
}

// 4. Gateway connections ----------------------------------------------------------------------------
export function gatewayConnections() {
  const g = D.tool('gateway-10k-connections.json');
  if (!g) return null;
  const k = D.k6List(/^k6-gateway-ws-/);
  const S = g.data.samples, W = 980, px = 90, pw = 840;
  const tmax = S.at(-1).t;
  const xs = linear(0, tmax, px, px + pw);
  const out = [];
  const panel = (y, h, key, color, title, unit, tgt, tl) => {
    const ymax = Math.max(...S.map((s) => s[key]), tgt ?? 0) * 1.08;
    const ys = linear(0, ymax, y + h, y);
    out.push(text(px, y - 8, title, { cls: 'h' }));
    out.push(plotFrame({ x: px, y, w: pw, h, xs, ys, xTicks: niceTicks(0, tmax, 6), yTicks: niceTicks(0, ymax, 4), xLabel: (tl ?? '').startsWith('ADR') ? 'seconds since the first sample' : undefined, yFmt: (v) => fmt(v, 0) }));
    if (tgt) out.push(line(px, ys(tgt), px + pw, ys(tgt), 'ln s-red dash'), text(px + pw - 4, ys(tgt) - 6, tl, { anchor: 'end', cls: 'f-red' }));
    out.push(path(S.map((s) => [xs(s.t), ys(s[key])]), `s-${color}`));
  };
  const Tg = D.targets.perInstance['station-gateway'];
  panel(90, 150, 'connections', 'blue', 'WebSocket connections held by one gateway process', '', Tg.connections, `target ${fmt(Tg.connections, 0)}`);
  panel(290, 110, 'rssMB', 'purple', 'Resident memory (MB)', 'MB');
  panel(450, 110, 'msgInPerS', 'teal', 'OCPP frames received per second (stub sessions, no database)', '', Tg.msgPerSec, `ADR 0000 msg/s target ${fmt(Tg.msgPerSec, 0)}`);
  // k6 runs
  let y = 640;
  const out2 = [text(px, y - 14, 'k6 runs against one gateway container (real sessions service): CALLRESULT latency', { cls: 'h' })];
  const bx = 480, bw = 300, xb = logScale(1, 10000, bx, bx + bw);
  out2.push(text(bx + bw / 2, y + 6, 'CALLRESULT p99 ms (log)', { anchor: 'middle', cls: 'm' }));
  k.forEach((r, i) => {
    const off = D.gatewayOfferedRate(r), yy = y + 20 + i * 38;
    const p99 = r.metric('ocpp_callresult_ms')['p(99)'], lim = r.threshold('ocpp_callresult_ms', /p\(99\)/)?.limit;
    out2.push(text(px - 70, yy + 20, `${fmt(r.metric('ws_sessions').count, 0)} stations connected, ${fmt(r.metric('ocpp_callresults').rate)} msg/s, ${r.metric('ocpp_callerrors').count} CALLERROR${off !== null && off < Tg.msgPerSec ? `, offered at most ${fmt(off, 0)} msg/s (rate target not tested)` : ''}`, { cls: 'b' }));
    out2.push(rect(bx, yy + 6, xb(p99) - bx, 20, p99 <= (lim ?? Infinity) ? 'f-green' : 'f-red', 'fill-opacity=".85"'), text(xb(p99) + 6, yy + 21, `${fmt(p99)} ms`, { cls: 'm' }));
    if (lim !== undefined) out2.push(line(xb(lim), yy, xb(lim), yy + 32, 'axis', 'style="stroke:var(--fg);stroke-width:2.5"'));
  });
  const hh = y + 20 + k.length * 38 + 20;
  const su = g.data.summary;
  return {
    file: 'gateway-connections.svg',
    svg: doc({
      w: W, h: hh, title: 'WebSocket connections per gateway instance', subtitle: `One gateway held ${fmt(su.peakConnections, 0)} connections at a peak RSS of ${su.peakRssMB} MB; the CALLRESULT p99 threshold (${fmt(Tg.callResultP99Ms, 0)} ms) was exceeded in ${k.filter((r) => r.metric('ocpp_callresult_ms')['p(99)'] > Tg.callResultP99Ms).length} of ${k.length} k6 runs.`, body: out.join('') + out2.join(''),
      footer: [`Sources: ${g.file}; ${k.map((r) => r.file).join(', ')}; ${D.TARGETS_FILE}.`, `Setup (10k run): ${g.data.setup.gateway}. Simulator: ${g.data.setup.simulator}. Machine: ${g.data.setup.machine}.`, `Setup (k6): ${D.envSummary(k)} Vertical tick = p99 threshold in the k6 script.`],
    }),
  };
}

// 5. Telemetry ingest -------------------------------------------------------------------------------
export function telemetryIngest() {
  const t = D.tool('telemetry-ingestbench.json');
  if (!t) return null;
  const d = t.data;
  const series = [['COPY into staging, then INSERT … ON CONFLICT', 'blue', d.copy_staging], ['row-by-row INSERT (baseline)', 'orange', d.row_by_row_baseline]];
  const all = series.flatMap(([, , r]) => r);
  const W = 980, H = 450, ph = 270, py = 112;
  const rmin = 50, rmax = 50000;
  const target = D.targets.perInstance['telemetry-ingest'];
  const out = [];
  // left panel: sustained vs offered
  const lx = 80, lw = 340;
  const xl = logScale(rmin, rmax, lx, lx + lw), yl = logScale(rmin, rmax, py + ph, py);
  out.push(text(lx, py - 22, 'Sustained rows per second versus offered rate', { cls: 'h' }));
  out.push(plotFrame({ x: lx, y: py, w: lw, h: ph, xs: xl, ys: yl, xTicks: logTicks(100, 10000).concat([rmax]), yTicks: logTicks(100, 10000).concat([rmax]), xFmt: (v) => si(v), yFmt: (v) => si(v), xLabel: 'offered rows per second', yLabel: 'sustained rows per second' }));
  out.push(line(xl(rmin), yl(rmin), xl(rmax), yl(rmax), 'ln s-muted dash'), text(xl(1500), yl(2300) - 4, 'keeping up (y = x)', { cls: 'm', rotate: -45 }));
  // right panel: durable lag p99
  const rx = 560, rw = 380, lagMax = Math.max(...all.map((r) => r.measured_durable_lag_p99_s), 5) * 1.1;
  const xr = logScale(rmin, rmax, rx, rx + rw), yr = linear(0, lagMax, py + ph, py);
  out.push(text(rx, py - 22, 'Durable lag p99 versus offered rate', { cls: 'h' }));
  out.push(plotFrame({ x: rx, y: py, w: rw, h: ph, xs: xr, ys: yr, xTicks: logTicks(100, 10000).concat([rmax]), yTicks: niceTicks(0, lagMax, 5), xFmt: (v) => si(v), xLabel: 'offered rows per second', yLabel: 'seconds' }));
  const lagT = target.durableP99Ms / 1000;
  out.push(line(rx, yr(lagT), rx + rw, yr(lagT), 'ln s-red dash'), text(rx + rw - 4, yr(lagT) - 6, `target ${lagT} s`, { anchor: 'end', cls: 'f-red' }));
  const tx = target.rowsPerSec;
  out.push(line(xl(tx), py, xl(tx), py + ph, 'ln s-green dash'), line(xr(tx), py, xr(tx), py + ph, 'ln s-green dash'));
  out.push(text(xl(tx) + 5, py + 14, `target ${fmt(tx, 0)} rows/s`, { cls: 'f-green' }));
  for (const [, c, rows] of series) {
    out.push(path(rows.map((r) => [xl(r.target_rows_per_s), yl(r.measured_sustained_rows_per_s)]), `s-${c}`));
    out.push(path(rows.map((r) => [xr(r.target_rows_per_s), yr(r.measured_durable_lag_p99_s)]), `s-${c}`));
    for (const r of rows) {
      const hollow = !r.keeping_up;
      const a = [xl(r.target_rows_per_s), yl(r.measured_sustained_rows_per_s)], b = [xr(r.target_rows_per_s), yr(r.measured_durable_lag_p99_s)];
      for (const [cx, cy] of [a, b]) out.push(hollow ? circle(cx, cy, 5.5, `s-${c}`, 'style="fill:none;stroke-width:2.5"') : circle(cx, cy, 4.5, `f-${c}`));
    }
  }
  out.push(legend(lx, 70, series.map(([l, c]) => ({ label: l, cls: `f-${c}`, kind: 'line' }))) + text(rx + rw, 70, 'hollow marker = did not keep up', { anchor: 'end', cls: 'm' }));
  const e = d.environment;
  return {
    file: 'telemetry-ingest.svg',
    svg: doc({
      w: W, h: H, title: 'telemetry-ingest: COPY versus row-by-row', subtitle: `Keeping up = ${d.summary.keeping_up_definition}.`, body: out.join(''),
      footer: [`Source: ${t.file}; target from ${D.TARGETS_FILE}.`, `Setup: ${d.note} Go ${e.go}, ${e.num_cpu} CPUs, ${e.partitions} partitions, Kafka ${e.brokers}, flush ${e.flush}; ${series.map(([l, , r]) => `${l}: ${r.length} offered rates`).join('; ')}.`],
    }),
  };
}

// 6. Storage ----------------------------------------------------------------------------------------
export function telemetryStorage() {
  const f = D.toolGlob(/^telemetry-backfill-/).at(-1);
  if (!f) return null;
  const d = f.data, s = Object.fromEntries(Object.entries(d.size).map(([k, v]) => [k, v.value])), c = d.chunks, q = d.queriesMs, x = d.fullFleetExtrapolation;
  const W = 980, out = [];
  const hbar = (px, py, pw, label, v, vmax, cls, txt) => {
    out.push(rect(px, py, (v / vmax) * pw, 16, cls, 'fill-opacity=".85"'), text(px + (v / vmax) * pw + 6, py + 13, txt, { cls: 'm' }));
    void label;
  };
  // (a) compression
  out.push(text(20, 84, 'Size before and after compression (measured)', { cls: 'h' }));
  const comp = [['all chunks, total', s.beforeCompressionTotalBytes, s.afterCompressionTotalBytes, s.overallRatioBeforeOverAfter], ['compressed raw chunks only', s.compressedChunksBeforeBytes, s.compressedChunksAfterBytes, s.compressedChunksRatioBeforeOverAfter], ['power_1m compressed chunks only', s.power1mCompressedChunksBeforeBytes, s.power1mCompressedChunksAfterBytes, s.power1mCompressedChunksRatioBeforeOverAfter]];
  const cmax = Math.max(...comp.map((r) => r[1]));
  comp.forEach(([l, b, a, r], i) => {
    const y = 104 + i * 62;
    out.push(text(20, y + 10, l, { cls: 'b' }));
    hbar(20, y + 16, 250, '', b, cmax, 'f-muted', `before ${bytesFmt(b)}`);
    hbar(20, y + 36, 250, '', a, cmax, 'f-blue', `after ${bytesFmt(a)}  (${fmt(r)}×)`);
  });
  // (b) chunk sizes
  const bx = 520;
  out.push(text(bx, 84, `Chunk size distribution, ${c.beforeCompression.chunkCount} chunks (${c.afterCompression.compressedChunkCount} compressed after)`, { cls: 'h' }));
  const keys = [['bytesMin', 'min'], ['bytesP50', 'p50'], ['bytesMean', 'mean'], ['bytesP95', 'p95'], ['bytesMax', 'max']];
  const cm = Math.max(c.beforeCompression.bytesMax, c.afterCompression.bytesMax), xc = logScale(1e5, cm * 1.2, bx + 40, bx + 330);
  keys.forEach(([k, l], i) => {
    const y = 100 + i * 36;
    out.push(text(bx, y + 22, l, { cls: 'm' }));
    out.push(rect(bx + 40, y + 2, xc(c.beforeCompression[k]) - bx - 40, 13, 'f-muted', 'fill-opacity=".85"'), text(xc(c.beforeCompression[k]) + 6, y + 13, bytesFmt(c.beforeCompression[k]), { cls: 'm' }));
    out.push(rect(bx + 40, y + 17, xc(c.afterCompression[k]) - bx - 40, 13, 'f-blue', 'fill-opacity=".85"'), text(xc(c.afterCompression[k]) + 6, y + 28, bytesFmt(c.afterCompression[k]), { cls: 'm' }));
  });
  out.push(legend(bx + 40, 292, [{ label: 'before compression', cls: 'f-muted' }, { label: 'after compression', cls: 'f-blue' }]));
  // (c) queries
  out.push(text(20, 350, 'Query latency, ms (measured, p50 / p95 / p99)', { cls: 'h' }));
  const qk = Object.entries(q), qmax = Math.max(...qk.map(([, v]) => v.p99)), xq = linear(0, qmax * 1.35, 330, 450);
  qk.forEach(([name, v], i) => {
    const y = 366 + i * 50;
    out.push(text(20, y + 24, name, { cls: 'b' }));
    [['p50', 'f-teal'], ['p95', 'f-blue'], ['p99', 'f-purple']].forEach(([p, cl], j) => {
      out.push(rect(330, y + 2 + j * 14, xq(v[p]) - 330, 11, cl, 'fill-opacity=".85"'), text(xq(v[p]) + 5, y + 12 + j * 14, `${p} ${fmt(v[p])}`, { cls: 'm' }));
    });
  });
  out.push(legend(20, 366 + qk.length * 50 + 6, [{ label: 'p50', cls: 'f-teal' }, { label: 'p95', cls: 'f-blue' }, { label: 'p99', cls: 'f-purple' }]));
  // (d) derived
  const dx = 600;
  out.push(text(dx, 350, 'Full fleet, 90 days raw + 2 years of rollups', { cls: 'h' }), text(dx, 368, 'DERIVED, not measured', { cls: 'bad' }));
  const dv = [['raw, 90 days', x.raw90dSizeGB], ['power_1m, 2 years', x.rollup2ySizeGB], ['total', x.totalSizeGB]], dmax = x.totalSizeGB.value;
  dv.forEach(([l, v], i) => {
    const y = 384 + i * 36, w = (v.value / dmax) * 150;
    out.push(text(dx, y + 15, l, { cls: 'b' }), rect(dx + 130, y, w, 20, 'f-purple', 'fill-opacity=".30" stroke-width="2" stroke-dasharray="5 3" style="stroke:var(--purple)"'), text(dx + 130 + w + 6, y + 15, `${v.value} ${v.unit}`, { cls: 'm' }));
  });
  const fl = [`${x.bytesPerRowUncompressed.value} B/row uncompressed = ${x.bytesPerRowUncompressed.formula}`, `${x.bytesPerRowCompressed.value} B/row compressed = ${x.bytesPerRowCompressed.formula}`, `raw = ${x.raw90dSizeGB.formula}`, `rollup = ${x.rollup2ySizeGB.formula}`];
  let yy = 508;
  for (const l of fl) for (const w of wrap(l, 48)) { out.push(text(dx, yy, w, { cls: 'm mono', size: 12 })); yy += 16; }
  const H = Math.max(yy + 10, 366 + qk.length * 50 + 24);
  const ld = d.load;
  return {
    file: 'telemetry-storage.svg',
    svg: doc({
      w: W, h: H, title: 'telemetry storage: TimescaleDB on a 7-day backfill', subtitle: `${fmt(ld.rowsInserted.value, 0)} rows in ${ld.wallSeconds.value} s (${fmt(ld.rowsPerSecond.value, 0)} rows/s, ${d.config.workers} workers); all bars are measured except the purple dashed ones.`, body: out.join(''),
      footer: [`Source: ${f.file}.`, `Setup: ${d.config.stations} stations x ${d.config.connectorsPerStation} connectors, ${d.config.days} days (${d.config.start} to ${d.config.end}), one reading every ${d.config.readingIntervalS} s, mean utilisation ${d.config.meanUtilisation}, seed ${d.config.seed}, ${d.config.queryRuns} runs per query. Derived assumptions: ${x.assumptions.value}.`, ...d.notes.slice(0, 1)],
    }),
  };
}

// Timeline machinery (drills) ----------------------------------------------------------------------------
const PHASE_CLS = { before: 'f-blue', during: 'f-red', after: 'f-green' };
function phaseRanges(rows) {
  const out = [];
  for (const r of rows) {
    const last = out.at(-1);
    if (last && last.phase === r.phase) last.end = r.s; else out.push({ phase: r.phase, start: r.s, end: r.s });
  }
  out.forEach((p, i) => { if (out[i + 1]) p.end = out[i + 1].start; });
  return out;
}
function shade(phases, xs, y, h) {
  return phases.map((p) => rect(xs(p.start), y, xs(p.end) - xs(p.start), h, PHASE_CLS[p.phase] ?? 'f-muted', 'fill-opacity=".13"')).join('');
}
function drillPanel({ rows, keys, x, y, w, h, xs, phases, title, unitFmt, refs = [], yMin = 0, ymaxFixed, xLabel }) {
  const vals = keys.flatMap((k) => rows.map((r) => r[k.key]).filter((v) => typeof v === 'number'));
  const top = Math.max(...vals, ...refs.map((r) => r.v));
  const ymax = ymaxFixed ?? (top > 0 ? top * 1.1 : 1);
  const ys = linear(yMin, ymax, y + h, y);
  const out = [text(x, y - 8, title, { cls: 'h' }), shade(phases, xs, y, h), plotFrame({ x, y, w, h, xs, ys, xTicks: niceTicks(rows[0].s, rows.at(-1).s, 8), yTicks: niceTicks(yMin, ymax, 3), yFmt: unitFmt, xLabel })];
  for (const r of refs) out.push(line(x, ys(r.v), x + w, ys(r.v), `ln s-${r.color ?? 'red'} dash`), text(x + w - 4, ys(r.v) - 6, r.label, { anchor: 'end', cls: `f-${r.color ?? 'red'}` }));
  keys.forEach((k) => {
    const pts = rows.filter((r) => typeof r[k.key] === 'number').map((r) => [xs(r.s), ys(r[k.key])]);
    out.push(path(pts, `s-${k.color}`));
  });
  if (keys.length > 1 || keys[0].label) out.push(legend(x + w - keys.reduce((a, k) => a + (k.label ?? k.key).length * 6.6 + 44, 0) + 18, y - 8, keys.map((k) => ({ label: k.label ?? k.key, cls: `f-${k.color}`, kind: 'line' }))));
  return out.join('');
}
function phaseLegend(x, y, phases) {
  const names = [...new Set(phases.map((p) => p.phase))];
  return legend(x, y, names.map((n) => ({ label: n, cls: PHASE_CLS[n] ?? 'f-muted', opacity: 0.45 })));
}
const unitFor = (key) => (key.endsWith('_w') ? (v) => si(v, 'W') : key.includes('utilisation') ? (v) => fmt(v, 2) : (v) => si(v));
const INCREASE = D.increaseChecks();
function checksBlock(x, y, result) {
  if (!result) return { svg: text(x, y, 'no result.json in this drill directory', { cls: 'm' }), h: 24 };
  const sym = { ge: '≥', le: '≤', gt: '>', lt: '<', eq: '=' };
  const out = [text(x, y, `Checks (result.json): drill ${result.pass ? 'PASSED' : 'FAILED'}`, { cls: result.pass ? 'ok' : 'bad', size: 14 })];
  result.checks.forEach((c, i) => out.push(text(x + 6, y + 22 + i * 20, `${c.pass ? 'PASS' : 'FAIL'}  ${c.check}: measured ${c.measured === null || c.measured === undefined ? 'no measurement' : fmt(c.measured)}${INCREASE.has(c.check) ? ' (PromQL increase, extrapolated)' : ''} ${sym[c.op] ?? c.op} ${fmt(c.threshold)}`, { cls: c.pass ? 'ok mono' : 'bad mono', size: 12 })));
  return { svg: out.join(''), h: 30 + result.checks.length * 20 };
}
const sampleStep = (rows) => median(rows.slice(1).map((r, i) => r.s - rows[i].s));
function drillFooter(dr) {
  const fs = ['timeline.jsonl', dr.result ? 'result.json' : null, dr.queries ? 'queries.json' : null, dr.hasEvents ? 'events.log' : null].filter(Boolean).map((f) => `${dr.rel}/${f}`);
  return [`Sources: ${fs.join(', ')}.`, `Setup: drill ${dr.result?.drill ?? dr.name}${dr.result?.startedAt ? ` started ${dr.result.startedAt}` : ''}; ${dr.rows.length} Prometheus samples about ${fmt(sampleStep(dr.rows), 2)} s apart on the compose stack (two gateway instances during drills, see docker-compose.yml for the other services; one Docker VM); the stop and start commands are in events.log.`];
}

// 7. Smart charging under the cap ---------------------------------------------------------------
export function smartChargingCap() {
  const dr = D.latestDrill('smart-charging-down');
  if (!dr) return null;
  const rows = dr.rows, phases = phaseRanges(rows);
  const W = 980, x = 90, w = 850, xs = linear(rows[0].s, rows.at(-1).s, x, x + w);
  const cap = rows.findLast((r) => typeof r.site_cap_w === 'number')?.site_cap_w ?? null;
  const out = [];
  out.push(drillPanel({ rows, keys: [{ key: 'site_drawn_w', color: 'blue', label: 'power drawn by all sites' }, { key: 'site_cap_w', color: 'red', label: 'sum of site caps' }], x, y: 100, w, h: 170, xs, phases, title: 'Power drawn versus the sum of the site caps', unitFmt: unitFor('_w') }));
  out.push(drillPanel({ rows, keys: [{ key: 'max_site_utilisation', color: 'purple', label: 'worst single site, drawn ÷ cap' }], x, y: 330, w, h: 120, xs, phases, title: 'Highest utilisation of any single site (must stay at or below 1.0)', unitFmt: unitFor('utilisation'), refs: [{ v: 1, label: 'site cap = 1.0' }], ymaxFixed: 1.15 }));
  out.push(drillPanel({ rows, keys: [{ key: 'rounds_optimal_per_s', color: 'green', label: 'optimal rounds/s' }, { key: 'rounds_fallback_per_s', color: 'orange', label: 'fallback-equal-share rounds/s' }], x, y: 510, w, h: 120, xs, phases, title: 'Allocation rounds per second, by strategy', unitFmt: unitFor('x'), xLabel: 'seconds since the sampler started' }));
  out.push(phaseLegend(x, 74, phases));
  const ck = checksBlock(20, 700, dr.result);
  out.push(ck.svg);
  const nums = (k) => rows.map((r) => r[k]).filter((v) => typeof v === 'number');
  const maxU = nums('max_site_utilisation').length ? Math.max(...nums('max_site_utilisation')) : null;
  return {
    file: 'smart-charging-cap.svg',
    svg: doc({
      w: W, h: 700 + ck.h + 10, title: 'smart-charging: allocation under the site cap, through an allocator outage', subtitle: `Peak single-site utilisation over the drill: ${fmt(maxU)}. Fleet in the sample: ${cap === null ? 'no cap sample' : `${fmt(cap / 1000, 0)} kW of total cap`}, up to ${nums('active_sessions').length ? Math.max(...nums('active_sessions')) : 'no measurement of'} active sessions.`, body: out.join(''),
      footer: drillFooter(dr),
    }),
  };
}

// 8. One timeline per drill ------------------------------------------------------------------------
export function drillCharts() {
  return D.drills().filter((dr) => dr.rows.length >= 2).flatMap((dr) => { try { return [(() => {
    const rows = dr.rows, phases = phaseRanges(rows);
    const numeric = [...new Set(rows.flatMap((r) => Object.keys(r)))].filter((k) => !['t', 's', 'phase'].includes(k) && rows.some((r) => typeof r[k] === 'number'));
    const groups = [];
    for (const k of numeric) {
      const mx = Math.max(...rows.map((r) => (typeof r[k] === 'number' ? Math.abs(r[k]) : 0)), 1e-9);
      const g = groups.find((gg) => gg.id === k.split('_')[0] && mx / gg.mx < 20 && gg.mx / mx < 20);
      if (g) g.keys.push(k); else groups.push({ id: k.split('_')[0], keys: [k], mx });
    }
    const W = 980, x = 90, w = 850, ph = 100, gap = 54, xs = linear(rows[0].s, rows.at(-1).s, x, x + w);
    const out = [phaseLegend(x, 74, phases)];
    groups.forEach((g, i) => {
      out.push(drillPanel({ rows, keys: g.keys.map((k, j) => ({ key: k, color: SERIES[j % SERIES.length] })), x, y: 100 + i * (ph + gap), w, h: ph, xs, phases, title: g.keys.join(' and '), unitFmt: unitFor(g.keys[0]), refs: g.keys.some((k) => k.includes('utilisation')) ? [{ v: 1, label: '1.0' }] : [], xLabel: i === groups.length - 1 ? 'seconds since the sampler started' : undefined }));
    });
    const y0 = 100 + groups.length * (ph + gap) + 10;
    const ck = checksBlock(20, y0, dr.result);
    out.push(ck.svg);
    const name = dr.result?.drill ?? dr.name;
    return {
      file: `drill-${dr.name}.svg`,
      svg: doc({ w: W, h: y0 + ck.h + 8, title: `Drill ${name}: timeline`, subtitle: `Phases: ${phases.map((p) => `${p.phase} from ${fmt(p.start, 0)} s`).join(', ')}.`, body: out.join(''), footer: drillFooter(dr) }),
    };
    })()]; } catch (e) { D.warn(`drill chart for ${dr.name} was not generated: ${e.message}`); return []; }
  });
}

export const ALL = () => {
  const out = [];
  for (const [name, fn] of [['targets-vs-measured', targetVsMeasured], ['throughput-p99', throughputAndP99], ['sessions-rate-sweep', sessionsSweep], ['gateway-connections', gatewayConnections], ['telemetry-ingest', telemetryIngest], ['telemetry-storage', telemetryStorage], ['smart-charging-cap', smartChargingCap], ['drill charts', drillCharts]]) {
    try { const r = fn(); out.push(...(Array.isArray(r) ? r : [r])); } catch (e) { D.warn(`chart ${name} was not generated: ${e.message}`); }
  }
  return out.filter(Boolean);
};
