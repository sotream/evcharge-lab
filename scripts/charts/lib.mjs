// Minimal SVG toolkit: theme, scales, ticks, text wrapping and a document wrapper. No third-party code.

export const FONT = 'system-ui, -apple-system, "Segoe UI", Helvetica, Arial, sans-serif';
export const MONO = 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';

// Colors live in CSS variables so that one file works in GitHub light and dark. The background stays transparent.
const THEME_CSS = `
svg{--fg:#1f2328;--muted:#59636e;--grid:#d1d9e0;--axis:#818b98;--blue:#0969da;--orange:#bc4c00;--green:#1a7f37;--red:#cf222e;--purple:#8250df;--teal:#0e7c86;--card:rgba(129,139,152,.10);--on:#fff}
@media (prefers-color-scheme: dark){svg{--fg:#e6edf3;--muted:#9198a1;--grid:#3d444d;--axis:#6e7681;--blue:#58a6ff;--orange:#f0883e;--green:#3fb950;--red:#ff7b72;--purple:#d2a8ff;--teal:#39c5cf;--card:rgba(110,118,129,.16);--on:#0d1117}}
text{fill:var(--fg);font-family:${FONT};font-size:12px}
.t{font-size:18px;font-weight:700}.st{fill:var(--muted);font-size:13px}.h{font-size:14px;font-weight:700}
.m{fill:var(--muted)}.b{font-weight:700}.mono{font-family:${MONO}}
.grid{stroke:var(--grid);stroke-width:1}.axis{stroke:var(--axis);stroke-width:1}
.card{fill:var(--card)}
.f-blue{fill:var(--blue)}.f-orange{fill:var(--orange)}.f-green{fill:var(--green)}.f-red{fill:var(--red)}.f-purple{fill:var(--purple)}.f-teal{fill:var(--teal)}.f-muted{fill:var(--muted)}
.s-blue{stroke:var(--blue)}.s-orange{stroke:var(--orange)}.s-green{stroke:var(--green)}.s-red{stroke:var(--red)}.s-purple{stroke:var(--purple)}.s-teal{stroke:var(--teal)}.s-muted{stroke:var(--muted)}
.ln{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}
.dash{stroke-dasharray:6 4}
.on{fill:var(--on)}
.ok{fill:var(--green);font-weight:700}.bad{fill:var(--red);font-weight:700}
`;

export const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const r1 = (n) => Math.round(n * 10) / 10;

export function fmt(n, maxDec = 2) {
  if (n === null || n === undefined || Number.isNaN(n)) return 'n/a';
  const a = Math.abs(n);
  if (a > 0 && a < 0.01) return Number(n.toPrecision(2)).toString();
  const dec = a >= 1e5 ? 0 : Math.max(maxDec, 2);
  return Number(n.toFixed(dec)).toLocaleString('en-US', { maximumFractionDigits: dec });
}
export function si(n, unit = '') {
  const a = Math.abs(n);
  if (a >= 1e9) return `${fmt(n / 1e9, 2)} G${unit}`;
  if (a >= 1e6) return `${fmt(n / 1e6, 2)} M${unit}`;
  if (a >= 1e3) return `${fmt(n / 1e3, 2)} k${unit}`;
  return `${fmt(n, 2)}${unit ? ' ' + unit : ''}`;
}

export function bytesFmt(b) {
  const a = Math.abs(b);
  if (a >= 1e9) return `${fmt(b / 1e9, 2)} GB`;
  if (a >= 1e6) return `${fmt(b / 1e6, 1)} MB`;
  if (a >= 1e3) return `${fmt(b / 1e3, 1)} kB`;
  return `${b} B`;
}

export function linear(d0, d1, r0, r1_) {
  const k = d1 === d0 ? 0 : (r1_ - r0) / (d1 - d0);
  return (v) => r0 + (v - d0) * k;
}
export function logScale(d0, d1, r0, r1_) {
  const l0 = Math.log10(d0), l1 = Math.log10(d1);
  return (v) => r0 + (Math.log10(Math.max(v, d0)) - l0) * ((r1_ - r0) / (l1 - l0));
}

export function niceTicks(min, max, count = 5) {
  if (max === min) return [min];
  const raw = (max - min) / count;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= raw) ?? 10 * mag;
  const out = [];
  for (let v = Math.ceil(min / step) * step; v <= max + step * 1e-9; v += step) out.push(Number(v.toPrecision(12)));
  return out;
}
export function logTicks(min, max) {
  const out = [];
  for (let e = Math.floor(Math.log10(min)); e <= Math.ceil(Math.log10(max)); e++) out.push(10 ** e);
  return out;
}

// Elements --------------------------------------------------------------------------------------
export function text(x, y, s, o = {}) {
  const { cls = '', anchor = 'start', size, rotate, weight } = o;
  const attrs = [`x="${r1(x)}"`, `y="${r1(y)}"`];
  if (anchor !== 'start') attrs.push(`text-anchor="${anchor}"`);
  if (size) attrs.push(`font-size="${size}"`);
  if (weight) attrs.push(`font-weight="${weight}"`);
  if (rotate) attrs.push(`transform="rotate(${rotate} ${r1(x)} ${r1(y)})"`);
  return `<text ${attrs.join(' ')}${cls ? ` class="${cls}"` : ''}>${esc(s)}</text>`;
}
export const line = (x1, y1, x2, y2, cls = 'grid', extra = '') => `<line x1="${r1(x1)}" y1="${r1(y1)}" x2="${r1(x2)}" y2="${r1(y2)}" class="${cls}" ${extra}/>`;
export const rect = (x, y, w, h, cls = '', extra = '') => `<rect x="${r1(x)}" y="${r1(y)}" width="${r1(Math.max(w, 0))}" height="${r1(Math.max(h, 0))}" class="${cls}" ${extra}/>`;
export const path = (pts, cls) => `<path d="${pts.map(([x, y], i) => `${i ? 'L' : 'M'}${r1(x)} ${r1(y)}`).join('')}" class="ln ${cls}"/>`;
export const circle = (x, y, r, cls, extra = '') => `<circle cx="${r1(x)}" cy="${r1(y)}" r="${r}" class="${cls}" ${extra}/>`;

export function wrap(s, maxChars) {
  const words = String(s).split(/\s+/);
  const lines = [];
  let cur = '';
  for (const w of words) {
    if (cur && (cur + ' ' + w).length > maxChars) { lines.push(cur); cur = w; } else cur = cur ? cur + ' ' + w : w;
  }
  if (cur) lines.push(cur);
  return lines;
}

// Legend: items = [{label, cls (e.g. 'f-blue'), kind: 'box'|'line'|'dash'}]
export function legend(x, y, items) {
  let cx = x;
  const out = [];
  for (const it of items) {
    if (it.kind === 'line' || it.kind === 'dash') out.push(line(cx, y - 4, cx + 18, y - 4, `ln ${it.cls.replace('f-', 's-')}${it.kind === 'dash' ? ' dash' : ''}`));
    else out.push(rect(cx, y - 11, 14, 12, it.cls, it.opacity ? `fill-opacity="${it.opacity}"` : ''));
    const w = it.kind === 'box' || !it.kind ? 14 : 18;
    out.push(text(cx + w + 6, y, it.label));
    cx += w + 6 + it.label.length * 6.6 + 18;
  }
  return out.join('');
}

// Document ---------------------------------------------------------------------------------------
// body: SVG string drawn in a w x h area; footer: array of strings, wrapped to the width.
export function doc({ w, h, title, subtitle, body, footer = [], desc }) {
  const maxChars = Math.floor((w - 40) / 7.4);
  const fl = [];
  for (const f of footer) fl.push(...wrap(f, maxChars));
  const fh = fl.length ? fl.length * 17 + 22 : 0;
  const H = h + fh;
  const foot = fl.length
    ? line(20, h + 6, w - 20, h + 6, 'grid') + fl.map((l, i) => text(20, h + 28 + i * 17, l, { cls: 'm mono', size: 12 })).join('')
    : '';
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${w} ${H}" width="${w}" height="${H}" role="img" aria-labelledby="t d">
<title id="t">${esc(title)}</title><desc id="d">${esc(desc ?? subtitle ?? title)}</desc>
<style>${THEME_CSS}</style>
${text(20, 28, title, { cls: 't' })}${subtitle ? text(20, 48, subtitle, { cls: 'st' }) : ''}
${body}
${foot}
</svg>
`;
}

// A framed plot area with axes. Returns helpers and the SVG for grid + axis labels.
export function plotFrame({ x, y, w, h, xs, ys, xTicks, yTicks, xFmt = fmt, yFmt = fmt, xLabel, yLabel, grid = true }) {
  const out = [];
  for (const t of yTicks) {
    const py = ys(t);
    if (grid) out.push(line(x, py, x + w, py));
    out.push(text(x - 8, py + 4, yFmt(t), { anchor: 'end', cls: 'm' }));
  }
  for (const t of xTicks) {
    const px = xs(t);
    out.push(line(px, y + h, px, y + h + 4, 'axis'));
    out.push(text(px, y + h + 19, xFmt(t), { anchor: 'middle', cls: 'm' }));
  }
  out.push(line(x, y + h, x + w, y + h, 'axis'), line(x, y, x, y + h, 'axis'));
  if (xLabel) out.push(text(x + w / 2, y + h + 40, xLabel, { anchor: 'middle', cls: 'm' }));
  if (yLabel) out.push(text(x - 52, y + h / 2, yLabel, { anchor: 'middle', cls: 'm', rotate: -90 }));
  return out.join('');
}
