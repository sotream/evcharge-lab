// Fails if a docs/<lang>/ translation has no English source or was made from an older version of it.
// Each translation starts with `<!-- i18n-source: <hash> -->`; the hash is of the English file without its
// generated blocks (`<!-- name:start ... name:end -->`), which change with every measurement run.
import { createHash } from 'node:crypto';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { join, relative } from 'node:path';

const docs = new URL('../docs/', import.meta.url).pathname;
const hash = (file) =>
  createHash('sha256')
    .update(readFileSync(file, 'utf8').replace(/<!-- ([\w-]+):start[\s\S]*?<!-- \1:end -->/g, ''))
    .digest('hex')
    .slice(0, 12);
const walk = (d) =>
  readdirSync(d, { withFileTypes: true }).flatMap((e) => (e.isDirectory() ? walk(join(d, e.name)) : [join(d, e.name)]));

const problems = [];
let n = 0;
for (const lang of ['de']) {
  const root = join(docs, lang);
  if (!existsSync(root)) continue;
  for (const file of walk(root).filter((f) => f.endsWith('.md'))) {
    n++;
    const rel = relative(root, file);
    const src = join(docs, rel);
    const have = readFileSync(file, 'utf8').match(/^<!-- i18n-source: (\w+) -->/)?.[1];
    if (!existsSync(src)) problems.push(`${lang}/${rel}: no English source docs/${rel}`);
    else if (have !== hash(src)) problems.push(`${lang}/${rel}: stale or missing marker (has ${have}); retranslate, then set <!-- i18n-source: ${hash(src)} -->`);
  }
}
if (problems.length) {
  console.error(problems.join('\n'));
  process.exit(1);
}
console.log(`translations in sync (${n})`);
