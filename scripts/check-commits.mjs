// Checks that commit subjects (and the PR title) follow Conventional Commits: `type(scope)!: description`.
// Pull requests are squash-merged, so the PR title becomes the subject on main; the commits are checked too, because
// a rebase merge or a single-commit squash keeps them. Merge commits are skipped.
//
//   node scripts/check-commits.mjs <base> <head>     # every non-merge commit in base..head
//   PR_TITLE="feat: x" node scripts/check-commits.mjs origin/main HEAD
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const TYPES = ['feat', 'fix', 'docs', 'style', 'refactor', 'perf', 'test', 'build', 'ci', 'chore', 'revert'];
const PATTERN = new RegExp(`^(${TYPES.join('|')})(\\([a-z0-9][a-z0-9,._/-]*\\))?!?: \\S.*$`);

export function isConventional(subject) {
  return PATTERN.test(subject);
}

function main([base, head]) {
  if (!base || !head) {
    console.error('usage: node scripts/check-commits.mjs <base> <head>');
    return 2;
  }
  const subjects = execFileSync('git', ['log', '--no-merges', '--format=%h %s', `${base}..${head}`], { encoding: 'utf8' })
    .split('\n')
    .filter(Boolean)
    .map((line) => ({ what: `commit ${line.slice(0, line.indexOf(' '))}`, subject: line.slice(line.indexOf(' ') + 1) }));
  if (process.env.PR_TITLE !== undefined) subjects.push({ what: 'PR title', subject: process.env.PR_TITLE });
  const bad = subjects.filter((s) => !isConventional(s.subject));
  for (const b of bad) console.error(`✗ ${b.what}: "${b.subject}"`);
  if (bad.length > 0) {
    console.error(`\nExpected Conventional Commits: <type>(<scope>)!: <description>, type one of ${TYPES.join(', ')}. See CLAUDE.md.`);
    return 1;
  }
  console.log(`✔ ${subjects.length} subject(s) follow Conventional Commits`);
  return 0;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) process.exitCode = main(process.argv.slice(2));
