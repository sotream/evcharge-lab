import assert from 'node:assert/strict';
import { test } from 'node:test';
import { isConventional } from './check-commits.mjs';

test('accepts Conventional Commits subjects', () => {
  for (const s of [
    'feat: add credit notes',
    'fix(billing): dunning only for card declines',
    'feat(sessions,gateway): close the session lifecycle',
    'build(deps): bump zod from 4.6.7 to 4.7.0',
    'ci(deps): bump actions/checkout',
    'refactor!: drop the v0 API',
    'feat(api)!: remove /v0',
    'revert: feat(billing): credit notes',
    'docs: slim the README (#31)',
  ]) assert.equal(isConventional(s), true, s);
});

test('rejects everything else', () => {
  for (const s of [
    'Low items 2: breaker trial abandon',
    'Bump zod from 4.6.7 to 4.7.0',
    'feature: add credit notes', // unknown type
    'Feat: add credit notes', // type is lower case
    'feat:add credit notes', // space after the colon
    'feat: ', // empty description
    'feat(): empty scope',
    'feat(Billing): scope is lower case',
    'fixup! feat: add credit notes',
    'Revert "feat: add credit notes"',
    '',
  ]) assert.equal(isConventional(s), false, s);
});
