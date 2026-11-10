# evcharge-lab

## Commits and pull requests

Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) for every commit subject and every PR title:

```
<type>(<scope>)!: <description>
```

- `type` is one of `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.
- `scope` is optional, lower case, and names the area: a service (`billing`, `sessions`, `gateway`, `tariff`,
  `telemetry`, `driver-api`), `contracts`, `platform`, `infra`, `simulator`, `deps`. Several scopes are comma-separated:
  `feat(sessions,gateway): ...`.
- `!` before the colon marks a breaking change; explain it in a `BREAKING CHANGE:` footer.
- The description is imperative and lower case, with no trailing period: `fix(billing): retry declined captures`.
- One commit per logical change. Do not commit fixups as separate commits on a PR; fold them into the commit they fix.

PRs are squash-merged, so the PR title becomes the subject on `main`. The `commits` workflow
(`.github/workflows/commits.yml`) checks the PR title and every commit of the PR with `scripts/check-commits.mjs`.
Run it locally before pushing:

```sh
node scripts/check-commits.mjs origin/main HEAD
```

## Checks

`make check` is the single definition of green (see the Makefile header and [ADR 0020](docs/adr/0020-ci-on-github-actions.md)).
