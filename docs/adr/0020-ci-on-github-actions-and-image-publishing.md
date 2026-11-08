# 0020. CI on GitHub Actions and image publishing

- Status: accepted
- Date: 2026-11-08
- Supersedes in part: ADR 0001 (checks run on the developer machine only) and ADR 0009 ("Checks run locally only ... There is no CI").
- Driven by ADR 0000: load and drill numbers belong to fixed hardware, so only checks that do not depend on hardware move to CI.

## Context

The repository becomes public under the MIT license. Until now `make check` was the only gate and ran on one
laptop. A public repo needs the same gate on every pull request, a scan for leaked secrets across the whole history,
and runnable images for readers who do not want to build seven services.

## Decision

Three workflows in `.github/workflows/`, all with `permissions: contents: read` at the top, a `concurrency` group that
cancels superseded runs, and a job timeout.

- **`ci.yml`, job `test`.** Runs `make check` after installing the toolchain pinned in ADR 0009: Node 26.13, pnpm from
  `packageManager` in `package.json`, Go 1.27.3, golangci-lint v2.15.0. `make check` stays the single definition of
  "green", so local and CI cannot drift. It covers install, contracts generation and drift check, topic sync, promtool
  rule tests, oxlint, build, typecheck, unit tests, the real infrastructure through docker compose (Postgres OLTP,
  replica and Timescale, Redis, 3-broker Kafka), integration tests, `go vet`, golangci-lint and `go test -race` for
  every `go.work` module. The job name `test` is the required status check of the main branch ruleset.
- **`ci.yml`, job `secrets`.** gitleaks v8.30.1 (pinned by image digest) over the full history of every branch.
  `.gitleaks.toml` extends the default rules and allowlists only the `generic-api-key` rule on test and fixture paths:
  `*_test.go`, `*.test.ts`, the driver-api integration harness, and the SHA-256 hashes of fake station secrets in
  `fleet-fixture.json`. A hit anywhere else, or from any other rule, fails the build.
- **`images.yml`.** Every Dockerfile (seven services and the simulator) is built in a matrix. On pull requests the
  images are built only: no login, no `packages` permission, nothing pushed. On push to main the `publish` job logs in
  with `GITHUB_TOKEN` (the only job with `packages: write`) and pushes
  `ghcr.io/sotream/evcharge-lab-<name>:<short sha>` and `:main`. Names are lowercase; `<name>` is `station-gateway`,
  `sessions`, `smart-charging`, `tariff`, `billing`, `telemetry-ingest`, `driver-api` or `simulator`. Build cache is the
  GitHub Actions cache, one scope per image.

### What stays local

Failure drills (`drills/`), k6 load tests (`load/`) and chart generation are not run in CI. Each needs minutes to hours,
and their numbers are only comparable on the fixed laptop (ADR 0000). A shared runner would produce different numbers
and would put them next to the measured ones in the README. They remain manual and their raw output stays under
`docs/drill-output/` and `docs/load-output/`.

### Action pinning

Every third-party action is pinned to a full commit SHA with the release tag in a comment (`# v7.0.1`). Tags can be
moved; a commit cannot. The gitleaks image is pinned by digest for the same reason. Dependabot updates both the SHA and
the comment.

### Dependabot

`.github/dependabot.yml` checks weekly: npm at the workspace root, `gomod` for every `go.work` module, `docker` for every
Dockerfile directory, `docker-compose` for the infrastructure images, and `github-actions`. Minor and patch updates are
grouped per ecosystem. Majors are separate PRs and stay manual; Node, Postgres, Redis, Kafka and Timescale majors are
ignored by the bot because each is a deliberate move and a re-measurement. `dependabot-auto-merge.yml` turns on GitHub
auto-merge for minor and patch PRs only (not GitHub Actions bumps, which the workflow token cannot merge), so a PR merges
only after the required checks pass.

## Rejected alternatives

- **Re-implementing `make check` step by step in the workflow.** It would give per-step annotations but a second
  definition of the gate that drifts from the Makefile.
- **Running drills or k6 on hosted runners.** The numbers would not be comparable with the measured ones.
- **Floating action tags (`@v4`).** Easier to read, but a moved tag changes what runs with write access to the repo.

## Consequences

- `test` needs a runner able to run about 3 GB of containers (3 Kafka brokers with 512 MB heap, three Postgres, Redis)
  next to the toolchain, and takes several minutes. A stricter machine size or a slimmer infra set is the lever if it gets slow.
- Published images are built from the same Dockerfiles and the same context as `docker compose build`, so compose still
  builds locally by default; GHCR is an alternative source, not a replacement.

## Not done

- No deployment. Nothing runs these images anywhere except the reader's machine.
- No image signing and no SBOM or provenance attestations yet. Add them with cosign and `build-push-action`
  attestations when someone consumes the images outside this repo.
