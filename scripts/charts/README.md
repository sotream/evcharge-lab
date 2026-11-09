# scripts/charts

Plain Node ESM (Node 26, no dependencies). It writes every chart as an SVG string and generates `docs/metrics.md`, `docs/drills.md` and the generated blocks of `README.md`, `docs/results.md` and `docs/findings.md` from the raw measurement files and the repository itself. No number is typed into a chart: each value is read from a file in `docs/load-output/`, `docs/drill-output/` or `load/targets.json`.

```sh
node scripts/charts/build.mjs
```

The command rewrites `docs/img/*.svg` (except the hand-made `architecture.svg`), deletes generated charts whose input has disappeared, writes two documents and replaces the text between these markers (the file is named in each item):

- `docs/metrics.md`: the full metrics table (metric, target, value, label, source file, formula) with the notes on individual rows, and the telemetry storage chart.
- `docs/drills.md`: a summary table (one row per drill: what it breaks, runs passed of total, latest run verdict, link to its timeline) and every drill run, including failed and inconclusive ones, with its result, check failures, links to `result.json` and `NOTE.md`, and its timeline chart.
- `README.md`, `<!-- stats:start -->` to `<!-- stats:end -->`: the stat tiles (services, ADRs, drill scripts, drill runs passed, findings fixed), counted from `services/`, `docs/adr/`, `drills/`, `docs/drill-output/` and the findings in `docs/findings.md`.
- `README.md`, `<!-- glance:start ... -->` to `<!-- glance:end -->`: "Targets at a glance", one row per target in `load/targets.json` with the measured value, the verdict (met, missed, not tested) and the source file.
- `docs/results.md`, `<!-- charts:start ... -->` to `<!-- charts:end -->`: the headline charts with captions.
- `docs/findings.md`, `<!-- findings:start ... -->` to `<!-- findings:end -->`: the findings table (number, title, found by, link). It is built from the `<details id="finding-N">` items in `docs/findings.md`; "found by" is classified from each item's "Found by" line as review, k6, drill or manual check.

The tests (`node --test scripts/charts/`, also run by `make check`) check the counts, the targets table, the findings table and that none of these outputs is stale.

To read drills from another checkout, set `DRILL_OUTPUT_DIR=/path/to/docs/drill-output`. The source footers still name `docs/drill-output/<run>/`.

## Files

| File | Purpose |
|---|---|
| `build.mjs` | entry point: writes the SVGs, the two documents and the generated blocks |
| `charts.mjs` | one function per chart; `ALL()` lists them |
| `data.mjs` | readers for k6 summaries, `.env.json` records, tool outputs and drill directories, plus the target-versus-measured metric model |
| `metrics-table.mjs` | the full metrics table (`docs/metrics.md`) |
| `readme-blocks.mjs` | stat tiles, targets at a glance, drills table, findings table and `docs/drills.md` |
| `readme-blocks.test.mjs` | tests for the above (`node:test`, no docker) |
| `lib.mjs` | SVG helpers: theme, scales, ticks, text wrapping, document wrapper |

## Charts and their sources

| SVG | Raw files |
|---|---|
| `targets-vs-measured.svg` | `load/targets.json` plus the measured values listed in the chart footer (k6 summaries, `gateway-10k-connections.json`, `telemetry-ingestbench.json`, `smart-charging-allocbench.json`, `tariff-quotebench-*.json`) |
| `throughput-p99.svg` | `k6-gateway-ws-*`, `k6-sessions-ocpp-rate*`, `k6-driver-api-reads`, `k6-smart-charging`, `k6-tariff` and their `.env.json` records |
| `sessions-rate-sweep.svg` | `k6-sessions-ocpp-rate*.json` |
| `gateway-connections.svg` | `gateway-10k-connections.json`, `k6-gateway-ws-*.json` |
| `telemetry-ingest.svg` | `telemetry-ingestbench.json` |
| `telemetry-storage.svg` | newest `telemetry-backfill-*.json` |
| `smart-charging-cap.svg` | newest `docs/drill-output/smart-charging-down-*/` (timeline, result, queries) |
| `drill-<run>.svg` | one per directory in `docs/drill-output/` that contains `timeline.jsonl`; found by globbing, never by name |
| `architecture.svg` | hand-made, based on ADR 0001 and `docker-compose.yml`; not generated |

## Rules the charts follow

- **Pass or miss.** Every verdict is computed from the numbers: the measured value is compared with the target in `load/targets.json` and no tolerance is applied. A value below a `>=` target is a miss, even by 0.01 (the README notes explain such near misses, for example a rate that is below the target only because k6 divides by a run that lasted 60.003 s). The k6 scripts have their own thresholds (for example a 1% dropped-iteration tolerance); those are shown as script thresholds, never as a pass against ADR 0000. A value that is null or absent in a raw file is drawn as "no measurement" and gets no verdict. The target-versus-measured chart divides each measured value by its target; green is a pass, red is a miss, grey is no data. **Which run is read:** the gateway rate and its CALLRESULT p99 both come from the k6 run with the highest achieved rate, whether or not that run kept its p99 within the target. Latency metrics for the other services are read at the target rate when the file has such a run. A gateway k6 run whose offered load (stations times Hz, derived) is below the rate target is labelled "not tested".
- **Labels.** Values copied from a raw file are `measured`. Anything calculated is `derived` and shows its formula; the storage extrapolation is drawn with dashed outlines and the word DERIVED. Drill check values that come from a PromQL `increase()` (found by reading `drills/*.sh`) are labelled "measured (PromQL increase, extrapolated)" because `increase()` extrapolates to the window edges and returns a float.
- **Footers.** Every chart names its source files and the setup record (the `.env.json` of a k6 run, or the `setup`, `environment` or `config` block of a tool output, or the drill start time and sampling interval).
- **Themes.** All colors are CSS variables in a `<style>` block with `@media (prefers-color-scheme: dark)`. Backgrounds are transparent, and the architecture infographic carries its own fixed palette so that it looks the same in both themes. Text is 12 px or larger.
- **Drill timelines.** Series are every numeric key of `timeline.jsonl`. Keys that share a first word and a similar magnitude are drawn in one panel. The before, during and after phases are shaded from the `phase` field. The checks from `result.json` are listed under the plot; a missing measurement is shown as "no measurement". A drill directory without a usable `timeline.jsonl` (fewer than two samples) is skipped.
- **Rounding.** Chart labels and the table show at most two decimals; the raw files keep full precision.
- **Malformed input.** The generator globs every drill directory and every result file. A malformed `result.json`, a `result.json` without a `checks` array, a damaged k6 summary, or an unreadable line in a timeline is skipped with a `WARNING:` line on stderr and a summary at the end of the build output. A chart that throws is skipped the same way. Nothing is skipped silently. Hand-edits to raw files are not needed or wanted; fix the harness instead.
