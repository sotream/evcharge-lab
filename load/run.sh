#!/usr/bin/env bash
# Runs one k6 script inside the compose network (no macOS port forwarding in the path) and records the setup
# next to the k6 summary: docs/load-output/k6-<name>.json (k6) and docs/load-output/k6-<name>.env.json (setup).
#   load/run.sh <script-name> [k6 -e args...]   e.g. load/run.sh tariff -e URL=http://tariff:8080
set -euo pipefail
cd "$(dirname "$0")/.."
NAME="$1"; shift
OUT=docs/load-output
K6_RC=0 # k6 exits 99 when a threshold is crossed; the setup record below is still written, then this script exits with k6's code
mkdir -p "$OUT"
docker run --rm --network evcharge_default -v "$PWD":/repo -w /repo/load/k6 "$@" grafana/k6:2.3.0 run -q "$NAME.js" | tee "$OUT/k6-$NAME.stdout.txt" || K6_RC=$?
DOCKER_CPUS=$(docker info --format "{{.NCPU}}") DOCKER_MEM=$(docker info --format "{{.MemTotal}}") RUNNING=$(docker compose ps --format "{{.Service}}" | tr "\n" ",") node -e '
  const [name, out] = process.argv.slice(1);
  const env = { script: `load/k6/${name}.js`, measuredAt: new Date().toISOString(), generator: "grafana/k6 container on the compose network",
    host: { os: process.platform, arch: process.arch, chip: require("child_process").execSync("sysctl -n machdep.cpu.brand_string").toString().trim(), model: require("child_process").execSync("sysctl -n hw.model").toString().trim(), cpus: Number(require("child_process").execSync("sysctl -n hw.ncpu")), memBytes: Number(require("child_process").execSync("sysctl -n hw.memsize")) }, dockerCpus: Number(process.env.DOCKER_CPUS), dockerMemBytes: Number(process.env.DOCKER_MEM),
    runningContainers: process.env.RUNNING.split(",").filter(Boolean), note: "single instance of each service; all containers share one Docker VM" };
  require("fs").writeFileSync(`${out}/k6-${name}.env.json`, JSON.stringify(env, null, 2) + "\n");
' "$NAME" "$OUT"
exit "$K6_RC"
