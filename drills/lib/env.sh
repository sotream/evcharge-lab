#!/usr/bin/env bash
# Prints the machine and Docker VM description as JSON, so every measurement records where it was taken.
# Usage: drills/lib/env.sh [extra-key extra-value]...
node -e '
const { execSync } = require("child_process");
const sh = (c) => { try { return execSync(c, { encoding: "utf8" }).trim(); } catch { return null; } };
const env = {
  measuredAt: new Date().toISOString(),
  machine: { model: sh("sysctl -n hw.model"), chip: sh("sysctl -n machdep.cpu.brand_string"), cpus: Number(sh("sysctl -n hw.ncpu")), memBytes: Number(sh("sysctl -n hw.memsize")), os: sh("sw_vers -productVersion"), arch: process.arch },
  dockerVm: { cpus: Number(sh("docker info --format {{.NCPU}}")), memBytes: Number(sh("docker info --format {{.MemTotal}}")), server: sh("docker info --format {{.ServerVersion}}") },
  runningContainers: (sh("docker compose ps --format {{.Service}}") || "").split("\n").filter(Boolean).sort(),
};
for (let i = 1; i + 1 < process.argv.length; i += 2) env[process.argv[i]] = process.argv[i + 1];
console.log(JSON.stringify(env, null, 2));
' "$@"
