#!/usr/bin/env bash
# Rebuilds the whole compose stack from empty volumes and waits until the simulated fleet is connected and charging.
# Drills that destroy a data store (postgres-failover, backup-restore) end with this, so the next drill starts clean.
# Env: SETTLE_S (extra wait after the fleet is up, default 120), WAIT_S (fleet timeout, default 600).
set -euo pipefail
cd "$(dirname "$0")/../.."
echo "reset-stack: docker compose down -v, then up -d" >&2
docker compose down -v --remove-orphans >&2
# `down -v` skips volumes that lack the compose labels ("not created by Docker Compose"), which then survive with their old
# data: a promoted replica came back as a primary and failed its standby healthcheck. Remove the project's volumes by name.
docker volume ls -q --filter name='^evcharge_' | xargs -r docker volume rm >&2
docker compose up -d >&2
deadline=$((SECONDS + ${WAIT_S:-600}))
until curl -s --max-time 3 127.0.0.1:28089/status | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{const j=JSON.parse(s);process.exit(j.started>0&&j.connected===j.started&&j.activeSessions>0?0:1)}catch{process.exit(1)}})'; do
  if (( SECONDS > deadline )); then echo "reset-stack: fleet not connected after ${WAIT_S:-600} s" >&2; exit 1; fi
  sleep 3
done
echo "reset-stack: fleet connected, settling ${SETTLE_S:-120} s" >&2
sleep "${SETTLE_S:-120}"
