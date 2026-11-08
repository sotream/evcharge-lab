#!/usr/bin/env bash
# Zero-loss check: every probe reading the simulator generated before $2 (ISO time) must be in Timescale.
# Usage: probe-check.sh <out-dir> <until-iso>   -> prints "generated missing"
set -euo pipefail
OUTDIR="$1"; UNTIL="$2"
rm -rf "$OUTDIR/probes" && mkdir -p "$OUTDIR/probes"
docker compose cp -q simulator:/tmp/sim-probes/. "$OUTDIR/probes/" >/dev/null 2>&1 || true
CSV="$OUTDIR/probes/readings.csv"
cat "$OUTDIR"/probes/*.jsonl 2>/dev/null | node -e '
  const until = Date.parse(process.argv[1]); let s = "";
  process.stdin.on("data", d => s += d).on("end", () => {
    for (const line of s.split("\n")) { if (!line) continue; const r = JSON.parse(line);
      if (Date.parse(r.ts) <= until) console.log([r.stationId, r.connectorId, r.ts].join(",")); } });' "$UNTIL" > "$CSV"
GEN=$(wc -l < "$CSV" | tr -d " ")
MISSING=$( { echo "CREATE TEMP TABLE p(station_id text, connector_id int, t timestamptz);"; echo "COPY p FROM STDIN WITH (FORMAT csv);"; cat "$CSV"; echo "\\."; \
  echo "SELECT count(*) FROM p WHERE NOT EXISTS (SELECT 1 FROM meter_readings m WHERE m.station_id = p.station_id AND m.connector_id = p.connector_id AND m.time = p.t);"; } \
  | docker compose exec -T postgres-telemetry psql -qtA -U evcharge -d telemetry | tail -1)
echo "$GEN $MISSING"
