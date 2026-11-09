#!/usr/bin/env bash
# Drill: the reservation invariant under faults, proved from an operation history (ADR 0019).
#
# A load script (load/history/generate.mjs) records every reserve and cancel as one JSON line: client op id, connector,
# invoke and complete timestamps, outcome. A fault is injected while it runs. Afterwards a separate checker
# (load/history/check.mjs) reads the history plus a dump of the reservations table and proves two things:
#   1. no connector has two acknowledged reservations whose held intervals overlap;
#   2. every acknowledged reservation exists in the database after recovery.
# This is NOT a full linearizability check (ADR 0019 says what it does not prove).
#
#   FAULT=pause    docker pause the sessions service for PAUSE_S seconds (default 15): requests hang, clients time out, then resume.
#   FAULT=restart  docker restart the sessions service (in-flight requests are cut).
#   FAULT=none     no fault (control run).
#   (kill primary + promote lives in drills/postgres-failover.sh, which runs the same history and checker.)
#   MUTATE=1       MUTATION TEST: drop the exclusion constraints before the run, so the protection is gone. The checker MUST go
#                  red; the drill passes only if it does. The constraints are put back afterwards.
# Env: BEFORE_S (20), AFTER_S (40), PAUSE_S (15), DRIVERS (12), RECOVERY_MAX_S (60).
set -euo pipefail
FAULT="${FAULT:-pause}"
MUTATE="${MUTATE:-0}"
case "$FAULT" in pause|restart|none) ;; *) echo "FAULT must be pause, restart or none" >&2; exit 2 ;; esac
DRILL="reservation-invariant-$FAULT$([ "$MUTATE" = 1 ] && echo -mutated)" source "$(dirname "$0")/lib/common.sh"
require_fleet
BEFORE_S="${BEFORE_S:-20}"
AFTER_S="${AFTER_S:-40}"
PAUSE_S="${PAUSE_S:-15}"
DRIVERS="${DRIVERS:-12}"
RECOVERY_MAX_S="${RECOVERY_MAX_S:-60}"
PRIM=$(docker compose ps -q postgres)
SESS=$(docker compose ps -q sessions)
now_ms() { node -e 'console.log(Date.now())'; }
qs() { docker exec "$PRIM" psql -U evcharge -d sessions -tAc "$1"; }
mkdir -p "$OUT/history"

constraints() { qs "SELECT conrelid::regclass || ':' || conname FROM pg_constraint WHERE contype = 'x' AND conrelid::regclass::text LIKE 'reservations_%' ORDER BY 1"; }
restore_protection() {
  log "restoring the exclusion constraints: ending every open reservation, then ADD CONSTRAINT"
  qs "UPDATE reservations SET status = 'Cancelled', ended_at = now() WHERE status IN ('Pending','Active')" >/dev/null
  for part in $(qs "SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent = 'reservations'::regclass"); do
    qs "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = '$part'::regclass AND contype = 'x') THEN EXECUTE 'ALTER TABLE $part ADD CONSTRAINT ${part}_no_overlap EXCLUDE USING gist (connector_pk WITH =, period WITH &&) WHERE (status IN (''Pending'', ''Active''))'; END IF; END \$\$" >/dev/null
  done
}

CONSTRAINTS_BEFORE=$(constraints | wc -l | tr -d ' ')
log "exclusion constraints on reservation partitions: $CONSTRAINTS_BEFORE"
MUTATED=0
if [ "$MUTATE" = 1 ]; then
  for c in $(constraints); do
    qs "ALTER TABLE ${c%%:*} DROP CONSTRAINT ${c##*:}" >/dev/null
    MUTATED=$((MUTATED + 1))
  done
  log "MUTATION: dropped $MUTATED exclusion constraints; the database no longer prevents overlapping reservations"
fi
GEN_PID=""
cleanup() {
  [ -n "$GEN_PID" ] && kill "$GEN_PID" 2>/dev/null || true
  docker unpause "$SESS" >/dev/null 2>&1 || true
  if [ "$MUTATE" = 1 ] && [ "$(constraints | wc -l | tr -d ' ')" != "$CONSTRAINTS_BEFORE" ]; then restore_protection; fi
}
trap 'cleanup; stop_sampler' EXIT

cat > "$OUT/q.json" <<'Q'
{
  "sim_connected": "sum(sim_connected_stations)",
  "sessions_5xx_per_s": "sum(rate(http_server_requests_seconds_count{service=\"sessions\",status=~\"5..\"}[10s]))",
  "driver_api_5xx_per_s": "sum(rate(http_server_requests_seconds_count{service=\"driver-api\",status=~\"5..\"}[10s]))",
  "sessions_p99_s": "histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{service=\"sessions\"}[15s])))"
}
Q
start_sampler "$OUT/q.json"
RUN_START_MS=$(now_ms) # the table dump below keeps only rows created from here on, not leftovers of earlier runs
OUT="$OUT/history" DURATION_S=3600 DRIVERS="$DRIVERS" node load/history/generate.mjs 2>>"$OUT/events.log" &
GEN_PID=$!
log "history generator pid $GEN_PID ($DRIVERS drivers)"
sleep "$BEFORE_S"

phase during
FAULT_START=$(now_ms)
case "$FAULT" in
  pause) log "RUN docker pause sessions for ${PAUSE_S}s"; docker pause "$SESS" >/dev/null; sleep "$PAUSE_S"; docker unpause "$SESS" >/dev/null ;;
  restart) log "RUN docker restart sessions"; docker restart "$SESS" >/dev/null ;;
  none) log "no fault (control)"; sleep 5 ;;
esac
FAULT_END=$(now_ms)
log "fault window ended after $(( (FAULT_END - FAULT_START) / 1000 )) s"
phase after
recovered() { node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").split("\n").filter(Boolean).map(JSON.parse);process.exit(l.some(o=>o.type==="reserve"&&o.outcome==="ok"&&o.invokeMs>Number(process.argv[2]))?0:1)' "$OUT/history/history.jsonl" "$FAULT_END"; }
T_REC=$(wait_until "$RECOVERY_MAX_S" "first accepted reservation after the fault" recovered) || T_REC=null
sleep "$AFTER_S"
kill "$GEN_PID" 2>/dev/null || true
wait "$GEN_PID" 2>/dev/null || true
GEN_PID=""

docker exec "$PRIM" psql -U evcharge -d sessions -At -c "SELECT COALESCE(json_agg(json_build_object('reservation_id', r.reservation_id, 'station_id', c.station_id, 'connector_id', c.connector_id, 'starts_at', r.starts_at, 'expires_at', r.expires_at, 'status', r.status, 'ended_at', r.ended_at)), '[]') FROM reservations r JOIN connectors c ON c.connector_pk = r.connector_pk WHERE r.created_at >= to_timestamp($RUN_START_MS / 1000.0)" > "$OUT/history/reservations.json"
set +e
node load/history/check.mjs --history "$OUT/history/history.jsonl" --db "$OUT/history/reservations.json" > "$OUT/history/check-report.txt" 2>&1
CHECK_RC=$?
set -e
head -3 "$OUT/history/check-report.txt" | while read -r l; do log "checker: $l"; done
node -e '
  const l = require("fs").readFileSync(process.argv[1], "utf8").split("\n").filter(Boolean).map(JSON.parse);
  const by = (f) => l.filter(f).length;
  const s = { ops: l.length, reserveOk: by((o) => o.type === "reserve" && o.outcome === "ok"), reserveRejected: by((o) => o.type === "reserve" && o.outcome === "rejected"), reserveError: by((o) => o.type === "reserve" && o.outcome === "error"), cancelOk: by((o) => o.type === "cancel" && o.outcome === "ok"), cancelError: by((o) => o.type === "cancel" && o.outcome === "error"), checkerExitCode: Number(process.argv[2]), mutated: Number(process.argv[3]) };
  require("fs").writeFileSync(process.argv[4], JSON.stringify(s, null, 2) + "\n");
  console.log(JSON.stringify(s));
' "$OUT/history/history.jsonl" "$CHECK_RC" "$MUTATED" "$OUT/summary.json" >&2

check "reserve_ok_before_fault" "$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").split("\n").filter(Boolean).map(JSON.parse);console.log(l.filter(o=>o.type==="reserve"&&o.outcome==="ok"&&o.invokeMs<Number(process.argv[2])).length)' "$OUT/history/history.jsonl" "$FAULT_START")" ge 1
check "recovered_within_s" "$T_REC" le "$RECOVERY_MAX_S"
if [ "$MUTATE" = 1 ]; then
  check "constraints_dropped" "$MUTATED" ge 1
  # Passing means the checker caught the missing protection.
  check "checker_detects_mutation" "$([ "$CHECK_RC" = 1 ] && echo 1 || echo 0)" eq 1
else
  check "history_checker_violations" "$CHECK_RC" eq 0
fi
if [ "$MUTATE" = 1 ]; then restore_protection; check "constraints_restored" "$(constraints | wc -l | tr -d ' ')" eq "$CONSTRAINTS_BEFORE"; fi
finish
