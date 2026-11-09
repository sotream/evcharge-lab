#!/usr/bin/env bash
# Drill: lose the OLTP primary and fail over to the streaming replica (ADR 0018, ADR 0000 "RPO 0 for sessions and billing").
#
#   MODE=auto    (default) the script promotes the replica and repoints the services itself.
#   MODE=manual  the script kills the primary, prints the commands, and waits for YOU to promote and repoint.
#   SYNC=0|1     0 (default) asynchronous replication; 1 sets synchronous_standby_names = 'FIRST 1 (replica_1)'.
#   STALL_S      seconds the replica is cut off from the primary (docker network disconnect) BEFORE the kill (default 3).
#                This is the "replica was behind when the primary died" fault. 0 = a clean kill with a healthy replica.
#   BEFORE_S (30), AFTER_S (60), MANUAL_WAIT_S (900), RTO_MAX_S (60, auto only), DRIVERS (12), RESET (1).
#
# What it measures (see docs/runbook.md "How to read the postgres-failover output"):
#   - RTO: time from the kill to the first acknowledged write, seen by a direct DB writer and by driver reservations;
#   - acknowledged writes lost: a DB probe commits one row at a time and records every acknowledgement; afterwards every
#     acknowledged row must exist on the new primary. Reservation history is checked by load/history/check.mjs;
#   - errors seen by drivers (reservation API) and by stations (OCPP CALLERROR) and services (HTTP 5xx).
# The primary is destroyed, so unless RESET=0 the drill ends by rebuilding the whole stack from empty volumes.
set -euo pipefail
MODE="${MODE:-auto}"
SYNC="${SYNC:-0}"
STALL_S="${STALL_S:-3}"
case "$MODE" in auto|manual) ;; *) echo "MODE must be auto or manual" >&2; exit 2 ;; esac
LABEL="$MODE-$([ "$SYNC" = 1 ] && echo sync || echo async)-stall${STALL_S}"
DRILL="postgres-failover-$LABEL" source "$(dirname "$0")/lib/common.sh"
require_fleet
BEFORE_S="${BEFORE_S:-30}"
AFTER_S="${AFTER_S:-60}"
MANUAL_WAIT_S="${MANUAL_WAIT_S:-900}"
RTO_MAX_S="${RTO_MAX_S:-60}"
DRIVERS="${DRIVERS:-12}"
RESET="${RESET:-1}"

PRIM=$(docker compose ps -q postgres)
REPL=$(docker compose ps -q postgres-replica)
NET=$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$REPL")
now_ms() { node -e 'console.log(Date.now())'; }
psql_p() { docker exec "$PRIM" psql -U evcharge -d "${DB:-postgres}" -tAc "$1"; }
psql_r() { docker exec "$REPL" psql -U evcharge -d "${DB:-postgres}" -tAc "$1"; }
mkdir -p "$OUT/ack" "$OUT/history"
TOTAL=$(curl -s --max-time 5 127.0.0.1:28089/status | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).started))')

# --- replication mode ---------------------------------------------------------------------------------------------
if [ "$SYNC" = 1 ]; then
  psql_p "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (replica_1)'" >/dev/null
else
  psql_p "ALTER SYSTEM RESET synchronous_standby_names" >/dev/null
fi
psql_p "SELECT pg_reload_conf()" >/dev/null
SSN=$(psql_p "SHOW synchronous_standby_names" | tr -d '\n')
log "replication mode: synchronous_standby_names='$SSN' synchronous_commit=$(psql_p 'SHOW synchronous_commit' | tr -d '\n')"
if [ "$SYNC" = 1 ]; then
  wait_until 30 "replica_1 is a synchronous standby" bash -c "docker exec $PRIM psql -U evcharge -d postgres -tAc \"select sync_state from pg_stat_replication where application_name='replica_1'\" | grep -q sync" >/dev/null \
    || { log "ABORT: the replica does not connect with application_name=replica_1 (see infra/postgres/replica-entrypoint.sh)"; exit 3; }
fi
log "pg_stat_replication: $(psql_p "select application_name||' '||state||' '||sync_state from pg_stat_replication" | tr '\n' ';')"

cat > "$OUT/q.json" <<'Q'
{
  "sim_connected": "sum(sim_connected_stations)",
  "sim_active_sessions": "sum(sim_active_sessions)",
  "sessions_5xx_per_s": "sum(rate(http_server_requests_seconds_count{service=\"sessions\",status=~\"5..\"}[10s]))",
  "driver_api_5xx_per_s": "sum(rate(http_server_requests_seconds_count{service=\"driver-api\",status=~\"5..\"}[10s]))",
  "ocpp_callerror_out_per_s": "sum(rate(ocpp_messages_total{direction=\"out\",type=\"error\"}[10s]))",
  "sessions_5xx_total": "sum(http_server_requests_seconds_count{service=\"sessions\",status=~\"5..\"})",
  "driver_api_5xx_total": "sum(http_server_requests_seconds_count{service=\"driver-api\",status=~\"5..\"})",
  "ocpp_error_out_total": "sum(ocpp_messages_total{direction=\"out\",type=\"error\"})",
  "callresult_p99_s": "histogram_quantile(0.99, sum by (le) (rate(ocpp_call_duration_seconds_bucket[30s])))"
}
Q
start_sampler "$OUT/q.json"
PROBE_PID=""; GEN_PID=""
cleanup() {
  docker stop -t 6 evcharge-ack-probe >/dev/null 2>&1 || true
  [ -n "$GEN_PID" ] && kill "$GEN_PID" 2>/dev/null || true
  # never leave the replica cut off from the network if the drill dies half way
  docker network connect "$NET" "$REPL" >/dev/null 2>&1 || true
}
trap 'cleanup; stop_sampler' EXIT

# The probe runs ON the compose network and connects to the name `postgres`, exactly like the services, so the alias move is part of what it measures.
docker rm -f evcharge-ack-probe >/dev/null 2>&1 || true
docker run --rm --name evcharge-ack-probe --network "$NET" -v "$PWD:/repo" -w /repo -e OUT="/repo/$OUT/ack" node:26-alpine node load/pg-ack-probe.mjs 2>>"$OUT/events.log" &
PROBE_PID=$!
RUN_START_MS=$(now_ms) # the table dump below keeps only rows created from here on, not leftovers of earlier runs
OUT="$OUT/history" DURATION_S=3600 DRIVERS="$DRIVERS" node load/history/generate.mjs 2>>"$OUT/events.log" &
GEN_PID=$!
log "workloads: DB ack probe pid $PROBE_PID, reservation history generator pid $GEN_PID ($DRIVERS drivers)"
sleep "$BEFORE_S"
check "fleet_connected_before_kill" "$(prom 'sum(sim_connected_stations)')" ge "$TOTAL"
check "probe_acks_before_kill" "$(wc -l < "$OUT/ack/acks.jsonl" | tr -d ' ')" ge 100
check "reserve_ok_before_kill" "$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").split("\n").filter(Boolean).map(JSON.parse);console.log(l.filter(o=>o.type==="reserve"&&o.outcome==="ok").length)' "$OUT/history/history.jsonl")" ge 1

# --- the fault ----------------------------------------------------------------------------------------------------
phase during
STALL_START=null
if [ "$STALL_S" != 0 ]; then
  STALL_START=$(now_ms)
  log "RUN docker network disconnect $NET <replica>  (partition: no WAL reaches the replica for ${STALL_S}s)"
  docker network disconnect "$NET" "$REPL"
  sleep "$STALL_S"
fi
log "RUN docker kill <primary>"
docker kill "$PRIM" >/dev/null
# T_KILL is taken when docker kill has returned, i.e. the primary is certainly dead. Taking it before the command counted
# acknowledgements from the last 100 ms of the primary's life as "recovery" (first run: RTO 0.002 s).
KILL=$(now_ms)
log "primary dead (T_KILL=$KILL)"
# The replica rejoins the network under its own name; the alias postgres is added after the promotion.
docker network connect "$NET" "$REPL" 2>/dev/null || true
PROMOTE_START=null; PROMOTED=null; REPOINTED=null
if [ "$MODE" = auto ]; then
  PROMOTE_START=$(now_ms)
  log "RUN pg_promote on the replica"
  PROM_OUT=$(docker exec "$REPL" psql -U evcharge -d postgres -tAc "select pg_promote(wait => true, wait_seconds => 60)" | tr -d '\n')
  PROMOTED=$(now_ms)
  log "pg_promote returned $PROM_OUT"
  log "RUN repoint: docker network disconnect/connect --alias postgres <replica>"
  docker network disconnect "$NET" "$REPL"
  docker network connect --alias postgres --alias postgres-replica "$NET" "$REPL"
  REPOINTED=$(now_ms)
else
  cat >&2 <<EOF

==================== YOUR TURN (MODE=manual) ====================
The primary is dead (docker kill). The replica must become the new primary, and the name 'postgres' must point at it,
because every service connects to the host name 'postgres'. In another terminal, from the repository root:

  1. docker compose exec postgres-replica psql -U evcharge -d postgres -c "select pg_promote()"
  2. docker compose exec postgres-replica psql -U evcharge -d postgres -tAc "select pg_is_in_recovery()"    # must print: f
  3. docker network disconnect $NET $REPL
  4. docker network connect --alias postgres --alias postgres-replica $NET $REPL
     (the container id $REPL is the replica; `docker compose ps -q postgres-replica` prints it)

Steps 3 and 4 move the DNS name 'postgres' to the replica without touching any service. Watch the services reconnect with
`docker compose logs -f sessions`. The drill waits up to ${MANUAL_WAIT_S}s for the first driver reservation to succeed again.
=================================================================
EOF
fi
phase after

# --- recovery -----------------------------------------------------------------------------------------------------
recovered() { grep -E '"outcome":"ok"' "$OUT/history/history.jsonl" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const k=Number(process.argv[1]);process.exit(s.split("\n").filter(Boolean).some(l=>{const o=JSON.parse(l);return o.type==="reserve"&&o.invokeMs>k})?0:1)})' "$KILL"; }
WAIT=$([ "$MODE" = auto ] && echo 120 || echo "$MANUAL_WAIT_S")
if T_REC=$(wait_until "$WAIT" "first accepted driver reservation after the kill" recovered); then :; else T_REC=null; fi
if [ "$MODE" = manual ]; then PROMOTED=$(now_ms); REPOINTED=$PROMOTED; fi # manual timings are not recorded step by step
sleep "$AFTER_S"

docker stop -t 6 evcharge-ack-probe >/dev/null 2>&1 || true
kill "$GEN_PID" 2>/dev/null || true
wait "$PROBE_PID" "$GEN_PID" 2>/dev/null || true
PROBE_PID=""; GEN_PID=""

# --- evidence from the NEW primary -----------------------------------------------------------------------------------
DB=evcharge psql_r "SELECT seq FROM drill_acks ORDER BY seq" > "$OUT/ack/db-seqs.txt" || : > "$OUT/ack/db-seqs.txt"
docker exec "$REPL" psql -U evcharge -d sessions -At -c "SELECT COALESCE(json_agg(json_build_object('reservation_id', r.reservation_id, 'station_id', c.station_id, 'connector_id', c.connector_id, 'starts_at', r.starts_at, 'expires_at', r.expires_at, 'status', r.status, 'ended_at', r.ended_at)), '[]') FROM reservations r JOIN connectors c ON c.connector_pk = r.connector_pk WHERE r.created_at >= to_timestamp($RUN_START_MS / 1000.0)" > "$OUT/history/reservations.json"
node -e '
  const [kill, stall, ps, pd, rp, mode, sync, stallS, ssn] = process.argv.slice(1);
  const n = (v) => (v === "null" ? null : Number(v));
  console.log(JSON.stringify({ kill: n(kill), stallStart: n(stall), promoteStart: n(ps), promoted: n(pd), repointed: n(rp), mode, sync: sync === "1", stallS: Number(stallS), synchronousStandbyNames: ssn }));
' "$KILL" "$STALL_START" "$PROMOTE_START" "$PROMOTED" "$REPOINTED" "$MODE" "$SYNC" "$STALL_S" "$SSN" > "$OUT/marks.json"
node drills/lib/failover-report.mjs "$OUT" >&2 || log "report failed"

set +e
node load/history/check.mjs --history "$OUT/history/history.jsonl" --db "$OUT/history/reservations.json" > "$OUT/history/check-report.txt" 2>&1
CHECK_RC=$?
set -e
log "history checker exit code $CHECK_RC (0 pass, 1 violation, 2 bad input); report in history/check-report.txt"

field() { node -e 'const s=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));console.log(process.argv[2].split(".").reduce((a,k)=>a?.[k],s)??"null")' "$OUT/summary.json" "$1"; }
if [ "$MODE" = auto ]; then
  check "rto_db_write_s" "$(field rtoDbS)" le "$RTO_MAX_S"
  check "rto_driver_reservation_s" "$(field rtoAppS)" le "$RTO_MAX_S"
else
  log "RTO (manual, depends on the operator): db $(field rtoDbS) s, driver reservation $(field rtoAppS) s"
fi
# ADR 0000: RPO 0 for sessions and billing. A non-zero value here is a finding, not a drill failure to be tuned away.
check "acknowledged_probe_writes_lost" "$(field probe.lost)" eq 0
check "history_checker_violations" "$CHECK_RC" eq 0
check "fleet_connected_after" "$(prom 'sum(sim_connected_stations)')" ge "$TOTAL"
DEFER_EXIT=1 finish
cat "$OUT/timeline.txt" >&2

if [ "$RESET" = 1 ]; then
  trap - EXIT
  drills/lib/reset-stack.sh
fi
[ "$DRILL_PASS" = true ] || exit 1
