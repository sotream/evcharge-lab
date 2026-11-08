#!/usr/bin/env bash
# Drill: the sessions read replica stops replaying WAL (pg_wal_replay_pause) for DOWN_S seconds, optionally with its CPU throttled.
# ADR 0015: stale-tolerant reads (site map) may only be as old as the replica lag, reads of a driver's own writes never go
# stale (they fall back to the primary after 200 ms), driver-api read p99 stays below 300 ms, and the lag drains afterwards.
# Env: DOWN_S (default 240: the SessionsReplicaLagHigh rule needs lag above 5 s for 2 m and Prometheus evaluates rules every 1 m, so it can fire up to 3 m after the pause; the first run with 150 s never reached it), BEFORE_S, AFTER_S, LOAD_RPS, PROBES,
#      THROTTLE_CPUS (optional, e.g. 0.2: also slows the replica's CPU during the pause).
set -euo pipefail
DRILL="replica-lag" source "$(dirname "$0")/lib/common.sh"
require_fleet
DOWN_S="${DOWN_S:-240}"
BEFORE_S="${BEFORE_S:-45}"
AFTER_S="${AFTER_S:-60}"
# Allowed extra staleness on top of the measured replica lag: driver-api sites cache (3 s) + sessions lag sampling (5 s) + scrape (5 s).
MARGIN_S="${MARGIN_S:-13}"
REPLICA_SQL() { docker compose exec -T postgres-replica psql -U evcharge -d evcharge -tAc "$1"; }

[ "$(REPLICA_SQL 'select pg_is_in_recovery()' | tr -d '[:space:]')" = t ] || { log "ABORT: postgres-replica is not a standby"; rm -rf "$OUT"; exit 3; }

cat > "$OUT/q.json" <<'Q'
{
  "replica_lag_s_exporter": "max(pg_replication_lag_seconds{db=\"oltp-replica\"})",
  "replica_lag_s_sessions": "max(sessions_replica_lag_seconds)",
  "replica_lag_bytes": "max(sessions_replica_lag_bytes)",
  "reads_replica_per_s": "sum(rate(sessions_reads_total{target=\"replica\"}[10s]))",
  "reads_primary_per_s": "sum(rate(sessions_reads_total{target=\"primary\"}[10s]))",
  "ryw_caught_up_per_s": "sum(rate(sessions_ryw_waits_total{outcome=\"replica_caught_up\"}[10s]))",
  "ryw_fallback_per_s": "sum(rate(sessions_ryw_waits_total{outcome=\"fallback_primary\"}[10s]))",
  "driver_api_read_p99_s": "histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{service=\"driver-api\",method=\"GET\",route!~\"/(health.*|ready.*|live.*|metrics)\"}[15s])))"
}
Q
start_sampler "$OUT/q.json"

TOTAL_S=$((BEFORE_S + DOWN_S + AFTER_S + 90))
OUT="$OUT" PHASE_FILE="$PHASE_FILE" DURATION_S="$TOTAL_S" LOAD_RPS="${LOAD_RPS:-200}" PROBES="${PROBES:-4}" node load/replica-lag-probe.mjs 2>>"$OUT/events.log" &
PROBE_PID=$!
log "driver probe pid $PROBE_PID for ${TOTAL_S}s"
sleep "$BEFORE_S"
check "replica_in_sync_before" "$(prom 'max(sessions_replica_lag_seconds)')" le 2

phase during
CID=$(docker compose ps -q postgres-replica)
if [ -n "${THROTTLE_CPUS:-}" ]; then run docker update --cpus "$THROTTLE_CPUS" "$CID"; fi
log "RUN pg_wal_replay_pause on the replica: $(REPLICA_SQL 'select pg_wal_replay_pause()' | tr -d '\n')"
sleep "$DOWN_S"
# Worst 15 s window of the pause, from the raw timeline (a single long-range Prometheus query returned null in the first run).
check "driver_api_read_p99_s_during" "$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").trim().split("\n").map(JSON.parse).filter(x=>x.phase==="during"&&x.driver_api_read_p99_s!=null);console.log(l.length?Math.max(...l.map(x=>x.driver_api_read_p99_s)):"null")' -- "$OUT/timeline.jsonl")" le 0.3
check "ryw_fell_back_to_primary" "$(prom "increase(sessions_ryw_waits_total{outcome=\"fallback_primary\"}[${DOWN_S}s])")" ge 1
check "replica_lag_alert_fired" "$(prom 'max_over_time(ALERTS{alertname="SessionsReplicaLagHigh",alertstate="firing"}[10m])')" ge 1

phase after
log "RUN pg_wal_replay_resume on the replica: $(REPLICA_SQL 'select pg_wal_replay_resume()' | tr -d '\n')"
# `--cpus 0` does not remove a limit; restore to every CPU of the Docker VM instead.
if [ -n "${THROTTLE_CPUS:-}" ]; then run docker update --cpus "$(docker info --format "{{.NCPU}}")" "$CID"; fi
T=$(wait_until 300 "replica lag drained" bash -c "v=\$(curl -s 'http://localhost:29090/api/v1/query' --data-urlencode 'query=max(sessions_replica_lag_seconds)' | node -e 'let s=\"\";process.stdin.on(\"data\",d=>s+=d).on(\"end\",()=>{const r=JSON.parse(s).data.result;console.log(r.length?Number(r[0].value[1]):999999)})'); [ \"\${v%.*}\" -lt 1 ]") || T=null
check "lag_drained_within_s" "$T" le 120
sleep "$AFTER_S"
log "waiting for the driver probe to finish"
wait "$PROBE_PID" || log "probe exited non-zero"

PEAK=$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").trim().split("\n").map(JSON.parse);console.log(Math.max(0,...l.map(x=>Math.max(x.replica_lag_s_exporter??0,x.replica_lag_s_sessions??0))))' "$OUT/timeline.jsonl")
log "peak replica lag: $PEAK s"
S="$OUT/probe-summary.json"
field() { node -e 'const s=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));const v=process.argv[2].split(".").reduce((a,k)=>a?.[k],s);console.log(v??"null")' "$S" "$1"; }
log "probe summary: $(tr -d '\n ' < "$S")"
check "probe_ryw_reads_made" "$(field ryw_checks)" ge 3
check "ryw_violations" "$(field ryw_violations)" eq 0
check "stale_probes_resolved" "$(field stale_probes)" ge 1
check "stale_unresolved" "$(field stale_unresolved)" eq 0
# Documented bound (ADR 0015): a stale-tolerant read is at most the replica lag old, plus the sites cache and sampling margin.
check "stale_excess_over_lag_s" "$(node -e 'console.log(Number(process.argv[1]) - (Number(process.argv[2]) + Number(process.argv[3])))' "$(field stale_max_s)" "$PEAK" "$MARGIN_S")" le 0
check "client_read_errors" "$(field read_errors)" eq 0
finish
