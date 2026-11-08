#!/usr/bin/env bash
# Drill: stations lose their network for up to 24 h of simulated time while charging (ADR 0000 offline window,
# ADR 0003 replay). They keep charging, queue StartTransaction/StopTransaction/MeterValues, and replay them in order
# with their original uniqueIds on reconnect. Pass: every probe reading reaches Timescale exactly once, replayed rows
# are flagged, and no station's transaction queue is left stuck.
#   OFFLINE_MIN  simulated minutes offline (default 1440 = 24 h; needs SIM_TIME_SCALE high enough to finish in time)
set -euo pipefail
DRILL="station-offline-replay" source "$(dirname "$0")/lib/common.sh"
require_fleet
OFFLINE_MIN="${OFFLINE_MIN:-1440}"
STATIONS="${STATIONS:-20}"
cat > "$OUT/q.json" <<'Q'
{
  "sim_connected": "sum(sim_connected_stations)",
  "sim_replay_queue_depth": "sum(sim_replay_queue_depth)",
  "sim_queue_dropped_total": "sum(sim_queue_dropped_total)",
  "meter_values_in_per_s": "sum(rate(ocpp_messages_total{direction=\"in\",type=\"call\",action=\"MeterValues\"}[10s]))",
  "rows_ingested_per_s": "sum(rate(telemetry_rows_ingested_total[10s]))",
  "dedup_dropped_per_s": "sum(rate(telemetry_dedup_dropped_total[10s]))",
  "ocpp_replays_per_s": "sum(rate(sessions_ocpp_replays_total[10s]))",
  "callresult_p99_s": "histogram_quantile(0.99, sum by (le) (rate(ocpp_call_duration_seconds_bucket[30s])))"
}
Q
SCALE=$(curl -s 127.0.0.1:28089/status | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).timeScale))')
log "simulator time scale $SCALE; offline ${OFFLINE_MIN} simulated min = $(( OFFLINE_MIN * 60 / SCALE )) s wall clock"
PROBES=$(curl -s -X POST 127.0.0.1:28089/probes -H 'content-type: application/json' -d "{\"count\":${STATIONS}}")
log "probes: $PROBES"
IDS=$(echo "$PROBES" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const j=JSON.parse(s);console.log(JSON.stringify(j.stations??[]))})')
start_sampler "$OUT/q.json"
sleep "${BEFORE_S:-60}"
phase during
OFF=$(curl -s -X POST 127.0.0.1:28089/offline -H 'content-type: application/json' -d "{\"ids\":${IDS},\"minutes\":${OFFLINE_MIN}}")
log "offline: $OFF"
OFFN=$(echo "$OFF" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{console.log(JSON.parse(s).stations??0)}catch{console.log(0)}})')
# The drill is only valid if stations really went offline (an earlier run silently got HTTP 400 here).
check "stations_taken_offline" "$OFFN" ge "$(echo "$IDS" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).length))')"
sleep 5
check "connected_dropped_during_offline" "$(( $(curl -s 127.0.0.1:28089/status | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const j=JSON.parse(s);console.log(j.started-j.connected)})') ))" ge "$OFFN"
sleep $(( OFFLINE_MIN * 60 / SCALE + 5 ))
phase after
T=$(wait_until 600 "replay queues drained" bash -c "[ \"\$(curl -s 127.0.0.1:28089/status | node -e 'let s=\"\";process.stdin.on(\"data\",d=>s+=d).on(\"end\",()=>console.log(JSON.parse(s).replayQueueDepth))')\" = 0 ]") || T=null
check "replay_drained_within_s" "$T" le 600
check "queue_dropped_total" "$(prom 'sum(sim_queue_dropped_total)')" eq 0
sleep "${AFTER_S:-60}"
# Readings carry simulated time; allow one wall-clock minute of ingest lag, converted to simulated time.
UNTIL=$(curl -s 127.0.0.1:28089/status | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const j=JSON.parse(s);console.log(new Date(Date.parse(j.simTime)-60000*j.timeScale).toISOString())})')
read -r GEN MISSING <<<"$(drills/lib/probe-check.sh "$OUT" "$UNTIL")"
log "probe readings generated=$GEN missing=$MISSING (until simulated $UNTIL)"
check "probe_readings_generated" "$GEN" ge 1
check "probe_readings_missing" "$MISSING" eq 0
DUP=$(docker compose exec -T postgres-telemetry psql -qtA -U evcharge -d telemetry -c "SELECT count(*) FROM (SELECT station_id, connector_id, time FROM meter_readings GROUP BY 1,2,3 HAVING count(*) > 1) d")
check "duplicate_rows" "$DUP" eq 0
# The gateway flags replays by comparing station time with the wall clock (> 60 s old), which only works at time
# scale 1; at higher scales simulated time runs ahead of the wall clock, so the flag is checked only at scale 1.
if [ "$SCALE" = 1 ]; then
  REPLAYED=$(docker compose exec -T postgres-telemetry psql -qtA -U evcharge -d telemetry -c "SELECT count(*) FROM meter_readings WHERE replayed AND time > now() - interval '2 days'")
  check "replayed_rows_flagged" "$REPLAYED" ge 1
else
  log "replayed flag not checked at time scale $SCALE"
fi
finish
