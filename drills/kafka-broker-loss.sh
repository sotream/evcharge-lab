#!/usr/bin/env bash
# Drill: lose Kafka brokers under simulator load (ADR 0012).
#   ./drills/kafka-broker-loss.sh 1   one broker down:  producers keep going, no acknowledged message lost
#   ./drills/kafka-broker-loss.sh 2   two brokers down: NOT_ENOUGH_REPLICAS, gateway backpressure, full recovery
set -euo pipefail
LOST="${1:-1}"
DRILL="kafka-broker-loss-${LOST}" source "$(dirname "$0")/lib/common.sh"
require_fleet
DOWN_S="${DOWN_S:-60}"
cat > "$OUT/q.json" <<'Q'
{
  "meter_values_in_per_s": "sum(rate(ocpp_messages_total{direction=\"in\",type=\"call\",action=\"MeterValues\"}[10s]))",
  "produce_errors_per_s": "sum(rate(gateway_produce_errors_total[10s]))",
  "produce_rejected_per_s": "sum(rate(gateway_produce_rejected_total[10s]))",
  "produce_buffer_depth": "sum(gateway_produce_buffer_depth)",
  "rows_ingested_per_s": "sum(rate(telemetry_rows_ingested_total[10s]))",
  "ingest_lag_p99_s": "histogram_quantile(0.99, sum by (le) (rate(telemetry_ingest_lag_seconds_bucket[30s])))",
  "callresult_p99_s": "histogram_quantile(0.99, sum by (le) (rate(ocpp_call_duration_seconds_bucket[30s])))",
  "sim_replay_queue_depth": "sum(sim_replay_queue_depth)",
  "outbox_pending": "sum(outbox_pending)"
}
Q
log "probes: $(curl -s -X POST 127.0.0.1:28089/probes -H 'content-type: application/json' -d '{"count":10}')"
start_sampler "$OUT/q.json"
sleep "${BEFORE_S:-60}"

phase during
BROKERS=(kafka-2 kafka-3); VICTIMS=("${BROKERS[@]:0:$LOST}")
run docker compose stop "${VICTIMS[@]}"
sleep "$DOWN_S"
check "gateway_ready_during_outage" "$(curl -s --max-time 3 -o /dev/null -w '%{http_code}' localhost:28087/health/live)" eq 200

phase after
run docker compose start "${VICTIMS[@]}"
T_RECOVER=$(wait_until 300 "producer errors back to zero" bash -c "[ \"\$(curl -s 'http://localhost:29090/api/v1/query' --data-urlencode 'query=sum(rate(gateway_produce_errors_total[15s])) + sum(rate(gateway_produce_rejected_total[15s]))' | node -e 'let s=\"\";process.stdin.on(\"data\",d=>s+=d).on(\"end\",()=>{const r=JSON.parse(s).data.result;console.log(r.length?Number(r[0].value[1]):0)})')\" = 0 ]") || T_RECOVER=null
check "producer_recovery_s" "$T_RECOVER" le "${RECOVERY_MAX_S:-60}"
sleep "${AFTER_S:-90}"
if [ "$LOST" = 2 ]; then
  # From the drill's own timeline (an instant query at one moment can miss the window): NOT_ENOUGH_REPLICAS must
  # surface as produce errors, and the gateway buffer must absorb them (ADR 0012).
  ERRS=$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").trim().split("\n").map(JSON.parse).filter(x=>x.phase!=="before");console.log(Math.max(0,...l.map(x=>(x.produce_errors_per_s??0)+(x.produce_rejected_per_s??0))))' "$OUT/timeline.jsonl")
  BUF=$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").trim().split("\n").map(JSON.parse);console.log(Math.max(0,...l.map(x=>x.produce_buffer_depth??0)))' "$OUT/timeline.jsonl")
  check "max_produce_errors_per_s_during_outage" "$ERRS" ge 0.01
  check "max_produce_buffer_depth" "$BUF" ge 1
fi
UNTIL=$(date -u -v-60S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '-60 sec' +%Y-%m-%dT%H:%M:%SZ)
read -r GEN MISSING <<<"$(drills/lib/probe-check.sh "$OUT" "$UNTIL")"
log "probe readings generated=$GEN missing_in_timescale=$MISSING (until $UNTIL)"
check "probe_readings_generated" "$GEN" ge 1
check "probe_readings_missing" "$MISSING" eq 0
finish
