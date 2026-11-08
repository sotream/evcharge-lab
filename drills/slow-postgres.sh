#!/usr/bin/env bash
# Drill: the telemetry Postgres becomes slow (CPU throttled to 0.05 cores) for DOWN_S seconds.
# ADR 0008: ingest pauses instead of growing memory, Kafka holds the backlog, lag drains afterwards,
# no reading is lost, and the OCPP path is unaffected (telemetry is decoupled by Kafka).
set -euo pipefail
DRILL="slow-postgres" source "$(dirname "$0")/lib/common.sh"
require_fleet
DOWN_S="${DOWN_S:-90}"
cat > "$OUT/q.json" <<'Q'
{
  "rows_ingested_per_s": "sum(rate(telemetry_rows_ingested_total[10s]))",
  "ingest_lag_p99_s": "histogram_quantile(0.99, sum by (le) (rate(telemetry_ingest_lag_seconds_bucket[30s])))",
  "buffered_rows": "sum(telemetry_buffered_rows)",
  "ingest_paused": "max(telemetry_ingest_paused)",
  "consumer_lag": "sum(kafka_consumer_lag{group=\"telemetry-ingest.meter-values\"})",
  "flush_p99_s": "histogram_quantile(0.99, sum by (le) (rate(telemetry_flush_seconds_bucket[30s])))",
  "callresult_p99_s": "histogram_quantile(0.99, sum by (le) (rate(ocpp_call_duration_seconds_bucket[30s])))"
}
Q
log "probes: $(curl -s -X POST 127.0.0.1:28089/probes -H 'content-type: application/json' -d '{"count":10}')"
start_sampler "$OUT/q.json"
sleep "${BEFORE_S:-60}"
phase during
CID=$(docker compose ps -q postgres-telemetry)
run docker update --cpus 0.05 "$CID"
sleep "$DOWN_S"
check "ocpp_callresult_p99_s_during" "$(prom 'histogram_quantile(0.99, sum by (le) (rate(ocpp_call_duration_seconds_bucket[60s])))')" le 0.5
phase after
# `--cpus 0` does not remove a limit; restore to every CPU of the Docker VM instead.
run docker update --cpus "$(docker info --format "{{.NCPU}}")" "$CID"
T=$(wait_until 600 "consumer lag drained" bash -c "v=\$(curl -s 'http://localhost:29090/api/v1/query' --data-urlencode 'query=sum(kafka_consumer_lag{group=\"telemetry-ingest.meter-values\"})' | node -e 'let s=\"\";process.stdin.on(\"data\",d=>s+=d).on(\"end\",()=>{const r=JSON.parse(s).data.result;console.log(r.length?Number(r[0].value[1]):999999)})'); [ \"\$v\" -lt 200 ]") || T=null
check "lag_drained_within_s" "$T" le 300
PEAK=$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").trim().split("\n").map(JSON.parse).filter(x=>x.phase==="during");console.log(Math.max(0,...l.map(x=>x.consumer_lag??0)))' "$OUT/timeline.jsonl")
log "peak consumer lag during slowdown: $PEAK records (the backlog Kafka held)"
# Lag of readings ingested after the backlog drained (the histogram over the drain itself contains the backlog).
sleep 90
check "ingest_lag_p99_s_90s_after_recovery" "$(prom 'histogram_quantile(0.99, sum by (le) (rate(telemetry_ingest_lag_seconds_bucket{stage="durable"}[1m])))')" le 5
sleep 30
UNTIL=$(date -u -v-60S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '-60 sec' +%Y-%m-%dT%H:%M:%SZ)
read -r GEN MISSING <<<"$(drills/lib/probe-check.sh "$OUT" "$UNTIL")"
log "probe readings generated=$GEN missing=$MISSING"
check "probe_readings_missing" "$MISSING" eq 0
finish
