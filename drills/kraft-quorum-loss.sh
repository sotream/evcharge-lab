#!/usr/bin/env bash
# Drill: lose the KRaft controller quorum (2 of 3 combined broker/controller nodes) under simulator load (ADR 0016).
# Stopping kafka-2 and kafka-3 loses the metadata quorum and two of three replicas at once. Expected:
#   - metadata changes stop, no leader election, produce with acks=all and min.insync.replicas=2 fails;
#   - the gateway stays up and buffers (ADR 0012), stations stay connected, nobody is told to stop charging;
#   - after the nodes return, a quorum leader appears, produce works, and no acknowledged reading is lost.
set -euo pipefail
DRILL=kraft-quorum-loss source "$(dirname "$0")/lib/common.sh"
require_fleet
DOWN_S="${DOWN_S:-60}"
KB=/opt/kafka/bin
BOOT=kafka-1:9092
PROBE_TOPIC=drill.kraft-probe
TOTAL=$(curl -s --max-time 5 127.0.0.1:28089/status | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).started))')
cat > "$OUT/q.json" <<'Q'
{
  "sim_connected": "sum(sim_connected_stations)",
  "sim_active_sessions": "sum(sim_active_sessions)",
  "remote_stop_total": "sum(gateway_commands_total{action=\"RemoteStopTransaction\"})",
  "meter_values_in_per_s": "sum(rate(ocpp_messages_total{direction=\"in\",type=\"call\",action=\"MeterValues\"}[10s]))",
  "produce_errors_per_s": "sum(rate(gateway_produce_errors_total[10s]))",
  "produce_rejected_per_s": "sum(rate(gateway_produce_rejected_total[10s]))",
  "produce_buffer_depth": "sum(gateway_produce_buffer_depth)",
  "rows_ingested_per_s": "sum(rate(telemetry_rows_ingested_total[10s]))",
  "ingest_lag_p99_s": "histogram_quantile(0.99, sum by (le) (rate(telemetry_ingest_lag_seconds_bucket[30s])))",
  "callresult_p99_s": "histogram_quantile(0.99, sum by (le) (rate(ocpp_call_duration_seconds_bucket[30s])))",
  "outbox_pending": "sum(outbox_pending)"
}
Q

kexec() { docker compose exec -T kafka-1 "$@"; }
# Raw quorum state, saved verbatim. Bounded by `timeout`, because a node without a quorum can hang.
quorum_raw() { # $1 = label
  {
    echo "# $(date -u +%FT%TZ) describe --status"
    kexec timeout 20 $KB/kafka-metadata-quorum.sh --bootstrap-server $BOOT describe --status 2>&1 || echo "(exit $?)"
    echo "# $(date -u +%FT%TZ) describe --replication"
    kexec timeout 20 $KB/kafka-metadata-quorum.sh --bootstrap-server $BOOT describe --replication 2>&1 || echo "(exit $?)"
  } > "$OUT/quorum-$1.txt"
}
# Quorum is back when a leader is elected and all 3 voters are listed with a lag of at most CAUGHT_UP_LAG records.
quorum_ok() {
  local st rep
  st=$(kexec timeout 20 $KB/kafka-metadata-quorum.sh --bootstrap-server $BOOT describe --status) || return 1
  rep=$(kexec timeout 20 $KB/kafka-metadata-quorum.sh --bootstrap-server $BOOT describe --replication) || return 1
  echo "$st" | grep -Eq '^LeaderId:[[:space:]]*[0-9]+' || return 1
  # Voter rows: NodeId DirectoryId LogEndOffset Lag LastFetch LastCaughtUp Status, so Lag is NF-3.
  echo "$rep" | awk -v max="${CAUGHT_UP_LAG:-5}" '$1 ~ /^[0-9]+$/ && ($NF=="Leader"||$NF=="Follower") { n++; if ($(NF-3) > max) bad++ } END { exit !(n==3 && bad==0) }'
}
produce_once() { # acks=all, one record, bounded; exit 0 only when the broker acknowledged
  kexec timeout 25 $KB/kafka-producer-perf-test.sh --topic $PROBE_TOPIC --num-records 1 --record-size 100 --throughput -1 \
    --producer-props bootstrap.servers=$BOOT acks=all request.timeout.ms=5000 delivery.timeout.ms=8000 max.block.ms=5000
}
errs_zero() {
  local v; v=$(prom 'sum(rate(gateway_produce_errors_total[15s])) + sum(rate(gateway_produce_rejected_total[15s]))')
  [ "$v" = null ] || awk -v v="$v" 'BEGIN{exit !(v<0.001)}'
}

# Setup: a private topic for the produce probe, created while the quorum is healthy (auto-create is off, ADR 0012).
run docker compose exec -T kafka-1 $KB/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --create --if-not-exists --topic $PROBE_TOPIC --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=3600000
quorum_raw before
log "probes: $(curl -s -X POST 127.0.0.1:28089/probes -H 'content-type: application/json' -d '{"count":10}')"
start_sampler "$OUT/q.json"
sleep "${BEFORE_S:-60}"
phase during
run docker compose stop kafka-2 kafka-3
sleep 10 # let the remaining node notice the loss
quorum_raw during
# Baseline taken only now, when the quorum is confirmed lost: `docker compose stop` takes seconds, and simulated drivers stop
# sessions at ~0.7/s, so a baseline taken before it counted two ordinary driver stops that arrived while the nodes were going down
# (first run, docs/drill-output/kraft-quorum-loss-20261108T184440Z).
STOPS_BEFORE=$(prom 'sum(gateway_commands_total{action="RemoteStopTransaction"})')
# A metadata change without a quorum must fail or time out.
if run docker compose exec -T kafka-1 timeout 25 $KB/kafka-topics.sh --bootstrap-server $BOOT --create --topic drill.kraft-denied --partitions 1 --replication-factor 1 > "$OUT/create-topic-during.txt" 2>&1; then
  CREATE_FAILED=0
else
  CREATE_FAILED=1
fi
log "create topic during outage failed=$CREATE_FAILED: $(tail -n 3 "$OUT/create-topic-during.txt" | tr '\n' ' ')"
# Produce with acks=all and min.insync=2 should not be acknowledged. Logged only, not a check: ADR 0012 notes the error code is hard to provoke.
if produce_once > "$OUT/produce-during.txt" 2>&1; then PRODUCE_DURING=1; else PRODUCE_DURING=0; fi
log "produce acknowledged during outage=$PRODUCE_DURING (see produce-during.txt)"
sleep "$DOWN_S"
check "gateway_live_during_outage" "$(curl -s --max-time 3 -o /dev/null -w '%{http_code}' localhost:28087/health/live)" eq 200
check "gateway_ready_during_outage" "$(curl -s --max-time 3 -o /dev/null -w '%{http_code}' localhost:28087/health/ready)" eq 200
check "fleet_connected_during_outage" "$(prom 'sum(sim_connected_stations)')" ge "$TOTAL"
check "topic_create_failed_during_outage" "$CREATE_FAILED" eq 1
STOPS_DURING=$(prom 'sum(gateway_commands_total{action="RemoteStopTransaction"})')
STOP_DELTA=$(node -e 'const a=Number(process.argv[1]),b=Number(process.argv[2]);console.log(Number.isFinite(a)&&Number.isFinite(b)?b-a:"null")' "$STOPS_BEFORE" "$STOPS_DURING")
check "remote_stop_sent_during_outage" "$STOP_DELTA" eq 0

phase after
T0=$SECONDS
run docker compose start kafka-2 kafka-3
T_QUORUM=$(wait_until 300 "KRaft leader elected and all voters caught up" quorum_ok) || T_QUORUM=null
quorum_raw after
check "time_to_quorum_s" "$T_QUORUM" le "${QUORUM_MAX_S:-60}"
if wait_until 300 "first acknowledged produce (acks=all)" produce_once >/dev/null; then T_PRODUCE=$((SECONDS - T0)); else T_PRODUCE=null; fi
check "time_to_first_produce_s" "$T_PRODUCE" le "${PRODUCE_MAX_S:-90}"
if wait_until 300 "gateway produce errors back to zero" errs_zero >/dev/null; then T_ERRS=$((SECONDS - T0)); else T_ERRS=null; fi
check "time_to_zero_produce_errors_s" "$T_ERRS" le "${ERRORS_MAX_S:-120}"
sleep "${AFTER_S:-90}"

# From the drill's own timeline: the outage must show up as produce errors, and charging must have continued.
ERRS=$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").trim().split("\n").map(JSON.parse).filter(x=>x.phase==="during");console.log(Math.max(0,...l.map(x=>(x.produce_errors_per_s??0)+(x.produce_rejected_per_s??0))))' "$OUT/timeline.jsonl")
MIN_ACTIVE=$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").trim().split("\n").map(JSON.parse).filter(x=>x.phase==="during"&&x.sim_active_sessions!=null);console.log(l.length?Math.min(...l.map(x=>x.sim_active_sessions)):"null")' "$OUT/timeline.jsonl")
check "max_produce_errors_per_s_during_outage" "$ERRS" ge 0.01
check "min_active_sessions_during_outage" "$MIN_ACTIVE" ge 1
UNTIL=$(date -u -v-60S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '-60 sec' +%Y-%m-%dT%H:%M:%SZ)
read -r GEN MISSING <<<"$(drills/lib/probe-check.sh "$OUT" "$UNTIL")"
log "probe readings generated=$GEN missing_in_timescale=$MISSING (until $UNTIL)"
check "probe_readings_generated" "$GEN" ge 1
check "probe_readings_missing" "$MISSING" eq 0
docker compose exec -T kafka-1 $KB/kafka-topics.sh --bootstrap-server $BOOT --delete --topic $PROBE_TOPIC >/dev/null 2>&1 || true
finish
