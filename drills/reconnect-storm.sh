#!/usr/bin/env bash
# Drill: whole fleet disconnects at once and reconnects per the ADR 0003 policy (first attempt U(0,10 s),
# then full-jitter backoff). Admission control must shed load (503 + Retry-After) without failing over,
# and the fleet must be back within 60 s (ADR 0000 gateway SLO).
set -euo pipefail
DRILL="reconnect-storm" source "$(dirname "$0")/lib/common.sh"
cat > "$OUT/q.json" <<'Q'
{
  "sim_connected": "sum(sim_connected_stations)",
  "upgrades_accepted_per_s": "sum(rate(gateway_upgrades_total{outcome=\"accepted\"}[5s]))",
  "upgrades_rejected_per_s": "sum(rate(gateway_upgrades_total{outcome!=\"accepted\"}[5s]))",
  "credential_lookups_per_s": "sum by () (rate(gateway_credential_lookups_total[5s]))",
  "boot_per_s": "sum(rate(ocpp_messages_total{direction=\"in\",type=\"call\",action=\"BootNotification\"}[5s]))",
  "callresult_p99_s": "histogram_quantile(0.99, sum by (le) (rate(ocpp_call_duration_seconds_bucket[30s])))"
}
Q
TOTAL=$(curl -s 127.0.0.1:28089/status | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).started))')
start_sampler "$OUT/q.json"
sleep "${BEFORE_S:-20}"
phase during
log "storm: $(curl -s -X POST 127.0.0.1:28089/storm -H 'content-type: application/json' -d '{}')"
T=$(wait_until 180 "all $TOTAL stations reconnected" bash -c "[ \"\$(curl -s 127.0.0.1:28089/status | node -e 'let s=\"\";process.stdin.on(\"data\",d=>s+=d).on(\"end\",()=>console.log(JSON.parse(s).connected))')\" = \"$TOTAL\" ]") || T=null
check "fleet_reconnected_within_s" "$T" le 60
phase after
sleep "${AFTER_S:-30}"
check "gateways_ready_after_storm" "$(( $(curl -s -o /dev/null -w '%{http_code}' localhost:28087/health/ready) + $(curl -s -o /dev/null -w '%{http_code}' localhost:28088/health/ready) ))" eq 400
finish
