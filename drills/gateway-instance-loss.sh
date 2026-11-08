#!/usr/bin/env bash
# Drill: kill one station-gateway instance (SIGKILL, no graceful close). ADR 0000 SLO: its stations are
# reconnected and routable within 60 s; ADR 0004: HAProxy marks it down, stations reconnect to the survivor.
set -euo pipefail
DRILL="gateway-instance-loss" source "$(dirname "$0")/lib/common.sh"
require_fleet
cat > "$OUT/q.json" <<'Q'
{
  "connected_gw1": "sum(gateway_connected_stations{gateway=\"gw-1\"})",
  "connected_gw2": "sum(gateway_connected_stations{gateway=\"gw-2\"})",
  "sim_connected": "sum(sim_connected_stations)",
  "sim_reconnects_per_s": "sum(rate(sim_reconnect_attempts_total[10s]))",
  "upgrades_rejected_per_s": "sum(rate(gateway_upgrades_total{outcome!=\"accepted\"}[10s]))",
  "callresult_p99_s": "histogram_quantile(0.99, sum by (le) (rate(ocpp_call_duration_seconds_bucket[30s])))"
}
Q
TOTAL=$(curl -s 127.0.0.1:28089/status | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).started))')
log "fleet size $TOTAL"
start_sampler "$OUT/q.json"
sleep "${BEFORE_S:-30}"
phase during
run docker compose kill -s SIGKILL station-gateway-1
T=$(wait_until 120 "all $TOTAL stations connected to gw-2" bash -c "[ \"\$(curl -s localhost:28088/metrics | awk '/^gateway_connected_stations/{print \$2}')\" = \"$TOTAL\" ]") || T=null
check "stations_reconnected_within_s" "$T" le 60
# Routable: every connected station has a route pointing at a live instance.
ROUTED=$(docker compose exec -T redis redis-cli --scan --pattern 'route:station:*' | wc -l | tr -d ' ')
check "routes_present_after_loss" "$ROUTED" ge "$TOTAL"
phase after
run docker compose start station-gateway-1
sleep "${AFTER_S:-60}"
check "fleet_connected_after_recovery" "$(prom 'sum(sim_connected_stations)')" ge "$TOTAL"
finish
