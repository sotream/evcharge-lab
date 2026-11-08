#!/usr/bin/env bash
# Drill: the Go allocator is down (ADR 0006). sessions falls back to safeShareW with strategy
# fallback-equal-share; the measured site power never exceeds the cap before, during or after; charging
# never stops; optimal allocation resumes when the allocator returns.
set -euo pipefail
DRILL="smart-charging-down" source "$(dirname "$0")/lib/common.sh"
DOWN_S="${DOWN_S:-120}"
cat > "$OUT/q.json" <<'Q'
{
  "site_drawn_w": "sum(sim_site_drawn_watts)",
  "site_cap_w": "sum(sim_site_cap_watts)",
  "max_site_utilisation": "max(sim_site_drawn_watts / on(site) sim_site_cap_watts)",
  "allocator_failures_per_s": "sum(rate(sessions_allocator_failures_total[10s]))",
  "rounds_optimal_per_s": "sum(rate(sessions_allocation_rounds_total{strategy=\"optimal\"}[30s]))",
  "rounds_fallback_per_s": "sum(rate(sessions_allocation_rounds_total{strategy=\"fallback-equal-share\"}[30s]))",
  "active_sessions": "sum(sim_active_sessions)",
  "safe_share_applied_total": "sum(gateway_safe_share_applied_total)"
}
Q
start_sampler "$OUT/q.json"
sleep "${BEFORE_S:-90}"
phase during
run docker compose stop smart-charging
sleep "$DOWN_S"
check "fallback_rounds_seen" "$(prom 'sum(increase(sessions_allocation_rounds_total{strategy="fallback-equal-share"}[2m]))')" ge 1
check "sessions_still_charging" "$(prom 'sum(sim_active_sessions)')" ge 1
phase after
run docker compose start smart-charging
sleep "${AFTER_S:-120}"
check "optimal_rounds_resumed" "$(prom 'sum(increase(sessions_allocation_rounds_total{strategy="optimal"}[1m]))')" ge 1
MAXU=$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").trim().split("\n").map(JSON.parse);console.log(Math.max(...l.map(x=>x.max_site_utilisation??0)))' "$OUT/timeline.jsonl")
check "max_site_utilisation_over_drill" "$MAXU" le 1
finish
