#!/usr/bin/env bash
# Shared helpers for failure drills. Each drill writes raw output to docs/drill-output/<drill>-<UTC>/:
#   timeline.jsonl  one Prometheus sample per second, labelled with the phase (before/during/after)
#   events.log      timestamped actions and checks, including the commands that were run
#   result.json     pass/fail per check with the measured value and the threshold
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
DRILL="${DRILL:?set DRILL}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="docs/drill-output/${DRILL}-${STAMP}"
mkdir -p "$OUT"
PHASE_FILE="$OUT/.phase"
echo before > "$PHASE_FILE"
SAMPLER_PID=""
CHECKS=()

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$OUT/events.log"; }
phase() { echo "$1" > "$PHASE_FILE"; log "PHASE $1"; }
run() { log "RUN $*"; "$@" 2>&1 | tee -a "$OUT/events.log"; }

start_sampler() { # $1 = queries json file
  cp "$1" "$OUT/queries.json"
  node drills/lib/sample.mjs "$OUT/queries.json" "$OUT/timeline.jsonl" "$PHASE_FILE" &
  SAMPLER_PID=$!
  log "sampler pid $SAMPLER_PID"
}
stop_sampler() { [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null || true; wait "$SAMPLER_PID" 2>/dev/null || true; }
trap stop_sampler EXIT

prom() { # instant query -> number (sum), or "null"
  curl -s --max-time 3 "http://localhost:29090/api/v1/query" --data-urlencode "query=$1" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{const r=JSON.parse(s).data.result;console.log(r.length?r.reduce((a,x)=>a+Number(x.value[1]),0):"null")}catch{console.log("null")}})'
}

check() { # name measured op threshold   (op: le, ge, eq)
  local name="$1" v="$2" op="$3" thr="$4" ok
  ok=$(node -e "const v=Number(process.argv[1]),t=Number(process.argv[3]);const o=process.argv[2];console.log(Number.isFinite(v)&&(o==='le'?v<=t:o==='ge'?v>=t:v===t))" "$v" "$op" "$thr")
  CHECKS+=("{\"check\":\"$name\",\"measured\":$( [ "$v" = null ] && echo null || echo "$v"),\"op\":\"$op\",\"threshold\":$thr,\"pass\":$ok}")
  log "CHECK $name measured=$v $op $thr pass=$ok"
}

finish() {
  stop_sampler
  local all=true
  for c in "${CHECKS[@]}"; do [[ "$c" == *'"pass":false'* ]] && all=false; done
  printf '{"drill":"%s","startedAt":"%s","pass":%s,"checks":[%s]}\n' "$DRILL" "$STAMP" "$all" "$(IFS=,; echo "${CHECKS[*]}")" > "$OUT/result.json"
  rm -f "$PHASE_FILE"
  log "RESULT pass=$all -> $OUT/result.json"
}

wait_until() { # timeout_s description command...
  local t="$1" what="$2"; shift 2
  local start=$SECONDS
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS - start > t )); then log "TIMEOUT waiting for $what"; return 1; fi
    sleep 1
  done
  log "OK $what after $((SECONDS - start)) s"
  echo $((SECONDS - start))
}
