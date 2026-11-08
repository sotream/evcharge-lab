#!/usr/bin/env bash
# Runs one k6 script from THIS machine against the compose stack on another machine, via published host ports.
#   TARGET=m4.local load/run-remote.sh tariff [k6 args...]   e.g. ... tariff -e DURATION=30s
# Results go to docs/load-output/remote/<name>-<UTC stamp>/ only (never the local k6-<name>.* series).
set -euo pipefail
cd "$(dirname "$0")/.."
NAME="${1:?usage: TARGET=host load/run-remote.sh <test-name> [k6 args]}"; shift
: "${TARGET:?set TARGET to the hostname or IP of the machine running the stack}"
[ -f "load/k6/$NAME.js" ] || { echo "no such script: load/k6/$NAME.js" >&2; exit 1; }

# Each script reads one base URL from -e URL; its default points at localhost, so URL is always overridden here.
case "$NAME" in
  driver-api-reads) PORT=28082; URL="http://$TARGET:$PORT" ;;
  gateway-ws)       PORT=28087; URL="ws://$TARGET:$PORT/ocpp" ;;
  reservation-contention|sessions-ocpp) PORT=28081; URL="http://$TARGET:$PORT" ;;
  smart-charging)   PORT=28084; URL="http://$TARGET:$PORT" ;;
  tariff)           PORT=28085; URL="http://$TARGET:$PORT" ;;
  *) echo "unknown test: $NAME" >&2; exit 1 ;;
esac
PORTS=("$PORT")

bad=()
for p in "${PORTS[@]}"; do
  nc -z -w 2 "$TARGET" "$p" >/dev/null 2>&1 || bad+=("$p")
done
if [ "${#bad[@]}" -gt 0 ]; then echo "unreachable on $TARGET: ${bad[*]}" >&2; exit 2; fi

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="docs/load-output/remote/$NAME-$STAMP"
mkdir -p "$OUT"
ping -c 5 "$TARGET" 2>&1 | tail -2 > "$OUT/ping.txt" || true

# Scripts write their summary to ../../docs/load-output relative to the run dir. Run in a scratch copy so that
# path lands in the scratch dir, not in the local series, then move it into $OUT.
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
cp -R load "$WORK/load"; mkdir -p "$WORK/docs/load-output"

if command -v k6 >/dev/null 2>&1; then
  K6VER=$(k6 version | head -1); HOW="local k6 binary"
  echo "$K6VER"
  (cd "$WORK/load/k6" && k6 run -q -e "URL=$URL" "$@" "$NAME.js") | tee "$OUT/stdout.txt" || true
else
  K6VER="grafana/k6:2.3.0"; HOW="docker run grafana/k6:2.3.0"
  docker run --rm -v "$WORK":/repo -w /repo/load/k6 grafana/k6:2.3.0 run -q -e "URL=$URL" "$@" "$NAME.js" | tee "$OUT/stdout.txt" || true
fi
mv "$WORK"/docs/load-output/*.json "$OUT/" 2>/dev/null || echo "warning: no k6 summary JSON produced" >&2

cat > "$OUT/env.json" <<JSON
{
  "script": "load/k6/$NAME.js",
  "measuredAt": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "generator": "remote",
  "generatorMachine": {
    "chip": "$(sysctl -n machdep.cpu.brand_string)", "model": "$(sysctl -n hw.model)", "cpus": $(sysctl -n hw.ncpu), "memBytes": $(sysctl -n hw.memsize),
    "macOS": "$(sw_vers -productVersion)", "k6": "$K6VER", "k6RunVia": "$HOW"
  },
  "target": { "host": "$TARGET", "ports": [$(IFS=,; echo "${PORTS[*]}")], "url": "$URL" },
  "note": "includes the network hop; never plot in the same series as local runs"
}
JSON
echo "results: $OUT"
