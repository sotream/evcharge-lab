#!/usr/bin/env bash
# Drill: consumer-group rebalance under load, and idempotency on redelivery (ADR 0012).
#
# Two instances of the telemetry-ingest consumer group (telemetry-ingest.meter-values, 24 partitions) read the meter-value topic
# under load. One instance is killed with SIGKILL (no clean group leave, uncommitted batch is redelivered), then started again.
# Measured: how long the survivor takes to own all partitions, the lag peak, the time to drain, and that nothing is lost or
# duplicated in TimescaleDB. A SIGKILL does not necessarily redeliver anything (offsets are committed after the flush transaction commits), so
# a second, forced phase proves idempotency: the whole group is stopped, its committed offsets are moved back by REPLAY_SHIFT
# records per partition, the group is started again, and the unique index must suppress every re-read record.
#
# The second instance is started with `docker run` from the same image and environment as the compose service, on the compose
# network, with host port $INSTANCE2_PORT (default 28096) so that its metrics can be read. The simulator is recreated with a
# shorter meter interval (METER_INTERVAL_S, default 2) to put real volume through the topic, and put back at the end.
#
# Env: REPLAY_SHIFT (200 records per partition), METER_INTERVAL_S (2), INSTANCE2_PORT (28096), BEFORE_S (45), DOWN_S (45), AFTER_S (60), DRAIN_MAX_S (180), REBALANCE_MAX_S (60).
set -euo pipefail
DRILL="consumer-rebalance" source "$(dirname "$0")/lib/common.sh"
REPLAY_SHIFT="${REPLAY_SHIFT:-200}"
METER_INTERVAL_S="${METER_INTERVAL_S:-2}"
INSTANCE2_PORT="${INSTANCE2_PORT:-28096}"
BEFORE_S="${BEFORE_S:-45}"
DOWN_S="${DOWN_S:-45}"
AFTER_S="${AFTER_S:-60}"
DRAIN_MAX_S="${DRAIN_MAX_S:-180}"
REBALANCE_MAX_S="${REBALANCE_MAX_S:-60}"
GROUP=telemetry-ingest.meter-values
I1=$(docker compose ps -q telemetry-ingest)
NET=$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$I1")
IMAGE=$(docker inspect -f '{{.Config.Image}}' "$I1")
I2=evcharge-telemetry-ingest-2
now_ms() { node -e 'console.log(Date.now())'; }

SAMPLER2_PID=""
cleanup() {
  [ -n "$SAMPLER2_PID" ] && kill "$SAMPLER2_PID" 2>/dev/null || true
  [ -n "${GRPSAMPLER_PID:-}" ] && kill "$GRPSAMPLER_PID" 2>/dev/null || true
  docker rm -f "$I2" >/dev/null 2>&1 || true
}
trap 'cleanup; stop_sampler' EXIT
docker rm -f "$I2" >/dev/null 2>&1 || true

require_fleet
# --- load: shorter meter interval -------------------------------------------------------------------------------------
if [ "$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$(docker compose ps -q simulator)" | grep -c "^SIM_METER_INTERVAL_S=$METER_INTERVAL_S$")" = 0 ]; then
  log "RUN recreate the simulator with SIM_METER_INTERVAL_S=$METER_INTERVAL_S"
  SIM_METER_INTERVAL_S="$METER_INTERVAL_S" docker compose up -d simulator >/dev/null 2>&1
  sleep 5
  wait_until 180 "fleet reconnected" bash -c 'curl -s 127.0.0.1:28089/status | node -e "let s=\"\";process.stdin.on(\"data\",d=>s+=d).on(\"end\",()=>{try{const j=JSON.parse(s);process.exit(j.started>0&&j.connected===j.started?0:1)}catch{process.exit(1)}})"' >/dev/null
  RESTORE_SIM=1
else
  RESTORE_SIM=0
fi
TOTAL=$(curl -s --max-time 5 127.0.0.1:28089/status | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).started))')

# --- second consumer instance ------------------------------------------------------------------------------------------
ENVFILE=$(mktemp) # environment of the compose instance; not kept in the drill output
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$I1" | grep -v '^$' | grep -E '^(SERVICE_NAME|OTEL_|SHUTDOWN_GRACE|LOG_LEVEL|HTTP_PORT|INTERNAL_TOKEN|KAFKA_BROKERS|REDIS_URL|DATABASE_URL)=' > "$ENVFILE"
log "RUN start the second instance ($IMAGE) on $NET, host port $INSTANCE2_PORT"
docker run -d --name "$I2" --network "$NET" --env-file "$ENVFILE" -p "$INSTANCE2_PORT:8080" "$IMAGE" >/dev/null
wait_until 60 "second instance serving /metrics" bash -c "curl -sf localhost:$INSTANCE2_PORT/metrics >/dev/null" >/dev/null

cat > "$OUT/q.json" <<Q
{
  "sim_connected": "sum(sim_connected_stations)",
  "group_lag_total": "sum(kafka_consumergroup_lag{consumergroup=\"$GROUP\"})",
  "group_members": "max(kafka_consumergroup_members{consumergroup=\"$GROUP\"})",
  "meter_values_in_per_s": "sum(rate(ocpp_messages_total{direction=\"in\",type=\"call\",action=\"MeterValues\"}[10s]))",
  "ingest_lag_p99_s": "histogram_quantile(0.99, sum by (le) (rate(telemetry_ingest_lag_seconds_bucket[30s])))"
}
Q
start_sampler "$OUT/q.json"
node drills/lib/rebalance-sample.mjs "$OUT/instances.jsonl" "$PHASE_FILE" "i1=http://localhost:28086" "i2=http://localhost:$INSTANCE2_PORT" 2>>"$OUT/events.log" &
SAMPLER2_PID=$!
# Ground truth for "who owns which partition": ask the group coordinator (the lag series above can trail the assignment).
node drills/lib/group-sample.mjs "$GROUP" "$OUT/group.jsonl" 2>>"$OUT/events.log" &
GRPSAMPLER_PID=$!
log "probes: $(curl -s -X POST 127.0.0.1:28089/probes -H 'content-type: application/json' -d '{"count":10}')"

balanced() { # the coordinator lists 2 members that together own all 24 partitions
  node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").split("\n").filter(Boolean).map(JSON.parse);const x=l[l.length-1];process.exit(x&&x.members===2&&x.assigned===24?0:1)' "$OUT/group.jsonl" "0"
}
wait_until 90 "both instances own partitions (24 in total)" balanced >/dev/null || { log "ABORT: the group did not split its 24 partitions over two instances"; exit 3; }
sleep "$BEFORE_S"
check "fleet_connected_before" "$(prom 'sum(sim_connected_stations)')" ge "$TOTAL"
check "both_instances_balanced_before" "$(balanced && echo 1 || echo 0)" eq 1
lastv() { node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").split("\n").filter(Boolean).map(JSON.parse);const x=[...l].reverse().find(r=>r.c===process.argv[2]&&r.up);console.log(x?x[process.argv[3]]:"null")' "$OUT/instances.jsonl" "$1" "$2"; }
DEDUP1_BASE=$(lastv i1 dedup)
ROWS_BEFORE=$(docker compose exec -T postgres-telemetry psql -qtA -U evcharge -d telemetry -c "SELECT count(*) FROM meter_readings" | tr -d ' ')
log "baseline: instance 1 dedup=$DEDUP1_BASE; meter_readings rows=$ROWS_BEFORE; group partitions i1=$(lastv i1 partitions) i2=$(lastv i2 partitions)"

# --- kill one instance -------------------------------------------------------------------------------------------------
phase during
log "RUN docker kill $I2   (SIGKILL: no group leave, its last uncommitted batch is redelivered)"
docker kill "$I2" >/dev/null
T_KILL=$(now_ms)
survivor_owns_all() { node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").split("\n").filter(Boolean).map(JSON.parse);const x=l[l.length-1];process.exit(x&&x.ms>Number(process.argv[2])&&x.members===1&&x.assigned===24?0:1)' "$OUT/group.jsonl" "$T_KILL"; }
REB1=$(wait_until 120 "instance 1 owns all 24 partitions" survivor_owns_all) || REB1=null
sleep "$DOWN_S"

phase after
log "RUN docker start $I2"
docker start "$I2" >/dev/null
T_START=$(now_ms)
rebalanced_back() { node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").split("\n").filter(Boolean).map(JSON.parse);const x=l[l.length-1];process.exit(x&&x.ms>Number(process.argv[2])&&x.members===2&&x.assigned===24?0:1)' "$OUT/group.jsonl" "$T_START"; }
REB2=$(wait_until 120 "both instances own partitions again" rebalanced_back) || REB2=null
DRAIN_T0=$SECONDS
drained() { v=$(prom "sum(kafka_consumergroup_lag{consumergroup=\"$GROUP\"})"); [ "$v" != null ] && awk -v v="$v" 'BEGIN{exit !(v<100)}'; }
DRAIN=$(wait_until "$DRAIN_MAX_S" "group lag below 100 records" drained) || DRAIN=null
sleep "$AFTER_S"

# --- forced redelivery ------------------------------------------------------------------------------------------------
phase replay
ROWS_PRE_REPLAY=$(docker compose exec -T postgres-telemetry psql -qtA -U evcharge -d telemetry -c "SELECT count(*) FROM meter_readings" | tr -d ' ')
log "RUN stop the whole consumer group, then move its committed offsets back by $REPLAY_SHIFT per partition"
# Graceful stops (-t 30 > SHUTDOWN_GRACE 15 s) so both members LEAVE the group; a killed member keeps the group Stable until its session times out.
docker compose stop -t 30 telemetry-ingest >/dev/null 2>&1
docker stop -t 30 "$I2" >/dev/null 2>&1 || true
group_empty() { docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --state --group "$GROUP" 2>/dev/null | grep -q Empty; }
wait_until 150 "consumer group is Empty" group_empty >/dev/null || log "group still not Empty, the offset reset will fail"
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --group "$GROUP" --topic telemetry.meter-values.v1 --reset-offsets --shift-by "-$REPLAY_SHIFT" --execute > "$OUT/reset-offsets.txt" 2>&1 || log "reset-offsets failed, see reset-offsets.txt"
tail -n 3 "$OUT/reset-offsets.txt" | while read -r l; do log "reset-offsets: $l"; done
log "RUN start both instances again (fresh processes: their counters start at 0)"
docker compose start telemetry-ingest >/dev/null 2>&1
docker start "$I2" >/dev/null
T_REPLAY=$(now_ms)
balanced_after_replay() { node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").split("\n").filter(Boolean).map(JSON.parse);const x=l[l.length-1];process.exit(x&&x.ms>Number(process.argv[2])&&x.members===2&&x.assigned===24?0:1)' "$OUT/group.jsonl" "$T_REPLAY"; }
wait_until 180 "both instances own partitions after the replay start" balanced_after_replay >/dev/null || log "group did not rebalance after the replay start"
REPLAY_DRAIN=$(wait_until "$DRAIN_MAX_S" "group lag below 100 records after the replay" drained) || REPLAY_DRAIN=null
sleep 20
REPLAY_DEDUP=$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").split("\n").filter(Boolean).map(JSON.parse);const f=(c)=>{const x=[...l].reverse().find(r=>r.c===c&&r.up&&r.ms>Number(process.argv[2]));return x?x.dedup:0};console.log(f("i1")+f("i2"))' "$OUT/instances.jsonl" "$T_REPLAY")
REPLAY_ROWS=$(node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").split("\n").filter(Boolean).map(JSON.parse);const f=(c)=>{const x=[...l].reverse().find(r=>r.c===c&&r.up&&r.ms>Number(process.argv[2]));return x?x.rows:0};console.log(f("i1")+f("i2"))' "$OUT/instances.jsonl" "$T_REPLAY")
ROWS_POST_REPLAY=$(docker compose exec -T postgres-telemetry psql -qtA -U evcharge -d telemetry -c "SELECT count(*) FROM meter_readings" | tr -d ' ')
log "forced redelivery: records re-read and suppressed by the unique index = $REPLAY_DEDUP; rows newly inserted by the restarted instances = $REPLAY_ROWS; meter_readings rows $ROWS_PRE_REPLAY -> $ROWS_POST_REPLAY"

kill "$SAMPLER2_PID" 2>/dev/null || true; SAMPLER2_PID=""
log "probe check and uniqueness check"
UNTIL=$(date -u -v-60S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '-60 sec' +%Y-%m-%dT%H:%M:%SZ)
read -r GEN MISSING <<<"$(drills/lib/probe-check.sh "$OUT" "$UNTIL")"
log "probe readings generated=$GEN missing_in_timescale=$MISSING (until $UNTIL)"
DUP=$(docker compose exec -T postgres-telemetry psql -qtA -U evcharge -d telemetry -c "SELECT count(*) FROM (SELECT station_id, connector_id, time FROM meter_readings GROUP BY 1,2,3 HAVING count(*) > 1) d" | tr -d ' ')
ROWS_AFTER=$(docker compose exec -T postgres-telemetry psql -qtA -U evcharge -d telemetry -c "SELECT count(*) FROM meter_readings" | tr -d ' ')

# --- numbers from the raw samples ----------------------------------------------------------------------------------------
REPLAY_SHIFT="$REPLAY_SHIFT" REPLAY_DEDUP_V="$REPLAY_DEDUP" REPLAY_ROWS_V="$REPLAY_ROWS" ROWS_PRE_V="$ROWS_PRE_REPLAY" ROWS_POST_V="$ROWS_POST_REPLAY" node -e '
  const fs = require("fs");
  const [dir, tKill, tStart, base1, rowsBefore, rowsAfter] = process.argv.slice(1);
  const inst = fs.readFileSync(dir + "/instances.jsonl", "utf8").split("\n").filter(Boolean).map(JSON.parse);
  const tl = fs.readFileSync(dir + "/timeline.jsonl", "utf8").split("\n").filter(Boolean).map(JSON.parse);
  const i1 = inst.filter((r) => r.c === "i1" && r.up), i2 = inst.filter((r) => r.c === "i2" && r.up);
  // instance 2 has two incarnations: the counters restart at the kill. Split where the cumulative counter goes down or the gap is > 5 s.
  const cut = Number(tStart);
  const i2a = i2.filter((r) => r.ms < Number(tKill)), i2b = i2.filter((r) => r.ms >= cut);
  const lastOf = (a, k) => (a.length ? a[a.length - 1][k] : 0);
  const dedup = { instance1: lastOf(i1, "dedup") - Number(base1), instance2BeforeKill: lastOf(i2a, "dedup"), instance2AfterRestart: lastOf(i2b, "dedup") };
  const rows = { instance1: lastOf(i1, "rows"), instance2BeforeKill: lastOf(i2a, "rows"), instance2AfterRestart: lastOf(i2b, "rows") };
  const lag = tl.filter((r) => r.group_lag_total != null && r.phase !== "replay");
  const peak = lag.reduce((m, r) => (r.group_lag_total > m.v ? { v: r.group_lag_total, t: r.t } : m), { v: 0, t: null });
  const baseLag = lag.filter((r) => r.phase === "before").map((r) => r.group_lag_total);
  const s = {
    // "partitions" is the number of partitions for which the instance exports a lag series, so it can trail the real assignment by a commit.
    groupMembersAssignedTimeline: (() => { const g = fs.readFileSync(dir + "/group.jsonl", "utf8").split("\n").filter(Boolean).map(JSON.parse); const o = []; for (const r of g) { const k = r.members + "m/" + r.assigned + "p"; if (!o.length || o[o.length - 1].s !== k) o.push({ t: new Date(r.ms).toISOString(), msAfterKill: r.ms - Number(tKill), s: k }); } return o; })(),
    survivorFirstGainsPartitionsS: (() => { const x = i1.find((r) => r.ms > Number(tKill) && r.partitions > 12); return x ? (x.ms - Number(tKill)) / 1000 : null; })(),
    survivorReports24PartitionsS: (() => { const x = i1.find((r) => r.ms > Number(tKill) && r.partitions === 24); return x ? (x.ms - Number(tKill)) / 1000 : null; })(),
    survivorPartitionsAfterKill: i1.filter((r) => r.ms > Number(tKill) && r.ms < Number(tStart)).map((r) => r.partitions).filter((v, i, a) => i === 0 || v !== a[i - 1]),
    lagPeakRecords: peak.v, lagPeakAt: peak.t, lagBeforeMaxRecords: baseLag.length ? Math.max(...baseLag) : null,
    dedupSuppressedRedeliveries: dedup, rowsInsertedByInstance: rows,
    rebalanceDropsInstance1: lastOf(i1, "rebalanceDrops"),
    meterReadingsRowsBefore: Number(rowsBefore), meterReadingsRowsAfter: Number(rowsAfter),
    forcedRedelivery: { shiftPerPartition: Number(process.env.REPLAY_SHIFT), suppressed: Number(process.env.REPLAY_DEDUP_V), newRowsInserted: Number(process.env.REPLAY_ROWS_V), rowsBefore: Number(process.env.ROWS_PRE_V), rowsAfter: Number(process.env.ROWS_POST_V) },
  };
  fs.writeFileSync(dir + "/summary.json", JSON.stringify(s, null, 2) + "\n");
  console.log(JSON.stringify(s));
' "$OUT" "$T_KILL" "$T_START" "$DEDUP1_BASE" "$ROWS_BEFORE" "$ROWS_AFTER" >&2

SUPPRESSED=$(node -e 'const s=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).dedupSuppressedRedeliveries;console.log(s.instance1+s.instance2BeforeKill+s.instance2AfterRestart)' "$OUT/summary.json")
LAGPEAK=$(node -e 'console.log(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).lagPeakRecords)' "$OUT/summary.json")
log "rebalance after the kill: ${REB1}s; both instances own partitions again ${REB2}s after the restart; lag peak $LAGPEAK records; drained ${DRAIN}s after the restart; redelivered records suppressed by the unique index: $SUPPRESSED"
check "rebalance_after_kill_s" "$REB1" le "$REBALANCE_MAX_S"
check "rebalance_after_restart_s" "$REB2" le "$REBALANCE_MAX_S"
check "lag_drained_s" "$DRAIN" le "$DRAIN_MAX_S"
check "probe_readings_generated" "$GEN" ge 1
check "probe_readings_missing" "$MISSING" eq 0
check "duplicate_rows" "$DUP" eq 0
log "records redelivered by the SIGKILL and suppressed (informational, not a check: may legitimately be 0): $SUPPRESSED"
check "forced_redelivery_records_suppressed" "$REPLAY_DEDUP" ge 1
check "forced_redelivery_lag_drained_s" "$REPLAY_DRAIN" le "$DRAIN_MAX_S"
check "fleet_connected_after" "$(prom 'sum(sim_connected_stations)')" ge "$TOTAL"
node drills/lib/rebalance-report.mjs "$OUT" >&2 || log "rebalance-report failed"
finish
if [ "$RESTORE_SIM" = 1 ]; then
  log "RUN recreate the simulator with the default meter interval"
  docker compose up -d simulator >/dev/null 2>&1
fi
