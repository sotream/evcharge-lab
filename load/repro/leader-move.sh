#!/usr/bin/env bash
# Isolated reproduction: one producer at 50 sends/s while leadership moves.
# 1) Create a private topic (12 partitions, RF 3) with preferred leaders spread over the brokers.
# 2) Start producing. 3) After 20 s, move leadership off one broker (unclean-free: elect the second replica as leader
#    by reassigning the replica order), then 20 s later run a PREFERRED election to move it back.
# Output: docs/load-output/leader-move-<UTC>.jsonl (one line per send) plus .events.log.
set -uo pipefail
cd "$(dirname "$0")/../.."
STAMP=$(date -u +%Y%m%dT%H%M%SZ); OUT=docs/load-output/leader-move-$STAMP; TOPIC=repro.leader-move.$STAMP
K="docker compose exec -T kafka-1 /opt/kafka/bin"
B=kafka-1:9092,kafka-2:9092,kafka-3:9092
log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$OUT.events.log"; }
$K/kafka-topics.sh --bootstrap-server $B --create --topic "$TOPIC" --partitions 12 --replication-factor 3 --config min.insync.replicas=2 >/dev/null
log "leaders at start: $($K/kafka-topics.sh --bootstrap-server $B --describe --topic "$TOPIC" | awk '/Partition:/{print $4"="$6}' | tr "\n" " ")"
log "topic $TOPIC created"
node load/repro/leader-move.mjs "$TOPIC" "${SECONDS_TOTAL:-80}" "$OUT.jsonl" &
P=$!
sleep 20
# Move leadership: reassign every partition so a different broker comes first, then elect preferred leaders.
python_free_json=$(node -e 'const t=process.argv[1];const p=[];for(let i=0;i<12;i++){const r=[1,2,3];const k=(i+1)%3;p.push({topic:t,partition:i,replicas:[r[k],r[(k+1)%3],r[(k+2)%3]]})}console.log(JSON.stringify({version:1,partitions:p}))' "$TOPIC")
echo "$python_free_json" | docker compose exec -T kafka-1 bash -c 'cat > /tmp/reassign.json'
log "reassign replica order"; $K/kafka-reassign-partitions.sh --bootstrap-server $B --reassignment-json-file /tmp/reassign.json --execute >/dev/null
sleep 5
log "PREFERRED election (leadership moves to the new first replica)"; $K/kafka-leader-election.sh --bootstrap-server $B --election-type PREFERRED --all-topic-partitions 2>&1 | tail -2 | tee -a "$OUT.events.log"
log "leaders now: $($K/kafka-topics.sh --bootstrap-server $B --describe --topic "$TOPIC" | awk '/Partition:/{print $4"="$6}' | tr "\n" " ")"
sleep 25
log "PREFERRED election on all topics (as auto.leader.rebalance does)"; $K/kafka-leader-election.sh --bootstrap-server $B --election-type PREFERRED --all-topic-partitions 2>&1 | tail -1 | tee -a "$OUT.events.log"
wait $P
$K/kafka-topics.sh --bootstrap-server $B --delete --topic "$TOPIC" >/dev/null || true
node -e 'const l=require("fs").readFileSync(process.argv[1],"utf8").trim().split("\n").map(JSON.parse);const ms=l.map(x=>x.ms).sort((a,b)=>a-b);const q=p=>ms[Math.min(ms.length-1,Math.floor(p*ms.length))];const s={sends:l.length,errors:l.filter(x=>!x.ok).length,p50:q(0.5),p99:q(0.99),max:ms.at(-1),over1s:l.filter(x=>x.ms>1000).length,errorKinds:[...new Set(l.filter(x=>!x.ok).map(x=>x.err))].slice(0,5)};require("fs").writeFileSync(process.argv[1].replace(/\.jsonl$/,".summary.json"),JSON.stringify(s,null,2)+"\n");console.log(JSON.stringify(s))' "$OUT.jsonl" | tee -a "$OUT.events.log"
