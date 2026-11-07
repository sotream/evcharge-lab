#!/usr/bin/env bash
# Creates topics from packages/contracts/src/topics.ts TOPIC_SPECS (kept in sync by scripts/check-topics.mjs).
# RF=3, min.insync.replicas=2: one broker may be lost with acks=all producers and no data loss (ADR 0009).
set -euo pipefail
B=kafka-1:9092,kafka-2:9092,kafka-3:9092
K=/opt/kafka/bin/kafka-topics.sh
create() {
  $K --bootstrap-server "$B" --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 3 \
     --config min.insync.replicas=2 --config retention.ms="$3" --config compression.type=producer
}
create station.events.v1 12 604800000
create telemetry.meter-values.v1 24 86400000
create station.commands.v1 12 3600000
create station.command-results.v1 12 86400000
create session.events.v1 12 604800000
create billing.events.v1 6 604800000
create ocpp.raw-log.v1 12 1209600000
# Dead-letter topics for every consumed topic (ADR 0002): 3 partitions, 14d retention.
for t in station.events.v1 telemetry.meter-values.v1 station.commands.v1 station.command-results.v1 session.events.v1 billing.events.v1; do
  $K --bootstrap-server "$B" --create --if-not-exists --topic "$t.dlq" --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=1209600000
done
$K --bootstrap-server "$B" --list
