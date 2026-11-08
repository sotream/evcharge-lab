# Leader-move reproduction (load/repro/leader-move.sh)

One producer sends 50 acks=all records/s through the platform `KafkaProducer` (the same client stack as the gateway)
to a private 12-partition topic (RF 3). Partition leadership is moved twice: a replica-order reassignment followed by a
PREFERRED election, then a cluster-wide PREFERRED election, as `auto.leader.rebalance` does. Each send is one line in
the `.jsonl` file; `.summary.json` is computed from it; `.events.log` records the steps and the leaders before and after.

| Run | Producer client settings | Result (summary.json) |
|---|---|---|
| 20261108T155240Z | @platformatic/kafka defaults: retries 3, retryDelay 1000 ms, metadataMaxAge 5000 ms | p99 4,146 ms, max 4,427 ms, 483 sends > 1 s, 0 errors |
| 20261108T155439Z | retries 10, retryDelay 100 ms (now the platform default) | p99 1,317 ms, max 1,693 ms, 60 sends > 1 s, 0 errors |
| 20261108T155638Z | retries 10, retryDelay 50 ms, metadataMaxAge 1000 ms | p99 1,387 ms, max 1,894 ms, 66 sends > 1 s, 0 errors |

Leadership did move in every run: 8 of 12 partitions changed leader in the first election, see `.events.log`.
