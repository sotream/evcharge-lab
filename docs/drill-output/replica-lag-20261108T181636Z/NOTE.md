This run is INCOMPLETE and kept as evidence (no result.json). It showed three problems, none of them a replica fault:

- The harness `check` passed a negative measured value to `node -e`, which read it as an option (`bad option: -158.38`).
  The script stopped before `finish`. Fixed with `--`.
- `replica_lag_alert_fired` failed because the pause (150 s) was too short. Prometheus evaluates rules every 1 m, the rule
  needs lag above 5 s for 2 m: ALERTS shows SessionsReplicaLagHigh `pending` from 18:18:30 and no `firing`; Critical fired
  at 18:19:30. The default pause is now 240 s.
- `driver_api_read_p99_s_during` was null: the 150 s range query returned nothing at check time (the same query returns a
  value afterwards). The check now reads the worst 15 s window from timeline.jsonl. The timeline max during the pause was 0.119 s.

Raw facts from this run: peak replica lag 148.4 s, 0 read-your-writes violations in 108 checks, lag drained 6 s after replay resumed.
6 of 36 stale probes never saw "Charging" in 240 s, before, during and after the pause (so not caused by the replica).
Their sessions had final_limit_w 4140 (the connector minimum), not 22000. Cause not isolated in this run.
