Three runs of the rewritten `load/k6/gateway-ws.js` (2,000 stations x 1.25 Hz = 2,500 msg/s offered, one gateway instance), one after the other on the same stack, no restart between them, on the Apple M4 Pro (Docker VM 8 CPUs / 11.7 GiB; see `k6-gateway-ws-2000st-1.25hz.env.json`). Stack: postgres, replica, telemetry postgres, redis, kafka-1..3, sessions, smart-charging, tariff, telemetry-ingest, driver-api, one gateway, monitoring. The simulator, billing, the second gateway and HAProxy were stopped, as in the 2000st-1hz run. The catalog was seeded with 200 sites x 10 stations (`SIM_SITES=200`). The gateway image includes the per-socket limits of PR #22 (16 unanswered CALLs).

| run | steady-window CALLRESULT/s | p99 | max | unanswered | CALLERROR |
|---|---|---|---|---|---|
| run-1 | 2,490.2 | 253 ms | 1,186 ms | 0 | 0 |
| run-2 | 2,498.1 | 237 ms | 904 ms | 0 | 0 |
| run-3 | 2,491.3 | 223 ms | 1,206 ms | 0 | 0 |

Steady-window rate = `ocpp_callresults_steady` count / 60 s. All three runs pass the script's own thresholds (rc=0). `k6-gateway-ws-2000st-1.25hz.json` is run-1, the first complete run with the final script, not the best one.

`discarded-script-flaw.json`: the very first attempt, with an earlier version of the script that closed each socket 1 s after the window while stations still sent. 23 CALLs sent in the last milliseconds were counted as unanswered and crossed that threshold (rc=99). Its other figures (steady 2,499.97/s, p99 47 ms, 0 CALLERROR) are kept here for completeness but it is not part of the series: the script was changed (no new CALL after the window, close after 2 s) and the runs above were taken with the final version.

Not comparable with `k6-gateway-ws-2000st-1hz.json` (1,139 msg/s, p99 3,228 ms, 8 CPUs / 8.3 GB VM, whole-run denominator, different build): three things changed at once (method, VM, build), so this series does not show which of them moved the numbers.
