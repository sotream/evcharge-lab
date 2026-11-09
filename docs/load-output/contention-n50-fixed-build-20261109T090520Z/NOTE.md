30 runs of load/k6/reservation-contention.js (N=50), one after the other, on the build with the per-connector advisory lock (ADR 0005).
The script now probes for a free connector and the winner releases it after the burst. 30 of 30 runs: exactly 1 x 201 and 49 x 409 (rc=0).
Slowest request per run: 26 to 60 ms (`http_req_duration` max in run-N.json). Postgres log: 0 "deadlock detected" (deadlocks.txt).
The first attempt with this script released the connector right after the win, so later requests also won (6 x 201 in run 1); that
attempt was discarded and the script fixed (the winner waits 3 s before releasing).
