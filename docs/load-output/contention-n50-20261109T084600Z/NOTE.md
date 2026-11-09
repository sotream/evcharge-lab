24 runs of load/k6/reservation-contention.js with N=50, one after the other, on the M4 with the fleet running.

The script picks the connector by the second of the run (`st-0020-00X` connector 1) and the simulator also uses those connectors, so most
runs landed on a connector that was already held. Such a run returns 0 x 201 and 50 x 409 and fails the script's own threshold (rc=99); it
says nothing about the race.

- Valid runs (exactly 1 x 201 and 49 x 409, rc=0): 1, 2, 5, 6, 7, 8, 10, 21 (8 runs).
- Runs on an already held connector (0 x 201, 50 x 409): 3, 4, 9, 11-20, 22-24 (16 runs).
- Slowest request in any of the 24 runs: 21.7 to 68.4 ms (`http_req_duration` max in each run-N.json).
- `deadlocks.txt`: the Postgres log had 0 "deadlock detected" entries before and after the series.

This is a check that the fix (ADR 0005, advisory lock per connector) leaves the 1 x 201 + 49 x 409 outcome intact and that no
deadlock stall occurs. There is no k6 run of the unfixed build to compare against; the before/after numbers are in PR #10 (integration probe).
