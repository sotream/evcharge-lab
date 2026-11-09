Same script, same 30 runs, on sessions built from 19632ab (without the advisory lock), for comparison with the fixed build.
28 of 30 runs: 1 x 201 and 49 x 409. Runs 5 and 22 (rc=99): 1 x 201 and no 409 counted; the slowest request was 10,021 ms and 6,018 ms
(the other 49 requests failed with an error instead of 409; the bodies are not in the summary JSON, see stdout.txt for the thresholds).
The other 28 runs: slowest request 24 to 75 ms. Postgres log: 12 "deadlock detected" during the series (deadlocks.txt).
