This run FAILED and is kept as evidence. Two bugs surfaced here, both fixed afterwards:

- The gateway process crashed while two brokers were down. The `@platformatic/kafka` consumer client emitted an
  unhandled `error` event when `joinGroup` failed, and the restart then blocked on joining. The `gateway_ready_during_outage`
  check got no HTTP answer at all.
- Three probe stations were stuck before the drill even started. After a simulator restart, their uniqueIds were
  reused, and sessions answered with a stored response that belonged to a different message, so their transaction
  queues blocked. Their readings show up here as "missing". Each probe station has 28 readings in
`probes/readings.csv`, so the three stuck stations explain at most 84 of the 126 missing readings
[derived: 3 x 28]. The remaining 42 have no named cause and are not isolated.

`result.json` originally had `"measured":000` (curl's code when there is no answer), which is invalid JSON. The drill
harness now normalises every check value; this one token was changed to `0` by hand so the file parses. Everything
else is raw output.

A third factor was found later and also applies to this run. The earlier slow-Postgres drill had restored the
telemetry Postgres CPU with `docker update --cpus 0`, which does not remove a limit, so the telemetry database was
still capped at 0.05 CPU here. Some of the missing readings may simply not have been ingested yet. The drill now
restores the full CPU count, and the slow-Postgres run affected by this was deleted.
