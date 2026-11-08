This run FAILED one check and is kept as evidence. `stale_unresolved` = 6: the probe waited for the site map to show
"Charging", but six sessions on site 9 were held by the allocator at the site cap and the connector showed
`SuspendedEVSE` the whole time (`lastStatus` in probe-events.jsonl). The replica had applied the session, so this is a
probe-definition error, not staleness. The probe now accepts Charging, SuspendedEVSE or SuspendedEV. All other checks passed.
Pause 240 s, peak replica lag 239.6 s, 0 read-your-writes violations in 99 checks.
