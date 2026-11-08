This run is valid for what the database probe measured (282 acknowledged commits lost, RTO and error counts). Its reservation verdict
is superseded: the failover script dumped `reservations` without `ended_at`, and the history checker then matched an acknowledged
reservation to a row by id and connector only, so a lost reservation whose id the promoted node issued again was not seen as lost
(the report says 9 violations; the current checker gives 10 on the same files, and with `ended_at` the two overlaps that involve a
reservation the station had rejected would not be reported). Rerun with the fixed script and checker:
`postgres-failover-auto-async-stall3-20261108T220826Z`.
