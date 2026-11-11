Expected failure of asynchronous replication: 280 acknowledged probe commits (3,075 ms) were lost on the promoted replica.
New in this run, `rto_driver_reservation_s` is null: every driver reservation after the kill got 409. During the 3 s stall the
drivers cancelled their reservations on all six drill connectors (451, 454, 457, 464, 467, 472; cancels acknowledged 1.4 to 3.0 s
before the kill). Those cancels were lost with the rest of the un-replicated WAL, so the reservations came back as Active on the
new primary and held the connectors until their 15 min expiry, past the end of the drill. Lost acknowledged cancels are the same
data loss as the lost probe commits; synchronous replication (`postgres-failover-auto-sync-stall3`) is the fix.
