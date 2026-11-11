The one `history_checker_violations` hit is a timestamp artefact, not a double booking. Reservation 535 on `st-0009-001#2`
starts 2 ms before reservation 480 ended: the reserve's transaction began (its `now()`) before the cancel of 480 committed, waited
for it on the exclusion constraint, and only then inserted. `starts_at` and `ended_at` were both transaction-start `now()`, so
the rows recorded overlapping periods although the writes were serialised. Fixed in the same change that adds this note:
reservation timestamps use `clock_timestamp()` and a cancel takes the per-connector advisory lock. The rerun
[postgres-failover-auto-sync-stall3-20261111T113930Z](../postgres-failover-auto-sync-stall3-20261111T113930Z/result.json) passes.
