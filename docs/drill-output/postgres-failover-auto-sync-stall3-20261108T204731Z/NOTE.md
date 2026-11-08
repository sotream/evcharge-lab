This run is valid. It used the failover script that dumped `reservations` without `ended_at` and the first version of the history
checker (matching by id and connector only); the checker verdict was PASS and the current checker also passes on the same files.
Rerun with the fixed script and checker: `postgres-failover-auto-sync-stall3-20261108T221218Z`.
