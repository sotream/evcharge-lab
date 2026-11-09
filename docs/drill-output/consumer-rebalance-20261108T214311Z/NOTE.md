This run is valid. `summary.json` mixes the forced-redelivery phase into `lagPeakRecords` (5,080 is the offset move of the forced phase,
not the kill); `timings.json` (derived from the raw files by drills/lib/rebalance-report.mjs after the report was fixed to
analyse the kill window and the forced phase separately) is the authoritative file for the kill window.
Three earlier runs of this drill were discarded: they took "partitions owned" from the lag series an instance exports,
which trails the real assignment, and their rebalance times (48 s, 70 s, 108 s) are therefore not comparable.
