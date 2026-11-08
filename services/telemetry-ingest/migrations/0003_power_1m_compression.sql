-- power_1m is ~143 B/row uncompressed: 2 years at full fleet would be ~1.5 TB [derived from
-- docs/load-output/telemetry-backfill-20261108T082949Z.json; measured
-- compressed power_1m: 6.7 B/row, ~86 GB for 2 years at full fleet]. Compress rollups after 7 days and
-- serve only materialized data (the real-time union scanned raw chunks: p99 372 ms per site/day).
-- Freshness is fine: the rollup refreshes every minute; "visible < 10 s" is served from Redis latest.
ALTER MATERIALIZED VIEW power_1m SET (timescaledb.materialized_only = true);
ALTER MATERIALIZED VIEW power_1m SET (timescaledb.compress = true);
SELECT add_compression_policy('power_1m', compress_after => INTERVAL '7 days');
