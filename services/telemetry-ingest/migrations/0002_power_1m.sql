-- 1-minute rollup per connector, kept 2 years (ADR 0000 / ADR 0008).
CREATE MATERIALIZED VIEW power_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '1 minute', time) AS bucket,
       station_id,
       connector_id,
       avg(power_w)::integer            AS avg_power_w,
       max(power_w)                     AS max_power_w,
       max(energy_wh) - min(energy_wh)  AS energy_wh_delta,
       count(*)                         AS samples
FROM meter_readings
GROUP BY 1, 2, 3
WITH NO DATA;

SELECT add_continuous_aggregate_policy('power_1m',
  start_offset => INTERVAL '2 hours', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 minute');
SELECT add_retention_policy('power_1m', INTERVAL '2 years');
