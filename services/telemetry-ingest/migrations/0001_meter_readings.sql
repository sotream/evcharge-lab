-- Canonical telemetry schema (ADR 0008). Shared by telemetry-ingest and scripts/backfill-telemetry.
-- One wide row per meter reading (~200 B), dedup on (station_id, connector_id, time).
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE meter_readings (
  time            timestamptz NOT NULL,
  station_id      text        NOT NULL,
  connector_id    smallint    NOT NULL,
  transaction_id  bigint,
  energy_wh       bigint,
  power_w         integer,
  current_a       real,
  voltage_v       real,
  soc_pct         real,
  replayed        boolean     NOT NULL DEFAULT false
);

SELECT create_hypertable('meter_readings', by_range('time', INTERVAL '1 hour'));
CREATE UNIQUE INDEX meter_readings_dedup ON meter_readings (station_id, connector_id, time);
CREATE INDEX meter_readings_tx ON meter_readings (transaction_id, time) WHERE transaction_id IS NOT NULL;

ALTER TABLE meter_readings SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'station_id, connector_id',
  timescaledb.compress_orderby = 'time'
);
SELECT add_compression_policy('meter_readings', INTERVAL '1 day');
SELECT add_retention_policy('meter_readings', INTERVAL '90 days');
