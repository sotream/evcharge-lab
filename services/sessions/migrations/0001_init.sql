-- sessions service schema (ADR 0001 data ownership, ADR 0005 reservations, ADR 0006 allocation).
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- ---------------------------------------------------------------- catalog
CREATE TABLE sites (
  site_id text PRIMARY KEY,
  name text NOT NULL,
  lat double precision NOT NULL,
  lng double precision NOT NULL,
  cap_w integer NOT NULL CHECK (cap_w >= 0),
  -- allocation orchestration (ADR 0006): next periodic round, pending trigger, lease for one round at a time
  alloc_due_at timestamptz NOT NULL DEFAULT now(),
  alloc_requested_at timestamptz NULL,
  alloc_lease_token uuid NULL,
  alloc_lease_until timestamptz NULL,
  last_alloc_at timestamptz NULL,
  last_alloc_strategy text NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX sites_lat_lng_idx ON sites (lat, lng);
CREATE INDEX sites_alloc_due_idx ON sites (alloc_due_at);

CREATE TABLE stations (
  station_id text PRIMARY KEY,
  site_id text NOT NULL REFERENCES sites (site_id),
  lat double precision NOT NULL,
  lng double precision NOT NULL,
  -- hex SHA-256 of the 128-bit random secret (ADR 0003: no slow KDF for high-entropy secrets)
  secret_sha256 text NOT NULL CHECK (secret_sha256 ~ '^[0-9a-f]{64}$'),
  enabled boolean NOT NULL DEFAULT true,
  online boolean NOT NULL DEFAULT false,
  online_changed_at timestamptz NULL,
  gateway_instance text NULL,
  vendor text NULL,
  model text NULL,
  firmware_version text NULL,
  booted_at timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX stations_site_idx ON stations (site_id);

CREATE TABLE connectors (
  connector_pk bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  station_id text NOT NULL REFERENCES stations (station_id),
  connector_id integer NOT NULL CHECK (connector_id >= 1),
  site_id text NOT NULL REFERENCES sites (site_id),
  current_type text NOT NULL CHECK (current_type IN ('AC', 'DC')),
  phases smallint NOT NULL CHECK (phases BETWEEN 1 AND 3),
  max_power_w integer NOT NULL CHECK (max_power_w > 0),
  min_power_w integer NOT NULL CHECK (min_power_w >= 0),
  tariff_id text NOT NULL,
  status text NOT NULL DEFAULT 'Unavailable',
  error_code text NOT NULL DEFAULT 'NoError',
  -- station timestamp of the applied StatusNotification (last-write-wins, ADR 0003)
  status_at timestamptz NULL,
  UNIQUE (station_id, connector_id)
);
CREATE INDEX connectors_site_idx ON connectors (site_id);

-- idTag -> driver mapping (registered by driver-api, ADR 0010)
CREATE TABLE drivers (
  driver_id uuid PRIMARY KEY,
  id_tag text NOT NULL UNIQUE CHECK (length(id_tag) BETWEEN 1 AND 20),
  blocked boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- OCPP integer ids
CREATE SEQUENCE transaction_id_seq AS integer;
CREATE SEQUENCE reservation_id_seq AS integer;

-- ---------------------------------------------------------------- sessions (10 years, partitioned by month: ADR 0001)
CREATE TABLE sessions (
  session_id uuid NOT NULL,
  transaction_id integer NOT NULL,
  driver_id uuid NULL,
  id_tag text NOT NULL,
  site_id text NOT NULL,
  station_id text NOT NULL,
  connector_id integer NOT NULL,
  connector_pk bigint NOT NULL,
  tariff_id text NOT NULL,
  reservation_id integer NULL,
  status text NOT NULL CHECK (status IN ('Active', 'Stopping', 'Completed')),
  meter_start_wh bigint NOT NULL,
  meter_stop_wh bigint NULL,
  energy_wh bigint NULL,
  started_at timestamptz NOT NULL,
  stopped_at timestamptz NULL,
  stop_reason text NULL,
  final_limit_w integer NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (session_id, started_at)
) PARTITION BY RANGE (started_at);
CREATE INDEX sessions_driver_idx ON sessions (driver_id, started_at DESC);
CREATE INDEX sessions_transaction_idx ON sessions (transaction_id);

-- Hot, small table: one row per running session. Allocation reads it per site; StopTransaction finds the
-- session by transactionId without touching every monthly partition.
CREATE TABLE active_sessions (
  transaction_id integer PRIMARY KEY,
  session_id uuid NOT NULL UNIQUE,
  started_at timestamptz NOT NULL,
  site_id text NOT NULL,
  station_id text NOT NULL,
  connector_id integer NOT NULL,
  connector_pk bigint NOT NULL UNIQUE,
  driver_id uuid NULL,
  max_w integer NOT NULL,
  min_w integer NOT NULL,
  -- last SetChargingProfile sent (ADR 0006 change threshold and refresh)
  limit_w integer NULL,
  limit_valid_to timestamptz NULL,
  limit_sent_at timestamptz NULL
);
CREATE INDEX active_sessions_site_idx ON active_sessions (site_id);

-- ---------------------------------------------------------------- reservations (ADR 0005)
CREATE TABLE reservations (
  reservation_id integer NOT NULL,
  driver_id uuid NOT NULL,
  id_tag text NOT NULL,
  site_id text NOT NULL,
  station_id text NOT NULL,
  connector_id integer NOT NULL,
  connector_pk bigint NOT NULL,
  status text NOT NULL CHECK (status IN ('Pending', 'Active', 'Used', 'Cancelled', 'Expired', 'Rejected')),
  starts_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  period tstzrange GENERATED ALWAYS AS (tstzrange(starts_at, expires_at, '[)')) STORED,
  ended_at timestamptz NULL,
  command_id uuid NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  -- the TTL is fixed at 15 min (ADR 0000); the boundary trigger below relies on this bound
  CHECK (expires_at > starts_at AND expires_at - starts_at <= interval '15 minutes'),
  PRIMARY KEY (reservation_id, starts_at)
) PARTITION BY RANGE (starts_at);
CREATE INDEX reservations_open_expiry_idx ON reservations (expires_at) WHERE status IN ('Pending', 'Active');
CREATE INDEX reservations_connector_idx ON reservations (connector_pk, starts_at DESC);

-- Station commands sent through the outbox, so station.command-results can be correlated.
CREATE TABLE commands (
  command_id uuid PRIMARY KEY,
  station_id text NOT NULL,
  action text NOT NULL,
  reservation_id integer NULL,
  reservation_starts_at timestamptz NULL,
  session_id uuid NULL,
  session_started_at timestamptz NULL,
  driver_id uuid NULL,
  expires_at timestamptz NOT NULL,
  outcome text NULL,
  response jsonb NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz NULL
);
CREATE INDEX commands_created_at_idx ON commands (created_at);

-- Dedup ledger for StartTransaction / StopTransaction on `${stationId}:${ocppMessageId}` (ADR 0003).
-- Kept 30 days (stations replay at most 24 h).
CREATE TABLE ocpp_messages (
  message_key text PRIMARY KEY,
  action text NOT NULL,
  response jsonb NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ocpp_messages_created_at_idx ON ocpp_messages (created_at);

-- ---------------------------------------------------------------- monthly partitions
-- Partitions are UTC months. Each reservations partition carries the ADR 0005 exclusion constraint and the
-- one-open-reservation-per-driver partial unique index (both must be per partition: a partitioned table cannot
-- have a global constraint that does not include the partition key).
CREATE OR REPLACE FUNCTION ensure_month_partitions(p_from timestamptz, p_months integer) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  m timestamp;
  lo timestamptz;
  hi timestamptz;
  part text;
  created integer := 0;
BEGIN
  PERFORM pg_advisory_xact_lock(72616002);
  FOR i IN 0 .. p_months - 1 LOOP
    m := date_trunc('month', p_from AT TIME ZONE 'UTC') + make_interval(months => i);
    lo := m AT TIME ZONE 'UTC';
    hi := (m + interval '1 month') AT TIME ZONE 'UTC';
    part := 'sessions_' || to_char(m, 'YYYY_MM');
    IF to_regclass(part) IS NULL THEN
      EXECUTE format('CREATE TABLE %I PARTITION OF sessions FOR VALUES FROM (%L) TO (%L)', part, lo, hi);
      created := created + 1;
    END IF;
    part := 'reservations_' || to_char(m, 'YYYY_MM');
    IF to_regclass(part) IS NULL THEN
      EXECUTE format('CREATE TABLE %I PARTITION OF reservations FOR VALUES FROM (%L) TO (%L)', part, lo, hi);
      EXECUTE format(
        'ALTER TABLE %I ADD CONSTRAINT %I EXCLUDE USING gist (connector_pk WITH =, period WITH &&) WHERE (status IN (''Pending'', ''Active''))',
        part, part || '_no_overlap');
      EXECUTE format('CREATE UNIQUE INDEX %I ON %I (driver_id) WHERE status IN (''Pending'', ''Active'')', part || '_one_open_per_driver', part);
      created := created + 1;
    END IF;
  END LOOP;
  RETURN created;
END $$;

-- A reservation lasts at most 15 minutes, so only rows that cross a month boundary, or start within 15 minutes
-- after one, can conflict with a row in the neighbouring partition. Those rows serialise on advisory locks
-- (per connector, per driver) and check the parent table; a fresh snapshot per statement in READ COMMITTED sees
-- the other transaction's committed row. Everything else is covered by the per-partition constraints alone.
CREATE OR REPLACE FUNCTION reservations_boundary_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  month_start timestamptz := date_trunc('month', NEW.starts_at, 'UTC');
BEGIN
  IF NEW.status NOT IN ('Pending', 'Active') THEN
    RETURN NULL;
  END IF;
  IF date_trunc('month', NEW.expires_at - interval '1 microsecond', 'UTC') = month_start
     AND NEW.starts_at >= month_start + interval '15 minutes' THEN
    RETURN NULL;
  END IF;
  PERFORM pg_advisory_xact_lock(72616010, hashtext(NEW.connector_pk::text));
  PERFORM pg_advisory_xact_lock(72616011, hashtext(NEW.driver_id::text));
  IF EXISTS (
    SELECT 1 FROM reservations r
     WHERE r.connector_pk = NEW.connector_pk AND r.status IN ('Pending', 'Active') AND r.period && NEW.period
       AND NOT (r.reservation_id = NEW.reservation_id AND r.starts_at = NEW.starts_at)) THEN
    RAISE EXCEPTION 'connector % already reserved for an overlapping period', NEW.connector_pk
      USING ERRCODE = 'exclusion_violation', CONSTRAINT = 'reservations_no_overlap';
  END IF;
  IF EXISTS (
    SELECT 1 FROM reservations r
     WHERE r.driver_id = NEW.driver_id AND r.status IN ('Pending', 'Active')
       AND NOT (r.reservation_id = NEW.reservation_id AND r.starts_at = NEW.starts_at)) THEN
    RAISE EXCEPTION 'driver % already holds an open reservation', NEW.driver_id
      USING ERRCODE = 'unique_violation', CONSTRAINT = 'reservations_one_open_per_driver';
  END IF;
  RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER reservations_boundary_trg AFTER INSERT ON reservations
  FOR EACH ROW EXECUTE FUNCTION reservations_boundary_check();

SELECT ensure_month_partitions(now() - interval '1 month', 5);

-- ---------------------------------------------------------------- platform tables (ADR 0002)
-- outbox (copied from @evcharge/platform OUTBOX_DDL)
CREATE TABLE IF NOT EXISTS outbox (
  id bigserial PRIMARY KEY,
  topic text NOT NULL,
  key text NOT NULL,
  payload jsonb NOT NULL,
  headers jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz NULL
);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS outbox_published_at_idx ON outbox (published_at) WHERE published_at IS NOT NULL;
CREATE OR REPLACE FUNCTION outbox_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('outbox', '');
  RETURN NULL;
END $$;
CREATE OR REPLACE TRIGGER outbox_notify_trg AFTER INSERT ON outbox FOR EACH STATEMENT EXECUTE FUNCTION outbox_notify();

-- inbox (copied from @evcharge/platform PROCESSED_EVENTS_DDL)
CREATE TABLE IF NOT EXISTS processed_events (
  consumer text NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE INDEX IF NOT EXISTS processed_events_processed_at_idx ON processed_events (processed_at);

-- HTTP idempotency keys (copied from @evcharge/platform IDEMPOTENCY_KEYS_DDL)
CREATE TABLE IF NOT EXISTS idempotency_keys (
  scope text NOT NULL,
  key text NOT NULL,
  request_hash text NOT NULL,
  status_code integer NULL,
  response_body jsonb NULL,
  locked_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz NULL,
  PRIMARY KEY (scope, key)
);
CREATE INDEX IF NOT EXISTS idempotency_keys_created_at_idx ON idempotency_keys (created_at);
