-- Versioned tariffs (ADR 0011). A version applies to sessions whose startedAt
-- is >= valid_from and before the next version. Rows are never updated: a price
-- change is a new version, so old sessions reprice identically.
CREATE TABLE tariffs (
  id          text PRIMARY KEY CHECK (id ~ '^[a-z0-9][a-z0-9_-]{0,63}$'),
  description text NOT NULL DEFAULT '',
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tariff_versions (
  tariff_id                  text        NOT NULL REFERENCES tariffs (id),
  version                    integer     NOT NULL CHECK (version > 0),
  valid_from                 timestamptz NOT NULL,
  currency                   char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  timezone                   text        NOT NULL,
  session_fee_minor          bigint      NOT NULL DEFAULT 0 CHECK (session_fee_minor >= 0),
  energy_price_minor_per_kwh bigint      NOT NULL CHECK (energy_price_minor_per_kwh >= 0),
  -- [{days?: ["mon",..], from: "HH:MM", to: "HH:MM", price_minor_per_kwh: int, label?: text}]
  tou_windows                jsonb       NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(tou_windows) = 'array'),
  time_price_minor_per_hour  bigint      NOT NULL DEFAULT 0 CHECK (time_price_minor_per_hour >= 0),
  idle_fee_minor_per_min     bigint      NOT NULL DEFAULT 0 CHECK (idle_fee_minor_per_min >= 0),
  idle_grace_min             integer     NOT NULL DEFAULT 0 CHECK (idle_grace_min >= 0),
  min_billable_wh            bigint      NOT NULL DEFAULT 0 CHECK (min_billable_wh >= 0),
  created_at                 timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tariff_id, version),
  UNIQUE (tariff_id, valid_from)
);

-- Versions are immutable once written: billing reprices old sessions with them.
CREATE FUNCTION tariff_versions_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'tariff_versions are immutable: insert a new version instead';
END $$;

CREATE TRIGGER tariff_versions_no_update BEFORE UPDATE OR DELETE ON tariff_versions
  FOR EACH ROW EXECUTE FUNCTION tariff_versions_immutable();
