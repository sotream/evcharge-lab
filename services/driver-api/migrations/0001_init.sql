-- Drivers database (ADR 0001, 0010).
CREATE TABLE drivers (
  driver_id     uuid PRIMARY KEY,
  email         text NOT NULL,           -- stored lower-cased
  password_hash text NOT NULL,           -- scrypt$N$r$p$saltB64$keyB64
  id_tag        text NOT NULL,           -- OCPP idTag, registered in sessions
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX drivers_email_uq ON drivers (email);
CREATE UNIQUE INDEX drivers_id_tag_uq ON drivers (id_tag);

-- Opaque refresh tokens, stored as SHA-256. One family per login; every use rotates the token within the family.
CREATE TABLE refresh_tokens (
  token_hash bytea PRIMARY KEY,
  family_id  uuid NOT NULL,
  driver_id  uuid NOT NULL REFERENCES drivers (driver_id) ON DELETE CASCADE,
  expires_at timestamptz NOT NULL,
  rotated_at timestamptz,                -- set when exchanged for a successor; presenting it again is reuse
  revoked_at timestamptz,                -- set on logout or reuse detection (whole family)
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX refresh_tokens_family_idx ON refresh_tokens (family_id);
CREATE INDEX refresh_tokens_expiry_idx ON refresh_tokens (expires_at);
