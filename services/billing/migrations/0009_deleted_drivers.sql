-- Drivers who deleted their account (GDPR). Their payment data is removed as soon as no saga of theirs is unfinished.
CREATE TABLE deleted_drivers (
  driver_id  uuid PRIMARY KEY,
  deleted_at timestamptz NOT NULL DEFAULT now()
);
