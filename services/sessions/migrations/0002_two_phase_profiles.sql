-- Two-phase profile apply (ADR 0006): track what is confirmed in force at each station, and what may be in force
-- because a SetChargingProfile was sent but not confirmed. A round only raises a limit when the cap holds with
-- every unconfirmed higher limit still assumed to be applied.
ALTER TABLE active_sessions
  ADD COLUMN confirmed_limit_w integer NULL,
  ADD COLUMN confirmed_valid_to timestamptz NULL,
  -- envelope (max limit, latest validTo) of unconfirmed profiles that may be in force
  ADD COLUMN possible_limit_w integer NULL,
  ADD COLUMN possible_valid_to timestamptz NULL,
  -- latest SetChargingProfile command sent (commands to one station are ordered: its result supersedes older ones)
  ADD COLUMN pending_command_id uuid NULL;
ALTER TABLE commands ADD COLUMN limit_w integer NULL, ADD COLUMN valid_to timestamptz NULL;
-- set when a round withheld increases: the next SetChargingProfile result re-triggers the site
ALTER TABLE sites ADD COLUMN alloc_deferred boolean NOT NULL DEFAULT false;
