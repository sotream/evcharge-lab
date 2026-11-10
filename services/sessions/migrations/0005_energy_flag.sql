ALTER TABLE sessions ADD COLUMN energy_flag text NULL CHECK (energy_flag IN ('ok','meter_regressed','implausible','stale'));
