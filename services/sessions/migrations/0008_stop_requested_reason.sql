-- Why the system (not the driver) asked for the stop: 'auth_declined', 'profile_noncompliant'. Such a stop is re-issued until the session completes.
ALTER TABLE sessions ADD COLUMN stop_requested_reason text NULL;
