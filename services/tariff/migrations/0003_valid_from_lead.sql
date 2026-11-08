-- A new version must start at least 15 minutes in the future: longer than the cache TTL (60 s) plus
-- the retry spacing while a cache entry is served stale, so every instance has reloaded the tariff
-- before a version takes effect and all instances price a session identically.
-- The seed (0002) runs before this trigger exists and is therefore exempt.
CREATE FUNCTION tariff_versions_valid_from_lead() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.valid_from < now() + interval '15 minutes' THEN
    RAISE EXCEPTION 'tariff_versions.valid_from must be at least 15 minutes in the future (got %)', NEW.valid_from
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER tariff_versions_valid_from_lead BEFORE INSERT ON tariff_versions
  FOR EACH ROW EXECUTE FUNCTION tariff_versions_valid_from_lead();
