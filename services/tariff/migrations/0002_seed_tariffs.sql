-- Seeded tariffs (ADR 0011). valid_from lies far in the past so every lab session has a version.
INSERT INTO tariffs (id, description) VALUES
  ('std',   'Standard: 0.39 EUR/kWh + 0.49 EUR session fee'),
  ('tou',   'Time of use (Europe/Kyiv): peak 0.49 EUR/kWh, off-peak 23:00-07:00 0.29 EUR/kWh'),
  ('fleet', 'Fleet: 0.25 EUR/kWh, no session fee');

INSERT INTO tariff_versions (tariff_id, version, valid_from, currency, timezone, session_fee_minor,
                             energy_price_minor_per_kwh, tou_windows) VALUES
  ('std',   1, '2020-01-01T00:00:00Z', 'EUR', 'Europe/Berlin', 49, 39, '[]'),
  ('tou',   1, '2020-01-01T00:00:00Z', 'EUR', 'Europe/Kyiv',    0, 49,
     '[{"from": "23:00", "to": "07:00", "price_minor_per_kwh": 29, "label": "off-peak"}]'),
  ('fleet', 1, '2020-01-01T00:00:00Z', 'EUR', 'Europe/Berlin',  0, 25, '[]');
