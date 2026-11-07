-- OLTP instance: one database per service (ADR 0001). Telemetry lives on its own instance (ADR 0008).
CREATE DATABASE sessions;
CREATE DATABASE billing;
CREATE DATABASE tariff;
CREATE DATABASE drivers;
