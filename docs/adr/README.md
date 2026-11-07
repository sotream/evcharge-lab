# Architecture Decision Records

MADR style. One decision per file; superseded ADRs stay with a pointer to the replacement.

| # | Decision |
|---|---|
| [0000](0000-requirements-and-capacity.md) | Requirements, load model and SLOs (every other ADR cites it) |
| [0001](0001-architecture-overview.md) | Architecture overview, service responsibilities, data ownership, ports |
| [0002](0002-contracts-events-and-delivery-semantics.md) | Contracts, event envelope, topics/keys, at-least-once + idempotency |
| [0003](0003-ocpp-subset-station-auth-and-offline-replay.md) | OCPP 1.6J subset, station auth, offline replay and dedup |
| [0004](0004-websocket-connection-routing.md) | Routing commands to the gateway instance holding a station |
| [0005](0005-reservation-contention.md) | Reservations under contention (exclusion constraint) |
| [0006](0006-smart-charging-allocation.md) | Smart charging: water-filling under a hard cap, safe fallback |
| [0007](0007-billing-saga-and-exactly-once-effect.md) | Billing saga and exactly-once effect |
| [0008](0008-telemetry-storage.md) | Telemetry ingest path and TimescaleDB |
| [0009](0009-toolchain-and-infrastructure-choices.md) | Toolchain and infrastructure versions |
| [0010](0010-authentication.md) | Authentication for drivers, stations, services |
| [0011](0011-tariff-model.md) | Tariff model and deterministic pricing |
| [0012](0012-kafka-replication-and-producer-backpressure.md) | Kafka replication (RF 3, min ISR 2), producer durability, backpressure, broker drills |
| [0013](0013-traffic-generators-simulator-and-backfill.md) | Traffic generators: OCPP station simulator and telemetry backfill |
