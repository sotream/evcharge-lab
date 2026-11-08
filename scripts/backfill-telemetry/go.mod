module github.com/sotream/evcharge-lab/scripts/backfill-telemetry

go 1.27

require (
	github.com/jackc/pgx/v5 v5.12.0
	github.com/sotream/evcharge-lab/packages/platform-go v0.0.0-00010101000000-000000000000
	golang.org/x/sync v0.24.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	golang.org/x/text v0.42.0 // indirect
)

replace (
	github.com/sotream/evcharge-lab/packages/contracts-go => ../../packages/contracts-go
	github.com/sotream/evcharge-lab/packages/platform-go => ../../packages/platform-go
)
