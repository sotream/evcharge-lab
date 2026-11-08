# Every check runs locally (no CI). `make check` must be green before each stage commit.
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
GO_MODULES := $(shell go work edit -json | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).Use.map(u=>u.DiskPath).join(" ")))')

# Integration tests talk to the compose infra on host ports (ADR 0001).
export TEST_PG_URL ?= postgres://evcharge:evcharge@localhost:25432
export TEST_PG_TELEMETRY_URL ?= postgres://evcharge:evcharge@localhost:25433/telemetry
export TEST_REDIS_URL ?= redis://localhost:26379
export TEST_KAFKA_BROKERS ?= localhost:29091,localhost:29092,localhost:29093
export INTEGRATION ?= 1

.PHONY: prom-check check install contracts lint typecheck build test-unit test-int go-check infra-up infra-down up down topics-sync

check: install contracts topics-sync prom-check lint build typecheck test-unit infra-up test-int go-check
	@echo "✔ make check passed"

install:
	pnpm install --frozen-lockfile=false --reporter=silent

contracts:
	pnpm --filter @evcharge/contracts build
	@git diff --quiet -- packages/contracts/schemas || (echo "generated schemas changed: commit packages/contracts/schemas"; git --no-pager diff --stat -- packages/contracts/schemas; exit 1)

topics-sync:
	node scripts/check-topics.mjs

lint:
	pnpm lint

typecheck:
	pnpm typecheck

build:
	pnpm build
	@for m in $(GO_MODULES); do (cd $$m && go build ./...) || exit 1; done

test-unit:
	pnpm test:unit

# Integration tests share Redis and Kafka with the compose stack; running app containers would claim the
# same station routes, so they are stopped first (infra keeps running).
APP_SERVICES := simulator station-lb station-gateway-1 station-gateway-2 sessions billing telemetry-ingest smart-charging tariff driver-api

infra-up:
	@docker compose stop $(APP_SERVICES) >/dev/null 2>&1 || true
	docker compose up -d --wait postgres postgres-telemetry redis kafka-1 kafka-2 kafka-3
	docker compose run --rm topics-init >/dev/null

infra-down:
	docker compose down

# Rule syntax and unit tests run in one-off containers that never touch the compose stack.
PROMTOOL := docker run --rm -v "$(CURDIR)/infra/prometheus:/etc/prometheus:ro" --entrypoint promtool prom/prometheus:v3.16.0
PROM_RULES := $(patsubst infra/prometheus/%,/etc/prometheus/%,$(wildcard infra/prometheus/rules/*.yml))
PROM_TESTS := $(patsubst infra/prometheus/%,/etc/prometheus/%,$(wildcard infra/prometheus/tests/*.yml))
prom-check:
	$(PROMTOOL) check rules $(PROM_RULES)
	$(PROMTOOL) test rules $(PROM_TESTS)

test-int: infra-up
	pnpm test:int

go-check: infra-up
	@for m in $(GO_MODULES); do \
	  echo "── go: $$m"; \
	  (cd $$m && go vet ./... && golangci-lint run ./... && go test -race -count=1 ./...) || exit 1; \
	done

up:
	docker compose up -d --build

down:
	docker compose down -v
