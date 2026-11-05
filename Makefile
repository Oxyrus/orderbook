# Run `make help` to list targets.
SHELL := /bin/bash
.DEFAULT_GOAL := help

APP          ?= api
PKG          := github.com/Oxyrus/orderbook
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT       ?= $(shell git rev-parse --verify -q HEAD 2>/dev/null || echo unknown)
BUILD_TIME   ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS      := -s -w \
	-X $(PKG)/internal/buildinfo.Version=$(VERSION) \
	-X $(PKG)/internal/buildinfo.Commit=$(COMMIT) \
	-X $(PKG)/internal/buildinfo.BuildTime=$(BUILD_TIME)
IMAGE        ?= orderbook
GOOSE_VERSION ?= v3.29.0
GOLANGCI_LINT_VERSION ?= v2.15.0

# Load .env if present (export every variable it defines).
ifneq (,$(wildcard .env))
include .env
export
endif

##@ Development

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

.PHONY: up
up: ## Start the Citus cluster (coordinator + 2 workers), redis, mock OAuth2, Kafka and Kafka Connect
	docker compose up -d --wait coordinator worker1 worker2 redis oauth2 kafka connect
	docker compose up cluster-init kafka-init --no-log-prefix --abort-on-container-failure

.PHONY: up-all
up-all: up ## Start everything incl. observability stack (Prometheus, Grafana, Jaeger)
	docker compose --profile observability up -d --wait prometheus grafana jaeger

.PHONY: up-app
up-app: ## Build and run the API + worker containers too and register the outbox connectors (full stack in Docker)
	docker compose --profile observability --profile app up -d --build --wait api worker prometheus grafana jaeger
	docker compose --profile app up connect-init --no-log-prefix --abort-on-container-failure

.PHONY: down
down: ## Stop all containers (keeps volumes)
	docker compose --profile observability --profile app down

.PHONY: reset
reset: ## Stop all containers and delete volumes (wipes the database)
	docker compose --profile observability --profile app down -v

.PHONY: run
run: ## Run the API locally (development mode, auto-migrates, console logs)
	APP_ENV=$${APP_ENV:-development} ADMIN_ENABLE_PPROF=$${ADMIN_ENABLE_PPROF:-true} DB_AUTO_MIGRATE=true LOG_FORMAT=$${LOG_FORMAT:-console} go run ./cmd/api serve

.PHONY: run-worker
run-worker: ## Run the event worker locally (consumes Kafka, maintains projections)
	APP_ENV=$${APP_ENV:-development} ADMIN_ADDR=$${WORKER_ADMIN_ADDR:-:9092} LOG_FORMAT=$${LOG_FORMAT:-console} go run ./cmd/api worker

.PHONY: token
token: ## Print an access token; the client id is the tenant (client=acme scope=... to customise; scope=platform:read for operators)
	@curl -sf -X POST http://localhost:8081/default/token \
		-d grant_type=client_credentials -d client_id=$${client:-acme} -d client_secret=secret \
		-d scope="$${scope:-all}" | tr -d ' \n' | sed -E 's/.*"access_token":"([^"]+)".*/\1/'

##@ Build

.PHONY: build
build: ## Build the binary into ./bin
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(APP) ./cmd/api

.PHONY: docker-build
docker-build: ## Build the Docker image
	docker build -t $(IMAGE):$(VERSION) \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_TIME=$(BUILD_TIME) .

##@ Quality

.PHONY: test
test: ## Run unit tests (fast, no Docker needed)
	go test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## Run unit + integration tests (needs Docker for testcontainers)
	go test -race -count=1 -tags integration ./...

.PHONY: e2e
e2e: ## End-to-end test against the running stack (make up-app first): API → Citus → Debezium → Kafka → worker
	go test -count=1 -tags e2e -v ./test/e2e/

.PHONY: cover
cover: ## Run all tests with coverage and write coverage.html
	go test -race -count=1 -tags integration -coverpkg=./internal/... -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1
	go tool cover -html=coverage.out -o coverage.html

.PHONY: lint
lint: ## Run golangci-lint
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	go tool govulncheck ./...

.PHONY: fmt
fmt: ## Format code (gofmt + goimports, configured in .golangci.yml)
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) fmt ./...

.PHONY: tidy
tidy: ## Tidy and verify modules
	go mod tidy
	go mod verify

.PHONY: check
check: tidy lint vuln test-integration ## Everything CI runs

##@ Database

.PHONY: migrate-up
migrate-up: ## Apply all pending migrations
	go run ./cmd/api migrate up

.PHONY: migrate-down
migrate-down: ## Roll back the latest migration
	go run ./cmd/api migrate down

.PHONY: migrate-status
migrate-status: ## Show migration status
	go run ./cmd/api migrate status

.PHONY: migration
migration: ## Create a new SQL migration: make migration name=add_users
	@test -n "$(name)" || { echo "usage: make migration name=<description>"; exit 1; }
	go run github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION) -dir migrations -s create $(name) sql

.PHONY: seed
seed: ## Migrate and load demo data through the services: make seed [orders=40 products=25 cancel=0.1 tenants=acme,globex]
	APP_ENV=$${APP_ENV:-development} LOG_FORMAT=$${LOG_FORMAT:-console} go run ./cmd/api seed \
		$(if $(tenants),-tenants=$(tenants)) $(if $(products),-products=$(products)) \
		$(if $(orders),-orders=$(orders)) $(if $(cancel),-cancel=$(cancel))

.PHONY: psql
psql: ## Open psql against the Citus coordinator
	docker compose exec coordinator psql -U orderbook orderbook

.PHONY: psql-worker
psql-worker: ## Open psql against a worker: make psql-worker n=1
	docker compose exec worker$${n:-1} psql -U orderbook orderbook

##@ Citus (sharding)

.PHONY: citus-nodes
citus-nodes: ## List cluster nodes and how many shards each holds
	@docker compose exec -T coordinator psql -U orderbook -c \
		"SELECT nodename, nodeport, count(*) AS shards, pg_size_pretty(sum(shard_size)) AS size FROM citus_shards GROUP BY 1, 2 ORDER BY 1"

.PHONY: citus-tables
citus-tables: ## List distributed and reference tables
	@docker compose exec -T coordinator psql -U orderbook -c \
		"SELECT table_name, citus_table_type, distribution_column, colocation_id, shard_count, table_size FROM citus_tables ORDER BY colocation_id, table_name"

.PHONY: citus-tenant
citus-tenant: ## Show which shard and node hold a tenant: make citus-tenant t=acme
	@test -n "$(t)" || { echo "usage: make citus-tenant t=<tenant id>"; exit 1; }
	@docker compose exec -T coordinator psql -U orderbook -c \
		"SELECT s.shardid, s.nodename, s.nodeport FROM citus_shards s WHERE s.table_name = 'tenants'::regclass AND s.shardid = get_shard_id_for_distribution_column('tenants', '$(t)')"

.PHONY: citus-rebalance
citus-rebalance: ## Start a background shard rebalance (after adding a worker), then watch citus_rebalance_status()
	@docker compose exec -T coordinator psql -U orderbook -c "SELECT citus_rebalance_start()" -c "SELECT * FROM citus_rebalance_status()"

##@ CDC (Debezium + Kafka)

.PHONY: cdc-register
cdc-register: ## Create/update the outbox connectors (one per worker); run after the first migration
	CONNECT_URL=$${CONNECT_URL:-http://localhost:8083} bash deploy/debezium/register.sh outbox

.PHONY: cdc-register-tables
cdc-register-tables: ## Optional: stream row-level changes of orders/order_lines to orderbook.cdc.public.*
	CONNECT_URL=$${CONNECT_URL:-http://localhost:8083} bash deploy/debezium/register.sh tables

.PHONY: cdc-status
cdc-status: ## Show connector states and replication slots on every worker
	@for c in $$(curl -s http://localhost:8083/connectors | tr -d '[]"' | tr ',' ' '); do \
		printf '%-28s connector/task: %s\n' "$$c" "$$(curl -s http://localhost:8083/connectors/$$c/status | grep -o '"state":"[A-Z]*"' | cut -d'"' -f4 | paste -sd/ -)"; done
	@for n in worker1 worker2; do echo "== $$n"; docker compose exec -T $$n psql -U orderbook -Atc \
		"SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots"; done

.PHONY: events
events: ## Tail the order events topic with headers (topic=... to change)
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
		--topic $${topic:-orderbook.events.order} --from-beginning --property print.headers=true --property print.key=true

.PHONY: kafka-topics
kafka-topics: ## List Kafka topics and the consumer group lag of the projection
	@docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
	@docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orderbook-sales-projection || true

.PHONY: redis-cli
redis-cli: ## Open redis-cli against the local Redis
	docker compose exec redis redis-cli
