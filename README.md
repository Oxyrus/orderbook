# Orderbook: multi-tenant order management on sharded PostgreSQL

Orderbook is a multi-tenant (B2B SaaS) order-management API. Merchants
(tenants) manage a product catalog, take orders and read sales reports. It
demonstrates two production patterns on top of the
`matrix` Go API template:

- **Sharding with [Citus](https://github.com/citusdata/citus)**: every
  tenant-owned table is hash-distributed by `tenant_id` across PostgreSQL
  worker nodes and co-located. Per-tenant requests are single-shard "router"
  queries and single-shard transactions; cross-tenant reports run in
  parallel on every shard.
- **Change data capture with [Debezium](https://debezium.io/)**: state
  changes write domain events into a co-located **transactional outbox**.
  Debezium streams them from each worker's WAL into Kafka, with no dual
  writes. An event worker consumes them idempotently to maintain a CQRS read
  model. Optional row-level CDC streams the order tables to Kafka for
  analytics and search.

```
                    ┌──────────── public :8080 ────────────┐
 client ──Bearer──▶ │ otel → request-id → logger → metrics │
  JWT with          │ → ... → rate limit → OAuth2 → tenant │──▶ catalog / orders / sales ──▶ ┌──────── Citus ────────┐
  tenant_id claim   │ (tenant from the verified token)     │     (tenant_id in every query)  │ coordinator           │
                    └──────────────────────────────────────┘                                 │  ├─ worker1 (shards)  │
                                                                                             │  └─ worker2 (shards)  │
            ┌───────────────── api worker ─────────────────┐     ┌─────── Kafka ──────┐      └────────┬──────────────┘
 /v1/sales ◀│ dedupe (event id) + apply, single-shard tx   │◀────│ orderbook.events.* │◀── Debezium ──┘ WAL, one
            │ retries, DLQ, trace continuation             │     │ orderbook.cdc.*    │   (Kafka        connector
            └──────────────────────────────────────────────┘     └────────────────────┘    Connect)     per worker
```

Everything from the template is still here: OAuth 2.0 resource server,
transactions bound to the context, embedded goose migrations, RFC 9457
errors, structured logging, Prometheus metrics, OpenTelemetry tracing,
health probes, Docker, CI and testcontainers-based integration tests.

## Contents

- [Why Citus and Debezium here](#why-citus-and-debezium-here)
- [Quick start](#quick-start)
- [API overview](#api-overview)
- [Project layout](#project-layout)
- [Common tasks](#common-tasks)
- [Technology choices](#technology-choices)
- [Documentation](#documentation)

## Why Citus and Debezium here

| Real-world problem | What this project does | Where |
|---|---|---|
| One database can't hold every tenant, and big tenants hurt small ones | Hash-shard by `tenant_id` with Citus; isolate or move big tenants online | [docs/sharding.md](docs/sharding.md), `migrations/` |
| Joins, FKs and transactions must keep working after sharding | Co-locate all tenant tables; reference table for shared lookups; PKs include `tenant_id` | `migrations/0000{2,3}_*.sql` |
| Tenant isolation | Tenant comes only from the verified token (`TENANT_CLAIM`); every query filters on it (it is also the shard key) | `internal/platform/tenant` |
| Cross-tenant reporting for operators | Multi-shard parallel aggregate behind a `platform:read` scope | `GET /v1/platform/sales/top-tenants` |
| Publishing events reliably (no lost or phantom events) | Transactional outbox in the same single-shard transaction; Debezium Outbox Event Router; one connector per worker | `internal/platform/outbox`, `deploy/debezium/` |
| Read models / other services that react to changes | Idempotent Kafka consumer with retries, DLQ and trace continuation | `internal/platform/kafkax`, `internal/sales` |
| Feeding a warehouse or search index without touching the app | Row-level CDC of `orders`/`order_lines` to compacted topics | `deploy/debezium/tables-*.json` |

## Quick start

Prerequisites: **Go 1.27+**, **Docker** (with Compose v2; give the VM
≥ 4 GB of memory), `make`, and `jq` for the examples.

```bash
# Everything in containers: Citus (coordinator + 2 workers), Kafka, Kafka Connect/Debezium,
# mock OAuth2, Redis, the API, the event worker, Prometheus/Grafana/Jaeger; registers the connectors.
make up-app
make e2e        # end-to-end check across both workers
```

Or run the Go processes on your machine:

```bash
make up               # infrastructure only
make run              # API on :8080 (auto-migrates the Citus cluster)
make cdc-register     # in another terminal, once: one Debezium outbox connector per worker
make run-worker       # event worker (admin on :9092)
```

Load demo data (8 tenants spread over both workers, 25 products and 40
orders each; re-run to add more orders):

```bash
make seed
```

Try it. The token's client id becomes its tenant:

```bash
TOKEN=$(make -s token client=acme)

PRODUCT=$(curl -s localhost:8080/v1/products -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"sku":"W-1","name":"Widget","price_minor":1250,"currency":"USD"}' | jq -r .id)

ORDER=$(curl -s localhost:8080/v1/orders -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"customer_ref\":\"cust-1\",\"lines\":[{\"product_id\":\"$PRODUCT\",\"quantity\":3}]}" | jq -r .id)

curl -s -X POST localhost:8080/v1/orders/$ORDER/cancel -H "Authorization: Bearer $TOKEN" -H 'If-Match: "1"'

make events                                                         # the outbox events in Kafka (Ctrl-C)
curl -s localhost:8080/v1/sales/daily -H "Authorization: Bearer $TOKEN" | jq   # read model, ~1s later

# Another tenant sees nothing of acme's (404), and lives on whichever shard its id hashes to
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/v1/orders/$ORDER -H "Authorization: Bearer $(make -s token client=globex)"
make citus-tenant t=acme; make citus-tenant t=globex

# Operators: cross-tenant report (parallel over all shards)
curl -s localhost:8080/v1/platform/sales/top-tenants -H "Authorization: Bearer $(make -s token client=ops scope=platform:read)" | jq
```

| Service | URL |
|---|---|
| API / admin | http://localhost:8080 / http://localhost:9090 |
| Worker admin (metrics, health) | http://localhost:9092 |
| Citus coordinator / workers | `localhost:5432` / `:5433`, `:5434` (`make psql`, `make psql-worker n=1`) |
| Kafka (host listener) | `localhost:29092` |
| Kafka Connect REST | http://localhost:8083/connectors |
| Mock OAuth2 server | http://localhost:8081/default/.well-known/openid-configuration |
| Jaeger / Prometheus / Grafana | http://localhost:16686 / :9091 / :3000 |

`make down` stops everything; `make reset` also deletes the volumes.

## API overview

The full contract is in [`docs/openapi.yaml`](docs/openapi.yaml). Every
`/v1` route except `/me` and `/platform/*` is tenant-scoped: the tenant
comes from the access token's `tenant_id` claim (`TENANT_CLAIM`).

| Method | Path | Scope | Description |
|---|---|---|---|
| GET | `/v1/me` | any valid token | The authenticated principal and its tenant |
| GET, POST | `/v1/products` | `catalog:read` / `catalog:write` | List (keyset pagination) / create products |
| GET, PUT | `/v1/products/{id}` | `catalog:read` / `catalog:write` | Get (`ETag`, 304) / replace (`If-Match` or body `version`) |
| GET | `/v1/orders?status=&limit=&cursor=` | `orders:read` | List orders, newest first |
| POST | `/v1/orders` | `orders:write` | Place an order: priced from the catalog, emits `order.placed` |
| GET | `/v1/orders/{id}` | `orders:read` | Order with its lines |
| POST | `/v1/orders/{id}/cancel` | `orders:write` | Cancel (optional `If-Match`), emits `order.cancelled` |
| GET | `/v1/sales/daily?from=&to=` | `sales:read` | Daily sales read model (eventually consistent, fed by CDC) |
| GET | `/v1/platform/sales/top-tenants?from=&to=&currency=&limit=` | `platform:read` | Cross-tenant report (multi-shard query) |

Errors are RFC 9457 `application/problem+json` documents with a stable
`code`.

## Project layout

```
.
├── cmd/api/                    # main: serve | worker | migrate | healthcheck | version
├── internal/
│   ├── app/                    # composition root: API (app.go), event worker (worker.go), routers
│   ├── catalog/                # products (tenant-scoped vertical slice)
│   ├── order/                  # orders: pricing, outbox events, event contract (events.go)
│   ├── sales/                  # read model: projector (Kafka handler), reports, platform report
│   ├── config/  buildinfo/  health/
│   ├── platform/
│   │   ├── tenant/             #   tenant from token claim, middleware, just-in-time provisioning
│   │   ├── outbox/             #   transactional outbox writer + pruning
│   │   ├── kafkax/             #   franz-go client, consumer loop (retries, DLQ, tracing, metrics)
│   │   ├── database/           #   pool, WithTx, migrator, Citus-aware constraint errors
│   │   └── auth/ httpx/ logger/ redisx/ requestid/ telemetry/
│   └── testutil/               # pgtest (Citus testcontainer), oauthtest, redistest
├── migrations/                 # Citus DDL: reference + distributed tables, outbox, publications
├── deploy/
│   ├── citus/                  # cluster registration (coordinator + workers)
│   ├── debezium/               # connector configs (one per worker) + register.sh
│   ├── kafka/                  # topic provisioning
│   └── prometheus/ grafana/ mock-oauth2/
├── test/e2e/                   # end-to-end test against the compose stack
└── docs/                       # sharding.md, cdc.md and the template's guides
```

## Common tasks

Run `make help` for the full list.

| Command | Description |
|---|---|
| `make up` / `make up-all` / `make up-app` | Infrastructure / + observability / + API and worker containers |
| `make run` / `make run-worker` | Run the API / the event worker locally |
| `make seed [orders=40 products=25 cancel=0.1 tenants=a,b]` | Migrate and load demo data (8 tenants across both workers) through the real services, so the events flow through CDC too |
| `make token client=acme [scope=...]` | Mint a token for tenant `acme` (`scope=platform:read` for operators) |
| `make test` / `make test-integration` / `make e2e` | Unit / + integration on Citus / end-to-end on the stack |
| `make citus-tables` / `citus-nodes` / `citus-tenant t=...` | Inspect distribution, placement and a tenant's shard |
| `make citus-rebalance` | Rebalance shards after adding a worker |
| `make cdc-register` / `cdc-register-tables` / `cdc-status` | Manage Debezium connectors, check slot lag |
| `make events [topic=...]` / `make kafka-topics` | Tail a topic / topics and consumer lag |
| `make lint` / `make vuln` / `make check` | Linters / govulncheck / everything CI runs |
| `make migration name=...` / `make migrate-up` | New migration ([Citus rules](docs/sharding.md#writing-migrations-for-citus)) / apply |

## Technology choices

| Concern | Choice | Why |
|---|---|---|
| Router | `go-chi/chi/v5` | The leading router built purely on `net/http`. Handlers are plain `http.HandlerFunc`, so there is zero lock-in. Adds route groups/sub-routers and per-route middleware (`r.With(...)`), exposes the matched route pattern (needed for metrics and span names), and ships spoof-safe client-IP middleware. The Go 1.22+ `ServeMux` can match methods and wildcards but has no middleware grouping or mounting. Switching later is mechanical; see [docs/architecture.md](docs/architecture.md#why-chi). |
| Logging | `rs/zerolog` | Benchmarks as the fastest structured logger, with zero allocations on the hot path. |
| DB driver | `jackc/pgx/v5` + `pgxpool` | The fastest and most complete PostgreSQL driver. Native types, batching, `COPY`, tracing hooks. |
| Migrations | `pressly/goose/v3` | Plain SQL files, `embed.FS` support, a provider API, Postgres advisory locking. |
| Auth | `coreos/go-oidc/v3` | Mature, minimal JWT/JWKS verification with caching and key-rotation handling. Works with any compliant issuer. |
| Config | `caarlos0/env/v11` | 12-factor configuration from environment variables with typed parsing and defaults. |
| Metrics | `prometheus/client_golang` | The industry standard. |
| Tracing | OpenTelemetry + `otelhttp` + `exaring/otelpgx` | Vendor-neutral. HTTP and SQL spans out of the box. |
| Validation | `go-playground/validator/v10` | De-facto standard tag-based validation. |
| Redis | `redis/go-redis/v9` + `redisotel` | The official client: pooling, Cluster/Sentinel support, OpenTelemetry instrumentation. |
| Rate limiting | `go-chi/httprate` | Sliding-window limiter with pluggable counters (e.g. Redis for multi-instance). |
| Sharding | Citus 14 (PostgreSQL 17 extension) | Real PostgreSQL with transparent hash sharding, co-location, reference tables, distributed queries and online rebalancing. See [docs/sharding.md](docs/sharding.md). |
| CDC | Debezium 3.3 on Kafka Connect, `pgoutput` | The standard log-based CDC platform; outbox event router SMT. See [docs/cdc.md](docs/cdc.md). |
| Kafka client | `twmb/franz-go` | Pure Go, fast, complete (consumer groups, cooperative rebalancing, transactions), ships an in-memory fake cluster (`kfake`) for tests. |
| Tests | `testify`, `testcontainers-go`, `go-jose`, `kfake` | Readable assertions, real dependencies (Citus), real JWTs, an in-memory Kafka. |

## Documentation

| Document | Topics |
|---|---|
| [Sharding](docs/sharding.md) | Why Citus, topology, distribution column, co-location, reference tables, router vs multi-shard queries, migration rules, testing, rebalancing, tenant isolation, operations |
| [CDC](docs/cdc.md) | Outbox and row-level CDC use cases, Debezium on Citus (per-worker connectors, shard-name normalisation, ordering), delivery guarantees, the consumer, slot-lag monitoring, adding workers, failover, production checklist |
| [Architecture](docs/architecture.md) | Layers, request lifecycle, middleware order, design decisions |
| [Configuration](docs/configuration.md) | Every environment variable |
| [Authentication](docs/authentication.md) | OAuth 2.0, providers, scopes, tenant claim |
| [Database](docs/database.md) | Pool, repositories, transactions, migrations, pagination, locking |
| [Observability](docs/observability.md) · [Redis](docs/redis.md) · [Testing](docs/testing.md) · [Deployment](docs/deployment.md) · [Adding a feature](docs/adding-a-feature.md) · [Development](docs/development.md) | As in the template, adapted |
| [OpenAPI spec](docs/openapi.yaml) | API contract |
