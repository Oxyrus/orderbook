# Testing

There are two tiers, separated by a build tag:

| Tier | Command | Needs | Speed | What it covers |
|---|---|---|---|---|
| **Unit** | `make test` | Nothing | ~2s | Business logic, handlers, middleware, auth, config, using in-memory fakes |
| **Integration** | `make test-integration` | Docker | ~10s | Real PostgreSQL (testcontainers): SQL, constraints, transactions, migrations, plus full-stack HTTP tests with real JWTs |

Both run with `-race` in CI. Integration test files start with
`//go:build integration` and are named `*_integration_test.go`.

```bash
make test                                   # unit
make test-integration                       # unit + integration
make cover                                  # all, with coverage.html
go test -tags integration -run TestAPI ./internal/app/ -v   # a subset
```

Tip for editors: add `-tags=integration` to gopls `buildFlags` so
integration files get completion and diagnostics.

## Unit tests

### Services with fakes

Services depend on interfaces (`order.Repository`, `order.Products`,
`order.Events`, `database.Transactor`), so unit tests pass in-memory fakes.
See `internal/order/service_test.go`:

```go
repo, events, tenants, tx := &fakeRepo{}, &fakeEvents{}, &fakeTenants{}, &fakeTx{}
svc := order.NewService(repo, fakeProducts{a, b}, events, tenants, tx)

o, err := svc.Place(ctx, "acme", order.PlaceInput{...})
assert.Equal(t, 1, tx.n, "placing must run in a transaction")
require.Len(t, events.events, 1)  // and emit order.placed
```

Hand-written fakes are preferred over mock frameworks. They behave like the
real thing (e.g. they enforce uniqueness), so tests stay meaningful when
call patterns change. Real commit/rollback behaviour is proven in the
integration tier.

### Handlers

Mount the feature's routes on a chi router behind `auth.Static(principal)`
and `tenant.Middleware`, and drive it with `httptest`. No tokens or database
are needed. See `internal/order/handler_test.go`:

```go
r := chi.NewRouter()
r.Use(auth.Static(&auth.Principal{Subject: "u", Scopes: []string{order.ScopeWrite},
    Claims: map[string]any{"tenant_id": "acme"}}))
r.Use(tenant.Middleware("tenant_id"))
order.NewHandler(svc).Routes(r)
```

Table-driven tests cover the error mapping (409/412/422/404…) cheaply.

### Kafka consumers

`internal/platform/kafkax/consumer_test.go` runs the real consumer loop
against **kfake**, franz-go's in-memory Kafka cluster (no Docker), and
proves retries, dead-lettering with diagnostics headers, metrics and offset
commits. Event handlers themselves (e.g. `sales.Projector`) are plain
functions of a `kafkax.Message` and are unit-tested directly.

### Auth

`internal/testutil/oauthtest` starts an in-process OIDC issuer (discovery +
JWKS over `httptest`) and mints **real signed JWTs**, so the production
verifier is tested end to end: expired, wrong audience, wrong issuer, forged
signature, every scope claim format. See
`internal/platform/auth/auth_test.go` and
[authentication.md](authentication.md#testing-auth).

### Configuration in tests

Build configs with `config.Defaults()`, not `config.Load()`. `Defaults`
ignores the process environment, and the Makefile exports your `.env` into
every target, including `make test`. With `Load`, a local `.env` (say
`RATE_LIMIT_REQUESTS=5`) would make tests behave differently on your machine
than in CI.

## Integration tests

### Citus with testcontainers

`internal/testutil/pgtest` manages a real PostgreSQL with Citus:

1. **One container per test binary** (package), started in `TestMain`. The
   image matches compose (`citusdata/citus:14.0-pg17`) and runs with
   `fsync=off` for speed. It is a **single-node** cluster (the coordinator
   holds every shard): the distributed planner, DDL rules and shard-level
   errors are real, without starting workers. The image is amd64-only and
   runs emulated on Apple Silicon (see [sharding.md](sharding.md#testing)).
2. Migrations run **once** into a template database.
3. `pg.NewDB(t)` clones that template (`CREATE DATABASE … TEMPLATE`, a few
   milliseconds) into a fresh, fully migrated database for **each test**, and
   drops it during cleanup. `pg.NewDatabase(t)` does the same but returns the
   connection URL, for code that opens its own pool (e.g. `app.Run` in
   `internal/app/run_integration_test.go`).

Tests are fully isolated, so they can call `t.Parallel()` and need no
truncation or ordering tricks. (Citus keeps a maintenance connection to
every database, so pgtest disallows connections to the template and
terminates them before cloning.)

```go
//go:build integration

package order_test

var pg *pgtest.Server

func TestMain(m *testing.M) { pgtest.Main(m, &pg) }

func TestSomething(t *testing.T) {
    t.Parallel()
    db := pg.NewDB(t)                       // *database.DB, isolated
    repo := order.NewPostgresRepository(db)
    ...
}
```

Set `TEST_DATABASE_URL` to an existing Citus superuser connection URL to skip
the container (e.g. a CI service container, or a local Postgres for faster
iteration):

```bash
TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable make test-integration
```

### Redis with testcontainers

`internal/testutil/redistest` starts one Redis container per test package.
Tests share it and isolate themselves with a unique key namespace
(`redistest.Namespace`), which is deleted when the test ends. Set
`TEST_REDIS_URL` to reuse an existing server. See
[redis.md](redis.md#testing) for examples, including packages that need
both Postgres and Redis.

### What the integration suite proves

| Test | Proves |
|---|---|
| `database_integration_test.go` | Commit, rollback on error, rollback on panic, nested join, savepoints, constraint helpers, **real serialization failures retried with no lost updates**, every migration's Down + re-Up |
| `order/repository_integration_test.go` | Placing/pricing/cancelling, tenant isolation, keyset pagination, **router-query plans (`Task Count: 1`) vs multi-shard fan-out**, **all tenant tables co-located**, **an order never commits without its outbox event**, exactly one success among concurrent cancels |
| `catalog/repository_integration_test.go` | CRUD, optimistic locking, per-tenant uniqueness, **constraint errors raised by shards mapped to domain errors** |
| `sales/store_integration_test.go` | **Idempotent projection** under redelivery, multi-shard top-tenants report, pruning |
| `platform/kafkax/consumer_test.go` (unit, kfake) | Retries, permanent errors, DLQ headers, offset commits |
| `platform/redisx/redisx_integration_test.go` | Connect fails fast, cache get/set/TTL/delete, **one load for concurrent misses**, load errors aren't cached, **a Redis outage falls back to the source**, pool metrics |
| `app/api_integration_test.go` | The production router over real HTTP with real JWTs and real Citus: order lifecycle (incl. outbox rows), tenant isolation, tenant/platform authorisation, probes, rate limiting |
| `test/e2e` (`make e2e`) | The whole compose stack: a tenant on **each** worker, API → Citus → Debezium (per-worker connectors) → Kafka → worker → projection, plus the cross-tenant report |

### Full-stack API tests

`internal/app/api_integration_test.go` builds the router with
`app.NewRouter` (the same function production uses), backed by a `pgtest`
database and an `oauthtest` issuer, served by `httptest.NewServer`. Use this
style for critical user journeys. Keep the edge cases in the cheaper
unit tiers.

## Conventions

- `testify/require` for preconditions (stop the test), `assert` for
  checks.
- Table-driven tests with `t.Run` for input/output matrices.
- External test packages (`package order_test`) test the public API, as
  callers see it.
- Each test builds its own fixtures. Shared mutable fixtures across tests
  are not allowed.
- No `time.Sleep` for synchronisation. Use channels, `require.Eventually` or
  injected clocks.
- Run with `-race`. CI always does.

## CI

`.github/workflows/ci.yml`:

1. **lint**: checks that `go mod tidy` is clean, runs golangci-lint (gosec,
   errorlint, sqlclosecheck, …) and `govulncheck`.
2. **test**: unit + integration with `-race` and coverage. GitHub-hosted
   runners have Docker, so testcontainers works without setup.
3. **docker**: builds the production image (with layer caching).

Run the same locally with `make check`.
