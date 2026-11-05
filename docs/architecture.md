# Architecture

## Goals

- **Boring and explicit.** Plain `net/http`, plain SQL, explicit wiring in a
  single composition root. No DI containers, no reflection magic, no ORMs.
- **Fast by default.** Zero-allocation logging, pgx's binary protocol,
  keyset pagination, bounded metric cardinality.
- **Testable.** Every layer can be tested alone with fakes, and the whole
  stack can be tested against real dependencies.
- **Production first.** Timeouts, limits, graceful shutdown, observability
  and security are built in, not bolted on.

## Layers

```
cmd/api            CLI entrypoint: parses commands, loads config, builds logger, handles signals
   │
internal/app       composition root: constructs everything, mounts routes, runs the API (Run)
   │               and the event worker (RunWorker)
internal/<feature> vertical slice (catalog, order, sales)
   ├── handler.go     transport: HTTP ⇄ DTOs, status codes, scopes
   ├── service.go     business rules, transaction boundaries, outbox events
   ├── order.go       domain types, domain errors, ports (Repository, Products, Events interfaces)
   ├── events.go      (order) the published event contract
   └── repository.go  adapter: PostgreSQL/Citus implementation of the ports
   │
internal/platform  reusable infrastructure with no business knowledge
   (auth, database, httpx, kafkax, logger, outbox, redisx, requestid, telemetry, tenant)
```

Rules:

1. **Dependencies point inward.** Handlers depend on services, and services
   depend on interfaces they define themselves (`order.Repository`). The
   PostgreSQL repository implements them.
2. **`platform` never imports a feature.** Features import `platform`.
3. **Services know nothing about HTTP.** They take and return domain types and
   domain errors (`order.ErrNotFound`). The handler maps those to HTTP
   (`mapError`). The request ID reaches services through `requestid`, not
   `httpx`.
4. **Repositories know nothing about transactions.** They call
   `db.Q(ctx)`, which returns the transaction bound to the context or the
   pool. Services decide where transactions start and end.
5. **Packages are organised by feature, not by layer.** Everything about
   orders lives in `internal/order`. That keeps a feature easy to find, copy
   and delete.

## Request lifecycle

A `POST /v1/orders` request flows through these steps:

```
net/http server (timeouts, max header bytes)
└─ otelhttp                 start server span (or continue the incoming W3C traceparent)
   └─ RequestID             reuse a valid X-Request-ID or generate a UUIDv7; echo it in the response
      └─ ClientIP           resolve the real client IP using the configured trust model
         └─ Logger          child logger {request_id, trace_id, span_id} → ctx; access log after response
            └─ Metrics      in-flight gauge; count/latency/size by route pattern after response
               └─ TraceRoute     rename the span "POST /v1/orders" once the route is known
                  └─ Recoverer   panic → logged with stack → 500 problem
                     └─ SecureHeaders, CORS, MaxBodyBytes, Timeout(ctx deadline)
                        └─ /v1 group
                           └─ RateLimit (per client IP) → 429
                              └─ Authenticate (JWT) → 401 / Principal in ctx, sub added to logger
                                 └─ tenant.Middleware (tenant_id claim) → 403 / tenant in ctx, logger, span
                                    └─ RequireScopes("orders:write") → 403
                                       └─ httpx.Handle(handler.place)
                                          ├─ httpx.Decode: content-type, size, strict JSON, validation → 400/415/422
                                          ├─ service.Place
                                          │   ├─ tenants.Ensure          (INSERT tenants … ON CONFLICT, cached)
                                          │   └─ db.WithTx ─┬─ products.LockForOrder (SELECT … FOR SHARE)
                                          │                 ├─ repo.Create          (INSERT orders, order_lines)
                                          │                 └─ events.Add           (INSERT outbox)
                                          │                    commit / rollback: one shard, one node, no 2PC
                                          └─ 201 JSON + Location + ETag
                                             (or the returned error → mapError → problem+json)

      … later, asynchronously: WAL → Debezium → Kafka → api worker → tenant_daily_sales (docs/cdc.md)
```

Middleware order is deliberate:

- **Logger and Metrics sit outside Recoverer**, so they record the 500 a panic
  produces.
- **The rate limiter runs before authentication**, so attackers can't burn CPU
  on signature verification.
- **Scope checks are per route** (`r.With(auth.RequireScopes(...))`), so each
  endpoint's permissions are visible next to its route.
- **Probes (`/livez`, `/readyz`) are outside `/v1`**, so they skip auth and
  rate limiting and are excluded from tracing.

## Error handling

Handlers have the signature `func(w, r) error` and are wrapped with
`httpx.Handle`. Returning an error renders it centrally:

| Returned value | Result |
|---|---|
| `*httpx.Error` (e.g. `httpx.NotFound(code, detail)`) | Its status, code and detail. Logged at debug (4xx) or error (5xx). |
| Any other error | `500 internal_error`. The real error is **logged, never sent** to the client. |

Domain packages define sentinel errors (`ErrNotFound`, `ErrNameTaken`, …)
and each handler's `mapError` translates them. Error `code`s are part of your
API contract: clients branch on `code`, never on `detail`.

## Concurrency control

- **Optimistic locking**: `products.version` and `orders.version` are
  incremented on every update.
  `UPDATE … WHERE version = $n` touches no row when someone else updated
  first. The `ETag` header carries the version. Clients send it back in
  `If-Match` (a stale one gets `412 Precondition Failed`) or as `version` in
  the body (a stale one gets `409 Conflict`). `POST /v1/orders/{id}/cancel`
  accepts an optional `If-Match` with the same 412 semantics. `GET` honours `If-None-Match` and
  answers `304 Not Modified`.
- **Row locks**: placing an order takes `FOR SHARE` locks on its products, so
  a concurrent price change can't slip in between pricing and commit.
- **Serializable transactions** with automatic retry are available through
  `WithTxOptions(ctx, TxOptions{IsoLevel: pgx.Serializable, MaxRetries: n}, fn)`
  for invariants that span several rows.

## IDs

Primary keys are **UUIDv7** (`uuid.NewV7()`), generated in the service:

- Time-ordered, so B-tree inserts stay local (much better than random v4 on
  large tables).
- Created before the INSERT, so the ID can go into events or logs inside
  the same transaction.
- No cross-node sequence: IDs are generated by the application, which suits
  a sharded database.
- Don't expose sequential integers (which leak business volume).

## Pagination

Lists use **keyset (cursor) pagination** on `(created_at, id)` (orders:
`(placed_at, id)`) with a matching composite index that starts with
`tenant_id`, so a page is a single-shard index range scan. Unlike `OFFSET`, it costs the same at page 1 and page
10,000, and it never skips or repeats rows when new rows are inserted. The
cursor is opaque to clients (base64 JSON), so the scheme can change without
breaking them.

## Multi-tenancy and sharding

The tenant comes from the verified access token (`TENANT_CLAIM`, default
`tenant_id`), never from the request. `tenant.Middleware` puts it in the
context. Every repository query filters on `tenant_id`. Another tenant's
resource returns **404, not 403**, so callers can't learn that the ID exists.

`tenant_id` is also the Citus **distribution column**: the same filter that
isolates tenants routes each query to the single shard that holds the
tenant. See [sharding.md](sharding.md). Cross-tenant endpoints live under
`/v1/platform`, outside the tenant middleware, behind `platform:read`.

## Events and the worker process

State changes that others react to are written to the transactional outbox
in the same transaction. Debezium publishes them to Kafka. `api worker` (a
second process from the same binary) consumes them and maintains read
models. The API never talks to Kafka, so a Kafka outage cannot fail a
request. See [cdc.md](cdc.md).

## Two servers

| Port | Server | Exposure |
|---|---|---|
| 8080 | Public API | Load balancer / ingress |
| 9090 | Admin: `/metrics`, probes, `/version`, pprof | Cluster-internal only (Prometheus, kubelet) |

Keeping them apart means metrics and profiling can't be reached from the
internet by accident, and probes keep working when the public port is
saturated.

## Startup

1. The admin server starts listening. `/livez` answers 200 and `/readyz`
   answers `503 starting`.
2. The service connects to Postgres (and Redis), applies migrations when
   `DB_AUTO_MIGRATE=true`, and runs OIDC discovery.
3. The public port starts listening and `/readyz` turns 200.

If any step fails, the admin server is stopped and the process exits 1.

## Graceful shutdown

On SIGTERM/SIGINT:

1. `/readyz` starts returning `503 draining`.
2. The service waits `HTTP_SHUTDOWN_DELAY` (load balancers notice and stop
   sending traffic).
3. `http.Server.Shutdown` stops accepting connections and waits up to
   `HTTP_SHUTDOWN_TIMEOUT` for in-flight requests. Request contexts are
   **not** cancelled by the signal, so they can finish their work.
4. If requests are still running when the timeout expires, their
   connections are closed, which cancels their contexts.
5. The tracer is flushed and the DB pool closed. The process exits 0.

A second SIGINT/SIGTERM during shutdown kills the process immediately
(handy for Ctrl-C twice locally).

If either server fails to start (e.g. the port is in use), the other is shut
down and the process exits 1.

## Why chi

The brief asked for "the best std router". The options are:

| | stdlib `ServeMux` (Go 1.22+) | **chi v5** | gin / echo / fiber |
|---|---|---|---|
| `net/http` handlers | ✅ | ✅ | ❌ custom contexts (fiber isn't even `net/http`) |
| Method + wildcard routing | ✅ | ✅ | ✅ |
| Route groups / sub-routers / mount | ❌ (manual) | ✅ | ✅ |
| Per-route middleware stacks | manual wrapping | ✅ `r.With(...)` | ✅ |
| Matched route pattern for metrics | ✅ `r.Pattern` | ✅ | ✅ |
| Spoof-safe client IP, rate limiting | ❌ | ✅ (chi, httprate) | partial |
| Dependencies | 0 | 0 (chi itself) | many |

chi is the most capable router that stays 100% `net/http`. Handlers,
middleware and tests are standard, so moving to `ServeMux` later is
mechanical: replace `chi.URLParam(r, "id")` with `r.PathValue("id")`, flatten
the route groups, and get the route label from `r.Pattern`.

## Why not an ORM or code generator?

Hand-written SQL in repositories is explicit, reviewable and as fast as pgx
allows. If a service grows many queries,
[sqlc](https://sqlc.dev/) is the recommended next step. It generates
type-safe Go from SQL, and its generated `Queries` accept a `DBTX`, so it
plugs straight into `db.Q(ctx)` and keeps the transaction design intact.
