# Adding a feature

This guide adds a new tenant-scoped resource, **customers**, following the
same structure as `internal/catalog` (simple CRUD) and `internal/order`
(transactions + outbox events). Read those alongside it: every step below
has a working counterpart there. The Citus-specific rules are explained in
[sharding.md](sharding.md).

## 1. Migration

```bash
make migration name=create_customers
```

Edit `migrations/0000N_create_customers.sql`:

```sql
-- +goose Up
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';

CREATE TABLE customers (
    tenant_id  text        NOT NULL,
    id         uuid        NOT NULL,
    email      text        NOT NULL CHECK (char_length(email) BETWEEN 3 AND 320),
    name       text        NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),                          -- keys include the distribution column
    CONSTRAINT customers_tenant_email_key UNIQUE (tenant_id, email)
);
-- Shard by tenant, co-located with every other tenant table.
SELECT create_distributed_table('customers', 'tenant_id', colocate_with => 'tenants');

-- Foreign keys only after distributing, one per ALTER TABLE.
ALTER TABLE customers
    ADD CONSTRAINT customers_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE;

CREATE INDEX customers_tenant_created_idx ON customers (tenant_id, created_at DESC, id DESC);

-- +goose Down
DROP TABLE IF EXISTS customers;
```

Apply it locally with `make migrate-up` (or just `make run`, which
auto-migrates). `make citus-tables` should list the table in the same
co-location group as `tenants`.

## 2. Domain: `internal/customer/customer.go`

Types, sentinel errors and the ports the service needs:

```go
package customer

var (
    ErrNotFound   = errors.New("customer not found")
    ErrEmailTaken = errors.New("email already in use")
)

type Customer struct {
    TenantID  string
    ID        uuid.UUID
    Email     string
    Name      string
    CreatedAt time.Time
}

type Repository interface {
    Create(ctx context.Context, c *Customer) error
    Get(ctx context.Context, tenantID string, id uuid.UUID) (*Customer, error)
}

// Events appends to the transactional outbox (outbox.Writer implements it).
type Events interface {
    Add(ctx context.Context, events ...outbox.Event) ([]uuid.UUID, error)
}
```

## 3. Repository: `internal/customer/repository.go`

Always go through `db.Q(ctx)` so transactions propagate, **always filter on
`tenant_id`** (tenant isolation and shard routing), and map driver errors to
domain errors.

```go
type PostgresRepository struct{ db *database.DB }

func NewPostgresRepository(db *database.DB) *PostgresRepository { return &PostgresRepository{db: db} }

func (r *PostgresRepository) Create(ctx context.Context, c *Customer) error {
    err := r.db.Q(ctx).QueryRow(ctx,
        `INSERT INTO customers (tenant_id, id, email, name) VALUES ($1, $2, $3, $4) RETURNING created_at`,
        c.TenantID, c.ID, c.Email, c.Name,
    ).Scan(&c.CreatedAt)
    if database.IsUniqueViolation(err, "customers_tenant_email_key") { // also matches the shard-suffixed name
        return ErrEmailTaken
    }
    if err != nil {
        return fmt.Errorf("insert customer: %w", err)
    }
    return nil
}

func (r *PostgresRepository) Get(ctx context.Context, tenantID string, id uuid.UUID) (*Customer, error) {
    var c Customer
    err := r.db.Q(ctx).QueryRow(ctx,
        `SELECT tenant_id, id, email, name, created_at FROM customers WHERE tenant_id = $1 AND id = $2`,
        tenantID, id,
    ).Scan(&c.TenantID, &c.ID, &c.Email, &c.Name, &c.CreatedAt)
    if errors.Is(err, database.ErrNoRows) {
        return nil, ErrNotFound
    }
    if err != nil {
        return nil, fmt.Errorf("get customer: %w", err)
    }
    return &c, nil
}
```

## 4. Service: `internal/customer/service.go`

Business rules live here, and so do transaction boundaries. If other systems
should learn about the change, write an outbox event **in the same
transaction**. Debezium will publish it to `orderbook.events.customer`.

```go
type Service struct {
    repo    Repository
    events  Events
    tenants TenantProvisioner // tenant.Provisioner
    tx      database.Transactor
}

func (s *Service) Create(ctx context.Context, tenantID, email, name string) (*Customer, error) {
    if err := s.tenants.Ensure(ctx, tenantID); err != nil { // first write of a new tenant
        return nil, err
    }
    c := &Customer{TenantID: tenantID, ID: uuid.Must(uuid.NewV7()), Email: email, Name: name}
    err := s.tx.WithTx(ctx, func(ctx context.Context) error {
        if err := s.repo.Create(ctx, c); err != nil {
            return err
        }
        _, err := s.events.Add(ctx, outbox.Event{
            TenantID: tenantID, AggregateType: "customer", AggregateID: c.ID.String(),
            Type: "customer.created", Payload: CreatedEvent{CustomerID: c.ID, TenantID: tenantID, Email: email},
        })
        return err
    })
    if err != nil {
        return nil, err
    }
    return c, nil
}
```

Everything in that transaction targets one tenant, so on Citus it runs on a
single node with an ordinary commit. If you publish a new aggregate type,
create its topic in `deploy/kafka/create-topics.sh` and document the payload
next to it (see `internal/order/events.go`).

## 5. Handler: `internal/customer/handler.go`

```go
const (
    ScopeRead  = "customers:read"
    ScopeWrite = "customers:write"
)

func (h *Handler) Routes(r chi.Router) {
    r.Route("/customers", func(r chi.Router) {
        r.With(auth.RequireScopes(ScopeWrite)).Post("/", httpx.Handle(h.create))
        r.With(auth.RequireScopes(ScopeRead)).Get("/{id}", httpx.Handle(h.get))
    })
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) error {
    var req createRequest
    if err := httpx.Decode(r, &req); err != nil { // 400/415/422 handled for you
        return err
    }
    c, err := h.svc.Create(r.Context(), tenant.MustFromContext(r.Context()), req.Email, req.Name)
    if err != nil {
        return mapError(err)
    }
    w.Header().Set("Location", "/v1/customers/"+c.ID.String())
    httpx.JSON(w, r, http.StatusCreated, toResponse(c))
    return nil
}
```

The tenant always comes from `tenant.MustFromContext`, which reads the
verified token, never from the request body, path or headers. Keep
request/response DTOs separate from domain types, so the API contract
doesn't change when you refactor the domain.

## 6. Wire it up

`internal/app/router.go`: add a field to `RouterDeps` and mount the routes
**inside the tenant group**:

```go
r.Group(func(r chi.Router) {
    r.Use(tenant.Middleware(cfg.Tenancy.Claim))
    d.Catalog.Routes(r)
    d.Orders.Routes(r)
    d.Sales.Routes(r)
    d.Customers.Routes(r)
})
```

`internal/app/app.go`: construct it in `NewFeatureHandlers`, which the
integration tests reuse.

If the feature consumes events, add a `kafkax.Handler` and run another
`kafkax.Consumer` (with its own consumer group) in `RunWorker`. Make the
handler idempotent the way `sales.Projector` is: record the event id and
apply the effect in one transaction.

## 7. Identity provider

Create the `customers:read` / `customers:write` scopes in your IdP and grant
them to the right clients and roles. Locally, `deploy/mock-oauth2/config.json`
controls which scopes and `tenant_id` tokens get.

## 8. Tests

- `internal/customer/service_test.go`: fake ports plus a fake transactor
  (copy the pattern in `internal/order/service_test.go`).
- `internal/customer/handler_test.go`: chi router + `auth.Static` +
  `tenant.Middleware` (see `internal/order/handler_test.go`).
- `internal/customer/repository_integration_test.go`: `//go:build integration`,
  `TestMain` with `pgtest.Main`, `pg.NewDB(t)` per test. This runs on a
  real Citus. Include a cross-tenant isolation case and, for hot queries, an
  `EXPLAIN` assertion that the plan has `Task Count: 1` (see
  `TestTenantQueriesAreRouterQueries`).

```bash
make test && make test-integration
```

## 9. Document

- Add the endpoints and schemas to `docs/openapi.yaml`.
- Mention any new configuration in `docs/configuration.md`, and any new
  events in `docs/cdc.md`.

## Checklist

- [ ] Migration: `tenant_id` in every key, `create_distributed_table(... colocate_with => 'tenants')`, FKs after distributing, working `Down`
- [ ] Every query filters by `tenant_id` (and is a router query)
- [ ] Repository uses `db.Q(ctx)` and maps `ErrNoRows` and constraint errors
- [ ] Multi-write operations are wrapped in `WithTx`; events go through the outbox in the same transaction
- [ ] Routes are mounted behind `tenant.Middleware`; each has a `RequireScopes`
- [ ] Request DTOs have `validate` tags; responses use UTC timestamps
- [ ] Domain errors are mapped to stable `code`s
- [ ] Unit + integration tests (with tenant isolation)
- [ ] OpenAPI updated
