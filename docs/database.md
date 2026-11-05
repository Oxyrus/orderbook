# Database

PostgreSQL with the [Citus](https://github.com/citusdata/citus) extension,
through [pgx/v5](https://github.com/jackc/pgx), using its native interface
(not `database/sql`) for performance and full type support. The code is in
`internal/platform/database`. The application connects to the Citus
**coordinator** like any PostgreSQL server. How the data is sharded, and the
rules that imposes on schema and queries, is in [sharding.md](sharding.md).

## Connection pool

`database.Connect` builds a `pgxpool.Pool` from `DB_*` settings
([configuration](configuration.md#database)), pings it, and fails fast if
the database is unreachable. With tracing enabled, every query becomes an
OpenTelemetry span (`otelpgx`). Pool statistics are exported as Prometheus
metrics (`db_pool_*`).

Sizing rule of thumb: `DB_MAX_CONNS × replicas ≤ max_connections − 10%` on
the coordinator. Remember that the coordinator itself opens connections to
the workers for distributed queries (bounded by `citus.max_shared_pool_size`).
Beyond a few dozen replicas, put PgBouncer (transaction mode) in front.
pgx works with it if you add `default_query_exec_mode=exec` (or
`simple_protocol`) to `DB_URL`.

## Repositories and `db.Q(ctx)`

Repositories receive `*database.DB` and **always** get their executor from
`db.Q(ctx)`:

```go
func (r *PostgresRepository) Get(ctx context.Context, tenantID string, id uuid.UUID) (*Product, error) {
    row := r.db.Q(ctx).QueryRow(ctx, `SELECT ... FROM products WHERE tenant_id = $1 AND id = $2`, tenantID, id)
    ...
}
```

`Q(ctx)` returns the `pgx.Tx` stored in `ctx` if the caller opened a
transaction, and the pool otherwise. The same repository method therefore
works both inside and outside transactions, and repositories never manage
transactions themselves.

`Q` returns the `database.DBTX` interface (`Exec`, `Query`, `QueryRow`,
`SendBatch`, `CopyFrom`), the method set shared by the pool, connections and
transactions. sqlc-generated code accepts the same interface.

Conventions:

- Translate `pgx.ErrNoRows` into a domain error (`ErrNotFound`).
- Every query of a tenant-owned table filters on (or inserts) `tenant_id`:
  that is both tenant isolation and Citus shard routing.
- Translate constraint violations into domain errors with
  `database.IsUniqueViolation(err, "constraint_name")` or
  `database.IsForeignKeyViolation(err, "constraint_name")`. Both also match
  the shard-suffixed names Citus reports (`products_tenant_sku_key_102043`).
- Wrap all other errors with context: `fmt.Errorf("insert order: %w", err)`.
- Use `RETURNING` to get server-generated values (timestamps, versions) in
  one round-trip.
- Use `pgx.CollectRows` with a scan function for lists.

## Transactions

Services own transaction boundaries:

```go
err := s.tx.WithTx(ctx, func(ctx context.Context) error {
    if err := s.repo.Create(ctx, o); err != nil {     // uses the tx
        return err                                    // → ROLLBACK
    }
    _, err := s.events.Add(ctx, orderPlaced(o))       // outbox, same tx
    return err
})                                                    // nil → COMMIT
```

Services depend on the tiny `database.Transactor` interface, so unit tests
can pass a fake that just calls `fn(ctx)` (see `internal/order/service_test.go`).

On Citus, a transaction whose statements all target the same tenant runs on
one node with an ordinary commit. Touching several tenants makes it a
distributed transaction (two-phase commit across workers). That is correct
but slower, so keep request paths single-tenant.

### Semantics

| Situation | Behaviour |
|---|---|
| `fn` returns `nil` | `COMMIT` (a commit error is returned) |
| `fn` returns an error | `ROLLBACK`; the error is returned unchanged (`errors.Is` works) |
| `fn` panics | `ROLLBACK`, then the panic is re-raised (the HTTP recoverer turns it into a 500) |
| `ctx` is cancelled | pgx aborts the query. Rollback still runs (on a non-cancellable context) |
| `WithTx` called inside `WithTx` | The inner call **joins** the outer transaction. The outermost call decides commit/rollback. |

Joining makes services composable. `A.DoThing` can call `B.DoOther`, and
both run in one transaction when called together, or in their own when
called alone.

**Always use the `ctx` passed to `fn`.** Using the outer `ctx` inside `fn`
runs that query outside the transaction, on another connection.

### Isolation levels and retries

```go
err := db.WithTxOptions(ctx, database.TxOptions{
    IsoLevel:   pgx.Serializable,
    MaxRetries: 5,
}, func(ctx context.Context) error {
    // read-modify-write logic
})
```

With `MaxRetries > 0`, the **whole function** is re-run after a
serialization failure (`40001`) or deadlock (`40P01`), with jittered
exponential backoff (10ms, 20ms, 40ms … capped at about 640ms). This is the
recommended way to enforce invariants that span several rows without
explicit locks. `fn` must not have non-database side effects (sending
emails, publishing messages) because it may run more than once. Do those
after the commit, or use an outbox table.

`TxOptions{AccessMode: pgx.ReadOnly}` is useful for consistent multi-query
reads, e.g. under `RepeatableRead`.

### Savepoints

To let part of a transaction fail without aborting the rest:

```go
db.WithTx(ctx, func(ctx context.Context) error {
    _ = repo.Create(ctx, a)
    err := database.Savepoint(ctx, func(ctx context.Context) error {
        return repo.Create(ctx, maybeDuplicate) // rolled back alone on error
    })
    // a is still pending commit regardless of err
    return nil
})
```

### Pessimistic locking

When contention is high and retries are expensive, lock rows explicitly
inside a transaction:

```sql
SELECT ... FROM accounts WHERE id = $1 FOR UPDATE;          -- wait for the lock
SELECT ... FROM jobs WHERE status = 'pending'
  ORDER BY id LIMIT 10 FOR UPDATE SKIP LOCKED;              -- work queues
```

### Optimistic locking

`products` and `orders` show the HTTP-friendly approach: a `version` column,
`UPDATE … SET version = version + 1 WHERE tenant_id = $1 AND id = $2 AND version = $3`.
Clients send back the `ETag` they read in `If-Match` (`412` when it is stale)
or the `version` in the body (`409` when it is stale). Cancelling an order
takes an optional expected version the same way (skipped when it is 0),
driven by `If-Match`.

## Migrations

Migrations are plain SQL files in `migrations/`, managed by
[goose](https://github.com/pressly/goose) and **embedded into the binary**
(`migrations.FS`). Each deployed artifact therefore carries exactly the
schema it expects.

### Creating one

```bash
make migration name=add_invoices
# → migrations/00007_add_invoices.sql (sequential numbering via goose -s)
```

Tables on Citus follow extra rules: distribute before adding foreign keys,
one `ADD CONSTRAINT` per `ALTER TABLE`, `tenant_id` in every key, sequential
mode when creating several tables. A complete template is in
[sharding.md](sharding.md#writing-migrations-for-citus).

- Each migration runs in a transaction. For statements that can't run in
  one (`CREATE INDEX CONCURRENTLY`), add `-- +goose NO TRANSACTION`.
- Wrap multi-statement PL/pgSQL (functions, `DO` blocks) in
  `-- +goose StatementBegin` / `-- +goose StatementEnd`.
- **Always write a working `Down`.** The integration test
  `TestMigrateCommands` (cmd/api) rolls everything back and applies it
  again, so a broken rollback fails CI.
- Sequential numbering (`-s`) avoids the ordering problems timestamped
  migrations cause across branches. If two branches add the same number, CI
  fails and the second one renumbers.

### Running them

```bash
api migrate up               # apply pending
api migrate status           # what's applied
api migrate down             # roll back one
api migrate down-to 1        # roll back to version 1
api migrate up-to 3
api migrate version
```

(`make migrate-up` etc. wrap these using your `.env`.)

Migrations run against the coordinator only; Citus propagates DDL on
distributed and reference tables to the workers. The migrator takes a
**PostgreSQL advisory lock**, so if several replicas
start with `DB_AUTO_MIGRATE=true` at once, one migrates and the others wait.
For production strategies, see [deployment](deployment.md#running-migrations).

### Writing zero-downtime migrations

During a rolling deploy, old and new code run against the same schema at the
same time. Use **expand → migrate → contract**:

1. *Expand*: add the new nullable column/table. Old code ignores it.
2. Deploy code that writes both and reads the new one. Backfill in batches.
3. *Contract*: in a later release, drop the old column and add constraints.

Avoid in a single step: renaming or dropping columns that running code still
uses, `ALTER TABLE … ADD COLUMN … DEFAULT volatile()` on huge tables, and
non-concurrent index creation on hot tables. Set `lock_timeout` in risky
migrations:

```sql
-- +goose Up
SET lock_timeout = '5s';
ALTER TABLE products ADD COLUMN archived_at timestamptz;
```

## Schema conventions used here

- `uuid` primary keys generated in Go (UUIDv7). See
  [architecture](architecture.md#ids).
- `timestamptz` everywhere, never `timestamp`. The API returns UTC RFC 3339.
- `text` with `CHECK` constraints instead of `varchar(n)`.
- Named constraints (`products_tenant_sku_key`), so code can match them.
- Tenant tables: `tenant_id` first in the primary key, distributed on it,
  co-located with `tenants`.
- Indexes that match query patterns, e.g. `(tenant_id, created_at DESC, id DESC)`
  for keyset pagination.
- An `outbox` table written in the same transaction as the change it
  announces ([cdc.md](cdc.md)).
