# Sharding with Citus

Orderbook is a multi-tenant B2B service: many merchants (tenants), each with
their own catalog, orders and reports. The data is sharded by tenant across
PostgreSQL nodes with [Citus](https://github.com/citusdata/citus). This
document explains the model, the rules it imposes on schema and queries, how
it is tested and how to operate it.

- [Why shard, and why Citus](#why-shard-and-why-citus)
- [Topology](#topology)
- [The data model](#the-data-model)
- [Query patterns](#query-patterns)
- [Writing migrations for Citus](#writing-migrations-for-citus)
- [Adding a tenant-scoped feature](#adding-a-tenant-scoped-feature)
- [Testing](#testing)
- [Operations](#operations)
- [Limits and trade-offs](#limits-and-trade-offs)

## Why shard, and why Citus

A single PostgreSQL primary scales a long way. Shard when one of these
becomes true:

- the working set or write volume outgrows the largest machine you are
  willing to run (or pay for);
- a few large tenants degrade everybody else (noisy neighbours) and you need
  to move them onto their own hardware;
- maintenance (vacuum, index builds, backups/restores) on one huge database
  takes too long.

Multi-tenant SaaS is the textbook case for sharding, because almost every
query is scoped to one tenant. If every tenant's rows live together, almost
every query touches one node.

Citus is a PostgreSQL extension, not a fork. The application keeps using
pgx, SQL, transactions, foreign keys and migrations. Citus also handles the
parts that make home-grown application-level sharding expensive:

| Concern | Application-level sharding | Citus |
|---|---|---|
| Routing a query to the right database | Your code (a shard map, N pools) | Coordinator, from the distribution column in the `WHERE` clause |
| Cross-tenant reports | Fan out by hand, merge in code | Parallel distributed query, merged on the coordinator |
| Schema changes | Run on every shard, handle partial failure | One `ALTER TABLE` on the coordinator, propagated |
| Moving tenants / rebalancing | Custom copy + cutover tooling | `citus_rebalance_start()`, `citus_move_shard_placement()`, online |
| Isolating a big tenant | New database + migration | `isolate_tenant_to_new_shard()` + move it to a dedicated node |
| Transactions across shards | Avoid or build 2PC yourself | Built-in 2PC (but avoid it in the hot path, see below) |

The alternatives are fine choices too: Vitess (MySQL), CockroachDB/YugabyteDB
(distributed SQL with PostgreSQL wire compatibility but different semantics),
or plain Postgres with one database per tenant (simple, but painful past a
few hundred tenants). Citus keeps you on real PostgreSQL. It is available
managed (Azure Cosmos DB for PostgreSQL) or self-hosted: on VMs with Patroni
for failover, or on Kubernetes with an operator that supports sharded
clusters, such as [StackGres](https://stackgres.io/).

## Topology

```
                         ┌────────────────────── Citus cluster ──────────────────────┐
  api / worker ──SQL──▶  │  coordinator  (metadata: which shard lives where,         │
  (DB_URL)               │               reference tables, goose_db_version)         │
                         │       │ routes single-tenant queries to one shard          │
                         │       │ parallelises cross-tenant queries over all shards  │
                         │       ▼                                                   │
                         │  worker1  shards 102008, 102010, ... (16 of 32 per table) │
                         │  worker2  shards 102009, 102011, ... (16 of 32 per table) │
                         └───────────────────────────────────────────────────────────┘
```

- The application connects **only to the coordinator**. It never knows
  about workers.
- `docker compose up` starts a coordinator and two workers.
  `deploy/citus/init-cluster.sql` registers them (`make up` runs it).
- Each distributed table is split into 32 hash shards (Citus' default
  `citus.shard_count`). Shards are ordinary PostgreSQL tables named
  `<table>_<shardid>` on the workers. The shard count is fixed when a table is
  distributed, so choose it for the cluster size you expect, not today's: a
  common rule of thumb is 2-4× the number of CPU cores you will ever have
  across workers.

Look around:

```bash
make citus-tables          # distributed / reference tables, co-location groups
make citus-nodes           # shards and bytes per node
make citus-tenant t=acme   # where tenant "acme" lives
make psql                  # coordinator;  make psql-worker n=1 for a worker
```

## The data model

### Distribution column: `tenant_id`

Every tenant-owned table has a `tenant_id` column and is hash-distributed on
it:

```sql
SELECT create_distributed_table('orders', 'tenant_id', colocate_with => 'tenants');
```

Choosing the distribution column is the most important (and hardest to
change) decision. `tenant_id` works because:

- nearly every query filters by it (the request's tenant comes from the
  access token), so queries go to a single shard;
- it has high cardinality and spreads evenly (many tenants);
- related rows (an order and its lines, a product, the outbox event)
  share it, so joins and foreign keys stay inside a shard.

### Co-location

All tenant tables are in **one co-location group** (`colocate_with =>
'tenants'`). Shard *N* of every table covers the same hash range, and the
shards of one group are always placed on the same node. A tenant's
`products`, `orders`, `order_lines`, `outbox`, `tenant_daily_sales` and
`processed_events` therefore sit together, and Citus can push down:

- joins on `tenant_id` (plus other keys);
- foreign keys that include `tenant_id` (`order_lines (tenant_id, order_id)
  → orders (tenant_id, id)`);
- transactions touching several tables of the same tenant. **These run on
  one node with no 2PC.** This is what makes the
  [transactional outbox](cdc.md) cheap: the order, its lines and its event
  commit atomically on one shard.

`TestTenantTablesAreColocated` (internal/order) fails if a new table forgets
`colocate_with`.

### Reference tables

`currencies` is a **reference table**: small, rarely written, replicated in
full to every node (`create_reference_table`). Distributed tables can have
foreign keys to it and join it locally. Use reference tables for lookup data
shared by all tenants: currencies, countries, plans, feature flags. Writes
to them are 2PC across all nodes, so they must stay rare.

### The tenants table

`tenants` is distributed by its primary key `id`. It is the root of the
co-location group that every other table references with
`FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE`. Tenant
rows are provisioned just in time by `tenant.Provisioner` the first time a
tenant writes. The identity provider is the source of truth for which
tenants exist. Replace it with explicit onboarding when tenants need more
than an id.

### Keys and constraints

Citus enforces uniqueness per shard, so **every primary key and unique
constraint must include the distribution column**:

```sql
PRIMARY KEY (tenant_id, id)                          -- not PRIMARY KEY (id)
CONSTRAINT products_tenant_sku_key UNIQUE (tenant_id, sku)   -- SKUs unique per tenant
```

IDs are UUIDv7 (time-ordered, so B-tree inserts stay local, and generated
by the application, so no cross-node sequence is involved). The API still
addresses resources by `id` alone (`/v1/orders/{id}`). The tenant comes from
the token and is added to every query.

## Query patterns

Every repository method takes the tenant id and puts `tenant_id = $1` in
the `WHERE` clause (or the `INSERT`). This is both the **isolation boundary**
(a tenant can never see another's rows, because the tenant comes from the
verified token, never from the request) and the **routing key**.

| Pattern | Example | What Citus does |
|---|---|---|
| Router query | `SELECT ... FROM orders WHERE tenant_id = $1 AND id = $2` | Sends the query, unchanged apart from the shard name, to the one node holding that tenant's shard. Same latency as plain Postgres |
| Co-located join | `orders JOIN order_lines USING (tenant_id) WHERE tenant_id = $1` | Pushed down to the shard |
| Single-tenant transaction | `Place`: lock products, insert order + lines + outbox | Runs entirely on one node, single-phase commit |
| Multi-shard query | `GROUP BY tenant_id` over all tenants (`/v1/platform/sales/top-tenants`) | Runs on all 32 shards in parallel, merges partial aggregates on the coordinator |
| Multi-shard write | `DELETE FROM outbox WHERE created_at < ...` (pruning) | Parallel on all shards; uses 2PC when it spans nodes |

`EXPLAIN` shows which one you got:

```
Custom Scan (Citus Adaptive)
  Task Count: 1          ← router query (good for per-request traffic)
  Task Count: 32         ← fans out to every shard
```

`TestTenantQueriesAreRouterQueries` asserts this for the main access paths.
Add a similar assertion when you add a hot query.

Guidelines:

- **Always filter by tenant in request paths.** A forgotten `tenant_id`
  filter is a data leak *and* a 32× fan-out.
- Keep cross-tenant queries out of the request path of tenant APIs. Serve
  them from platform/operator endpoints, background jobs or a warehouse fed
  by [CDC](cdc.md#use-case-2-row-level-cdc-for-analytics-and-search).
- Avoid multi-tenant transactions. They work (2PC), but they are slower,
  hold locks on several nodes, and their changes reach CDC consumers from
  different nodes in no particular order.
- `FOR UPDATE` / `FOR SHARE` work on router queries (`LockForOrder`).
- Keyset pagination stays a router query because the cursor includes
  `tenant_id` implicitly (the filter) and the index is
  `(tenant_id, placed_at DESC, id DESC)`.

## Writing migrations for Citus

Migrations run on the coordinator (`api migrate up`, or auto-migrate). Citus
propagates DDL on distributed and reference tables to every worker. Rules
learned the hard way (each one is a real error message you would hit):

1. **Distribute first, add foreign keys after.** A table that is still
   local cannot reference a distributed one: *"cannot create foreign key
   constraint since foreign keys from reference tables and local tables to
   distributed tables are not supported"*. Create the table without the
   FKs, call `create_distributed_table`, then `ALTER TABLE ... ADD
   CONSTRAINT`.
2. **One `ADD CONSTRAINT` per `ALTER TABLE`**: *"cannot execute ADD
   CONSTRAINT command with other subcommands"*.
3. **Use sequential mode when a migration creates and links several
   tables** in one transaction (goose wraps each file in one):
   `SET LOCAL citus.multi_shard_modify_mode TO 'sequential';`. Otherwise
   Citus opens one connection per shard for the first statement and then
   refuses DDL that has to reuse them.
4. **Include `tenant_id` in every primary key, unique constraint and
   foreign key between distributed tables.**
5. **Never `CREATE PUBLICATION ... FOR ALL TABLES`**. It would include
   Citus' metadata tables. Name the tables (see [CDC](cdc.md)).
6. Data migrations that touch every tenant (`UPDATE orders SET ...`) are
   multi-shard. On large tables, batch them by tenant or run them with
   `run_command_on_shards` / `citus.max_adaptive_executor_pool_size` tuned,
   off-peak.
7. `alter_distributed_table` (changing shard count, distribution column or
   co-location) rewrites the table and **breaks Debezium's replication
   stream**. Plan it like a re-platforming.

Template for a new tenant table:

```sql
-- +goose Up
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';

CREATE TABLE invoices (
    tenant_id  text        NOT NULL,
    id         uuid        NOT NULL,
    order_id   uuid        NOT NULL,
    amount     bigint      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id)
);
SELECT create_distributed_table('invoices', 'tenant_id', colocate_with => 'tenants');

ALTER TABLE invoices
    ADD CONSTRAINT invoices_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE;
ALTER TABLE invoices
    ADD CONSTRAINT invoices_order_fkey FOREIGN KEY (tenant_id, order_id) REFERENCES orders (tenant_id, id);

CREATE INDEX invoices_tenant_created_idx ON invoices (tenant_id, created_at DESC, id DESC);

-- +goose Down
DROP TABLE IF EXISTS invoices;
```

Tables that are not tenant-scoped (global configuration, job queues used by
operators) can stay **local tables on the coordinator**. They cannot have
foreign keys to or from distributed tables, and joining them with
distributed tables is limited. Prefer reference tables if shards need them.

## Adding a tenant-scoped feature

Follow [adding-a-feature.md](adding-a-feature.md), plus:

1. Migration as above (`tenant_id`, distributed, co-located, keys include
   `tenant_id`).
2. Every repository method takes `tenantID string` and filters on it.
3. Handlers get the tenant with `tenant.MustFromContext(r.Context())` and
   are mounted in the tenant group in `internal/app/router.go`
   (behind `tenant.Middleware`).
4. Services call `tenants.Ensure(ctx, tenantID)` before the first write of
   a tenant (or rely on the tenant already existing).
5. State changes that other systems care about write an
   [outbox](cdc.md) event in the same `WithTx`.
6. Add an `EXPLAIN` router-query assertion for the hot paths.

## Testing

- **Integration tests run on real Citus.** `internal/testutil/pgtest`
  starts a single-node `citusdata/citus` container (the coordinator holds
  all shards). This exercises the distributed planner, the DDL rules above
  and shard-level constraint errors without a multi-node cluster. Every
  test gets its own database cloned from a migrated template in
  milliseconds. Because Citus' maintenance daemon keeps a connection to
  every database with the extension, the template is marked
  `ALLOW_CONNECTIONS false` and its backends are terminated before cloning.
- **Errors from shards** name the shard's constraint
  (`products_tenant_sku_key_102043`). `database.IsUniqueViolation` and
  `IsForeignKeyViolation` match the logical name with or without the shard
  suffix.
- **The multi-node behaviour** (tenants on different workers, CDC from each
  worker) is covered by the end-to-end test against docker compose:
  `make up-app && make e2e`. It picks a fresh tenant on every worker and
  checks the full path.
- The `citusdata/citus` image is published for `linux/amd64` only. On Apple
  Silicon it runs emulated. That works, but it is slower; Colima users can
  enable Rosetta (`colima start --vm-type vz --vz-rosetta`) for a large
  speed-up.

## Operations

### Adding a worker and rebalancing

```sql
-- on the coordinator
SELECT citus_add_node('worker3', 5432);
-- BEFORE moving shards: create the CDC slots on the new node (docs/cdc.md#adding-a-worker)
SELECT citus_rebalance_start();          -- online, in the background (make citus-rebalance)
SELECT * FROM citus_rebalance_status();
```

Shard moves use logical replication and block writes to a shard only
briefly at the end. Rebalance during low traffic and watch replication lag.

### Isolating a large tenant

When one tenant outgrows its neighbours:

```sql
-- give the tenant a shard of its own (in every co-located table)
SELECT isolate_tenant_to_new_shard('tenants', 'bigcorp', 'CASCADE');
-- then move that shard to a dedicated node
SELECT citus_move_shard_placement(<new shard id>, 'worker1', 5432, 'worker-big', 5432);
```

### Connection management

The coordinator opens connections to workers for every distributed query.
In production:

- put **PgBouncer** (transaction pooling) in front of the coordinator for the
  application pools, and keep `DB_MAX_CONNS` × replicas within its limits;
- tune `citus.max_shared_pool_size` and
  `citus.max_adaptive_executor_pool_size` on the coordinator to bound
  coordinator→worker connections;
- Citus 11+ syncs the metadata to every worker, so at very high
  throughput the application can also connect to the workers and run
  distributed queries from any node, which takes load off the coordinator.

### High availability and backups

Every node is a normal PostgreSQL server. Give each one a streaming replica
and automated failover (Patroni, a Kubernetes operator, or the managed
service).
Back up every node consistently: managed services do it for you. Self-hosted
clusters can use `citus_create_restore_point()` with per-node PITR
(pgBackRest/WAL-G) so the restore points line up across nodes.

### Schema-based sharding (alternative)

Citus 12+ also supports **schema-based sharding**
(`citus.enable_schema_based_sharding`): each tenant gets its own schema,
and Citus places each schema on one node. It suits few, large tenants with
custom schemas, or migrating a schema-per-tenant app. Row-based sharding
(this project) suits many small-to-medium tenants with a shared schema, and
gives efficient cross-tenant queries.

## Limits and trade-offs

- The **distribution column is effectively permanent**. Changing it means
  `alter_distributed_table` (a rewrite) or a new table plus a migration.
- **Global uniqueness** across tenants (e.g. a globally unique email) cannot
  be a constraint. Use a reference table (if small), a separate
  service-level check, or a dedicated table distributed by that value.
- **Cross-tenant queries** are parallel but still touch every shard. Keep
  them off hot paths, or serve them from a warehouse.
- **Sequences**: `bigserial` works on distributed tables, but values come
  from the coordinator (or ranges per node). App-generated UUIDv7 avoids the
  question.
- Some PostgreSQL features have restrictions on distributed tables: certain
  correlated subqueries across shards, `CREATE INDEX CONCURRENTLY` (supported,
  but run per shard), triggers (allowed, but `citus.enable_unsafe_triggers`
  is needed and they run on shards). Check the Citus docs before relying on
  something exotic.
