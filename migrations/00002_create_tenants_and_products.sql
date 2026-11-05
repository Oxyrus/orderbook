-- +goose Up
-- Creating, distributing and linking several tables in one transaction makes
-- Citus open one connection per shard placement in parallel, after which it
-- refuses further DDL that has to reuse those shards' connections. Sequential
-- mode keeps a single connection per node for the whole transaction.
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';

-- tenants is the root of the co-location group: it is distributed on its
-- primary key, and every other tenant table is distributed on tenant_id
-- with colocate_with => 'tenants', so shard N of every table holds the same
-- hash range of tenant ids.
CREATE TABLE tenants (
    id         text        PRIMARY KEY CHECK (id ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
    name       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
SELECT create_distributed_table('tenants', 'id');

-- Primary keys and unique constraints on distributed tables must include the
-- distribution column: uniqueness is enforced per shard.
CREATE TABLE products (
    tenant_id   text        NOT NULL,
    id          uuid        NOT NULL,
    sku         text        NOT NULL CHECK (char_length(sku) BETWEEN 1 AND 64),
    name        text        NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    price_minor bigint      NOT NULL CHECK (price_minor >= 0),
    currency    char(3)     NOT NULL,
    active      boolean     NOT NULL DEFAULT true,
    version     integer     NOT NULL DEFAULT 1,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT products_tenant_sku_key UNIQUE (tenant_id, sku)
);
SELECT create_distributed_table('products', 'tenant_id', colocate_with => 'tenants');

-- Foreign keys are added after distributing: Citus does not allow a still
-- local table to reference a distributed one. Distributed -> distributed
-- keys must include the distribution column (so the referenced row is on the
-- same shard); distributed -> reference keys are always allowed.
ALTER TABLE products
    ADD CONSTRAINT products_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE;
ALTER TABLE products
    ADD CONSTRAINT products_currency_fkey FOREIGN KEY (currency) REFERENCES currencies (code);

CREATE INDEX products_tenant_created_idx ON products (tenant_id, created_at DESC, id DESC);

-- +goose Down
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS tenants;
