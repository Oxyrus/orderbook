-- +goose Up
-- Orderbook is a multi-tenant (B2B SaaS) order management service. Data is
-- sharded with Citus by tenant: every tenant-owned table has a tenant_id
-- column, is hash-distributed on it, and is co-located with every other
-- tenant table. A tenant's rows therefore live on the same shard (and node),
-- so per-tenant queries, joins, foreign keys and transactions are routed to a
-- single node and behave like plain PostgreSQL. See docs/sharding.md.
--
-- Migrations run against the coordinator only. Citus propagates DDL on
-- distributed and reference tables to every worker.

-- Managed offerings (Azure Cosmos DB for PostgreSQL, etc.) pre-install the
-- extension; the citusdata/citus image creates it in POSTGRES_DB too.
CREATE EXTENSION IF NOT EXISTS citus;

-- Reference tables are small, rarely-written lookup tables replicated in
-- full to every node, so any shard can join to (or reference) them locally.
CREATE TABLE currencies (
    code        char(3)  PRIMARY KEY,
    minor_units smallint NOT NULL CHECK (minor_units BETWEEN 0 AND 4),
    name        text     NOT NULL
);
SELECT create_reference_table('currencies');

INSERT INTO currencies (code, minor_units, name) VALUES
    ('USD', 2, 'US Dollar'),
    ('EUR', 2, 'Euro'),
    ('GBP', 2, 'Pound Sterling'),
    ('COP', 2, 'Colombian Peso'),
    ('JPY', 0, 'Yen');

-- +goose Down
DROP TABLE IF EXISTS currencies;
