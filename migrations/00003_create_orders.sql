-- +goose Up
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';

CREATE TABLE orders (
    tenant_id    text        NOT NULL,
    id           uuid        NOT NULL,
    customer_ref text        NOT NULL CHECK (char_length(customer_ref) BETWEEN 1 AND 200),
    status       text        NOT NULL CHECK (status IN ('placed', 'cancelled')),
    currency     char(3)     NOT NULL,
    total_minor  bigint      NOT NULL CHECK (total_minor >= 0),
    version      integer     NOT NULL DEFAULT 1,
    placed_at    timestamptz NOT NULL DEFAULT now(),
    cancelled_at timestamptz,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id)
);
SELECT create_distributed_table('orders', 'tenant_id', colocate_with => 'tenants');

ALTER TABLE orders
    ADD CONSTRAINT orders_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE;
ALTER TABLE orders
    ADD CONSTRAINT orders_currency_fkey FOREIGN KEY (currency) REFERENCES currencies (code);

-- Keyset pagination: WHERE tenant_id = $1 AND (placed_at, id) < ($2, $3)
CREATE INDEX orders_tenant_placed_idx ON orders (tenant_id, placed_at DESC, id DESC);

CREATE TABLE order_lines (
    tenant_id        text    NOT NULL,
    order_id         uuid    NOT NULL,
    line_no          integer NOT NULL CHECK (line_no >= 1),
    product_id       uuid    NOT NULL,
    sku              text    NOT NULL,
    name             text    NOT NULL,
    quantity         integer NOT NULL CHECK (quantity BETWEEN 1 AND 10000),
    unit_price_minor bigint  NOT NULL CHECK (unit_price_minor >= 0),
    PRIMARY KEY (tenant_id, order_id, line_no)
);
SELECT create_distributed_table('order_lines', 'tenant_id', colocate_with => 'tenants');

-- Co-located foreign keys: both sides include tenant_id, so they are
-- enforced shard-locally.
ALTER TABLE order_lines
    ADD CONSTRAINT order_lines_order_fkey FOREIGN KEY (tenant_id, order_id)
        REFERENCES orders (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE order_lines
    ADD CONSTRAINT order_lines_product_fkey FOREIGN KEY (tenant_id, product_id)
        REFERENCES products (tenant_id, id);

-- +goose Down
DROP TABLE IF EXISTS order_lines;
DROP TABLE IF EXISTS orders;
