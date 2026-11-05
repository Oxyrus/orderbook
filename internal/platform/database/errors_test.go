package database

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestConstraintMatchesShardNames(t *testing.T) {
	assert.True(t, constraintMatches("products_tenant_sku_key", "products_tenant_sku_key"))
	assert.True(t, constraintMatches("products_tenant_sku_key_102043", "products_tenant_sku_key"), "Citus shard constraint")
	assert.True(t, constraintMatches("anything", ""))
	assert.False(t, constraintMatches("products_tenant_sku_key_x", "products_tenant_sku_key"))
	assert.False(t, constraintMatches("products_tenant_sku_key_", "products_tenant_sku_key"))
	assert.False(t, constraintMatches("other_key_1", "products_tenant_sku_key"))

	err := fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: CodeUniqueViolation, ConstraintName: "orders_pkey_102008"})
	assert.True(t, IsUniqueViolation(err, "orders_pkey"))
	assert.False(t, IsForeignKeyViolation(err, ""))
	fk := &pgconn.PgError{Code: CodeForeignKeyViolation, ConstraintName: "products_currency_fkey_102040"}
	assert.True(t, IsForeignKeyViolation(fk, "products_currency_fkey"))
}
