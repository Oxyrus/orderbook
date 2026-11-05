//go:build integration

package app_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/app"
	"github.com/Oxyrus/orderbook/internal/platform/logger"
)

func TestSeed(t *testing.T) {
	t.Parallel()
	db := pg.NewDB(t)
	ctx := context.Background()
	opts := app.SeedOptions{Tenants: []string{"acme", "globex"}, ProductsPerTenant: 5, OrdersPerTenant: 10, CancelRatio: 0.5, RandSeed: 7}

	res, err := app.Seed(ctx, db, logger.Nop(), opts)
	require.NoError(t, err)
	require.Len(t, res, 2)
	cancelled := 0
	for _, r := range res {
		assert.Equal(t, 5, r.ProductsCreated)
		assert.Equal(t, 10, r.OrdersPlaced)
		assert.NotEmpty(t, r.Node)
		cancelled += r.OrdersCancelled
	}

	count := func(q string) int {
		var n int
		require.NoError(t, db.Pool.QueryRow(ctx, q).Scan(&n))
		return n
	}
	assert.Equal(t, 10, count(`SELECT count(*) FROM products`))
	assert.Equal(t, 20, count(`SELECT count(*) FROM orders`))
	assert.Equal(t, 20+cancelled, count(`SELECT count(*) FROM outbox`), "one event per placement and per cancellation")
	assert.Equal(t, cancelled, count(`SELECT count(*) FROM orders WHERE status = 'cancelled'`))

	// Re-running reuses the catalog and adds orders.
	res, err = app.Seed(ctx, db, logger.Nop(), opts)
	require.NoError(t, err)
	assert.Zero(t, res[0].ProductsCreated)
	assert.Equal(t, 10, count(`SELECT count(*) FROM products`))
	assert.Equal(t, 40, count(`SELECT count(*) FROM orders`))
}

func TestSeedRejectsInvalidTenant(t *testing.T) {
	t.Parallel()
	_, err := app.Seed(context.Background(), pg.NewDB(t), logger.Nop(), app.SeedOptions{Tenants: []string{"Bad Tenant"}, ProductsPerTenant: 1})
	assert.ErrorContains(t, err, "invalid tenant id")
}
