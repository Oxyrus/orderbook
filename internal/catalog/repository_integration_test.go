//go:build integration

package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/catalog"
	"github.com/Oxyrus/orderbook/internal/platform/tenant"
	"github.com/Oxyrus/orderbook/internal/testutil/pgtest"
)

var pg *pgtest.Server

func TestMain(m *testing.M) { pgtest.Main(m, &pg) }

func newService(t *testing.T) *catalog.Service {
	t.Helper()
	t.Parallel()
	db := pg.NewDB(t)
	return catalog.NewService(catalog.NewPostgresRepository(db), tenant.NewProvisioner(db))
}

func input(sku string) catalog.CreateInput {
	return catalog.CreateInput{SKU: sku, Name: "n", PriceMinor: 100, Currency: "USD", Active: true}
}

func TestCreateGetUpdate(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()

	p, err := svc.Create(ctx, "acme", input("A"))
	require.NoError(t, err)
	assert.Equal(t, 1, p.Version)

	got, err := svc.Get(ctx, "acme", p.ID)
	require.NoError(t, err)
	assert.Equal(t, "A", got.SKU)
	_, err = svc.Get(ctx, "globex", p.ID)
	assert.ErrorIs(t, err, catalog.ErrNotFound)

	in := input("A2")
	in.PriceMinor = 999
	up, err := svc.Update(ctx, "acme", p.ID, catalog.UpdateInput{CreateInput: in, Version: 1})
	require.NoError(t, err)
	assert.Equal(t, 2, up.Version)
	assert.EqualValues(t, 999, up.PriceMinor)

	_, err = svc.Update(ctx, "acme", p.ID, catalog.UpdateInput{CreateInput: in, Version: 1})
	assert.ErrorIs(t, err, catalog.ErrVersionConflict)
	_, err = svc.Update(ctx, "globex", p.ID, catalog.UpdateInput{CreateInput: in, Version: 2})
	assert.ErrorIs(t, err, catalog.ErrNotFound)
}

// Constraint violations raised on a shard name the shard's constraint
// (products_tenant_sku_key_1020xx); the repository must still recognise them.
func TestConstraintErrorsFromShards(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()

	_, err := svc.Create(ctx, "acme", input("DUP"))
	require.NoError(t, err)
	_, err = svc.Create(ctx, "acme", input("DUP"))
	assert.ErrorIs(t, err, catalog.ErrSKUTaken)
	_, err = svc.Create(ctx, "globex", input("DUP"))
	assert.NoError(t, err, "SKUs are unique per tenant")

	bad := input("CUR")
	bad.Currency = "ZZZ"
	_, err = svc.Create(ctx, "acme", bad)
	assert.ErrorIs(t, err, catalog.ErrUnknownCurrency)
}
