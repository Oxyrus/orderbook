//go:build integration

package sales_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/order"
	"github.com/Oxyrus/orderbook/internal/platform/kafkax"
	"github.com/Oxyrus/orderbook/internal/sales"
	"github.com/Oxyrus/orderbook/internal/testutil/pgtest"
)

var pg *pgtest.Server

func TestMain(m *testing.M) { pgtest.Main(m, &pg) }

func placed(t *testing.T, tenantID string, total int64, at time.Time) kafkax.Message {
	t.Helper()
	b, err := json.Marshal(order.PlacedEvent{OrderID: uuid.New(), TenantID: tenantID, Currency: "USD", TotalMinor: total, PlacedAt: at})
	require.NoError(t, err)
	return kafkax.Message{Value: b, Headers: map[string]string{
		"id": uuid.NewString(), "tenant_id": tenantID, "event_type": order.EventOrderPlaced,
	}}
}

func TestProjectionIsIdempotentAndCrossTenant(t *testing.T) {
	t.Parallel()
	db := pg.NewDB(t)
	ctx := context.Background()
	store := sales.NewPostgresStore(db)
	p := sales.NewProjector(store, db)
	day := time.Date(2026, 3, 14, 15, 0, 0, 0, time.UTC)

	for _, tn := range []string{"acme", "globex", "initech"} {
		_, err := db.Pool.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $1)`, tn)
		require.NoError(t, err)
	}

	m := placed(t, "acme", 1000, day)
	for range 3 { // redelivery
		require.NoError(t, p.Handle(ctx, m))
	}
	require.NoError(t, p.Handle(ctx, placed(t, "acme", 500, day.Add(time.Hour))))
	require.NoError(t, p.Handle(ctx, placed(t, "globex", 9000, day)))
	require.NoError(t, p.Handle(ctx, placed(t, "initech", 10, day)))

	d := time.Date(2026, 3, 14, 0, 0, 0, 0, time.UTC)
	rows, err := store.Daily(ctx, "acme", d, d)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.EqualValues(t, 2, rows[0].OrdersPlaced, "the redelivered event counted once")
	assert.EqualValues(t, 1500, rows[0].GrossMinor)

	top, err := store.TopTenants(ctx, d, d, "USD", 2)
	require.NoError(t, err)
	require.Len(t, top, 2)
	assert.Equal(t, "globex", top[0].TenantID)
	assert.Equal(t, "acme", top[1].TenantID)

	n, err := sales.PruneProcessed(ctx, db, time.Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t, 4, n)
}
