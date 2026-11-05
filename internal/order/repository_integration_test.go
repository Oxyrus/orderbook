//go:build integration

package order_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/catalog"
	"github.com/Oxyrus/orderbook/internal/order"
	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/internal/platform/outbox"
	"github.com/Oxyrus/orderbook/internal/platform/tenant"
	"github.com/Oxyrus/orderbook/internal/testutil/pgtest"
)

var pg *pgtest.Server

func TestMain(m *testing.M) { pgtest.Main(m, &pg) }

type env struct {
	db       *database.DB
	catalog  *catalog.Service
	products *catalog.PostgresRepository
	orders   *order.Service
}

func newEnv(t *testing.T, events order.Events) env {
	t.Helper()
	t.Parallel()
	db := pg.NewDB(t)
	tenants := tenant.NewProvisioner(db)
	products := catalog.NewPostgresRepository(db)
	if events == nil {
		events = outbox.NewWriter(db)
	}
	return env{
		db:       db,
		catalog:  catalog.NewService(products, tenants),
		products: products,
		orders:   order.NewService(order.NewPostgresRepository(db), products, events, tenants, db),
	}
}

func (e env) product(t *testing.T, tenantID, sku string, price int64) *catalog.Product {
	t.Helper()
	p, err := e.catalog.Create(context.Background(), tenantID, catalog.CreateInput{
		SKU: sku, Name: sku, PriceMinor: price, Currency: "USD", Active: true,
	})
	require.NoError(t, err)
	return p
}

func (e env) explain(t *testing.T, query string, args ...any) string {
	t.Helper()
	rows, err := e.db.Pool.Query(context.Background(), "EXPLAIN (COSTS OFF) "+query, args...)
	require.NoError(t, err)
	var b strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		b.WriteString(line + "\n")
	}
	require.NoError(t, rows.Err())
	return b.String()
}

func TestPlaceAndGet(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	a, b := e.product(t, "acme", "A", 250), e.product(t, "acme", "B", 1000)

	o, err := e.orders.Place(ctx, "acme", order.PlaceInput{CustomerRef: "c-1", Lines: []order.LineInput{
		{ProductID: a.ID, Quantity: 4}, {ProductID: b.ID, Quantity: 1},
	}})
	require.NoError(t, err)
	assert.EqualValues(t, 2000, o.TotalMinor)
	assert.Equal(t, "USD", o.Currency)
	assert.False(t, o.PlacedAt.IsZero())

	got, err := e.orders.Get(ctx, "acme", o.ID)
	require.NoError(t, err)
	require.Len(t, got.Lines, 2)
	assert.Equal(t, "A", got.Lines[0].SKU)
	assert.EqualValues(t, 250, got.Lines[0].UnitPriceMinor)

	_, err = e.orders.Get(ctx, "globex", o.ID)
	assert.ErrorIs(t, err, order.ErrNotFound, "another tenant's id is simply not found")

	var typ, aggregate, payloadTenant string
	require.NoError(t, e.db.Pool.QueryRow(ctx,
		`SELECT type, aggregateid, payload->>'tenant_id' FROM outbox WHERE tenant_id = 'acme'`).Scan(&typ, &aggregate, &payloadTenant))
	assert.Equal(t, order.EventOrderPlaced, typ)
	assert.Equal(t, o.ID.String(), aggregate)
	assert.Equal(t, "acme", payloadTenant)
}

// The point of sharding by tenant: every per-tenant statement is routed to
// exactly one shard, while cross-tenant statements fan out to all of them.
func TestTenantQueriesAreRouterQueries(t *testing.T) {
	e := newEnv(t, nil)

	plan := e.explain(t, `SELECT * FROM orders WHERE tenant_id = 'acme' ORDER BY placed_at DESC LIMIT 20`)
	assert.Contains(t, plan, "Task Count: 1", plan)

	// Joins between co-located tables on the distribution column stay on
	// the shard too.
	plan = e.explain(t, `SELECT o.id, sum(l.quantity) FROM orders o
		JOIN order_lines l ON l.tenant_id = o.tenant_id AND l.order_id = o.id
		WHERE o.tenant_id = 'acme' GROUP BY o.id`)
	assert.Contains(t, plan, "Task Count: 1", plan)

	// A query without the tenant has to visit every shard.
	plan = e.explain(t, `SELECT tenant_id, count(*) FROM orders GROUP BY tenant_id`)
	assert.Contains(t, plan, "Task Count: 32", plan)
}

func TestTenantTablesAreColocated(t *testing.T) {
	e := newEnv(t, nil)
	rows, err := e.db.Pool.Query(context.Background(), `
		SELECT table_name::text, citus_table_type, colocation_id FROM citus_tables ORDER BY 1`)
	require.NoError(t, err)
	colocation := map[string]int{}
	for rows.Next() {
		var name, typ string
		var id int
		require.NoError(t, rows.Scan(&name, &typ, &id))
		if typ == "distributed" {
			colocation[name] = id
		} else {
			assert.Equal(t, "currencies", name, "the only reference table")
		}
	}
	require.NoError(t, rows.Err())
	for _, table := range []string{"tenants", "products", "orders", "order_lines", "outbox", "tenant_daily_sales", "processed_events"} {
		assert.Contains(t, colocation, table)
		assert.Equal(t, colocation["tenants"], colocation[table], "%s must be co-located with tenants", table)
	}
}

type failingEvents struct{}

func (failingEvents) Add(context.Context, ...outbox.Event) ([]uuid.UUID, error) {
	return nil, errors.New("outbox unavailable")
}

func TestPlaceIsAtomicWithItsEvent(t *testing.T) {
	e := newEnv(t, failingEvents{})
	p := e.product(t, "acme", "A", 100)

	_, err := e.orders.Place(context.Background(), "acme", order.PlaceInput{CustomerRef: "c", Lines: []order.LineInput{{ProductID: p.ID, Quantity: 1}}})
	require.Error(t, err)

	var n int
	require.NoError(t, e.db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM orders WHERE tenant_id = 'acme'`).Scan(&n))
	assert.Zero(t, n, "no order without its event")
}

func TestPlaceValidatesAgainstCatalog(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	usd := e.product(t, "acme", "USD-1", 100)
	eur, err := e.catalog.Create(ctx, "acme", catalog.CreateInput{SKU: "EUR-1", Name: "e", PriceMinor: 100, Currency: "EUR", Active: true})
	require.NoError(t, err)
	inactive, err := e.catalog.Create(ctx, "acme", catalog.CreateInput{SKU: "OFF", Name: "o", PriceMinor: 100, Currency: "USD", Active: false})
	require.NoError(t, err)
	other := e.product(t, "globex", "G", 100)

	place := func(ids ...uuid.UUID) error {
		var lines []order.LineInput
		for _, id := range ids {
			lines = append(lines, order.LineInput{ProductID: id, Quantity: 1})
		}
		_, err := e.orders.Place(ctx, "acme", order.PlaceInput{CustomerRef: "c", Lines: lines})
		return err
	}
	assert.ErrorIs(t, place(usd.ID, eur.ID), order.ErrMixedCurrencies)
	assert.ErrorIs(t, place(inactive.ID), order.ErrInactiveProduct)
	assert.ErrorIs(t, place(other.ID), order.ErrUnknownProduct, "products of other tenants are invisible")
	assert.ErrorIs(t, place(usd.ID, usd.ID), order.ErrDuplicateProduct)
}

func TestConcurrentCancelSucceedsOnce(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	p := e.product(t, "acme", "A", 100)
	o, err := e.orders.Place(ctx, "acme", order.PlaceInput{CustomerRef: "c", Lines: []order.LineInput{{ProductID: p.ID, Quantity: 1}}})
	require.NoError(t, err)

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { _, errs[i] = e.orders.Cancel(ctx, "acme", o.ID, 0) })
	}
	wg.Wait()

	ok := 0
	for _, err := range errs {
		if err == nil {
			ok++
		} else {
			assert.ErrorIs(t, err, order.ErrAlreadyCancelled)
		}
	}
	assert.Equal(t, 1, ok)

	var cancelled int
	require.NoError(t, e.db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE tenant_id = 'acme' AND type = $1`, order.EventOrderCanceled).Scan(&cancelled))
	assert.Equal(t, 1, cancelled, "exactly one order.cancelled event")
}

func TestCancelVersionCheck(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	p := e.product(t, "acme", "A", 100)
	o, err := e.orders.Place(ctx, "acme", order.PlaceInput{CustomerRef: "c", Lines: []order.LineInput{{ProductID: p.ID, Quantity: 1}}})
	require.NoError(t, err)

	_, err = e.orders.Cancel(ctx, "acme", o.ID, 7)
	assert.ErrorIs(t, err, order.ErrVersionConflict)
	_, err = e.orders.Cancel(ctx, "acme", uuid.New(), 0)
	assert.ErrorIs(t, err, order.ErrNotFound)

	c, err := e.orders.Cancel(ctx, "acme", o.ID, 1)
	require.NoError(t, err)
	assert.Equal(t, 2, c.Version)
	assert.NotNil(t, c.CancelledAt)
}

func TestListKeysetPagination(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	p := e.product(t, "acme", "A", 100)
	for range 5 {
		_, err := e.orders.Place(ctx, "acme", order.PlaceInput{CustomerRef: "c", Lines: []order.LineInput{{ProductID: p.ID, Quantity: 1}}})
		require.NoError(t, err)
	}
	seen := map[uuid.UUID]bool{}
	params := order.ListParams{TenantID: "acme", Limit: 2}
	for pages := 0; ; pages++ {
		require.Less(t, pages, 5)
		page, err := e.orders.List(ctx, params)
		require.NoError(t, err)
		for _, o := range page.Orders {
			assert.False(t, seen[o.ID])
			seen[o.ID] = true
		}
		if page.NextCursor == nil {
			break
		}
		params.After = page.NextCursor
	}
	assert.Len(t, seen, 5)

	page, err := e.orders.List(ctx, order.ListParams{TenantID: "globex", Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, page.Orders)
}

func TestOutboxRequiresTransaction(t *testing.T) {
	e := newEnv(t, nil)
	_, err := outbox.NewWriter(e.db).Add(context.Background(), outbox.Event{
		TenantID: "acme", AggregateType: "order", AggregateID: "x", Type: "t", Payload: 1,
	})
	assert.ErrorIs(t, err, outbox.ErrNoTransaction)
}
