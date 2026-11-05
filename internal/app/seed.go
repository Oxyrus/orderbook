package app

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/Oxyrus/orderbook/internal/catalog"
	"github.com/Oxyrus/orderbook/internal/order"
	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/internal/platform/httpx"
	"github.com/Oxyrus/orderbook/internal/platform/outbox"
	"github.com/Oxyrus/orderbook/internal/platform/tenant"
)

// DefaultSeedTenants are the tenants `api seed` creates when none are given.
// Their ids hash to shards spread over the workers.
var DefaultSeedTenants = []string{"acme", "globex", "initech", "umbrella", "hooli", "soylent", "wonka", "stark"}

// SeedOptions controls the demo data.
type SeedOptions struct {
	Tenants           []string
	ProductsPerTenant int
	OrdersPerTenant   int
	// CancelRatio is the fraction of new orders cancelled right after
	// being placed (0..1).
	CancelRatio float64
	// RandSeed makes the shape of the data reproducible.
	RandSeed uint64
}

// SeedResult summarises what Seed wrote for one tenant.
type SeedResult struct {
	Tenant          string
	Node            string // the worker holding the tenant's shard
	Currency        string
	ProductsCreated int
	OrdersPlaced    int
	OrdersCancelled int
}

// seedCurrencies gives each tenant a single currency (orders can't mix).
var seedCurrencies = []string{"USD", "USD", "USD", "EUR", "GBP", "COP"}

// Seed fills the database with demo data through the real services, so
// every order also writes its outbox event. With the Debezium connectors and
// `api worker` running, the events flow into the sales read model like real
// traffic. It applies pending migrations first.
//
// Re-running it is safe: existing products (by SKU) are reused and only the
// missing ones are created; every run places OrdersPerTenant new orders.
func Seed(ctx context.Context, db *database.DB, log zerolog.Logger, opts SeedOptions) ([]SeedResult, error) {
	if err := Migrate(ctx, db, log); err != nil {
		return nil, err
	}
	for _, t := range opts.Tenants {
		if !tenant.Valid(t) {
			return nil, fmt.Errorf("invalid tenant id %q", t)
		}
	}

	tenants := tenant.NewProvisioner(db)
	products := catalog.NewPostgresRepository(db)
	catalogSvc := catalog.NewService(products, tenants)
	orderSvc := order.NewService(order.NewPostgresRepository(db), products, outbox.NewWriter(db), tenants, db)
	rng := rand.New(rand.NewPCG(opts.RandSeed, opts.RandSeed^0x9e3779b97f4a7c15)) //nolint:gosec // demo data, not security sensitive

	results := make([]SeedResult, 0, len(opts.Tenants))
	for i, tn := range opts.Tenants {
		res := SeedResult{Tenant: tn, Node: tenantNode(ctx, db, tn), Currency: seedCurrencies[i%len(seedCurrencies)]}

		catalogIDs, created, err := seedCatalog(ctx, catalogSvc, tn, res.Currency, opts.ProductsPerTenant, rng)
		if err != nil {
			return results, fmt.Errorf("seed catalog of %s: %w", tn, err)
		}
		res.ProductsCreated = created

		for range opts.OrdersPerTenant {
			o, err := orderSvc.Place(ctx, tn, randomOrder(catalogIDs, rng))
			if err != nil {
				return results, fmt.Errorf("seed order for %s: %w", tn, err)
			}
			res.OrdersPlaced++
			if rng.Float64() < opts.CancelRatio {
				if _, err := orderSvc.Cancel(ctx, tn, o.ID, o.Version); err != nil {
					return results, fmt.Errorf("seed cancel for %s: %w", tn, err)
				}
				res.OrdersCancelled++
			}
		}
		log.Info().Str("tenant", tn).Str("node", res.Node).Int("products_created", res.ProductsCreated).
			Int("orders", res.OrdersPlaced).Int("cancelled", res.OrdersCancelled).Msg("tenant seeded")
		results = append(results, res)
	}
	return results, nil
}

var productNames = []string{
	"Widget", "Gadget", "Sprocket", "Gizmo", "Doohickey", "Flange", "Bracket", "Coupling",
	"Valve", "Bearing", "Gasket", "Spring", "Hinge", "Latch", "Pulley", "Rivet",
}

// seedCatalog makes sure the tenant has n active products (SKU-001..n) and
// returns the ids of all of them.
func seedCatalog(ctx context.Context, svc *catalog.Service, tn, currency string, n int, rng *rand.Rand) ([]uuid.UUID, int, error) {
	existing := map[string]uuid.UUID{}
	var after *httpx.Cursor
	for {
		page, err := svc.List(ctx, tn, catalog.MaxPageSize, after)
		if err != nil {
			return nil, 0, err
		}
		for _, prod := range page.Products {
			existing[prod.SKU] = prod.ID
		}
		if page.NextCursor == nil {
			break
		}
		after = page.NextCursor
	}

	ids := make([]uuid.UUID, 0, n)
	created := 0
	for i := 1; i <= n; i++ {
		sku := fmt.Sprintf("SKU-%03d", i)
		if id, ok := existing[sku]; ok {
			ids = append(ids, id)
			continue
		}
		p, err := svc.Create(ctx, tn, catalog.CreateInput{
			SKU:        sku,
			Name:       fmt.Sprintf("%s %d", productNames[rng.IntN(len(productNames))], i),
			PriceMinor: int64(100 + rng.IntN(20_000)), // 1.00 – 200.99
			Currency:   currency,
			Active:     true,
		})
		if errors.Is(err, catalog.ErrSKUTaken) { // created concurrently by another seed run
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		ids = append(ids, p.ID)
		created++
	}
	return ids, created, nil
}

// randomOrder picks 1-4 distinct products with quantities 1-5.
func randomOrder(productIDs []uuid.UUID, rng *rand.Rand) order.PlaceInput {
	n := min(1+rng.IntN(4), len(productIDs))
	lines := make([]order.LineInput, 0, n)
	for _, i := range rng.Perm(len(productIDs))[:n] {
		lines = append(lines, order.LineInput{ProductID: productIDs[i], Quantity: 1 + rng.IntN(5)})
	}
	return order.PlaceInput{CustomerRef: fmt.Sprintf("cust-%04d", rng.IntN(500)), Lines: lines}
}

// tenantNode returns the node holding the tenant's shard ("" if unknown).
func tenantNode(ctx context.Context, db *database.DB, tn string) string {
	var node string
	_ = db.Pool.QueryRow(ctx, `
		SELECT nodename FROM citus_shards
		WHERE table_name = 'tenants'::regclass
		  AND shardid = get_shard_id_for_distribution_column('tenants', $1::text)`, tn).Scan(&node)
	return node
}
