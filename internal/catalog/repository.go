package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/internal/platform/httpx"
)

// PostgresRepository implements Repository with pgx. It obtains its
// executor through db.Q(ctx), so it joins the caller's transaction.
type PostgresRepository struct {
	db *database.DB
}

// NewPostgresRepository returns a repository backed by db.
func NewPostgresRepository(db *database.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

var _ Repository = (*PostgresRepository)(nil)

const productColumns = `tenant_id, id, sku, name, price_minor, currency, active, version, created_at, updated_at`

func scanProduct(row pgx.Row) (*Product, error) {
	var p Product
	err := row.Scan(&p.TenantID, &p.ID, &p.SKU, &p.Name, &p.PriceMinor, &p.Currency, &p.Active, &p.Version, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func collectProducts(rows pgx.Rows) ([]Product, error) {
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Product, error) {
		p, err := scanProduct(row)
		if err != nil {
			return Product{}, err
		}
		return *p, nil
	})
}

// domainError translates constraint violations into domain errors.
func domainError(err error) (error, bool) {
	switch {
	case database.IsUniqueViolation(err, "products_tenant_sku_key"):
		return ErrSKUTaken, true
	case database.IsForeignKeyViolation(err, "products_currency_fkey"):
		return ErrUnknownCurrency, true
	}
	return nil, false
}

func (r *PostgresRepository) Create(ctx context.Context, p *Product) error {
	const q = `
		INSERT INTO products (tenant_id, id, sku, name, price_minor, currency, active)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING ` + productColumns
	created, err := scanProduct(r.db.Q(ctx).QueryRow(ctx, q, p.TenantID, p.ID, p.SKU, p.Name, p.PriceMinor, p.Currency, p.Active))
	if err != nil {
		if derr, ok := domainError(err); ok {
			return derr
		}
		return fmt.Errorf("insert product: %w", err)
	}
	*p = *created
	return nil
}

func (r *PostgresRepository) Get(ctx context.Context, tenantID string, id uuid.UUID) (*Product, error) {
	const q = `SELECT ` + productColumns + ` FROM products WHERE tenant_id = $1 AND id = $2`
	p, err := scanProduct(r.db.Q(ctx).QueryRow(ctx, q, tenantID, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get product: %w", err)
	}
	return p, nil
}

func (r *PostgresRepository) List(ctx context.Context, p ListParams) (Page, error) {
	args := []any{p.TenantID, p.Limit + 1}
	q := `SELECT ` + productColumns + ` FROM products WHERE tenant_id = $1`
	if p.After != nil {
		q += ` AND (created_at, id) < ($3, $4)`
		args = append(args, p.After.At, p.After.ID)
	}
	q += ` ORDER BY created_at DESC, id DESC LIMIT $2`

	rows, err := r.db.Q(ctx).Query(ctx, q, args...)
	if err != nil {
		return Page{}, fmt.Errorf("list products: %w", err)
	}
	products, err := collectProducts(rows)
	if err != nil {
		return Page{}, fmt.Errorf("scan products: %w", err)
	}
	page := Page{Products: products}
	if len(products) > p.Limit {
		page.Products = products[:p.Limit]
		last := page.Products[len(page.Products)-1]
		page.NextCursor = &httpx.Cursor{At: last.CreatedAt, ID: last.ID}
	}
	return page, nil
}

func (r *PostgresRepository) Update(ctx context.Context, p *Product) error {
	const q = `
		UPDATE products
		SET sku = $3, name = $4, price_minor = $5, currency = $6, active = $7,
		    version = version + 1, updated_at = now()
		WHERE tenant_id = $1 AND id = $2 AND version = $8
		RETURNING ` + productColumns
	updated, err := scanProduct(r.db.Q(ctx).QueryRow(ctx, q, p.TenantID, p.ID, p.SKU, p.Name, p.PriceMinor, p.Currency, p.Active, p.Version))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if _, getErr := r.Get(ctx, p.TenantID, p.ID); errors.Is(getErr, ErrNotFound) {
			return ErrNotFound
		}
		return ErrVersionConflict
	case err != nil:
		if derr, ok := domainError(err); ok {
			return derr
		}
		return fmt.Errorf("update product: %w", err)
	}
	*p = *updated
	return nil
}

func (r *PostgresRepository) LockForOrder(ctx context.Context, tenantID string, ids []uuid.UUID) ([]Product, error) {
	// tenant_id = $1 makes this a router query; Citus supports row locks
	// on single-shard queries.
	const q = `SELECT ` + productColumns + ` FROM products
		WHERE tenant_id = $1 AND id = ANY($2)
		ORDER BY id
		FOR SHARE`
	rows, err := r.db.Q(ctx).Query(ctx, q, tenantID, ids)
	if err != nil {
		return nil, fmt.Errorf("lock products: %w", err)
	}
	products, err := collectProducts(rows)
	if err != nil {
		return nil, fmt.Errorf("scan products: %w", err)
	}
	return products, nil
}
