package order

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/internal/platform/httpx"
)

// PostgresRepository implements Repository with pgx.
//
// Every statement filters on (or inserts) tenant_id, which makes it a Citus
// router query: the coordinator sends it, unchanged apart from the shard
// name, to the one node holding the tenant's shard.
type PostgresRepository struct {
	db *database.DB
}

// NewPostgresRepository returns a repository backed by db.
func NewPostgresRepository(db *database.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

var _ Repository = (*PostgresRepository)(nil)

const orderColumns = `tenant_id, id, customer_ref, status, currency, total_minor, version, placed_at, cancelled_at, updated_at`

func scanOrder(row pgx.Row) (*Order, error) {
	var o Order
	err := row.Scan(&o.TenantID, &o.ID, &o.CustomerRef, &o.Status, &o.Currency, &o.TotalMinor,
		&o.Version, &o.PlacedAt, &o.CancelledAt, &o.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (r *PostgresRepository) Create(ctx context.Context, o *Order) error {
	q := r.db.Q(ctx)
	const insertOrder = `
		INSERT INTO orders (tenant_id, id, customer_ref, status, currency, total_minor)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING ` + orderColumns
	created, err := scanOrder(q.QueryRow(ctx, insertOrder, o.TenantID, o.ID, o.CustomerRef, o.Status, o.Currency, o.TotalMinor))
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	// One multi-row INSERT for all lines: a single round trip and a single
	// router statement, whatever the number of lines.
	n := len(o.Lines)
	lineNos, products, skus, names := make([]int32, n), make([]uuid.UUID, n), make([]string, n), make([]string, n)
	qtys, prices := make([]int32, n), make([]int64, n)
	for i, l := range o.Lines {
		lineNos[i], products[i], skus[i], names[i] = int32(l.LineNo), l.ProductID, l.SKU, l.Name //nolint:gosec // bounded by MaxLines
		qtys[i], prices[i] = int32(l.Quantity), l.UnitPriceMinor                                 //nolint:gosec // bounded by MaxQuantity
	}
	const insertLines = `
		INSERT INTO order_lines (tenant_id, order_id, line_no, product_id, sku, name, quantity, unit_price_minor)
		SELECT $1, $2, l.line_no, l.product_id, l.sku, l.name, l.quantity, l.unit_price_minor
		FROM unnest($3::int[], $4::uuid[], $5::text[], $6::text[], $7::int[], $8::bigint[])
		     AS l(line_no, product_id, sku, name, quantity, unit_price_minor)`
	if _, err := q.Exec(ctx, insertLines, o.TenantID, o.ID, lineNos, products, skus, names, qtys, prices); err != nil {
		return fmt.Errorf("insert order lines: %w", err)
	}
	created.Lines = o.Lines
	*o = *created
	return nil
}

func (r *PostgresRepository) Get(ctx context.Context, tenantID string, id uuid.UUID) (*Order, error) {
	q := r.db.Q(ctx)
	o, err := scanOrder(q.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE tenant_id = $1 AND id = $2`, tenantID, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get order: %w", err)
	}
	rows, err := q.Query(ctx, `
		SELECT line_no, product_id, sku, name, quantity, unit_price_minor
		FROM order_lines WHERE tenant_id = $1 AND order_id = $2 ORDER BY line_no`, tenantID, id)
	if err != nil {
		return nil, fmt.Errorf("get order lines: %w", err)
	}
	o.Lines, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (Line, error) {
		var l Line
		err := row.Scan(&l.LineNo, &l.ProductID, &l.SKU, &l.Name, &l.Quantity, &l.UnitPriceMinor)
		return l, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan order lines: %w", err)
	}
	return o, nil
}

func (r *PostgresRepository) List(ctx context.Context, p ListParams) (Page, error) {
	args := []any{p.TenantID, p.Limit + 1}
	q := `SELECT ` + orderColumns + ` FROM orders WHERE tenant_id = $1`
	if p.Status != "" {
		args = append(args, p.Status)
		q += fmt.Sprintf(` AND status = $%d`, len(args))
	}
	if p.After != nil {
		args = append(args, p.After.At, p.After.ID)
		q += fmt.Sprintf(` AND (placed_at, id) < ($%d, $%d)`, len(args)-1, len(args))
	}
	q += ` ORDER BY placed_at DESC, id DESC LIMIT $2`

	rows, err := r.db.Q(ctx).Query(ctx, q, args...)
	if err != nil {
		return Page{}, fmt.Errorf("list orders: %w", err)
	}
	orders, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Order, error) {
		o, err := scanOrder(row)
		if err != nil {
			return Order{}, err
		}
		return *o, nil
	})
	if err != nil {
		return Page{}, fmt.Errorf("scan orders: %w", err)
	}
	page := Page{Orders: orders}
	if len(orders) > p.Limit {
		page.Orders = orders[:p.Limit]
		last := page.Orders[len(page.Orders)-1]
		page.NextCursor = &httpx.Cursor{At: last.PlacedAt, ID: last.ID}
	}
	return page, nil
}

func (r *PostgresRepository) Cancel(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int) (*Order, error) {
	const q = `
		UPDATE orders
		SET status = 'cancelled', cancelled_at = now(), version = version + 1, updated_at = now()
		WHERE tenant_id = $1 AND id = $2 AND status = 'placed' AND ($3 = 0 OR version = $3)
		RETURNING ` + orderColumns
	o, err := scanOrder(r.db.Q(ctx).QueryRow(ctx, q, tenantID, id, expectedVersion))
	if errors.Is(err, pgx.ErrNoRows) {
		// Find out why nothing matched.
		current, getErr := r.Get(ctx, tenantID, id)
		switch {
		case getErr != nil:
			return nil, getErr
		case current.Status == StatusCancelled:
			return nil, ErrAlreadyCancelled
		default:
			return nil, ErrVersionConflict
		}
	}
	if err != nil {
		return nil, fmt.Errorf("cancel order: %w", err)
	}
	return o, nil
}
