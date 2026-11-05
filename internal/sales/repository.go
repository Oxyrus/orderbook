package sales

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Oxyrus/orderbook/internal/platform/database"
)

// PostgresStore implements Store.
type PostgresStore struct {
	db *database.DB
}

// NewPostgresStore returns a store backed by db.
func NewPostgresStore(db *database.DB) *PostgresStore { return &PostgresStore{db: db} }

var _ Store = (*PostgresStore)(nil)

func (s *PostgresStore) MarkProcessed(ctx context.Context, tenantID string, eventID uuid.UUID) (bool, error) {
	tag, err := s.db.Q(ctx).Exec(ctx, `
		INSERT INTO processed_events (tenant_id, consumer, event_id) VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`, tenantID, ConsumerName, eventID)
	if err != nil {
		return false, fmt.Errorf("mark event processed: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Apply upserts the delta. Increments commute, so events may be applied in
// any order, which matters because Citus CDC only orders changes per node.
func (s *PostgresStore) Apply(ctx context.Context, d Delta) error {
	_, err := s.db.Q(ctx).Exec(ctx, `
		INSERT INTO tenant_daily_sales AS t
		       (tenant_id, day, currency, orders_placed, orders_cancelled, gross_minor, cancelled_minor)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (tenant_id, day, currency) DO UPDATE SET
		    orders_placed    = t.orders_placed    + EXCLUDED.orders_placed,
		    orders_cancelled = t.orders_cancelled + EXCLUDED.orders_cancelled,
		    gross_minor      = t.gross_minor      + EXCLUDED.gross_minor,
		    cancelled_minor  = t.cancelled_minor  + EXCLUDED.cancelled_minor`,
		d.TenantID, d.Day, d.Currency, d.OrdersPlaced, d.OrdersCancelled, d.GrossMinor, d.CancelledMinor)
	if err != nil {
		return fmt.Errorf("apply sales delta: %w", err)
	}
	return nil
}

// Daily is a router query: one tenant, one shard.
func (s *PostgresStore) Daily(ctx context.Context, tenantID string, from, to time.Time) ([]DailySales, error) {
	rows, err := s.db.Q(ctx).Query(ctx, `
		SELECT tenant_id, day, currency, orders_placed, orders_cancelled, gross_minor, cancelled_minor
		FROM tenant_daily_sales
		WHERE tenant_id = $1 AND day BETWEEN $2 AND $3
		ORDER BY day, currency`, tenantID, from, to)
	if err != nil {
		return nil, fmt.Errorf("daily sales: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (DailySales, error) {
		var d DailySales
		err := row.Scan(&d.TenantID, &d.Day, &d.Currency, &d.OrdersPlaced, &d.OrdersCancelled, &d.GrossMinor, &d.CancelledMinor)
		return d, err
	})
}

// TopTenants spans every tenant, so Citus runs it as a multi-shard query:
// each shard computes partial aggregates in parallel on its worker and the
// coordinator merges them, sorts and applies the limit.
func (s *PostgresStore) TopTenants(ctx context.Context, from, to time.Time, currency string, limit int) ([]TenantTotal, error) {
	rows, err := s.db.Q(ctx).Query(ctx, `
		SELECT tenant_id, currency, sum(orders_placed)::bigint, sum(gross_minor - cancelled_minor)::bigint AS net
		FROM tenant_daily_sales
		WHERE day BETWEEN $1 AND $2 AND currency = $3
		GROUP BY tenant_id, currency
		ORDER BY net DESC, tenant_id
		LIMIT $4`, from, to, currency, limit)
	if err != nil {
		return nil, fmt.Errorf("top tenants: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (TenantTotal, error) {
		var t TenantTotal
		err := row.Scan(&t.TenantID, &t.Currency, &t.OrdersPlaced, &t.NetMinor)
		return t, err
	})
}

// PruneProcessed forgets processed event ids older than retention. It is a
// multi-shard DELETE, run periodically by the worker.
func PruneProcessed(ctx context.Context, db *database.DB, retention time.Duration) (int64, error) {
	tag, err := db.Pool.Exec(ctx,
		`DELETE FROM processed_events WHERE processed_at < now() - make_interval(secs => $1)`, retention.Seconds())
	if err != nil {
		return 0, fmt.Errorf("prune processed events: %w", err)
	}
	return tag.RowsAffected(), nil
}
