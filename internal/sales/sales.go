// Package sales maintains and serves a per-tenant daily sales read model
// built from order events (CQRS).
//
//	order service ──tx──▶ outbox ──WAL──▶ Debezium ──▶ Kafka ──▶ Projector ──▶ tenant_daily_sales
//
// The projection is eventually consistent: it trails the orders tables by
// the CDC pipeline's latency (typically well under a second). It is also
// the example of a cross-tenant query: the platform report aggregates every
// tenant's shard in parallel.
package sales

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ConsumerName identifies the projection in processed_events.
const ConsumerName = "sales-projection"

// DailySales is one row of the read model.
type DailySales struct {
	TenantID        string
	Day             time.Time
	Currency        string
	OrdersPlaced    int64
	OrdersCancelled int64
	GrossMinor      int64
	CancelledMinor  int64
}

// NetMinor is revenue after cancellations.
func (d DailySales) NetMinor() int64 { return d.GrossMinor - d.CancelledMinor }

// Delta is an increment applied to one (tenant, day, currency) row.
type Delta struct {
	TenantID        string
	Day             time.Time
	Currency        string
	OrdersPlaced    int64
	OrdersCancelled int64
	GrossMinor      int64
	CancelledMinor  int64
}

// TenantTotal is one row of the cross-tenant report.
type TenantTotal struct {
	TenantID     string
	Currency     string
	OrdersPlaced int64
	NetMinor     int64
}

// Store persists the read model.
type Store interface {
	// MarkProcessed records that eventID has been applied and reports
	// whether it was new. It must run in the same transaction as Apply.
	MarkProcessed(ctx context.Context, tenantID string, eventID uuid.UUID) (bool, error)
	Apply(ctx context.Context, d Delta) error
	Daily(ctx context.Context, tenantID string, from, to time.Time) ([]DailySales, error)
	TopTenants(ctx context.Context, from, to time.Time, currency string, limit int) ([]TenantTotal, error)
}
