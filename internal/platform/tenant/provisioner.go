package tenant

import (
	"context"
	"fmt"
	"sync"

	"github.com/Oxyrus/orderbook/internal/platform/database"
)

// Provisioner creates tenant rows just in time: the identity provider is the
// source of truth for which tenants exist, and a tenant's row is created the
// first time one of its users writes data. The tenants row is the root of
// the co-location group that every tenant table references.
//
// Swap this for explicit onboarding (an admin API, a signup flow or an
// event from your billing system) when tenants need more than an id.
type Provisioner struct {
	db *database.DB

	mu    sync.RWMutex
	known map[string]struct{}
}

// maxCached bounds memory. Beyond it Ensure keeps working, it just goes to
// the database (a cheap single-shard upsert) for uncached tenants.
const maxCached = 100_000

// NewProvisioner returns a Provisioner backed by db.
func NewProvisioner(db *database.DB) *Provisioner {
	return &Provisioner{db: db, known: map[string]struct{}{}}
}

// Ensure makes sure the tenant row exists. It runs outside any transaction
// in ctx (directly on the pool) and commits immediately, so a caller's
// rollback can't leave the cache claiming a row that does not exist.
func (p *Provisioner) Ensure(ctx context.Context, id string) error {
	p.mu.RLock()
	_, ok := p.known[id]
	p.mu.RUnlock()
	if ok {
		return nil
	}
	// Single-shard statement: Citus routes it by the tenants.id value.
	const q = `INSERT INTO tenants (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING`
	if _, err := p.db.Pool.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("provision tenant: %w", err)
	}
	p.mu.Lock()
	if len(p.known) < maxCached {
		p.known[id] = struct{}{}
	}
	p.mu.Unlock()
	return nil
}
