// Package database owns the PostgreSQL connection pool, transaction handling
// and schema migrations.
//
// Repositories never hold a *pgxpool.Pool directly. Instead they call
// DB.Q(ctx), which transparently returns the transaction bound to ctx (if a
// caller opened one with WithTx) or the pool otherwise. This keeps
// repositories transaction-agnostic and lets services compose several
// repository calls atomically.
package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Oxyrus/orderbook/internal/config"
)

// DBTX is the subset of methods shared by *pgxpool.Pool, *pgxpool.Conn and
// pgx.Tx. Repositories depend on this interface only.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
}

// DB wraps the connection pool.
type DB struct {
	Pool *pgxpool.Pool
}

// Connect creates a pool from cfg and verifies connectivity.
func Connect(ctx context.Context, cfg config.DB, tracing bool) (*DB, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse DB_URL: %w", err)
	}
	pcfg.MaxConns = cfg.MaxConns
	pcfg.MinConns = cfg.MinConns
	pcfg.MaxConnLifetime = cfg.MaxConnLifetime
	pcfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	pcfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	if cfg.ConnectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	if tracing {
		pcfg.ConnConfig.Tracer = otelpgx.NewTracer()
	}

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	pingTimeout := cfg.ConnectTimeout
	if pingTimeout <= 0 {
		pingTimeout = 5 * time.Second
	}
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &DB{Pool: pool}, nil
}

// Close releases all pool resources.
func (db *DB) Close() { db.Pool.Close() }

// Ping checks connectivity (used by the readiness probe).
func (db *DB) Ping(ctx context.Context) error { return db.Pool.Ping(ctx) }

// Q returns the transaction bound to ctx, or the pool if there is none.
func (db *DB) Q(ctx context.Context) DBTX {
	if tx, ok := txFromContext(ctx); ok {
		return tx
	}
	return db.Pool
}

// Common PostgreSQL error codes.
// See https://www.postgresql.org/docs/current/errcodes-appendix.html
const (
	CodeUniqueViolation      = "23505"
	CodeForeignKeyViolation  = "23503"
	CodeCheckViolation       = "23514"
	CodeNotNullViolation     = "23502"
	CodeSerializationFailure = "40001"
	CodeDeadlockDetected     = "40P01"
)

// ErrNoRows is re-exported so callers do not need to import pgx.
var ErrNoRows = pgx.ErrNoRows

// IsCode reports whether err is a PostgreSQL error with the given SQLSTATE.
func IsCode(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}

// IsUniqueViolation reports whether err is a unique-constraint violation,
// optionally restricted to a named constraint.
func IsUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != CodeUniqueViolation {
		return false
	}
	return constraintMatches(pgErr.ConstraintName, constraint)
}

// IsForeignKeyViolation reports whether err is a foreign-key violation,
// optionally restricted to a named constraint.
func IsForeignKeyViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != CodeForeignKeyViolation {
		return false
	}
	return constraintMatches(pgErr.ConstraintName, constraint)
}

// constraintMatches compares a reported constraint name with the logical
// name used in migrations. On Citus, errors raised by a shard may name the
// shard's constraint, which carries the shard id as a suffix
// (products_tenant_sku_key_102043).
func constraintMatches(reported, want string) bool {
	if want == "" || reported == want {
		return true
	}
	suffix, ok := strings.CutPrefix(reported, want+"_")
	if !ok || suffix == "" {
		return false
	}
	for _, c := range suffix {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
