package database

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// Migrator applies the embedded SQL migrations using goose. A PostgreSQL
// advisory lock guarantees that only one instance migrates at a time, so it
// is safe to run from several replicas simultaneously.
type Migrator struct {
	provider *goose.Provider
	sqlDB    *sql.DB
}

// NewMigrator creates a Migrator for the migrations found in fsys (usually
// migrations.FS).
func NewMigrator(db *DB, fsys fs.FS) (*Migrator, error) {
	sqlDB := stdlib.OpenDBFromPool(db.Pool)
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, fmt.Errorf("create migration locker: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, sqlDB, fsys,
		goose.WithSessionLocker(locker),
	)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("create migration provider: %w", err)
	}
	return &Migrator{provider: p, sqlDB: sqlDB}, nil
}

// Close releases the database/sql handle (the underlying pool stays open).
func (m *Migrator) Close() error { return m.sqlDB.Close() }

// Up applies all pending migrations.
func (m *Migrator) Up(ctx context.Context) ([]*goose.MigrationResult, error) {
	return m.provider.Up(ctx)
}

// UpTo applies migrations up to and including version.
func (m *Migrator) UpTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error) {
	return m.provider.UpTo(ctx, version)
}

// Down rolls back the most recent migration.
func (m *Migrator) Down(ctx context.Context) (*goose.MigrationResult, error) {
	return m.provider.Down(ctx)
}

// DownTo rolls back migrations down to (but excluding) version. Use 0 to
// roll back everything.
func (m *Migrator) DownTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error) {
	return m.provider.DownTo(ctx, version)
}

// Status lists all migrations and whether they are applied.
func (m *Migrator) Status(ctx context.Context) ([]*goose.MigrationStatus, error) {
	return m.provider.Status(ctx)
}

// Version returns the current schema version.
func (m *Migrator) Version(ctx context.Context) (int64, error) {
	return m.provider.GetDBVersion(ctx)
}
