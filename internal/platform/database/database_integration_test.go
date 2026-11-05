//go:build integration

package database_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/internal/testutil/pgtest"
	"github.com/Oxyrus/orderbook/migrations"
)

var pg *pgtest.Server

func TestMain(m *testing.M) { pgtest.Main(m, &pg) }

func setup(t *testing.T) (*database.DB, context.Context) {
	t.Helper()
	t.Parallel()
	db := pg.NewDB(t)
	ctx := context.Background()
	_, err := db.Pool.Exec(ctx, `CREATE TABLE kv (k text PRIMARY KEY, v int NOT NULL)`)
	require.NoError(t, err)
	return db, ctx
}

func count(t *testing.T, db *database.DB) int {
	t.Helper()
	var n int
	require.NoError(t, db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM kv`).Scan(&n))
	return n
}

func insert(ctx context.Context, db *database.DB, k string) error {
	_, err := db.Q(ctx).Exec(ctx, `INSERT INTO kv (k, v) VALUES ($1, 1)`, k)
	return err
}

func TestWithTxCommit(t *testing.T) {
	db, ctx := setup(t)
	err := db.WithTx(ctx, func(ctx context.Context) error {
		require.True(t, database.InTx(ctx))
		require.NoError(t, insert(ctx, db, "a"))
		return insert(ctx, db, "b")
	})
	require.NoError(t, err)
	assert.Equal(t, 2, count(t, db))
}

func TestWithTxRollbackOnError(t *testing.T) {
	db, ctx := setup(t)
	boom := errors.New("boom")
	err := db.WithTx(ctx, func(ctx context.Context) error {
		require.NoError(t, insert(ctx, db, "a"))
		return boom
	})
	require.ErrorIs(t, err, boom)
	assert.Equal(t, 0, count(t, db))
}

func TestWithTxRollbackOnPanic(t *testing.T) {
	db, ctx := setup(t)
	assert.PanicsWithValue(t, "kaboom", func() {
		_ = db.WithTx(ctx, func(ctx context.Context) error {
			require.NoError(t, insert(ctx, db, "a"))
			panic("kaboom")
		})
	})
	assert.Equal(t, 0, count(t, db))
}

func TestWithTxNestedJoinsOuter(t *testing.T) {
	db, ctx := setup(t)
	err := db.WithTx(ctx, func(ctx context.Context) error {
		require.NoError(t, insert(ctx, db, "a"))
		// The inner call joins the outer transaction...
		require.NoError(t, db.WithTx(ctx, func(ctx context.Context) error { return insert(ctx, db, "b") }))
		// ...so failing the outer one rolls back both.
		return errors.New("abort")
	})
	require.Error(t, err)
	assert.Equal(t, 0, count(t, db))
}

func TestSavepoint(t *testing.T) {
	db, ctx := setup(t)
	err := db.WithTx(ctx, func(ctx context.Context) error {
		require.NoError(t, insert(ctx, db, "kept"))
		spErr := database.Savepoint(ctx, func(ctx context.Context) error {
			require.NoError(t, insert(ctx, db, "discarded"))
			return errors.New("partial failure")
		})
		require.Error(t, spErr)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, count(t, db))

	require.Error(t, database.Savepoint(ctx, func(context.Context) error { return nil }), "savepoint requires a tx")
}

func TestUniqueViolationHelper(t *testing.T) {
	db, ctx := setup(t)
	require.NoError(t, insert(ctx, db, "a"))
	err := insert(ctx, db, "a")
	assert.True(t, database.IsUniqueViolation(err, ""))
	assert.True(t, database.IsUniqueViolation(err, "kv_pkey"))
	assert.False(t, database.IsUniqueViolation(err, "other"))
}

// TestSerializableRetry provokes real serialization failures with two
// concurrent SERIALIZABLE read-modify-write transactions and checks that
// WithTxOptions retries them transparently.
func TestSerializableRetry(t *testing.T) {
	db, ctx := setup(t)
	_, err := db.Pool.Exec(ctx, `INSERT INTO kv (k, v) VALUES ('counter', 0)`)
	require.NoError(t, err)

	const workers = 8
	var attempts atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Go(func() {
			errs <- db.WithTxOptions(ctx, database.TxOptions{IsoLevel: pgx.Serializable, MaxRetries: 20}, func(ctx context.Context) error {
				attempts.Add(1)
				var v int
				if err := db.Q(ctx).QueryRow(ctx, `SELECT v FROM kv WHERE k = 'counter'`).Scan(&v); err != nil {
					return err
				}
				_, err := db.Q(ctx).Exec(ctx, `UPDATE kv SET v = $1 WHERE k = 'counter'`, v+1)
				return err
			})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	var v int
	require.NoError(t, db.Pool.QueryRow(ctx, `SELECT v FROM kv WHERE k = 'counter'`).Scan(&v))
	assert.Equal(t, workers, v, "no lost updates")
	t.Logf("%d transactions needed %d attempts", workers, attempts.Load())
}

func TestMigrationsRoundTrip(t *testing.T) {
	t.Parallel()
	db := pg.NewDB(t) // already migrated from the template
	ctx := context.Background()

	m, err := database.NewMigrator(db, migrations.FS)
	require.NoError(t, err)
	defer m.Close()

	latest, err := m.Version(ctx)
	require.NoError(t, err)
	require.Positive(t, latest)

	// Every Down migration must work, and Up must be re-appliable.
	_, err = m.DownTo(ctx, 0)
	require.NoError(t, err)
	v, err := m.Version(ctx)
	require.NoError(t, err)
	assert.Zero(t, v)

	_, err = m.Up(ctx)
	require.NoError(t, err)
	v, err = m.Version(ctx)
	require.NoError(t, err)
	assert.Equal(t, latest, v)
}
