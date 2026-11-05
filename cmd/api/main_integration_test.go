//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/testutil/pgtest"
)

var pg *pgtest.Server

func TestMain(m *testing.M) { pgtest.Main(m, &pg) }

func TestMigrateCommands(t *testing.T) {
	url := pg.NewDatabase(t) // already fully migrated
	t.Setenv("DB_URL", url)
	t.Setenv("LOG_LEVEL", "disabled")

	conn, err := pgx.Connect(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	tableExists := func(name string) bool {
		var ok bool
		require.NoError(t, conn.QueryRow(context.Background(), `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&ok))
		return ok
	}

	assert.Equal(t, 0, run([]string{"migrate", "up"}), "up is a no-op when current")
	assert.Equal(t, 0, run([]string{"migrate", "status"}))
	assert.Equal(t, 0, run([]string{"migrate", "version"}))

	pubExists := func(name string) bool {
		var ok bool
		require.NoError(t, conn.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, name).Scan(&ok))
		return ok
	}

	require.Equal(t, 0, run([]string{"migrate", "down"}))
	assert.True(t, tableExists("outbox"))
	assert.False(t, pubExists("orderbook_tables"), "down rolls back only the latest migration")

	require.Equal(t, 0, run([]string{"migrate", "down-to", "0"}))
	assert.False(t, tableExists("orders"))
	assert.False(t, tableExists("tenants"))

	require.Equal(t, 0, run([]string{"migrate", "up-to", "3"}))
	assert.True(t, tableExists("orders"))
	assert.False(t, tableExists("outbox"))

	require.Equal(t, 0, run([]string{"migrate", "up"}))
	assert.True(t, tableExists("outbox"))
	assert.True(t, pubExists("orderbook_outbox"))
	assert.True(t, pubExists("orderbook_tables"))

	assert.Equal(t, 1, run([]string{"migrate", "up-to"}), "missing VERSION")
	assert.Equal(t, 1, run([]string{"migrate", "up-to", "x"}), "invalid VERSION")
	assert.Equal(t, 1, run([]string{"migrate", "sideways"}))
}
