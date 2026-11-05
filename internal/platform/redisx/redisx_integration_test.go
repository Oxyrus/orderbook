//go:build integration

package redisx_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/platform/redisx"
	"github.com/Oxyrus/orderbook/internal/testutil/redistest"
)

var rs *redistest.Server

func TestMain(m *testing.M) { redistest.Main(m, &rs) }

type user struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func TestConnectFailsFast(t *testing.T) {
	t.Parallel()
	cfg := rs.Config()
	cfg.URL = "redis://127.0.0.1:1/0" // nothing listens here
	cfg.DialTimeout = 500 * time.Millisecond
	_, err := redisx.Connect(context.Background(), cfg, false)
	require.ErrorContains(t, err, "ping redis")
}

func TestRawClient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rdb := rs.NewClient(t)
	ns := redistest.Namespace(t, rdb)

	require.NoError(t, rdb.Set(ctx, ns+":k", "v", time.Minute).Err())
	got, err := rdb.Get(ctx, ns+":k").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", got)

	_, err = rdb.Get(ctx, ns+":missing").Result()
	assert.True(t, redisx.IsNil(err))
}

func TestCacheGetSetDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rdb := rs.NewClient(t)
	c := redisx.NewCache[user](rdb, redistest.Namespace(t, rdb), time.Minute)

	_, ok, err := c.Get(ctx, "1")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, c.Set(ctx, "1", user{ID: "1", Name: "Ada"}))
	got, ok, err := c.Get(ctx, "1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, user{ID: "1", Name: "Ada"}, got)

	ttl, err := rdb.TTL(ctx, c.Key("1")).Result()
	require.NoError(t, err)
	assert.InDelta(t, time.Minute.Seconds(), ttl.Seconds(), 2)

	require.NoError(t, c.Delete(ctx, "1"))
	_, ok, err = c.Get(ctx, "1")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCacheGetOrLoad(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rdb := rs.NewClient(t)
	c := redisx.NewCache[user](rdb, redistest.Namespace(t, rdb), time.Minute)

	var calls atomic.Int32
	load := func(context.Context) (user, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond) // widen the stampede window
		return user{ID: "1", Name: "Ada"}, nil
	}

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			got, err := c.GetOrLoad(ctx, "1", load)
			assert.NoError(t, err)
			assert.Equal(t, "Ada", got.Name)
		})
	}
	wg.Wait()
	assert.Equal(t, int32(1), calls.Load(), "concurrent misses share one load")

	// Now cached: the loader is not called again.
	_, err := c.GetOrLoad(ctx, "1", load)
	require.NoError(t, err)
	assert.Equal(t, int32(1), calls.Load())
}

func TestCacheGetOrLoadErrorsAreNotCached(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rdb := rs.NewClient(t)
	c := redisx.NewCache[user](rdb, redistest.Namespace(t, rdb), time.Minute)

	boom := errors.New("boom")
	_, err := c.GetOrLoad(ctx, "1", func(context.Context) (user, error) { return user{}, boom })
	require.ErrorIs(t, err, boom)

	_, ok, err := c.Get(ctx, "1")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCacheFallsBackWhenRedisIsDown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rdb := rs.NewClient(t)
	c := redisx.NewCache[user](rdb, redistest.Namespace(t, rdb), time.Minute)
	require.NoError(t, rdb.Close())

	got, err := c.GetOrLoad(ctx, "1", func(context.Context) (user, error) {
		return user{ID: "1", Name: "Ada"}, nil
	})
	require.NoError(t, err, "a Redis outage must not fail the request")
	assert.Equal(t, "Ada", got.Name)
}

func TestPoolCollector(t *testing.T) {
	t.Parallel()
	rdb := rs.NewClient(t)
	reg := prometheus.NewRegistry()
	reg.MustRegister(redisx.NewPoolCollector(rdb))

	n, err := testutil.GatherAndCount(reg, "redis_pool_total_conns", "redis_pool_max_conns")
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}
