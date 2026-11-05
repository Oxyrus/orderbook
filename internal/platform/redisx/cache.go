package redisx

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"github.com/Oxyrus/orderbook/internal/platform/logger"
)

// Cache is a typed, JSON-encoded cache-aside helper over Redis.
//
// Keys are namespaced as "<namespace>:<key>" so features sharing one Redis
// never collide. Values are stored with a fixed TTL; every entry expires.
//
// The cache is best-effort: GetOrLoad treats Redis errors as misses and
// falls back to the loader, so a Redis outage degrades latency, not
// availability. Use Get/Set directly when you need to see Redis errors.
type Cache[T any] struct {
	rdb       redis.Cmdable
	namespace string
	ttl       time.Duration
	group     singleflight.Group
}

// NewCache returns a cache storing values under namespace for ttl. ttl must
// be positive. Pick a namespace per feature and value shape, and version it
// when the shape changes (e.g. "items:v1") so old entries are ignored.
func NewCache[T any](rdb redis.Cmdable, namespace string, ttl time.Duration) *Cache[T] {
	if ttl <= 0 {
		panic("redisx: cache ttl must be positive")
	}
	return &Cache[T]{rdb: rdb, namespace: namespace, ttl: ttl}
}

// Key returns the full Redis key for key.
func (c *Cache[T]) Key(key string) string { return c.namespace + ":" + key }

// Get returns the cached value and whether it was found.
func (c *Cache[T]) Get(ctx context.Context, key string) (T, bool, error) {
	var v T
	b, err := c.rdb.Get(ctx, c.Key(key)).Bytes()
	if IsNil(err) {
		return v, false, nil
	}
	if err != nil {
		return v, false, fmt.Errorf("redis get %s: %w", c.Key(key), err)
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return v, false, fmt.Errorf("redis decode %s: %w", c.Key(key), err)
	}
	return v, true, nil
}

// Set stores v under key with the cache TTL.
func (c *Cache[T]) Set(ctx context.Context, key string, v T) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("redis encode %s: %w", c.Key(key), err)
	}
	if err := c.rdb.Set(ctx, c.Key(key), b, c.ttl).Err(); err != nil {
		return fmt.Errorf("redis set %s: %w", c.Key(key), err)
	}
	return nil
}

// Delete removes keys (invalidation). Call it after the source of truth
// changes, ideally after the database transaction commits.
func (c *Cache[T]) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = c.Key(k)
	}
	if err := c.rdb.Del(ctx, full...).Err(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}
	return nil
}

// GetOrLoad returns the cached value for key, or calls load, caches its
// result and returns it. Concurrent misses for the same key within this
// process share a single load call (stampede protection).
//
// Errors from load are returned and never cached. Redis errors are logged
// and otherwise ignored.
func (c *Cache[T]) GetOrLoad(ctx context.Context, key string, load func(context.Context) (T, error)) (T, error) {
	v, ok, err := c.Get(ctx, key)
	if err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("cache read failed, loading from source")
	}
	if ok {
		return v, nil
	}

	ch := c.group.DoChan(key, func() (any, error) {
		// Detach from the first caller's cancellation so one aborted request
		// doesn't fail the others waiting on the same key, but keep its
		// deadline so a slow load can't run forever.
		lctx := context.WithoutCancel(ctx)
		if dl, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc
			lctx, cancel = context.WithDeadline(lctx, dl)
			defer cancel()
		}
		v, err := load(lctx)
		if err != nil {
			return v, err
		}
		if err := c.Set(lctx, key, v); err != nil {
			logger.FromContext(ctx).Warn().Err(err).Msg("cache write failed")
		}
		return v, nil
	})

	var zero T
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return zero, r.Err
		}
		v, _ := r.Val.(T) // nil interface values are a valid T
		return v, nil
	}
}
