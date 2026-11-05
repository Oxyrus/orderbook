// Package redisx owns the Redis client: connection, pooling, tracing,
// metrics and a typed cache-aside helper (Cache).
//
// Redis is optional (REDIS_ENABLED). Features that need it receive the
// client from app.Run; see docs/redis.md for usage patterns.
package redisx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/Oxyrus/orderbook/internal/config"
)

// Nil is returned by the raw client when a key does not exist.
const Nil = redis.Nil

// IsNil reports whether err means "key not found".
func IsNil(err error) bool { return errors.Is(err, redis.Nil) }

// Connect creates a client from cfg and verifies connectivity. The client
// manages its own connection pool and is safe for concurrent use; create
// one per process and share it.
func Connect(ctx context.Context, cfg config.Redis, tracing bool) (*redis.Client, error) {
	opts, err := Options(cfg)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewClient(opts)

	if tracing {
		// Spans carry the command name only (not arguments), so values never
		// end up in traces.
		if err := redisotel.InstrumentTracing(rdb, redisotel.WithDBStatement(false)); err != nil {
			_ = rdb.Close()
			return nil, fmt.Errorf("redis tracing: %w", err)
		}
	}

	pingTimeout := cfg.DialTimeout
	if pingTimeout <= 0 {
		pingTimeout = 5 * time.Second
	}
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return rdb, nil
}

// Options converts cfg into go-redis options. Settings encoded in the URL
// (credentials, DB number, TLS via rediss://) are kept; pool and timeout
// settings come from cfg.
func Options(cfg config.Redis) (*redis.Options, error) {
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	if cfg.PoolSize > 0 {
		opts.PoolSize = cfg.PoolSize
	}
	opts.MinIdleConns = cfg.MinIdleConns
	opts.PoolTimeout = cfg.PoolTimeout
	opts.ConnMaxIdleTime = cfg.ConnMaxIdleTime
	opts.ConnMaxLifetime = cfg.ConnMaxLifetime
	opts.DialTimeout = cfg.DialTimeout
	opts.ReadTimeout = cfg.ReadTimeout
	opts.WriteTimeout = cfg.WriteTimeout
	// Honour request deadlines (HTTP_REQUEST_TIMEOUT) in addition to the
	// socket timeouts above.
	opts.ContextTimeoutEnabled = true
	return opts, nil
}
