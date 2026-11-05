// Package redistest provides a real Redis server for integration tests.
//
// One Redis container is started per test binary (package). Tests share it
// and isolate themselves with a unique key namespace from Namespace, which
// deletes its keys when the test finishes, so tests can run with
// t.Parallel().
//
// Usage:
//
//	var rs *redistest.Server
//
//	func TestMain(m *testing.M) { redistest.Main(m, &rs) }
//
//	func TestSomething(t *testing.T) {
//		rdb := rs.NewClient(t)
//		ns := redistest.Namespace(t, rdb) // prefix every key with ns
//		...
//	}
//
// Set TEST_REDIS_URL to an existing server (e.g. redis://localhost:6379/15)
// to skip starting a container.
package redistest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/Oxyrus/orderbook/internal/config"
	"github.com/Oxyrus/orderbook/internal/platform/redisx"
)

// Image is the Redis image used for tests. Keep it in sync with
// docker-compose.yml and production.
const Image = "redis:8-alpine"

// Server is a running Redis server.
type Server struct {
	URL       string
	container *tcredis.RedisContainer
}

// Main starts the server, runs the tests and tears everything down. It
// calls os.Exit with the test result.
func Main(m *testing.M, srv **Server) {
	ctx := context.Background()
	s, err := Start(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "redistest: %v\n", err)
		os.Exit(1)
	}
	*srv = s
	code := m.Run()
	if err := s.Stop(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "redistest: stop: %v\n", err)
	}
	os.Exit(code)
}

// Start launches (or connects to) Redis.
func Start(ctx context.Context) (*Server, error) {
	s := &Server{URL: os.Getenv("TEST_REDIS_URL")}
	if s.URL != "" {
		return s, nil
	}
	c, err := tcredis.Run(ctx, Image)
	if err != nil {
		return nil, fmt.Errorf("start redis container: %w", err)
	}
	s.container = c
	if s.URL, err = c.ConnectionString(ctx); err != nil {
		_ = s.Stop(ctx)
		return nil, err
	}
	return s, nil
}

// Stop terminates the container (no-op for external servers).
func (s *Server) Stop(context.Context) error {
	if s.container == nil {
		return nil
	}
	return testcontainers.TerminateContainer(s.container)
}

// Config returns a client configuration pointing at the server.
func (s *Server) Config() config.Redis {
	return config.Redis{
		Enabled:         true,
		URL:             s.URL,
		PoolSize:        10,
		PoolTimeout:     2 * time.Second,
		ConnMaxIdleTime: time.Minute,
		ConnMaxLifetime: time.Hour,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     2 * time.Second,
		WriteTimeout:    2 * time.Second,
	}
}

// NewClient returns a connected client that is closed when t finishes.
func (s *Server) NewClient(t testing.TB) *redis.Client {
	t.Helper()
	rdb, err := redisx.Connect(context.Background(), s.Config(), false)
	if err != nil {
		t.Fatalf("redistest: connect: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// Namespace returns a key prefix unique to t (e.g. "test:3fa1c2"; build keys
// as ns+":..." or pass it to redisx.NewCache) and
// deletes every key under it when t finishes.
func Namespace(t testing.TB, rdb *redis.Client) string {
	t.Helper()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	ns := "test:" + hex.EncodeToString(b)
	t.Cleanup(func() {
		ctx := context.Background()
		iter := rdb.Scan(ctx, 0, ns+":*", 100).Iterator()
		for iter.Next(ctx) {
			_ = rdb.Del(ctx, iter.Val()).Err()
		}
	})
	return ns
}
