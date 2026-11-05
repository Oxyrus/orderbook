// Package pgtest provides real PostgreSQL + Citus databases for integration
// tests.
//
// One single-node Citus container (the coordinator holds every shard) is
// started per test binary (package). That exercises the real distributed
// planner, executor and DDL rules (distribution columns in keys, co-located
// foreign keys, router vs multi-shard queries) without a multi-node
// cluster; docker-compose runs the real coordinator + workers topology. Migrations
// run once into a template database; every test then gets its own database
// cloned from that template with CREATE DATABASE ... TEMPLATE, which takes a
// few milliseconds. Tests are therefore fully isolated and can run with
// t.Parallel().
//
// Usage:
//
//	var pg *pgtest.Server
//
//	func TestMain(m *testing.M) { pgtest.Main(m, &pg) }
//
//	func TestSomething(t *testing.T) {
//		db := pg.NewDB(t)
//		...
//	}
//
// Set TEST_DATABASE_URL to an existing Citus server (superuser connection
// URL) to skip starting a container, e.g. in CI with a service container.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/Oxyrus/orderbook/internal/config"
	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/migrations"
)

// Image is the Citus image used for tests. Keep it in sync with
// docker-compose.yml and production. It is published for linux/amd64 only
// and runs emulated on ARM machines (slower, but works).
const Image = "citusdata/citus:14.0-pg17"

const templateDB = "orderbook_template"

// Server is a PostgreSQL server holding a migrated template database.
type Server struct {
	adminURL  string
	container *postgres.PostgresContainer
}

// Main starts the server, runs the tests and tears everything down. It
// calls os.Exit with the test result.
func Main(m *testing.M, srv **Server) {
	ctx := context.Background()
	s, err := Start(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: %v\n", err)
		os.Exit(1)
	}
	*srv = s
	code := m.Run()
	if err := s.Stop(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: stop: %v\n", err)
	}
	os.Exit(code)
}

// Start launches (or connects to) PostgreSQL and prepares the template.
func Start(ctx context.Context) (*Server, error) {
	s := &Server{adminURL: os.Getenv("TEST_DATABASE_URL")}
	if s.adminURL == "" {
		c, err := postgres.Run(ctx, Image,
			postgres.WithDatabase("postgres"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			postgres.BasicWaitStrategies(),
			// Durability is irrelevant in tests; trade it for speed.
			testcontainers.WithCmd("postgres", "-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off"),
		)
		if err != nil {
			return nil, fmt.Errorf("start postgres container: %w", err)
		}
		s.container = c
		if s.adminURL, err = c.ConnectionString(ctx, "sslmode=disable"); err != nil {
			return nil, err
		}
	}
	if err := s.prepareTemplate(ctx); err != nil {
		_ = s.Stop(ctx)
		return nil, err
	}
	return s, nil
}

// Stop terminates the container (no-op for external servers).
func (s *Server) Stop(ctx context.Context) error {
	if s.container == nil {
		return nil
	}
	return testcontainers.TerminateContainer(s.container)
}

func (s *Server) prepareTemplate(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, s.adminURL)
	if err != nil {
		return fmt.Errorf("connect admin: %w", err)
	}
	defer conn.Close(ctx)

	for _, q := range []string{
		`DROP DATABASE IF EXISTS ` + templateDB + ` WITH (FORCE)`,
		// Citus does not propagate CREATE DATABASE to other nodes (it logs
		// a NOTICE); irrelevant on a single node.
		`CREATE DATABASE ` + templateDB,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}

	db, err := database.Connect(ctx, dbConfig(s.urlFor(templateDB)), false)
	if err != nil {
		return err
	}
	defer db.Close()
	m, err := database.NewMigrator(db, migrations.FS)
	if err != nil {
		return err
	}
	defer m.Close()
	if _, err := m.Up(ctx); err != nil {
		return fmt.Errorf("migrate template: %w", err)
	}
	_ = m.Close()
	db.Close()

	// CREATE DATABASE ... TEMPLATE requires that nobody is connected to the
	// template, but Citus runs a maintenance daemon in every database that
	// has the extension. Forbid new connections and terminate the existing
	// ones; clones get ALLOW_CONNECTIONS true again by default.
	for _, q := range []string{
		`ALTER DATABASE ` + templateDB + ` WITH ALLOW_CONNECTIONS false`,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '` + templateDB + `' AND pid <> pg_backend_pid()`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}
	return nil
}

// NewDB creates an isolated, fully migrated database for t and drops it when
// the test finishes.
func (s *Server) NewDB(t testing.TB) *database.DB {
	t.Helper()
	db, err := database.Connect(context.Background(), dbConfig(s.NewDatabase(t)), false)
	if err != nil {
		t.Fatalf("pgtest: connect: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

// NewDatabase creates an isolated, fully migrated database for t and returns
// its connection URL (for code that opens its own pool, such as app.Run). It
// is dropped when the test finishes.
func (s *Server) NewDatabase(t testing.TB) string {
	t.Helper()
	ctx := context.Background()
	name := "test_" + randomSuffix()

	conn, err := pgx.Connect(ctx, s.adminURL)
	if err != nil {
		t.Fatalf("pgtest: connect admin: %v", err)
	}
	defer conn.Close(ctx)
	if err := createFromTemplate(ctx, conn, name); err != nil {
		t.Fatalf("pgtest: create database: %v", err)
	}

	t.Cleanup(func() {
		c, err := pgx.Connect(ctx, s.adminURL)
		if err != nil {
			return
		}
		defer c.Close(ctx)
		_, _ = c.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, name))
	})
	return s.urlFor(name)
}

// createFromTemplate clones the template. Clones run in parallel and a
// maintenance daemon may still be shutting down, so "being accessed by
// other users" is retried briefly.
func createFromTemplate(ctx context.Context, conn *pgx.Conn, name string) error {
	var err error
	for range 50 {
		if _, err = conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s TEMPLATE %s`, name, templateDB)); err == nil {
			return nil
		}
		if !database.IsCode(err, "55006") { // object_in_use
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}

func (s *Server) urlFor(dbName string) string {
	u, err := url.Parse(s.adminURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + dbName
	return u.String()
}

func dbConfig(dsn string) config.DB {
	return config.DB{
		URL:               dsn,
		MaxConns:          10,
		MinConns:          0,
		MaxConnLifetime:   time.Hour,
		MaxConnIdleTime:   time.Minute,
		HealthCheckPeriod: time.Minute,
		ConnectTimeout:    10 * time.Second,
	}
}

func randomSuffix() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
