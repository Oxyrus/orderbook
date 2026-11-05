// Package app wires every component together and manages the process
// lifecycle (startup, serving, graceful shutdown).
package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/Oxyrus/orderbook/internal/catalog"
	"github.com/Oxyrus/orderbook/internal/config"
	"github.com/Oxyrus/orderbook/internal/health"
	"github.com/Oxyrus/orderbook/internal/order"
	"github.com/Oxyrus/orderbook/internal/platform/auth"
	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/internal/platform/outbox"
	"github.com/Oxyrus/orderbook/internal/platform/redisx"
	"github.com/Oxyrus/orderbook/internal/platform/telemetry"
	"github.com/Oxyrus/orderbook/internal/platform/tenant"
	"github.com/Oxyrus/orderbook/internal/sales"
	"github.com/Oxyrus/orderbook/migrations"
)

// Run starts the service and blocks until ctx is cancelled (SIGINT/SIGTERM)
// or a server fails, then shuts down gracefully.
//
// The admin server starts first, before any dependency is touched, so
// /livez answers and /readyz reports "starting" while the service connects
// to the database, runs migrations and discovers the OAuth issuer. Otherwise
// a slow migration or identity provider looks like a hung process and the
// orchestrator restarts it halfway through.
func Run(ctx context.Context, cfg config.Config, log zerolog.Logger) error {
	// --- Telemetry --------------------------------------------------------
	shutdownTracing, err := telemetry.SetupTracing(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(sctx); err != nil {
			log.Warn().Err(err).Msg("tracing shutdown")
		}
	}()
	reg := telemetry.NewRegistry(cfg.ServiceName)

	// --- Admin server (probes, metrics) -----------------------------------
	hc := health.New(2 * time.Second)
	hc.SetStarting()
	admin := &http.Server{
		Handler:           NewAdminRouter(cfg, reg, hc),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       120 * time.Second,
		// No WriteTimeout: /debug/pprof/profile and /trace stream for as
		// long as the caller asks (?seconds=30).
		ErrorLog: newStdLogger(log),
	}
	adminLn, err := listen(ctx, "admin", cfg.Admin.Addr)
	if err != nil {
		return err
	}
	adminDone := make(chan error, 1)
	go func() { adminDone <- serve(log, "admin", admin, adminLn) }()
	running := false
	defer func() {
		// Initialisation failed: stop the admin server. Once running, the
		// shutdown goroutine below owns it.
		if !running {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = admin.Shutdown(sctx)
		}
	}()

	// --- Database ---------------------------------------------------------
	db, err := database.Connect(ctx, cfg.DB, cfg.Telemetry.Enabled)
	if err != nil {
		return err
	}
	defer db.Close()
	reg.MustRegister(database.NewPoolCollector(db.Pool))
	log.Info().Msg("database connected")

	if cfg.DB.AutoMigrate {
		if err := Migrate(ctx, db, log); err != nil {
			return err
		}
	}

	// --- Redis (optional) -------------------------------------------------
	// rdb is nil when REDIS_ENABLED=false. Pass it to features that need it
	// (see docs/redis.md); such features should require REDIS_ENABLED in
	// config.Validate rather than checking for nil at runtime.
	var rdb *goredis.Client
	if cfg.Redis.Enabled {
		rdb, err = redisx.Connect(ctx, cfg.Redis, cfg.Telemetry.Enabled)
		if err != nil {
			return err
		}
		defer func() {
			if err := rdb.Close(); err != nil {
				log.Warn().Err(err).Msg("redis close")
			}
		}()
		reg.MustRegister(redisx.NewPoolCollector(rdb))
		log.Info().Msg("redis connected")
	}

	// --- Auth -------------------------------------------------------------
	authMW, err := newAuthMiddleware(ctx, cfg, log)
	if err != nil {
		return err
	}

	// --- Health -----------------------------------------------------------
	hc.Register("postgres", db.Ping)
	if rdb != nil && cfg.Redis.ReadinessCheck {
		hc.Register("redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() })
	}

	// --- Features ---------------------------------------------------------
	handlers := NewFeatureHandlers(db)

	// --- Public API server ------------------------------------------------
	api := &http.Server{
		Handler: NewRouter(RouterDeps{
			Config:   cfg,
			Logger:   log,
			Registry: reg,
			Health:   hc,
			Auth:     authMW,
			Catalog:  handlers.Catalog,
			Orders:   handlers.Orders,
			Sales:    handlers.Sales,
		}),
		ReadTimeout:       cfg.HTTP.ReadTimeout,
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
		WriteTimeout:      cfg.HTTP.WriteTimeout,
		IdleTimeout:       cfg.HTTP.IdleTimeout,
		MaxHeaderBytes:    1 << 20,
		ErrorLog:          newStdLogger(log),
		BaseContext:       func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}
	apiLn, err := listen(ctx, "api", cfg.HTTP.Addr)
	if err != nil {
		return err
	}

	running = true
	hc.SetStarted()
	log.Info().Msg("ready")

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return serve(log, "api", api, apiLn) })
	g.Go(func() error { return <-adminDone })

	// Graceful shutdown.
	g.Go(func() error {
		<-gctx.Done()
		log.Info().Msg("shutdown initiated")
		hc.SetDraining()
		if d := cfg.HTTP.ShutdownDelay; d > 0 && ctx.Err() != nil {
			log.Info().Dur("delay", d).Msg("draining before shutdown")
			time.Sleep(d)
		}

		sctx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
		defer cancel()
		apiErr := forceShutdown(sctx, log, "api", api)
		adminErr := forceShutdown(sctx, log, "admin", admin)
		if err := errors.Join(apiErr, adminErr); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		log.Info().Msg("servers stopped")
		return nil
	})

	return g.Wait()
}

// FeatureHandlers are the HTTP handlers of every feature.
type FeatureHandlers struct {
	Catalog *catalog.Handler
	Orders  *order.Handler
	Sales   *sales.Handler
}

// NewFeatureHandlers wires the features on top of db. Tests reuse it to
// build a router against a real database.
func NewFeatureHandlers(db *database.DB) FeatureHandlers {
	tenants := tenant.NewProvisioner(db)
	products := catalog.NewPostgresRepository(db)
	orders := order.NewService(order.NewPostgresRepository(db), products, outbox.NewWriter(db), tenants, db)
	return FeatureHandlers{
		Catalog: catalog.NewHandler(catalog.NewService(products, tenants)),
		Orders:  order.NewHandler(orders),
		Sales:   sales.NewHandler(sales.NewPostgresStore(db)),
	}
}

// listen binds addr up front so a port conflict fails startup immediately
// and the logged address is the real one (useful with ":0").
func listen(ctx context.Context, name, addr string) (net.Listener, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%s server: %w", name, err)
	}
	return ln, nil
}

// forceShutdown shuts srv down gracefully and, if in-flight requests are
// still running when ctx expires, closes their connections. Closing them
// cancels the request contexts, so handlers stop their database work
// instead of running on while the pools are closed underneath them.
func forceShutdown(ctx context.Context, log zerolog.Logger, name string, srv *http.Server) error {
	err := srv.Shutdown(ctx)
	if err == nil {
		return nil
	}
	log.Warn().Err(err).Str("server", name).Msg("graceful shutdown timed out, closing remaining connections")
	return errors.Join(err, srv.Close())
}

// serve runs srv on ln until it is shut down.
func serve(log zerolog.Logger, name string, srv *http.Server, ln net.Listener) error {
	log.Info().Str("server", name).Str("addr", ln.Addr().String()).Msg("listening")
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s server: %w", name, err)
	}
	return nil
}

func newAuthMiddleware(ctx context.Context, cfg config.Config, log zerolog.Logger) (func(http.Handler) http.Handler, error) {
	if !cfg.Auth.Enabled {
		log.Warn().Msg("AUTHENTICATION DISABLED: every request runs as dev-user with all scopes")
		return auth.Static(auth.DevPrincipal), nil
	}
	v, err := auth.NewJWTVerifier(context.WithoutCancel(ctx), cfg.Auth)
	if err != nil {
		return nil, err
	}
	log.Info().Str("issuer", cfg.Auth.Issuer).Str("audience", cfg.Auth.Audience).Msg("oauth2 token validation enabled")
	return auth.Authenticate(v), nil
}

// Migrate applies all pending migrations.
func Migrate(ctx context.Context, db *database.DB, log zerolog.Logger) error {
	m, err := database.NewMigrator(db, migrations.FS)
	if err != nil {
		return err
	}
	defer m.Close()
	results, err := m.Up(ctx)
	if err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	for _, r := range results {
		log.Info().Int64("schema_version", r.Source.Version).Str("file", r.Source.Path).Dur("duration", r.Duration).Msg("migration applied")
	}
	v, err := m.Version(ctx)
	if err != nil {
		return err
	}
	log.Info().Int64("schema_version", v).Int("applied", len(results)).Msg("database schema up to date")
	return nil
}
