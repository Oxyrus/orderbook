// Command api is the service entrypoint.
//
// Usage:
//
//	api [serve]                 start the HTTP servers (default)
//	api worker                  consume order events (Kafka) and maintain
//	                            projections; prune the outbox
//	api seed [flags]            migrate, then create demo tenants, products
//	                            and orders (see api seed -h)
//	api migrate up              apply all pending migrations
//	api migrate up-to VERSION   apply migrations up to VERSION
//	api migrate down            roll back the latest migration
//	api migrate down-to VERSION roll back to VERSION (0 = everything)
//	api migrate status          list migrations and their state
//	api migrate version         print the current schema version
//	api healthcheck             probe the local admin /livez endpoint
//	                            (used by the Docker HEALTHCHECK; distroless
//	                            images have no curl/wget)
//	api version                 print build information
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/Oxyrus/orderbook/internal/app"
	"github.com/Oxyrus/orderbook/internal/buildinfo"
	"github.com/Oxyrus/orderbook/internal/config"
	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/internal/platform/logger"
	"github.com/Oxyrus/orderbook/migrations"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	cmd := "serve"
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "version":
		_ = json.NewEncoder(os.Stdout).Encode(buildinfo.Get())
		return 0
	case "healthcheck":
		return healthcheck()
	case "serve", "worker", "migrate", "seed":
	case "-h", "--help", "help":
		fmt.Fprintln(os.Stderr, "usage: api [serve | worker | seed [flags] | migrate <up|up-to N|down|down-to N|status|version> | healthcheck | version]")
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q (try --help)\n", cmd)
		return 2
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration:\n%v\n", err)
		return 1
	}
	log := logger.New(logger.Options{
		Level:       cfg.Log.Level,
		Format:      cfg.Log.Format,
		ServiceName: cfg.ServiceName,
		Environment: cfg.Environment,
		Version:     buildinfo.Version,
	})
	zerolog.DefaultContextLogger = &log
	for _, w := range cfg.Warnings() {
		log.Warn().Msg("config: " + w)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// After the first signal, restore the default handlers so a second
	// Ctrl-C (or SIGTERM) kills the process instead of waiting for the
	// graceful shutdown to finish.
	go func() {
		<-ctx.Done()
		stop()
	}()

	switch cmd {
	case "serve":
		log.Info().Interface("build", buildinfo.Get()).Msg("starting")
		err = app.Run(ctx, cfg, log)
	case "worker":
		log.Info().Interface("build", buildinfo.Get()).Msg("starting worker")
		err = app.RunWorker(ctx, cfg, log)
	case "migrate":
		err = migrate(ctx, cfg, log, args)
	case "seed":
		err = seed(ctx, cfg, log, args)
	}
	if err != nil {
		log.Error().Err(err).Msg("exiting with error")
		return 1
	}
	return 0
}

func migrate(ctx context.Context, cfg config.Config, log zerolog.Logger, args []string) error {
	if len(args) == 0 {
		return errors.New("migrate: missing subcommand (up|up-to|down|down-to|status|version)")
	}
	db, err := database.Connect(ctx, cfg.DB, false)
	if err != nil {
		return err
	}
	defer db.Close()

	if args[0] == "up" {
		return app.Migrate(ctx, db, log)
	}

	m, err := database.NewMigrator(db, migrations.FS)
	if err != nil {
		return err
	}
	defer m.Close()

	versionArg := func() (int64, error) {
		if len(args) < 2 {
			return 0, fmt.Errorf("migrate %s: missing VERSION", args[0])
		}
		return strconv.ParseInt(args[1], 10, 64)
	}

	switch args[0] {
	case "up-to":
		v, err := versionArg()
		if err != nil {
			return err
		}
		res, err := m.UpTo(ctx, v)
		for _, r := range res {
			log.Info().Int64("schema_version", r.Source.Version).Str("file", r.Source.Path).Msg("migration applied")
		}
		return err
	case "down":
		r, err := m.Down(ctx)
		if r != nil {
			log.Info().Int64("schema_version", r.Source.Version).Str("file", r.Source.Path).Msg("migration rolled back")
		}
		return err
	case "down-to":
		v, err := versionArg()
		if err != nil {
			return err
		}
		res, err := m.DownTo(ctx, v)
		for _, r := range res {
			log.Info().Int64("schema_version", r.Source.Version).Str("file", r.Source.Path).Msg("migration rolled back")
		}
		return err
	case "status":
		st, err := m.Status(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("%-8s %-10s %-25s %s\n", "VERSION", "STATE", "APPLIED AT", "FILE")
		for _, s := range st {
			applied := "-"
			if !s.AppliedAt.IsZero() {
				applied = s.AppliedAt.Format(time.RFC3339)
			}
			fmt.Printf("%-8d %-10s %-25s %s\n", s.Source.Version, s.State, applied, s.Source.Path)
		}
		return nil
	case "version":
		v, err := m.Version(ctx)
		if err != nil {
			return err
		}
		fmt.Println(v)
		return nil
	default:
		return fmt.Errorf("migrate: unknown subcommand %q", args[0])
	}
}

func seed(ctx context.Context, cfg config.Config, log zerolog.Logger, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	tenants := fs.String("tenants", strings.Join(app.DefaultSeedTenants, ","), "comma-separated tenant ids")
	products := fs.Int("products", 25, "products per tenant (existing SKUs are reused)")
	orders := fs.Int("orders", 40, "new orders per tenant, on every run")
	cancel := fs.Float64("cancel", 0.1, "fraction of new orders to cancel (0..1)")
	randSeed := fs.Uint64("seed", 1, "random seed (same seed → same shape of data)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch {
	case *products < 1:
		return errors.New("seed: -products must be >= 1")
	case *orders < 0:
		return errors.New("seed: -orders must be >= 0")
	case *cancel < 0 || *cancel > 1:
		return errors.New("seed: -cancel must be between 0 and 1")
	}

	db, err := database.Connect(ctx, cfg.DB, false)
	if err != nil {
		return err
	}
	defer db.Close()

	results, err := app.Seed(ctx, db, log, app.SeedOptions{
		Tenants:           strings.Split(*tenants, ","),
		ProductsPerTenant: *products,
		OrdersPerTenant:   *orders,
		CancelRatio:       *cancel,
		RandSeed:          *randSeed,
	})
	fmt.Printf("%-12s %-10s %-8s %9s %7s %10s\n", "TENANT", "NODE", "CURRENCY", "PRODUCTS+", "ORDERS", "CANCELLED")
	for _, r := range results {
		fmt.Printf("%-12s %-10s %-8s %9d %7d %10d\n", r.Tenant, r.Node, r.Currency, r.ProductsCreated, r.OrdersPlaced, r.OrdersCancelled)
	}
	return err
}

func healthcheck() int {
	addr := os.Getenv("ADMIN_ADDR")
	if addr == "" {
		addr = ":9090"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/livez") //nolint:gosec,noctx // local probe of our own admin port
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "unhealthy:", resp.Status)
		return 1
	}
	return 0
}
