package app

import (
	"context"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/Oxyrus/orderbook/internal/config"
	"github.com/Oxyrus/orderbook/internal/health"
	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/internal/platform/kafkax"
	"github.com/Oxyrus/orderbook/internal/platform/outbox"
	"github.com/Oxyrus/orderbook/internal/platform/telemetry"
	"github.com/Oxyrus/orderbook/internal/sales"
)

// RunWorker runs the event-processing side of the service (`api worker`):
//
//   - consumes the order events Debezium publishes from the outbox and
//     maintains the sales projection;
//   - prunes outbox rows and remembered event ids past their retention.
//
// It is a separate process from the API so the two scale and fail
// independently: a Kafka outage stops projections, never order taking.
// Run as many replicas as the topic has partitions; the consumer group
// spreads partitions over them.
func RunWorker(ctx context.Context, cfg config.Config, log zerolog.Logger) error {
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

	hc := health.New(2 * time.Second)
	hc.SetStarting()
	admin := &http.Server{
		Handler:           NewAdminRouter(cfg, reg, hc),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          newStdLogger(log),
	}
	adminLn, err := listen(ctx, "admin", cfg.Admin.Addr)
	if err != nil {
		return err
	}
	adminDone := make(chan error, 1)
	go func() { adminDone <- serve(log, "admin", admin, adminLn) }()
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = admin.Shutdown(sctx)
	}()

	db, err := database.Connect(ctx, cfg.DB, cfg.Telemetry.Enabled)
	if err != nil {
		return err
	}
	defer db.Close()
	reg.MustRegister(database.NewPoolCollector(db.Pool))

	cl, err := kafkax.NewClient(cfg.Kafka, kafkax.ConsumerClientOpts(cfg.Kafka.GroupID, cfg.Kafka.OrderEventsTopic)...)
	if err != nil {
		return err
	}
	// Close leaves the consumer group, so partitions move to the other
	// replicas immediately instead of after the session timeout.
	defer cl.Close()
	if err := kafkax.Ping(ctx, cl); err != nil {
		return err
	}
	log.Info().Strs("brokers", cfg.Kafka.Brokers).Str("topic", cfg.Kafka.OrderEventsTopic).Str("group", cfg.Kafka.GroupID).Msg("kafka connected")

	hc.Register("postgres", db.Ping)
	hc.Register("kafka", func(ctx context.Context) error { return kafkax.Ping(ctx, cl) })

	consumer := kafkax.NewConsumer(cl, kafkax.ConsumerOptions{
		Name:        sales.ConsumerName,
		Handler:     sales.NewProjector(sales.NewPostgresStore(db), db).Handle,
		DLQTopic:    cfg.Kafka.DLQTopic,
		MaxAttempts: cfg.Kafka.MaxAttempts,
		Logger:      log,
		Registerer:  reg,
	})

	hc.SetStarted()
	log.Info().Msg("worker ready")

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return consumer.Run(gctx) })
	g.Go(func() error {
		housekeeping(gctx, log, db, cfg.Outbox)
		return nil
	})
	g.Go(func() error {
		select {
		case err := <-adminDone:
			return err
		case <-gctx.Done():
			hc.SetDraining()
			return nil
		}
	})
	return g.Wait()
}

// housekeeping prunes the outbox and the dedup table periodically. Every
// replica runs it; the DELETEs are idempotent, so overlapping runs are
// harmless.
func housekeeping(ctx context.Context, log zerolog.Logger, db *database.DB, cfg config.Outbox) {
	t := time.NewTicker(cfg.PruneInterval)
	defer t.Stop()
	for {
		if n, err := outbox.Prune(ctx, db, cfg.Retention); err != nil {
			if ctx.Err() == nil {
				log.Warn().Err(err).Msg("outbox prune failed")
			}
		} else if n > 0 {
			log.Info().Int64("deleted", n).Msg("outbox pruned")
		}
		if n, err := sales.PruneProcessed(ctx, db, cfg.DedupRetention); err != nil {
			if ctx.Err() == nil {
				log.Warn().Err(err).Msg("processed events prune failed")
			}
		} else if n > 0 {
			log.Info().Int64("deleted", n).Msg("processed events pruned")
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
