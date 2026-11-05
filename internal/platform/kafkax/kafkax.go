// Package kafkax wraps franz-go (github.com/twmb/franz-go) for consuming the
// domain events Debezium publishes.
//
// Delivery is at least once, end to end: Debezium may re-publish events
// after a connector restart, and the consumer commits offsets only after the
// handler succeeded. Handlers must therefore be idempotent, typically by
// recording the event id (the "id" header) in the same transaction as their
// effect.
package kafkax

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/Oxyrus/orderbook/internal/config"
)

// NewClient returns a Kafka client for cfg. opts add role-specific options
// (consumer group, topics). Connections are established lazily; use Ping
// to fail fast at startup.
func NewClient(cfg config.Kafka, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DialTimeout(cfg.DialTimeout),
	}
	if cfg.TLS {
		base = append(base, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	if cfg.SASLUser != "" {
		m, err := saslMechanism(cfg)
		if err != nil {
			return nil, err
		}
		base = append(base, kgo.SASL(m))
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return cl, nil
}

func saslMechanism(cfg config.Kafka) (sasl.Mechanism, error) {
	switch cfg.SASLMechanism {
	case "PLAIN":
		return plain.Auth{User: cfg.SASLUser, Pass: cfg.SASLPassword}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: cfg.SASLUser, Pass: cfg.SASLPassword}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: cfg.SASLUser, Pass: cfg.SASLPassword}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", cfg.SASLMechanism)
	}
}

// Ping checks connectivity to the cluster (used at startup and by the
// readiness probe).
func Ping(ctx context.Context, cl *kgo.Client) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err != nil {
		return fmt.Errorf("kafka ping: %w", err)
	}
	return nil
}
