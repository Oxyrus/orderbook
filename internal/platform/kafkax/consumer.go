package kafkax

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Message is a consumed Kafka record.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

// Header returns the value of header k ("" when absent).
func (m Message) Header(k string) string { return m.Headers[k] }

// Handler processes one message. Returning an error retries the message
// (with backoff) up to MaxAttempts times before it goes to the dead-letter
// topic; wrap the error with Permanent to skip the retries.
type Handler func(ctx context.Context, m Message) error

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks err as non-retryable (malformed payload, unknown event
// version): retrying can't fix it, so the message goes straight to the DLQ.
func Permanent(err error) error { return permanentError{err} }

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool { return errors.As(err, new(permanentError)) }

// ConsumerOptions configures a Consumer.
type ConsumerOptions struct {
	// Name identifies the consumer in logs and metrics.
	Name        string
	Handler     Handler
	DLQTopic    string
	MaxAttempts int
	Logger      zerolog.Logger
	Registerer  prometheus.Registerer
	// Backoff before retry n (starting at 1). Defaults to exponential
	// backoff from 200ms, capped at 10s.
	Backoff func(n int) time.Duration
}

// Consumer runs a consume-process-commit loop on a client created with a
// consumer group, kgo.DisableAutoCommit and kgo.BlockRebalanceOnPoll (see
// ConsumerClientOpts).
//
// Records are processed in order per partition. Offsets are committed after
// each polled batch has been handled, so a crash re-delivers at most that
// batch. Blocking rebalances during processing guarantees that a partition
// is never revoked while one of its records is in flight, which would let
// another instance process it concurrently.
type Consumer struct {
	cl      *kgo.Client
	opts    ConsumerOptions
	tracer  trace.Tracer
	results *prometheus.CounterVec
	latency *prometheus.HistogramVec
}

// ConsumerClientOpts returns the client options Consumer relies on.
func ConsumerClientOpts(group string, topics ...string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		// A brand-new group starts at the beginning: events published
		// before the first deployment of the consumer must not be lost.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
}

// NewConsumer returns a Consumer. It registers its metrics on
// opts.Registerer.
func NewConsumer(cl *kgo.Client, opts ConsumerOptions) *Consumer {
	if opts.MaxAttempts < 1 {
		opts.MaxAttempts = 1
	}
	if opts.Backoff == nil {
		opts.Backoff = func(n int) time.Duration {
			return min(200*time.Millisecond<<min(n-1, 6), 10*time.Second)
		}
	}
	c := &Consumer{
		cl:     cl,
		opts:   opts,
		tracer: otel.Tracer("github.com/Oxyrus/orderbook/internal/platform/kafkax"),
		results: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "kafka_consumer_messages_total",
			Help:        "Messages handled by outcome (processed, retried, dead_lettered).",
			ConstLabels: prometheus.Labels{"consumer": opts.Name},
		}, []string{"topic", "outcome"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "kafka_consumer_handle_duration_seconds",
			Help:        "Time spent in the message handler per attempt.",
			ConstLabels: prometheus.Labels{"consumer": opts.Name},
			Buckets:     prometheus.ExponentialBuckets(0.001, 2, 14),
		}, []string{"topic"}),
	}
	if opts.Registerer != nil {
		opts.Registerer.MustRegister(c.results, c.latency)
	}
	return c
}

// Run consumes until ctx is cancelled. It returns nil on a clean shutdown.
// The caller closes the client afterwards (which leaves the group).
func (c *Consumer) Run(ctx context.Context) error {
	log := c.opts.Logger.With().Str("consumer", c.opts.Name).Logger()
	log.Info().Msg("consumer started")
	for {
		fetches := c.cl.PollRecords(ctx, 500)
		if fetches.IsClientClosed() || stopping(ctx) {
			c.cl.AllowRebalance()
			log.Info().Msg("consumer stopped")
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			// franz-go retries fetch errors itself; these are informational.
			log.Warn().Err(err).Str("topic", topic).Int32("partition", partition).Msg("fetch error")
		})

		var done []*kgo.Record
		stopped := false
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, rec := range p.Records {
				if stopped {
					return
				}
				if !c.process(ctx, log, rec) {
					stopped = true // shutting down: leave the rest uncommitted
					return
				}
				done = append(done, rec)
			}
		})

		if len(done) > 0 {
			// Commit even while shutting down, so finished work isn't
			// redelivered; bounded so a dead broker can't block shutdown.
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			if err := c.cl.CommitRecords(cctx, done...); err != nil {
				log.Warn().Err(err).Int("records", len(done)).Msg("offset commit failed; records will be redelivered")
			}
			cancel()
		}
		c.cl.AllowRebalance()
		if stopped {
			log.Info().Msg("consumer stopped")
			return nil
		}
	}
}

// process handles one record with retries and dead-lettering. It returns
// false only if ctx was cancelled before the record was dealt with.
func (c *Consumer) process(ctx context.Context, log zerolog.Logger, rec *kgo.Record) bool {
	msg := toMessage(rec)
	log = log.With().Str("topic", msg.Topic).Int32("partition", msg.Partition).Int64("offset", msg.Offset).
		Str("event_id", msg.Header("id")).Str("event_type", msg.Header("event_type")).
		Str("tenant", msg.Header("tenant_id")).Logger()

	for attempt := 1; ; attempt++ {
		err := c.handle(ctx, log, msg)
		if err == nil {
			c.results.WithLabelValues(msg.Topic, "processed").Inc()
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		if IsPermanent(err) || attempt >= c.opts.MaxAttempts {
			return c.deadLetter(ctx, log, rec, err, attempt)
		}
		c.results.WithLabelValues(msg.Topic, "retried").Inc()
		wait := c.opts.Backoff(attempt)
		log.Warn().Err(err).Int("attempt", attempt).Dur("retry_in", wait).Msg("handler failed, retrying")
		select {
		case <-ctx.Done():
			return false
		case <-time.After(wait):
		}
	}
}

func (c *Consumer) handle(ctx context.Context, log zerolog.Logger, msg Message) error {
	// Continue the trace of the HTTP request that wrote the outbox row.
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(msg.Headers))
	ctx, span := c.tracer.Start(ctx, msg.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.destination.name", msg.Topic),
			attribute.String("messaging.consumer.group.name", c.opts.Name),
			attribute.Int("messaging.destination.partition.id", int(msg.Partition)),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
			attribute.String("messaging.message.id", msg.Header("id")),
		))
	defer span.End()

	start := time.Now()
	err := c.opts.Handler(log.WithContext(ctx), msg)
	c.latency.WithLabelValues(msg.Topic).Observe(time.Since(start).Seconds())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// deadLetter publishes rec to the DLQ topic. It keeps retrying the publish
// (a lost event is worse than a stalled partition) until ctx is cancelled.
func (c *Consumer) deadLetter(ctx context.Context, log zerolog.Logger, rec *kgo.Record, cause error, attempts int) bool {
	dlq := &kgo.Record{
		Topic: c.opts.DLQTopic,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: append(append([]kgo.RecordHeader(nil), rec.Headers...),
			kgo.RecordHeader{Key: "dlq.error", Value: []byte(cause.Error())},
			kgo.RecordHeader{Key: "dlq.attempts", Value: []byte(strconv.Itoa(attempts))},
			kgo.RecordHeader{Key: "dlq.consumer", Value: []byte(c.opts.Name)},
			kgo.RecordHeader{Key: "dlq.original_topic", Value: []byte(rec.Topic)},
			kgo.RecordHeader{Key: "dlq.original_partition", Value: []byte(strconv.Itoa(int(rec.Partition)))},
			kgo.RecordHeader{Key: "dlq.original_offset", Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		),
	}
	for n := 1; ; n++ {
		err := c.cl.ProduceSync(ctx, dlq).FirstErr()
		if err == nil {
			c.results.WithLabelValues(rec.Topic, "dead_lettered").Inc()
			log.Error().Err(cause).Int("attempts", attempts).Str("dlq_topic", c.opts.DLQTopic).Msg("message dead-lettered")
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		log.Error().Err(err).Msg(fmt.Sprintf("publishing to %s failed, retrying", c.opts.DLQTopic))
		select {
		case <-ctx.Done():
			return false
		case <-time.After(c.opts.Backoff(n)):
		}
	}
}

// stopping reports whether ctx is done. Cancellation is the normal way to
// stop the consumer, not an error.
func stopping(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func toMessage(rec *kgo.Record) Message {
	h := make(map[string]string, len(rec.Headers))
	for _, kv := range rec.Headers {
		h[kv.Key] = string(kv.Value)
	}
	return Message{
		Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset,
		Key: rec.Key, Value: rec.Value, Headers: h, Timestamp: rec.Timestamp,
	}
}
