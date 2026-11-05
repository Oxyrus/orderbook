package kafkax_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Oxyrus/orderbook/internal/platform/kafkax"
)

const (
	topic = "orderbook.events.order"
	dlq   = "orderbook.events.order.dlq"
	group = "test-group"
)

func newCluster(t *testing.T) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic, dlq))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func produce(t *testing.T, brokers []string, recs ...*kgo.Record) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer cl.Close()
	require.NoError(t, cl.ProduceSync(context.Background(), recs...).FirstErr())
}

func record(key, id string) *kgo.Record {
	return &kgo.Record{Topic: topic, Key: []byte(key), Value: []byte(`{}`),
		Headers: []kgo.RecordHeader{{Key: "id", Value: []byte(id)}}}
}

// runConsumer consumes until stop returns true for the handled ids so far.
func runConsumer(t *testing.T, brokers []string, reg prometheus.Registerer, h kafkax.Handler, done func() bool) {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, kafkax.ConsumerClientOpts(group, topic)...)...)
	require.NoError(t, err)
	defer cl.Close()

	c := kafkax.NewConsumer(cl, kafkax.ConsumerOptions{
		Name: "test", Handler: h, DLQTopic: dlq, MaxAttempts: 3,
		Logger: zerolog.Nop(), Registerer: reg, Backoff: func(int) time.Duration { return time.Millisecond },
	})
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()
	require.Eventually(t, done, 10*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-errc)
}

func TestConsumerRetriesDeadLettersAndCommits(t *testing.T) {
	brokers := newCluster(t)
	produce(t, brokers, record("a", "ok-1"), record("b", "flaky"), record("c", "poison"), record("d", "broken"))

	var mu sync.Mutex
	calls := map[string]int{}
	handler := func(_ context.Context, m kafkax.Message) error {
		mu.Lock()
		defer mu.Unlock()
		id := m.Header("id")
		calls[id]++
		switch {
		case id == "flaky" && calls[id] < 3:
			return errors.New("transient")
		case id == "poison":
			return kafkax.Permanent(errors.New("cannot decode"))
		case id == "broken":
			return errors.New("always fails")
		}
		return nil
	}
	reg := prometheus.NewRegistry()
	runConsumer(t, brokers, reg, handler, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls["ok-1"] == 1 && calls["flaky"] == 3 && calls["poison"] == 1 && calls["broken"] == 3
	})

	mu.Lock()
	assert.Equal(t, 1, calls["poison"], "permanent errors are not retried")
	assert.Equal(t, 3, calls["broken"], "retried MaxAttempts times")
	mu.Unlock()

	// Both failures went to the DLQ with diagnostics.
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(dlq), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	got := map[string]map[string]string{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for len(got) < 2 && ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			h := map[string]string{}
			for _, kv := range r.Headers {
				h[kv.Key] = string(kv.Value)
			}
			got[h["id"]] = h
		})
	}
	require.Contains(t, got, "poison")
	require.Contains(t, got, "broken")
	assert.Equal(t, "cannot decode", got["poison"]["dlq.error"])
	assert.Equal(t, "3", got["broken"]["dlq.attempts"])
	assert.Equal(t, topic, got["broken"]["dlq.original_topic"])

	assert.InDelta(t, 2, counter(t, reg, "dead_lettered"), 0)
	assert.InDelta(t, 2, counter(t, reg, "processed"), 0)

	// Offsets were committed: a new member of the group only sees new data.
	produce(t, brokers, record("e", "ok-2"))
	var seen []string
	runConsumer(t, brokers, nil, func(_ context.Context, m kafkax.Message) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, m.Header("id"))
		return nil
	}, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) > 0
	})
	assert.Equal(t, []string{"ok-2"}, seen)
}

func counter(t *testing.T, reg *prometheus.Registry, outcome string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "kafka_consumer_messages_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "outcome" && l.GetValue() == outcome {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func TestPermanent(t *testing.T) {
	base := errors.New("x")
	assert.True(t, kafkax.IsPermanent(kafkax.Permanent(base)))
	assert.ErrorIs(t, kafkax.Permanent(base), base)
	assert.False(t, kafkax.IsPermanent(base))
}
