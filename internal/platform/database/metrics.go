package database

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// PoolCollector exports pgxpool statistics as Prometheus metrics.
type PoolCollector struct {
	pool *pgxpool.Pool

	acquireCount         *prometheus.Desc
	acquireDuration      *prometheus.Desc
	acquiredConns        *prometheus.Desc
	canceledAcquireCount *prometheus.Desc
	constructingConns    *prometheus.Desc
	emptyAcquireCount    *prometheus.Desc
	idleConns            *prometheus.Desc
	maxConns             *prometheus.Desc
	totalConns           *prometheus.Desc
	newConnsCount        *prometheus.Desc
	maxLifetimeDestroy   *prometheus.Desc
	maxIdleDestroy       *prometheus.Desc
}

var _ prometheus.Collector = (*PoolCollector)(nil)

// NewPoolCollector creates a collector for pool.
func NewPoolCollector(pool *pgxpool.Pool) *PoolCollector {
	d := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("db_pool_"+name, help, nil, nil)
	}
	return &PoolCollector{
		pool:                 pool,
		acquireCount:         d("acquire_total", "Cumulative count of successful acquires from the pool."),
		acquireDuration:      d("acquire_duration_seconds_total", "Total duration of all successful acquires from the pool."),
		acquiredConns:        d("acquired_conns", "Number of currently acquired connections in the pool."),
		canceledAcquireCount: d("canceled_acquire_total", "Cumulative count of acquires cancelled by a context."),
		constructingConns:    d("constructing_conns", "Number of connections being constructed."),
		emptyAcquireCount:    d("empty_acquire_total", "Cumulative count of acquires that waited because the pool was empty."),
		idleConns:            d("idle_conns", "Number of currently idle connections in the pool."),
		maxConns:             d("max_conns", "Maximum size of the pool."),
		totalConns:           d("total_conns", "Total number of connections currently in the pool."),
		newConnsCount:        d("new_conns_total", "Cumulative count of new connections opened."),
		maxLifetimeDestroy:   d("max_lifetime_destroy_total", "Cumulative count of connections destroyed because they exceeded MaxConnLifetime."),
		maxIdleDestroy:       d("max_idle_destroy_total", "Cumulative count of connections destroyed because they exceeded MaxConnIdleTime."),
	}
}

// Describe implements prometheus.Collector.
func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

// Collect implements prometheus.Collector.
func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	counter := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v)
	}
	gauge := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v)
	}
	counter(c.acquireCount, float64(s.AcquireCount()))
	counter(c.acquireDuration, s.AcquireDuration().Seconds())
	gauge(c.acquiredConns, float64(s.AcquiredConns()))
	counter(c.canceledAcquireCount, float64(s.CanceledAcquireCount()))
	gauge(c.constructingConns, float64(s.ConstructingConns()))
	counter(c.emptyAcquireCount, float64(s.EmptyAcquireCount()))
	gauge(c.idleConns, float64(s.IdleConns()))
	gauge(c.maxConns, float64(s.MaxConns()))
	gauge(c.totalConns, float64(s.TotalConns()))
	counter(c.newConnsCount, float64(s.NewConnsCount()))
	counter(c.maxLifetimeDestroy, float64(s.MaxLifetimeDestroyCount()))
	counter(c.maxIdleDestroy, float64(s.MaxIdleDestroyCount()))
}
