package redisx

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// PoolCollector exports go-redis connection pool statistics as Prometheus
// metrics (redis_pool_*).
type PoolCollector struct {
	rdb *redis.Client

	hits       *prometheus.Desc
	misses     *prometheus.Desc
	timeouts   *prometheus.Desc
	waits      *prometheus.Desc
	waitTime   *prometheus.Desc
	totalConns *prometheus.Desc
	idleConns  *prometheus.Desc
	staleConns *prometheus.Desc
	pending    *prometheus.Desc
	maxConns   *prometheus.Desc
}

var _ prometheus.Collector = (*PoolCollector)(nil)

// NewPoolCollector creates a collector for rdb.
func NewPoolCollector(rdb *redis.Client) *PoolCollector {
	d := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("redis_pool_"+name, help, nil, nil)
	}
	return &PoolCollector{
		rdb:        rdb,
		hits:       d("hits_total", "Cumulative count of times a free connection was found in the pool."),
		misses:     d("misses_total", "Cumulative count of times a free connection was not found in the pool."),
		timeouts:   d("timeouts_total", "Cumulative count of pool wait timeouts (REDIS_POOL_TIMEOUT exceeded)."),
		waits:      d("wait_total", "Cumulative count of times a caller waited for a connection."),
		waitTime:   d("wait_duration_seconds_total", "Total time spent waiting for a connection."),
		totalConns: d("total_conns", "Number of connections currently in the pool."),
		idleConns:  d("idle_conns", "Number of idle connections in the pool."),
		staleConns: d("stale_conns_total", "Cumulative count of stale connections removed from the pool."),
		pending:    d("pending_requests", "Number of requests currently waiting for a connection."),
		maxConns:   d("max_conns", "Maximum size of the pool."),
	}
}

// Describe implements prometheus.Collector.
func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

// Collect implements prometheus.Collector.
func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.rdb.PoolStats()
	counter := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v)
	}
	gauge := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v)
	}
	counter(c.hits, float64(s.Hits))
	counter(c.misses, float64(s.Misses))
	counter(c.timeouts, float64(s.Timeouts))
	counter(c.waits, float64(s.WaitCount))
	counter(c.waitTime, float64(s.WaitDurationNs)/1e9)
	gauge(c.totalConns, float64(s.TotalConns))
	gauge(c.idleConns, float64(s.IdleConns))
	counter(c.staleConns, float64(s.StaleConns))
	gauge(c.pending, float64(s.PendingRequests))
	gauge(c.maxConns, float64(c.rdb.Options().PoolSize))
}
