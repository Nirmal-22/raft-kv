// Package metrics holds the Prometheus instruments for one node. Each node
// gets its own registry so several nodes can run in one process (the tests do
// this) without their metrics colliding.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Metrics struct {
	Registry *prometheus.Registry

	HTTPDuration  *prometheus.HistogramVec
	ApplyDuration prometheus.Histogram
	ReadDuration  *prometheus.HistogramVec
	Forwarded     *prometheus.CounterVec
	LeaderChanges prometheus.Counter
	KeysExpired   prometheus.Counter
	Watchers      prometheus.Gauge
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "raftkv_http_request_duration_seconds",
			Help:    "Time to serve a client HTTP request.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
		}, []string{"method", "route", "status"}),
		ApplyDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "raftkv_fsm_apply_duration_seconds",
			Help:    "Time to apply one committed command to the local store.",
			Buckets: prometheus.ExponentialBuckets(0.00002, 2, 14),
		}),
		ReadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "raftkv_read_duration_seconds",
			Help:    "Time to serve a read on the node that executes it.",
			Buckets: prometheus.ExponentialBuckets(0.00002, 2, 16),
		}, []string{"consistency"}),
		Forwarded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_forwarded_requests_total",
			Help: "Requests this node passed on to the leader.",
		}, []string{"op"}),
		LeaderChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raftkv_leadership_gained_total",
			Help: "Times this node became the leader.",
		}),
		KeysExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raftkv_keys_expired_total",
			Help: "Keys removed by TTL expiry, counted on every node as it applies them.",
		}),
		Watchers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "raftkv_watchers",
			Help: "Open watch streams on this node.",
		}),
	}
	reg.MustRegister(
		m.HTTPDuration, m.ApplyDuration, m.ReadDuration, m.Forwarded,
		m.LeaderChanges, m.KeysExpired, m.Watchers,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Gauge registers a value that is read each time /metrics is scraped.
func (m *Metrics) Gauge(name, help string, fn func() float64) {
	m.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, fn))
}
