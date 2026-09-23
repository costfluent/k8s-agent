// Package metrics holds the agent's own Prometheus metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	NodeScrapes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "costfluent_agent_node_scrape_total",
		Help: "Kubelet /metrics/resource scrapes by result (ok, error).",
	}, []string{"result"})

	PollDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "costfluent_agent_poll_duration_seconds",
		Help:    "Duration of one poll across every node.",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	})

	Reports = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "costfluent_agent_reports_total",
		Help: "Report submissions by result (accepted, rejected, unauthorized, retry).",
	}, []string{"result"})

	ReportBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "costfluent_agent_report_bytes",
		Help:    "Size of gzip report bodies.",
		Buckets: prometheus.ExponentialBuckets(1024, 4, 8),
	})

	BufferReports = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "costfluent_agent_buffer_reports",
		Help: "Reports waiting in the buffer.",
	})

	BufferBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "costfluent_agent_buffer_bytes",
		Help: "Bytes of reports waiting in the buffer.",
	})

	BufferDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "costfluent_agent_buffer_dropped_total",
		Help: "Reports dropped from the buffer by reason (age, size, rejected).",
	}, []string{"reason"})

	Info = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "costfluent_agent_info",
		Help: "Agent version and cluster.",
	}, []string{"version", "cluster_id"})
)
