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
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
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

	MapEntriesDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "costfluent_agent_map_entries_dropped_total",
		Help: "Label and annotation entries left out of a report to fit the API's caps, counted each time a map is read, by map.",
	}, []string{"map"})

	VolumesDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "costfluent_agent_volumes_dropped_total",
		Help: "Persistent volumes left out of a closed window past the API's volume cap; the first by name are sent.",
	})

	StorageUnreadable = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "costfluent_agent_storage_unreadable",
		Help: "1 while the last list of persistent volumes or storage classes failed, so reports carry no volumes.",
	})

	OverSupportedSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "costfluent_agent_over_supported_size",
		Help: "1 when the last closed window had more nodes, pods or containers per pod than the API accepts, so it was not sent.",
	})

	ElapsedSeconds = promauto.NewCounter(prometheus.CounterOpts{
		Name: "costfluent_agent_elapsed_seconds_total",
		Help: "Wall-clock seconds between successful polls.",
	})

	CreditedSeconds = promauto.NewCounter(prometheus.CounterOpts{
		Name: "costfluent_agent_credited_seconds_total",
		Help: "Seconds of usage credited to windows; below elapsed seconds when polls were missed.",
	})

	KubeletBodiesTruncated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "costfluent_agent_kubelet_body_truncated_total",
		Help: "Kubelet /metrics/resource bodies cut at the agent's size limit.",
	})

	Info = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "costfluent_agent_info",
		Help: "Agent version and cluster.",
	}, []string{"version", "cluster_id"})
)
