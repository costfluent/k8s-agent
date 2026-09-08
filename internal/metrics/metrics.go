package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Scrape metrics
	ScrapeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "costfluent_agent_scrape_duration_seconds",
		Help:    "Duration of metrics scrape operations",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	})

	NodesScraped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "costfluent_agent_nodes_scraped_total",
		Help: "Total number of nodes scraped",
	})

	PodsScraped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "costfluent_agent_pods_scraped_total",
		Help: "Total number of pods scraped",
	})

	SamplesCollected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "costfluent_agent_samples_collected_total",
		Help: "Total number of metric samples collected",
	})

	// Report metrics
	ReportSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "costfluent_agent_report_success_total",
		Help: "Total number of successful report submissions",
	})

	ReportFailure = promauto.NewCounter(prometheus.CounterOpts{
		Name: "costfluent_agent_report_failure_total",
		Help: "Total number of failed report submissions",
	})

	ReportSizeBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "costfluent_agent_report_size_bytes",
		Help:    "Size of compressed report payloads in bytes",
		Buckets: []float64{1024, 10240, 102400, 512000, 1048576, 5242880},
	})

	ReportDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "costfluent_agent_report_duration_seconds",
		Help:    "Duration of report submission operations",
		Buckets: []float64{0.5, 1, 2.5, 5, 10, 30},
	})

	// Buffer metrics
	BufferSizeBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "costfluent_agent_buffer_size_bytes",
		Help: "Current size of buffered reports in bytes",
	})

	BufferReportCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "costfluent_agent_buffer_report_count",
		Help: "Current number of buffered reports",
	})

	// Heartbeat metrics
	HeartbeatSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "costfluent_agent_heartbeat_success_total",
		Help: "Total number of successful heartbeats",
	})

	HeartbeatFailure = promauto.NewCounter(prometheus.CounterOpts{
		Name: "costfluent_agent_heartbeat_failure_total",
		Help: "Total number of failed heartbeats",
	})

	// Info metric
	AgentInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "costfluent_agent_info",
		Help: "Agent version and cluster information",
	}, []string{"version", "cluster_id", "cluster_name"})
)

// RecordScrape records metrics for a scrape operation
func RecordScrape(duration float64, nodes, pods, samples int) {
	ScrapeDuration.Observe(duration)
	NodesScraped.Add(float64(nodes))
	PodsScraped.Add(float64(pods))
	SamplesCollected.Add(float64(samples))
}

// RecordReport records metrics for a report submission
func RecordReport(success bool, duration float64, sizeBytes int) {
	if success {
		ReportSuccess.Inc()
	} else {
		ReportFailure.Inc()
	}
	ReportDuration.Observe(duration)
	ReportSizeBytes.Observe(float64(sizeBytes))
}

// UpdateBuffer updates buffer metrics
func UpdateBuffer(count int, sizeBytes int64) {
	BufferReportCount.Set(float64(count))
	BufferSizeBytes.Set(float64(sizeBytes))
}

// RecordHeartbeat records heartbeat result
func RecordHeartbeat(success bool) {
	if success {
		HeartbeatSuccess.Inc()
	} else {
		HeartbeatFailure.Inc()
	}
}

// SetAgentInfo sets the agent info metric
func SetAgentInfo(version, clusterID, clusterName string) {
	AgentInfo.WithLabelValues(version, clusterID, clusterName).Set(1)
}
