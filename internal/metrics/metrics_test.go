package metrics_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/costfluent/k8s-agent/internal/metrics"
)

func TestMetrics_AreRegisteredOnTheDefaultRegistry(t *testing.T) {
	collectors := map[string]prometheus.Collector{
		"costfluent_agent_node_scrape_total":            metrics.NodeScrapes,
		"costfluent_agent_poll_duration_seconds":        metrics.PollDuration,
		"costfluent_agent_reports_total":                metrics.Reports,
		"costfluent_agent_report_bytes":                 metrics.ReportBytes,
		"costfluent_agent_buffer_reports":               metrics.BufferReports,
		"costfluent_agent_buffer_bytes":                 metrics.BufferBytes,
		"costfluent_agent_buffer_dropped_total":         metrics.BufferDropped,
		"costfluent_agent_map_entries_dropped_total":    metrics.MapEntriesDropped,
		"costfluent_agent_over_supported_size":          metrics.OverSupportedSize,
		"costfluent_agent_elapsed_seconds_total":        metrics.ElapsedSeconds,
		"costfluent_agent_credited_seconds_total":       metrics.CreditedSeconds,
		"costfluent_agent_kubelet_body_truncated_total": metrics.KubeletBodiesTruncated,
		"costfluent_agent_info":                         metrics.Info,
	}
	for name, c := range collectors {
		var already prometheus.AlreadyRegisteredError
		if err := prometheus.DefaultRegisterer.Register(c); !errors.As(err, &already) {
			t.Errorf("%s: Register = %v, want it already on the default registry that /metrics serves", name, err)
		}
		descs := make(chan *prometheus.Desc, 1)
		c.Describe(descs)
		if d := (<-descs).String(); !strings.Contains(d, `fqName: "`+name+`"`) {
			t.Errorf("descriptor = %s, want %s", d, name)
		}
	}
}

func TestMetrics_PassPrometheusLint(t *testing.T) {
	metrics.NodeScrapes.WithLabelValues("ok")
	metrics.Reports.WithLabelValues("accepted")
	metrics.BufferDropped.WithLabelValues("age")
	metrics.MapEntriesDropped.WithLabelValues("node_labels")
	metrics.Info.WithLabelValues("test", "lint-cluster")
	problems, err := testutil.GatherAndLint(prometheus.DefaultGatherer,
		"costfluent_agent_node_scrape_total", "costfluent_agent_poll_duration_seconds",
		"costfluent_agent_reports_total", "costfluent_agent_report_bytes",
		"costfluent_agent_buffer_reports", "costfluent_agent_buffer_bytes",
		"costfluent_agent_buffer_dropped_total", "costfluent_agent_map_entries_dropped_total",
		"costfluent_agent_over_supported_size", "costfluent_agent_elapsed_seconds_total",
		"costfluent_agent_credited_seconds_total", "costfluent_agent_kubelet_body_truncated_total", "costfluent_agent_info")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Errorf("%s: %s", p.Metric, p.Text)
	}
}

func TestMetrics_CountersAndGaugesKeepTheirLabels(t *testing.T) {
	for _, tc := range []struct {
		vec   *prometheus.CounterVec
		label string
	}{
		{metrics.NodeScrapes, "error"},
		{metrics.Reports, "retry"},
		{metrics.BufferDropped, "size"},
	} {
		before := testutil.ToFloat64(tc.vec.WithLabelValues(tc.label))
		tc.vec.WithLabelValues(tc.label).Inc()
		tc.vec.WithLabelValues(tc.label).Add(2)
		if got := testutil.ToFloat64(tc.vec.WithLabelValues(tc.label)) - before; got != 3 {
			t.Errorf("%s increase = %v, want 3", tc.label, got)
		}
	}

	metrics.BufferReports.Set(4)
	metrics.BufferBytes.Set(2048)
	if testutil.ToFloat64(metrics.BufferReports) != 4 || testutil.ToFloat64(metrics.BufferBytes) != 2048 {
		t.Error("buffer gauges do not hold the value set")
	}

	metrics.Info.WithLabelValues("1.2.3", "info-cluster").Set(1)
	if testutil.ToFloat64(metrics.Info.WithLabelValues("1.2.3", "info-cluster")) != 1 {
		t.Error("info gauge for version and cluster is not 1")
	}
}

func TestMetrics_HistogramBuckets(t *testing.T) {
	poll := histogram(t, "costfluent_agent_poll_duration_seconds", func() { metrics.PollDuration.Observe(0.3) })
	wantPoll := []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}
	if len(poll) != len(wantPoll) {
		t.Fatalf("poll buckets = %v, want %v", poll, wantPoll)
	}
	for i, b := range wantPoll {
		if poll[i] != b {
			t.Fatalf("poll buckets = %v, want %v", poll, wantPoll)
		}
	}

	report := histogram(t, "costfluent_agent_report_bytes", func() { metrics.ReportBytes.Observe(5000) })
	if len(report) != 8 || report[0] != 1024 || report[7] != 1024*16384 {
		t.Fatalf("report byte buckets = %v, want 8 from 1 KiB growing by 4", report)
	}
}

// histogram observes once and returns the bucket bounds, checking the observation landed.
func histogram(t *testing.T, name string, observe func()) []float64 {
	t.Helper()
	count := func() uint64 {
		families, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range families {
			if f.GetName() == name {
				return f.GetMetric()[0].GetHistogram().GetSampleCount()
			}
		}
		return 0
	}
	before := count()
	observe()
	if count()-before != 1 {
		t.Fatalf("%s: observation was not counted", name)
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		var bounds []float64
		for _, b := range f.GetMetric()[0].GetHistogram().GetBucket() {
			bounds = append(bounds, b.GetUpperBound())
		}
		return bounds
	}
	t.Fatalf("%s not gathered", name)
	return nil
}
