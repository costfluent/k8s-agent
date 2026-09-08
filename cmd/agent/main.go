package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/costfluent/k8s-agent/internal/collector"
	"github.com/costfluent/k8s-agent/internal/config"
	"github.com/costfluent/k8s-agent/internal/health"
	"github.com/costfluent/k8s-agent/internal/metrics"
	"github.com/costfluent/k8s-agent/internal/reporter"
	"github.com/costfluent/k8s-agent/internal/storage"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	logger := setupLogger(cfg.LogLevel, cfg.LogFormat)
	defer logger.Sync()

	logger.Info("starting costfluent-k8s-agent",
		zap.String("version", Version),
		zap.String("build_time", BuildTime),
		zap.String("git_commit", GitCommit),
		zap.String("cluster_id", cfg.ClusterID))

	// Create Kubernetes clients
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Fatal("failed to get in-cluster config", zap.Error(err))
	}

	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		logger.Fatal("failed to create kubernetes client", zap.Error(err))
	}

	metricsClient, err := metricsclient.NewForConfig(k8sConfig)
	if err != nil {
		logger.Fatal("failed to create metrics client", zap.Error(err))
	}

	// Initialize components
	buffer, err := storage.NewBuffer(cfg.DataDir, logger)
	if err != nil {
		logger.Fatal("failed to create buffer", zap.Error(err))
	}

	kubeletCollector := collector.NewKubeletCollector(k8sClient, metricsClient, cfg, logger)
	metadataCollector := collector.NewMetadataCollector(k8sClient, cfg, logger)
	aggregator := collector.NewAggregator(logger)
	httpReporter := reporter.NewHTTPReporter(cfg, Version, logger)
	healthChecker := health.NewChecker(k8sClient, cfg.APIEndpoint)

	// Start metrics server
	if cfg.MetricsEnabled {
		go startMetricsServer(cfg.MetricsPort, healthChecker, logger)
	}

	// Set agent info metric
	metrics.SetAgentInfo(Version, cfg.ClusterID, cfg.ClusterName)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received shutdown signal", zap.String("signal", sig.String()))
		cancel()
	}()

	// Retry any pending reports from buffer
	go retryPendingReports(ctx, buffer, httpReporter, logger)

	// Run the agent
	run(ctx, cfg, kubeletCollector, metadataCollector, aggregator, httpReporter, buffer, healthChecker, logger)

	logger.Info("agent shutdown complete")
}

func run(
	ctx context.Context,
	cfg *config.Config,
	kubeletCollector *collector.KubeletCollector,
	metadataCollector *collector.MetadataCollector,
	aggregator *collector.Aggregator,
	httpReporter *reporter.HTTPReporter,
	buffer *storage.Buffer,
	healthChecker *health.Checker,
	logger *zap.Logger,
) {
	pollingTicker := time.NewTicker(cfg.PollingInterval)
	defer pollingTicker.Stop()

	reportingTicker := time.NewTicker(cfg.ReportingInterval)
	defer reportingTicker.Stop()

	heartbeatTicker := time.NewTicker(cfg.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	// Start first aggregation window
	aggregator.StartWindow(time.Now().UTC())

	logger.Info("agent started",
		zap.Duration("polling_interval", cfg.PollingInterval),
		zap.Duration("reporting_interval", cfg.ReportingInterval),
		zap.Duration("heartbeat_interval", cfg.HeartbeatInterval))

	for {
		select {
		case <-ctx.Done():
			// Send final report before shutdown
			logger.Info("sending final report before shutdown")
			sendReport(ctx, cfg, aggregator, httpReporter, buffer, healthChecker, logger)
			return

		case <-pollingTicker.C:
			collectMetrics(ctx, kubeletCollector, metadataCollector, aggregator, healthChecker, logger)

		case <-reportingTicker.C:
			sendReport(ctx, cfg, aggregator, httpReporter, buffer, healthChecker, logger)
			aggregator.StartWindow(time.Now().UTC())

		case <-heartbeatTicker.C:
			sendHeartbeat(ctx, aggregator, httpReporter, logger)
		}
	}
}

func collectMetrics(
	ctx context.Context,
	kubeletCollector *collector.KubeletCollector,
	metadataCollector *collector.MetadataCollector,
	aggregator *collector.Aggregator,
	healthChecker *health.Checker,
	logger *zap.Logger,
) {
	start := time.Now()

	// Collect node info
	nodes, err := metadataCollector.CollectNodes(ctx)
	if err != nil {
		logger.Error("failed to collect nodes", zap.Error(err))
	} else {
		aggregator.AddNodes(nodes)
	}

	// Collect container metrics
	containerMetrics, err := kubeletCollector.Collect(ctx)
	if err != nil {
		logger.Error("failed to collect metrics", zap.Error(err))
		return
	}

	aggregator.AddMetrics(containerMetrics)

	// Enrich with metadata
	podMetrics := collector.BuildPodMetricsFromRaw(containerMetrics)
	if err := metadataCollector.CollectPodMetadata(ctx, podMetrics); err != nil {
		logger.Warn("failed to collect pod metadata", zap.Error(err))
	}

	// Merge enriched metadata into aggregator
	aggregator.AddPodMetadata(podMetrics)

	nodeCount, podCount, sampleCount := aggregator.GetStats()

	// Record prometheus metrics and health
	metrics.RecordScrape(time.Since(start).Seconds(), nodeCount, podCount, sampleCount)
	healthChecker.RecordScrape()

	logger.Debug("collected metrics",
		zap.Int("nodes", nodeCount),
		zap.Int("pods", podCount),
		zap.Int("samples", sampleCount),
		zap.Duration("duration", time.Since(start)))
}

func sendReport(
	ctx context.Context,
	cfg *config.Config,
	aggregator *collector.Aggregator,
	httpReporter *reporter.HTTPReporter,
	buffer *storage.Buffer,
	healthChecker *health.Checker,
	logger *zap.Logger,
) {
	report := aggregator.CloseWindow(time.Now().UTC(), cfg.ClusterID, cfg.ClusterName, Version)

	if len(report.PodMetrics) == 0 {
		logger.Warn("no metrics to report, skipping")
		return
	}

	// Try to send immediately
	err := httpReporter.SendReport(ctx, report)
	if err != nil {
		logger.Error("failed to send report, buffering", zap.Error(err))

		// Save to buffer for retry
		reportID, bufferErr := buffer.SaveReport(report)
		if bufferErr != nil {
			logger.Error("failed to buffer report", zap.Error(bufferErr))
			return
		}

		logger.Info("report buffered for retry", zap.String("report_id", reportID))
		buffer.UpdateMetrics()
	} else {
		healthChecker.RecordReport()
	}
}

func sendHeartbeat(
	ctx context.Context,
	aggregator *collector.Aggregator,
	httpReporter *reporter.HTTPReporter,
	logger *zap.Logger,
) {
	nodeCount, podCount, _ := aggregator.GetStats()

	err := httpReporter.SendHeartbeat(ctx, nodeCount, podCount)
	metrics.RecordHeartbeat(err == nil)
	if err != nil {
		logger.Warn("failed to send heartbeat", zap.Error(err))
	}
}

func retryPendingReports(
	ctx context.Context,
	buffer *storage.Buffer,
	httpReporter *reporter.HTTPReporter,
	logger *zap.Logger,
) {
	// Wait a bit before retrying
	time.Sleep(10 * time.Second)

	reportIDs, err := buffer.ListPendingReports()
	if err != nil {
		logger.Error("failed to list pending reports", zap.Error(err))
		return
	}

	if len(reportIDs) == 0 {
		return
	}

	logger.Info("retrying pending reports", zap.Int("count", len(reportIDs)))

	for _, reportID := range reportIDs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		report, err := buffer.LoadReport(reportID)
		if err != nil {
			logger.Error("failed to load buffered report",
				zap.String("report_id", reportID),
				zap.Error(err))
			continue
		}

		err = httpReporter.SendReport(ctx, report)
		if err != nil {
			logger.Warn("failed to send buffered report",
				zap.String("report_id", reportID),
				zap.Error(err))
			continue
		}

		// Delete from buffer on success
		if err := buffer.DeleteReport(reportID); err != nil {
			logger.Error("failed to delete buffered report",
				zap.String("report_id", reportID),
				zap.Error(err))
		}

		buffer.UpdateMetrics()
		logger.Info("sent buffered report", zap.String("report_id", reportID))
	}
}

func startMetricsServer(port int, healthChecker *health.Checker, logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", healthChecker.LivenessHandler())
	mux.HandleFunc("/readyz", healthChecker.ReadinessHandler())

	addr := fmt.Sprintf(":%d", port)
	logger.Info("starting metrics server", zap.String("addr", addr))

	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("metrics server error", zap.Error(err))
	}
}

func setupLogger(level, format string) *zap.Logger {
	var lvl zapcore.Level
	switch level {
	case "debug":
		lvl = zapcore.DebugLevel
	case "warn":
		lvl = zapcore.WarnLevel
	case "error":
		lvl = zapcore.ErrorLevel
	default:
		lvl = zapcore.InfoLevel
	}

	var cfg zap.Config
	if format == "json" {
		cfg = zap.NewProductionConfig()
	} else {
		cfg = zap.NewDevelopmentConfig()
	}

	cfg.Level = zap.NewAtomicLevelAt(lvl)
	cfg.DisableStacktrace = true

	logger, _ := cfg.Build()
	return logger
}
