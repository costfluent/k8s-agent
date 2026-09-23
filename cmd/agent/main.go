// Command agent collects Kubernetes usage and reports it to Costfluent.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/collector"
	"github.com/costfluent/k8s-agent/internal/config"
	"github.com/costfluent/k8s-agent/internal/metrics"
	"github.com/costfluent/k8s-agent/internal/reporter"
	"github.com/costfluent/k8s-agent/internal/storage"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

const (
	snapshotEvery   = 5 * time.Minute
	shutdownTimeout = 20 * time.Second
	snapshotFile    = "window.json"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "costfluent-k8s-agent: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}
	logger, err := newLogger(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}
	defer func() { _ = logger.Sync() }()
	for _, w := range cfg.Warnings {
		logger.Warn(w)
	}
	logger.Info("starting costfluent-k8s-agent",
		zap.String("version", Version), zap.String("git_commit", GitCommit), zap.String("build_time", BuildTime),
		zap.String("cluster_id", cfg.ClusterID), zap.String("api_endpoint", cfg.APIEndpoint.String()),
		zap.Duration("polling_interval", cfg.PollingInterval))

	buffer, err := storage.Open(cfg.DataDir, logger)
	if err != nil {
		return err
	}
	instanceID, err := storage.InstanceID(cfg.DataDir)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var ready atomic.Bool
	server := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(cfg.MetricsPort)),
		Handler:           newMux(&ready),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("serving health and metrics on %s: %w", server.Addr, err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("in-cluster configuration: %w", err)
	}
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}
	kubeletClient, err := kubeletHTTPClient(restCfg, cfg.KubeSkipTLSVerify)
	if err != nil {
		return err
	}

	metadata := collector.NewMetadata(client, cfg, logger)
	if err := metadata.Start(ctx); err != nil {
		return err
	}
	kubelet := collector.NewKubeletCollector(kubeletClient, cfg.NodeAddressTypes, logger)
	metrics.Info.WithLabelValues(Version, cfg.ClusterID).Set(1)

	a := &agent{
		interval:     cfg.PollingInterval,
		snapshotPath: filepath.Join(cfg.DataDir, snapshotFile),
		aggregator:   collector.NewAggregator(cfg.ClusterID, Version, instanceID, cfg.PollingInterval, time.Now()),
		buffer:       buffer,
		sender:       reporter.NewSender(buffer, reporter.NewClient(cfg.APIEndpoint, cfg.Token, Version, cfg.ReportHTTPProxy), logger),
		collect:      collectFunc(metadata, kubelet),
		now:          time.Now,
		ready:        &ready,
		logger:       logger,
	}

	done := make(chan struct{})
	go func() {
		a.run(ctx)
		close(done)
	}()
	select {
	case err := <-serverErr:
		stop()
		<-done
		return err
	case <-done:
		logger.Info("agent stopped")
		return nil
	}
}

func newMux(ready *atomic.Bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "waiting for the first poll", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

// kubeletHTTPClient authenticates with the service-account token, re-read as it rotates, and
// verifies kubelets against the cluster CA unless skipVerify. Many distributions (kind, Talos)
// serve kubelet certificates that are self-signed per node, which only skipVerify accepts.
func kubeletHTTPClient(restCfg *rest.Config, skipVerify bool) (*http.Client, error) {
	cfg := rest.CopyConfig(restCfg)
	if skipVerify {
		cfg.TLSClientConfig = rest.TLSClientConfig{Insecure: true}
	}
	rt, err := rest.TransportFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubelet transport: %w", err)
	}
	return &http.Client{Transport: rt}, nil
}

func collectFunc(metadata *collector.Metadata, kubelet *collector.KubeletCollector) func(context.Context, time.Time) (collector.Observation, error) {
	return func(ctx context.Context, now time.Time) (collector.Observation, error) {
		nodes, err := metadata.Nodes()
		if err != nil {
			return collector.Observation{}, err
		}
		pods, err := metadata.Pods(ctx)
		if err != nil {
			return collector.Observation{}, err
		}
		obs := collector.Observation{Time: now, Pods: pods, Usage: kubelet.Scrape(ctx, nodes)}
		for _, n := range nodes {
			obs.Nodes = append(obs.Nodes, collector.ConvertNode(n))
		}
		return obs, nil
	}
}

type agent struct {
	interval     time.Duration
	snapshotPath string
	aggregator   *collector.Aggregator
	buffer       *storage.Buffer
	sender       *reporter.Sender
	collect      func(context.Context, time.Time) (collector.Observation, error)
	now          func() time.Time
	ready        *atomic.Bool
	logger       *zap.Logger
}

// run polls until ctx ends, then closes the open window and flushes the buffer on a fresh
// context, because ctx is already cancelled by then.
func (a *agent) run(ctx context.Context) {
	if closed, err := a.aggregator.RestoreSnapshot(a.snapshotPath, a.now()); err != nil {
		a.logger.Warn("discarding the window snapshot", zap.Error(err))
	} else if closed != nil {
		a.put(*closed)
	}
	start, end := a.aggregator.Window()
	a.logger.Info("collecting", zap.Time("window_start", start), zap.Time("window_end", end))

	senderCtx, stopSender := context.WithCancel(context.Background())
	var senderDone sync.WaitGroup
	senderDone.Add(1)
	go func() {
		defer senderDone.Done()
		a.sender.Run(senderCtx)
	}()
	a.sender.Wake()

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	lastSnapshot := a.now()
	a.poll(ctx)
	for {
		select {
		case <-ctx.Done():
			stopSender()
			senderDone.Wait()
			a.shutdown()
			return
		case <-ticker.C:
			a.poll(ctx)
			if now := a.now(); now.Sub(lastSnapshot) >= snapshotEvery {
				lastSnapshot = now
				if err := a.aggregator.SaveSnapshot(a.snapshotPath); err != nil {
					a.logger.Warn("saving the window snapshot failed", zap.Error(err))
				}
			}
		}
	}
}

func (a *agent) poll(ctx context.Context) {
	began := time.Now()
	obs, err := a.collect(ctx, a.now())
	metrics.PollDuration.Observe(time.Since(began).Seconds())
	if err != nil {
		if ctx.Err() == nil {
			a.logger.Warn("poll failed", zap.Error(err))
		}
		return
	}
	a.ready.Store(true)
	for _, r := range a.aggregator.Observe(obs) {
		a.put(r)
	}
	a.sender.Wake()
}

func (a *agent) put(r v1.Report) {
	if err := a.buffer.Put(r); err != nil {
		a.logger.Error("buffering a closed window failed; it is lost", zap.Error(err))
		return
	}
	a.logger.Info("window closed", zap.Time("window_start", r.WindowStart), zap.Time("window_end", r.WindowEnd),
		zap.Int("nodes", len(r.Nodes)), zap.Int("pods", len(r.Pods)))
}

func (a *agent) shutdown() {
	a.logger.Info("shutting down: closing the open window and flushing the buffer")
	if r, ok := a.aggregator.Finish(a.now()); ok {
		a.put(r)
	}
	if err := os.Remove(a.snapshotPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		a.logger.Warn("removing the window snapshot failed", zap.Error(err))
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	a.sender.Drain(ctx, true)
	if n, err := a.buffer.Len(); err == nil && n > 0 {
		a.logger.Warn("reports left in the buffer are sent on the next start", zap.Int("buffered_reports", n))
	}
}

func newLogger(level, format string) (*zap.Logger, error) {
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("log level %q: %w", level, err)
	}
	cfg := zap.NewProductionConfig()
	if format == "console" {
		cfg = zap.NewDevelopmentConfig()
	}
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, err := cfg.Build()
	if err != nil {
		return nil, fmt.Errorf("building logger: %w", err)
	}
	return logger, nil
}
