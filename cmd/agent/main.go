// Command agent collects Kubernetes usage and reports it to Costfluent.
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

// inClusterConfig reads the pod's service account from fixed paths no test can provide, so a test
// replaces it to reach the startup that follows.
var inClusterConfig = rest.InClusterConfig

// listen binds the health and metrics port. A test replaces it to hand run a listener the test bound
// itself, because a port found free and released can be taken by another listener before run binds it.
var listen = net.Listen

const (
	snapshotEvery   = 5 * time.Minute
	shutdownTimeout = 20 * time.Second

	healthStaleIntervals = 5
	healthStaleFloor     = 10 * time.Minute
	snapshotFile         = "window.json"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		fmt.Fprintf(os.Stderr, "costfluent-k8s-agent: %v\n", err)
		os.Exit(1)
	}
}

// run starts the agent and returns when ctx ends or the agent cannot continue.
func run(ctx context.Context) error {
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
	if limit := applyMemoryLimit(cfg, os.LookupEnv, debug.SetMemoryLimit); limit > 0 {
		logger.Info("keeping memory under a soft limit", zap.Int64("go_memory_limit_bytes", limit), zap.Int64("container_memory_limit_bytes", cfg.MemoryLimitBytes))
	}
	logger.Info("starting costfluent-k8s-agent",
		zap.String("version", Version), zap.String("git_commit", GitCommit), zap.String("build_time", BuildTime),
		zap.String("api_endpoint", cfg.APIEndpoint.String()),
		zap.Duration("polling_interval", cfg.PollingInterval))

	buffer, err := storage.Open(cfg.DataDir, logger)
	if err != nil {
		return err
	}
	instanceID, err := storage.InstanceID(cfg.DataDir)
	if err != nil {
		return err
	}
	kept, err := storage.ClusterID(cfg.DataDir)
	if err != nil {
		return err
	}
	reports := reporter.NewClient(cfg.APIEndpoint, cfg.Token, Version, cfg.ReportHTTPProxy)
	clusterID, fromToken, err := checkToken(ctx, reports, cfg.ClusterID, kept, logger)
	if err != nil {
		return err
	}
	if fromToken && clusterID != kept {
		if err := storage.SaveClusterID(cfg.DataDir, clusterID); err != nil {
			logger.Warn("keeping the token's cluster failed; a start while the API is unreachable then needs agent.clusterID", zap.Error(err))
		}
	}
	cfg.ClusterID = clusterID
	logger.Info("reporting for cluster", zap.String("cluster_id", cfg.ClusterID))

	ctx, stop := context.WithCancel(ctx)
	defer stop()

	var ready atomic.Bool
	var lastSuccess atomic.Int64
	lastSuccess.Store(time.Now().UnixNano())
	server := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(cfg.MetricsPort)),
		Handler:           newMux(&ready, func() error { return pollHealth(&lastSuccess, cfg.PollingInterval, time.Now()) }),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		l, err := listen("tcp", server.Addr)
		if err == nil {
			err = server.Serve(l)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("serving health and metrics on %s: %w", server.Addr, err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	restCfg, err := inClusterConfig()
	if err != nil {
		return fmt.Errorf("in-cluster configuration: %w", err)
	}
	client, err := collector.NewClient(restCfg, cfg)
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
	kubelet := collector.NewKubeletCollector(kubeletClient, cfg.NodeAddressTypes, cfg.ScrapeConcurrency, logger)
	metrics.Info.WithLabelValues(Version, cfg.ClusterID).Set(1)

	a := &agent{
		interval:     cfg.PollingInterval,
		snapshotPath: filepath.Join(cfg.DataDir, snapshotFile),
		aggregator:   collector.NewAggregator(cfg.ClusterID, Version, instanceID, cfg.PollingInterval, time.Now()),
		buffer:       buffer,
		sender:       reporter.NewSender(buffer, reports, logger),
		collect:      collectFunc(metadata, kubelet),
		now:          time.Now,
		ready:        &ready,
		logger:       logger,
		lastSuccess:  &lastSuccess,
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

// clusterTokenType is the token type GET /v1/me names for a cluster token, the only kind the report
// endpoint accepts.
const clusterTokenType = "cluster"

// checkToken reads the agent's token before anything is collected and returns the cluster it
// reports for, the one the cluster token names, and whether the API answered it. It returns an
// error for a token every report would be refused with (the API refuses it, or it is not a cluster
// token) and for a configured cluster id that is not the token's cluster. The pod then exits with
// the reason in its log and restarts once it is fixed; buffered reports stay on the volume. An API
// the agent cannot reach, or one that answers 429 or 5xx, is logged and left to the sender's
// retries when the agent knows its cluster without asking: the configured id, else the one kept on
// the volume from an earlier answer. Readiness is then unchanged; knowing neither, the agent cannot
// know its cluster, and exits to retry.
func checkToken(ctx context.Context, client *reporter.Client, configured, kept string, logger *zap.Logger) (string, bool, error) {
	me, res := client.Me(ctx)
	switch res.Outcome {
	case reporter.Accepted:
	case reporter.Unauthorized:
		return "", false, fmt.Errorf("the Costfluent API refused the token (HTTP %d: %s): %s", res.Status, res.Message, reporter.Advice(res))
	default:
		known := cmp.Or(configured, kept)
		if known == "" {
			return "", false, fmt.Errorf("could not read the cluster from the API token at start (HTTP %d: %s); "+
				"the agent restarts and asks again, or set agent.clusterID to start without asking", res.Status, res.Message)
		}
		logger.Warn("could not check the API token at start; reports are sent and retried as usual",
			zap.Int("status", res.Status), zap.String("message", res.Message), zap.Bool("cluster_id_configured", configured != ""))
		return known, false, nil
	}

	const mint = "in Costfluent, use Add cluster under Kubernetes, Clusters, or Mint token on the cluster, put that token in agent.token (or the secret in agent.secret), and restart the agent; buffered reports are kept and sent then"
	if me.TokenType == clusterTokenType && me.ClusterID == "" {
		return "", false, fmt.Errorf("the cluster the API token %q belongs to was deleted in Costfluent: "+
			"add the cluster again under Kubernetes, Clusters, and install its new token, or uninstall the agent", me.TokenName)
	}
	if me.TokenType != clusterTokenType {
		return "", false, fmt.Errorf("the API token %q is not a cluster token, and only a cluster token can report a cluster: %s", me.TokenName, mint)
	}
	if configured != "" && configured != me.ClusterID {
		return "", false, fmt.Errorf("agent.clusterID %q is not the cluster %q the API token %q belongs to: "+
			"remove agent.clusterID, or use the token of cluster %q", configured, me.ClusterID, me.TokenName, configured)
	}
	return me.ClusterID, true, nil
}

// applyMemoryLimit sets the Go runtime's soft memory limit from the container's, unless GOMEMLIMIT
// already sets one, and returns what it set.
func applyMemoryLimit(cfg *config.Config, lookup func(string) (string, bool), set func(int64) int64) int64 {
	if v, ok := lookup("GOMEMLIMIT"); ok && v != "" {
		return 0
	}
	limit := cfg.GoMemoryLimit()
	if limit > 0 {
		set(limit)
	}
	return limit
}

func newMux(ready *atomic.Bool, healthy func() error) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if err := healthy(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
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
		return collector.Collect(ctx, metadata, kubelet, now)
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
	// lastSuccess is when the last poll succeeded, or the agent started, in Unix nanoseconds.
	lastSuccess *atomic.Int64
}

// healthy fails once no poll has succeeded for longer than a live agent ever takes: five intervals,
// and at least ten minutes, which a first poll of a cluster at the caps fits in.
func (a *agent) healthy() error {
	return pollHealth(a.lastSuccess, a.interval, a.now())
}

func pollHealth(lastSuccess *atomic.Int64, interval time.Duration, now time.Time) error {
	last := time.Unix(0, lastSuccess.Load())
	if since := now.Sub(last); since > max(healthStaleIntervals*interval, healthStaleFloor) {
		return fmt.Errorf("no successful poll since %s, %s ago", last.UTC().Format(time.RFC3339), since.Round(time.Second))
	}
	return nil
}

// run polls until ctx ends, then closes the open window and flushes the buffer on a fresh
// context, because ctx is already cancelled by then.
func (a *agent) run(ctx context.Context) {
	if closed, err := a.aggregator.RestoreSnapshot(a.snapshotPath, a.now()); err != nil {
		a.logger.Warn("discarding the window snapshot", zap.Error(err))
	} else if closed != nil {
		a.restoreClosed(*closed)
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
	began, at := time.Now(), a.now()
	obs, err := a.collect(ctx, at)
	metrics.PollDuration.Observe(time.Since(began).Seconds())
	if err != nil {
		if ctx.Err() == nil {
			a.logger.Warn("poll failed", zap.Error(err))
		}
		return
	}
	a.ready.Store(true)
	a.lastSuccess.Store(at.UnixNano())
	closed := a.aggregator.Observe(obs)
	for _, r := range closed {
		a.put(r)
	}
	if len(closed) > 0 {
		a.replaceSnapshot()
	}
	a.sender.Wake()
}

// replaceSnapshot saves the window that is open now over the snapshot of one that just closed, so a
// restart before the next periodic save never restores the closed window's partial copy, which the
// API would store over the complete report.
func (a *agent) replaceSnapshot() {
	err := a.aggregator.SaveSnapshot(a.snapshotPath)
	if err == nil {
		return
	}
	a.logger.Warn("saving the window snapshot failed; removing the closed window's", zap.Error(err))
	if err := os.Remove(a.snapshotPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		a.logger.Error("removing the closed window's snapshot failed; a restart may resend it partially", zap.Error(err))
	}
}

// restoreClosed buffers a window that closed while the agent was down, unless the buffer already
// holds it: a crash between buffering a closing window and replacing its snapshot leaves the
// partial snapshot behind, which the API would store over the complete report.
func (a *agent) restoreClosed(r v1.Report) {
	held, err := a.buffer.Holds(r.WindowStart)
	switch {
	case err != nil:
		a.logger.Warn("discarding the window snapshot", zap.Error(err))
	case held:
		a.logger.Info("discarding the window snapshot: its window is already buffered", zap.Time("window_start", r.WindowStart))
	default:
		a.put(r)
	}
}

func (a *agent) put(r v1.Report) {
	if err := r.CheckSize(); err != nil {
		metrics.OverSupportedSize.Set(1)
		a.logger.Error("the cluster exceeds the size Costfluent collects; this window is not sent",
			zap.Error(err), zap.Time("window_start", r.WindowStart), zap.Time("window_end", r.WindowEnd))
		return
	}
	metrics.OverSupportedSize.Set(0)
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
