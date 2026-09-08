package reporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/config"
	"github.com/costfluent/k8s-agent/internal/metrics"
)

const (
	userAgent         = "costfluent-k8s-agent"
	reportEndpoint    = "/api/internal/k8s/report"
	heartbeatEndpoint = "/api/internal/k8s/heartbeat"
	maxRetries        = 5
)

// HTTPReporter sends reports to Costfluent API
type HTTPReporter struct {
	client   *http.Client
	cfg      *config.Config
	logger   *zap.Logger
	version  string
	retryDelay time.Duration
}

// NewHTTPReporter creates a new HTTP reporter
func NewHTTPReporter(cfg *config.Config, version string, logger *zap.Logger) *HTTPReporter {
	return &HTTPReporter{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		cfg:        cfg,
		logger:     logger.Named("reporter"),
		version:    version,
		retryDelay: time.Second,
	}
}

// SendReport sends a metrics report to the API
func (r *HTTPReporter) SendReport(ctx context.Context, report *v1.MetricsReport) error {
	start := time.Now()

	// Compress the payload
	payload, err := r.compressPayload(report)
	if err != nil {
		return fmt.Errorf("compressing payload: %w", err)
	}

	r.logger.Info("sending report",
		zap.Int("nodes", len(report.Nodes)),
		zap.Int("pods", len(report.PodMetrics)),
		zap.Int("compressed_bytes", len(payload)),
		zap.Time("report_start", report.ReportStart),
		zap.Time("report_end", report.ReportEnd))

	// Send with retries
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := r.doSendReport(ctx, payload)
		if err == nil {
			r.logger.Info("report sent successfully")
			metrics.RecordReport(true, time.Since(start).Seconds(), len(payload))
			return nil
		}

		lastErr = err
		r.logger.Warn("report send failed",
			zap.Int("attempt", attempt),
			zap.Error(err))

		if attempt < maxRetries {
			delay := r.retryDelay * time.Duration(1<<(attempt-1)) // Exponential backoff
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}

			select {
			case <-ctx.Done():
				metrics.RecordReport(false, time.Since(start).Seconds(), len(payload))
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	metrics.RecordReport(false, time.Since(start).Seconds(), len(payload))
	return fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

func (r *HTTPReporter) doSendReport(ctx context.Context, payload []byte) error {
	url := r.cfg.APIEndpoint + reportEndpoint

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	r.setHeaders(req)
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
		return nil
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized: invalid or expired token")
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("rate limited, retry later")
	}

	if resp.StatusCode >= 500 {
		return fmt.Errorf("server error %d: %s", resp.StatusCode, string(body))
	}

	return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
}

// SendHeartbeat sends a lightweight health check
func (r *HTTPReporter) SendHeartbeat(ctx context.Context, nodeCount, podCount int) error {
	heartbeat := &v1.Heartbeat{
		ClusterID:    r.cfg.ClusterID,
		AgentVersion: r.version,
		NodeCount:    nodeCount,
		PodCount:     podCount,
	}

	payload, err := json.Marshal(heartbeat)
	if err != nil {
		return fmt.Errorf("marshaling heartbeat: %w", err)
	}

	url := r.cfg.APIEndpoint + heartbeatEndpoint

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	r.setHeaders(req)

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending heartbeat: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("heartbeat failed with status %d: %s", resp.StatusCode, string(body))
	}

	r.logger.Debug("heartbeat sent",
		zap.Int("nodes", nodeCount),
		zap.Int("pods", podCount))

	return nil
}

func (r *HTTPReporter) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.cfg.Token)
	req.Header.Set("User-Agent", fmt.Sprintf("%s/%s", userAgent, r.version))
	req.Header.Set("X-Agent-Version", r.version)
	req.Header.Set("X-Cluster-ID", r.cfg.ClusterID)
}

func (r *HTTPReporter) compressPayload(report *v1.MetricsReport) ([]byte, error) {
	data, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("marshaling report: %w", err)
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	if _, err := gz.Write(data); err != nil {
		return nil, fmt.Errorf("gzip write: %w", err)
	}

	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}

	r.logger.Debug("compressed payload",
		zap.Int("original", len(data)),
		zap.Int("compressed", buf.Len()),
		zap.Float64("ratio", float64(buf.Len())/float64(len(data))))

	return buf.Bytes(), nil
}

// HealthCheck verifies connectivity to the API
func (r *HTTPReporter) HealthCheck(ctx context.Context) error {
	url := r.cfg.APIEndpoint + "/health"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed: %d", resp.StatusCode)
	}

	return nil
}
