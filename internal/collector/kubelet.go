package collector

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/config"
	"github.com/costfluent/k8s-agent/internal/metrics"
)

const (
	defaultKubeletPort = 10250
	// A cluster gets one scrape at a time per nodesPerScrape nodes, at least minScrapes and at most
	// the configured ceiling, so a large cluster's poll stays short without a burst on small ones.
	minScrapes      = 10
	nodesPerScrape  = 20
	scrapeTimeout   = 5 * time.Second
	maxMetricsBytes = 32 << 20

	metricCPU    = "container_cpu_usage_seconds_total"
	metricMemory = "container_memory_working_set_bytes"
)

// ContainerKey names a container cluster-wide.
type ContainerKey struct {
	Namespace, Pod, Container string
}

// ContainerUsage is one poll's measurement. HasCPU is false until a container has two counter
// samples to take a rate from.
type ContainerUsage struct {
	CPUCores    float64
	HasCPU      bool
	MemoryBytes int64
}

type counterSample struct {
	value   float64
	at      time.Time
	rate    float64
	hasRate bool
}

// KubeletCollector scrapes each node's kubelet /metrics/resource endpoint.
type KubeletCollector struct {
	client       *http.Client
	addressTypes []corev1.NodeAddressType
	logger       *zap.Logger
	timeout      time.Duration
	maxBody      int64
	maxScrapes   int

	mu   sync.Mutex
	prev map[string]map[ContainerKey]counterSample
}

// NewKubeletCollector uses client, which must carry the bearer token the kubelets accept and the
// TLS settings for them; a failure to get that token is a *TokenError.
func NewKubeletCollector(client *http.Client, addressTypes []corev1.NodeAddressType, maxConcurrency int, logger *zap.Logger) *KubeletCollector {
	return &KubeletCollector{
		client:       client,
		addressTypes: addressTypes,
		logger:       logger.Named("kubelet"),
		timeout:      scrapeTimeout,
		maxBody:      maxMetricsBytes,
		maxScrapes:   maxConcurrency,
		prev:         map[string]map[ContainerKey]counterSample{},
	}
}

// Scrape reads every node, a few at a time, and returns the usage and, per node that failed, the
// v1.Unscraped* reason. A node that fails is counted and left out; it never aborts the poll.
func (c *KubeletCollector) Scrape(ctx context.Context, nodes []*corev1.Node) (map[ContainerKey]ContainerUsage, map[string]string) {
	type result struct {
		node    string
		samples map[ContainerKey]rawSample
		failed  string
	}
	results := make(chan result, len(nodes))
	sem := make(chan struct{}, c.concurrency(len(nodes)))
	var wg sync.WaitGroup
	for _, node := range nodes {
		sem <- struct{}{}
		wg.Add(1)
		go func(node *corev1.Node) {
			defer wg.Done()
			defer func() { <-sem }()
			samples, err := c.scrapeNode(ctx, node)
			if err != nil {
				metrics.NodeScrapes.WithLabelValues("error").Inc()
				reason := unscrapedReason(err)
				c.logger.Warn("kubelet scrape failed", zap.String("node", node.Name), zap.String("reason", reason), zap.Error(err))
				results <- result{node: node.Name, failed: reason}
				return
			}
			metrics.NodeScrapes.WithLabelValues("ok").Inc()
			results <- result{node: node.Name, samples: samples}
		}(node)
	}
	wg.Wait()
	close(results)

	c.mu.Lock()
	defer c.mu.Unlock()
	live := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		live[n.Name] = true
	}
	for name := range c.prev {
		if !live[name] {
			delete(c.prev, name)
		}
	}

	usage := map[ContainerKey]ContainerUsage{}
	var failed map[string]string
	for r := range results {
		if r.failed != "" {
			if failed == nil {
				failed = map[string]string{}
			}
			failed[r.node] = r.failed
			continue
		}
		prev := c.prev[r.node]
		next := make(map[ContainerKey]counterSample, len(r.samples))
		for key, s := range r.samples {
			u := ContainerUsage{MemoryBytes: s.memory}
			if s.hasCPU {
				cur := counterSample{value: s.cpu, at: s.cpuAt}
				if p, ok := prev[key]; ok {
					if rate, ok := cpuRate(p, cur); ok {
						cur.rate, cur.hasRate = rate, true
					} else if !cur.at.After(p.at) {
						// cAdvisor has not refreshed the sample since the last poll.
						cur = p
					}
				}
				u.CPUCores, u.HasCPU = cur.rate, cur.hasRate
				next[key] = cur
			}
			usage[key] = u
		}
		c.prev[r.node] = next
	}
	return usage, failed
}

func (c *KubeletCollector) concurrency(nodes int) int {
	ceiling := c.maxScrapes
	if ceiling <= 0 {
		ceiling = config.DefaultScrapeConcurrency
	}
	return max(1, min(ceiling, max(minScrapes, (nodes+nodesPerScrape-1)/nodesPerScrape)))
}

// cpuRate is the core rate between two counter samples. A counter that went down restarted from
// zero, so its whole current value is the increase.
func cpuRate(prev, cur counterSample) (float64, bool) {
	dt := cur.at.Sub(prev.at).Seconds()
	if dt <= 0 {
		return 0, false
	}
	increase := cur.value - prev.value
	if increase < 0 {
		increase = cur.value
	}
	return increase / dt, true
}

type rawSample struct {
	cpu    float64
	cpuAt  time.Time
	hasCPU bool
	memory int64
}

func (c *KubeletCollector) scrapeNode(ctx context.Context, node *corev1.Node) (map[ContainerKey]rawSample, error) {
	endpoint, err := c.endpoint(node)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("building request for %s: %w", endpoint, err)
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")
	resp, err := c.client.Do(req)
	var certErr *tls.CertificateVerificationError
	if errors.As(err, &certErr) {
		return nil, fmt.Errorf("GET %s: %w (a kubelet whose certificate the cluster CA does not verify, such as the self-signed ones on kind and Talos, needs agent.disableKubeTLSverify=true in the chart, or COSTFLUENT_KUBE_SKIP_TLS_VERIFY=true)", endpoint, err)
	}
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &statusError{endpoint: endpoint, code: resp.StatusCode}
	}
	body := &cappedReader{r: resp.Body, left: c.maxBody}
	samples, err := parseResourceMetrics(body, time.Now())
	if body.truncated {
		// Kubelet writes each metric family whole, so the containers past the cut, and for an early
		// cut every container's memory, have no sample, and a container without one is credited zero
		// usage. A partial body is refused even when it parses, so the node is reported as unscraped.
		metrics.KubeletBodiesTruncated.Inc()
		return nil, fmt.Errorf("kubelet metrics exceed the %d byte limit; the node is left out of this poll", c.maxBody)
	}
	return samples, err
}

// cappedReader ends a body after left bytes and records whether more followed.
type cappedReader struct {
	r         io.Reader
	left      int64
	truncated bool
}

func (c *cappedReader) Read(p []byte) (int, error) {
	if c.left <= 0 {
		var probe [1]byte
		if n, _ := c.r.Read(probe[:]); n > 0 {
			c.truncated = true
		}
		return 0, io.EOF
	}
	if int64(len(p)) > c.left {
		p = p[:c.left]
	}
	n, err := c.r.Read(p)
	c.left -= int64(n)
	return n, err
}

func (c *KubeletCollector) endpoint(node *corev1.Node) (string, error) {
	port := int(node.Status.DaemonEndpoints.KubeletEndpoint.Port)
	if port == 0 {
		port = defaultKubeletPort
	}
	for _, t := range c.addressTypes {
		for _, a := range node.Status.Addresses {
			if a.Type == t && a.Address != "" {
				return "https://" + net.JoinHostPort(a.Address, strconv.Itoa(port)) + "/metrics/resource", nil
			}
		}
	}
	return "", &noAddressError{node: node.Name, types: c.addressTypes}
}

func parseResourceMetrics(r io.Reader, scrapedAt time.Time) (map[ContainerKey]rawSample, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return nil, fmt.Errorf("parsing /metrics/resource: %w", err)
	}
	out := map[ContainerKey]rawSample{}
	if f, ok := families[metricCPU]; ok {
		for _, m := range f.GetMetric() {
			key, ok := containerKey(m)
			if !ok || m.GetCounter() == nil {
				continue
			}
			s := out[key]
			s.cpu, s.hasCPU = m.GetCounter().GetValue(), true
			s.cpuAt = scrapedAt
			if ms := m.GetTimestampMs(); ms > 0 {
				s.cpuAt = time.UnixMilli(ms)
			}
			out[key] = s
		}
	}
	if f, ok := families[metricMemory]; ok {
		for _, m := range f.GetMetric() {
			key, ok := containerKey(m)
			if !ok || m.GetGauge() == nil {
				continue
			}
			s := out[key]
			s.memory = int64(m.GetGauge().GetValue())
			out[key] = s
		}
	}
	return out, nil
}

func containerKey(m *dto.Metric) (ContainerKey, bool) {
	var k ContainerKey
	for _, l := range m.GetLabel() {
		switch l.GetName() {
		case "namespace":
			k.Namespace = l.GetValue()
		case "pod":
			k.Pod = l.GetValue()
		case "container":
			k.Container = l.GetValue()
		}
	}
	return k, k.Namespace != "" && k.Pod != "" && k.Container != ""
}

// statusError is a kubelet's answer other than 200.
type statusError struct {
	endpoint string
	code     int
}

func (e *statusError) Error() string {
	if e.code == http.StatusUnauthorized || e.code == http.StatusForbidden {
		return fmt.Sprintf("GET %s: status %d (the kubelet refused the token: the ClusterRole of the account the agent scrapes with needs nodes/metrics get)", e.endpoint, e.code)
	}
	return fmt.Sprintf("GET %s: status %d", e.endpoint, e.code)
}

// noAddressError is a node without an address of the types the agent tries.
type noAddressError struct {
	node  string
	types []corev1.NodeAddressType
}

func (e *noAddressError) Error() string {
	return fmt.Sprintf("node %s has no address of types %v", e.node, e.types)
}

// TokenError is a failure to get the token a kubelet request carries, before any kubelet was
// contacted. The transport that adds the token returns it.
type TokenError struct{ Err error }

func (e *TokenError) Error() string { return "requesting the kubelet token: " + e.Err.Error() }

func (e *TokenError) Unwrap() error { return e.Err }

// unscrapedReason is the v1.Unscraped* reason a scrape failed for. net.Error is never matched:
// *url.Error, which wraps every client.Do failure, implements it, TLS failures included.
func unscrapedReason(err error) string {
	var (
		tokenErr  *TokenError
		certErr   *tls.CertificateVerificationError
		status    *statusError
		noAddress *noAddressError
		opErr     *net.OpError
		dnsErr    *net.DNSError
	)
	switch {
	case errors.As(err, &tokenErr):
		// The API server refusing the TokenRequest is the same fix as a kubelet refusing the token:
		// the chart's RBAC for the metrics account. Any other failure to get one is not the kubelet's.
		if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
			return v1.UnscrapedUnauthorized
		}
		return v1.UnscrapedOther
	case errors.As(err, &certErr):
		return v1.UnscrapedTLS
	case errors.As(err, &status):
		if status.code == http.StatusUnauthorized || status.code == http.StatusForbidden {
			return v1.UnscrapedUnauthorized
		}
		return v1.UnscrapedOther
	case errors.As(err, &noAddress), errors.As(err, &opErr), errors.As(err, &dnsErr), errors.Is(err, context.DeadlineExceeded):
		return v1.UnscrapedUnreachable
	default:
		return v1.UnscrapedOther
	}
}
