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

// NewKubeletCollector uses client, which must carry the service-account bearer token and the TLS
// settings for the kubelets.
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

// Scrape reads every node, a few at a time, and returns the usage and the nodes that failed. A node
// that fails is counted and left out; it never aborts the poll.
func (c *KubeletCollector) Scrape(ctx context.Context, nodes []*corev1.Node) (map[ContainerKey]ContainerUsage, map[string]bool) {
	type result struct {
		node    string
		samples map[ContainerKey]rawSample
		failed  bool
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
				c.logger.Warn("kubelet scrape failed", zap.String("node", node.Name), zap.Error(err))
				results <- result{node: node.Name, failed: true}
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
	var failed map[string]bool
	for r := range results {
		if r.failed {
			if failed == nil {
				failed = map[string]bool{}
			}
			failed[r.node] = true
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
		return nil, fmt.Errorf("GET %s: status %d (the agent's ClusterRole needs nodes/metrics get)", endpoint, resp.StatusCode)
	}
	body := &cappedReader{r: resp.Body, left: c.maxBody}
	samples, err := parseResourceMetrics(body, time.Now())
	if body.truncated {
		metrics.KubeletBodiesTruncated.Inc()
		c.logger.Warn("kubelet metrics cut at the size limit; containers past it are missing", zap.String("node", node.Name), zap.Int64("limit_bytes", c.maxBody))
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
	return "", fmt.Errorf("node %s has no address of types %v", node.Name, c.addressTypes)
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
