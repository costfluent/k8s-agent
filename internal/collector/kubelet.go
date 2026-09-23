package collector

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"

	"github.com/costfluent/k8s-agent/internal/metrics"
)

const (
	defaultKubeletPort = 10250
	scrapeConcurrency  = 10
	scrapeTimeout      = 5 * time.Second
	maxMetricsBytes    = 32 << 20

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

	mu   sync.Mutex
	prev map[string]map[ContainerKey]counterSample
}

// NewKubeletCollector uses client, which must carry the service-account bearer token and the TLS
// settings for the kubelets.
func NewKubeletCollector(client *http.Client, addressTypes []corev1.NodeAddressType, logger *zap.Logger) *KubeletCollector {
	return &KubeletCollector{
		client:       client,
		addressTypes: addressTypes,
		logger:       logger.Named("kubelet"),
		timeout:      scrapeTimeout,
		prev:         map[string]map[ContainerKey]counterSample{},
	}
}

// Scrape reads every node, scrapeConcurrency at a time. A node that fails is counted and left out;
// it never aborts the poll.
func (c *KubeletCollector) Scrape(ctx context.Context, nodes []*corev1.Node) map[ContainerKey]ContainerUsage {
	type result struct {
		node    string
		samples map[ContainerKey]rawSample
	}
	results := make(chan result, len(nodes))
	sem := make(chan struct{}, scrapeConcurrency)
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(node *corev1.Node) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			samples, err := c.scrapeNode(ctx, node)
			if err != nil {
				metrics.NodeScrapes.WithLabelValues("error").Inc()
				c.logger.Warn("kubelet scrape failed", zap.String("node", node.Name), zap.Error(err))
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
	for r := range results {
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
	return usage
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
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d (the agent's ClusterRole needs nodes/metrics get)", endpoint, resp.StatusCode)
	}
	return parseResourceMetrics(io.LimitReader(resp.Body, maxMetricsBytes), time.Now())
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
	var parser expfmt.TextParser
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
