package collector

import (
	"sync"
	"time"

	"go.uber.org/zap"

	v1 "github.com/costfluent/k8s-agent/api/v1"
)

// Aggregator collects and aggregates metrics over time
type Aggregator struct {
	mu     sync.RWMutex
	logger *zap.Logger

	// Current aggregation window
	windowStart time.Time
	windowEnd   time.Time

	// Aggregated data
	nodes      []v1.Node
	podMetrics map[string]*v1.PodMetrics // key: namespace/podName
}

// NewAggregator creates a new metrics aggregator
func NewAggregator(logger *zap.Logger) *Aggregator {
	return &Aggregator{
		logger:     logger.Named("aggregator"),
		podMetrics: make(map[string]*v1.PodMetrics),
	}
}

// StartWindow begins a new aggregation window
func (a *Aggregator) StartWindow(start time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.windowStart = start
	a.windowEnd = time.Time{}
	a.nodes = nil
	a.podMetrics = make(map[string]*v1.PodMetrics)

	a.logger.Debug("started new aggregation window", zap.Time("start", start))
}

// AddNodes updates the node list
func (a *Aggregator) AddNodes(nodes []v1.Node) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.nodes = nodes
}

// AddMetrics adds a batch of container metrics
func (a *Aggregator) AddMetrics(metrics []ContainerMetrics) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, m := range metrics {
		key := m.Namespace + "/" + m.PodName

		pm, exists := a.podMetrics[key]
		if !exists {
			pm = &v1.PodMetrics{
				Namespace:  m.Namespace,
				PodName:    m.PodName,
				NodeName:   m.NodeName,
				Containers: []v1.Container{},
			}
			a.podMetrics[key] = pm
		}

		// Find or create container
		var container *v1.Container
		for i := range pm.Containers {
			if pm.Containers[i].Name == m.ContainerName {
				container = &pm.Containers[i]
				break
			}
		}
		if container == nil {
			pm.Containers = append(pm.Containers, v1.Container{
				Name:    m.ContainerName,
				Samples: []v1.ContainerSample{},
			})
			container = &pm.Containers[len(pm.Containers)-1]
		}

		// Add sample
		container.Samples = append(container.Samples, v1.ContainerSample{
			Timestamp:        m.Timestamp,
			CpuUsageCores:    m.CpuCores,
			MemoryUsageBytes: m.MemoryBytes,
		})
	}
}

// AddPodMetadata enriches existing pod metrics with metadata
func (a *Aggregator) AddPodMetadata(metadata map[string]*v1.PodMetrics) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for key, md := range metadata {
		if pm, exists := a.podMetrics[key]; exists {
			pm.Labels = md.Labels
			pm.Annotations = md.Annotations
			pm.ControllerName = md.ControllerName
			pm.ControllerKind = md.ControllerKind
			pm.StartTime = md.StartTime
			pm.Phase = md.Phase

			// Merge container resource info
			for i := range pm.Containers {
				for _, mc := range md.Containers {
					if pm.Containers[i].Name == mc.Name {
						pm.Containers[i].CpuRequestCores = mc.CpuRequestCores
						pm.Containers[i].CpuLimitCores = mc.CpuLimitCores
						pm.Containers[i].MemoryRequestBytes = mc.MemoryRequestBytes
						pm.Containers[i].MemoryLimitBytes = mc.MemoryLimitBytes
						break
					}
				}
			}
		}
	}
}

// CloseWindow finalizes the current window and returns the report
func (a *Aggregator) CloseWindow(end time.Time, clusterID, clusterName, agentVersion string) *v1.MetricsReport {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.windowEnd = end

	// Convert map to slice
	podMetrics := make([]v1.PodMetrics, 0, len(a.podMetrics))
	for _, pm := range a.podMetrics {
		podMetrics = append(podMetrics, *pm)
	}

	report := &v1.MetricsReport{
		ClusterID:    clusterID,
		ClusterName:  clusterName,
		ReportStart:  a.windowStart,
		ReportEnd:    a.windowEnd,
		AgentVersion: agentVersion,
		Nodes:        a.nodes,
		PodMetrics:   podMetrics,
	}

	a.logger.Info("closed aggregation window",
		zap.Time("start", a.windowStart),
		zap.Time("end", a.windowEnd),
		zap.Int("nodes", len(a.nodes)),
		zap.Int("pods", len(podMetrics)),
		zap.Int("total_samples", a.countSamples()))

	return report
}

// GetStats returns current aggregation statistics
func (a *Aggregator) GetStats() (nodeCount, podCount, sampleCount int) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return len(a.nodes), len(a.podMetrics), a.countSamples()
}

func (a *Aggregator) countSamples() int {
	count := 0
	for _, pm := range a.podMetrics {
		for _, c := range pm.Containers {
			count += len(c.Samples)
		}
	}
	return count
}

// Reset clears all aggregated data
func (a *Aggregator) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.windowStart = time.Time{}
	a.windowEnd = time.Time{}
	a.nodes = nil
	a.podMetrics = make(map[string]*v1.PodMetrics)
}
