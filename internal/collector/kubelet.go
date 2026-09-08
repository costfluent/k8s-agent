package collector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/config"
)

// KubeletCollector scrapes container metrics from kubelet
type KubeletCollector struct {
	client        kubernetes.Interface
	metricsClient metricsclient.Interface
	cfg           *config.Config
	logger        *zap.Logger
}

// NewKubeletCollector creates a new kubelet metrics collector
func NewKubeletCollector(
	client kubernetes.Interface,
	metricsClient metricsclient.Interface,
	cfg *config.Config,
	logger *zap.Logger,
) *KubeletCollector {
	return &KubeletCollector{
		client:        client,
		metricsClient: metricsClient,
		cfg:           cfg,
		logger:        logger.Named("kubelet"),
	}
}

// ContainerMetrics holds raw metrics for a container
type ContainerMetrics struct {
	Namespace     string
	PodName       string
	ContainerName string
	NodeName      string
	Timestamp     time.Time
	CpuCores      float64
	MemoryBytes   int64
}

// Collect gathers metrics from all nodes
func (c *KubeletCollector) Collect(ctx context.Context) ([]ContainerMetrics, error) {
	// Get pod metrics from metrics-server
	podMetricsList, err := c.metricsClient.MetricsV1beta1().PodMetricses("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing pod metrics: %w", err)
	}

	// Get pods to get node assignments
	pods, err := c.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing pods: %w", err)
	}

	// Build pod -> node map
	podNodeMap := make(map[string]string)
	for _, pod := range pods.Items {
		key := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
		podNodeMap[key] = pod.Spec.NodeName
	}

	var metrics []ContainerMetrics
	now := time.Now().UTC()

	for _, pm := range podMetricsList.Items {
		if !c.cfg.ShouldCollectNamespace(pm.Namespace) {
			continue
		}

		podKey := fmt.Sprintf("%s/%s", pm.Namespace, pm.Name)
		nodeName := podNodeMap[podKey]

		for _, container := range pm.Containers {
			cpuCores := float64(container.Usage.Cpu().MilliValue()) / 1000.0
			memoryBytes := container.Usage.Memory().Value()

			metrics = append(metrics, ContainerMetrics{
				Namespace:     pm.Namespace,
				PodName:       pm.Name,
				ContainerName: container.Name,
				NodeName:      nodeName,
				Timestamp:     now,
				CpuCores:      cpuCores,
				MemoryBytes:   memoryBytes,
			})
		}
	}

	c.logger.Debug("collected metrics",
		zap.Int("containers", len(metrics)),
		zap.Int("pods", len(podMetricsList.Items)))

	return metrics, nil
}

// CollectFromNodes scrapes each node's kubelet directly (fallback)
func (c *KubeletCollector) CollectFromNodes(ctx context.Context) ([]ContainerMetrics, error) {
	nodes, err := c.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing nodes: %w", err)
	}

	var (
		allMetrics []ContainerMetrics
		mu         sync.Mutex
		wg         sync.WaitGroup
	)

	for _, node := range nodes.Items {
		wg.Add(1)
		go func(nodeName string) {
			defer wg.Done()

			nodeMetrics, err := c.scrapeNode(ctx, nodeName)
			if err != nil {
				c.logger.Warn("failed to scrape node",
					zap.String("node", nodeName),
					zap.Error(err))
				return
			}

			mu.Lock()
			allMetrics = append(allMetrics, nodeMetrics...)
			mu.Unlock()
		}(node.Name)
	}

	wg.Wait()
	return allMetrics, nil
}

func (c *KubeletCollector) scrapeNode(ctx context.Context, nodeName string) ([]ContainerMetrics, error) {
	// Use metrics-server API for this specific node
	podMetrics, err := c.metricsClient.MetricsV1beta1().PodMetricses("").List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	})
	if err != nil {
		return nil, fmt.Errorf("getting pod metrics for node %s: %w", nodeName, err)
	}

	var metrics []ContainerMetrics
	now := time.Now().UTC()

	for _, pm := range podMetrics.Items {
		if !c.cfg.ShouldCollectNamespace(pm.Namespace) {
			continue
		}

		for _, container := range pm.Containers {
			metrics = append(metrics, ContainerMetrics{
				Namespace:     pm.Namespace,
				PodName:       pm.Name,
				ContainerName: container.Name,
				NodeName:      nodeName,
				Timestamp:     now,
				CpuCores:      float64(container.Usage.Cpu().MilliValue()) / 1000.0,
				MemoryBytes:   container.Usage.Memory().Value(),
			})
		}
	}

	return metrics, nil
}

// BuildPodMetricsFromRaw converts raw container metrics to pod metrics
func BuildPodMetricsFromRaw(raw []ContainerMetrics) map[string]*v1.PodMetrics {
	podMap := make(map[string]*v1.PodMetrics)

	for _, m := range raw {
		key := fmt.Sprintf("%s/%s", m.Namespace, m.PodName)

		pm, exists := podMap[key]
		if !exists {
			pm = &v1.PodMetrics{
				Namespace:  m.Namespace,
				PodName:    m.PodName,
				NodeName:   m.NodeName,
				Containers: []v1.Container{},
			}
			podMap[key] = pm
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

	return podMap
}

// isContainerMetric checks if the metric is a container metric we care about
func isContainerMetric(name string) bool {
	return name == "container_cpu_usage_seconds_total" ||
		name == "container_memory_working_set_bytes"
}

// Helper to safely convert metrics
func podMetricsToV1(pm *metricsv1beta1.PodMetrics) *v1.PodMetrics {
	containers := make([]v1.Container, 0, len(pm.Containers))
	for _, c := range pm.Containers {
		containers = append(containers, v1.Container{
			Name: c.Name,
			Samples: []v1.ContainerSample{{
				Timestamp:        pm.Timestamp.Time,
				CpuUsageCores:    float64(c.Usage.Cpu().MilliValue()) / 1000.0,
				MemoryUsageBytes: c.Usage.Memory().Value(),
			}},
		})
	}
	return &v1.PodMetrics{
		Namespace:  pm.Namespace,
		PodName:    pm.Name,
		Containers: containers,
	}
}
