package collector

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/config"
)

// MetadataCollector gathers pod and node metadata from kube-apiserver
type MetadataCollector struct {
	client kubernetes.Interface
	cfg    *config.Config
	logger *zap.Logger
}

// NewMetadataCollector creates a new metadata collector
func NewMetadataCollector(
	client kubernetes.Interface,
	cfg *config.Config,
	logger *zap.Logger,
) *MetadataCollector {
	return &MetadataCollector{
		client: client,
		cfg:    cfg,
		logger: logger.Named("metadata"),
	}
}

// CollectNodes gathers node information
func (c *MetadataCollector) CollectNodes(ctx context.Context) ([]v1.Node, error) {
	nodes, err := c.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing nodes: %w", err)
	}

	result := make([]v1.Node, 0, len(nodes.Items))
	for _, node := range nodes.Items {
		n := c.convertNode(&node)
		result = append(result, n)
	}

	c.logger.Debug("collected nodes", zap.Int("count", len(result)))
	return result, nil
}

func (c *MetadataCollector) convertNode(node *corev1.Node) v1.Node {
	// Extract capacity
	cpuCores := float64(node.Status.Capacity.Cpu().MilliValue()) / 1000.0
	memoryBytes := node.Status.Capacity.Memory().Value()

	// Extract allocatable (if different from capacity)
	allocatableCpu := float64(node.Status.Allocatable.Cpu().MilliValue()) / 1000.0
	allocatableMemory := node.Status.Allocatable.Memory().Value()

	// GPU capacity
	gpuCount := 0
	if gpu, ok := node.Status.Capacity["nvidia.com/gpu"]; ok {
		gpuCount = int(gpu.Value())
	}

	// Instance type and zone from labels
	instanceType := node.Labels["node.kubernetes.io/instance-type"]
	if instanceType == "" {
		instanceType = node.Labels["beta.kubernetes.io/instance-type"]
	}

	zone := node.Labels["topology.kubernetes.io/zone"]
	if zone == "" {
		zone = node.Labels["failure-domain.beta.kubernetes.io/zone"]
	}

	region := node.Labels["topology.kubernetes.io/region"]
	if region == "" {
		region = node.Labels["failure-domain.beta.kubernetes.io/region"]
	}

	// Spot instance detection
	isSpot := c.isSpotInstance(node)

	// Filter annotations for pricing info
	annotations := c.filterAnnotations(node.Annotations, []string{
		"costfluent.com/vcpu-hourly-rate",
		"costfluent.com/ram-gb-hourly-rate",
		"costfluent.com/gpu-hourly-rate",
		"costfluent.com/storage-gb-hourly-rate",
	})

	return v1.Node{
		Name:                   node.Name,
		InstanceType:           instanceType,
		Region:                 region,
		Zone:                   zone,
		CapacityCpuCores:       cpuCores,
		CapacityMemoryBytes:    memoryBytes,
		AllocatableCpuCores:    allocatableCpu,
		AllocatableMemoryBytes: allocatableMemory,
		CapacityGpu:            gpuCount,
		IsSpotInstance:         isSpot,
		ProviderID:             node.Spec.ProviderID,
		Labels:                 c.filterLabels(node.Labels),
		Annotations:            annotations,
	}
}

func (c *MetadataCollector) isSpotInstance(node *corev1.Node) bool {
	// AWS EKS
	if val, ok := node.Labels["eks.amazonaws.com/capacityType"]; ok && val == "SPOT" {
		return true
	}
	// GCP GKE
	if val, ok := node.Labels["cloud.google.com/gke-spot"]; ok && val == "true" {
		return true
	}
	// Azure AKS
	if val, ok := node.Labels["kubernetes.azure.com/scalesetpriority"]; ok && strings.ToLower(val) == "spot" {
		return true
	}
	return false
}

// CollectPodMetadata enriches pod metrics with metadata
func (c *MetadataCollector) CollectPodMetadata(ctx context.Context, podMetrics map[string]*v1.PodMetrics) error {
	pods, err := c.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing pods: %w", err)
	}

	for _, pod := range pods.Items {
		if !c.cfg.ShouldCollectNamespace(pod.Namespace) {
			continue
		}

		key := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
		pm, exists := podMetrics[key]
		if !exists {
			continue
		}

		// Enrich with metadata
		pm.Labels = c.filterLabels(pod.Labels)
		pm.Annotations = c.filterPodAnnotations(pod.Annotations)
		pm.NodeName = pod.Spec.NodeName
		pm.Phase = string(pod.Status.Phase)

		if pod.Status.StartTime != nil {
			pm.StartTime = pod.Status.StartTime.Time
		}

		// Resolve controller
		controllerName, controllerKind := c.resolveController(ctx, &pod)
		pm.ControllerName = controllerName
		pm.ControllerKind = controllerKind

		// Add container resource requests/limits
		c.enrichContainerResources(pm, &pod)
	}

	return nil
}

func (c *MetadataCollector) enrichContainerResources(pm *v1.PodMetrics, pod *corev1.Pod) {
	containerSpecs := make(map[string]corev1.Container)
	for _, cs := range pod.Spec.Containers {
		containerSpecs[cs.Name] = cs
	}

	for i := range pm.Containers {
		container := &pm.Containers[i]
		if spec, ok := containerSpecs[container.Name]; ok {
			container.CpuRequestCores = resourceToFloat(spec.Resources.Requests.Cpu())
			container.CpuLimitCores = resourceToFloat(spec.Resources.Limits.Cpu())
			container.MemoryRequestBytes = resourceToInt(spec.Resources.Requests.Memory())
			container.MemoryLimitBytes = resourceToInt(spec.Resources.Limits.Memory())
		}
	}
}

func resourceToFloat(q *resource.Quantity) float64 {
	if q == nil || q.IsZero() {
		return 0
	}
	return float64(q.MilliValue()) / 1000.0
}

func resourceToInt(q *resource.Quantity) int64 {
	if q == nil || q.IsZero() {
		return 0
	}
	return q.Value()
}

func (c *MetadataCollector) resolveController(ctx context.Context, pod *corev1.Pod) (string, string) {
	if len(pod.OwnerReferences) == 0 {
		return "", ""
	}

	owner := pod.OwnerReferences[0]
	name := owner.Name
	kind := owner.Kind

	// Resolve ReplicaSet -> Deployment
	if kind == "ReplicaSet" {
		rs, err := c.client.AppsV1().ReplicaSets(pod.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil && len(rs.OwnerReferences) > 0 {
			return rs.OwnerReferences[0].Name, rs.OwnerReferences[0].Kind
		}
	}

	// Resolve Job -> CronJob
	if kind == "Job" {
		job, err := c.client.BatchV1().Jobs(pod.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil && len(job.OwnerReferences) > 0 {
			return job.OwnerReferences[0].Name, job.OwnerReferences[0].Kind
		}
	}

	return name, kind
}

func (c *MetadataCollector) filterLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}

	// Filter out common noise labels
	filtered := make(map[string]string)
	skipPrefixes := []string{
		"pod-template-hash",
		"controller-revision-hash",
		"statefulset.kubernetes.io/pod-name",
	}

	for k, v := range labels {
		skip := false
		for _, prefix := range skipPrefixes {
			if strings.HasPrefix(k, prefix) || k == prefix {
				skip = true
				break
			}
		}
		if !skip {
			filtered[k] = v
		}
	}

	return filtered
}

func (c *MetadataCollector) filterPodAnnotations(annotations map[string]string) map[string]string {
	if !c.cfg.CollectAnnotations || annotations == nil {
		return nil
	}

	filtered := make(map[string]string)
	count := 0

	for k, v := range annotations {
		// Skip system annotations
		if strings.HasPrefix(k, "kubernetes.io/") ||
			strings.HasPrefix(k, "kubectl.kubernetes.io/") {
			continue
		}

		// Check allowed list if specified
		if len(c.cfg.AllowedAnnotations) > 0 {
			allowed := false
			for _, prefix := range c.cfg.AllowedAnnotations {
				if strings.HasPrefix(k, prefix) {
					allowed = true
					break
				}
			}
			if !allowed {
				continue
			}
		}

		// Truncate value if needed
		if len(v) > c.cfg.MaxAnnotationLength {
			v = v[:c.cfg.MaxAnnotationLength]
		}

		filtered[k] = v
		count++

		if count >= c.cfg.MaxAnnotations {
			break
		}
	}

	return filtered
}

func (c *MetadataCollector) filterAnnotations(annotations map[string]string, allowList []string) map[string]string {
	if annotations == nil {
		return nil
	}

	filtered := make(map[string]string)
	for _, key := range allowList {
		if val, ok := annotations[key]; ok {
			filtered[key] = val
		}
	}

	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

// Helper types for internal use
var _ = appsv1.Deployment{}
var _ = batchv1.Job{}
