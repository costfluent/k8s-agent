package collector

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/config"
)

const (
	gpuResource            = corev1.ResourceName("nvidia.com/gpu")
	namespaceLabelsRefresh = 5 * time.Minute
	ownerCacheLimit        = 10000
)

// Pod labels that identify a revision or an ordinal rather than a workload. They are never sent.
var noisePodLabels = map[string]bool{
	"pod-template-hash":                  true,
	"controller-revision-hash":           true,
	"statefulset.kubernetes.io/pod-name": true,
}

// PodInfo is a running pod and the container requests the aggregator allocates against.
type PodInfo struct {
	v1.Pod
	Specs []ContainerSpec
}

// ContainerSpec is one container's resource requests.
type ContainerSpec struct {
	Name               string
	CPURequestCores    float64
	MemoryRequestBytes int64
	GPURequest         int
}

// Metadata reads nodes and pods from informer caches and resolves pod controllers.
type Metadata struct {
	client  kubernetes.Interface
	cfg     *config.Config
	logger  *zap.Logger
	factory informers.SharedInformerFactory
	nodes   listersv1.NodeLister
	pods    listersv1.PodLister

	mu              sync.Mutex
	owners          map[string]owner
	namespaceLabels map[string]map[string]string
	namespacesAt    time.Time
}

type owner struct{ kind, name string }

// NewMetadata wires node and pod informers. Call Start before reading.
func NewMetadata(client kubernetes.Interface, cfg *config.Config, logger *zap.Logger) *Metadata {
	factory := informers.NewSharedInformerFactory(client, 0)
	return &Metadata{
		client:  client,
		cfg:     cfg,
		logger:  logger.Named("metadata"),
		factory: factory,
		nodes:   factory.Core().V1().Nodes().Lister(),
		pods:    factory.Core().V1().Pods().Lister(),
		owners:  map[string]owner{},
	}
}

// Start runs the informers until ctx ends and waits for their first sync.
func (m *Metadata) Start(ctx context.Context) error {
	m.factory.Start(ctx.Done())
	for informer, synced := range m.factory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			return fmt.Errorf("informer %v did not sync: %w", informer, ctx.Err())
		}
	}
	return nil
}

// Nodes returns the cluster's nodes, sorted by name.
func (m *Metadata) Nodes() ([]*corev1.Node, error) {
	nodes, err := m.nodes.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("listing nodes: %w", err)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return nodes, nil
}

// ConvertNode maps a node to the wire shape. ObservedSeconds is left to the aggregator.
func ConvertNode(node *corev1.Node) v1.Node {
	gpus := 0
	if q, ok := node.Status.Capacity[gpuResource]; ok {
		gpus = int(q.Value())
	}
	return v1.Node{
		Name:                   node.Name,
		ProviderID:             node.Spec.ProviderID,
		InstanceType:           firstLabel(node.Labels, "node.kubernetes.io/instance-type", "beta.kubernetes.io/instance-type"),
		Region:                 firstLabel(node.Labels, "topology.kubernetes.io/region", "failure-domain.beta.kubernetes.io/region"),
		Zone:                   firstLabel(node.Labels, "topology.kubernetes.io/zone", "failure-domain.beta.kubernetes.io/zone"),
		Spot:                   isSpot(node.Labels),
		CapacityCPUCores:       cores(node.Status.Capacity.Cpu()),
		CapacityMemoryBytes:    bytesOf(node.Status.Capacity.Memory()),
		CapacityGPU:            gpus,
		AllocatableCPUCores:    cores(node.Status.Allocatable.Cpu()),
		AllocatableMemoryBytes: bytesOf(node.Status.Allocatable.Memory()),
		Labels:                 copyMap(node.Labels),
		Annotations:            pick(node.Annotations, v1.RateAnnotations, 0),
	}
}

func isSpot(l map[string]string) bool {
	return l["eks.amazonaws.com/capacityType"] == "SPOT" ||
		l["karpenter.sh/capacity-type"] == "spot" ||
		l["cloud.google.com/gke-spot"] == "true" ||
		l["cloud.google.com/gke-preemptible"] == "true" ||
		strings.EqualFold(l["kubernetes.azure.com/scalesetpriority"], "spot")
}

// Pods returns the running pods bound to a node, with controllers resolved and labels filtered.
func (m *Metadata) Pods(ctx context.Context) ([]PodInfo, error) {
	pods, err := m.pods.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("listing pods: %w", err)
	}
	nsLabels := m.namespaceLabelsFor(ctx)

	out := make([]PodInfo, 0, len(pods))
	for _, pod := range pods {
		if pod.Spec.NodeName == "" || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		kind, name := m.resolveController(ctx, pod)
		info := PodInfo{
			Pod: v1.Pod{
				Namespace:       pod.Namespace,
				Name:            pod.Name,
				Node:            pod.Spec.NodeName,
				ControllerKind:  kind,
				Controller:      name,
				Labels:          m.podLabels(pod.Labels),
				Annotations:     pick(pod.Annotations, m.cfg.AllowedAnnotations, config.MaxAnnotationValueChars),
				NamespaceLabels: nsLabels[pod.Namespace],
			},
		}
		for _, c := range pod.Spec.Containers {
			info.Specs = append(info.Specs, ContainerSpec{
				Name:               c.Name,
				CPURequestCores:    cores(c.Resources.Requests.Cpu()),
				MemoryRequestBytes: bytesOf(c.Resources.Requests.Memory()),
				GPURequest:         gpuRequest(c.Resources),
			})
		}
		out = append(out, info)
	}
	return out, nil
}

func gpuRequest(r corev1.ResourceRequirements) int {
	if q, ok := r.Requests[gpuResource]; ok {
		return int(q.Value())
	}
	if q, ok := r.Limits[gpuResource]; ok {
		return int(q.Value())
	}
	return 0
}

func (m *Metadata) podLabels(l map[string]string) map[string]string {
	if len(m.cfg.AllowedLabels) > 0 {
		return pick(l, m.cfg.AllowedLabels, 0)
	}
	out := map[string]string{}
	for k, v := range l {
		if !noisePodLabels[k] {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (m *Metadata) namespaceLabelsFor(ctx context.Context) map[string]map[string]string {
	if !m.cfg.CollectNamespaceLabels {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.namespaceLabels != nil && time.Since(m.namespacesAt) < namespaceLabelsRefresh {
		return m.namespaceLabels
	}
	list, err := m.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		m.logger.Warn("listing namespaces failed; keeping the previous namespace labels", zap.Error(err))
		return m.namespaceLabels
	}
	labelsByNamespace := make(map[string]map[string]string, len(list.Items))
	for _, ns := range list.Items {
		labelsByNamespace[ns.Name] = copyMap(ns.Labels)
	}
	m.namespaceLabels, m.namespacesAt = labelsByNamespace, time.Now()
	return labelsByNamespace
}

// resolveController follows a ReplicaSet to its Deployment and a Job to its CronJob.
func (m *Metadata) resolveController(ctx context.Context, pod *corev1.Pod) (string, string) {
	ref := metav1.GetControllerOf(pod)
	if ref == nil {
		if len(pod.OwnerReferences) == 0 {
			return "", ""
		}
		ref = &pod.OwnerReferences[0]
	}
	if ref.Kind != "ReplicaSet" && ref.Kind != "Job" {
		return ref.Kind, ref.Name
	}

	key := pod.Namespace + "/" + ref.Kind + "/" + ref.Name
	m.mu.Lock()
	cached, ok := m.owners[key]
	m.mu.Unlock()
	if ok {
		return cached.kind, cached.name
	}

	owners, err := m.ownersOf(ctx, pod.Namespace, ref.Kind, ref.Name)
	if err != nil {
		m.logger.Debug("resolving controller failed; using the direct owner",
			zap.String("namespace", pod.Namespace), zap.String("kind", ref.Kind), zap.String("name", ref.Name), zap.Error(err))
		return ref.Kind, ref.Name
	}

	resolved := owner{kind: ref.Kind, name: ref.Name}
	if len(owners) > 0 {
		resolved = owner{kind: owners[0].Kind, name: owners[0].Name}
	}
	m.mu.Lock()
	if len(m.owners) >= ownerCacheLimit {
		m.owners = map[string]owner{}
	}
	m.owners[key] = resolved
	m.mu.Unlock()
	return resolved.kind, resolved.name
}

func (m *Metadata) ownersOf(ctx context.Context, namespace, kind, name string) ([]metav1.OwnerReference, error) {
	if kind == "ReplicaSet" {
		rs, err := m.client.AppsV1().ReplicaSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return rs.OwnerReferences, nil
	}
	job, err := m.client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return job.OwnerReferences, nil
}

// pick returns only the allowed keys, with values cut to maxChars runes when maxChars > 0.
func pick(src map[string]string, allowed []string, maxChars int) map[string]string {
	if len(src) == 0 || len(allowed) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, key := range allowed {
		v, ok := src[key]
		if !ok {
			continue
		}
		if maxChars > 0 {
			if r := []rune(v); len(r) > maxChars {
				v = string(r[:maxChars])
			}
		}
		out[key] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func firstLabel(l map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := l[k]; v != "" {
			return v
		}
	}
	return ""
}

func copyMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]string, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func cores(q *resource.Quantity) float64 {
	if q == nil {
		return 0
	}
	return float64(q.MilliValue()) / 1000
}

func bytesOf(q *resource.Quantity) int64 {
	if q == nil {
		return 0
	}
	return q.Value()
}
