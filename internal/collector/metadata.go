package collector

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/config"
	"github.com/costfluent/k8s-agent/internal/metrics"
)

const (
	gpuResource            = corev1.ResourceName(v1.ResourceNVIDIAGPU)
	namespaceLabelsRefresh = 5 * time.Minute
	cacheSyncTimeout       = 5 * time.Minute
	listPageSize           = 500
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
	// GPURequests is nil for a container without GPUs, so the pod cache holds nothing more for it.
	GPURequests map[string]int
}

// Metadata reads nodes and pods from informer caches and resolves pod controllers.
type Metadata struct {
	client kubernetes.Interface
	cfg    *config.Config
	logger *zap.Logger
	nodes  listersv1.NodeLister
	pods   cache.Store
	// storage reads volumes beside nodes and pods, never holding them back.
	storage *storage

	syncTimeout time.Duration

	mu              sync.Mutex
	owners          *ownerCache
	namespaceLabels map[string]map[string]string
	namespacesAt    time.Time
}

type owner struct{ kind, name string }

// NewClient builds the Kubernetes API client the agent reads metadata with.
// Its rate limit is the configured one: client-go's default of 5 requests a second resolves the
// owners of a large cluster's pods for minutes.
func NewClient(restCfg *rest.Config, cfg *config.Config) (kubernetes.Interface, error) {
	c := rest.CopyConfig(restCfg)
	if cfg.KubeAPIQPS > 0 {
		c.QPS, c.Burst = float32(cfg.KubeAPIQPS), cfg.KubeAPIBurst
	}
	return kubernetes.NewForConfig(c)
}

// Collect is one poll: the nodes, the running pods, and each kubelet's usage.
func Collect(ctx context.Context, metadata *Metadata, kubelet *KubeletCollector, now time.Time) (Observation, error) {
	began := time.Now()
	nodes, err := metadata.Nodes()
	if err != nil {
		return Observation{}, err
	}
	pods, err := metadata.Pods(ctx)
	if err != nil {
		return Observation{}, err
	}
	obs := Observation{Time: now, Pods: pods}
	obs.Volumes, _ = metadata.Volumes(ctx)
	obs.Usage, obs.Unscraped = kubelet.Scrape(ctx, nodes)
	// Owner resolution and scrapes fall back rather than fail, so a poll cut short reads as a cluster
	// with no usage and unresolved controllers.
	if err := ctx.Err(); err != nil {
		return Observation{}, fmt.Errorf("poll interrupted: %w", err)
	}
	for _, n := range nodes {
		obs.Nodes = append(obs.Nodes, ConvertNode(n))
	}
	obs.Duration = time.Since(began)
	return obs, nil
}

// NewMetadata prepares node and pod informers. Call Start before reading.
func NewMetadata(client kubernetes.Interface, cfg *config.Config, logger *zap.Logger) *Metadata {
	size := cfg.OwnerCacheSize
	if size <= 0 {
		size = config.DefaultOwnerCacheSize
	}
	logger = logger.Named("metadata")
	return &Metadata{client: client, cfg: cfg, logger: logger, owners: newOwnerCache(size), syncTimeout: cacheSyncTimeout,
		storage: newStorage(client, cfg, logger)}
}

// Start runs the node, pod and storage informers until ctx ends and waits for the node and pod
// ones' first sync. Each lists in pages that are cut down to what the agent reads before the next
// page arrives, so the whole cluster is never held in memory as the API server sent it. Storage is
// not waited for: an agent whose ClusterRole predates it reports nodes and pods all the same.
func (m *Metadata) Start(ctx context.Context) error {
	core := m.client.CoreV1()
	nodes := cache.NewSharedIndexInformer(pagedListWatch{&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
			items, rv, err := listPages(ctx, opts, func(ctx context.Context, opts metav1.ListOptions) ([]corev1.Node, metav1.ListMeta, error) {
				l, err := core.Nodes().List(ctx, opts)
				if err != nil {
					return nil, metav1.ListMeta{}, err
				}
				return l.Items, l.ListMeta, nil
			}, func(n *corev1.Node) corev1.Node {
				stripNode(n)
				return *n
			})
			return &corev1.NodeList{ListMeta: metav1.ListMeta{ResourceVersion: rv}, Items: items}, err
		},
		WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			return core.Nodes().Watch(ctx, opts)
		},
	}}, &corev1.Node{}, 0, cache.Indexers{})
	pods := cache.NewSharedIndexInformer(pagedListWatch{&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
			items, rv, err := listPages(ctx, opts, func(ctx context.Context, opts metav1.ListOptions) ([]corev1.Pod, metav1.ListMeta, error) {
				l, err := core.Pods("").List(ctx, opts)
				if err != nil {
					return nil, metav1.ListMeta{}, err
				}
				return l.Items, l.ListMeta, nil
			}, m.cachePod)
			return &cachedPodList{ListMeta: metav1.ListMeta{ResourceVersion: rv}, Items: items}, err
		},
		WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			return core.Pods("").Watch(ctx, opts)
		},
	}}, &corev1.Pod{}, 0, cache.Indexers{})
	if err := nodes.SetTransform(func(obj any) (any, error) {
		if n, ok := obj.(*corev1.Node); ok {
			stripNode(n)
		}
		return obj, nil
	}); err != nil {
		return fmt.Errorf("node informer: %w", err)
	}
	if err := pods.SetTransform(func(obj any) (any, error) {
		if p, ok := obj.(*corev1.Pod); ok {
			return m.cachePod(p), nil
		}
		return obj, nil
	}); err != nil {
		return fmt.Errorf("pod informer: %w", err)
	}
	m.nodes = listersv1.NewNodeLister(nodes.GetIndexer())
	m.pods = pods.GetStore()
	if err := m.storage.start(ctx); err != nil {
		return err
	}
	go nodes.Run(ctx.Done())
	go pods.Run(ctx.Done())
	syncCtx, cancel := context.WithTimeout(ctx, m.syncTimeout)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), nodes.HasSynced, pods.HasSynced) {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("node and pod informers did not sync: %w", err)
		}
		return fmt.Errorf("node and pod informers did not sync within %s: check that the agent's ClusterRole grants list and watch on nodes and pods", m.syncTimeout)
	}
	return nil
}

// pagedListWatch keeps an informer's initial list on its ListWithContextFunc, which listPages serves. Since
// client-go v0.35 a reflector streams the initial list as watch events instead (WatchListClient),
// which bypasses it and is not what the load test measures the agent's budgets with.
type pagedListWatch struct{ *cache.ListWatch }

// IsWatchListSemanticsUnSupported opts the reflector out of streaming lists.
func (pagedListWatch) IsWatchListSemanticsUnSupported() bool { return true }

// listPages reads a whole list listPageSize items at a time from a consistent read, which the API
// server pages, and converts each item to what the cache keeps before the next page is read. It
// ignores the reflector's own resource version and limit: a paged list is always the latest state.
func listPages[T, C any](ctx context.Context, opts metav1.ListOptions,
	page func(context.Context, metav1.ListOptions) ([]T, metav1.ListMeta, error), convert func(*T) C,
) ([]C, string, error) {
	opts.Limit, opts.Continue, opts.ResourceVersion, opts.ResourceVersionMatch = listPageSize, "", "", ""
	var all []C
	for {
		items, meta, err := page(ctx, opts)
		if err != nil {
			return nil, "", err
		}
		if all == nil && meta.RemainingItemCount != nil {
			all = make([]C, 0, len(items)+int(*meta.RemainingItemCount))
		}
		for i := range items {
			all = append(all, convert(&items[i]))
		}
		if meta.Continue == "" {
			return all, meta.ResourceVersion, nil
		}
		opts.Continue = meta.Continue
	}
}

// stripNode cuts a node down, in place, to what ConvertNode and the kubelet scrape read.
func stripNode(n *corev1.Node) {
	*n = corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: n.Name, Labels: n.Labels, Annotations: keepKeys(n.Annotations, v1.ForwardedAnnotations)},
		Spec:       corev1.NodeSpec{ProviderID: n.Spec.ProviderID},
		Status: corev1.NodeStatus{
			Capacity:        keepCapacity(n.Status.Capacity),
			Allocatable:     keepResources(n.Status.Allocatable, corev1.ResourceCPU, corev1.ResourceMemory),
			Addresses:       n.Status.Addresses,
			DaemonEndpoints: n.Status.DaemonEndpoints,
		},
	}
}

// keepCapacity returns a node's capacity of CPU, memory and every GPU family, reusing r when
// nothing is dropped.
func keepCapacity(r corev1.ResourceList) corev1.ResourceList {
	kept := 0
	for name := range r {
		if keptCapacity(name) {
			kept++
		}
	}
	if kept == len(r) {
		return r
	}
	if kept == 0 {
		return nil
	}
	out := make(corev1.ResourceList, kept)
	for name, q := range r {
		if keptCapacity(name) {
			out[name] = q
		}
	}
	return out
}

func keptCapacity(name corev1.ResourceName) bool {
	return name == corev1.ResourceCPU || name == corev1.ResourceMemory || v1.IsGPUResource(string(name))
}

// keepKeys returns m without the keys outside allowed, reusing m when nothing is dropped.
func keepKeys(m map[string]string, allowed []string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	kept := 0
	for _, k := range allowed {
		if _, ok := m[k]; ok {
			kept++
		}
	}
	if kept == len(m) {
		return m
	}
	if kept == 0 {
		return nil
	}
	out := make(map[string]string, kept)
	for _, k := range allowed {
		if v, ok := m[k]; ok {
			out[k] = v
		}
	}
	return out
}

func keepResources(r corev1.ResourceList, names ...corev1.ResourceName) corev1.ResourceList {
	if len(r) == 0 {
		return nil
	}
	kept := 0
	for _, n := range names {
		if _, ok := r[n]; ok {
			kept++
		}
	}
	if kept == len(r) {
		return r
	}
	if kept == 0 {
		return nil
	}
	out := make(corev1.ResourceList, kept)
	for _, n := range names {
		if q, ok := r[n]; ok {
			out[n] = q
		}
	}
	return out
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
		GPUResources:           gpuCounts(node.Status.Capacity, nil),
		AllocatableCPUCores:    cores(node.Status.Allocatable.Cpu()),
		AllocatableMemoryBytes: bytesOf(node.Status.Allocatable.Memory()),
		Labels:                 fitMap(node.Labels, "node_labels"),
		Annotations:            fitMap(pick(node.Annotations, v1.ForwardedAnnotations, 0), "node_annotations"),
		// Read before the label cap: GPU Feature Discovery's labels sort after the dozens Node
		// Feature Discovery writes, so the first MaxMapEntries labels rarely hold them.
		GPULabels: pick(node.Labels, v1.GPULabels, 0),
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
	cached := m.pods.List()
	nsLabels := m.namespaceLabelsFor(ctx)
	out := make([]PodInfo, 0, len(cached))
	for _, obj := range cached {
		pod, ok := obj.(*cachedPod)
		if !ok {
			return nil, fmt.Errorf("listing pods: the cache holds a %T", obj)
		}
		if pod.Node == "" || pod.Phase != string(corev1.PodRunning) {
			continue
		}
		out = append(out, m.podInfo(ctx, pod, nsLabels))
	}
	return out, nil
}

// Volumes returns every persistent volume, sorted by name, and false while storage cannot be read,
// when a report leaves volumes out.
func (m *Metadata) Volumes(ctx context.Context) ([]v1.Volume, bool) {
	return m.storage.read(ctx)
}

func (m *Metadata) podInfo(ctx context.Context, pod *cachedPod, nsLabels map[string]map[string]string) PodInfo {
	kind, name := m.resolveController(ctx, pod)
	return PodInfo{
		Pod: v1.Pod{
			Namespace:       pod.Namespace,
			Name:            pod.Name,
			Node:            pod.Node,
			ControllerKind:  kind,
			Controller:      name,
			Labels:          fitMap(pod.Labels, "pod_labels"),
			Annotations:     fitMap(pod.Annotations, "pod_annotations"),
			NamespaceLabels: nsLabels[pod.Namespace],
			Claims:          pod.Claims,
		},
		Specs: pod.Containers,
	}
}

// gpuCounts returns the whole count of each GPU family resource in primary, falling back to
// fallback, within the API's caps: the first MaxGPUMapEntries names in order whose name and count
// fit. Nil when there is none.
func gpuCounts(primary, fallback corev1.ResourceList) map[string]int {
	var names []string
	for _, list := range []corev1.ResourceList{primary, fallback} {
		for name := range list {
			if n := string(name); v1.IsGPUResource(n) && !slices.Contains(names, n) {
				names = append(names, n)
			}
		}
	}
	if len(names) == 0 {
		return nil
	}
	slices.Sort(names)
	var out map[string]int
	for _, n := range names {
		q, ok := primary[corev1.ResourceName(n)]
		if !ok {
			q = fallback[corev1.ResourceName(n)]
		}
		count := q.Value()
		if count <= 0 || count > v1.MaxGPUUnits || utf16Len(n) > v1.MaxGPUResourceNameLength {
			continue
		}
		if out == nil {
			out = make(map[string]int, min(len(names), v1.MaxGPUMapEntries))
		}
		out[n] = int(count)
		if len(out) == v1.MaxGPUMapEntries {
			break
		}
	}
	return out
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
		labelsByNamespace[ns.Name] = fitMap(ns.Labels, "namespace_labels")
	}
	m.namespaceLabels, m.namespacesAt = labelsByNamespace, time.Now()
	return labelsByNamespace
}

// resolveController follows a ReplicaSet to its Deployment and a Job to its CronJob.
func (m *Metadata) resolveController(ctx context.Context, pod *cachedPod) (string, string) {
	kind, name := pod.OwnerKind, pod.OwnerName
	if kind != "ReplicaSet" && kind != "Job" {
		return kind, name
	}

	key := pod.Namespace + "/" + kind + "/" + name
	m.mu.Lock()
	cached, ok := m.owners.get(key)
	m.mu.Unlock()
	if ok {
		return cached.kind, cached.name
	}

	owners, err := m.ownersOf(ctx, pod.Namespace, kind, name)
	if err != nil {
		m.logger.Debug("resolving controller failed; using the direct owner",
			zap.String("namespace", pod.Namespace), zap.String("kind", kind), zap.String("name", name), zap.Error(err))
		return kind, name
	}

	resolved := owner{kind: kind, name: name}
	if len(owners) > 0 {
		resolved = owner{kind: owners[0].Kind, name: owners[0].Name}
	}
	m.mu.Lock()
	m.owners.put(key, resolved)
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

// fitMap returns src within the API's map caps: src itself when it fits, which callers share and
// never modify, otherwise in key order the first MaxMapEntries entries whose key and value fit the
// length caps. The API refuses a whole report over them, so the entries left out are counted under
// mapName instead.
func fitMap(src map[string]string, mapName string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	if fits(src) {
		return src
	}
	keys := slices.Sorted(maps.Keys(src))
	out := make(map[string]string, min(len(src), v1.MaxMapEntries))
	for _, k := range keys {
		if len(out) == v1.MaxMapEntries {
			break
		}
		if v := src[k]; utf16Len(k) <= v1.MaxMapKeyLength && utf16Len(v) <= v1.MaxMapValueLength {
			out[k] = v
		}
	}
	if dropped := len(src) - len(out); dropped > 0 {
		metrics.MapEntriesDropped.WithLabelValues(mapName).Add(float64(dropped))
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// fits reports whether m is within the map caps as it is, so fitMap can share it rather than copy.
func fits(m map[string]string) bool {
	if len(m) > v1.MaxMapEntries {
		return false
	}
	for k, v := range m {
		if utf16Len(k) > v1.MaxMapKeyLength || utf16Len(v) > v1.MaxMapValueLength {
			return false
		}
	}
	return true
}

// utf16Len is s's length as the API counts it.
func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		n += utf16.RuneLen(r)
	}
	return n
}

func firstLabel(l map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := l[k]; v != "" {
			return v
		}
	}
	return ""
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
