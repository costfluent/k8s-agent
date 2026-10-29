package collector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/config"
)

func TestNewClient_NegotiatesProtobuf(t *testing.T) {
	accept := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accept <- r.Header.Get("Accept")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"NodeList","apiVersion":"v1","metadata":{},"items":[]}`))
	}))
	defer srv.Close()
	client, err := NewClient(&rest.Config{Host: srv.URL}, &config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := <-accept; !strings.HasPrefix(got, "application/vnd.kubernetes.protobuf") || !strings.Contains(got, "application/json") {
		t.Fatalf("Accept = %q, want protobuf first, JSON as the fallback", got)
	}
}

// richPod carries every field a real workload pod has, and the ones the agent reads in each
// variant: allowlisted and other annotations, noise labels, GPU limits, owners that are not the
// controller.
func richPod(i int) *corev1.Pod {
	yes := true
	created := metav1.NewTime(time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC))
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("web-%d", i), Namespace: "shop", UID: "pod-uid", ResourceVersion: strconv.Itoa(i + 1),
			GenerateName: "web-", CreationTimestamp: created,
			Labels:      map[string]string{"app": "web", "team": "core", "pod-template-hash": "7f9"},
			Annotations: map[string]string{"owner": "core@example.com", "checksum/config": "abc", "kubectl.kubernetes.io/restartedAt": "now"},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "v1", Kind: "ConfigMap", Name: "not-the-controller"},
				{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "web", Controller: &yes, BlockOwnerDeletion: &yes, UID: "s-1"},
			},
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubelet", FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:status":{}}`)}},
			Finalizers:    []string{"example.com/keep"},
		},
		Spec: corev1.PodSpec{
			NodeName: "n1", ServiceAccountName: "web",
			Volumes:        []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "web"}}}}},
			Tolerations:    []corev1.Toleration{{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists}},
			InitContainers: []corev1.Container{{Name: "init", Image: "busybox"}},
			Containers: []corev1.Container{
				{Name: "app", Image: "web:1", Env: []corev1.EnvVar{{Name: "A", Value: "b"}}, Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi"), corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi"), gpuResource: resource.MustParse("1")},
				}},
				{Name: "sidecar", Image: "proxy:1", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{gpuResource: resource.MustParse("2")}}},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "10.0.0.1", HostIP: "192.168.1.1", StartTime: &created,
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "app", Ready: true, Image: "web:1", ImageID: "sha256:1"}},
		},
	}
	if i%2 == 1 {
		p.OwnerReferences = p.OwnerReferences[:1]
	}
	return p
}

func richNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, UID: "node-uid", ResourceVersion: "7",
			Labels: map[string]string{"node.kubernetes.io/instance-type": "m6i.large", "topology.kubernetes.io/region": "eu-central-1",
				"topology.kubernetes.io/zone": "eu-central-1a", "karpenter.sh/capacity-type": "spot", "team": "core"},
			Annotations: map[string]string{v1.AnnotationVCPUHourlyRate: "0.02", v1.AnnotationGPUHourlyRate: "0.9", v1.AnnotationPhysicalGPUCount: "1",
				"csi.volume.kubernetes.io/nodeid": "{}"},
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubelet"}},
		},
		Spec: corev1.NodeSpec{ProviderID: "aws:///eu-central-1a/i-1", PodCIDR: "10.0.0.0/24", Taints: []corev1.Taint{{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule}}},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("8Gi"),
				gpuResource: resource.MustParse("1"), "nvidia.com/mig-1g.5gb": resource.MustParse("7"), "nvidia.com/gpu.shared": resource.MustParse("4"),
				"amd.com/gpu": resource.MustParse("2"), corev1.ResourcePods: resource.MustParse("58")},
			Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1930m"), corev1.ResourceMemory: resource.MustParse("7Gi"),
				corev1.ResourcePods: resource.MustParse("58")},
			Addresses:       []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.1.1.1"}, {Type: corev1.NodeHostName, Address: name}},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: 10250}},
			Images:          []corev1.ContainerImage{{Names: []string{"web:1"}, SizeBytes: 1}},
			Conditions:      []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			NodeInfo:        corev1.NodeSystemInfo{KubeletVersion: "v1.32.0"},
		},
	}
}

func TestMetadata_CachedObjectsYieldWhatTheOriginalsDo(t *testing.T) {
	cfg := &config.Config{AllowedAnnotations: []string{"owner"}, CollectNamespaceLabels: true, NodeAddressTypes: config.DefaultNodeAddressTypes}
	nodes := []*corev1.Node{richNode("n1"), richNode("n2")}
	m := startMetadata(t, cfg, nodes[0], nodes[1], richPod(0), richPod(1),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shop", Labels: map[string]string{"cost-center": "rnd"}}})

	got, err := m.Pods(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	specs := []ContainerSpec{
		{Name: "app", CPURequestCores: 0.25, MemoryRequestBytes: 512 << 20, GPURequest: 1, GPURequests: map[string]int{"nvidia.com/gpu": 1}},
		{Name: "sidecar", GPURequest: 2, GPURequests: map[string]int{"nvidia.com/gpu": 2}},
	}
	want := map[string]PodInfo{
		"web-0": {Pod: v1.Pod{Namespace: "shop", Name: "web-0", Node: "n1", ControllerKind: "StatefulSet", Controller: "web",
			Labels: map[string]string{"app": "web", "team": "core"}, Annotations: map[string]string{"owner": "core@example.com"},
			NamespaceLabels: map[string]string{"cost-center": "rnd"}}, Specs: specs},
		"web-1": {Pod: v1.Pod{Namespace: "shop", Name: "web-1", Node: "n1", ControllerKind: "ConfigMap", Controller: "not-the-controller",
			Labels: map[string]string{"app": "web", "team": "core"}, Annotations: map[string]string{"owner": "core@example.com"},
			NamespaceLabels: map[string]string{"cost-center": "rnd"}}, Specs: specs},
	}
	if len(got) != len(want) {
		t.Fatalf("pods = %+v, want %d", got, len(want))
	}
	for _, p := range got {
		if !reflect.DeepEqual(p, want[p.Name]) {
			t.Errorf("pod %s from the cache = %+v, want %+v", p.Name, p, want[p.Name])
		}
	}

	stored, err := m.Nodes()
	if err != nil {
		t.Fatal(err)
	}
	kubelet := NewKubeletCollector(http.DefaultClient, cfg.NodeAddressTypes, 0, zap.NewNop())
	for i, raw := range nodes {
		if want, gotNode := ConvertNode(raw), ConvertNode(stored[i]); !reflect.DeepEqual(gotNode, want) {
			t.Errorf("node %s from the cache = %+v, want %+v", raw.Name, gotNode, want)
		}
		wantURL, _ := kubelet.endpoint(raw)
		if gotURL, err := kubelet.endpoint(stored[i]); err != nil || gotURL != wantURL {
			t.Errorf("kubelet endpoint from the cache = %q %v, want %q", gotURL, err, wantURL)
		}
	}
}

func TestMetadata_CacheKeepsOnlyWhatTheAgentReads(t *testing.T) {
	cfg := &config.Config{AllowedAnnotations: []string{"owner"}}
	m := startMetadata(t, cfg, richNode("n1"), richPod(0))
	pods := m.pods.List()
	if len(pods) != 1 {
		t.Fatalf("cached pods = %v", pods)
	}
	want := &cachedPod{Namespace: "shop", Name: "web-0", Node: "n1", Phase: "Running", OwnerKind: "StatefulSet", OwnerName: "web",
		Labels: map[string]string{"app": "web", "team": "core"}, Annotations: map[string]string{"owner": "core@example.com"},
		Containers: []ContainerSpec{
			{Name: "app", CPURequestCores: 0.25, MemoryRequestBytes: 512 << 20, GPURequest: 1, GPURequests: map[string]int{"nvidia.com/gpu": 1}},
			{Name: "sidecar", GPURequest: 2, GPURequests: map[string]int{"nvidia.com/gpu": 2}},
		}}
	if !reflect.DeepEqual(pods[0], want) {
		t.Errorf("cached pod = %+v, want only what the agent reads: %+v", pods[0], want)
	}

	nodes, err := m.Nodes()
	if err != nil || len(nodes) != 1 {
		t.Fatalf("cached nodes = %v %v", nodes, err)
	}
	n := nodes[0]
	if n.ManagedFields != nil || n.Status.Images != nil || n.Status.Conditions != nil || n.Spec.Taints != nil || n.Status.NodeInfo.KubeletVersion != "" {
		t.Errorf("the cached node keeps fields the agent never reads: %+v", n)
	}
	wantCapacity := []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, gpuResource, "nvidia.com/mig-1g.5gb", "nvidia.com/gpu.shared", "amd.com/gpu"}
	if len(n.Status.Capacity) != len(wantCapacity) {
		t.Errorf("node capacity = %v, want only %v", n.Status.Capacity, wantCapacity)
	}
	for _, r := range wantCapacity {
		if _, ok := n.Status.Capacity[r]; !ok {
			t.Errorf("node capacity = %v, missing %s", n.Status.Capacity, r)
		}
	}
	wantAnnotations := map[string]string{v1.AnnotationVCPUHourlyRate: "0.02", v1.AnnotationGPUHourlyRate: "0.9", v1.AnnotationPhysicalGPUCount: "1"}
	if !reflect.DeepEqual(n.Annotations, wantAnnotations) {
		t.Errorf("node annotations = %v, want the forwarded annotations only: %v", n.Annotations, wantAnnotations)
	}
}

func TestCachedPod_SharesRepeatedStrings(t *testing.T) {
	m := &Metadata{cfg: &config.Config{}}
	a, b := m.cachePod(richPod(0)), m.cachePod(richPod(2))
	if unsafe.StringData(a.Namespace) != unsafe.StringData(b.Namespace) || unsafe.StringData(a.Labels["app"]) != unsafe.StringData(b.Labels["app"]) {
		t.Fatal("two pods hold separate copies of the same namespace and label value")
	}
	c := a.DeepCopyObject().(*cachedPod)
	if !reflect.DeepEqual(c, a) || c == a {
		t.Fatalf("DeepCopyObject = %+v, want an equal, separate pod", c)
	}
	if meta := a.GetObjectMeta(); meta.GetNamespace() != "shop" || meta.GetName() != "web-0" || a.GetObjectKind() == nil {
		t.Fatalf("object meta = %v", meta)
	}
	list := &cachedPodList{Items: []*cachedPod{a}}
	if l := list.DeepCopyObject().(*cachedPodList); len(l.Items) != 1 || &l.Items[0] == &list.Items[0] || list.GetObjectKind() == nil {
		t.Fatal("cachedPodList.DeepCopyObject must copy the item slice")
	}
}

// pagedPods serves a pod list in the pages a request asks for, and records each request.
type pagedPods struct {
	mu    sync.Mutex
	pods  []corev1.Pod
	calls []metav1.ListOptions
}

func (p *pagedPods) react(action clienttesting.Action) (bool, runtime.Object, error) {
	opts := action.(clienttesting.ListActionImpl).ListOptions
	p.mu.Lock()
	p.calls = append(p.calls, opts)
	p.mu.Unlock()
	from := 0
	if opts.Continue != "" {
		from, _ = strconv.Atoi(opts.Continue)
	}
	to := len(p.pods)
	if opts.Limit > 0 {
		to = min(to, from+int(opts.Limit))
	}
	list := &corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: "42"}}
	for i := from; i < to; i++ {
		list.Items = append(list.Items, *p.pods[i].DeepCopy())
	}
	if to < len(p.pods) {
		remaining := int64(len(p.pods) - to)
		list.Continue, list.RemainingItemCount = strconv.Itoa(to), &remaining
	}
	return true, list, nil
}

func TestMetadata_ListsPodsInPagesAndTransformsEachPage(t *testing.T) {
	const count = 2*listPageSize + 1
	served := &pagedPods{}
	for i := range count {
		served.pods = append(served.pods, *richPod(i))
	}
	client := fake.NewClientset()
	client.PrependReactor("list", "pods", served.react)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m := NewMetadata(client, &config.Config{}, zap.NewNop())
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}

	served.mu.Lock()
	calls := append([]metav1.ListOptions(nil), served.calls...)
	served.mu.Unlock()
	if len(calls) != 3 {
		t.Fatalf("list calls = %+v, want three pages of %d", calls, listPageSize)
	}
	for i, c := range calls {
		if c.Limit != listPageSize || c.ResourceVersion != "" {
			t.Errorf("page %d asked limit=%d resourceVersion=%q; want %d from a consistent read, which the API server pages", i, c.Limit, c.ResourceVersion, listPageSize)
		}
	}
	pods := m.pods.List()
	if len(pods) != count {
		t.Fatalf("cached %d pods, want %d", len(pods), count)
	}
	for _, p := range pods {
		if _, ok := p.(*cachedPod); !ok {
			t.Fatalf("cached %T, want each page cut down to a cachedPod", p)
		}
	}

	added := richPod(count)
	if err := client.Tracker().Add(added); err != nil {
		t.Fatal(err)
	}
	for {
		if p, ok, err := m.pods.GetByKey(added.Namespace + "/" + added.Name); err == nil && ok {
			if _, ok := p.(*cachedPod); !ok {
				t.Fatalf("a pod delivered by the watch was cached as %T", p)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the watched pod never reached the cache")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestListPages_ReturnsAPageFailure(t *testing.T) {
	calls := 0
	_, _, err := listPages(context.Background(), metav1.ListOptions{}, func(context.Context, metav1.ListOptions) ([]corev1.Pod, metav1.ListMeta, error) {
		calls++
		if calls == 2 {
			return nil, metav1.ListMeta{}, errors.New("the API server went away")
		}
		return []corev1.Pod{*richPod(calls)}, metav1.ListMeta{Continue: "next"}, nil
	}, func(p *corev1.Pod) *corev1.Pod { return p })
	if err == nil || err.Error() != "the API server went away" || calls != 2 {
		t.Fatalf("err = %v after %d pages, want the second page's failure", err, calls)
	}
}
