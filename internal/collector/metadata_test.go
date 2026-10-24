package collector

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/config"
	"github.com/costfluent/k8s-agent/internal/metrics"
)

func controllerRef(kind, name string) []metav1.OwnerReference {
	yes := true
	return []metav1.OwnerReference{{Kind: kind, Name: name, Controller: &yes}}
}

func runningPod(ns, name, node string, owners []metav1.OwnerReference, labels, annotations map[string]string, containers ...corev1.Container) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, OwnerReferences: owners, Labels: labels, Annotations: annotations},
		Spec:       corev1.PodSpec{NodeName: node, Containers: containers},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func startMetadata(t *testing.T, cfg *config.Config, objects ...interface{ GetName() string }) *Metadata {
	t.Helper()
	client := fake.NewClientset()
	for _, o := range objects {
		var err error
		switch obj := o.(type) {
		case *corev1.Node:
			_, err = client.CoreV1().Nodes().Create(context.Background(), obj, metav1.CreateOptions{})
		case *corev1.Pod:
			_, err = client.CoreV1().Pods(obj.Namespace).Create(context.Background(), obj, metav1.CreateOptions{})
		case *corev1.Namespace:
			_, err = client.CoreV1().Namespaces().Create(context.Background(), obj, metav1.CreateOptions{})
		case *appsv1.ReplicaSet:
			_, err = client.AppsV1().ReplicaSets(obj.Namespace).Create(context.Background(), obj, metav1.CreateOptions{})
		case *batchv1.Job:
			_, err = client.BatchV1().Jobs(obj.Namespace).Create(context.Background(), obj, metav1.CreateOptions{})
		default:
			t.Fatalf("unsupported object %T", o)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	m := NewMetadata(client, cfg, zap.NewNop())
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestConvertNode_SendsOnlyTheRateAnnotations(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "n1",
			Labels: map[string]string{
				"node.kubernetes.io/instance-type": "m6i.large",
				"topology.kubernetes.io/region":    "eu-central-1",
				"topology.kubernetes.io/zone":      "eu-central-1a",
				"eks.amazonaws.com/capacityType":   "SPOT",
			},
			Annotations: map[string]string{
				v1.AnnotationVCPUHourlyRate: "0.02",
				"secret.example.com/config": "do-not-send",
				// Persistent-volume cost is not a feature, and the backend never read this rate.
				"costfluent.com/storage-gb-hourly-rate": "0.0001",
			},
		},
		Spec: corev1.NodeSpec{ProviderID: "aws:///eu-central-1a/i-0abc"},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("8Gi"),
				gpuResource:           resource.MustParse("1"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1930m"),
				corev1.ResourceMemory: resource.MustParse("7Gi"),
			},
		},
	}
	n := ConvertNode(node)
	if n.InstanceType != "m6i.large" || n.Region != "eu-central-1" || n.Zone != "eu-central-1a" || !n.Spot || n.ProviderID != "aws:///eu-central-1a/i-0abc" {
		t.Errorf("identity = %+v", n)
	}
	if n.CapacityCPUCores != 2 || n.AllocatableCPUCores != 1.93 || n.CapacityMemoryBytes != 8<<30 || n.CapacityGPU != 1 {
		t.Errorf("shape = %+v", n)
	}
	if len(n.Annotations) != 1 || n.Annotations[v1.AnnotationVCPUHourlyRate] != "0.02" {
		t.Errorf("annotations = %v", n.Annotations)
	}
}

func TestMetadata_PodsFiltersAndResolvesControllers(t *testing.T) {
	cfg := &config.Config{AllowedAnnotations: []string{"owner"}, CollectNamespaceLabels: true}
	long := strings.Repeat("é", 150)
	gpu := corev1.Container{Name: "trainer", Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi")},
		Limits:   corev1.ResourceList{gpuResource: resource.MustParse("2")},
	}}
	m := startMetadata(t, cfg,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ml", Labels: map[string]string{"cost-center": "rnd"}}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "web-7f9", OwnerReferences: controllerRef("Deployment", "web")}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "nightly-123", OwnerReferences: controllerRef("CronJob", "nightly")}},
		runningPod("ml", "web-7f9-abc", "n1", controllerRef("ReplicaSet", "web-7f9"),
			map[string]string{"app": "web", "pod-template-hash": "7f9"},
			map[string]string{"owner": long, "kubectl.kubernetes.io/last-applied-configuration": "{}"},
			corev1.Container{Name: "web"}),
		runningPod("ml", "nightly-123-x", "n1", controllerRef("Job", "nightly-123"), nil, nil, gpu),
		runningPod("ml", "orphan-rs-pod", "n1", controllerRef("ReplicaSet", "missing"), nil, nil, corev1.Container{Name: "c"}),
		runningPod("ml", "unscheduled", "", nil, nil, nil, corev1.Container{Name: "c"}),
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "done"}, Spec: corev1.PodSpec{NodeName: "n1"}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
	)

	pods, err := m.Pods(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]PodInfo{}
	for _, p := range pods {
		byName[p.Name] = p
	}
	if len(byName) != 3 {
		t.Fatalf("pods = %v, want only the three running and scheduled ones", byName)
	}

	web := byName["web-7f9-abc"]
	if web.ControllerKind != "Deployment" || web.Controller != "web" {
		t.Errorf("web controller = %s/%s", web.ControllerKind, web.Controller)
	}
	if len(web.Labels) != 1 || web.Labels["app"] != "web" {
		t.Errorf("web labels = %v", web.Labels)
	}
	if len(web.Annotations) != 1 || len([]rune(web.Annotations["owner"])) != config.MaxAnnotationValueChars {
		t.Errorf("web annotations = %v", web.Annotations)
	}
	if web.NamespaceLabels["cost-center"] != "rnd" {
		t.Errorf("namespace labels = %v", web.NamespaceLabels)
	}

	job := byName["nightly-123-x"]
	if job.ControllerKind != "CronJob" || job.Controller != "nightly" {
		t.Errorf("job controller = %s/%s", job.ControllerKind, job.Controller)
	}
	if len(job.Specs) != 1 || job.Specs[0].GPURequest != 2 || job.Specs[0].CPURequestCores != 0.5 || job.Specs[0].MemoryRequestBytes != 1<<30 {
		t.Errorf("job specs = %+v", job.Specs)
	}

	orphan := byName["orphan-rs-pod"]
	if orphan.ControllerKind != "ReplicaSet" || orphan.Controller != "missing" {
		t.Errorf("unresolvable owner = %s/%s", orphan.ControllerKind, orphan.Controller)
	}
}

func TestMetadata_DefaultsSendNoAnnotationsOrNamespaceLabels(t *testing.T) {
	m := startMetadata(t, &config.Config{},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"x": "y"}}},
		runningPod("a", "p", "n1", nil, map[string]string{"app": "p"}, map[string]string{"owner": "me"}, corev1.Container{Name: "c"}),
	)
	pods, err := m.Pods(context.Background())
	if err != nil || len(pods) != 1 {
		t.Fatalf("pods = %v, %v", pods, err)
	}
	if pods[0].Annotations != nil || pods[0].NamespaceLabels != nil {
		t.Errorf("defaults sent %v %v", pods[0].Annotations, pods[0].NamespaceLabels)
	}
	if pods[0].Labels["app"] != "p" {
		t.Errorf("labels = %v", pods[0].Labels)
	}
}

func TestMetadata_LabelAllowlist(t *testing.T) {
	m := startMetadata(t, &config.Config{AllowedLabels: []string{"team"}},
		runningPod("a", "p", "n1", nil, map[string]string{"app": "p", "team": "core"}, nil, corev1.Container{Name: "c"}),
	)
	pods, err := m.Pods(context.Background())
	if err != nil || len(pods) != 1 {
		t.Fatalf("pods = %v, %v", pods, err)
	}
	if len(pods[0].Labels) != 1 || pods[0].Labels["team"] != "core" {
		t.Errorf("labels = %v", pods[0].Labels)
	}
}

// manyLabels returns n labels with short keys and values, named so that sorted order is index
// order.
func manyLabels(n int) map[string]string {
	m := make(map[string]string, n)
	for i := range n {
		m[fmt.Sprintf("example.com/l%03d", i)] = "v"
	}
	return m
}

func dropped(t *testing.T, mapName string) float64 {
	t.Helper()
	return testutil.ToFloat64(metrics.MapEntriesDropped.WithLabelValues(mapName))
}

func assertFitsTheAPI(t *testing.T, what string, m map[string]string) {
	t.Helper()
	if len(m) > v1.MaxMapEntries {
		t.Errorf("%s has %d entries, over the API's %d", what, len(m), v1.MaxMapEntries)
	}
	for k, v := range m {
		if len(utf16.Encode([]rune(k))) > v1.MaxMapKeyLength || len(utf16.Encode([]rune(v))) > v1.MaxMapValueLength {
			t.Errorf("%s entry %q is over the API's length caps", what, k)
		}
	}
}

func TestConvertNode_TrimsLabelsToTheAPICapsDeterministically(t *testing.T) {
	labels := manyLabels(200)
	tooLongKey := "example.com/" + strings.Repeat("k", v1.MaxMapKeyLength)
	labels[tooLongKey] = "v"
	tooLongValue := "example.com/a-too-long-value"
	labels[tooLongValue] = strings.Repeat("v", v1.MaxMapValueLength+1)
	// 1024 runes outside the Basic Multilingual Plane are 2048 UTF-16 code units: the API refuses it.
	astral := "example.com/a-astral-value"
	labels[astral] = strings.Repeat("\U0001F600", v1.MaxMapValueLength)
	// 1024 two-byte runes are 2048 bytes but 1024 code units: the API accepts it.
	accented := "example.com/a-accented-value"
	labels[accented] = strings.Repeat("é", v1.MaxMapValueLength)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:        "gpu-1",
		Labels:      labels,
		Annotations: map[string]string{v1.AnnotationVCPUHourlyRate: strings.Repeat("9", v1.MaxMapValueLength+1)},
	}}

	before, beforeAnnotations := dropped(t, "node_labels"), dropped(t, "node_annotations")
	got := ConvertNode(node)

	assertFitsTheAPI(t, "node labels", got.Labels)
	if len(got.Labels) != v1.MaxMapEntries {
		t.Fatalf("kept %d labels, want the first %d that fit", len(got.Labels), v1.MaxMapEntries)
	}
	if got.Labels[accented] == "" {
		t.Error("a value within the cap in UTF-16 code units was dropped for its byte length")
	}
	for _, k := range []string{tooLongKey, tooLongValue, astral} {
		if _, ok := got.Labels[k]; ok {
			t.Errorf("label %q over the length caps was kept", k)
		}
	}
	if _, ok := got.Labels["example.com/l126"]; !ok {
		t.Error("the 127th label in sorted order was dropped; trimming must keep the first that fit")
	}
	if _, ok := got.Labels["example.com/l127"]; ok {
		t.Error("a label past the first 128 that fit was kept")
	}
	if again := ConvertNode(node); !reflect.DeepEqual(again.Labels, got.Labels) {
		t.Error("trimming is not deterministic")
	}
	if d := dropped(t, "node_labels") - before; d != 2*(float64(len(labels))-v1.MaxMapEntries) {
		t.Errorf("node_labels drops = %v, want %d per conversion", d, len(labels)-v1.MaxMapEntries)
	}
	if got.Annotations != nil || dropped(t, "node_annotations")-beforeAnnotations != 2 {
		t.Errorf("annotations = %v, want the over-long rate dropped and counted", got.Annotations)
	}
}

func TestMetadata_PodsTrimLabelMapsToTheAPICaps(t *testing.T) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shop", Labels: manyLabels(200)}}
	pod := runningPod("shop", "web-1", "n1", nil, manyLabels(200), map[string]string{"owner": strings.Repeat("o", 50)},
		corev1.Container{Name: "web"})

	for _, tc := range []struct {
		name string
		cfg  *config.Config
	}{
		{"every label", &config.Config{AllowedAnnotations: []string{"owner"}, CollectNamespaceLabels: true}},
		{"allowlisted labels", &config.Config{AllowedLabels: slices.Collect(maps.Keys(manyLabels(200))), CollectNamespaceLabels: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			podBefore, nsBefore := dropped(t, "pod_labels"), dropped(t, "namespace_labels")
			m := startMetadata(t, tc.cfg, ns, pod)
			pods, err := m.Pods(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(pods) != 1 {
				t.Fatalf("pods = %d, want 1", len(pods))
			}
			got := pods[0]
			assertFitsTheAPI(t, "pod labels", got.Labels)
			assertFitsTheAPI(t, "namespace labels", got.NamespaceLabels)
			if len(got.Labels) != v1.MaxMapEntries || len(got.NamespaceLabels) != v1.MaxMapEntries {
				t.Fatalf("kept %d pod and %d namespace labels, want %d each", len(got.Labels), len(got.NamespaceLabels), v1.MaxMapEntries)
			}
			if d := dropped(t, "pod_labels") - podBefore; d != 72 {
				t.Errorf("pod_labels drops = %v, want 72", d)
			}
			if d := dropped(t, "namespace_labels") - nsBefore; d != 72 {
				t.Errorf("namespace_labels drops = %v, want 72", d)
			}
		})
	}
}

func TestNewClient_BuildsAClientOrReportsWhyNot(t *testing.T) {
	if c, err := NewClient(&rest.Config{Host: "https://10.0.0.1:443"}, &config.Config{}); err != nil || c == nil {
		t.Fatalf("NewClient = %v, %v; want a client", c, err)
	}
	if _, err := NewClient(&rest.Config{Host: "https://10.0.0.1:443", TLSClientConfig: rest.TLSClientConfig{CAFile: filepath.Join(t.TempDir(), "missing-ca.crt")}}, &config.Config{}); err == nil {
		t.Fatal("NewClient accepted an unreadable CA file")
	}
}

func TestMetadata_PodsCollectNativeSidecarsAndNoOtherInitContainers(t *testing.T) {
	always, never := corev1.ContainerRestartPolicyAlways, corev1.ContainerRestartPolicy("Never")
	requests := func(cpu, mem string) corev1.ResourceRequirements {
		return corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(mem)}}
	}
	app := corev1.Container{Name: "app", Resources: requests("500m", "256Mi")}
	sidecar := corev1.Container{Name: "mesh-proxy", RestartPolicy: &always, Resources: requests("100m", "64Mi")}
	migrate := corev1.Container{Name: "migrate", Resources: requests("1", "1Gi")}
	oneShot := corev1.Container{Name: "fetch", RestartPolicy: &never, Resources: requests("200m", "128Mi")}
	withInit := func(p *corev1.Pod, init ...corev1.Container) *corev1.Pod {
		p.Spec.InitContainers = init
		return p
	}
	appSpec := ContainerSpec{Name: "app", CPURequestCores: 0.5, MemoryRequestBytes: 256 << 20}
	sidecarSpec := ContainerSpec{Name: "mesh-proxy", CPURequestCores: 0.1, MemoryRequestBytes: 64 << 20}

	m := startMetadata(t, &config.Config{},
		runningPod("shop", "regular", "n1", nil, nil, nil, app),
		withInit(runningPod("shop", "sidecar", "n1", nil, nil, nil, app), sidecar),
		withInit(runningPod("shop", "plain-init", "n1", nil, nil, nil, app), migrate),
		withInit(runningPod("shop", "not-always", "n1", nil, nil, nil, app), oneShot),
		withInit(runningPod("shop", "both", "n1", nil, nil, nil, app), migrate, sidecar, oneShot),
	)
	want := map[string][]ContainerSpec{
		"regular":    {appSpec},
		"sidecar":    {appSpec, sidecarSpec},
		"plain-init": {appSpec},
		"not-always": {appSpec},
		"both":       {appSpec, sidecarSpec},
	}

	pods, err := m.Pods(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != len(want) {
		t.Fatalf("pods = %+v, want %d", pods, len(want))
	}
	usage := map[ContainerKey]ContainerUsage{}
	for _, p := range pods {
		if got := slices.SortedFunc(slices.Values(p.Specs), func(a, b ContainerSpec) int { return strings.Compare(a.Name, b.Name) }); !reflect.DeepEqual(got, want[p.Name]) {
			t.Errorf("pod %s specs = %+v, want %+v", p.Name, got, want[p.Name])
		}
		for _, c := range []string{"app", "mesh-proxy", "migrate", "fetch"} {
			usage[ContainerKey{p.Namespace, p.Name, c}] = ContainerUsage{CPUCores: 0.2, HasCPU: true, MemoryBytes: 32 << 20}
		}
	}

	a := NewAggregator("c", "0.2.0", "id", 10*time.Second, t0)
	a.Observe(obsAt(t0, []string{"n1"}, pods, usage))
	a.Observe(obsAt(t0.Add(10*time.Second), []string{"n1"}, pods, usage))
	r, ok := a.Finish(t0.Add(10 * time.Second))
	if !ok {
		t.Fatal("the window saw nothing")
	}
	for name, specs := range want {
		p := findPod(r, name, "n1")
		if p == nil {
			t.Errorf("pod %s missing from the report", name)
			continue
		}
		var got []string
		for _, c := range p.Containers {
			got = append(got, c.Name)
			if !near(c.ObservedSeconds, 10) || !near(c.CPUUsageCoreSeconds, 2) {
				t.Errorf("pod %s container %s = %+v, want 10s observed at 0.2 cores", name, c.Name, c)
			}
		}
		var names []string
		for _, s := range specs {
			names = append(names, s.Name)
		}
		if !slices.Equal(got, names) {
			t.Errorf("pod %s reports containers %v, want %v", name, got, names)
		}
	}
}
