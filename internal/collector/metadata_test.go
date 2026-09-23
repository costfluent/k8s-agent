package collector

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/config"
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
