//go:build load

package load

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	restwatch "k8s.io/client-go/rest/watch"

	v1 "github.com/costfluent/k8s-agent/api/v1"
)

// The cluster's shape. Every count is fixed, so two runs measure the same cluster.
const (
	nodeCount          = 1000
	daemonSetsPerNode  = 5
	statefulSets       = 100
	podsPerStatefulSet = 10
	jobPods            = 500
	replicaSets        = 3000
	churnPerGap        = 20
	namespaceCount     = 50

	daemonSetPods   = nodeCount * daemonSetsPerNode
	statefulSetPods = statefulSets * podsPerStatefulSet
	firstJobPod     = daemonSetPods + statefulSetPods
	firstRSPod      = firstJobPod + jobPods
)

var created = metav1.NewTime(time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC))

func namespaceName(i int) string {
	switch i {
	case 0:
		return "kube-system"
	case 1:
		return "monitoring"
	case 2:
		return "logging"
	}
	return fmt.Sprintf("team-%02d", i)
}

func nodeName(i int) string {
	return fmt.Sprintf("ip-10-%d-%d-%d.eu-central-1.compute.internal", i/250, i%250, i%7)
}

func nodeAddress(i int) string { return fmt.Sprintf("127.0.%d.%d", i/250+1, i%250+1) }

const loopback = "127.0.0.1"

func uid(kind string, i int) types.UID {
	return types.UID(fmt.Sprintf("%08x-%04x-4%03x-8%03x-%012x", i, len(kind), i%4096, i%4093, i*7919))
}

// fieldsV1 is a managedFields entry the size the API server records for a workload's pod.
func fieldsV1(containers []string) *metav1.FieldsV1 {
	var b strings.Builder
	b.WriteString(`{"f:metadata":{"f:generateName":{},"f:labels":{".":{},"f:app.kubernetes.io/instance":{},"f:app.kubernetes.io/managed-by":{},"f:app.kubernetes.io/name":{},"f:app.kubernetes.io/part-of":{},"f:app.kubernetes.io/version":{},"f:helm.sh/chart":{},"f:pod-template-hash":{},"f:team":{}},"f:ownerReferences":{".":{},"k:{\"uid\":\"0d3c5f8e\"}":{}}},"f:spec":{"f:containers":{`)
	for i, c := range containers {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `"k:{\"name\":\"%s\"}":{".":{},"f:env":{".":{},"k:{\"name\":\"LOG_LEVEL\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"POD_NAME\"}":{".":{},"f:name":{},"f:valueFrom":{".":{},"f:fieldRef":{}}}},"f:image":{},"f:imagePullPolicy":{},"f:livenessProbe":{".":{},"f:failureThreshold":{},"f:httpGet":{".":{},"f:path":{},"f:port":{},"f:scheme":{}},"f:periodSeconds":{}},"f:name":{},"f:ports":{".":{},"k:{\"containerPort\":8080,\"protocol\":\"TCP\"}":{".":{},"f:containerPort":{},"f:name":{},"f:protocol":{}}},"f:resources":{".":{},"f:limits":{".":{},"f:memory":{}},"f:requests":{".":{},"f:cpu":{},"f:memory":{}}},"f:securityContext":{".":{},"f:allowPrivilegeEscalation":{},"f:readOnlyRootFilesystem":{}},"f:volumeMounts":{".":{},"k:{\"mountPath\":\"/etc/config\"}":{".":{},"f:mountPath":{},"f:name":{}}}}`, c)
	}
	b.WriteString(`},"f:dnsPolicy":{},"f:enableServiceLinks":{},"f:restartPolicy":{},"f:schedulerName":{},"f:securityContext":{},"f:terminationGracePeriodSeconds":{},"f:volumes":{".":{},"k:{\"name\":\"config\"}":{".":{},"f:configMap":{".":{},"f:defaultMode":{},"f:name":{}},"f:name":{}}}}}`)
	return &metav1.FieldsV1{Raw: []byte(b.String())}
}

func statusFields() *metav1.FieldsV1 {
	return &metav1.FieldsV1{Raw: []byte(`{"f:status":{"f:conditions":{"k:{\"type\":\"ContainersReady\"}":{".":{},"f:lastProbeTime":{},"f:lastTransitionTime":{},"f:status":{},"f:type":{}},"k:{\"type\":\"Initialized\"}":{".":{},"f:lastProbeTime":{},"f:lastTransitionTime":{},"f:status":{},"f:type":{}},"k:{\"type\":\"PodReadyToStartContainers\"}":{".":{},"f:lastProbeTime":{},"f:lastTransitionTime":{},"f:status":{},"f:type":{}},"k:{\"type\":\"Ready\"}":{".":{},"f:lastProbeTime":{},"f:lastTransitionTime":{},"f:status":{},"f:type":{}}},"f:containerStatuses":{},"f:hostIP":{},"f:hostIPs":{},"f:phase":{},"f:podIP":{},"f:podIPs":{".":{},"k:{\"ip\":\"10.0.0.1\"}":{".":{},"f:ip":{}}},"f:startTime":{}}}`)}
}

type podShape struct {
	namespace, name, node string
	ownerKind, ownerName  string
	app                   string
	containers            []string
}

// shape places pod i: DaemonSet pods first, one of each per node, then StatefulSet, Job and
// ReplicaSet pods spread over the nodes.
func shape(i int) podShape {
	switch {
	case i < daemonSetPods:
		ds := []string{"kube-proxy", "aws-node", "ebs-csi-node", "node-exporter", "fluent-bit"}[i%daemonSetsPerNode]
		ns := map[string]int{"kube-proxy": 0, "aws-node": 0, "ebs-csi-node": 0, "node-exporter": 1, "fluent-bit": 2}[ds]
		containers := []string{ds}
		if ds == "ebs-csi-node" || ds == "aws-node" {
			containers = append(containers, "node-driver-registrar")
		}
		return podShape{namespaceName(ns), fmt.Sprintf("%s-%05x", ds, i/daemonSetsPerNode*31+7), nodeName(i / daemonSetsPerNode), "DaemonSet", ds, ds, containers}
	case i < firstJobPod:
		k := i - daemonSetPods
		sts := k / podsPerStatefulSet
		name := fmt.Sprintf("store-%03d", sts)
		return podShape{namespaceName(3 + sts%(namespaceCount-3)), fmt.Sprintf("%s-%d", name, k%podsPerStatefulSet), nodeName(i % nodeCount), "StatefulSet", name, name, []string{"db", "metrics"}}
	case i < firstRSPod:
		j := i - firstJobPod
		return jobShape(fmt.Sprintf("report-%04d-29%06d", j%50, j), j, i%nodeCount)
	default:
		k := i - firstRSPod
		rs := k % replicaSets
		app := fmt.Sprintf("svc-%04d", rs)
		containers := []string{"app"}
		if rs%10 < 3 {
			containers = append(containers, "istio-proxy")
		}
		return podShape{namespaceName(3 + rs%(namespaceCount-3)), fmt.Sprintf("%s-%08x-%05x", app, rs*2654435761%(1<<32), k/replicaSets), nodeName(i % nodeCount), "ReplicaSet", fmt.Sprintf("%s-%08x", app, rs*2654435761%(1<<32)), app, containers}
	}
}

func jobShape(job string, j, node int) podShape {
	return podShape{namespaceName(3 + j%(namespaceCount-3)), fmt.Sprintf("%s-%05x", job, j*13+5), nodeName(node), "Job", job, "report", []string{"job"}}
}

func pod(i int, s podShape) *corev1.Pod {
	yes, no := true, false
	grace := int64(30)
	labels := map[string]string{
		"app.kubernetes.io/name":       s.app,
		"app.kubernetes.io/instance":   s.app + "-prod",
		"app.kubernetes.io/version":    "1.24.3",
		"app.kubernetes.io/part-of":    "platform",
		"app.kubernetes.io/managed-by": "Helm",
		"helm.sh/chart":                s.app + "-4.12.0",
		"team":                         fmt.Sprintf("team-%02d", i%17),
	}
	switch s.ownerKind {
	case "ReplicaSet":
		labels["pod-template-hash"] = s.ownerName[len(s.ownerName)-8:]
	case "StatefulSet":
		labels["controller-revision-hash"] = s.ownerName + "-6d4f9c7b8"
		labels["statefulset.kubernetes.io/pod-name"] = s.name
	case "DaemonSet":
		labels["controller-revision-hash"] = "5f7c9d8b6"
		labels["pod-template-generation"] = "3"
	case "Job":
		labels["batch.kubernetes.io/job-name"] = s.ownerName
		labels["batch.kubernetes.io/controller-uid"] = string(uid("job", i))
	}
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              s.name,
			GenerateName:      s.ownerName + "-",
			Namespace:         s.namespace,
			UID:               uid("pod", i),
			ResourceVersion:   strconv.Itoa(100000 + i),
			CreationTimestamp: created,
			Labels:            labels,
			Annotations: map[string]string{
				"checksum/config":                   fmt.Sprintf("%064x", i*104729),
				"kubectl.kubernetes.io/restartedAt": "2026-09-01T08:00:00Z",
				"prometheus.io/scrape":              "true",
				"prometheus.io/port":                "9090",
				"prometheus.io/path":                "/metrics",
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: s.ownerKind, Name: s.ownerName, UID: uid(s.ownerKind, i), Controller: &yes, BlockOwnerDeletion: &yes}},
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "kube-controller-manager", Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: "v1", Time: &created, FieldsType: "FieldsV1", FieldsV1: fieldsV1(s.containers)},
				{Manager: "kubelet", Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: "v1", Time: &created, FieldsType: "FieldsV1", FieldsV1: statusFields(), Subresource: "status"},
			},
		},
		Spec: corev1.PodSpec{
			NodeName:                      s.node,
			ServiceAccountName:            s.app,
			RestartPolicy:                 corev1.RestartPolicyAlways,
			DNSPolicy:                     corev1.DNSClusterFirst,
			SchedulerName:                 "default-scheduler",
			TerminationGracePeriodSeconds: &grace,
			EnableServiceLinks:            &no,
			PriorityClassName:             "standard",
			Tolerations: []corev1.Toleration{
				{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
				{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
			},
			Volumes: []corev1.Volume{
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: s.app + "-config"}}}},
				{Name: "kube-api-access-" + s.name[len(s.name)-5:], VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
					{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
					{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
					{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
				}}}},
			},
		},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			QOSClass:  corev1.PodQOSBurstable,
			HostIP:    nodeAddress(i % nodeCount),
			PodIP:     fmt.Sprintf("10.%d.%d.%d", 100+i/65536, i/256%256, i%256),
			StartTime: &created,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReadyToStartContainers, Status: corev1.ConditionTrue, LastTransitionTime: created},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, LastTransitionTime: created},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: created},
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: created},
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: created},
			},
		},
	}
	p.Status.PodIPs = []corev1.PodIP{{IP: p.Status.PodIP}}
	for _, c := range s.containers {
		image := fmt.Sprintf("123456789012.dkr.ecr.eu-central-1.amazonaws.com/%s:1.24.3", c)
		p.Spec.Containers = append(p.Spec.Containers, corev1.Container{
			Name:            c,
			Image:           image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP}},
			Env: []corev1.EnvVar{
				{Name: "LOG_LEVEL", Value: "info"},
				{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: "http://otel-collector.monitoring:4317"},
				{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.name"}}},
				{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}},
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
			},
			VolumeMounts:             []corev1.VolumeMount{{Name: "config", MountPath: "/etc/config", ReadOnly: true}, {Name: "kube-api-access", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true}},
			LivenessProbe:            &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8080), Scheme: corev1.URISchemeHTTP}}, PeriodSeconds: 10, FailureThreshold: 3},
			SecurityContext:          &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes},
			TerminationMessagePath:   "/dev/termination-log",
			TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		})
		p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, corev1.ContainerStatus{
			Name:        c,
			Ready:       true,
			Started:     &yes,
			Image:       image,
			ImageID:     fmt.Sprintf("%s@sha256:%064x", image[:strings.LastIndex(image, ":")], len(c)*7919),
			ContainerID: fmt.Sprintf("containerd://%064x", i*31+len(c)),
			State:       corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: created}},
		})
	}
	return p
}

func node(i, port int) *corev1.Node {
	labels := map[string]string{
		"beta.kubernetes.io/arch":                       "amd64",
		"beta.kubernetes.io/instance-type":              "m6i.2xlarge",
		"beta.kubernetes.io/os":                         "linux",
		"eks.amazonaws.com/capacityType":                "ON_DEMAND",
		"eks.amazonaws.com/nodegroup":                   fmt.Sprintf("workers-%d", i%8),
		"eks.amazonaws.com/nodegroup-image":             "ami-0a1b2c3d4e5f67890",
		"eks.amazonaws.com/sourceLaunchTemplateId":      "lt-0123456789abcdef0",
		"eks.amazonaws.com/sourceLaunchTemplateVersion": "7",
		"failure-domain.beta.kubernetes.io/region":      "eu-central-1",
		"failure-domain.beta.kubernetes.io/zone":        fmt.Sprintf("eu-central-1%c", 'a'+i%3),
		"k8s.io/cloud-provider-aws":                     fmt.Sprintf("%032x", 4242),
		"kubernetes.io/arch":                            "amd64",
		"kubernetes.io/hostname":                        nodeName(i),
		"kubernetes.io/os":                              "linux",
		"node.kubernetes.io/instance-type":              "m6i.2xlarge",
		"topology.ebs.csi.aws.com/zone":                 fmt.Sprintf("eu-central-1%c", 'a'+i%3),
		"topology.k8s.aws/zone-id":                      fmt.Sprintf("euc1-az%d", 1+i%3),
		"topology.kubernetes.io/region":                 "eu-central-1",
		"topology.kubernetes.io/zone":                   fmt.Sprintf("eu-central-1%c", 'a'+i%3),
		"team":                                          fmt.Sprintf("team-%02d", i%17),
		"workload-class":                                "general",
	}
	if i%50 == 0 {
		for f := range 120 {
			labels[fmt.Sprintf("feature.node.kubernetes.io/cpu-cpuid.FEATURE%03d", f)] = "true"
		}
	}
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              nodeName(i),
			UID:               uid("node", i),
			ResourceVersion:   strconv.Itoa(50000 + i),
			CreationTimestamp: created,
			Labels:            labels,
			Annotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip":                   nodeAddress(i),
				"csi.volume.kubernetes.io/nodeid":                        fmt.Sprintf(`{"ebs.csi.aws.com":"i-%017x"}`, i),
				"node.alpha.kubernetes.io/ttl":                           "0",
				"volumes.kubernetes.io/controller-managed-attach-detach": "true",
			},
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "kubelet", Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: "v1", Time: &created, FieldsType: "FieldsV1", FieldsV1: statusFields(), Subresource: "status"},
			},
		},
		Spec: corev1.NodeSpec{ProviderID: fmt.Sprintf("aws:///eu-central-1%c/i-%017x", 'a'+i%3, i)},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("32386524Ki"),
				corev1.ResourcePods: resource.MustParse("58"), corev1.ResourceEphemeralStorage: resource.MustParse("104845292Ki"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("7910m"), corev1.ResourceMemory: resource.MustParse("31369692Ki"),
				corev1.ResourcePods: resource.MustParse("58"), corev1.ResourceEphemeralStorage: resource.MustParse("95551679124"),
			},
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: nodeAddress(i)},
				{Type: corev1.NodeHostName, Address: nodeName(i)},
				{Type: corev1.NodeInternalDNS, Address: nodeName(i)},
			},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: int32(port)}},
			NodeInfo: corev1.NodeSystemInfo{
				KernelVersion: "6.1.109-118.189.amzn2023.x86_64", OSImage: "Amazon Linux 2023.6.20241010", ContainerRuntimeVersion: "containerd://1.7.22",
				KubeletVersion: "v1.32.0-eks-7f9249a", KubeProxyVersion: "v1.32.0-eks-7f9249a", OperatingSystem: "linux", Architecture: "amd64",
				MachineID: fmt.Sprintf("%032x", i), SystemUUID: fmt.Sprintf("ec2%029x", i), BootID: fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i),
			},
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse, Reason: "KubeletHasSufficientMemory", Message: "kubelet has sufficient memory available"},
				{Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse, Reason: "KubeletHasNoDiskPressure", Message: "kubelet has no disk pressure"},
				{Type: corev1.NodePIDPressure, Status: corev1.ConditionFalse, Reason: "KubeletHasSufficientPID", Message: "kubelet has sufficient PID available"},
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady", Message: "kubelet is posting ready status"},
			},
		},
	}
	for img := range 40 {
		name := fmt.Sprintf("123456789012.dkr.ecr.eu-central-1.amazonaws.com/svc-%04d", (i+img*97)%replicaSets)
		n.Status.Images = append(n.Status.Images, corev1.ContainerImage{
			Names:     []string{fmt.Sprintf("%s@sha256:%064x", name, img*7+i), name + ":1.24.3"},
			SizeBytes: int64(50_000_000 + img*1_000_003),
		})
	}
	return n
}

func namespace(i int) *corev1.Namespace {
	name := namespaceName(i)
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid("ns", i), Labels: map[string]string{
		"kubernetes.io/metadata.name": name,
		"team":                        fmt.Sprintf("team-%02d", i%17),
		"cost-center":                 fmt.Sprintf("cc-%04d", 1000+i%9),
		"env":                         "production",
		"istio-injection":             "enabled",
	}}}
}

// fakeAPI serves the cluster as the API server would: whole lists, watches, and the owner GETs
// the agent resolves controllers with. Requests are counted, so a test can bound them.
type fakeAPI struct {
	t        *testing.T
	nodes    []int
	port     func(node int) int
	requests atomic.Int64
	ownerGET atomic.Int64

	mu       sync.Mutex
	rv       int
	churn    map[int]podShape
	watchers map[chan watchEvent]struct{}
}

type watchEvent struct {
	kind string
	pod  *corev1.Pod
	sent chan struct{}
}

func newFakeAPI(t *testing.T, port func(int) int) *fakeAPI {
	return &fakeAPI{t: t, port: port, rv: 1, churn: map[int]podShape{}, watchers: map[chan watchEvent]struct{}{}}
}

func basePods() int { return v1.MaxPods - 2*churnPerGap }

func (a *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c := negotiate(r)
	if q.Get("watch") == "true" {
		a.watch(w, r, c)
		return
	}
	a.requests.Add(1)
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case r.URL.Path == "/api/v1/nodes":
		a.list(w, r, c, "NodeList", func() int { return nodeCount }, func(i int) runtime.Object { return node(i, a.port(i)) },
			func(items []runtime.Object, meta metav1.ListMeta) runtime.Object {
				l := &corev1.NodeList{ListMeta: meta}
				for _, o := range items {
					l.Items = append(l.Items, *o.(*corev1.Node))
				}
				return l
			})
	case r.URL.Path == "/api/v1/pods":
		churn := a.churnPods()
		indices := slices.Sorted(maps.Keys(churn))
		a.list(w, r, c, "PodList", func() int { return basePods() + len(indices) }, func(i int) runtime.Object {
			if i < basePods() {
				return pod(i, shape(i))
			}
			j := indices[i-basePods()]
			return pod(j, churn[j])
		}, func(items []runtime.Object, meta metav1.ListMeta) runtime.Object {
			l := &corev1.PodList{ListMeta: meta}
			for _, o := range items {
				l.Items = append(l.Items, *o.(*corev1.Pod))
			}
			return l
		})
	case r.URL.Path == "/api/v1/namespaces":
		l := &corev1.NamespaceList{ListMeta: metav1.ListMeta{ResourceVersion: a.resourceVersion()}}
		for i := range namespaceCount {
			l.Items = append(l.Items, *namespace(i))
		}
		a.write(w, c, l, corev1.SchemeGroupVersion)
	case len(parts) == 7 && parts[0] == "apis" && parts[5] == "replicasets":
		a.ownerGET.Add(1)
		yes := true
		name := parts[6]
		a.write(w, c, &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: parts[4], OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: name[:strings.LastIndex(name, "-")], Controller: &yes}}},
		}, appsv1.SchemeGroupVersion)
	case len(parts) == 7 && parts[0] == "apis" && parts[5] == "jobs":
		a.ownerGET.Add(1)
		yes := true
		name := parts[6]
		a.write(w, c, &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: parts[4], OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "CronJob", Name: name[:strings.LastIndex(name, "-")], Controller: &yes}}},
		}, batchv1.SchemeGroupVersion)
	default:
		a.t.Errorf("fake API: unexpected request %s %s", r.Method, r.URL)
		http.NotFound(w, r)
	}
}

// negotiate answers in protobuf when the client accepts it, as the API server does for built-in
// types, and in JSON otherwise.
func negotiate(r *http.Request) runtime.SerializerInfo {
	mediaType := runtime.ContentTypeJSON
	if strings.Contains(r.Header.Get("Accept"), runtime.ContentTypeProtobuf) {
		mediaType = runtime.ContentTypeProtobuf
	}
	info, ok := runtime.SerializerInfoForMediaType(scheme.Codecs.SupportedMediaTypes(), mediaType)
	if !ok {
		panic("no serializer for " + mediaType)
	}
	return info
}

func (a *fakeAPI) resourceVersion() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return strconv.Itoa(a.rv)
}

func (a *fakeAPI) write(w http.ResponseWriter, c runtime.SerializerInfo, obj runtime.Object, gv schema.GroupVersion) {
	w.Header().Set("Content-Type", c.MediaType)
	if err := scheme.Codecs.EncoderForVersion(c.Serializer, gv).Encode(obj, w); err != nil {
		a.t.Errorf("encoding %T: %v", obj, err)
	}
}

// list serves a list the way the API server does. A request with a limit and a resource version
// other than "0" gets one page and a continue token; any other request gets the whole list, which
// is streamed item by item so the server holds one item at a time.
func (a *fakeAPI) list(w http.ResponseWriter, r *http.Request, c runtime.SerializerInfo, kind string, count func() int,
	item func(int) runtime.Object, build func([]runtime.Object, metav1.ListMeta) runtime.Object,
) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	rv := a.resourceVersion()
	n := count()
	if limit <= 0 || q.Get("resourceVersion") == "0" {
		a.stream(w, kind, rv, n, item)
		return
	}
	from, _ := strconv.Atoi(q.Get("continue"))
	to := min(n, from+limit)
	meta := metav1.ListMeta{ResourceVersion: rv}
	if to < n {
		remaining := int64(n - to)
		meta.Continue, meta.RemainingItemCount = strconv.Itoa(to), &remaining
	}
	items := make([]runtime.Object, 0, to-from)
	for i := from; i < to; i++ {
		items = append(items, item(i))
	}
	a.write(w, c, build(items, meta), corev1.SchemeGroupVersion)
}

func (a *fakeAPI) stream(w http.ResponseWriter, kind, rv string, n int, item func(int) runtime.Object) {
	w.Header().Set("Content-Type", "application/json")
	bw := bufio.NewWriterSize(w, 256<<10)
	fmt.Fprintf(bw, `{"kind":%q,"apiVersion":"v1","metadata":{"resourceVersion":%q},"items":[`, kind, rv)
	for i := range n {
		if i > 0 {
			_ = bw.WriteByte(',')
		}
		data, err := json.Marshal(item(i))
		if err != nil {
			a.t.Errorf("encoding %s item: %v", kind, err)
			return
		}
		_, _ = bw.Write(data)
	}
	_, _ = bw.WriteString("]}")
	_ = bw.Flush()
}

func (a *fakeAPI) watch(w http.ResponseWriter, r *http.Request, c runtime.SerializerInfo) {
	timeout := 5 * time.Minute
	if s, err := strconv.Atoi(r.URL.Query().Get("timeoutSeconds")); err == nil && s > 0 {
		timeout = time.Duration(s) * time.Second
	}
	stream := c.StreamSerializer
	w.Header().Set("Content-Type", c.MediaType+";stream=watch")
	if c.MediaType == runtime.ContentTypeJSON {
		w.Header().Set("Content-Type", c.MediaType)
	}
	w.WriteHeader(http.StatusOK)
	flusher := w.(http.Flusher)
	flusher.Flush()
	if r.URL.Path != "/api/v1/pods" {
		select {
		case <-r.Context().Done():
		case <-time.After(timeout):
		}
		return
	}
	encoder := restwatch.NewEncoder(streaming.NewEncoder(stream.Framer.NewFrameWriter(w), stream.Serializer),
		scheme.Codecs.EncoderForVersion(c.Serializer, corev1.SchemeGroupVersion))
	events := make(chan watchEvent, churnPerGap*2)
	a.mu.Lock()
	a.watchers[events] = struct{}{}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.watchers, events)
		a.mu.Unlock()
	}()
	deadline := time.After(timeout)
	for {
		select {
		case <-r.Context().Done():
			return
		case <-deadline:
			return
		case e := <-events:
			if err := encoder.Encode(&watch.Event{Type: watch.EventType(e.kind), Object: e.pod}); err != nil {
				a.t.Errorf("encoding watch event: %v", err)
				return
			}
			flusher.Flush()
			close(e.sent)
		}
	}
}

// churnJobs adds churnPerGap new Job pods and deletes the ones a previous call added, as finished
// CronJob runs do, and returns once the events are on the wire.
func (a *fakeAPI) churnJobs(round int, previous []int) []int {
	var added []int
	var events []watchEvent
	a.mu.Lock()
	for _, i := range previous {
		s := a.churn[i]
		delete(a.churn, i)
		a.rv++
		p := pod(i, s)
		p.ResourceVersion = strconv.Itoa(a.rv)
		events = append(events, watchEvent{kind: "DELETED", pod: p})
	}
	for k := range churnPerGap {
		i := basePods() + round*churnPerGap + k
		s := jobShape(fmt.Sprintf("churn-%02d-29%06d", round, k), i, i%nodeCount)
		a.churn[i] = s
		a.rv++
		p := pod(i, s)
		p.ResourceVersion = strconv.Itoa(a.rv)
		events = append(events, watchEvent{kind: "ADDED", pod: p})
		added = append(added, i)
	}
	watchers := make([]chan watchEvent, 0, len(a.watchers))
	for c := range a.watchers {
		watchers = append(watchers, c)
	}
	a.mu.Unlock()
	if len(watchers) == 0 {
		a.t.Fatal("no pod watch is open to deliver Job churn to")
	}
	for _, c := range watchers {
		for _, e := range events {
			e.sent = make(chan struct{})
			c <- e
			<-e.sent
		}
	}
	return added
}

func (a *fakeAPI) churnPods() map[int]podShape {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[int]podShape, len(a.churn))
	for k, v := range a.churn {
		out[k] = v
	}
	return out
}

// kubelets serves /metrics/resource for every node, each at its own node address, so the agent
// reaches each node's kubelet exactly as in a cluster. The listeners bind 127.0.0.1 and the agent's
// client dials there (dial), because macOS, unlike Linux, does not route the rest of 127.0.0.0/8 to
// the loopback interface.
type kubelets struct {
	server  *http.Server
	ports   []int
	latency time.Duration
	byAddr  map[string]int
	pods    [][]int
	api     *fakeAPI
	started time.Time
}

func startKubelets(t *testing.T, latency time.Duration) *kubelets {
	t.Helper()
	cert := httptest.NewUnstartedServer(nil)
	cert.StartTLS()
	tlsConfig := cert.TLS.Clone()
	cert.Close()

	k := &kubelets{latency: latency, byAddr: map[string]int{}, pods: make([][]int, nodeCount), ports: make([]int, nodeCount), started: time.Now()}
	for i := range basePods() {
		n := podNode(i)
		k.pods[n] = append(k.pods[n], i)
	}
	k.server = &http.Server{Handler: http.HandlerFunc(k.serve), TLSConfig: tlsConfig, ReadHeaderTimeout: 10 * time.Second}
	for i := range nodeCount {
		l, err := net.Listen("tcp", net.JoinHostPort(loopback, "0"))
		if err != nil {
			t.Fatalf("listening for node %d: %v", i, err)
		}
		k.ports[i] = l.Addr().(*net.TCPAddr).Port
		k.byAddr[net.JoinHostPort(nodeAddress(i), strconv.Itoa(k.ports[i]))] = i
		go func() { _ = k.server.Serve(tls.NewListener(l, tlsConfig)) }()
	}
	t.Cleanup(func() { _ = k.server.Close() })
	return k
}

func (k *kubelets) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	var d net.Dialer
	return d.DialContext(ctx, network, net.JoinHostPort(loopback, port))
}

func podNode(i int) int {
	if i < daemonSetPods {
		return i / daemonSetsPerNode
	}
	return i % nodeCount
}

func (k *kubelets) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/metrics/resource" {
		http.NotFound(w, r)
		return
	}
	n, ok := k.byAddr[r.Host]
	if !ok {
		http.Error(w, "unknown node "+r.Host, http.StatusNotFound)
		return
	}
	select {
	case <-time.After(k.latency):
	case <-r.Context().Done():
		return
	}
	type entry struct {
		s podShape
		i int
	}
	var entries []entry
	for _, i := range k.pods[n] {
		entries = append(entries, entry{shape(i), i})
	}
	for i, s := range k.api.churnPods() {
		if i%nodeCount == n {
			entries = append(entries, entry{s, i})
		}
	}
	now := time.Now()
	ts := now.UnixMilli()
	up := now.Sub(k.started).Seconds() + 3600
	bw := bufio.NewWriter(w)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	family := func(name, typ string, line func(e entry, c string) string) {
		fmt.Fprintf(bw, "# HELP %s %s\n# TYPE %s %s\n", name, name, name, typ)
		for _, e := range entries {
			for _, c := range e.s.containers {
				fmt.Fprintf(bw, "%s{container=%q,namespace=%q,pod=%q} %s\n", name, c, e.s.namespace, e.s.name, line(e, c))
			}
		}
	}
	family("container_cpu_usage_seconds_total", "counter", func(e entry, _ string) string {
		return fmt.Sprintf("%g %d", up*float64(1+e.i%40)/100, ts)
	})
	family("container_memory_working_set_bytes", "gauge", func(e entry, _ string) string {
		return fmt.Sprintf("%d %d", (64+e.i%300)<<20, ts)
	})
	family("container_start_time_seconds", "gauge", func(_ entry, _ string) string {
		return fmt.Sprintf("%d %d", created.Unix(), ts)
	})
	fmt.Fprintf(bw, "# HELP node_cpu_usage_seconds_total node_cpu_usage_seconds_total\n# TYPE node_cpu_usage_seconds_total counter\nnode_cpu_usage_seconds_total %g %d\n", up*3, ts)
	fmt.Fprintf(bw, "# HELP node_memory_working_set_bytes node_memory_working_set_bytes\n# TYPE node_memory_working_set_bytes gauge\nnode_memory_working_set_bytes %d %d\n", int64(12)<<30, ts)
	fmt.Fprintf(bw, "# HELP scrape_error scrape_error\n# TYPE scrape_error gauge\nscrape_error 0\n")
	_ = bw.Flush()
}

func serveAPI(t *testing.T, a *fakeAPI) string {
	t.Helper()
	srv := httptest.NewServer(a)
	t.Cleanup(srv.Close)
	return srv.URL
}
