package collector

import (
	"slices"
	"unique"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/costfluent/k8s-agent/internal/config"
)

// cachedPod is all the agent keeps of a pod between polls: about a tenth of a decoded Pod. Its
// maps, strings and containers are shared with the observations and reports built from it, so
// it is never modified once stored.
type cachedPod struct {
	Namespace, Name, Node, Phase string
	OwnerKind, OwnerName         string
	Labels, Annotations          map[string]string
	Containers                   []ContainerSpec
}

func (p *cachedPod) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

func (p *cachedPod) DeepCopyObject() runtime.Object {
	c := *p
	return &c
}

// GetObjectMeta gives the informer the namespace and name it keys the pod by.
func (p *cachedPod) GetObjectMeta() metav1.Object {
	return &metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name}
}

// cachedPodList is a page-built pod list the reflector stores item by item.
type cachedPodList struct {
	metav1.ListMeta
	Items []*cachedPod
}

func (l *cachedPodList) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

func (l *cachedPodList) DeepCopyObject() runtime.Object {
	c := *l
	c.Items = slices.Clone(l.Items)
	return &c
}

// intern returns the one shared copy of s, so the label keys and values, namespaces and node names
// thousands of pods repeat are held once. Copies nothing references any more are collected.
func intern(s string) string { return unique.Make(s).Value() }

// cachePod keeps what the agent reads of p: where it runs, its phase, its controller, the labels
// and annotations it may send, and its containers' requests.
func (m *Metadata) cachePod(p *corev1.Pod) *cachedPod {
	c := &cachedPod{
		Namespace:   intern(p.Namespace),
		Name:        p.Name,
		Node:        intern(p.Spec.NodeName),
		Phase:       intern(string(p.Status.Phase)),
		Labels:      m.cachedLabels(p.Labels),
		Annotations: cachedAnnotations(p.Annotations, m.cfg.AllowedAnnotations),
	}
	ref := metav1.GetControllerOf(p)
	if ref == nil && len(p.OwnerReferences) > 0 {
		ref = &p.OwnerReferences[0]
	}
	if ref != nil {
		c.OwnerKind, c.OwnerName = intern(ref.Kind), intern(ref.Name)
	}
	n := len(p.Spec.Containers)
	for i := range p.Spec.InitContainers {
		if isSidecar(&p.Spec.InitContainers[i]) {
			n++
		}
	}
	if n > 0 {
		c.Containers = make([]ContainerSpec, 0, n)
	}
	for i := range p.Spec.InitContainers {
		if isSidecar(&p.Spec.InitContainers[i]) {
			c.Containers = append(c.Containers, containerSpec(&p.Spec.InitContainers[i]))
		}
	}
	for i := range p.Spec.Containers {
		c.Containers = append(c.Containers, containerSpec(&p.Spec.Containers[i]))
	}
	return c
}

// isSidecar reports a native sidecar: an init container that keeps running beside the pod's
// containers for the pod's whole life. Other init containers finish before the pod is Running.
func isSidecar(ct *corev1.Container) bool {
	return ct.RestartPolicy != nil && *ct.RestartPolicy == corev1.ContainerRestartPolicyAlways
}

func containerSpec(ct *corev1.Container) ContainerSpec {
	return ContainerSpec{
		Name:               intern(ct.Name),
		CPURequestCores:    cores(ct.Resources.Requests.Cpu()),
		MemoryRequestBytes: bytesOf(ct.Resources.Requests.Memory()),
		GPURequest:         gpuRequest(ct.Resources),
	}
}

// cachedLabels are the pod labels the agent may send: the allowlisted ones, or every label but the
// revision and ordinal ones.
func (m *Metadata) cachedLabels(l map[string]string) map[string]string {
	var out map[string]string
	put := func(k, v string) {
		if out == nil {
			out = make(map[string]string, len(l))
		}
		out[intern(k)] = intern(v)
	}
	if len(m.cfg.AllowedLabels) > 0 {
		for _, k := range m.cfg.AllowedLabels {
			if v, ok := l[k]; ok {
				put(k, v)
			}
		}
		return out
	}
	for k, v := range l {
		if !noisePodLabels[k] {
			put(k, v)
		}
	}
	return out
}

func cachedAnnotations(a map[string]string, allowed []string) map[string]string {
	picked := pick(a, allowed, config.MaxAnnotationValueChars)
	for k, v := range picked {
		picked[k] = intern(v)
	}
	return picked
}
