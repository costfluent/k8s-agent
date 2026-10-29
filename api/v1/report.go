// Package v1 is the wire contract between the agent and
// POST {endpoint}/v1/kubernetes/reports. testdata/report.json is the golden
// report; the Costfluent backend binds the same file in its contract tests, so
// a change here is a change to both sides.
package v1

import (
	"strings"
	"time"
)

// SchemaVersion is the value every report carries in schema_version.
const SchemaVersion = 1

// Node annotations a cluster owner sets for Costfluent. They are the only node annotations the
// agent sends: three hourly rates for a node no cloud bills, and the physical GPUs of a node whose
// GPU labels do not say.
const (
	AnnotationVCPUHourlyRate   = "costfluent.com/vcpu-hourly-rate"
	AnnotationRAMGBHourlyRate  = "costfluent.com/ram-gb-hourly-rate"
	AnnotationGPUHourlyRate    = "costfluent.com/gpu-hourly-rate"
	AnnotationPhysicalGPUCount = "costfluent.com/physical-gpu-count"
)

// AnnotationStorageGBHourlyRate is the one storage class annotation the agent sends: the hourly
// rate per GiB of a volume of that class, for a volume no cloud bills by its id.
const AnnotationStorageGBHourlyRate = "costfluent.com/storage-gb-hourly-rate"

// ForwardedAnnotations lists the node annotations the agent forwards.
var ForwardedAnnotations = []string{
	AnnotationVCPUHourlyRate,
	AnnotationRAMGBHourlyRate,
	AnnotationGPUHourlyRate,
	AnnotationPhysicalGPUCount,
}

// GPU resource families. A node's capacity and a container's request in each are sent in
// gpu_resources and gpu_requests; ResourceNVIDIAMIGPrefix matches every mixed-strategy MIG profile,
// nvidia.com/mig-<n>g.<m>gb.
const (
	ResourceNVIDIAGPU       = "nvidia.com/gpu"
	ResourceNVIDIAGPUShared = "nvidia.com/gpu.shared"
	ResourceAMDGPU          = "amd.com/gpu"
	ResourceNVIDIAMIGPrefix = "nvidia.com/mig-"
)

// IsGPUResource reports whether name is a resource of one of the GPU families.
func IsGPUResource(name string) bool {
	switch name {
	case ResourceNVIDIAGPU, ResourceNVIDIAGPUShared, ResourceAMDGPU:
		return true
	}
	return strings.HasPrefix(name, ResourceNVIDIAMIGPrefix) && len(name) > len(ResourceNVIDIAMIGPrefix)
}

// GPU Feature Discovery's node labels the agent sends in gpu_labels, read before any label cap:
// they say how a node's GPU devices map onto physical GPUs.
const (
	LabelMIGStrategy = "nvidia.com/mig.strategy"
	LabelGPUProduct  = "nvidia.com/gpu.product"
	LabelGPUCount    = "nvidia.com/gpu.count"
	LabelGPUReplicas = "nvidia.com/gpu.replicas"
	LabelGPUSlicesGI = "nvidia.com/gpu.slices.gi"
)

// GPULabels lists the node labels the agent sends in gpu_labels.
var GPULabels = []string{LabelMIGStrategy, LabelGPUProduct, LabelGPUCount, LabelGPUReplicas, LabelGPUSlicesGI}

// Report is one aggregated window. Windows are UTC clock hours, except the
// first window after start, which closes early; a window never crosses
// midnight UTC.
type Report struct {
	SchemaVersion   int       `json:"schema_version"`
	ClusterID       string    `json:"cluster_id"`
	AgentVersion    string    `json:"agent_version"`
	AgentInstanceID string    `json:"agent_instance_id"`
	WindowStart     time.Time `json:"window_start"`
	WindowEnd       time.Time `json:"window_end"`
	Nodes           []Node    `json:"nodes"`
	Pods            []Pod     `json:"pods"`
	// Volumes is every persistent volume seen during the window, in any phase; absent when the
	// agent could not read storage, or the cluster has none.
	Volumes []Volume `json:"volumes,omitempty"`
}

// Node is a node seen during the window.
type Node struct {
	Name                string  `json:"name"`
	ProviderID          string  `json:"provider_id,omitempty"`
	InstanceType        string  `json:"instance_type,omitempty"`
	Region              string  `json:"region,omitempty"`
	Zone                string  `json:"zone,omitempty"`
	Spot                bool    `json:"spot"`
	ObservedSeconds     float64 `json:"observed_seconds"`
	CapacityCPUCores    float64 `json:"capacity_cpu_cores"`
	CapacityMemoryBytes int64   `json:"capacity_memory_bytes"`
	// CapacityGPU is the nvidia.com/gpu capacity alone, as servers before gpu_resources read it.
	CapacityGPU int `json:"capacity_gpu"`
	// GPUResources is the capacity of each GPU family resource the node advertises.
	GPUResources           map[string]int    `json:"gpu_resources,omitempty"`
	AllocatableCPUCores    float64           `json:"allocatable_cpu_cores"`
	AllocatableMemoryBytes int64             `json:"allocatable_memory_bytes"`
	Labels                 map[string]string `json:"labels,omitempty"`
	Annotations            map[string]string `json:"annotations,omitempty"`
	// GPULabels holds those of GPULabels the node carries.
	GPULabels map[string]string `json:"gpu_labels,omitempty"`
}

// Pod is a pod seen during the window. A pod that moved node within one window
// appears once per node.
type Pod struct {
	Namespace       string            `json:"namespace"`
	Name            string            `json:"name"`
	Node            string            `json:"node"`
	ControllerKind  string            `json:"controller_kind,omitempty"`
	Controller      string            `json:"controller,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Annotations     map[string]string `json:"annotations,omitempty"`
	NamespaceLabels map[string]string `json:"namespace_labels,omitempty"`
	Containers      []Container       `json:"containers"`
	// Claims names the persistent volume claims the pod mounts, in its namespace, a generic
	// ephemeral volume's as <pod>-<volume>; the first MaxClaimsPerPod in name order.
	Claims []string `json:"claims,omitempty"`
}

// Container carries seconds-weighted sums over the window. Allocated is
// max(request, usage) integrated over the observed seconds.
type Container struct {
	Name               string  `json:"name"`
	ObservedSeconds    float64 `json:"observed_seconds"`
	CPURequestCores    float64 `json:"cpu_request_cores"`
	MemoryRequestBytes int64   `json:"memory_request_bytes"`
	// GPURequest is the nvidia.com/gpu request alone, falling back to its limit, as servers before
	// gpu_requests read it.
	GPURequest int `json:"gpu_request"`
	// GPURequests is the request of each GPU family resource, falling back to its limit; absent
	// when the container asks for no GPU.
	GPURequests                map[string]int `json:"gpu_requests,omitempty"`
	CPUUsageCoreSeconds        float64        `json:"cpu_usage_core_seconds"`
	MemoryUsageByteSeconds     float64        `json:"memory_usage_byte_seconds"`
	CPUAllocatedCoreSeconds    float64        `json:"cpu_allocated_core_seconds"`
	MemoryAllocatedByteSeconds float64        `json:"memory_allocated_byte_seconds"`
	CPUUsagePeakCores          float64        `json:"cpu_usage_peak_cores"`
	MemoryUsagePeakBytes       int64          `json:"memory_usage_peak_bytes"`
}

// Volume is a persistent volume seen during the window.
type Volume struct {
	Name          string `json:"name"`
	CapacityBytes int64  `json:"capacity_bytes"`
	StorageClass  string `json:"storage_class,omitempty"`
	// CSIDriver is the CSI driver's name, or for an in-tree source the plugin's:
	// kubernetes.io/aws-ebs, kubernetes.io/azure-disk or kubernetes.io/gce-pd.
	CSIDriver string `json:"csi_driver,omitempty"`
	// VolumeHandle is the CSI volume handle, or the in-tree source's volume id, disk URI or disk name.
	VolumeHandle string `json:"volume_handle,omitempty"`
	// Phase is the volume's phase when last seen: Pending, Available, Bound, Released or Failed.
	Phase string `json:"phase"`
	// ClaimNamespace and ClaimName name the claim a Bound volume serves; empty in any other phase.
	ClaimNamespace  string  `json:"claim_namespace,omitempty"`
	ClaimName       string  `json:"claim_name,omitempty"`
	ObservedSeconds float64 `json:"observed_seconds"`
	// StorageClassAnnotations holds the storage class's AnnotationStorageGBHourlyRate, if it has one.
	StorageClassAnnotations map[string]string `json:"storage_class_annotations,omitempty"`
	// ClaimLabels holds the claim's labels, only when the agent is set to collect them.
	ClaimLabels map[string]string `json:"claim_labels,omitempty"`
}

// ReportAccepted is the 202 response body.
type ReportAccepted struct {
	ID        string `json:"id"`
	ClusterID string `json:"cluster_id"`
	Status    string `json:"status"`
}

// Problem is the error body the API returns on a refusal.
type Problem struct {
	Title  string `json:"title,omitempty"`
	Detail string `json:"detail,omitempty"`
	Status int    `json:"status,omitempty"`
	// Errors names each refused field and why.
	Errors []ProblemError `json:"errors,omitempty"`
}

// ProblemError is one refused field of a Problem.
type ProblemError struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}
