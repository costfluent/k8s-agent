// Package v1 is the wire contract between the agent and
// POST {endpoint}/v1/kubernetes/reports. testdata/report.json is the golden
// report; the Costfluent backend binds the same file in its contract tests, so
// a change here is a change to both sides.
package v1

import "time"

// SchemaVersion is the value every report carries in schema_version.
const SchemaVersion = 1

// Node rate annotations. They are the only node annotations the agent sends.
const (
	AnnotationVCPUHourlyRate      = "costfluent.com/vcpu-hourly-rate"
	AnnotationRAMGBHourlyRate     = "costfluent.com/ram-gb-hourly-rate"
	AnnotationGPUHourlyRate       = "costfluent.com/gpu-hourly-rate"
	AnnotationStorageGBHourlyRate = "costfluent.com/storage-gb-hourly-rate"
)

// RateAnnotations lists the node annotations the agent forwards.
var RateAnnotations = []string{
	AnnotationVCPUHourlyRate,
	AnnotationRAMGBHourlyRate,
	AnnotationGPUHourlyRate,
	AnnotationStorageGBHourlyRate,
}

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
}

// Node is a node seen during the window.
type Node struct {
	Name                   string            `json:"name"`
	ProviderID             string            `json:"provider_id,omitempty"`
	InstanceType           string            `json:"instance_type,omitempty"`
	Region                 string            `json:"region,omitempty"`
	Zone                   string            `json:"zone,omitempty"`
	Spot                   bool              `json:"spot"`
	ObservedSeconds        float64           `json:"observed_seconds"`
	CapacityCPUCores       float64           `json:"capacity_cpu_cores"`
	CapacityMemoryBytes    int64             `json:"capacity_memory_bytes"`
	CapacityGPU            int               `json:"capacity_gpu"`
	AllocatableCPUCores    float64           `json:"allocatable_cpu_cores"`
	AllocatableMemoryBytes int64             `json:"allocatable_memory_bytes"`
	Labels                 map[string]string `json:"labels,omitempty"`
	Annotations            map[string]string `json:"annotations,omitempty"`
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
}

// Container carries seconds-weighted sums over the window. Allocated is
// max(request, usage) integrated over the observed seconds.
type Container struct {
	Name                       string  `json:"name"`
	ObservedSeconds            float64 `json:"observed_seconds"`
	CPURequestCores            float64 `json:"cpu_request_cores"`
	MemoryRequestBytes         int64   `json:"memory_request_bytes"`
	GPURequest                 int     `json:"gpu_request"`
	CPUUsageCoreSeconds        float64 `json:"cpu_usage_core_seconds"`
	MemoryUsageByteSeconds     float64 `json:"memory_usage_byte_seconds"`
	CPUAllocatedCoreSeconds    float64 `json:"cpu_allocated_core_seconds"`
	MemoryAllocatedByteSeconds float64 `json:"memory_allocated_byte_seconds"`
	CPUUsagePeakCores          float64 `json:"cpu_usage_peak_cores"`
	MemoryUsagePeakBytes       int64   `json:"memory_usage_peak_bytes"`
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
}
