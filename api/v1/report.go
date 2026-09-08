package v1

import "time"

// MetricsReport is the payload sent to Costfluent API
type MetricsReport struct {
	ClusterID    string       `json:"cluster_id"`
	ClusterName  string       `json:"cluster_name"`
	ReportStart  time.Time    `json:"report_start"`
	ReportEnd    time.Time    `json:"report_end"`
	AgentVersion string       `json:"agent_version"`
	Nodes        []Node       `json:"nodes"`
	PodMetrics   []PodMetrics `json:"pod_metrics"`
}

// Node represents a Kubernetes node with pricing info
type Node struct {
	Name                   string            `json:"name"`
	InstanceType           string            `json:"instance_type,omitempty"`
	Region                 string            `json:"region,omitempty"`
	Zone                   string            `json:"zone,omitempty"`
	CapacityCpuCores       float64           `json:"capacity_cpu_cores"`
	CapacityMemoryBytes    int64             `json:"capacity_memory_bytes"`
	AllocatableCpuCores    float64           `json:"allocatable_cpu_cores,omitempty"`
	AllocatableMemoryBytes int64             `json:"allocatable_memory_bytes,omitempty"`
	CapacityGpu            int               `json:"capacity_gpu"`
	IsSpotInstance         bool              `json:"is_spot_instance"`
	ProviderID             string            `json:"provider_id,omitempty"`
	Labels                 map[string]string `json:"labels,omitempty"`
	Annotations            map[string]string `json:"annotations,omitempty"`
}

// PodMetrics contains metrics for a single pod
type PodMetrics struct {
	Namespace      string            `json:"namespace"`
	PodName        string            `json:"pod_name"`
	ControllerName string            `json:"controller_name,omitempty"`
	ControllerKind string            `json:"controller_kind,omitempty"`
	NodeName       string            `json:"node_name"`
	Labels         map[string]string `json:"labels,omitempty"`
	Annotations    map[string]string `json:"annotations,omitempty"`
	Containers     []Container       `json:"containers"`
	StartTime      time.Time         `json:"start_time"`
	Phase          string            `json:"phase"`
}

// Container represents container resource usage
type Container struct {
	Name               string            `json:"name"`
	CpuRequestCores    float64           `json:"cpu_request_cores"`
	CpuLimitCores      float64           `json:"cpu_limit_cores"`
	MemoryRequestBytes int64             `json:"memory_request_bytes"`
	MemoryLimitBytes   int64             `json:"memory_limit_bytes"`
	Samples            []ContainerSample `json:"samples"`
}

// ContainerSample is a single metrics sample
type ContainerSample struct {
	Timestamp        time.Time `json:"timestamp"`
	CpuUsageCores    float64   `json:"cpu_usage_cores"`
	MemoryUsageBytes int64     `json:"memory_usage_bytes"`
}

// Heartbeat is the lightweight health check payload
type Heartbeat struct {
	ClusterID    string `json:"cluster_id"`
	AgentVersion string `json:"agent_version"`
	NodeCount    int    `json:"node_count"`
	PodCount     int    `json:"pod_count"`
}

// HeartbeatResponse from API
type HeartbeatResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// ReportResponse from API
type ReportResponse struct {
	Status    string `json:"status"`
	ReportID  string `json:"report_id,omitempty"`
	Message   string `json:"message,omitempty"`
	NextRetry int    `json:"next_retry_seconds,omitempty"`
}
