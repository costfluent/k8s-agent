package v1

import (
	"fmt"
	"slices"
)

// The API's caps on one report. testdata/report-at-limits.json.gz sits exactly at them, and the
// backend's contract tests accept it, so the two sides cannot drift apart. Lengths count UTF-16
// code units, as the API does.
const (
	MaxNodes            = 1000
	MaxPods             = 50000
	MaxContainersPerPod = 100
	MaxMapEntries       = 128
	MaxMapKeyLength     = 317
	MaxMapValueLength   = 1024
	// MaxGPUMapEntries caps gpu_resources, gpu_labels and gpu_requests.
	MaxGPUMapEntries = 16
	// MaxGPUResourceNameLength caps a key of gpu_resources and gpu_requests: a family's
	// nvidia.com/ prefix and a 63-character name, the longest a Kubernetes resource name has.
	MaxGPUResourceNameLength = 74
	// MaxGPUUnits caps a count in gpu_resources and gpu_requests, far above any node's devices and
	// replicas.
	MaxGPUUnits = 16384
	// MaxVolumes caps volumes: a claim for every pod at MaxPods. Past it the agent sends the first by
	// name and counts the rest, so storage never holds back a window's compute.
	MaxVolumes = 50000
	// MaxClaimsPerPod caps a pod's claims; past it a pod keeps its first by name.
	MaxClaimsPerPod = 32
	// MaxNameLength caps a name: a volume's, a claim's, a storage class's, a CSI driver's.
	MaxNameLength = 253
	// MaxVolumeHandleLength caps a volume handle, as the API caps a node's provider id.
	MaxVolumeHandleLength = 1024
)

// The API's caps on one report body, on the wire and once inflated: it refuses a report over
// either with 413. No golden report reaches them; the load test sends a report at the count caps
// above against them.
const (
	// MaxCompressedBytes caps the gzip body as sent.
	MaxCompressedBytes = 16 << 20
	// MaxDecompressedBytes caps the JSON the body inflates to.
	MaxDecompressedBytes = 128 << 20
)

// CheckSize returns an error naming the first count cap r exceeds. The API refuses such a report
// whole, and no agent version changes that.
func (r Report) CheckSize() error {
	if err := checkUnscraped(r.UnscrapedNodes); err != nil {
		return err
	}
	if len(r.Nodes) > MaxNodes {
		return fmt.Errorf("%d nodes; the API accepts at most %d", len(r.Nodes), MaxNodes)
	}
	if len(r.Pods) > MaxPods {
		return fmt.Errorf("%d pods; the API accepts at most %d", len(r.Pods), MaxPods)
	}
	for _, p := range r.Pods {
		if len(p.Containers) > MaxContainersPerPod {
			return fmt.Errorf("pod %s/%s has %d containers; the API accepts at most %d", p.Namespace, p.Name, len(p.Containers), MaxContainersPerPod)
		}
	}
	return nil
}

// checkUnscraped holds unscraped_nodes to the API's bounds: only the reasons UnscrapedReasons
// names, and no count, nor their sum, below 0 or above MaxNodes.
func checkUnscraped(unscraped map[string]int) error {
	sum := 0
	for reason, n := range unscraped {
		if !slices.Contains(UnscrapedReasons, reason) {
			return fmt.Errorf("unscraped_nodes reason %q; the API accepts only %v", reason, UnscrapedReasons)
		}
		if n < 0 || n > MaxNodes {
			return fmt.Errorf("unscraped_nodes %s is %d; the API accepts 0 to %d", reason, n, MaxNodes)
		}
		sum += n
	}
	if sum > MaxNodes {
		return fmt.Errorf("%d unscraped nodes; the API accepts at most %d", sum, MaxNodes)
	}
	return nil
}
