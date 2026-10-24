package v1

import "fmt"

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
)

// CheckSize returns an error naming the first count cap r exceeds. The API refuses such a report
// whole, and no agent version changes that.
func (r Report) CheckSize() error {
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
