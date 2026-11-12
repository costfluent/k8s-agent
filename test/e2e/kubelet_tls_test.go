//go:build e2e

package e2e

import (
	"encoding/json"
	"maps"
	"strings"
	"testing"
	"time"

	v1 "github.com/costfluent/k8s-agent/api/v1"
)

// twoNodes is a kind cluster of a control plane and a worker, so the count of nodes whose kubelet
// failed verification is a count and not a flag.
const twoNodes = `kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
`

// TestKindKubeletVerification installs with kubelet verification on, the chart's default, on kind,
// whose kubelets serve self-signed certificates. The agent cannot read any kubelet, and its first
// window must say so, so Costfluent can tell the customer to install with the flag instead of
// showing a cluster that never reports: no nodes, and every node counted under unscraped_nodes.tls.
func TestKindKubeletVerification(t *testing.T) {
	h := newHarness(t, 10*time.Minute)
	cluster := h.kindCluster("costfluent-e2e-tls", twoNodes)
	h.buildImages("receiver")
	h.run("kind", "load", "docker-image", "--name", cluster, receiverImage+":"+imageTag, agentImage+":"+imageTag)
	h.deployReceiver(0)
	nodes := len(strings.Fields(h.run("kubectl", "get", "nodes", "-o", "name")))
	if nodes != 2 {
		t.Fatalf("the cluster has %d nodes, want 2", nodes)
	}

	installed := h.installAgent("agent.disableKubeTLSverify=false")
	body := h.awaitFirstReport(installed)
	report, problem := decode(body)
	if problem != "" {
		t.Fatal(problem)
	}
	var raw struct {
		Nodes json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	if string(raw.Nodes) != "[]" {
		t.Errorf("the first window's nodes = %s, want []", raw.Nodes)
	}
	if want := map[string]int{v1.UnscrapedTLS: nodes}; !maps.Equal(report.UnscrapedNodes, want) {
		t.Errorf("the first window's unscraped_nodes = %v, want %v", report.UnscrapedNodes, want)
	}
	t.Logf("first window %s to %s after %s: unscraped_nodes %v, %d pods",
		report.WindowStart.Format(time.RFC3339), report.WindowEnd.Format(time.RFC3339),
		time.Since(installed).Round(time.Second), report.UnscrapedNodes, len(report.Pods))
}

// awaitFirstReport polls the receiver until it accepted a report, and returns the first one.
func (h *harness) awaitFirstReport(since time.Time) json.RawMessage {
	h.t.Helper()
	last := "no report"
	for time.Since(since) < reportWithin {
		time.Sleep(5 * time.Second)
		got, err := h.receiverReports()
		switch {
		case err != "":
			last = err
		case len(got.Reports) > 0:
			return got.Reports[0]
		}
	}
	h.t.Fatalf("no report within %s; last: %s", reportWithin, last)
	return nil
}
