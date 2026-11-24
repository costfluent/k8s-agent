//go:build e2e

package e2e

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// allowPolicy is the name the Kubernetes connect guide gives the agent's egress policy.
const allowPolicy = "costfluent-agent-egress"

const (
	// startWithin bounds the time from a clock-hour guard to the agent's next pod starting: an
	// install, or a pod delete that waits out the chart's 30-second grace period.
	startWithin = time.Minute
	// reportIdle is the report client's idle timeout, net/http's default, which it keeps.
	reportIdle = 90 * time.Second
)

// defaultDenyEgress denies every pod in the agent's namespace all egress, as a cluster that denies
// egress by default does.
const defaultDenyEgress = `apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-egress
  namespace: costfluent
spec:
  podSelector: {}
  policyTypes:
    - Egress
`

// agentEgress is the policy the Kubernetes connect guide gives under Default-deny egress for a
// release reporting through a proxy, its example values replaced by the policy's name, the release,
// the API server's endpoint and port, the node network and the proxy's address. A change to the
// guide's policy changes this copy.
const agentEgress = `apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: %[1]s
  namespace: costfluent
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/instance: %[2]s
  policyTypes:
    - Egress
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
          podSelector:
            matchLabels:
              k8s-app: kube-dns
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
    - to:
        - ipBlock:
            cidr: %[3]s/32
      ports:
        - protocol: TCP
          port: %[4]d
    - to:
        - ipBlock:
            cidr: %[5]s
      ports:
        - protocol: TCP
          port: 10250
    - to:
        - ipBlock:
            cidr: %[6]s/32
      ports:
        - protocol: TCP
          port: 3128
`

// TestKindDefaultDenyEgress installs through the report proxy into a namespace that denies egress
// by default, under the guide's policy for the agent, on a control plane and a worker, so the agent
// reaches the API server and a kubelet across nodes. The agent must report every node and log no
// failed flow: a flow the policy does not name is dropped, and the agent logs each one at warn,
// even where a retry through the proxy still delivers the report. Then, with the guide's policy
// removed and the agent's pod replaced, no report may arrive in the time a fresh start is given:
// kindnet enforces NetworkPolicy, and without that the first half proves nothing.
//
// Under kindnet, a flow the pod opens in its first second passes the default-deny: with kind
// v0.33.0 that was the start's /v1/me through the proxy, the API server list and both kubelet
// scrapes. A connection opened then also carries later requests unchecked while it stays open: the
// watches, and the kubelet connections each poll reuses. So a flow made only at start, such as an
// instance metadata call, or a new kubelet port, passes here. The leg holds every connection opened
// later: each DNS lookup, and each report, which dials the proxy afresh because the first window,
// two minutes unless the clock hour ends it sooner (which the leg keeps clear of), outlasts the
// report client's 90-second idle timeout.
func TestKindDefaultDenyEgress(t *testing.T) {
	h := newHarness(t, 14*time.Minute)
	cluster := h.kindCluster("costfluent-e2e-netpol", twoNodes)
	h.buildImages("receiver", "proxy")
	h.run("kind", "load", "docker-image", "--name", cluster,
		receiverImage+":"+imageTag, proxyImage+":"+imageTag, agentImage+":"+imageTag)
	h.deployReceiver(0)

	secret := make([]byte, 16)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	h.apply(proxyWorkload(hex.EncodeToString(secret)))
	h.run("kubectl", "-n", "e2e", "rollout", "status", "deployment/proxy", "--timeout=120s")

	apiServer, apiPort := h.apiServerEndpoint()
	nodeNetwork := h.nodeNetwork()
	proxyIP := h.podIP("e2e", "app=proxy")
	h.apply(defaultDenyEgress)
	h.apply(fmt.Sprintf(agentEgress, allowPolicy, release, apiServer, apiPort, nodeNetwork, proxyIP))
	t.Logf("egress allowed to the API server at %s:%d, kubelets in %s and the proxy at %s:3128",
		apiServer, apiPort, nodeNetwork, proxyIP)

	h.clearOfHour()
	// kind kubelets serve self-signed certificates.
	installed := h.installAgent("agent.disableKubeTLSverify=true",
		"agent.reportHTTPProxySecret.name="+proxySecret,
		"agent.reportHTTPProxySecret.key=url")
	report := h.awaitReport(installed)
	t.Logf("report after %s: window %s to %s, %d nodes, %d pods", time.Since(installed).Round(time.Second),
		report.WindowStart.Format(time.RFC3339), report.WindowEnd.Format(time.RFC3339), len(report.Nodes), len(report.Pods))
	if nodes := len(strings.Fields(h.run("kubectl", "get", "nodes", "-o", "name"))); len(report.Nodes) != nodes || len(report.UnscrapedNodes) > 0 {
		t.Errorf("the report counts %d of %d nodes, unscraped_nodes %v", len(report.Nodes), nodes, report.UnscrapedNodes)
	}
	if restarts := h.run("kubectl", "-n", "costfluent", "get", "pod", agentPod,
		"-o", "jsonpath={.status.containerStatuses[*].restartCount}"); strings.TrimSpace(restarts) != "0" {
		t.Errorf("the agent restarted under the policy: restart count %s", restarts)
	}
	for _, line := range h.agentWarnings() {
		t.Errorf("the agent logged a failure under the policy: %s", line)
	}
	got, problem := h.receiverReports()
	if problem != "" {
		t.Fatal(problem)
	}
	for i, peer := range got.Peers {
		if peer != proxyIP {
			t.Errorf("report %d of %d came from %s, not through the proxy at %s", i+1, len(got.Peers), peer, proxyIP)
		}
	}

	h.clearOfHour()
	h.run("kubectl", "-n", "costfluent", "delete", "networkpolicy", allowPolicy)
	// The pod is found by name, not by the instance label the policy selects, so a chart that drops
	// the label fails on the policy and not here.
	replaced := h.uid("pod", agentPod)
	h.run("kubectl", "-n", "costfluent", "delete", "pod", agentPod, "--wait=true", "--timeout=90s")
	before, problem := h.receiverReports()
	if problem != "" {
		t.Fatal(problem)
	}
	// The receiver keeps every report, so one read at the end sees any that arrived meanwhile.
	h.sleepUntil(time.Now().Add(reportWithin))
	after, problem := h.receiverReports()
	if problem != "" {
		t.Fatal(problem)
	}
	if arrived := len(after.Reports) - len(before.Reports); arrived > 0 {
		t.Fatalf("%d report(s) arrived within %s of the agent's pod being replaced under default-deny egress alone: the CNI does not enforce NetworkPolicy",
			arrived, reportWithin)
	}
	status := h.run("kubectl", "-n", "costfluent", "get", "pod", agentPod, "-o", "jsonpath={.metadata.uid} {.status.phase}")
	if uid, phase, _ := strings.Cut(status, " "); uid == replaced || phase == "Pending" {
		t.Fatalf("the agent's pod was not replaced and started (pod %s, phase %s), so no report proves nothing", uid, phase)
	}
	t.Logf("no report in %s under default-deny egress alone", reportWithin)
}

// clearOfHour waits until just after the clock hour when the agent's next pod, starting within
// startWithin, could see the hour end its first window before reportIdle has passed. A window the
// hour cuts short is sent at once, and would ride the proxy connection the start's /v1/me opened in
// the first second, which kindnet lets through: the negative half would then fail on a cluster that
// enforces, and the positive half pass without the proxy rule. Just after the hour, the old pod has
// sent the window the hour closed, so its shutdown flush still rides an open connection; a flush
// that failed would be sent by the new pod in its first second.
func (h *harness) clearOfHour() {
	h.t.Helper()
	if now := time.Now(); nextHour(now).Sub(now) < startWithin+reportIdle+hourMargin {
		h.t.Logf("waiting for the clock hour at %s, which would end the next pod's first window early",
			nextHour(now).Format(time.RFC3339))
		h.sleepUntil(nextHour(now).Add(3 * pollEvery * time.Second))
	}
}

// apiServerEndpoint returns the address and port of the API server's one endpoint, which the guide
// tells the reader to read from the kubernetes EndpointSlice.
func (h *harness) apiServerEndpoint() (string, int) {
	h.t.Helper()
	out := h.run("kubectl", "-n", "default", "get", "endpointslices", "-l", "kubernetes.io/service-name=kubernetes",
		"-o", `jsonpath={range .items[*].endpoints[*]}{.addresses[0]}{" "}{end}|{.items[0].ports[0].port}`)
	addresses, port, _ := strings.Cut(out, "|")
	fields := strings.Fields(addresses)
	n, err := strconv.Atoi(strings.TrimSpace(port))
	if len(fields) != 1 || net.ParseIP(fields[0]) == nil || err != nil {
		h.t.Fatalf("the kubernetes EndpointSlice names %q, want one address and its port", out)
	}
	return fields[0], n
}

// nodeNetwork returns the IPv4 subnet of kind's Docker network, which every node's InternalIP, and
// so every kubelet the agent scrapes, must fall in.
func (h *harness) nodeNetwork() string {
	h.t.Helper()
	network := env("KIND_EXPERIMENTAL_DOCKER_NETWORK", "kind")
	var subnet *net.IPNet
	for _, s := range strings.Fields(h.run("docker", "network", "inspect", network,
		"--format", "{{range .IPAM.Config}}{{.Subnet}} {{end}}")) {
		if _, n, err := net.ParseCIDR(s); err == nil && n.IP.To4() != nil {
			subnet = n
		}
	}
	if subnet == nil {
		h.t.Fatalf("the Docker network %s has no IPv4 subnet", network)
	}
	for _, ip := range strings.Fields(h.run("kubectl", "get", "nodes",
		"-o", `jsonpath={range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{" "}{end}`)) {
		if !subnet.Contains(net.ParseIP(ip)) {
			h.t.Fatalf("the node address %s is outside the Docker network %s's subnet %s", ip, network, subnet)
		}
	}
	return subnet.String()
}

// agentWarnings returns the agent's log lines at warn or above. A log with no JSON line fails the
// test, so a change of log format cannot leave nothing checked.
func (h *harness) agentWarnings() []string {
	h.t.Helper()
	var warnings []string
	parsed := false
	scanner := bufio.NewScanner(strings.NewReader(h.run("kubectl", "-n", "costfluent", "logs", "pod/"+agentPod)))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var line struct {
			Level string `json:"level"`
		}
		if json.Unmarshal(scanner.Bytes(), &line) != nil {
			continue
		}
		parsed = true
		if line.Level != "debug" && line.Level != "info" {
			warnings = append(warnings, scanner.Text())
		}
	}
	if err := scanner.Err(); err != nil {
		h.t.Fatalf("reading the agent's log: %v", err)
	}
	if !parsed {
		h.t.Fatal("no line of the agent's log is JSON, so its levels cannot be checked")
	}
	return warnings
}
