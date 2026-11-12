//go:build e2e

package e2e

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	proxyImage = "costfluent-e2e/proxy"
	proxyUser  = "agent"
	// proxySecret is the Secret in the agent's namespace that agent.reportHTTPProxySecret names.
	proxySecret = "report-proxy"
	// receiverHost is the host and port of apiEndpoint, the only target the proxy tunnels to.
	receiverHost = "receiver.e2e:8443"
)

// TestKindReportProxy installs with the report proxy's URL, credentials included, in a Secret named
// by agent.reportHTTPProxySecret, as a cluster that reaches Costfluent only through an
// authenticating proxy does. The proxy (test/e2e/proxy) tunnels only CONNECTs to the receiver that
// carry its credentials, and every report must reach the receiver from the proxy's address: one
// from the agent's own address went around the proxy.
func TestKindReportProxy(t *testing.T) {
	h := newHarness(t, 10*time.Minute)
	cluster := h.kindCluster("costfluent-e2e-proxy", "")
	h.buildImages("receiver", "proxy")
	h.run("kind", "load", "docker-image", "--name", cluster,
		receiverImage+":"+imageTag, proxyImage+":"+imageTag, agentImage+":"+imageTag)
	h.deployReceiver(0)

	secret := make([]byte, 16)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	password := hex.EncodeToString(secret)
	h.apply(proxyWorkload(password))
	h.run("kubectl", "-n", "e2e", "rollout", "status", "deployment/proxy", "--timeout=120s")

	// kind kubelets serve self-signed certificates.
	installed := h.installAgent("agent.disableKubeTLSverify=true",
		"agent.reportHTTPProxySecret.name="+proxySecret,
		"agent.reportHTTPProxySecret.key=url")
	report := h.awaitReport(installed)
	t.Logf("report after %s: window %s to %s, %d nodes, %d pods", time.Since(installed).Round(time.Second),
		report.WindowStart.Format(time.RFC3339), report.WindowEnd.Format(time.RFC3339), len(report.Nodes), len(report.Pods))

	proxyIP, agentIP := h.podIP("e2e", "app=proxy"), h.podIP("costfluent", "app.kubernetes.io/instance="+release)
	if proxyIP == agentIP {
		t.Fatalf("the proxy and the agent share the address %s, so the receiver cannot tell them apart", proxyIP)
	}
	got, problem := h.receiverReports()
	if problem != "" {
		t.Fatal(problem)
	}
	for i, peer := range got.Peers {
		if peer != proxyIP {
			t.Errorf("report %d of %d came from %s, not through the proxy at %s (the agent is at %s)",
				i+1, len(got.Peers), peer, proxyIP, agentIP)
		}
	}

	raw := h.run("kubectl", "get", "--raw", "/api/v1/namespaces/e2e/services/proxy:3128/proxy/stats")
	var stats struct {
		Tunnels  int      `json:"tunnels"`
		Refusals []string `json:"refusals"`
	}
	if err := json.Unmarshal([]byte(raw), &stats); err != nil {
		t.Fatalf("proxy answered %q: %v", raw, err)
	}
	if len(stats.Refusals) > 0 {
		t.Errorf("the proxy refused the agent's requests: %v", stats.Refusals)
	}
	if stats.Tunnels == 0 {
		t.Errorf("the proxy opened no tunnel, yet the receiver holds reports from it")
	}
	t.Logf("%d reports through the proxy at %s, in %d tunnels", len(got.Peers), proxyIP, stats.Tunnels)
}

// podIP returns the IP of the one pod selector matches in namespace.
func (h *harness) podIP(namespace, selector string) string {
	h.t.Helper()
	ips := strings.Fields(h.run("kubectl", "-n", namespace, "get", "pods", "-l", selector,
		"-o", "jsonpath={.items[*].status.podIP}"))
	if len(ips) != 1 {
		h.t.Fatalf("pods %s in %s have IPs %v, want one", selector, namespace, ips)
	}
	return ips[0]
}

// proxyWorkload is the proxy, taking password with its user, and the Secret the agent reads its URL
// from.
func proxyWorkload(password string) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: proxy
  namespace: e2e
spec:
  selector:
    matchLabels: {app: proxy}
  template:
    metadata:
      labels: {app: proxy}
    spec:
      containers:
        - name: proxy
          image: %[1]s
          imagePullPolicy: Never
          env:
            - {name: PROXY_USER, value: %[2]q}
            - {name: PROXY_PASSWORD, value: %[3]q}
            - {name: PROXY_TARGET, value: %[6]q}
          ports:
            - {name: proxy, containerPort: 3128}
          readinessProbe:
            tcpSocket: {port: 3128}
---
apiVersion: v1
kind: Service
metadata:
  name: proxy
  namespace: e2e
spec:
  selector: {app: proxy}
  ports:
    - {name: proxy, port: 3128, targetPort: 3128}
---
apiVersion: v1
kind: Namespace
metadata:
  name: costfluent
---
apiVersion: v1
kind: Secret
metadata:
  name: %[4]s
  namespace: costfluent
stringData:
  url: %[5]q
`, proxyImage+":"+imageTag, proxyUser, password, proxySecret,
		fmt.Sprintf("http://%s:%s@proxy.e2e:3128", proxyUser, password), receiverHost)
}
