//go:build e2e

// Package e2e installs the chart into a kind cluster, points the agent at a stub receiver, and
// asserts that a real report arrives. Run it with `make e2e`; it needs docker, kind, kubectl and
// helm on the PATH.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	v1 "github.com/costfluent/k8s-agent/api/v1"
)

const (
	token         = "cf_org_e2e-smoke-token"
	clusterID     = "kind-e2e"
	release       = "cfa"
	agentImage    = "costfluent-e2e/k8s-agent"
	receiverImage = "costfluent-e2e/receiver"
	imageTag      = "e2e"
	fillerPods    = 50
	reportWithin  = 3 * time.Minute
)

// kindest/node for kind v0.33.0, the version the CI runner image pins. CI overrides it with the
// Harbor proxy path (KIND_NODE_IMAGE); the default pulls upstream, because this module is also
// published where Harbor cannot be reached.
const defaultNodeImage = "kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type harness struct {
	t          *testing.T
	ctx        context.Context
	kubeconfig string
}

func (h *harness) run(name string, args ...string) string {
	h.t.Helper()
	out, err := h.try(name, args...)
	if err != nil {
		h.t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return out
}

func (h *harness) try(name string, args ...string) (string, error) {
	cmd := exec.CommandContext(h.ctx, name, args...)
	cmd.Env = append(os.Environ(), "KUBECONFIG="+h.kubeconfig)
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	err := cmd.Run()
	return buf.String(), err
}

func (h *harness) apply(manifest string) {
	h.t.Helper()
	cmd := exec.CommandContext(h.ctx, "kubectl", "apply", "-f", "-")
	cmd.Env = append(os.Environ(), "KUBECONFIG="+h.kubeconfig)
	cmd.Stdin = strings.NewReader(manifest)
	if out, err := cmd.CombinedOutput(); err != nil {
		h.t.Fatalf("kubectl apply: %v\n%s", err, out)
	}
}

func TestKindSmoke(t *testing.T) {
	for _, tool := range []string{"docker", "kind", "kubectl", "helm", "go"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("%s is not on the PATH", tool)
		}
	}
	moduleRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	chart := env("E2E_CHART", filepath.Join(moduleRoot, "..", "helm-charts", "costfluent-k8s-agent"))
	if _, err := os.Stat(filepath.Join(chart, "Chart.yaml")); err != nil {
		t.Fatalf("no chart at %s (set E2E_CHART): %v", chart, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	defer cancel()
	dir := t.TempDir()
	h := &harness{t: t, ctx: ctx, kubeconfig: filepath.Join(dir, "kubeconfig")}
	cluster := env("KIND_CLUSTER_NAME", "costfluent-e2e")

	h.run("kind", "create", "cluster", "--name", cluster, "--image", env("KIND_NODE_IMAGE", defaultNodeImage),
		"--kubeconfig", h.kubeconfig, "--wait", "180s")
	t.Cleanup(func() {
		if os.Getenv("E2E_KEEP_CLUSTER") != "" {
			t.Logf("keeping kind cluster %s (KUBECONFIG=%s)", cluster, h.kubeconfig)
			return
		}
		out, err := exec.Command("kind", "delete", "cluster", "--name", cluster).CombinedOutput()
		if err != nil {
			t.Logf("kind delete cluster: %v\n%s", err, out)
		}
	})

	receiverDir := filepath.Join(dir, "receiver")
	build := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", filepath.Join(receiverDir, "receiver"), "./test/e2e/receiver")
	build.Dir = moduleRoot
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+runtime.GOARCH)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build receiver: %v\n%s", err, out)
	}
	dockerfile, err := os.ReadFile(filepath.Join(moduleRoot, "test", "e2e", "receiver", "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(receiverDir, "Dockerfile"), dockerfile, 0o644); err != nil {
		t.Fatal(err)
	}
	h.run("docker", "build", "-q", "-t", receiverImage+":"+imageTag, receiverDir)
	h.run("docker", "build", "-q", "-t", agentImage+":"+imageTag, "--build-arg", "VERSION=e2e", moduleRoot)
	h.run("kind", "load", "docker-image", "--name", cluster, receiverImage+":"+imageTag, agentImage+":"+imageTag)

	h.apply(workloads())
	h.run("kubectl", "-n", "e2e", "rollout", "status", "deployment/receiver", "--timeout=120s")
	h.run("kubectl", "-n", "e2e-filler", "rollout", "status", "deployment/filler", "--timeout=180s")

	installed := time.Now()
	h.run("helm", "install", release, chart, "--namespace", "costfluent", "--create-namespace",
		"--set", "agent.token="+token,
		"--set", "agent.clusterID="+clusterID,
		"--set", "agent.apiEndpoint=http://receiver.e2e:8080",
		"--set", "agent.pollingInterval=5",
		// kind kubelets serve self-signed certificates.
		"--set", "agent.disableKubeTLSverify=true",
		"--set", "image.repository="+agentImage,
		"--set", "image.tag="+imageTag,
		"--set", "image.pullPolicy=Never")
	t.Cleanup(func() {
		if t.Failed() {
			out, _ := h.try("kubectl", "-n", "costfluent", "describe", "pods")
			t.Logf("agent pods:\n%s", out)
			out, _ = h.try("kubectl", "-n", "costfluent", "logs", "statefulset/"+release+"-costfluent-k8s-agent", "--tail=100")
			t.Logf("agent logs:\n%s", out)
		}
	})

	report := h.awaitReport(installed)
	t.Logf("report after %s: window %s to %s, %d nodes, %d pods",
		time.Since(installed).Round(time.Second), report.WindowStart.Format(time.RFC3339), report.WindowEnd.Format(time.RFC3339),
		len(report.Nodes), len(report.Pods))
	logAgentFootprint(t, report)
}

// awaitReport polls the receiver until a report meets criterion 3 or the deadline passes.
func (h *harness) awaitReport(since time.Time) v1.Report {
	h.t.Helper()
	var last string
	for time.Since(since) < reportWithin {
		time.Sleep(5 * time.Second)
		raw, err := h.try("kubectl", "get", "--raw", "/api/v1/namespaces/e2e/services/receiver:8080/proxy/reports")
		if err != nil {
			last = raw
			continue
		}
		var got struct {
			Reports  []json.RawMessage `json:"reports"`
			Refusals []string          `json:"refusals"`
		}
		if err := json.Unmarshal([]byte(raw), &got); err != nil {
			h.t.Fatalf("receiver answered %q: %v", raw, err)
		}
		if len(got.Refusals) > 0 {
			h.t.Fatalf("the receiver refused the agent's requests: %v", got.Refusals)
		}
		for _, body := range got.Reports {
			report, problem := check(body)
			if problem == "" {
				return report
			}
			last = problem
		}
	}
	h.t.Fatalf("no acceptable report within %s; last: %s", reportWithin, last)
	return v1.Report{}
}

func check(body json.RawMessage) (v1.Report, string) {
	var r v1.Report
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return r, fmt.Sprintf("report does not decode into api/v1: %v", err)
	}
	if r.SchemaVersion != v1.SchemaVersion || r.ClusterID != clusterID || r.AgentInstanceID == "" {
		return r, fmt.Sprintf("header: schema %d, cluster %q, instance %q", r.SchemaVersion, r.ClusterID, r.AgentInstanceID)
	}
	if len(r.Nodes) == 0 {
		return r, "no nodes"
	}
	var kubeSystem int
	var cpu float64
	for _, p := range r.Pods {
		if p.Namespace == "kube-system" {
			kubeSystem++
		}
		for _, c := range p.Containers {
			cpu += c.CPUUsageCoreSeconds
		}
	}
	if kubeSystem == 0 {
		return r, "no kube-system pods"
	}
	if cpu <= 0 {
		return r, "CPU usage is zero"
	}
	return r, ""
}

// logAgentFootprint prints the agent's own memory as the report measured it, which is what the
// chart README's sizing is taken from.
func logAgentFootprint(t *testing.T, r v1.Report) {
	for _, p := range r.Pods {
		if p.Namespace != "costfluent" {
			continue
		}
		for _, c := range p.Containers {
			if c.ObservedSeconds == 0 {
				continue
			}
			t.Logf("agent at %d pods: working set mean %.1f MiB, peak %.1f MiB, CPU mean %.4f cores",
				len(r.Pods),
				c.MemoryUsageByteSeconds/c.ObservedSeconds/(1<<20),
				float64(c.MemoryUsagePeakBytes)/(1<<20),
				c.CPUUsageCoreSeconds/c.ObservedSeconds)
		}
	}
}

func workloads() string {
	return fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: e2e
---
apiVersion: v1
kind: Namespace
metadata:
  name: e2e-filler
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: receiver
  namespace: e2e
spec:
  selector:
    matchLabels: {app: receiver}
  template:
    metadata:
      labels: {app: receiver}
    spec:
      containers:
        - name: receiver
          image: %[1]s
          imagePullPolicy: Never
          env:
            - {name: RECEIVER_TOKEN, value: %[2]q}
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet: {path: /healthz, port: 8080}
---
apiVersion: v1
kind: Service
metadata:
  name: receiver
  namespace: e2e
spec:
  selector: {app: receiver}
  ports:
    - port: 8080
      targetPort: 8080
---
# Brings the cluster to about 60 pods, the size the chart README's sizing is measured at.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: filler
  namespace: e2e-filler
spec:
  replicas: %[3]d
  selector:
    matchLabels: {app: filler}
  template:
    metadata:
      labels: {app: filler, team: e2e}
    spec:
      containers:
        - name: filler
          image: %[1]s
          imagePullPolicy: Never
          env:
            - {name: RECEIVER_TOKEN, value: unused}
          resources:
            requests: {cpu: 1m, memory: 8Mi}
`, receiverImage+":"+imageTag, token, fillerPods)
}
