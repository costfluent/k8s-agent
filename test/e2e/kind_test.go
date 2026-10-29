//go:build e2e

// Package e2e installs the chart into a kind cluster, points the agent at a stub receiver, and
// asserts that a real report arrives. The agent reports over TLS and trusts the receiver only
// through the CA bundle its own image ships, extended with the test's CA, so an image that cannot
// verify the API's certificate fails the run. Run it with `make e2e`; it needs docker, kind,
// kubectl and helm on the PATH.
package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	v1 "github.com/costfluent/k8s-agent/api/v1"
)

const (
	token         = "cf_k8s_e2e-smoke-token"
	clusterID     = "kind-e2e"
	release       = "cfa"
	agentImage    = "costfluent-e2e/k8s-agent"
	receiverImage = "costfluent-e2e/receiver"
	imageTag      = "e2e"
	apiEndpoint   = "https://receiver.e2e:8443"
	fillerPods    = 50
	reportWithin  = 3 * time.Minute
)

// agentVersion is the VERSION the agent image is built with, which every report must carry as
// agent_version. Go's linker ignores an -X whose target is gone, so a renamed main.Version or a
// dropped ARG or flag still builds, and only this check stops a release that reports "dev" or no
// version.
const agentVersion = "e2e"

// kindest/node for kind v0.33.0. KIND_NODE_IMAGE overrides it, for example with a registry mirror
// or another Kubernetes version; the default pulls upstream.
const defaultNodeImage = "kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"

// bundlePath is the first file Go reads its system roots from on Linux, and the only trust store
// the agent's scratch image has.
const bundlePath = "/etc/ssl/certs/ca-certificates.crt"

// minBundleCertificates is far below the roughly 140 roots of the Mozilla bundle, so a routine
// update never trips it, while an emptied or truncated bundle does.
const minBundleCertificates = 100

// notInImageTag matches the characters a Docker tag may not hold. A tag is at most 128 characters
// and may not start with '.' or '-'.
var notInImageTag = regexp.MustCompile(`[^A-Za-z0-9_.-]`)

// tlsImageTag is the tag of the agent image that trusts this run's CA. Every run commits a
// different CA, so the tag is per cluster, like the cluster: under one shared tag, concurrent runs
// on one Docker daemon would load each other's image or remove each other's tag.
func tlsImageTag(cluster string) string {
	tag := "e2e-tls-" + notInImageTag.ReplaceAllString(cluster, "-")
	return tag[:min(len(tag), 128)]
}

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

	// A signal would otherwise kill the binary before t.Cleanup deletes the cluster: a test runner
	// may send SIGTERM to stop a run early, and SIGPIPE arrives once the
	// killed go command stops reading our output. Cancelling instead fails the test and cleans up.
	signalled, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGPIPE)
	defer stop()
	ctx, cancel := context.WithTimeout(signalled, 14*time.Minute)
	defer cancel()
	dir := t.TempDir()
	h := &harness{t: t, ctx: ctx, kubeconfig: filepath.Join(dir, "kubeconfig")}
	// Per run, so concurrent runs on one Docker daemon do not collide.
	cluster := env("KIND_CLUSTER_NAME", fmt.Sprintf("costfluent-e2e-%d", os.Getpid()))

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
	h.run("kind", "create", "cluster", "--name", cluster, "--image", env("KIND_NODE_IMAGE", defaultNodeImage),
		"--kubeconfig", h.kubeconfig, "--wait", "180s")
	h.assertVolumeClaimGuard(chart)

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
	h.run("docker", "build", "-q", "-t", agentImage+":"+imageTag, "--build-arg", "VERSION="+agentVersion, moduleRoot)
	caPEM, certPEM, keyPEM := testCertificates(t)
	tlsTag := tlsImageTag(cluster)
	h.trustInImage(caPEM, dir, tlsTag)
	h.run("kind", "load", "docker-image", "--name", cluster, receiverImage+":"+imageTag, agentImage+":"+tlsTag)

	h.apply(workloads(certPEM, keyPEM))
	h.run("kubectl", "-n", "e2e", "rollout", "status", "deployment/receiver", "--timeout=120s")
	h.run("kubectl", "-n", "e2e-filler", "rollout", "status", "deployment/filler", "--timeout=180s")

	installed := time.Now()
	h.run("helm", "install", release, chart, "--namespace", "costfluent", "--create-namespace",
		"--set", "agent.token="+token,
		"--set", "agent.apiEndpoint="+apiEndpoint,
		"--set", "agent.pollingInterval=5",
		// kind kubelets serve self-signed certificates.
		"--set", "agent.disableKubeTLSverify=true",
		"--set", "image.repository="+agentImage,
		"--set", "image.tag="+tlsTag,
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
	t.Logf("report from %s after %s: window %s to %s, %d nodes, %d pods", apiEndpoint,
		time.Since(installed).Round(time.Second), report.WindowStart.Format(time.RFC3339), report.WindowEnd.Format(time.RFC3339),
		len(report.Nodes), len(report.Pods))
	logAgentFootprint(t, report)
}

// testCertificates returns a CA and a server certificate it signs for the receiver's Service names,
// as PEM. The hour of slack before now covers a node clock slightly behind the host's.
func testCertificates(t *testing.T) (caPEM, certPEM, keyPEM []byte) {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "costfluent e2e CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	server := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "receiver.e2e"},
		DNSNames:     []string{"receiver.e2e", "receiver.e2e.svc", "receiver.e2e.svc.cluster.local"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, server, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

// trustInImage checks the CA bundle the agent image ships and commits a copy of the image, tagged
// tag, whose bundle also holds caPEM. The CA is appended to the shipped file rather than
// added beside it or pointed at with SSL_CERT_FILE, so the agent still trusts only what the image
// carries, and a bundle that is missing, moved or empty fails here.
func (h *harness) trustInImage(caPEM []byte, dir, tag string) {
	h.t.Helper()
	container := strings.TrimSpace(h.run("docker", "create", agentImage+":"+imageTag))
	h.t.Cleanup(func() {
		// -v also removes the anonymous volume the image's VOLUME gives the container.
		if out, err := exec.Command("docker", "rm", "-v", container).CombinedOutput(); err != nil {
			h.t.Logf("docker rm: %v\n%s", err, out)
		}
	})
	file := filepath.Join(dir, "ca-certificates.crt")
	if out, err := h.try("docker", "cp", container+":"+bundlePath, file); err != nil {
		h.t.Fatalf("the agent image ships no CA bundle at %s: %v\n%s", bundlePath, err, out)
	}
	bundle, err := os.ReadFile(file)
	if err != nil {
		h.t.Fatal(err)
	}
	if n := countCertificates(bundle); n < minBundleCertificates {
		h.t.Fatalf("the agent image's CA bundle holds %d certificates, fewer than %d", n, minBundleCertificates)
	}
	if len(bundle) > 0 && !bytes.HasSuffix(bundle, []byte("\n")) {
		bundle = append(bundle, '\n')
	}
	bundle = append(bundle, caPEM...)
	if err := os.WriteFile(file, bundle, 0o644); err != nil {
		h.t.Fatal(err)
	}
	// docker cp keeps the file's mode, and the image runs as user 1000, so the file must be
	// world-readable. It already exists from the copy out, which set its mode, and WriteFile's
	// mode applies only to a file it creates, so this is the one line that sets it.
	if err := os.Chmod(file, 0o644); err != nil {
		h.t.Fatal(err)
	}
	h.run("docker", "cp", file, container+":"+bundlePath)
	h.run("docker", "commit", container, agentImage+":"+tag)
	h.t.Cleanup(func() {
		if out, err := exec.Command("docker", "rmi", agentImage+":"+tag).CombinedOutput(); err != nil {
			h.t.Logf("docker rmi: %v\n%s", err, out)
		}
	})
}

// countCertificates counts the PEM certificates in bundle that Go can parse, the ones it would
// trust.
func countCertificates(bundle []byte) int {
	var n int
	for {
		var block *pem.Block
		block, bundle = pem.Decode(bundle)
		if block == nil {
			return n
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		if _, err := x509.ParseCertificate(block.Bytes); err == nil {
			n++
		}
	}
}

// defaultClassAnnotation marks the StorageClass a claim naming none is given; kind's "standard"
// class carries it.
const defaultClassAnnotation = "storageclass.kubernetes.io/is-default-class"

// assertVolumeClaimGuard holds the chart to refusing an install whose volume claim no StorageClass
// in the cluster can provision. Such a claim stays Pending, so the pod is never scheduled and the
// agent never starts to log why. Only a render against a cluster sees its StorageClasses, so this
// runs here and not in check-helm-chart.sh, whose offline renders the guard must let through.
func (h *harness) assertVolumeClaimGuard(chart string) {
	h.t.Helper()
	render := func(args ...string) (string, error) {
		return h.try("helm", append([]string{"template", release, chart, "--namespace", "costfluent",
			"--dry-run=server", "--set", "agent.token=" + token}, args...)...)
	}
	refused := func(want string, args ...string) {
		h.t.Helper()
		out, err := render(args...)
		if err == nil {
			h.t.Fatalf("helm template %v rendered, but must refuse the install with %q", args, want)
		}
		if !strings.Contains(out, want) {
			h.t.Fatalf("helm template %v failed without %q:\n%s", args, want, out)
		}
	}
	accepted := func(args ...string) {
		h.t.Helper()
		if out, err := render(args...); err != nil {
			h.t.Fatalf("helm template %v must render: %v\n%s", args, err, out)
		}
	}

	h.run("kubectl", "annotate", "storageclass", "standard", defaultClassAnnotation+"-")
	refused("no default StorageClass")
	refused(`StorageClass "missing"`, "--set", "persist.storageClassName=missing")
	accepted("--set", "persist.storageClassName=standard")
	accepted("--set", "persist=null")
	// The claim of an installed release is fixed and may already be bound, so an upgrade is not held
	// to the classes the cluster has now.
	accepted("--is-upgrade")

	h.run("kubectl", "annotate", "storageclass", "standard", defaultClassAnnotation+"=true")
	accepted()
	accepted("--set", "persist.storageClassName=standard")
	refused(`StorageClass "missing"`, "--set", "persist.storageClassName=missing")
}

// awaitReport polls the receiver until a report meets criterion 3 or the deadline passes.
func (h *harness) awaitReport(since time.Time) v1.Report {
	h.t.Helper()
	var last string
	for time.Since(since) < reportWithin {
		time.Sleep(5 * time.Second)
		if err := h.ctx.Err(); err != nil {
			h.t.Fatalf("waiting for a report: %v", err)
		}
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
	if r.SchemaVersion != v1.SchemaVersion || r.ClusterID != clusterID || r.AgentInstanceID == "" || r.AgentVersion != agentVersion {
		return r, fmt.Sprintf("header: schema %d, cluster %q, instance %q, agent version %q (built with %q)",
			r.SchemaVersion, r.ClusterID, r.AgentInstanceID, r.AgentVersion, agentVersion)
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
	return r, checkAgentClaim(r)
}

// checkAgentClaim holds the report to the agent's own buffer volume: a Bound volume of kind's
// standard class serving the StatefulSet's data claim, which the agent's pod mounts.
func checkAgentClaim(r v1.Report) string {
	claim := "data-" + release + "-costfluent-k8s-agent-0"
	var volume *v1.Volume
	for i := range r.Volumes {
		if r.Volumes[i].ClaimNamespace == "costfluent" && r.Volumes[i].ClaimName == claim {
			volume = &r.Volumes[i]
		}
	}
	if volume == nil {
		return fmt.Sprintf("no volume serves the agent's claim %s among %d volumes", claim, len(r.Volumes))
	}
	if volume.Phase != "Bound" || volume.StorageClass != "standard" || volume.CapacityBytes <= 0 || volume.ObservedSeconds <= 0 {
		return fmt.Sprintf("the agent's volume = %+v, want Bound, of class standard, sized and observed", *volume)
	}
	for _, p := range r.Pods {
		if p.Namespace == "costfluent" && slices.Contains(p.Claims, claim) {
			return ""
		}
	}
	return "the agent's pod does not report mounting its claim " + claim
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

func workloads(certPEM, keyPEM []byte) string {
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
apiVersion: v1
kind: Secret
metadata:
  name: receiver-tls
  namespace: e2e
type: kubernetes.io/tls
data:
  tls.crt: %[4]s
  tls.key: %[5]s
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
            - {name: RECEIVER_TLS_CERT, value: /tls/tls.crt}
            - {name: RECEIVER_TLS_KEY, value: /tls/tls.key}
          ports:
            - {name: http, containerPort: 8080}
            - {name: https, containerPort: 8443}
          # Over TLS, so the rollout waits for the listener the agent reports to.
          readinessProbe:
            httpGet: {path: /healthz, port: 8443, scheme: HTTPS}
          volumeMounts:
            - {name: tls, mountPath: /tls, readOnly: true}
      volumes:
        - name: tls
          secret: {secretName: receiver-tls}
---
apiVersion: v1
kind: Service
metadata:
  name: receiver
  namespace: e2e
spec:
  selector: {app: receiver}
  ports:
    - {name: http, port: 8080, targetPort: 8080}
    - {name: https, port: 8443, targetPort: 8443}
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
`, receiverImage+":"+imageTag, token, fillerPods,
		base64.StdEncoding.EncodeToString(certPEM), base64.StdEncoding.EncodeToString(keyPEM))
}
