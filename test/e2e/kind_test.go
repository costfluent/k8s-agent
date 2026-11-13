//go:build e2e

// Package e2e installs the chart into real clusters, points the agent at a stub receiver, and
// asserts what reaches it. The agent reports over TLS and trusts the receiver's private CA only
// through agent.reportCABundle, which adds it to the bundle the image ships. Each test is one leg of
// the kind-smoke job and has its own make target: TestKindSmoke (`make e2e-kind`), TestK3s
// (`make e2e-k3s`), TestKindReportProxy (`make e2e-proxy`), TestKindKubeletVerification
// (`make e2e-kubelet-tls`) and TestKindUpgrade (`make e2e-upgrade`); `make e2e` runs them all. They
// need docker, kind, kubectl, helm and go on the PATH.
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
	agentFullname = release + "-costfluent-k8s-agent"
	agentImage    = "costfluent-e2e/k8s-agent"
	receiverImage = "costfluent-e2e/receiver"
	imageTag      = "e2e"
	apiEndpoint   = "https://receiver.e2e:8443"
	// receiverCA is the ConfigMap in the agent's namespace that agent.reportCABundle names.
	receiverCA   = "receiver-ca"
	fillerPods   = 50
	reportWithin = 3 * time.Minute
	// pollEvery is the agent.pollingInterval the release is installed with, in seconds.
	pollEvery = 5
	// shutdownWithin runs from the return of helm uninstall --wait, not from the stop, and is the
	// chart's 30 second grace period plus a poll of the receiver: a report later than that was not
	// sent by the stopped agent.
	shutdownWithin = 40 * time.Second
	// hourMargin keeps the stop this far from a clock hour, where the agent closes its window by the
	// clock: a clock close between the stop and the SIGTERM would leave the shutdown's window starting
	// at the hour, where no report ending before the stop does, and fail a correct agent.
	hourMargin = time.Minute
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
	dir        string
	moduleRoot string
	chart      string
	// storageClass is the class the cluster provisions the agent's claim from.
	storageClass string
	// caPEM is the CA that signs the receiver's certificate, set by deployReceiver.
	caPEM []byte
	// diagnosing is set once installChart has registered the failure diagnostics, so a leg that
	// installs twice logs them once.
	diagnosing bool
}

// newHarness checks the tools every leg needs and gives the test a context that ends after within
// or on a signal. A signal would otherwise kill the binary before t.Cleanup deletes the cluster: a
// test runner may send SIGTERM to stop a run early, and SIGPIPE arrives once the killed go command
// stops reading our output. Cancelling instead fails the test and cleans up.
func newHarness(t *testing.T, within time.Duration) *harness {
	t.Helper()
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
	signalled, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGPIPE)
	ctx, cancel := context.WithTimeout(signalled, within)
	// Registered first, so they run after every other cleanup, which therefore cannot use h.ctx.
	t.Cleanup(stop)
	t.Cleanup(cancel)
	dir := t.TempDir()
	return &harness{t: t, ctx: ctx, kubeconfig: filepath.Join(dir, "kubeconfig"), dir: dir,
		moduleRoot: moduleRoot, chart: chart, storageClass: "standard"}
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

// keep reports whether E2E_KEEP_CLUSTER asks to leave the cluster running for a look.
func keep() bool { return os.Getenv("E2E_KEEP_CLUSTER") != "" }

// Cleanups must end before go test's -timeout: at the test's deadline it panics and exits without
// running the cleanups still queued, so a cluster whose removal had not started stays on the shared
// Docker daemon, unnamed. Each Makefile leg's -timeout is only a minute over the harness's within.
const (
	// cleanupMargin is how long before the deadline removeOrFail has stopped and reported.
	cleanupMargin = 10 * time.Second
	// removalReserve is how long before the deadline the failure diagnostics stop, leaving the
	// cluster's removal, which runs after them, its turn.
	removalReserve = 30 * time.Second
)

// removeOrFail removes a leg's cluster and fails the test, naming it and the command that removes it
// by hand, unless gone reports it removed: a leftover keeps running, privileged in k3s's case, beside
// every later leg and run on a Docker daemon agents share. Success is gone reading the daemon, not
// remove's exit status, because another run may already have removed it and docker and kind answer a
// missing target differently across versions. It makes up to 3 attempts, 5 s apart, each a removal
// of up to 2 minutes and a check of up to 1 minute, on contexts of their own because h.ctx has expired
// when the leg timed out. Under a test deadline it starts an attempt only while at least 15 s remain
// until cleanupMargin before it, caps each command at the time left until then, and reports by then.
func (h *harness) removeOrFail(what, manual string, remove func(context.Context) ([]byte, error), gone func(context.Context) (bool, error)) {
	h.t.Helper()
	const (
		attempts   = 3
		pause      = 5 * time.Second
		removeFor  = 2 * time.Minute
		checkFor   = time.Minute
		checkMin   = 5 * time.Second
		attemptMin = 15 * time.Second
	)
	end, bounded := h.t.Deadline()
	end = end.Add(-cleanupMargin)
	left := func() time.Duration {
		if !bounded {
			return removeFor + checkFor
		}
		return time.Until(end)
	}
	var (
		out      []byte
		err      error
		checkErr error
		tried    int
	)
	for tried < attempts {
		if tried > 0 {
			if left() < pause+attemptMin {
				break
			}
			time.Sleep(pause)
		}
		if left() < attemptMin {
			break
		}
		tried++
		ctx, cancel := context.WithTimeout(context.Background(), min(removeFor, left()-checkMin))
		out, err = remove(ctx)
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), min(checkFor, left()))
		var removed bool
		removed, checkErr = gone(ctx)
		cancel()
		if removed {
			return
		}
	}
	stopped := ""
	if tried < attempts {
		stopped = fmt.Sprintf(", stopped %s before the test's deadline", (left() + cleanupMargin).Round(time.Second))
	}
	switch {
	case tried == 0:
		h.t.Errorf("%s was not removed: no time to try%s; remove it by hand with `%s`", what, stopped, manual)
	case checkErr != nil:
		h.t.Errorf("%s may still be on the Docker daemon: the check that it is gone failed after %d removal(s)%s: %v; check, and remove it by hand with `%s`. Last removal: %v\n%s",
			what, tried, stopped, checkErr, manual, err, out)
	default:
		h.t.Errorf("%s is still on the Docker daemon after %d removal(s)%s; remove it by hand with `%s`. Last removal: %v\n%s",
			what, tried, stopped, manual, err, out)
	}
}

// kindCluster creates a kind cluster named prefix and the process ID, so concurrent runs on one
// Docker daemon do not collide, from config when it is not empty, and deletes it when the test
// ends. KIND_CLUSTER_NAME overrides the name.
func (h *harness) kindCluster(prefix, config string) string {
	h.t.Helper()
	cluster := env("KIND_CLUSTER_NAME", fmt.Sprintf("%s-%d", prefix, os.Getpid()))
	h.t.Cleanup(func() {
		if keep() {
			h.t.Logf("keeping kind cluster %s (KUBECONFIG=%s)", cluster, h.kubeconfig)
			return
		}
		h.removeOrFail("kind cluster "+cluster, "kind delete cluster --name "+cluster,
			func(ctx context.Context) ([]byte, error) {
				return exec.CommandContext(ctx, "kind", "delete", "cluster", "--name", cluster).CombinedOutput()
			},
			func(ctx context.Context) (bool, error) {
				out, err := exec.CommandContext(ctx, "kind", "get", "clusters").CombinedOutput()
				if err != nil {
					return false, fmt.Errorf("kind get clusters: %w\n%s", err, out)
				}
				return !slices.Contains(strings.Fields(string(out)), cluster), nil
			})
	})
	args := []string{"create", "cluster", "--name", cluster, "--image", env("KIND_NODE_IMAGE", defaultNodeImage),
		"--kubeconfig", h.kubeconfig, "--wait", "180s"}
	if config != "" {
		file := filepath.Join(h.dir, "kind.yaml")
		if err := os.WriteFile(file, []byte(config), 0o644); err != nil {
			h.t.Fatal(err)
		}
		args = append(args, "--config", file)
	}
	h.run("kind", args...)
	return cluster
}

// buildImages builds the agent image from the module and an image for each named helper under
// test/e2e, from its static binary built on the host and its own Dockerfile.
func (h *harness) buildImages(helpers ...string) {
	h.t.Helper()
	for _, name := range helpers {
		dir := filepath.Join(h.dir, name)
		build := exec.CommandContext(h.ctx, "go", "build", "-trimpath", "-o", filepath.Join(dir, name), "./test/e2e/"+name)
		build.Dir = h.moduleRoot
		build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+runtime.GOARCH)
		if out, err := build.CombinedOutput(); err != nil {
			h.t.Fatalf("build %s: %v\n%s", name, err, out)
		}
		dockerfile, err := os.ReadFile(filepath.Join(h.moduleRoot, "test", "e2e", name, "Dockerfile"))
		if err != nil {
			h.t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), dockerfile, 0o644); err != nil {
			h.t.Fatal(err)
		}
		h.run("docker", "build", "-q", "-t", "costfluent-e2e/"+name+":"+imageTag, dir)
	}
	h.run("docker", "build", "-q", "-t", agentImage+":"+imageTag, "--build-arg", "VERSION="+agentVersion, h.moduleRoot)
}

// deployReceiver deploys the receiver behind a certificate of a fresh CA, and fillers pods beside
// it, and waits for both.
func (h *harness) deployReceiver(fillers int) {
	h.t.Helper()
	caPEM, certPEM, keyPEM := testCertificates(h.t)
	h.caPEM = caPEM
	h.apply(workloads(certPEM, keyPEM, fillers))
	h.run("kubectl", "-n", "e2e", "rollout", "status", "deployment/receiver", "--timeout=180s")
	h.run("kubectl", "-n", "e2e-filler", "rollout", "status", "deployment/filler", "--timeout=180s")
}

// installAgent installs the tree's chart with the agent image buildImages built, as installChart
// does, and returns when the install began. Once installed, it holds the release to the split of the
// agent's two accounts.
func (h *harness) installAgent(values ...string) time.Time {
	h.t.Helper()
	installed := h.installChart(h.chart, agentImage, imageTag, values...)
	h.assertKubeletAccount()
	return installed
}

// installChart installs chart as the console's command does, with helm upgrade --install --wait,
// running the agent image repository:tag already loaded into the cluster, plus values, and returns
// when the install began. The agent trusts the receiver's CA through agent.reportCABundle. On the
// release that is already installed it is an upgrade.
func (h *harness) installChart(chart, repository, tag string, values ...string) time.Time {
	h.t.Helper()
	h.apply(fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: costfluent
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: %s
  namespace: costfluent
data:
  ca.crt: %q
`, receiverCA, h.caPEM))
	if !h.diagnosing {
		h.diagnosing = true
		h.t.Cleanup(h.logDiagnostics)
	}
	installed := time.Now()
	args := []string{"upgrade", release, chart, "--install", "--namespace", "costfluent", "--create-namespace",
		"--wait", "--timeout", "5m",
		"--set", "agent.token=" + token,
		"--set", "agent.apiEndpoint=" + apiEndpoint,
		"--set", fmt.Sprintf("agent.pollingInterval=%d", pollEvery),
		"--set", "agent.reportCABundle.configMap=" + receiverCA,
		"--set", "image.repository=" + repository,
		"--set", "image.tag=" + tag,
		"--set", "image.pullPolicy=Never"}
	for _, v := range values {
		args = append(args, "--set", v)
	}
	h.run("helm", args...)
	return installed
}

// logDiagnostics logs the agent's pods and logs and the nodes when the test failed.
func (h *harness) logDiagnostics() {
	if !h.t.Failed() {
		return
	}
	// Cleanups run after the context's own cleanup cancelled it, so a command on h.ctx never
	// starts here: the diagnostics get a context of their own.
	// They stop removalReserve before the test's deadline, so the cluster's removal still gets its turn.
	diagUntil := time.Now().Add(time.Minute)
	if end, ok := h.t.Deadline(); ok && end.Add(-removalReserve).Before(diagUntil) {
		diagUntil = end.Add(-removalReserve)
	}
	diagCtx, cancel := context.WithDeadline(context.Background(), diagUntil)
	defer cancel()
	diag := &harness{t: h.t, ctx: diagCtx, kubeconfig: h.kubeconfig}
	for _, c := range []struct {
		title string
		args  []string
	}{
		{"agent pods", []string{"-n", "costfluent", "describe", "pods"}},
		{"agent logs", []string{"-n", "costfluent", "logs", "statefulset/" + agentFullname, "--tail=100"}},
		// Conditions and allocated resources tell a starved node from an agent that never reported.
		{"nodes", []string{"describe", "nodes"}},
	} {
		out, err := diag.try("kubectl", c.args...)
		if err != nil {
			out += fmt.Sprintf("\n(kubectl %s: %v)", strings.Join(c.args, " "), err)
		}
		h.t.Logf("%s:\n%s", c.title, out)
	}
}

// assertKubeletAccount holds the chart to sending kubelets only the metrics account's token: the
// agent's own account, which reads every node and pod, may not get nodes/metrics, and the kubelet
// account the agent mints tokens for may. A report with usage after this proves the minted token
// works against a real kubelet.
func (h *harness) assertKubeletAccount() {
	h.t.Helper()
	agent := "system:serviceaccount:costfluent:" + agentFullname
	for _, c := range []struct {
		as   string
		want string
	}{{agent, "no"}, {agent + "-kubelet", "yes"}} {
		// can-i exits 1 on "no", so the answer is read from its output rather than its status.
		out, _ := h.try("kubectl", "auth", "can-i", "get", "nodes", "--subresource=metrics", "--as="+c.as)
		lines := strings.Split(strings.TrimSpace(out), "\n")
		if answer := strings.Fields(lines[len(lines)-1]); len(answer) == 0 || answer[0] != c.want {
			h.t.Fatalf("kubectl auth can-i get nodes/metrics --as=%s answered %q, want %q", c.as, out, c.want)
		}
	}
}

func TestKindSmoke(t *testing.T) {
	h := newHarness(t, 14*time.Minute)
	cluster := h.kindCluster("costfluent-e2e", "")
	h.assertVolumeClaimGuard()
	h.buildImages("receiver")
	h.assertImageBundle()
	h.run("kind", "load", "docker-image", "--name", cluster, receiverImage+":"+imageTag, agentImage+":"+imageTag)
	h.deployReceiver(fillerPods)

	// kind kubelets serve self-signed certificates.
	installed := h.installAgent("agent.disableKubeTLSverify=true")
	report := h.awaitReport(installed)
	t.Logf("report from %s after %s: window %s to %s, %d nodes, %d pods", apiEndpoint,
		time.Since(installed).Round(time.Second), report.WindowStart.Format(time.RFC3339), report.WindowEnd.Format(time.RFC3339),
		len(report.Nodes), len(report.Pods))
	logAgentFootprint(t, report)

	h.assertShutdownReport(report)
}

// assertShutdownReport stops the agent the way Kubernetes does, by uninstalling the release so the
// kubelet sends SIGTERM and waits the chart's grace period, and holds the agent to closing and
// sending the window that is open then. The uninstall leaves no pod to start again, so a report
// after the stop can only come from the shutdown.
func (h *harness) assertShutdownReport(first v1.Report) {
	h.t.Helper()
	// At least two polls into the open window, so it holds samples to send.
	h.sleepUntil(first.WindowEnd.Add(3 * pollEvery * time.Second))
	if now := time.Now(); nextHour(now).Sub(now) < hourMargin {
		h.sleepUntil(nextHour(now).Add(3 * pollEvery * time.Second))
	}
	// Against a receiver that answers at once the whole shutdown fits in any grace period, so only a
	// send slower than a too-short one tells the chart's period from one the kubelet kills the drain in.
	h.postReceiver("delay")
	stopped := time.Now()
	h.run("helm", "uninstall", release, "--namespace", "costfluent", "--wait")
	report := h.awaitReportAfter(first, stopped, shutdownWithin)
	h.t.Logf("shutdown report %s after the uninstall began: window %s to %s, %d pods",
		time.Since(stopped).Round(time.Second), report.WindowStart.Format(time.RFC3339), report.WindowEnd.Format(time.RFC3339Nano),
		len(report.Pods))
}

// sleepUntil waits until t, failing the test when its context ends first.
func (h *harness) sleepUntil(t time.Time) {
	h.t.Helper()
	select {
	case <-time.After(time.Until(t)):
	case <-h.ctx.Done():
		h.t.Fatalf("waiting until %s: %v", t.Format(time.RFC3339), h.ctx.Err())
	}
}

func nextHour(t time.Time) time.Time { return t.Truncate(time.Hour).Add(time.Hour) }

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

// assertImageBundle holds the agent image to shipping the CA bundle it verifies the API with. The
// agent adds agent.reportCABundle's certificates to that bundle rather than replacing it, so an image
// whose bundle is missing, moved or emptied would still trust this test's receiver while failing
// every customer against the real API: only this check sees it.
func (h *harness) assertImageBundle() {
	h.t.Helper()
	container := strings.TrimSpace(h.run("docker", "create", agentImage+":"+imageTag))
	defer func() {
		// -v also removes the anonymous volume the image's VOLUME gives the container.
		if out, err := exec.Command("docker", "rm", "-v", container).CombinedOutput(); err != nil {
			h.t.Logf("docker rm: %v\n%s", err, out)
		}
	}()
	file := filepath.Join(h.dir, "ca-certificates.crt")
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
func (h *harness) assertVolumeClaimGuard() {
	h.t.Helper()
	render := func(args ...string) (string, error) {
		return h.try("helm", append([]string{"template", release, h.chart, "--namespace", "costfluent",
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
		got, err := h.receiverReports()
		if err != "" {
			last = err
			continue
		}
		for _, body := range got.Reports {
			report, problem := check(body, h.storageClass)
			if problem == "" {
				return report
			}
			last = problem
		}
	}
	h.t.Fatalf("no acceptable report within %s; last: %s", reportWithin, last)
	return v1.Report{}
}

// awaitReportAfter polls the receiver until it holds the report of the window the agent's shutdown
// closed: from first's agent instance, ending at or after stopped and before the clock would have
// closed it, and starting where an earlier report of that instance ended, so no usage between the
// two is lost. It fails the test when no such report arrives within within of now.
func (h *harness) awaitReportAfter(first v1.Report, stopped time.Time, within time.Duration) v1.Report {
	h.t.Helper()
	deadline := time.Now().Add(within)
	last := "no report ends after the stop"
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
		got, err := h.receiverReports()
		if err != "" {
			last = err
			continue
		}
		ends := map[time.Time]bool{}
		var after []v1.Report
		for _, body := range got.Reports {
			report, problem := check(body, h.storageClass)
			switch {
			case report.AgentInstanceID != first.AgentInstanceID:
			case problem != "":
				last = problem
			case report.WindowEnd.Before(stopped):
				ends[report.WindowEnd.UTC()] = true
			default:
				after = append(after, report)
			}
		}
		for _, report := range after {
			clockEnd := nextHour(report.WindowStart)
			switch {
			case !report.WindowEnd.Before(clockEnd):
				last = fmt.Sprintf("the window %s to %s ends by the clock, not at the stop at %s",
					report.WindowStart.Format(time.RFC3339), report.WindowEnd.Format(time.RFC3339Nano), stopped.Format(time.RFC3339Nano))
			case report.WindowStart.Before(first.WindowEnd) || !ends[report.WindowStart.UTC()]:
				last = fmt.Sprintf("the shutdown's window starts at %s, where no earlier report of the agent ends",
					report.WindowStart.Format(time.RFC3339Nano))
			default:
				return report
			}
		}
	}
	h.t.Fatalf("the stopped agent sent no report of its open window within %s of the uninstall; last: %s", within, last)
	return v1.Report{}
}

// postReceiver sends an empty POST to the receiver's path through the API server's service proxy.
func (h *harness) postReceiver(path string) {
	h.t.Helper()
	cmd := exec.CommandContext(h.ctx, "kubectl", "create", "--raw", "/api/v1/namespaces/e2e/services/receiver:8080/proxy/"+path, "-f", "-")
	cmd.Env = append(os.Environ(), "KUBECONFIG="+h.kubeconfig)
	cmd.Stdin = strings.NewReader("")
	if out, err := cmd.CombinedOutput(); err != nil {
		h.t.Fatalf("POST receiver /%s: %v\n%s", path, err, out)
	}
}

// received is what the receiver accepted: each report's body and, at the same index, the address
// of the peer that sent it.
type received struct {
	Reports  []json.RawMessage `json:"reports"`
	Peers    []string          `json:"peers"`
	Refusals []string          `json:"refusals"`
}

// receiverReports returns what the receiver accepted, or why it could not be read. It fails the
// test when the receiver refused any request of the agent's.
func (h *harness) receiverReports() (received, string) {
	h.t.Helper()
	if err := h.ctx.Err(); err != nil {
		h.t.Fatalf("waiting for a report: %v", err)
	}
	raw, err := h.try("kubectl", "get", "--raw", "/api/v1/namespaces/e2e/services/receiver:8080/proxy/reports")
	if err != nil {
		return received{}, raw
	}
	var got received
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		h.t.Fatalf("receiver answered %q: %v", raw, err)
	}
	if len(got.Refusals) > 0 {
		h.t.Fatalf("the receiver refused the agent's requests: %v", got.Refusals)
	}
	if len(got.Peers) != len(got.Reports) {
		h.t.Fatalf("the receiver holds %d reports and %d peers", len(got.Reports), len(got.Peers))
	}
	return got, ""
}

// decode reads body into api/v1 as the API does, refusing members it does not know, and checks
// the header every report carries.
func decode(body json.RawMessage) (v1.Report, string) {
	var r v1.Report
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return r, fmt.Sprintf("report does not decode into api/v1: %v", err)
	}
	return r, checkHeader(r, agentVersion)
}

// checkHeader holds r to the header every report carries, from an agent built as version.
func checkHeader(r v1.Report, version string) string {
	if r.SchemaVersion != v1.SchemaVersion || r.ClusterID != clusterID || r.AgentInstanceID == "" || r.AgentVersion != version {
		return fmt.Sprintf("header: schema %d, cluster %q, instance %q, agent version %q (built with %q)",
			r.SchemaVersion, r.ClusterID, r.AgentInstanceID, r.AgentVersion, version)
	}
	return ""
}

// check holds a report to carrying usage: nodes, kube-system pods with CPU and memory, and the
// agent's own claim, provisioned from storageClass.
func check(body json.RawMessage, storageClass string) (v1.Report, string) {
	r, problem := decode(body)
	if problem != "" {
		return r, problem
	}
	if len(r.Nodes) == 0 {
		return r, fmt.Sprintf("no nodes; unscraped_nodes %v", r.UnscrapedNodes)
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
	if problem := checkMemory(r); problem != "" {
		return r, problem
	}
	return r, checkAgentClaim(r, storageClass)
}

// checkMemory holds the report to the working set the kubelet measures beside CPU: every container
// that used CPU carries memory byte-seconds and a peak, and so do a kube-system container and the
// agent's own. Both figures come from the same /metrics/resource scrape and container key as CPU, so
// a container with CPU and no memory is a memory sample the parser or aggregator lost, which would
// otherwise reach the report as a zero working set with no other signal.
func checkMemory(r v1.Report) string {
	var kubeSystem, agent bool
	for _, p := range r.Pods {
		for _, c := range p.Containers {
			hasMemory := c.MemoryUsageByteSeconds > 0 && c.MemoryUsagePeakBytes > 0
			if c.CPUUsageCoreSeconds > 0 && !hasMemory {
				return fmt.Sprintf("container %s/%s/%s used %g CPU core-seconds but reports memory of %g byte-seconds, peak %d bytes",
					p.Namespace, p.Name, c.Name, c.CPUUsageCoreSeconds, c.MemoryUsageByteSeconds, c.MemoryUsagePeakBytes)
			}
			if hasMemory {
				kubeSystem = kubeSystem || p.Namespace == "kube-system"
				agent = agent || p.Namespace == "costfluent"
			}
		}
	}
	if !kubeSystem {
		return "no kube-system container reports memory usage"
	}
	if !agent {
		return "the agent's own container reports no memory usage"
	}
	return ""
}

// checkAgentClaim holds the report to the agent's own buffer volume: a Bound volume of storageClass
// serving the StatefulSet's data claim, which the agent's pod mounts.
func checkAgentClaim(r v1.Report, storageClass string) string {
	claim := "data-" + agentFullname + "-0"
	var volume *v1.Volume
	for i := range r.Volumes {
		if r.Volumes[i].ClaimNamespace == "costfluent" && r.Volumes[i].ClaimName == claim {
			volume = &r.Volumes[i]
		}
	}
	if volume == nil {
		return fmt.Sprintf("no volume serves the agent's claim %s among %d volumes", claim, len(r.Volumes))
	}
	if volume.Phase != "Bound" || volume.StorageClass != storageClass || volume.CapacityBytes <= 0 || volume.ObservedSeconds <= 0 {
		return fmt.Sprintf("the agent's volume = %+v, want Bound, of class %s, sized and observed", *volume, storageClass)
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

// workloads is the receiver behind certPEM and keyPEM, and fillers pods of a filler Deployment.
func workloads(certPEM, keyPEM []byte, fillers int) string {
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
# TestKindSmoke brings the cluster to about 60 pods, the size the chart README's sizing is measured at.
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
`, receiverImage+":"+imageTag, token, fillers,
		base64.StdEncoding.EncodeToString(certPEM), base64.StdEncoding.EncodeToString(keyPEM))
}
