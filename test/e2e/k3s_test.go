//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// defaultK3sImage is the k3s release the k3s leg runs. K3S_IMAGE overrides it, as CI does with the
// same digest through a Docker Hub pull-through proxy; the default pulls upstream.
const defaultK3sImage = "rancher/k3s:v1.37.1-k3s1@sha256:ca7f37d993d82ef0dcdcfecb2e0e2618ea541dbaffc620c8cedebe01a82acd0d"

// TestK3s installs on a single-node k3s cluster as the console's "k3s, RKE2 or other self-managed"
// choice does, with no value beyond the token and endpoint, so kubelet verification is on. k3s signs
// its kubelets' serving certificates with the cluster's CA, so the agent must read them and report
// usage; a report without nodes says, through unscraped_nodes, that k3s belongs with the choices
// that turn verification off.
func TestK3s(t *testing.T) {
	h := newHarness(t, 12*time.Minute)
	h.storageClass = "local-path"
	server := h.k3sCluster(fmt.Sprintf("costfluent-e2e-k3s-%d", os.Getpid()))
	h.buildImages("receiver")
	h.k3sLoad(server, receiverImage+":"+imageTag, agentImage+":"+imageTag)
	h.deployReceiver(0)

	installed := h.installAgent()
	report := h.awaitReport(installed)
	t.Logf("report after %s: window %s to %s, %d nodes, %d pods", time.Since(installed).Round(time.Second),
		report.WindowStart.Format(time.RFC3339), report.WindowEnd.Format(time.RFC3339), len(report.Nodes), len(report.Pods))
}

// k3sCluster starts a k3s server as a privileged container, the way k3d does without k3d, so the
// runner needs only docker, and points h.kubeconfig at its API server on a loopback port. Traefik,
// ServiceLB and metrics-server are left out: the agent reads none of them, and each is an image
// pull. K3S_REGISTRY_MIRROR, a registry host and project such as registry.example.com/docker, makes
// k3s pull its own Docker Hub images (pause, CoreDNS, the local-path provisioner and its helper)
// through that proxy, as CI must.
func (h *harness) k3sCluster(name string) string {
	h.t.Helper()
	h.t.Cleanup(func() {
		if keep() {
			h.t.Logf("keeping k3s container %s (KUBECONFIG=%s)", name, h.kubeconfig)
			return
		}
		// -v also removes the anonymous volumes the image declares, which hold the cluster's state.
		if out, err := exec.Command("docker", "rm", "-f", "-v", name).CombinedOutput(); err != nil {
			h.t.Logf("docker rm: %v\n%s", err, out)
		}
	})
	args := []string{"create", "--name", name, "--hostname", name, "--privileged",
		"--tmpfs", "/run", "--tmpfs", "/var/run", "-p", "127.0.0.1::6443",
		env("K3S_IMAGE", defaultK3sImage), "server",
		"--disable=traefik", "--disable=servicelb", "--disable=metrics-server"}
	mirror := os.Getenv("K3S_REGISTRY_MIRROR")
	if mirror != "" {
		args = append(args, "--private-registry=/registries.yaml")
	}
	h.run("docker", args...)
	if mirror != "" {
		host, project, ok := strings.Cut(mirror, "/")
		if !ok || host == "" || project == "" {
			h.t.Fatalf("K3S_REGISTRY_MIRROR=%q is not <registry host>/<project>", mirror)
		}
		// A project-based proxy serves a proxied repository under its project, so every Docker Hub path gains the
		// project as its first segment.
		registries := fmt.Sprintf("mirrors:\n  docker.io:\n    endpoint: [%q]\n    rewrite:\n      %q: %q\n",
			"https://"+host, "^(.*)$", project+"/$1")
		file := filepath.Join(h.dir, "registries.yaml")
		if err := os.WriteFile(file, []byte(registries), 0o644); err != nil {
			h.t.Fatal(err)
		}
		h.run("docker", "cp", file, name+":/registries.yaml")
	}
	h.run("docker", "start", name)

	port := strings.Fields(h.run("docker", "port", name, "6443/tcp"))
	if len(port) == 0 {
		h.t.Fatalf("k3s %s publishes no API server port", name)
	}
	var kubeconfig string
	h.eventually(2*time.Minute, "k3s writes its kubeconfig", func() error {
		out, err := h.try("docker", "exec", name, "cat", "/etc/rancher/k3s/k3s.yaml")
		if err != nil {
			return fmt.Errorf("%w: %s", err, out)
		}
		kubeconfig = out
		return nil
	})
	kubeconfig = strings.ReplaceAll(kubeconfig, "https://127.0.0.1:6443", "https://"+port[0])
	if err := os.WriteFile(h.kubeconfig, []byte(kubeconfig), 0o600); err != nil {
		h.t.Fatal(err)
	}
	h.eventually(3*time.Minute, "the k3s node is Ready", func() error {
		out, err := h.try("kubectl", "wait", "--for=condition=Ready", "nodes", "--all", "--timeout=10s")
		if err != nil {
			return fmt.Errorf("%w: %s", err, out)
		}
		return nil
	})
	// k3s deploys these from its manifests after the API server starts, so they may not exist yet.
	for _, deployment := range []string{"coredns", "local-path-provisioner"} {
		h.eventually(3*time.Minute, deployment+" rolls out", func() error {
			out, err := h.try("kubectl", "-n", "kube-system", "rollout", "status", "deployment/"+deployment, "--timeout=10s")
			if err != nil {
				return fmt.Errorf("%w: %s", err, out)
			}
			return nil
		})
	}
	return name
}

// k3sLoad imports images from the host's Docker into the k3s server's containerd, which kind load
// does for kind.
func (h *harness) k3sLoad(server string, images ...string) {
	h.t.Helper()
	archive := filepath.Join(h.dir, "images.tar")
	h.run("docker", append([]string{"save", "-o", archive}, images...)...)
	h.run("docker", "cp", archive, server+":/images.tar")
	h.run("docker", "exec", server, "ctr", "-n", "k8s.io", "images", "import", "/images.tar")
}

// eventually retries try every 5 seconds until it succeeds, and fails the test with its last error
// after within.
func (h *harness) eventually(within time.Duration, what string, try func() error) {
	h.t.Helper()
	deadline := time.Now().Add(within)
	for {
		err := try()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("waiting until %s for %s: %v", what, within, err)
		}
		h.sleepUntil(time.Now().Add(5 * time.Second))
	}
}
