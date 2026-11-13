//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	v1 "github.com/costfluent/k8s-agent/api/v1"
)

const (
	// helmRepository is the repository the chart README's install and upgrade commands name.
	helmRepository = "https://costfluent.github.io/helm-charts"
	chartName      = "costfluent-k8s-agent"
	// publishedImage is the repository the published agent image is tagged into and loaded as, so
	// the published release runs offline with pullPolicy Never, as the tree's does.
	publishedImage = "costfluent-e2e/k8s-agent-published"
	agentPod       = agentFullname + "-0"
	agentClaim     = "data-" + agentPod
	// upgradeWithin runs from the receiver's return. It holds the sender's retry of the buffered
	// window, whose backoff starts at 30 s, and the new agent's first window of two minutes.
	upgradeWithin = 4 * time.Minute
)

// published is the newest release in the public Helm repository: the package and the agent image
// it pins.
type published struct {
	chart      string
	version    string
	appVersion string
	repository string
	digest     string
}

// TestKindUpgrade installs the newest published release and upgrades it to the tree's chart and
// agent with the command a customer runs, helm upgrade --install on the same release. The receiver
// is unavailable for the upgrade, so the old agent's shutdown buffers the window it closes on the
// volume and only the new agent can send it. The leg holds the tree to an upgrade the API server
// accepts, which no change to an immutable StatefulSet field passes, to the new pod keeping the
// claim, and to the new agent reading what the old one wrote there: the buffered window and the
// agent instance ID.
func TestKindUpgrade(t *testing.T) {
	h := newHarness(t, 14*time.Minute)
	pub := h.publishedRelease()
	image := h.pullPublishedImage(pub)
	cluster := h.kindCluster("costfluent-e2e-upgrade", "")
	h.buildImages("receiver")
	h.run("kind", "load", "docker-image", "--name", cluster, receiverImage+":"+imageTag, agentImage+":"+imageTag)
	h.loadPublishedImage(cluster, image)
	h.deployReceiver(0)

	pubVersion := "v" + pub.appVersion
	// kind kubelets serve self-signed certificates.
	installed := h.installChart(pub.chart, publishedImage, pub.appVersion, "agent.disableKubeTLSverify=true", "image.digest=")
	first := h.awaitPublishedReport(installed, pubVersion)
	claimUID := h.uid("pvc", agentClaim)
	podUID := h.uid("pod", agentPod)
	t.Logf("published chart %s, agent %s: first report after %s, instance %s, window %s to %s",
		pub.version, pubVersion, time.Since(installed).Round(time.Second), first.AgentInstanceID,
		first.WindowStart.Format(time.RFC3339), first.WindowEnd.Format(time.RFC3339))

	// As assertShutdownReport waits: the open window holds samples, and the clock does not close it
	// during the upgrade.
	h.sleepUntil(first.WindowEnd.Add(3 * pollEvery * time.Second))
	if now := time.Now(); nextHour(now).Sub(now) < hourMargin {
		h.sleepUntil(nextHour(now).Add(3 * pollEvery * time.Second))
	}
	h.postReceiver("unavailable")
	upgradeStart := time.Now()
	t.Logf("upgrading release %s from the published chart %s to the tree's chart", release, pub.version)
	h.installAgent("agent.disableKubeTLSverify=true")
	h.assertUpgraded(podUID, claimUID)
	h.postReceiver("available")
	h.awaitUpgradeReports(first, pubVersion, upgradeStart)
}

// publishedRelease resolves the newest published chart, or the package E2E_PUBLISHED_CHART names,
// and the agent image it pins. A package without a version, appVersion, image repository or digest
// fails the leg: without them it is not the release customers install.
func (h *harness) publishedRelease() published {
	h.t.Helper()
	pkg := os.Getenv("E2E_PUBLISHED_CHART")
	if pkg == "" {
		dir := filepath.Join(h.dir, "published")
		// helm pull does not create its destination.
		if err := os.Mkdir(dir, 0o755); err != nil {
			h.t.Fatal(err)
		}
		h.retry("helm", "pull", chartName, "--repo", helmRepository, "--destination", dir)
		found, err := filepath.Glob(filepath.Join(dir, chartName+"-*.tgz"))
		if err != nil || len(found) != 1 {
			h.t.Fatalf("helm pull %s from %s left %v in %s (%v), want one package", chartName, helmRepository, found, dir, err)
		}
		pkg = found[0]
	}
	var chart struct {
		Version    string `json:"version"`
		AppVersion string `json:"appVersion"`
	}
	if err := yaml.Unmarshal([]byte(h.run("helm", "show", "chart", pkg)), &chart); err != nil {
		h.t.Fatalf("helm show chart %s: %v", pkg, err)
	}
	var values struct {
		Image struct {
			Repository string `json:"repository"`
			Digest     string `json:"digest"`
		} `json:"image"`
	}
	if err := yaml.Unmarshal([]byte(h.run("helm", "show", "values", pkg)), &values); err != nil {
		h.t.Fatalf("helm show values %s: %v", pkg, err)
	}
	p := published{chart: pkg, version: chart.Version, appVersion: chart.AppVersion,
		repository: values.Image.Repository, digest: values.Image.Digest}
	if p.version == "" || p.appVersion == "" || p.repository == "" || p.digest == "" {
		h.t.Fatalf("published chart %s: version %q, appVersion %q, image.repository %q, image.digest %q; each is required",
			pkg, p.version, p.appVersion, p.repository, p.digest)
	}
	return p
}

// pullPublishedImage pulls the image the published chart pins, by its digest and for this machine's
// platform, and tags it as publishedImage at the chart's appVersion. E2E_GHCR_MIRROR, a registry
// host and project such as registry.example.com/ghcr that proxies ghcr.io, replaces ghcr.io in the
// reference, as CI must.
func (h *harness) pullPublishedImage(p published) string {
	h.t.Helper()
	ref := p.repository + "@" + p.digest
	if mirror := os.Getenv("E2E_GHCR_MIRROR"); mirror != "" {
		host, project, ok := strings.Cut(mirror, "/")
		if !ok || host == "" || project == "" {
			h.t.Fatalf("E2E_GHCR_MIRROR=%q is not <registry host>/<project>", mirror)
		}
		path, ok := strings.CutPrefix(p.repository, "ghcr.io/")
		if !ok {
			h.t.Fatalf("E2E_GHCR_MIRROR proxies ghcr.io, but the published chart pins %s", p.repository)
		}
		ref = mirror + "/" + path + "@" + p.digest
	}
	h.retry("docker", "pull", "--platform", "linux/"+runtime.GOARCH, ref)
	image := publishedImage + ":" + p.appVersion
	h.run("docker", "tag", ref, image)
	return image
}

// loadPublishedImage loads image into the kind cluster. The published image is a multi-platform
// index of which only this platform was pulled, which kind load docker-image cannot save from a
// containerd image store, so a failed load falls back to an archive of this platform alone.
func (h *harness) loadPublishedImage(cluster, image string) {
	h.t.Helper()
	out, err := h.try("kind", "load", "docker-image", "--name", cluster, image)
	if err == nil {
		return
	}
	platform := "linux/" + runtime.GOARCH
	h.t.Logf("kind load docker-image %s failed, loading a %s archive instead: %v\n%s", image, platform, err, out)
	archive := filepath.Join(h.dir, "published.tar")
	h.run("docker", "save", "--platform", platform, "-o", archive, image)
	h.run("kind", "load", "image-archive", "--name", cluster, archive)
}

// retry runs a command that reads the network up to three times, 10 s apart, so a blip of the
// registry does not fail the leg.
func (h *harness) retry(name string, args ...string) string {
	h.t.Helper()
	const attempts = 3
	var (
		out string
		err error
	)
	for i := range attempts {
		if i > 0 {
			h.sleepUntil(time.Now().Add(10 * time.Second))
		}
		if out, err = h.try(name, args...); err == nil {
			return out
		}
	}
	h.t.Fatalf("%s %s failed %d times: %v\n%s", name, strings.Join(args, " "), attempts, err, out)
	return ""
}

// uid returns the UID of the named object of kind in the agent's namespace.
func (h *harness) uid(kind, name string) string {
	h.t.Helper()
	uid := strings.TrimSpace(h.run("kubectl", "-n", "costfluent", "get", kind, name, "-o", "jsonpath={.metadata.uid}"))
	if uid == "" {
		h.t.Fatalf("%s %s has no UID", kind, name)
	}
	return uid
}

// awaitPublishedReport polls the receiver until it holds a report of the published agent, held to
// its header alone, or reportWithin of since passes.
func (h *harness) awaitPublishedReport(since time.Time, version string) v1.Report {
	h.t.Helper()
	last := "no report"
	for time.Since(since) < reportWithin {
		time.Sleep(5 * time.Second)
		got, err := h.receiverReports()
		if err != "" {
			last = err
			continue
		}
		for _, body := range got.Reports {
			report, problem := decodeHeader(body, version)
			if problem == "" {
				return report
			}
			last = problem
		}
	}
	h.t.Fatalf("no report of the published agent within %s; last: %s", reportWithin, last)
	return v1.Report{}
}

// decodeHeader reads a report of the published agent and holds it to the header every report
// carries. It neither refuses members the tree's api/v1 no longer has nor applies check: the tree
// may add checks and drop fields, and the published agent is held to what it shipped with.
func decodeHeader(body json.RawMessage, version string) (v1.Report, string) {
	var r v1.Report
	if err := json.Unmarshal(body, &r); err != nil {
		return r, fmt.Sprintf("report does not decode into api/v1: %v", err)
	}
	return r, checkHeader(r, version)
}

// assertUpgraded holds the release to an upgrade in place: a second revision of the release, a new
// pod running the tree's agent image, and the same claim, which the new pod mounts.
func (h *harness) assertUpgraded(podUID, claimUID string) {
	h.t.Helper()
	var history []struct {
		Revision int    `json:"revision"`
		Status   string `json:"status"`
	}
	out := h.run("helm", "history", release, "--namespace", "costfluent", "-o", "json")
	if err := json.Unmarshal([]byte(out), &history); err != nil {
		h.t.Fatalf("helm history %s: %v\n%s", release, err, out)
	}
	if len(history) != 2 || history[1].Revision != 2 || history[1].Status != "deployed" {
		h.t.Fatalf("helm history %s = %+v, want revision 1 and a deployed revision 2: the release was not upgraded in place", release, history)
	}

	var pod struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
		Spec struct {
			Containers []struct {
				Name  string `json:"name"`
				Image string `json:"image"`
			} `json:"containers"`
			Volumes []struct {
				Name                  string `json:"name"`
				PersistentVolumeClaim *struct {
					ClaimName string `json:"claimName"`
				} `json:"persistentVolumeClaim"`
			} `json:"volumes"`
		} `json:"spec"`
	}
	out = h.run("kubectl", "-n", "costfluent", "get", "pod", agentPod, "-o", "json")
	if err := json.Unmarshal([]byte(out), &pod); err != nil {
		h.t.Fatalf("pod %s: %v\n%s", agentPod, err, out)
	}
	if pod.Metadata.UID == podUID {
		h.t.Fatalf("pod %s still has the published release's UID %s: the upgrade did not replace it", agentPod, podUID)
	}
	var image string
	for _, c := range pod.Spec.Containers {
		if c.Name == "agent" {
			image = c.Image
		}
	}
	if want := agentImage + ":" + imageTag; image != want {
		h.t.Fatalf("pod %s runs the agent image %q after the upgrade, want %q", agentPod, image, want)
	}
	var mounted string
	for _, v := range pod.Spec.Volumes {
		if v.Name == "data" && v.PersistentVolumeClaim != nil {
			mounted = v.PersistentVolumeClaim.ClaimName
		}
	}
	if mounted != agentClaim {
		h.t.Fatalf("pod %s mounts claim %q as its data volume after the upgrade, want the published release's %s", agentPod, mounted, agentClaim)
	}
	if uid := h.uid("pvc", agentClaim); uid != claimUID {
		h.t.Fatalf("claim %s has UID %s after the upgrade, want the published release's %s: the buffer and instance ID on it are gone",
			agentClaim, uid, claimUID)
	}
}

// awaitUpgradeReports polls the receiver until it holds both (a) the window the published agent
// closed at its stop, from first's instance, ending at or after the upgrade began and starting where
// an earlier report of that instance ended, which the old pod wrote to the volume and only the new
// one can have sent; and (b) a report of the tree's agent that passes check and carries first's
// instance ID, read from the volume. It fails the test naming each one missing within upgradeWithin.
func (h *harness) awaitUpgradeReports(first v1.Report, version string, upgradeStart time.Time) {
	h.t.Helper()
	deadline := time.Now().Add(upgradeWithin)
	buffered := "(a) no report of the published agent ends after the upgrade began: the window the old pod buffered at its stop was not sent"
	tree := "(b) no report of the tree's agent arrived"
	var gotA, gotB bool
	for !gotA || !gotB {
		if time.Now().After(deadline) {
			var broke []string
			if gotA {
				broke = append(broke, "(a) held")
			} else {
				broke = append(broke, buffered)
			}
			if gotB {
				broke = append(broke, "(b) held")
			} else {
				broke = append(broke, tree)
			}
			h.t.Fatalf("within %s of the receiver's return after the upgrade: %s", upgradeWithin, strings.Join(broke, "; "))
		}
		time.Sleep(5 * time.Second)
		got, err := h.receiverReports()
		if err != "" {
			continue
		}
		ends := map[time.Time]bool{}
		var after []v1.Report
		for _, body := range got.Reports {
			if r, problem := decodeHeader(body, version); r.AgentVersion == version {
				switch {
				case problem != "":
					buffered = "(a) " + problem
				case r.AgentInstanceID != first.AgentInstanceID:
					buffered = fmt.Sprintf("(a) a report of the published agent carries instance %q, not %q", r.AgentInstanceID, first.AgentInstanceID)
				case r.WindowEnd.Before(upgradeStart):
					ends[r.WindowEnd.UTC()] = true
				default:
					after = append(after, r)
				}
				continue
			}
			r, problem := check(body, h.storageClass)
			switch {
			case gotB:
			case problem != "":
				tree = "(b) " + problem
			case r.AgentInstanceID != first.AgentInstanceID:
				tree = fmt.Sprintf("(b) the tree's agent reports as instance %q, not the published agent's %q: it did not read the instance ID on the volume",
					r.AgentInstanceID, first.AgentInstanceID)
			default:
				gotB = true
				h.t.Logf("(b) the tree's agent reports as instance %s: window %s to %s", r.AgentInstanceID,
					r.WindowStart.Format(time.RFC3339), r.WindowEnd.Format(time.RFC3339))
			}
		}
		for _, r := range after {
			switch {
			case gotA:
			case r.WindowStart.Before(first.WindowEnd) || !ends[r.WindowStart.UTC()]:
				buffered = fmt.Sprintf("(a) the published agent's window %s to %s starts where no earlier report of it ends",
					r.WindowStart.Format(time.RFC3339Nano), r.WindowEnd.Format(time.RFC3339Nano))
			default:
				gotA = true
				h.t.Logf("(a) the window the published agent buffered at its stop arrived: %s to %s",
					r.WindowStart.Format(time.RFC3339), r.WindowEnd.Format(time.RFC3339Nano))
			}
		}
	}
}
