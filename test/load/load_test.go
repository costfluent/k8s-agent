//go:build load

// Package load runs the agent's own poll, aggregate and report path against a fake API server and
// fake kubelets at the API's caps: 1,000 nodes and 50,000 pod entries in one window. It holds the
// agent to the budgets that keep that cluster within the chart's memory limit and the API's
// limits. Run it with make load.
package load

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/collector"
	"github.com/costfluent/k8s-agent/internal/config"
	agentmetrics "github.com/costfluent/k8s-agent/internal/metrics"
	"github.com/costfluent/k8s-agent/internal/reporter"
	"github.com/costfluent/k8s-agent/internal/storage"
)

// The budgets of the agent's own work, and the response delay of the simulated kubelets.
const (
	maxSecondPollGETs   = 50
	minCreditedShare    = 0.95
	defaultKubeletDelay = 25 * time.Millisecond
)

// heapSampler records the peak of live and not yet swept heap objects, per phase.
type heapSampler struct {
	mu       sync.Mutex
	peak     uint64
	phase    string
	baseline uint64
	peaks    []phasePeak
	stop     chan struct{}
	done     chan struct{}
}

type phasePeak struct {
	phase string
	bytes uint64
}

func heapObjects() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

func startSampler(baseline uint64) *heapSampler {
	h := &heapSampler{baseline: baseline, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(h.done)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-tick.C:
				v := heapObjects()
				h.mu.Lock()
				h.peak = max(h.peak, v)
				h.mu.Unlock()
			}
		}
	}()
	return h
}

// enter closes the current phase and starts the next one.
func (h *heapSampler) enter(phase string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.phase != "" {
		h.peaks = append(h.peaks, phasePeak{h.phase, h.peak - min(h.peak, h.baseline)})
	}
	h.phase, h.peak = phase, heapObjects()
}

func (h *heapSampler) finish() []phasePeak {
	close(h.stop)
	<-h.done
	h.enter("")
	return h.peaks
}

// receiver holds a report to the API's limits: the compressed and decompressed byte caps, the
// count caps and the map caps.
type receiver struct {
	t                        *testing.T
	compressed, decompressed int
	report                   v1.Report
}

func (rc *receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, v1.MaxCompressedBytes+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rc.compressed = len(body)
	if len(body) > v1.MaxCompressedBytes {
		http.Error(w, "compressed body over the cap", http.StatusRequestEntityTooLarge)
		return
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(zr, v1.MaxDecompressedBytes+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rc.decompressed = len(raw)
	if len(raw) > v1.MaxDecompressedBytes {
		http.Error(w, "decompressed body over the cap", http.StatusRequestEntityTooLarge)
		return
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rc.report); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := rc.report.CheckSize(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := mapsFit(rc.report); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func mapsFit(r v1.Report) error {
	check := func(what string, m map[string]string) error {
		if len(m) > v1.MaxMapEntries {
			return fmt.Errorf("%s has %d entries", what, len(m))
		}
		return nil
	}
	for _, n := range r.Nodes {
		if err := check("node "+n.Name+" labels", n.Labels); err != nil {
			return err
		}
	}
	for _, v := range r.Volumes {
		for what, m := range map[string]map[string]string{"class annotations": v.StorageClassAnnotations, "claim labels": v.ClaimLabels} {
			if err := check("volume "+v.Name+" "+what, m); err != nil {
				return err
			}
		}
	}
	for _, p := range r.Pods {
		for what, m := range map[string]map[string]string{"labels": p.Labels, "annotations": p.Annotations, "namespace labels": p.NamespaceLabels} {
			if err := check("pod "+p.Name+" "+what, m); err != nil {
				return err
			}
		}
	}
	return nil
}

type pollResult struct {
	duration  time.Duration
	requests  int64
	ownerGETs int64
	pods      int
}

// chartMemoryLimit returns the memory limit the chart gives the agent's pod by default:
// resources.limits.memory in its values.yaml. The chart is LOAD_CHART when set, else the sibling
// helm-charts checkout.
func chartMemoryLimit(t *testing.T) int64 {
	t.Helper()
	chart := os.Getenv("LOAD_CHART")
	if chart == "" {
		chart = filepath.Join("..", "..", "..", "helm-charts", "costfluent-k8s-agent")
	}
	path := filepath.Join(chart, "values.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("no chart values at %s (set LOAD_CHART): %v", path, err)
	}
	var values struct {
		Resources struct {
			Limits struct {
				Memory string `json:"memory"`
			} `json:"limits"`
		} `json:"resources"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatalf("parsing %s (set LOAD_CHART): %v", path, err)
	}
	if values.Resources.Limits.Memory == "" {
		t.Fatalf("%s sets no resources.limits.memory (set LOAD_CHART)", path)
	}
	limit, err := resource.ParseQuantity(values.Resources.Limits.Memory)
	if err != nil {
		t.Fatalf("resources.limits.memory in %s (set LOAD_CHART): %v", path, err)
	}
	return limit.Value()
}

func TestLoad_AgentAtTheAPICaps(t *testing.T) {
	memoryLimit := chartMemoryLimit(t)
	// Peak heap may take 25/32 of the limit, 200 MiB of 256Mi, which leaves the Go runtime's own
	// overhead room under it.
	maxHeapBytes := uint64(memoryLimit) * 25 / 32
	latency := defaultKubeletDelay
	if v := os.Getenv("LOAD_KUBELET_LATENCY"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("LOAD_KUBELET_LATENCY: %v", err)
		}
		latency = d
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kube := startKubelets(t, latency)
	api := newFakeAPI(t, func(i int) int { return kube.ports[i] })
	kube.api = api
	apiURL := serveAPI(t, api)
	rc := &receiver{t: t}
	receiverSrv := httptest.NewServer(rc)
	defer receiverSrv.Close()

	runtime.GC()
	baseline := heapObjects()
	heap := startSampler(baseline)

	// The agent's own configuration with its defaults, as the chart installs it: namespace labels
	// on, and the soft memory limit from the chart's default limit, on top of what the fake API
	// server and kubelets in this process hold.
	t.Setenv("COSTFLUENT_TOKEN", "cf_org_load")
	t.Setenv("COSTFLUENT_CLUSTER_ID", "load-cluster")
	t.Setenv("COSTFLUENT_COLLECT_NAMESPACE_LABELS", "true")
	t.Setenv("COSTFLUENT_MEMORY_LIMIT", strconv.FormatInt(memoryLimit, 10))
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	debug.SetMemoryLimit(int64(baseline) + cfg.GoMemoryLimit())
	defer debug.SetMemoryLimit(math.MaxInt64)
	client, err := collector.NewClient(&rest.Config{Host: apiURL}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	heap.enter("informer sync")
	syncStart := time.Now()
	metadata := collector.NewMetadata(client, cfg, zap.NewNop())
	if err := metadata.Start(ctx); err != nil {
		t.Fatal(err)
	}
	syncTook := time.Since(syncStart)
	// Storage syncs beside nodes and pods rather than before them; the polls measured here read it.
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(10 * time.Millisecond) {
		if _, ok := metadata.Volumes(ctx); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the storage informers did not sync within a minute")
		}
	}
	kubeletClient := &http.Client{Transport: &http.Transport{DialContext: kube.dial, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, MaxIdleConnsPerHost: 2}}
	kubelet := collector.NewKubeletCollector(kubeletClient, []corev1.NodeAddressType{corev1.NodeInternalIP}, cfg.ScrapeConcurrency, zap.NewNop())
	scrapeErrorsBefore := testutil.ToFloat64(agentmetrics.NodeScrapes.WithLabelValues("error"))

	interval := cfg.PollingInterval
	// The window is a recent hour, because the buffer's age cap runs on the real clock.
	hour := time.Now().UTC().Truncate(time.Hour)
	at := hour.Add(-interval * 3 / 2)
	aggregator := collector.NewAggregator("load-cluster", "load", "load-instance", interval, at)

	var polls []pollResult
	var closed *v1.Report
	var churned []int
	for poll := 0; poll < 3 && closed == nil; poll++ {
		if poll > 0 {
			churned = api.churnJobs(poll-1, churned)
			time.Sleep(time.Second)
		}
		heap.enter(fmt.Sprintf("poll %d", poll))
		requests, owners := api.requests.Load(), api.ownerGET.Load()
		began := time.Now()
		obs, err := collector.Collect(ctx, metadata, kubelet, at)
		took := time.Since(began)
		if err != nil {
			t.Fatalf("poll %d: %v", poll, err)
		}
		polls = append(polls, pollResult{took, api.requests.Load() - requests, api.ownerGET.Load() - owners, len(obs.Pods)})
		if reports := aggregator.Observe(obs); len(reports) > 0 {
			closed = &reports[0]
		}
		at = at.Add(max(interval, took))
	}

	for i, p := range polls {
		t.Logf("poll %d: %s, %d API requests (%d owner GETs), %d pods", i, p.duration.Round(time.Millisecond), p.requests, p.ownerGETs, p.pods)
	}
	t.Logf("informer sync %s", syncTook.Round(time.Millisecond))

	heap.enter("report")
	windowClosed := closed != nil
	if closed == nil {
		r, ok := aggregator.Finish(at)
		if !ok {
			t.Fatal("no window closed and the open one saw nothing")
		}
		closed = &r
	}
	buffer, err := storage.Open(t.TempDir(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := buffer.Put(*closed); err != nil {
		t.Fatal(err)
	}
	entry, ok, err := buffer.Oldest()
	if err != nil || !ok {
		t.Fatalf("reading the buffered report: %v %v", ok, err)
	}
	// The receiver stands in for the API, which inflates and decodes the body in another process,
	// so its memory is not the agent's: sampling stops before the send.
	peaks := heap.finish()
	endpoint, _ := url.Parse(receiverSrv.URL)
	res := reporter.NewClient(endpoint, "cf_org_load", "load", nil, nil).Send(ctx, entry.Body)
	if out := os.Getenv("LOAD_SAMPLE_OUT"); out != "" {
		if err := os.WriteFile(out, entry.Body, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote the gzip report body to %s", out)
	}
	runtime.GC()
	retained := heapObjects() - min(heapObjects(), baseline)

	wall := closed.WindowEnd.Sub(closed.WindowStart).Seconds()
	credited := 0.0
	if len(closed.Nodes) > 0 {
		credited = closed.Nodes[0].ObservedSeconds
	}
	withoutPods := *closed
	withoutPods.Pods = []v1.Pod{}
	podless, err := json.Marshal(withoutPods)
	if err != nil {
		t.Fatal(err)
	}
	var peakHeap uint64
	for _, p := range peaks {
		t.Logf("peak heap above baseline, %-13s %7.1f MiB", p.phase, mib(p.bytes))
		peakHeap = max(peakHeap, p.bytes)
	}
	t.Logf("heap retained after the run %.1f MiB", mib(retained))
	t.Logf("report: %d nodes, %d pod entries, %d volumes; %.1f MiB decompressed, %.2f MiB gzip; %.0f bytes per pod entry",
		len(closed.Nodes), len(closed.Pods), len(closed.Volumes), mib(uint64(rc.decompressed)), mib(uint64(rc.compressed)),
		float64(rc.decompressed-len(podless))/float64(max(len(closed.Pods), 1)))
	t.Logf("credited %.0f s of a %.0f s window (%.1f%%); window closed by a poll: %v", credited, wall, 100*credited/wall, windowClosed)

	if peakHeap > maxHeapBytes {
		t.Errorf("peak heap %.1f MiB, over the %.1f MiB budget", mib(peakHeap), mib(maxHeapBytes))
	}
	if len(closed.Nodes) != v1.MaxNodes {
		t.Errorf("report carries %d nodes, want the API's cap of %d", len(closed.Nodes), v1.MaxNodes)
	}
	if res.Outcome != reporter.Accepted {
		t.Errorf("the report was refused: %d %s", res.Status, res.Message)
	}
	if len(polls) > 1 && polls[1].requests >= maxSecondPollGETs {
		t.Errorf("the second poll made %d API requests, over the %d budget", polls[1].requests, maxSecondPollGETs)
	}
	if !windowClosed || credited < minCreditedShare*wall {
		t.Errorf("credited %.0f s of %.0f s wall, under %.0f%%", credited, wall, 100*minCreditedShare)
	}
	claimed := map[string]bool{}
	for _, p := range closed.Pods {
		for _, c := range p.Claims {
			claimed[p.Namespace+"/"+c] = true
		}
	}
	if len(closed.Volumes) != statefulSetPods || len(claimed) != statefulSetPods {
		t.Errorf("report carries %d volumes and %d claimed by pods, want one for each of the %d StatefulSet pods", len(closed.Volumes), len(claimed), statefulSetPods)
	}
	for _, v := range closed.Volumes {
		if v.Phase != "Bound" || v.VolumeHandle == "" || v.ObservedSeconds <= 0 || !claimed[v.ClaimNamespace+"/"+v.ClaimName] {
			t.Errorf("volume %+v is not a bound EBS volume observed in the window and mounted by its pod", v)
			break
		}
	}
	if n := testutil.ToFloat64(agentmetrics.NodeScrapes.WithLabelValues("error")) - scrapeErrorsBefore; n > 0 {
		t.Errorf("%v kubelet scrapes failed", n)
	}
}

func mib(b uint64) float64 { return float64(b) / (1 << 20) }
