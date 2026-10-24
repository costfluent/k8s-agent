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
	"k8s.io/client-go/rest"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/collector"
	"github.com/costfluent/k8s-agent/internal/config"
	agentmetrics "github.com/costfluent/k8s-agent/internal/metrics"
	"github.com/costfluent/k8s-agent/internal/reporter"
	"github.com/costfluent/k8s-agent/internal/storage"
)

// The budgets. Peak heap leaves the Go runtime's own overhead room under the chart's 256Mi limit.
// The byte caps are the API's own for one report body.
const (
	maxHeapBytes        = 200 << 20
	chartMemoryLimit    = 256 << 20
	maxSecondPollGETs   = 50
	minCreditedShare    = 0.95
	apiMaxCompressed    = 16 << 20
	apiMaxDecompressed  = 128 << 20
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
	body, err := io.ReadAll(io.LimitReader(r.Body, apiMaxCompressed+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rc.compressed = len(body)
	if len(body) > apiMaxCompressed {
		http.Error(w, "compressed body over the cap", http.StatusRequestEntityTooLarge)
		return
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(zr, apiMaxDecompressed+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rc.decompressed = len(raw)
	if len(raw) > apiMaxDecompressed {
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

func TestLoad_AgentAtTheAPICaps(t *testing.T) {
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
	// on, and the soft memory limit from the chart's default 256Mi limit, on top of what the fake
	// API server and kubelets in this process hold.
	t.Setenv("COSTFLUENT_TOKEN", "cf_org_load")
	t.Setenv("COSTFLUENT_CLUSTER_ID", "load-cluster")
	t.Setenv("COSTFLUENT_COLLECT_NAMESPACE_LABELS", "true")
	t.Setenv("COSTFLUENT_MEMORY_LIMIT", strconv.Itoa(chartMemoryLimit))
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
	res := reporter.NewClient(endpoint, "cf_org_load", "load", nil).Send(ctx, entry.Body)
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
	t.Logf("report: %d nodes, %d pod entries; %.1f MiB decompressed, %.2f MiB gzip; %.0f bytes per pod entry",
		len(closed.Nodes), len(closed.Pods), mib(uint64(rc.decompressed)), mib(uint64(rc.compressed)),
		float64(rc.decompressed-len(podless))/float64(max(len(closed.Pods), 1)))
	t.Logf("credited %.0f s of a %.0f s window (%.1f%%); window closed by a poll: %v", credited, wall, 100*credited/wall, windowClosed)

	if peakHeap > maxHeapBytes {
		t.Errorf("peak heap %.1f MiB, over the %d MiB budget", mib(peakHeap), maxHeapBytes>>20)
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
	if n := testutil.ToFloat64(agentmetrics.NodeScrapes.WithLabelValues("error")) - scrapeErrorsBefore; n > 0 {
		t.Errorf("%v kubelet scrapes failed", n)
	}
}

func mib(b uint64) float64 { return float64(b) / (1 << 20) }
