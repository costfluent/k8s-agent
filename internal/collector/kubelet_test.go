package collector

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/costfluent/k8s-agent/internal/config"
	"github.com/costfluent/k8s-agent/internal/metrics"
)

// testdata/metrics_resource.txt was captured from a Talos v1.14 node (Kubernetes v1.37) with
// kubectl get --raw /api/v1/nodes/<node>/proxy/metrics/resource.
func fixture(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("testdata/metrics_resource.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

const (
	proxyCPU     = `container_cpu_usage_seconds_total{container="kube-proxy",namespace="kube-system",pod="kube-proxy-f5h6w"} 1.545405 1790148978221`
	proxyCPUNext = `container_cpu_usage_seconds_total{container="kube-proxy",namespace="kube-system",pod="kube-proxy-f5h6w"} 4.545405 1790148988221`
	crashCPU     = `container_cpu_usage_seconds_total{container="ceph-crash",namespace="rook-ceph",pod="rook-ceph-crashcollector-dev-cluster-worker-1-84cbb76465-5h6lg"} 0.175665 1790148986919`
	crashCPUNext = `container_cpu_usage_seconds_total{container="ceph-crash",namespace="rook-ceph",pod="rook-ceph-crashcollector-dev-cluster-worker-1-84cbb76465-5h6lg"} 0.05 1790148996919`
	gonePod      = `pod="rook-ceph-mon-b-7c7cc8f655-5h4lt"`
)

func secondScrape(t *testing.T) string {
	s := fixture(t)
	for _, pair := range [][2]string{{proxyCPU, proxyCPUNext}, {crashCPU, crashCPUNext}} {
		if !strings.Contains(s, pair[0]) {
			t.Fatalf("fixture no longer contains %q", pair[0])
		}
		s = strings.Replace(s, pair[0], pair[1], 1)
	}
	var kept []string
	for _, line := range strings.Split(s, "\n") {
		if !strings.Contains(line, gonePod) || !strings.Contains(line, `container="log-collector"`) {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

func kubeletNode(t *testing.T, name, serverURL string) *corev1.Node {
	t.Helper()
	host, port, err := net.SplitHostPort(strings.TrimPrefix(serverURL, "https://"))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := strconv.Atoi(port)
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Addresses:       []corev1.NodeAddress{{Type: corev1.NodeHostName, Address: "unresolvable.invalid"}, {Type: corev1.NodeInternalIP, Address: host}},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: int32(p)}},
		},
	}
}

func TestKubeletCollector_RatesResetsMissingContainersAndSlowNodes(t *testing.T) {
	var calls atomic.Int32
	first, second := fixture(t), secondScrape(t)
	var sawAuth atomic.Bool
	healthy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics/resource" {
			http.NotFound(w, r)
			return
		}
		sawAuth.Store(r.Header.Get("Authorization") == "Bearer sa-token")
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(first))
			return
		}
		_, _ = w.Write([]byte(second))
	}))
	defer healthy.Close()
	release := make(chan struct{})
	slow := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	defer close(release)

	client := healthy.Client()
	client.Transport = authTransport{token: "sa-token", next: client.Transport}
	c := NewKubeletCollector(client, config.DefaultNodeAddressTypes, 0, zap.NewNop())
	c.timeout = 200 * time.Millisecond
	nodes := []*corev1.Node{
		kubeletNode(t, "dev-cluster-worker-1", healthy.URL),
		kubeletNode(t, "slow", slow.URL),
		{ObjectMeta: metav1.ObjectMeta{Name: "no-address"}},
	}

	failedBefore := testutil.ToFloat64(metrics.NodeScrapes.WithLabelValues("error"))
	usage, failed := c.Scrape(context.Background(), nodes)
	if got := testutil.ToFloat64(metrics.NodeScrapes.WithLabelValues("error")) - failedBefore; got != 2 {
		t.Fatalf("failed scrapes = %v, want 2 (the slow node and the node without an address)", got)
	}
	if want := map[string]bool{"slow": true, "no-address": true}; !maps.Equal(failed, want) {
		t.Fatalf("unscraped nodes = %v, want %v", failed, want)
	}
	if !sawAuth.Load() {
		t.Fatal("the scrape did not carry the service-account bearer token")
	}
	proxy := ContainerKey{"kube-system", "kube-proxy-f5h6w", "kube-proxy"}
	u, ok := usage[proxy]
	if !ok || u.HasCPU || u.MemoryBytes != 61407232 {
		t.Fatalf("first scrape kube-proxy = %+v %v; want memory and no CPU rate yet", u, ok)
	}
	if len(usage) != 42 {
		t.Fatalf("containers = %d, want the fixture's 42", len(usage))
	}

	usage, _ = c.Scrape(context.Background(), nodes)
	if u := usage[proxy]; !u.HasCPU || math.Abs(u.CPUCores-0.3) > 1e-9 {
		t.Errorf("kube-proxy rate = %+v, want 3 core-seconds over 10s", u)
	}
	crash := ContainerKey{"rook-ceph", "rook-ceph-crashcollector-dev-cluster-worker-1-84cbb76465-5h6lg", "ceph-crash"}
	if u := usage[crash]; !u.HasCPU || math.Abs(u.CPUCores-0.005) > 1e-9 {
		t.Errorf("reset counter rate = %+v, want the new value over 10s", u)
	}
	if _, ok := usage[ContainerKey{"rook-ceph", "rook-ceph-mon-b-7c7cc8f655-5h4lt", "log-collector"}]; ok {
		t.Error("a container missing from the scrape still reported usage")
	}
	unchanged := ContainerKey{"kube-system", "kube-flannel-g585n", "kube-flannel"}
	if u := usage[unchanged]; u.HasCPU {
		t.Errorf("a counter sample seen twice produced a rate: %+v", u)
	}

	usage, _ = c.Scrape(context.Background(), nodes)
	if u := usage[proxy]; !u.HasCPU || math.Abs(u.CPUCores-0.3) > 1e-9 {
		t.Errorf("an unrefreshed sample dropped the last rate: %+v", u)
	}
}

type authTransport struct {
	token string
	next  http.RoundTripper
}

func (a authTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+a.token)
	return a.next.RoundTrip(r)
}

// inFlight is a kubelet that counts concurrent scrapes and holds each for a while, or until
// released when hold is set.
type inFlight struct {
	now, peak atomic.Int32
	hold      chan struct{}
}

func (f *inFlight) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := f.now.Add(1)
	defer f.now.Add(-1)
	for p := f.peak.Load(); n > p && !f.peak.CompareAndSwap(p, n); p = f.peak.Load() {
	}
	if f.hold != nil {
		select {
		case <-f.hold:
		case <-r.Context().Done():
		}
	} else {
		time.Sleep(20 * time.Millisecond)
	}
	_, _ = w.Write([]byte("# TYPE scrape_error gauge\nscrape_error 0\n"))
}

func manyNodes(t *testing.T, n int, url string) []*corev1.Node {
	t.Helper()
	nodes := make([]*corev1.Node, n)
	for i := range nodes {
		nodes[i] = kubeletNode(t, "node-"+strconv.Itoa(i), url)
	}
	return nodes
}

func TestKubeletCollector_ScrapeConcurrencyScalesWithNodesUnderTheCeiling(t *testing.T) {
	for _, tc := range []struct {
		name           string
		nodes, ceiling int
		want           int32
	}{
		{"a small cluster keeps the floor", 100, 0, 10},
		{"a large cluster scales to the default ceiling", 1000, 0, 20},
		{"a configured ceiling holds", 1000, 15, 15},
		{"a ceiling below the floor holds", 100, 4, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kubelet := &inFlight{}
			srv := httptest.NewTLSServer(kubelet)
			defer srv.Close()
			c := NewKubeletCollector(srv.Client(), []corev1.NodeAddressType{corev1.NodeInternalIP}, tc.ceiling, zap.NewNop())
			c.Scrape(context.Background(), manyNodes(t, tc.nodes, srv.URL))
			if got := kubelet.peak.Load(); got != tc.want {
				t.Fatalf("%d nodes, ceiling %d: %d scrapes at once, want %d", tc.nodes, tc.ceiling, got, tc.want)
			}
		})
	}
}

func TestKubeletCollector_StartsAScrapeOnlyWhenOneCanRun(t *testing.T) {
	kubelet := &inFlight{hold: make(chan struct{})}
	srv := httptest.NewTLSServer(kubelet)
	defer srv.Close()
	c := NewKubeletCollector(srv.Client(), []corev1.NodeAddressType{corev1.NodeInternalIP}, 10, zap.NewNop())
	nodes := manyNodes(t, 500, srv.URL)
	before := runtime.NumGoroutine()
	done := make(chan struct{})
	go func() {
		c.Scrape(context.Background(), nodes)
		close(done)
	}()
	for kubelet.now.Load() < 10 {
		time.Sleep(time.Millisecond)
	}
	waiting := runtime.NumGoroutine() - before
	close(kubelet.hold)
	<-done
	if waiting > 100 {
		t.Fatalf("%d goroutines while 10 scrapes ran for 500 nodes; each must start only when it can run", waiting)
	}
}

func TestKubeletCollector_CountsATruncatedBody(t *testing.T) {
	body := fixture(t)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
	defer srv.Close()
	c := NewKubeletCollector(srv.Client(), []corev1.NodeAddressType{corev1.NodeInternalIP}, 0, zap.NewNop())
	nodes := []*corev1.Node{kubeletNode(t, "n1", srv.URL)}

	before := testutil.ToFloat64(metrics.KubeletBodiesTruncated)
	c.maxBody = int64(len(body))
	c.Scrape(context.Background(), nodes)
	if got := testutil.ToFloat64(metrics.KubeletBodiesTruncated) - before; got != 0 {
		t.Fatalf("a body exactly at the limit was counted as truncated %v times", got)
	}
	c.maxBody = int64(len(body)) - 1
	c.Scrape(context.Background(), nodes)
	if got := testutil.ToFloat64(metrics.KubeletBodiesTruncated) - before; got != 1 {
		t.Fatalf("truncated bodies = %v, want 1", got)
	}
}

func appMetrics(pod string, cpu float64, at time.Time) string {
	return fmt.Sprintf(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{container="app",namespace="shop",pod=%[1]q} %[2]g %[3]d
# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{container="app",namespace="shop",pod=%[1]q} 100 %[3]d
`, pod, cpu, at.UnixMilli())
}

func TestKubeletCollector_ReportsAFailedNodeAndRatesItAcrossTheGapOnRecovery(t *testing.T) {
	at := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	var flakyCalls, steadyCalls atomic.Int32
	flaky := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch flakyCalls.Add(1) {
		case 1:
			_, _ = w.Write([]byte(appMetrics("web-0", 10, at)))
		case 2:
			http.Error(w, "kubelet unavailable", http.StatusServiceUnavailable)
		default:
			_, _ = w.Write([]byte(appMetrics("web-0", 13, at.Add(20*time.Second))))
		}
	}))
	defer flaky.Close()
	steady := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := steadyCalls.Add(1)
		_, _ = w.Write([]byte(appMetrics("api-0", float64(n), at.Add(time.Duration(n)*10*time.Second))))
	}))
	defer steady.Close()

	c := NewKubeletCollector(flaky.Client(), config.DefaultNodeAddressTypes, 0, zap.NewNop())
	nodes := []*corev1.Node{kubeletNode(t, "flaky", flaky.URL), kubeletNode(t, "steady", steady.URL)}
	web, api := ContainerKey{"shop", "web-0", "app"}, ContainerKey{"shop", "api-0", "app"}

	if _, failed := c.Scrape(context.Background(), nodes); len(failed) != 0 {
		t.Fatalf("unscraped nodes = %v, want none", failed)
	}

	usage, failed := c.Scrape(context.Background(), nodes)
	if want := map[string]bool{"flaky": true}; !maps.Equal(failed, want) {
		t.Fatalf("unscraped nodes = %v, want %v", failed, want)
	}
	if _, ok := usage[web]; ok {
		t.Errorf("the failed node's container reported usage: %+v", usage[web])
	}
	if u := usage[api]; !u.HasCPU || math.Abs(u.CPUCores-0.1) > 1e-9 {
		t.Errorf("the other node's container = %+v, want its rate of 0.1 cores", u)
	}

	usage, failed = c.Scrape(context.Background(), nodes)
	if len(failed) != 0 {
		t.Fatalf("unscraped nodes after recovery = %v, want none", failed)
	}
	if u := usage[web]; !u.HasCPU || math.Abs(u.CPUCores-0.15) > 1e-9 {
		t.Errorf("recovered rate = %+v, want 3 core-seconds over the 20 s since the last sample", u)
	}
}

func TestCollect_MarksTheNodesWhoseScrapeFailed(t *testing.T) {
	at := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(appMetrics("web-0", 1, at)))
	}))
	defer srv.Close()
	m := startMetadata(t, &config.Config{NodeAddressTypes: config.DefaultNodeAddressTypes},
		kubeletNode(t, "scraped", srv.URL),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "unreachable"}},
		runningPod("shop", "web-0", "scraped", nil, nil, nil, corev1.Container{Name: "app"}),
		runningPod("shop", "api-0", "unreachable", nil, nil, nil, corev1.Container{Name: "app"}),
	)
	kubelet := NewKubeletCollector(srv.Client(), config.DefaultNodeAddressTypes, 0, zap.NewNop())

	obs, err := Collect(context.Background(), m, kubelet, at)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs.Nodes) != 2 || len(obs.Pods) != 2 {
		t.Fatalf("nodes = %+v, pods = %+v, want both", obs.Nodes, obs.Pods)
	}
	if want := map[string]bool{"unreachable": true}; !maps.Equal(obs.Unscraped, want) {
		t.Fatalf("unscraped nodes = %v, want %v", obs.Unscraped, want)
	}
	if u, ok := obs.Usage[ContainerKey{"shop", "web-0", "app"}]; !ok || u.MemoryBytes != 100 {
		t.Errorf("scraped usage = %+v %v", u, ok)
	}
}

// A poll that SIGTERM interrupts has scraped no kubelet and resolved controllers only as far as it
// got, so it is not an observation of the cluster.
func TestCollect_RefusesAPollItsContextEnded(t *testing.T) {
	at := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	for _, cancelled := range []string{"before the poll", "while resolving a controller", "while scraping a kubelet"} {
		t.Run(cancelled, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if cancelled == "while scraping a kubelet" {
					cancel()
					<-r.Context().Done()
					return
				}
				_, _ = w.Write([]byte(appMetrics("web-7f9-abc", 1, at)))
			}))
			defer srv.Close()
			m := startMetadata(t, &config.Config{NodeAddressTypes: config.DefaultNodeAddressTypes},
				&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "web-7f9", OwnerReferences: controllerRef("Deployment", "web")}},
				kubeletNode(t, "n1", srv.URL),
				runningPod("shop", "web-7f9-abc", "n1", controllerRef("ReplicaSet", "web-7f9"), nil, nil, corev1.Container{Name: "app"}),
			)
			switch cancelled {
			case "before the poll":
				cancel()
			case "while resolving a controller":
				m.client.(*fake.Clientset).PrependReactor("get", "replicasets", func(clienttesting.Action) (bool, k8sruntime.Object, error) {
					cancel()
					return true, nil, ctx.Err()
				})
			}
			kubelet := NewKubeletCollector(srv.Client(), config.DefaultNodeAddressTypes, 0, zap.NewNop())

			obs, err := Collect(ctx, m, kubelet, at)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Collect = pods %+v, usage %v, unscraped %v, error %v; want the interrupted poll refused", obs.Pods, obs.Usage, obs.Unscraped, err)
			}
		})
	}
}
