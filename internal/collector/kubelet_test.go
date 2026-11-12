package collector

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	v1 "github.com/costfluent/k8s-agent/api/v1"
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
	if want := map[string]string{"slow": v1.UnscrapedUnreachable, "no-address": v1.UnscrapedUnreachable}; !maps.Equal(failed, want) {
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

// scrapeCapped scrapes one node serving body with the collector's body cap at maxBody and returns
// the usage, the failed nodes, how far the truncation counter rose, and the warnings logged.
func scrapeCapped(t *testing.T, body string, maxBody int) (map[ContainerKey]ContainerUsage, map[string]string, float64, *observer.ObservedLogs) {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
	defer srv.Close()
	core, logs := observer.New(zapcore.WarnLevel)
	c := NewKubeletCollector(srv.Client(), []corev1.NodeAddressType{corev1.NodeInternalIP}, 0, zap.New(core))
	c.maxBody = int64(maxBody)
	before := testutil.ToFloat64(metrics.KubeletBodiesTruncated)
	usage, failed := c.Scrape(context.Background(), []*corev1.Node{kubeletNode(t, "n1", srv.URL)})
	return usage, failed, testutil.ToFloat64(metrics.KubeletBodiesTruncated) - before, logs
}

// assertTruncatedNodeFailed holds a truncated body to failing its node with one logged error that
// names the size limit, and to no log entry claiming the rest of the node was reported.
func assertTruncatedNodeFailed(t *testing.T, usage map[ContainerKey]ContainerUsage, failed map[string]string, counted float64, logs *observer.ObservedLogs) {
	t.Helper()
	if failed["n1"] != v1.UnscrapedOther {
		t.Errorf("failed nodes = %v, want n1 as other: a truncated body leaves the containers past the cut without samples", failed)
	}
	if len(usage) != 0 {
		t.Errorf("usage has %d containers, want none from a truncated body", len(usage))
	}
	if counted != 1 {
		t.Errorf("truncated bodies counted %v times, want 1", counted)
	}
	entries := logs.FilterMessage("kubelet scrape failed").All()
	if len(entries) != 1 {
		t.Fatalf("logged %d scrape failures, want 1: %v", len(entries), logs.All())
	}
	if logged, _ := entries[0].ContextMap()["error"].(string); !strings.Contains(logged, "byte limit") {
		t.Errorf("logged error %q, want it to name the size limit", logged)
	}
	for _, e := range logs.All() {
		if strings.Contains(e.Message, "missing") {
			t.Errorf("logged %q, which claims part of the node was reported", e.Message)
		}
	}
}

func TestKubeletCollector_CountsATruncatedBody(t *testing.T) {
	body := fixture(t)

	usage, failed, counted, logs := scrapeCapped(t, body, len(body))
	if len(failed) != 0 || len(usage) == 0 || counted != 0 || logs.Len() != 0 {
		t.Fatalf("a body exactly at the limit: failed %v, %d containers, counted %v, logs %v; want reported in full", failed, len(usage), counted, logs.All())
	}

	for _, tc := range []struct {
		name    string
		maxBody int
	}{
		{"cut mid-line", len(body) - 1},
		{"cut mid-family", len(body) / 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			usage, failed, counted, logs := scrapeCapped(t, body, tc.maxBody)
			assertTruncatedNodeFailed(t, usage, failed, counted, logs)
		})
	}
}

func TestKubeletCollector_TruncatedAtALineBoundaryIsStillFailed(t *testing.T) {
	body := fixture(t)
	// The cut falls just before the memory family, so the parser reads a complete CPU family and
	// succeeds on what arrived.
	cut := strings.Index(body, "\n# HELP container_memory_working_set_bytes")
	if cut < 0 {
		t.Fatal("fixture no longer has the memory family")
	}
	usage, failed, counted, logs := scrapeCapped(t, body, cut+1)
	assertTruncatedNodeFailed(t, usage, failed, counted, logs)
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
	if want := map[string]string{"flaky": v1.UnscrapedOther}; !maps.Equal(failed, want) {
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

func TestKubeletCollector_NamesTheSkipVerifySettingOnlyForACertificateThatDoesNotVerify(t *testing.T) {
	const hint = "agent.disableKubeTLSverify=true"
	at := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(appMetrics("web-0", 1, at)))
	}))
	defer srv.Close()
	forbidden := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer forbidden.Close()
	closed := httptest.NewTLSServer(http.NotFoundHandler())
	closed.Close()

	// An empty pool trusts no certificate, as the cluster CA trusts none a kubelet signed itself.
	untrusting := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool()}}}
	// The httptest certificate names 127.0.0.1 and example.com, not localhost.
	otherName := kubeletNode(t, "kubelet", srv.URL)
	otherName.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "localhost"}}

	cases := []struct {
		name     string
		client   *http.Client
		node     *corev1.Node
		wantHint bool
		want     string
	}{
		{"self-signed", untrusting, kubeletNode(t, "kubelet", srv.URL), true, "x509"},
		{"another host's certificate", srv.Client(), otherName, true, "x509"},
		{"forbidden", forbidden.Client(), kubeletNode(t, "kubelet", forbidden.URL), false, "ClusterRole"},
		{"unreachable", srv.Client(), kubeletNode(t, "kubelet", closed.URL), false, "connect"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.WarnLevel)
			kubelet := NewKubeletCollector(tc.client, config.DefaultNodeAddressTypes, 0, zap.New(core))

			_, failed := kubelet.Scrape(context.Background(), []*corev1.Node{tc.node})
			if failed["kubelet"] == "" {
				t.Fatalf("failed nodes = %v, want the kubelet", failed)
			}
			entries := logs.FilterMessage("kubelet scrape failed").All()
			if len(entries) != 1 {
				t.Fatalf("logged %d scrape failures, want 1: %v", len(entries), logs.All())
			}
			logged, _ := entries[0].ContextMap()["error"].(string)
			if !strings.Contains(logged, tc.want) {
				t.Errorf("logged error %q, want it to keep the cause %q", logged, tc.want)
			}
			if got := strings.Contains(logged, hint); got != tc.wantHint {
				t.Errorf("logged error %q names %s: %v, want %v", logged, hint, got, tc.wantHint)
			}
		})
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
	if want := map[string]string{"unreachable": v1.UnscrapedUnreachable}; !maps.Equal(obs.Unscraped, want) {
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

// expiredKubelet serves with a certificate for 127.0.0.1 that expired an hour ago, and returns a
// client that trusts that certificate, so only its expiry fails verification.
func expiredKubelet(t *testing.T) (*httptest.Server, *http.Client) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kubelet"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:             now.Add(-2 * time.Hour),
		NotAfter:              now.Add(-time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.NotFoundHandler())
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return srv, &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
}

func statusKubelet(t *testing.T, code int) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
	t.Cleanup(srv.Close)
	return srv
}

func TestScrape_ClassifiesEachFailure(t *testing.T) {
	ok := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(appMetrics("web-0", 1, time.Now())))
	}))
	defer ok.Close()
	closed := httptest.NewTLSServer(http.NotFoundHandler())
	closed.Close()
	hang := make(chan struct{})
	slow := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	defer close(hang)
	expired, trustsExpired := expiredKubelet(t)
	forbidden, unauthorized, broken := statusKubelet(t, http.StatusForbidden), statusKubelet(t, http.StatusUnauthorized), statusKubelet(t, http.StatusInternalServerError)
	garbage := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("container_cpu_usage_seconds_total{ not metrics\n"))
	}))
	defer garbage.Close()
	untrusting := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12}}}

	for _, tc := range []struct {
		name   string
		client *http.Client
		node   *corev1.Node
		want   string
	}{
		{"an unknown CA", untrusting, kubeletNode(t, "n", ok.URL), v1.UnscrapedTLS},
		{"an expired certificate", trustsExpired, kubeletNode(t, "n", expired.URL), v1.UnscrapedTLS},
		{"403", forbidden.Client(), kubeletNode(t, "n", forbidden.URL), v1.UnscrapedUnauthorized},
		{"401", unauthorized.Client(), kubeletNode(t, "n", unauthorized.URL), v1.UnscrapedUnauthorized},
		{"a refused dial", closed.Client(), kubeletNode(t, "n", closed.URL), v1.UnscrapedUnreachable},
		{"no address", ok.Client(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n"}}, v1.UnscrapedUnreachable},
		{"a timeout", slow.Client(), kubeletNode(t, "n", slow.URL), v1.UnscrapedUnreachable},
		{"500", broken.Client(), kubeletNode(t, "n", broken.URL), v1.UnscrapedOther},
		{"a body that does not parse", garbage.Client(), kubeletNode(t, "n", garbage.URL), v1.UnscrapedOther},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.WarnLevel)
			c := NewKubeletCollector(tc.client, config.DefaultNodeAddressTypes, 0, zap.New(core))
			c.timeout = 200 * time.Millisecond
			_, failed := c.Scrape(context.Background(), []*corev1.Node{tc.node})
			if want := map[string]string{"n": tc.want}; !maps.Equal(failed, want) {
				t.Fatalf("unscraped = %v, want %v", failed, want)
			}
			entries := logs.FilterMessage("kubelet scrape failed").All()
			if len(entries) != 1 {
				t.Fatalf("logged %d scrape failures, want 1", len(entries))
			}
			if fields := entries[0].ContextMap(); fields["reason"] != tc.want || fields["error"] == "" {
				t.Errorf("logged %v, want the reason %q beside the error", fields, tc.want)
			}
		})
	}
}

// *url.Error, which wraps every client.Do failure, implements net.Error, so matching that interface
// would take a TLS failure for an unreachable kubelet.
func TestUnscrapedReason_ReadsTheCauseInsideTheURLError(t *testing.T) {
	wrap := func(err error) error {
		return fmt.Errorf("GET https://10.0.0.1:10250/metrics/resource: %w", &url.Error{Op: "Get", URL: "https://10.0.0.1:10250/metrics/resource", Err: err})
	}
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"a certificate", wrap(&tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}), v1.UnscrapedTLS},
		{"a DNS failure", wrap(&net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host", Name: "node-1", IsNotFound: true}}), v1.UnscrapedUnreachable},
		{"a bare DNS failure", wrap(&net.DNSError{Err: "no such host", Name: "node-1"}), v1.UnscrapedUnreachable},
		{"a deadline", wrap(context.DeadlineExceeded), v1.UnscrapedUnreachable},
		{"a cancelled poll", wrap(context.Canceled), v1.UnscrapedOther},
		{"a token request the API server forbids", wrap(&TokenError{Err: apierrors.NewForbidden(schema.GroupResource{Resource: "serviceaccounts/token"}, "metrics", nil)}), v1.UnscrapedUnauthorized},
		{"a token request the API server does not authenticate", wrap(&TokenError{Err: apierrors.NewUnauthorized("expired")}), v1.UnscrapedUnauthorized},
		// The API server, not the kubelet, was unreachable, so the node is not called unreachable.
		{"a token request that cannot connect", wrap(&TokenError{Err: &net.OpError{Op: "dial", Err: errors.New("connection refused")}}), v1.UnscrapedOther},
		{"anything else", wrap(errors.New("unexpected EOF")), v1.UnscrapedOther},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err.Error() == "" {
				t.Fatal("the error has no text")
			}
			if got := unscrapedReason(tc.err); got != tc.want {
				t.Fatalf("unscrapedReason(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
