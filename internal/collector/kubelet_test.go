package collector

import (
	"context"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	c := NewKubeletCollector(client, config.DefaultNodeAddressTypes, zap.NewNop())
	c.timeout = 200 * time.Millisecond
	nodes := []*corev1.Node{
		kubeletNode(t, "dev-cluster-worker-1", healthy.URL),
		kubeletNode(t, "slow", slow.URL),
		{ObjectMeta: metav1.ObjectMeta{Name: "no-address"}},
	}

	failedBefore := testutil.ToFloat64(metrics.NodeScrapes.WithLabelValues("error"))
	usage := c.Scrape(context.Background(), nodes)
	if got := testutil.ToFloat64(metrics.NodeScrapes.WithLabelValues("error")) - failedBefore; got != 2 {
		t.Fatalf("failed scrapes = %v, want 2 (the slow node and the node without an address)", got)
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

	usage = c.Scrape(context.Background(), nodes)
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

	usage = c.Scrape(context.Background(), nodes)
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
