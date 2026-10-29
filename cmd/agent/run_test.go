package main

import (
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/collector"
	"github.com/costfluent/k8s-agent/internal/config"
	"github.com/costfluent/k8s-agent/internal/metrics"
	"github.com/costfluent/k8s-agent/internal/reporter"
	"github.com/costfluent/k8s-agent/internal/storage"
)

// validEnv sets a configuration config.Load accepts, so each test changes only what it is about.
func validEnv(t *testing.T, dataDir string) {
	t.Helper()
	t.Setenv("COSTFLUENT_TOKEN", "cf_org_test")
	t.Setenv("COSTFLUENT_CLUSTER_ID", "test-cluster")
	t.Setenv("COSTFLUENT_DATA_DIR", dataDir)
	t.Setenv("COSTFLUENT_API_ENDPOINT", acceptingReceiver(t))
	t.Setenv("COSTFLUENT_POLLING_INTERVAL", "")
	t.Setenv("COSTFLUENT_NODE_ADDRESS_TYPES", "")
	t.Setenv("COSTFLUENT_KUBE_SKIP_TLS_VERIFY", "")
	t.Setenv("COSTFLUENT_REPORT_HTTP_PROXY", "")
	t.Setenv("COSTFLUENT_METRICS_PORT", strconv.Itoa(heldPort(t)))
	t.Setenv("COSTFLUENT_LOG_LEVEL", "error")
	t.Setenv("COSTFLUENT_LOG_FORMAT", "json")
	// Outside a pod these are unset; clearing them keeps run hermetic when the tests run in one.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
}

// heldPort returns a loopback port this test keeps listening on, so the agent's health server
// never exposes anything while run is under test.
func heldPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l.Addr().(*net.TCPAddr).Port
}

func TestRun_StartupFailures(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(t *testing.T)
		wantErr []string
	}{
		{
			name: "missing token",
			setup: func(t *testing.T) {
				validEnv(t, t.TempDir())
				t.Setenv("COSTFLUENT_TOKEN", "")
			},
			wantErr: []string{"configuration:", "COSTFLUENT_TOKEN is required"},
		},
		{
			name: "invalid polling interval",
			setup: func(t *testing.T) {
				validEnv(t, t.TempDir())
				t.Setenv("COSTFLUENT_POLLING_INTERVAL", "7")
			},
			wantErr: []string{"configuration:", "COSTFLUENT_POLLING_INTERVAL"},
		},
		{
			name: "data directory is a file",
			setup: func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "not-a-dir")
				if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
					t.Fatal(err)
				}
				validEnv(t, path)
			},
			wantErr: []string{"is not writable"},
		},
		{
			name: "instance id cannot be read",
			setup: func(t *testing.T) {
				dir := t.TempDir()
				if err := os.Mkdir(filepath.Join(dir, "instance-id"), 0o700); err != nil {
					t.Fatal(err)
				}
				validEnv(t, dir)
			},
			wantErr: []string{"reading", "instance-id"},
		},
		{
			name: "kept cluster id cannot be read",
			setup: func(t *testing.T) {
				dir := t.TempDir()
				if err := os.Mkdir(filepath.Join(dir, "cluster-id"), 0o700); err != nil {
					t.Fatal(err)
				}
				validEnv(t, dir)
			},
			wantErr: []string{"reading", "cluster-id"},
		},
		{
			name: "not running in a cluster",
			setup: func(t *testing.T) {
				validEnv(t, t.TempDir())
			},
			wantErr: []string{"in-cluster configuration:"},
		},
		{
			name: "token refused",
			setup: func(t *testing.T) {
				validEnv(t, t.TempDir())
				t.Setenv("COSTFLUENT_API_ENDPOINT", tokenReceiver(t, http.StatusUnauthorized,
					`{"title":"Authentication Error","detail":"The API token is invalid, expired or revoked"}`))
			},
			wantErr: []string{"the Costfluent API refused the token", "401", "invalid, expired or revoked", "the token is not valid"},
		},
		{
			name: "token lacks the capability",
			setup: func(t *testing.T) {
				validEnv(t, t.TempDir())
				t.Setenv("COSTFLUENT_API_ENDPOINT", tokenReceiver(t, http.StatusForbidden,
					`{"title":"Forbidden","detail":"API tokens are not included in the Free plan"}`))
			},
			wantErr: []string{"the Costfluent API refused the token", "403", "not included in the Free plan", "Mint token"},
		},
		{
			name: "organization token",
			setup: func(t *testing.T) {
				validEnv(t, t.TempDir())
				t.Setenv("COSTFLUENT_API_ENDPOINT", tokenReceiver(t, http.StatusOK,
					`{"tokenType":"organization","tokenName":"ci","capabilities":["reportKubernetesUsage"]}`))
			},
			wantErr: []string{`the API token "ci" is not a cluster token`, "Add", "Mint token", "agent.token"},
		},
		{
			name: "workspace token",
			setup: func(t *testing.T) {
				validEnv(t, t.TempDir())
				t.Setenv("COSTFLUENT_API_ENDPOINT", tokenReceiver(t, http.StatusOK,
					`{"tokenType":"workspace","tokenName":"ws","capabilities":["viewCosts"]}`))
			},
			wantErr: []string{`the API token "ws" is not a cluster token`},
		},
		{
			name: "deleted cluster's token",
			setup: func(t *testing.T) {
				validEnv(t, t.TempDir())
				t.Setenv("COSTFLUENT_API_ENDPOINT", tokenReceiver(t, http.StatusOK,
					`{"tokenType":"cluster","tokenName":"old-cluster","capabilities":["reportKubernetesUsage"],"clusterId":null}`))
			},
			wantErr: []string{`the cluster the API token "old-cluster" belongs to was deleted`, "add the cluster again"},
		},
		{
			name: "configured cluster is not the token's",
			setup: func(t *testing.T) {
				validEnv(t, t.TempDir())
				t.Setenv("COSTFLUENT_CLUSTER_ID", "other-cluster")
			},
			wantErr: []string{`agent.clusterID "other-cluster" is not the cluster "test-cluster"`, "remove agent.clusterID"},
		},
		{
			name: "no cluster configured and the token cannot be read",
			setup: func(t *testing.T) {
				validEnv(t, t.TempDir())
				t.Setenv("COSTFLUENT_CLUSTER_ID", "")
				t.Setenv("COSTFLUENT_API_ENDPOINT", tokenReceiver(t, http.StatusServiceUnavailable, `{"title":"Service Unavailable"}`))
			},
			wantErr: []string{"could not read the cluster from the API token at start", "503", "agent.clusterID"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup(t)
			err := run(t.Context())
			if err == nil {
				t.Fatal("run succeeded; want a startup error")
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
			if strings.Contains(err.Error(), "cf_org_test") {
				t.Errorf("error %q discloses the token", err)
			}
		})
	}
}

func TestRun_NotInClusterLeavesTheInstanceIDAndBuffer(t *testing.T) {
	dir := t.TempDir()
	validEnv(t, dir)
	if err := run(t.Context()); !errors.Is(err, rest.ErrNotInCluster) {
		t.Fatalf("err = %v, want rest.ErrNotInCluster wrapped", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "instance-id")); err != nil {
		t.Errorf("the instance id must be created before the cluster is reached: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(dir, "buffer")); err != nil || !fi.IsDir() {
		t.Errorf("the buffer directory must be prepared at start: %v", err)
	}
}

func TestRun_ReportsAKubernetesClientItCannotBuild(t *testing.T) {
	validEnv(t, t.TempDir())
	original := inClusterConfig
	t.Cleanup(func() { inClusterConfig = original })
	inClusterConfig = func() (*rest.Config, error) {
		return &rest.Config{
			Host:            "https://10.0.0.1:443",
			TLSClientConfig: rest.TLSClientConfig{CAFile: filepath.Join(t.TempDir(), "missing-ca.crt")},
		}, nil
	}
	err := run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "kubernetes client:") || !strings.Contains(err.Error(), "missing-ca.crt") {
		t.Fatalf("err = %v, want the kubernetes client failure naming the unreadable CA", err)
	}
}

// stubCluster points run at a Kubernetes API served by the test instead of the pod's own.
func stubCluster(t *testing.T, apiURL string) {
	t.Helper()
	original := inClusterConfig
	t.Cleanup(func() { inClusterConfig = original })
	inClusterConfig = func() (*rest.Config, error) { return &rest.Config{Host: apiURL}, nil }
}

// fakeKubernetes serves an empty cluster: node, pod, namespace, volume and storage class lists with
// no items, and watches held open until the client leaves. With answer false every request hangs
// instead, as an API server the agent cannot list from does.
func fakeKubernetes(t *testing.T, answer bool) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !answer {
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		kind, apiVersion := map[string]string{
			"/api/v1/nodes": "NodeList", "/api/v1/pods": "PodList", "/api/v1/namespaces": "NamespaceList",
			"/api/v1/persistentvolumes": "PersistentVolumeList", "/apis/storage.k8s.io/v1/storageclasses": "StorageClassList",
		}[r.URL.Path], "v1"
		if kind == "StorageClassList" {
			apiVersion = "storage.k8s.io/v1"
		}
		if kind == "" {
			t.Errorf("fake Kubernetes API: unexpected request %s %s", r.Method, r.URL)
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprintf(w, `{"kind":%q,"apiVersion":%q,"metadata":{"resourceVersion":"1"},"items":[]}`, kind, apiVersion)
	}))
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
	})
	return srv.URL
}

// agentToken is GET /v1/me for the token the chart asks for: the cluster token of test-cluster,
// carrying only reportKubernetesUsage.
const agentToken = `{"organizationId":"org_1","tokenType":"cluster","tokenName":"test-cluster","capabilities":["reportKubernetesUsage"],"clusterId":"test-cluster"}`

// acceptingReceiver stands in for the Costfluent API, so no test reaches the real one: it describes
// the agent's token and accepts every report.
func acceptingReceiver(t *testing.T) string {
	t.Helper()
	return tokenReceiver(t, http.StatusOK, agentToken)
}

// tokenReceiver answers GET /v1/me with status and body, and accepts every report.
func tokenReceiver(t *testing.T, status int, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/me" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// healthPort binds a port on every interface for the agent's health server, points
// COSTFLUENT_METRICS_PORT at it, and hands run that listener, so nothing else can bind the port
// between this call and run serving on it.
func healthPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	addr := net.JoinHostPort("", strconv.Itoa(port))
	t.Setenv("COSTFLUENT_METRICS_PORT", strconv.Itoa(port))
	original := listen
	t.Cleanup(func() {
		listen = original
		_ = l.Close()
	})
	listen = func(network, address string) (net.Listener, error) {
		if network != "tcp" || address != addr {
			return nil, fmt.Errorf("listen %s %s, want the reserved tcp %s", network, address, addr)
		}
		return l, nil
	}
	return port
}

func TestRun_ReportsInformersThatCannotSync(t *testing.T) {
	validEnv(t, t.TempDir())
	t.Setenv("COSTFLUENT_API_ENDPOINT", acceptingReceiver(t))
	stubCluster(t, fakeKubernetes(t, false))
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	err := run(ctx)
	if err == nil || !strings.Contains(err.Error(), "node and pod informers did not sync") {
		t.Fatalf("err = %v, want the informer sync failure", err)
	}
}

// startLogged runs the agent with warnings logged, waits until it reports ready, stops it, and
// returns what it wrote to stderr, where its logger writes.
func startLogged(t *testing.T) string {
	t.Helper()
	return startLoggedAt(t, "warn")
}

// startLoggedAt is startLogged at another log level.
func startLoggedAt(t *testing.T, level string) string {
	t.Helper()
	port := healthPort(t)
	t.Setenv("COSTFLUENT_LOG_LEVEL", level)
	stubCluster(t, fakeKubernetes(t, true))

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := os.Stderr
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = stderr })
	logged := make(chan string, 1)
	go func() {
		out, _ := io.ReadAll(r)
		logged <- string(out)
	}()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	readyz := fmt.Sprintf("http://127.0.0.1:%d/readyz", port)
	deadline := time.Now().Add(10 * time.Second)
	for ready := false; !ready; {
		if resp, err := http.Get(readyz); err == nil {
			ready = resp.StatusCode == http.StatusOK
			_ = resp.Body.Close()
		}
		if !ready && time.Now().After(deadline) {
			cancel()
			t.Fatalf("the agent never became ready; run returned %v", <-done)
		}
		if !ready {
			time.Sleep(20 * time.Millisecond)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run = %v, want nil after its context ends", err)
	}
	os.Stderr = stderr
	_ = w.Close()
	return <-logged
}

// The chart needs no cluster id: the agent reads it from its cluster token.
func TestRun_LearnsItsClusterFromTheToken(t *testing.T) {
	validEnv(t, t.TempDir())
	t.Setenv("COSTFLUENT_CLUSTER_ID", "")

	log := startLoggedAt(t, "info")
	if !strings.Contains(log, "reporting for cluster") || !strings.Contains(log, `"cluster_id":"test-cluster"`) {
		t.Fatalf("the agent did not take its cluster from the token:\n%s", log)
	}
}

// persistedClusterID is the cluster id the agent kept on its volume, or "" when it kept none.
func persistedClusterID(t *testing.T, dataDir string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dataDir, "cluster-id"))
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(data))
}

// unreachableAPI returns the address of an API that refuses every connection.
func unreachableAPI(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	return srv.URL
}

// The cluster the token named is kept on the volume, so a restart while the API is unreachable
// reports for the same cluster instead of exiting until the API answers.
func TestRun_KeepsTheTokensClusterAndStartsOfflineWithIt(t *testing.T) {
	dir := t.TempDir()
	validEnv(t, dir)
	t.Setenv("COSTFLUENT_CLUSTER_ID", "")
	startLoggedAt(t, "info")
	if got := persistedClusterID(t, dir); got != "test-cluster" {
		t.Fatalf("persisted cluster id = %q, want the token's test-cluster", got)
	}

	t.Setenv("COSTFLUENT_API_ENDPOINT", unreachableAPI(t))
	log := startLoggedAt(t, "info")
	if !strings.Contains(log, "could not check the API token at start") || !strings.Contains(log, `"cluster_id":"test-cluster"`) {
		t.Fatalf("the agent did not start offline for the cluster it kept:\n%s", log)
	}
}

// The token's answer wins over what the volume kept: a token for another cluster moves the agent
// to that cluster, and the next offline start uses it.
func TestRun_ReplacesAKeptClusterTheTokenNoLongerNames(t *testing.T) {
	dir := t.TempDir()
	validEnv(t, dir)
	t.Setenv("COSTFLUENT_CLUSTER_ID", "")
	if err := os.WriteFile(filepath.Join(dir, "cluster-id"), []byte("old-cluster\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	log := startLoggedAt(t, "info")
	if !strings.Contains(log, `"cluster_id":"test-cluster"`) || strings.Contains(log, `"cluster_id":"old-cluster"`) {
		t.Fatalf("the agent did not report for the token's cluster:\n%s", log)
	}
	if got := persistedClusterID(t, dir); got != "test-cluster" {
		t.Fatalf("persisted cluster id = %q, want it replaced by the token's test-cluster", got)
	}
}

// Keeping the cluster only spares a later offline start, so failing to keep it is no reason to
// stop reporting.
func TestRun_StartsWhenItCannotKeepTheTokensCluster(t *testing.T) {
	dir := t.TempDir()
	validEnv(t, dir)
	t.Setenv("COSTFLUENT_CLUSTER_ID", "")
	if err := os.Mkdir(filepath.Join(dir, "buffer"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"instance-id": "kept-id\n", "cluster-id": "old-cluster\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	log := startLogged(t)
	if !strings.Contains(log, "keeping the token's cluster failed") {
		t.Fatalf("the agent did not warn that it could not keep its cluster:\n%s", log)
	}
}

// A configured agent.clusterID is checked against the token, so an offline start uses it over a
// kept id, as it did before the agent kept one.
func TestRun_PrefersTheConfiguredClusterOfflineOverTheKeptOne(t *testing.T) {
	dir := t.TempDir()
	validEnv(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "cluster-id"), []byte("old-cluster\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COSTFLUENT_API_ENDPOINT", unreachableAPI(t))

	log := startLoggedAt(t, "info")
	if !strings.Contains(log, `"cluster_id":"test-cluster"`) {
		t.Fatalf("the agent did not start offline for its configured cluster:\n%s", log)
	}
}

// An API that does not answer at start is the sender's to retry, so a cluster whose egress comes
// up late still starts.
func TestRun_StartsWhenTheTokenCannotBeChecked(t *testing.T) {
	validEnv(t, t.TempDir())
	t.Setenv("COSTFLUENT_API_ENDPOINT", tokenReceiver(t, http.StatusServiceUnavailable, `{"title":"Service Unavailable"}`))

	log := startLogged(t)
	if !strings.Contains(log, "could not check the API token at start") || !strings.Contains(log, "Service Unavailable") {
		t.Fatalf("the agent did not warn that it could not check its token:\n%s", log)
	}
}

func TestRun_StopsCleanlyWhenItsContextEnds(t *testing.T) {
	validEnv(t, t.TempDir())
	port := healthPort(t)
	t.Setenv("COSTFLUENT_API_ENDPOINT", acceptingReceiver(t))
	stubCluster(t, fakeKubernetes(t, true))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()

	readyz := fmt.Sprintf("http://127.0.0.1:%d/readyz", port)
	deadline := time.Now().Add(10 * time.Second)
	for ready := false; !ready; {
		if resp, err := http.Get(readyz); err == nil {
			ready = resp.StatusCode == http.StatusOK
			_ = resp.Body.Close()
		}
		if !ready && time.Now().After(deadline) {
			cancel()
			t.Fatalf("the agent never became ready; run returned %v", <-done)
		}
		if !ready {
			time.Sleep(20 * time.Millisecond)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run = %v, want nil after its context ends", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not return after its context ended")
	}
}

// The port a test chooses for the agent's health server stays bound until run serves on it. A port
// chosen and released again was once taken by the fake Kubernetes API's httptest server, which
// binds an ephemeral port next: the agent's bind failed and its readiness probes reached the fake.
func TestRun_HealthPortStaysReservedUntilRunBindsIt(t *testing.T) {
	port := healthPort(t)
	if l, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(port))); err == nil {
		_ = l.Close()
		t.Fatalf("port %d was free for another listener before run bound it", port)
	}
}

func TestRun_ReportsAHealthPortItCannotBind(t *testing.T) {
	validEnv(t, t.TempDir())
	taken, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = taken.Close() })
	t.Setenv("COSTFLUENT_METRICS_PORT", strconv.Itoa(taken.Addr().(*net.TCPAddr).Port))
	t.Setenv("COSTFLUENT_API_ENDPOINT", acceptingReceiver(t))
	stubCluster(t, fakeKubernetes(t, true))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	err = run(ctx)
	if err == nil || !strings.Contains(err.Error(), "serving health and metrics on") {
		t.Fatalf("err = %v, want the health server's bind failure", err)
	}
}

func TestNewLogger(t *testing.T) {
	if _, err := newLogger("verbose", "json"); err == nil || !strings.Contains(err.Error(), `log level "verbose"`) {
		t.Fatalf("err = %v, want the invalid level named", err)
	}
	for _, tc := range []struct {
		level, format string
		enabled       zapcore.Level
		disabled      zapcore.Level
	}{
		{"warn", "json", zapcore.WarnLevel, zapcore.InfoLevel},
		{"debug", "console", zapcore.DebugLevel, zapcore.DebugLevel - 1},
	} {
		logger, err := newLogger(tc.level, tc.format)
		if err != nil {
			t.Fatalf("%s/%s: %v", tc.level, tc.format, err)
		}
		if !logger.Core().Enabled(tc.enabled) || logger.Core().Enabled(tc.disabled) {
			t.Errorf("%s/%s: level gate is wrong", tc.level, tc.format)
		}
	}
}

func kubeletServer(t *testing.T, body string) (*httptest.Server, *atomic.Value) {
	t.Helper()
	var auth atomic.Value
	auth.Store("")
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		if r.URL.Path != "/metrics/resource" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &auth
}

func certPEM(srv *httptest.Server) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
}

// otherCA is a CA that did not sign the httptest certificate, which every httptest server shares.
func otherCA(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "other cluster CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// recentHour is a whole hour within the buffer's age cap, which runs on the real clock.
func recentHour() time.Time { return time.Now().UTC().Truncate(time.Hour).Add(-6 * time.Hour) }

func TestKubeletHTTPClient_VerifiesAgainstTheClusterCAUnlessSkipped(t *testing.T) {
	srv, auth := kubeletServer(t, "")

	get := func(client *http.Client) error {
		resp, err := client.Get(srv.URL + "/metrics/resource")
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		return nil
	}

	trusted := &rest.Config{Host: srv.URL, BearerToken: "sa-token", TLSClientConfig: rest.TLSClientConfig{CAData: certPEM(srv)}}
	client, err := kubeletHTTPClient(trusted, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := get(client); err != nil {
		t.Fatalf("a kubelet signed by the cluster CA was refused: %v", err)
	}
	if got := auth.Load(); got != "Bearer sa-token" {
		t.Fatalf("Authorization = %q, want the service-account token", got)
	}

	untrusted := &rest.Config{Host: srv.URL, BearerToken: "sa-token", TLSClientConfig: rest.TLSClientConfig{CAData: otherCA(t)}}
	client, err = kubeletHTTPClient(untrusted, false)
	if err != nil {
		t.Fatal(err)
	}
	var unknown x509.UnknownAuthorityError
	if err := get(client); !errors.As(err, &unknown) {
		t.Fatalf("err = %v, want an unknown-authority failure for a kubelet the cluster CA did not sign", err)
	}

	auth.Store("")
	client, err = kubeletHTTPClient(untrusted, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := get(client); err != nil {
		t.Fatalf("skipVerify must accept a self-signed kubelet: %v", err)
	}
	if got := auth.Load(); got != "Bearer sa-token" {
		t.Fatalf("Authorization = %q after skipVerify, want the token kept", got)
	}
	if untrusted.Insecure || untrusted.CAData == nil {
		t.Fatal("kubeletHTTPClient must not modify the caller's rest.Config")
	}
}

func TestKubeletHTTPClient_ReportsAnUnreadableCA(t *testing.T) {
	cfg := &rest.Config{Host: "https://127.0.0.1:1", TLSClientConfig: rest.TLSClientConfig{CAFile: filepath.Join(t.TempDir(), "missing-ca.crt")}}
	if _, err := kubeletHTTPClient(cfg, false); err == nil || !strings.Contains(err.Error(), "kubelet transport") {
		t.Fatalf("err = %v, want a kubelet transport error", err)
	}
	if _, err := kubeletHTTPClient(cfg, true); err != nil {
		t.Fatalf("skipVerify ignores the CA file, so it must not fail: %v", err)
	}
}

const kubeletMetrics = `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{container="web",namespace="shop",pod="web-1"} 12.5 1790000000000
# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{container="web",namespace="shop",pod="web-1"} 1.048576e+06 1790000000000
`

func TestCollectFunc_JoinsNodesPodsAndKubeletUsage(t *testing.T) {
	srv, _ := kubeletServer(t, kubeletMetrics)
	host, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "https://"))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := strconv.Atoi(port)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"node.kubernetes.io/instance-type": "cx22"}},
		Status: corev1.NodeStatus{
			Addresses:       []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: host}},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: int32(p)}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "web-1"},
		Spec:       corev1.PodSpec{NodeName: "node-b", Containers: []corev1.Container{{Name: "web"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	client := fake.NewClientset(node, pod, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := &config.Config{NodeAddressTypes: config.DefaultNodeAddressTypes}
	metadata := collector.NewMetadata(client, cfg, zap.NewNop())
	if err := metadata.Start(ctx); err != nil {
		t.Fatal(err)
	}
	kubelet := collector.NewKubeletCollector(srv.Client(), []corev1.NodeAddressType{corev1.NodeInternalIP}, 0, zap.NewNop())

	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	obs, err := collectFunc(metadata, kubelet)(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if !obs.Time.Equal(now) {
		t.Errorf("time = %s, want the poll time", obs.Time)
	}
	if len(obs.Nodes) != 2 || obs.Nodes[0].Name != "node-a" || obs.Nodes[1].Name != "node-b" || obs.Nodes[1].InstanceType != "cx22" {
		t.Errorf("nodes = %+v, want both nodes converted in name order", obs.Nodes)
	}
	if len(obs.Pods) != 1 || obs.Pods[0].Name != "web-1" || obs.Pods[0].Node != "node-b" {
		t.Errorf("pods = %+v", obs.Pods)
	}
	u, ok := obs.Usage[collector.ContainerKey{Namespace: "shop", Pod: "web-1", Container: "web"}]
	if !ok || u.MemoryBytes != 1<<20 {
		t.Errorf("usage = %+v %v, want the kubelet's working set for the scraped container", u, ok)
	}
}

// fakeSender records report bodies and answers with a fixed outcome.
type fakeSender struct {
	mu      sync.Mutex
	outcome reporter.Outcome
	reports []v1.Report
}

func (f *fakeSender) Send(_ context.Context, body []byte) reporter.Result {
	zr, err := gzip.NewReader(strings.NewReader(string(body)))
	if err != nil {
		return reporter.Result{Outcome: reporter.Rejected, Message: err.Error()}
	}
	var r v1.Report
	if err := json.NewDecoder(zr).Decode(&r); err != nil {
		return reporter.Result{Outcome: reporter.Rejected, Message: err.Error()}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reports = append(f.reports, r)
	return reporter.Result{Outcome: f.outcome, Status: http.StatusAccepted}
}

func (f *fakeSender) sent() []v1.Report {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]v1.Report(nil), f.reports...)
}

func newTestAgent(t *testing.T, client *fakeSender, now time.Time) (*agent, *observer.ObservedLogs) {
	t.Helper()
	return testAgentAt(t, t.TempDir(), client, now)
}

// testAgentAt builds an agent on dir, so a second one on the same dir is the first restarted.
func testAgentAt(t *testing.T, dir string, client *fakeSender, now time.Time) (*agent, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)
	buffer, err := storage.Open(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	return &agent{
		interval:     time.Millisecond,
		snapshotPath: filepath.Join(dir, snapshotFile),
		aggregator:   collector.NewAggregator("test-cluster", "test", "instance", 10*time.Second, now),
		buffer:       buffer,
		sender:       reporter.NewSender(buffer, client, logger),
		now:          func() time.Time { return now },
		ready:        new(atomic.Bool),
		logger:       logger,
		lastSuccess:  new(atomic.Int64),
	}, logs
}

func TestAgent_PollFailureKeepsTheAgentUnready(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	a, logs := newTestAgent(t, &fakeSender{}, now)
	a.collect = func(context.Context, time.Time) (collector.Observation, error) {
		return collector.Observation{}, errors.New("informer not synced")
	}

	a.poll(context.Background())
	if a.ready.Load() {
		t.Fatal("a failed poll must not mark the agent ready")
	}
	if got := logs.FilterMessage("poll failed").Len(); got != 1 {
		t.Fatalf("poll failed logged %d times, want 1", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a.poll(ctx)
	if got := logs.FilterMessage("poll failed").Len(); got != 1 {
		t.Fatal("a poll failing because the agent is stopping must not be logged as a failure")
	}
}

func TestAgent_PutLogsALostWindowWhenTheBufferFails(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	a, logs := newTestAgent(t, &fakeSender{}, now)
	if err := os.RemoveAll(filepath.Join(filepath.Dir(a.snapshotPath), "buffer")); err != nil {
		t.Fatal(err)
	}
	a.put(v1.Report{ClusterID: "test-cluster", WindowStart: now, WindowEnd: now.Add(time.Hour)})
	if got := logs.FilterMessage("buffering a closed window failed; it is lost").Len(); got != 1 {
		t.Fatalf("lost window logged %d times, want 1", got)
	}
	if logs.FilterMessage("window closed").Len() != 0 {
		t.Fatal("a window that was not buffered must not be logged as closed")
	}
}

func TestAgent_PutRefusesAWindowOverTheSupportedSize(t *testing.T) {
	now := recentHour()
	window := func(nodes, pods, containers int) v1.Report {
		r := v1.Report{ClusterID: "test-cluster", WindowStart: now, WindowEnd: now.Add(time.Hour)}
		for i := range nodes {
			r.Nodes = append(r.Nodes, v1.Node{Name: fmt.Sprintf("node-%d", i)})
		}
		for i := range pods {
			r.Pods = append(r.Pods, v1.Pod{Namespace: "ns", Name: fmt.Sprintf("pod-%d", i), Node: "node-0"})
		}
		if pods > 0 {
			r.Pods[0].Containers = make([]v1.Container, containers)
		}
		return r
	}
	for _, tc := range []struct {
		name   string
		report v1.Report
		want   string
	}{
		{"too many nodes", window(v1.MaxNodes+1, 1, 1), "nodes"},
		{"too many pods", window(1, v1.MaxPods+1, 1), "pods"},
		{"too many containers in a pod", window(1, 1, v1.MaxContainersPerPod+1), "containers"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, logs := newTestAgent(t, &fakeSender{}, now)
			a.put(tc.report)
			if n, _ := a.buffer.Len(); n != 0 {
				t.Fatalf("buffered = %d, want a window the API refuses whole kept out of the buffer", n)
			}
			refused := logs.FilterMessage("the cluster exceeds the size Costfluent collects; this window is not sent").All()
			if len(refused) != 1 || !strings.Contains(fmt.Sprint(refused[0].ContextMap()["error"]), tc.want) {
				t.Fatalf("refusal logged as %+v, want one error naming the %s cap", refused, tc.want)
			}
			if got := testutil.ToFloat64(metrics.OverSupportedSize); got != 1 {
				t.Fatalf("over_supported_size = %v, want 1", got)
			}

			a.put(window(v1.MaxNodes, 1, v1.MaxContainersPerPod))
			if n, _ := a.buffer.Len(); n != 1 {
				t.Fatalf("buffered = %d, want a window at the caps buffered", n)
			}
			if got := testutil.ToFloat64(metrics.OverSupportedSize); got != 0 {
				t.Fatalf("over_supported_size = %v after a window that fits, want 0", got)
			}
		})
	}
}

func TestAgent_RestoresAWindowThatClosedWhileDown(t *testing.T) {
	windowStart := recentHour()
	earlier := collector.NewAggregator("test-cluster", "old", "instance", 10*time.Second, windowStart.Add(-time.Minute))
	earlier.Observe(collector.Observation{Time: windowStart.Add(10 * time.Second), Nodes: []v1.Node{{Name: "node-a"}}})
	earlier.Observe(collector.Observation{Time: windowStart.Add(20 * time.Second), Nodes: []v1.Node{{Name: "node-a"}}})

	now := windowStart.Add(3 * time.Hour)
	client := &fakeSender{outcome: reporter.Accepted}
	a, logs := newTestAgent(t, client, now)
	if err := earlier.SaveSnapshot(a.snapshotPath); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.collect = func(_ context.Context, at time.Time) (collector.Observation, error) {
		cancel()
		return collector.Observation{Time: at}, nil
	}
	a.run(ctx)

	sent := client.sent()
	if len(sent) != 1 || !sent[0].WindowStart.Equal(windowStart) || sent[0].AgentVersion != "test" || sent[0].Nodes[0].Name != "node-a" {
		t.Fatalf("sent = %+v, want the restored window, stamped with this agent's version", sent)
	}
	if logs.FilterMessage("discarding the window snapshot").Len() != 0 {
		t.Fatal("a valid snapshot was discarded")
	}
	if _, err := os.Stat(a.snapshotPath); !os.IsNotExist(err) {
		t.Fatalf("the restored snapshot must be removed: %v", err)
	}
}

func TestAgent_DiscardsAnUnreadableSnapshot(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	a, logs := newTestAgent(t, &fakeSender{outcome: reporter.Accepted}, now)
	if err := os.WriteFile(a.snapshotPath, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.collect = func(_ context.Context, at time.Time) (collector.Observation, error) {
		cancel()
		return collector.Observation{Time: at}, nil
	}
	a.run(ctx)
	if logs.FilterMessage("discarding the window snapshot").Len() != 1 {
		t.Fatal("a corrupt snapshot must be reported and discarded")
	}
}

func TestAgent_SavesASnapshotEveryFiveMinutesAndLogsAFailure(t *testing.T) {
	start := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	a, logs := newTestAgent(t, &fakeSender{outcome: reporter.Accepted}, start)
	var calls atomic.Int64
	a.now = func() time.Time { return start.Add(time.Duration(calls.Add(1)) * snapshotEvery) }
	// The snapshot directory does not exist, so the periodic save fails and must be logged.
	a.snapshotPath = filepath.Join(t.TempDir(), "missing", snapshotFile)

	ctx, cancel := context.WithCancel(context.Background())
	var polls atomic.Int32
	a.collect = func(_ context.Context, at time.Time) (collector.Observation, error) {
		if polls.Add(1) == 3 {
			cancel()
		}
		return collector.Observation{Time: at, Nodes: []v1.Node{{Name: "node-a"}}}, nil
	}
	a.run(ctx)
	if logs.FilterMessage("saving the window snapshot failed").Len() == 0 {
		t.Fatal("a failed periodic snapshot was not logged")
	}
}

func TestAgent_ShutdownKeepsUnsentReportsAndReportsThem(t *testing.T) {
	now := recentHour().Add(30 * time.Minute)
	client := &fakeSender{outcome: reporter.Retry}
	a, logs := newTestAgent(t, client, now.Add(-time.Minute))
	a.aggregator.Observe(collector.Observation{Time: now.Add(-50 * time.Second), Nodes: []v1.Node{{Name: "node-a"}}})
	a.aggregator.Observe(collector.Observation{Time: now.Add(-40 * time.Second), Nodes: []v1.Node{{Name: "node-a"}}})
	a.now = func() time.Time { return now }
	// A directory that is not empty cannot be removed like a snapshot file.
	if err := os.MkdirAll(filepath.Join(a.snapshotPath, "x"), 0o700); err != nil {
		t.Fatal(err)
	}

	a.shutdown()

	if len(client.sent()) != 1 {
		t.Fatalf("sent = %d, want the open window attempted once at shutdown", len(client.sent()))
	}
	if n, _ := a.buffer.Len(); n != 1 {
		t.Fatalf("buffered = %d, want the refused-for-now report kept for the next start", n)
	}
	left := logs.FilterMessage("reports left in the buffer are sent on the next start").All()
	if len(left) != 1 || left[0].ContextMap()["buffered_reports"] != int64(1) {
		t.Fatalf("leftover warning = %+v", left)
	}
	if logs.FilterMessage("removing the window snapshot failed").Len() != 1 {
		t.Fatal("a snapshot that could not be removed was not logged")
	}
}

func TestAgent_PollBuffersTheWindowsItCloses(t *testing.T) {
	start := recentHour()
	client := &fakeSender{outcome: reporter.Accepted}
	a, _ := newTestAgent(t, client, start)
	at := start
	a.now = func() time.Time { return at }
	a.collect = func(_ context.Context, now time.Time) (collector.Observation, error) {
		return collector.Observation{Time: now, Nodes: []v1.Node{{Name: "node-a"}}}, nil
	}
	for _, offset := range []time.Duration{0, 10 * time.Second, 20 * time.Second, collector.FirstWindow + 5*time.Second} {
		at = start.Add(offset)
		a.poll(context.Background())
	}
	if !a.ready.Load() {
		t.Fatal("a successful poll must mark the agent ready")
	}
	e, ok, err := a.buffer.Oldest()
	if err != nil || !ok || !e.WindowStart.Equal(start) {
		t.Fatalf("oldest buffered = %+v %v %v, want the first window closed by the poll", e, ok, err)
	}
	if n, _ := a.buffer.Len(); n != 1 {
		t.Fatalf("buffered = %d, want exactly the closed first window", n)
	}
}

func TestAgent_CrashAfterAWindowClosesNeverResendsItsPartialSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name            string
		sentBeforeCrash bool
	}{
		{name: "complete window already sent", sentBeforeCrash: true},
		{name: "complete window still buffered", sentBeforeCrash: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := recentHour()
			dir := t.TempDir()
			client := &fakeSender{outcome: reporter.Accepted}
			first, _ := testAgentAt(t, dir, client, start)
			at := start
			first.now = func() time.Time { return at }
			first.collect = func(_ context.Context, now time.Time) (collector.Observation, error) {
				return collector.Observation{Time: now, Nodes: []v1.Node{{Name: "node-a"}}}, nil
			}
			pollAt := func(offset time.Duration) {
				at = start.Add(offset)
				first.poll(context.Background())
			}
			for offset := time.Duration(0); offset < collector.FirstWindow; offset += 10 * time.Second {
				pollAt(offset)
				if offset == time.Minute {
					if err := first.aggregator.SaveSnapshot(first.snapshotPath); err != nil {
						t.Fatal(err)
					}
				}
			}
			pollAt(collector.FirstWindow + 5*time.Second)
			if tc.sentBeforeCrash {
				first.sender.Drain(context.Background(), true)
			}

			restartedAt := start.Add(collector.FirstWindow + 10*time.Second)
			second, _ := testAgentAt(t, dir, client, restartedAt)
			ctx, cancel := context.WithCancel(context.Background())
			second.collect = func(_ context.Context, now time.Time) (collector.Observation, error) {
				cancel()
				return collector.Observation{Time: now}, nil
			}
			second.run(ctx)

			var closedWindow, resumedWindow []v1.Report
			for _, r := range client.sent() {
				switch {
				case r.WindowStart.Equal(start):
					closedWindow = append(closedWindow, r)
				case r.WindowStart.Equal(start.Add(collector.FirstWindow)):
					resumedWindow = append(resumedWindow, r)
				}
			}
			if len(closedWindow) != 1 || closedWindow[0].Nodes[0].ObservedSeconds != collector.FirstWindow.Seconds() {
				t.Fatalf("closed window sent as %+v, want it once and complete", closedWindow)
			}
			if len(resumedWindow) != 1 || resumedWindow[0].Nodes[0].ObservedSeconds != 5 {
				t.Fatalf("next window sent as %+v, want the seconds observed before the crash kept", resumedWindow)
			}
		})
	}
}

// A crash after a closing window is buffered but before its snapshot is replaced leaves the
// window's partial snapshot behind. The restart must not buffer it over the complete report.
func TestAgent_CrashBeforeTheSnapshotIsReplacedKeepsTheBufferedWindow(t *testing.T) {
	start := recentHour()
	dir := t.TempDir()
	client := &fakeSender{outcome: reporter.Accepted}
	first, _ := testAgentAt(t, dir, client, start)
	observe := func(offset time.Duration) []v1.Report {
		return first.aggregator.Observe(collector.Observation{Time: start.Add(offset), Nodes: []v1.Node{{Name: "node-a"}}})
	}
	for offset := time.Duration(0); offset < collector.FirstWindow; offset += 10 * time.Second {
		observe(offset)
		if offset == time.Minute {
			if err := first.aggregator.SaveSnapshot(first.snapshotPath); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The poll that closes the window, cut off between buffering it and replacing the snapshot.
	closed := observe(collector.FirstWindow + 5*time.Second)
	if len(closed) != 1 {
		t.Fatalf("closed = %+v, want the first window", closed)
	}
	first.put(closed[0])

	second, logs := testAgentAt(t, dir, client, start.Add(collector.FirstWindow+10*time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	second.collect = func(_ context.Context, now time.Time) (collector.Observation, error) {
		cancel()
		return collector.Observation{Time: now}, nil
	}
	second.run(ctx)

	var closedWindow []v1.Report
	for _, r := range client.sent() {
		if r.WindowStart.Equal(start) {
			closedWindow = append(closedWindow, r)
		}
	}
	if len(closedWindow) != 1 || closedWindow[0].Nodes[0].ObservedSeconds != collector.FirstWindow.Seconds() {
		t.Fatalf("closed window sent as %+v, want it once and complete", closedWindow)
	}
	if logs.FilterMessage("discarding the window snapshot: its window is already buffered").Len() != 1 {
		t.Fatal("the discarded partial snapshot was not logged")
	}
}

func TestAgent_DiscardsAClosedWindowsSnapshotWhenTheBufferCannotBeRead(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	a, logs := newTestAgent(t, &fakeSender{}, now)
	if err := os.RemoveAll(filepath.Join(filepath.Dir(a.snapshotPath), "buffer")); err != nil {
		t.Fatal(err)
	}
	a.restoreClosed(v1.Report{ClusterID: "test-cluster", WindowStart: now.Add(-time.Hour), WindowEnd: now, Nodes: []v1.Node{{Name: "node-a"}}})
	if logs.FilterMessage("discarding the window snapshot").Len() != 1 {
		t.Fatal("a snapshot that could not be checked against the buffer was not reported")
	}
	if logs.FilterMessage("buffering a closed window failed; it is lost").Len() != 0 {
		t.Fatal("a snapshot that could not be checked against the buffer must not be buffered")
	}
}

func TestAgent_ReportsASnapshotItCouldNotReplaceAfterAWindowCloses(t *testing.T) {
	start := recentHour()
	a, logs := newTestAgent(t, &fakeSender{outcome: reporter.Accepted}, start)
	at := start
	a.now = func() time.Time { return at }
	a.collect = func(_ context.Context, now time.Time) (collector.Observation, error) {
		return collector.Observation{Time: now, Nodes: []v1.Node{{Name: "node-a"}}}, nil
	}
	// A directory that is not empty can neither be replaced nor removed like a snapshot file.
	if err := os.MkdirAll(filepath.Join(a.snapshotPath, "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []time.Duration{0, 10 * time.Second, collector.FirstWindow + 5*time.Second} {
		at = start.Add(offset)
		a.poll(context.Background())
	}
	if logs.FilterMessage("saving the window snapshot failed; removing the closed window's").Len() != 1 {
		t.Fatal("a snapshot that could not be replaced was not logged")
	}
	if logs.FilterMessage("removing the closed window's snapshot failed; a restart may resend it partially").Len() != 1 {
		t.Fatal("a closed window's snapshot that could not be removed was not logged")
	}
}

// TestMain_ExitsNonZeroWithTheCause runs the test binary as the agent, because main exits the
// process.
func TestMain_ExitsNonZeroWithTheCause(t *testing.T) {
	if os.Getenv("AGENT_MAIN_UNDER_TEST") == "1" {
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMain_ExitsNonZeroWithTheCause$")
	cmd.Env = []string{"AGENT_MAIN_UNDER_TEST=1", "COSTFLUENT_CLUSTER_ID=test-cluster"}
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("err = %v, want exit status 1; output:\n%s", err, out)
	}
	if !strings.Contains(string(out), "costfluent-k8s-agent: configuration: COSTFLUENT_TOKEN is required") {
		t.Fatalf("output = %q, want the configuration error on stderr", out)
	}
}
