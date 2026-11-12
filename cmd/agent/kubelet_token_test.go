package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/collector"
	"github.com/costfluent/k8s-agent/internal/config"
)

const agentOwnToken = "agent-sa-token-reads-every-pod"

// tokenAPI is a fake API server's TokenRequest endpoint: it mints metrics-token-1, -2, ... valid
// for what was asked, or refuses while refuse is set.
type tokenAPI struct {
	mu     sync.Mutex
	now    func() time.Time
	refuse bool
	// answer, when set, replaces the minted status, for an API server that answers wrongly.
	answer   *authenticationv1.TokenRequestStatus
	minted   int
	requests []*authenticationv1.TokenRequest
	targets  []string
}

func (a *tokenAPI) react(action clienttesting.Action) (bool, k8sruntime.Object, error) {
	create, ok := action.(clienttesting.CreateAction)
	if !ok || action.GetSubresource() != "token" {
		return false, nil, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	req := create.GetObject().(*authenticationv1.TokenRequest)
	a.requests = append(a.requests, req)
	a.targets = append(a.targets, action.GetNamespace()+"/"+create.(clienttesting.CreateActionImpl).Name)
	if a.refuse {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "serviceaccounts/token"}, "costfluent-k8s-agent-kubelet", nil)
	}
	out := req.DeepCopy()
	if a.answer != nil {
		out.Status = *a.answer
		return true, out, nil
	}
	a.minted++
	out.Status = authenticationv1.TokenRequestStatus{
		Token:               fmt.Sprintf("metrics-token-%d", a.minted),
		ExpirationTimestamp: metav1.NewTime(a.now().Add(time.Duration(*req.Spec.ExpirationSeconds) * time.Second)),
	}
	return true, out, nil
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestKubeletClient_SendsOnlyTheMetricsToken(t *testing.T) {
	srv, auth := kubeletServer(t, kubeletMetrics)
	clock := &fakeClock{t: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	api := &tokenAPI{now: clock.now}
	clientset := fake.NewClientset()
	clientset.PrependReactor("create", "serviceaccounts", api.react)

	restCfg := &rest.Config{Host: srv.URL, BearerToken: agentOwnToken, TLSClientConfig: rest.TLSClientConfig{CAData: certPEM(srv)}}
	token := newKubeletToken(clientset, "costfluent", "costfluent-k8s-agent-kubelet", clock.now)
	client, err := kubeletHTTPClient(restCfg, false, token)
	if err != nil {
		t.Fatal(err)
	}
	core, logs := observer.New(zapcore.DebugLevel)
	kubelet := collector.NewKubeletCollector(client, config.DefaultNodeAddressTypes, 0, zap.New(core))
	node := kubeletNodeAt(t, srv.URL)
	scrape := func() map[string]string {
		t.Helper()
		_, failed := kubelet.Scrape(context.Background(), []*corev1.Node{node})
		return failed
	}

	if failed := scrape(); len(failed) != 0 {
		t.Fatalf("unscraped = %v, want the node read", failed)
	}
	if got := auth.Load(); got != "Bearer metrics-token-1" {
		t.Fatalf("Authorization = %q, want the minted metrics token", got)
	}
	if len(api.requests) != 1 || api.targets[0] != "costfluent/costfluent-k8s-agent-kubelet" {
		t.Fatalf("token requests = %d for %v, want one for costfluent/costfluent-k8s-agent-kubelet", len(api.requests), api.targets)
	}
	if exp := api.requests[0].Spec.ExpirationSeconds; exp == nil || *exp != 3600 {
		t.Fatalf("requested validity = %v, want 3600 s", exp)
	}

	clock.advance(30 * time.Minute)
	scrape()
	if got := auth.Load(); got != "Bearer metrics-token-1" || len(api.requests) != 1 {
		t.Fatalf("after 30 min: Authorization = %q after %d requests, want the first token reused", got, len(api.requests))
	}

	clock.advance(25 * time.Minute)
	scrape()
	if got := auth.Load(); got != "Bearer metrics-token-1" {
		t.Fatalf("after 55 min: Authorization = %q, want the still valid token while it renews", got)
	}
	settle(token)
	scrape()
	if got := auth.Load(); got != "Bearer metrics-token-2" || len(api.requests) != 2 {
		t.Fatalf("after 55 min: Authorization = %q after %d requests, want a token renewed before the first expired", got, len(api.requests))
	}

	api.refuse = true
	clock.advance(time.Hour)
	if failed := scrape(); failed["kubelet"] != v1.UnscrapedUnauthorized {
		t.Fatalf("with the token refused and the last one expired: unscraped = %v, want unauthorized", failed)
	}
	if got := auth.Load(); got != "Bearer metrics-token-2" {
		t.Fatalf("Authorization = %q, want no request sent while no token is valid", got)
	}

	api.refuse = false
	clock.advance(time.Minute)
	if failed := scrape(); len(failed) != 0 {
		t.Fatalf("the next poll after a refusal: unscraped = %v, want the token requested again", failed)
	}
	if got := auth.Load(); got != "Bearer metrics-token-3" {
		t.Fatalf("Authorization = %q, want the token minted on retry", got)
	}

	for _, e := range logs.All() {
		line := fmt.Sprint(e.Message, e.ContextMap())
		if strings.Contains(line, "metrics-token-") || strings.Contains(line, agentOwnToken) {
			t.Errorf("logged a token: %s", line)
		}
	}
	if restCfg.BearerToken != agentOwnToken {
		t.Fatal("kubeletHTTPClient must not modify the caller's rest.Config")
	}
}

func TestKubeletToken_RefusesAnAnswerWithoutAUsableToken(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	for name, status := range map[string]authenticationv1.TokenRequestStatus{
		"no token":      {ExpirationTimestamp: metav1.NewTime(clock.now().Add(time.Hour))},
		"expired token": {Token: "metrics-token-stale", ExpirationTimestamp: metav1.NewTime(clock.now())},
	} {
		t.Run(name, func(t *testing.T) {
			api := &tokenAPI{now: clock.now, answer: &status}
			clientset := fake.NewClientset()
			clientset.PrependReactor("create", "serviceaccounts", api.react)
			token, err := newKubeletToken(clientset, "costfluent", "costfluent-k8s-agent-kubelet", clock.now).get(context.Background())
			var tokenErr *collector.TokenError
			if token != "" || !errors.As(err, &tokenErr) {
				t.Fatalf("get = %q, %v; want no token and a *collector.TokenError", token, err)
			}
			if strings.Contains(err.Error(), "metrics-token-stale") {
				t.Errorf("err = %v, which shows the token", err)
			}
		})
	}
}

// Under a chart that creates no metrics account, the scrapes keep carrying the agent's own token.
func TestKubeletClient_WithoutTheMetricsAccountSendsTheAgentsOwnToken(t *testing.T) {
	srv, auth := kubeletServer(t, kubeletMetrics)
	client, err := kubeletHTTPClient(&rest.Config{Host: srv.URL, BearerToken: agentOwnToken, TLSClientConfig: rest.TLSClientConfig{CAData: certPEM(srv)}}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	kubelet := collector.NewKubeletCollector(client, config.DefaultNodeAddressTypes, 0, zap.NewNop())
	if _, failed := kubelet.Scrape(context.Background(), []*corev1.Node{kubeletNodeAt(t, srv.URL)}); len(failed) != 0 {
		t.Fatalf("unscraped = %v, want the node read", failed)
	}
	if got := auth.Load(); got != "Bearer "+agentOwnToken {
		t.Fatalf("Authorization = %q, want the agent's own token", got)
	}
}

// A renewal that fails keeps the token still valid, so a short API outage does not cost a poll.
func TestKubeletClient_KeepsAValidTokenWhenARenewalFails(t *testing.T) {
	srv, auth := kubeletServer(t, kubeletMetrics)
	clock := &fakeClock{t: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	api := &tokenAPI{now: clock.now}
	clientset := fake.NewClientset()
	clientset.PrependReactor("create", "serviceaccounts", api.react)
	token := newKubeletToken(clientset, "costfluent", "costfluent-k8s-agent-kubelet", clock.now)
	client, err := kubeletHTTPClient(&rest.Config{Host: srv.URL, TLSClientConfig: rest.TLSClientConfig{CAData: certPEM(srv)}}, false, token)
	if err != nil {
		t.Fatal(err)
	}
	kubelet := collector.NewKubeletCollector(client, config.DefaultNodeAddressTypes, 0, zap.NewNop())
	node := kubeletNodeAt(t, srv.URL)

	kubelet.Scrape(context.Background(), []*corev1.Node{node})
	api.refuse = true
	clock.advance(55 * time.Minute)
	if _, failed := kubelet.Scrape(context.Background(), []*corev1.Node{node}); len(failed) != 0 {
		t.Fatalf("unscraped = %v, want the still valid token used", failed)
	}
	settle(token)
	if got := auth.Load(); got != "Bearer metrics-token-1" || len(api.requests) != 2 {
		t.Fatalf("Authorization = %q after %d requests, want the renewal tried and the first token kept", got, len(api.requests))
	}
}

func kubeletNodeAt(t *testing.T, url string) *corev1.Node {
	t.Helper()
	host, port, err := net.SplitHostPort(strings.TrimPrefix(url, "https://"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.ParseInt(port, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "kubelet"},
		Status: corev1.NodeStatus{
			Addresses:       []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: host}},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: int32(p)}},
		},
	}
}

// hangingTokens is an API server whose TokenRequest endpoint can hang instead of refusing: while
// hanging, each CreateToken blocks until its context ends or letThrough passes it on to the fake
// clientset behind it.
type hangingTokens struct {
	kubernetes.Interface
	calls atomic.Int32

	mu   sync.Mutex
	gate chan struct{}
	ctxs []context.Context
	// arrived is closed, and replaced, each time a token request reaches the server.
	arrived chan struct{}
}

func newHangingTokens(t *testing.T, api *tokenAPI) *hangingTokens {
	clientset := fake.NewClientset()
	clientset.PrependReactor("create", "serviceaccounts", api.react)
	h := &hangingTokens{Interface: clientset, arrived: make(chan struct{})}
	t.Cleanup(h.letThrough)
	return h
}

func (h *hangingTokens) hang() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gate = make(chan struct{})
}

func (h *hangingTokens) letThrough() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.gate != nil {
		close(h.gate)
		h.gate = nil
	}
}

// awaitCalls waits until n token requests have reached the server. get starts a renewal on a
// goroutine of its own and, holding a valid token, returns without waiting for it, so the renewal's
// request arrives some time after get returns; counting before then sees one request too few.
func (h *hangingTokens) awaitCalls(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		h.mu.Lock()
		got, arrived := len(h.ctxs), h.arrived
		h.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-arrived:
		case <-deadline:
			t.Fatalf("token requests = %d after 10s, want %d", got, n)
		}
	}
}

// lastContext is the context the latest CreateToken was called with.
func (h *hangingTokens) lastContext(t *testing.T) context.Context {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.ctxs) == 0 {
		t.Fatal("no token was requested")
	}
	return h.ctxs[len(h.ctxs)-1]
}

func (h *hangingTokens) CoreV1() typedcorev1.CoreV1Interface {
	return hangingCore{CoreV1Interface: h.Interface.CoreV1(), h: h}
}

type hangingCore struct {
	typedcorev1.CoreV1Interface
	h *hangingTokens
}

func (c hangingCore) ServiceAccounts(namespace string) typedcorev1.ServiceAccountInterface {
	return hangingAccounts{ServiceAccountInterface: c.CoreV1Interface.ServiceAccounts(namespace), h: c.h}
}

type hangingAccounts struct {
	typedcorev1.ServiceAccountInterface
	h *hangingTokens
}

func (a hangingAccounts) CreateToken(ctx context.Context, name string, tr *authenticationv1.TokenRequest, opts metav1.CreateOptions) (*authenticationv1.TokenRequest, error) {
	a.h.calls.Add(1)
	a.h.mu.Lock()
	a.h.ctxs = append(a.h.ctxs, ctx)
	close(a.h.arrived)
	a.h.arrived = make(chan struct{})
	gate := a.h.gate
	a.h.mu.Unlock()
	if gate != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-gate:
		}
	}
	return a.ServiceAccountInterface.CreateToken(ctx, name, tr, opts)
}

// assertRenewalOnItsOwnContext fails unless the token request in flight runs on a context with a
// deadline that the caller's ending did not end.
func assertRenewalOnItsOwnContext(t *testing.T, h *hangingTokens) {
	t.Helper()
	ctx := h.lastContext(t)
	if err := ctx.Err(); err != nil {
		t.Fatalf("the token request ended with its caller (%v), want it on a context of its own", err)
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("the token request has no deadline, want it bounded")
	}
}

// An API server that hangs at the renewal point must not cost a poll while the token is still
// valid: kubelet scrapes never waited on the API server before the metrics account existed.
func TestKubeletClient_ScrapesWithTheValidTokenWhileARenewalHangs(t *testing.T) {
	srv, auth := kubeletServer(t, kubeletMetrics)
	clock := &fakeClock{t: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	api := &tokenAPI{now: clock.now}
	h := newHangingTokens(t, api)
	token := newKubeletToken(h, "costfluent", "costfluent-k8s-agent-kubelet", clock.now)
	client, err := kubeletHTTPClient(&rest.Config{Host: srv.URL, TLSClientConfig: rest.TLSClientConfig{CAData: certPEM(srv)}}, false, token)
	if err != nil {
		t.Fatal(err)
	}
	kubelet := collector.NewKubeletCollector(client, config.DefaultNodeAddressTypes, 0, zap.NewNop())
	node := kubeletNodeAt(t, srv.URL)
	if _, failed := kubelet.Scrape(context.Background(), []*corev1.Node{node}); len(failed) != 0 {
		t.Fatalf("first scrape: unscraped = %v", failed)
	}

	h.hang()
	clock.advance(55 * time.Minute)
	for scrape := 1; scrape <= 2; scrape++ {
		if _, failed := kubelet.Scrape(context.Background(), []*corev1.Node{node}); len(failed) != 0 {
			t.Fatalf("scrape %d while the renewal hangs: unscraped = %v, want the still valid token used", scrape, failed)
		}
		if got := auth.Load(); got != "Bearer metrics-token-1" {
			t.Fatalf("scrape %d: Authorization = %q, want the still valid token", scrape, got)
		}
	}
	h.awaitCalls(t, 2)
	if got := h.calls.Load(); got != 2 {
		t.Fatalf("token requests = %d, want the first and one renewal still in flight", got)
	}
	assertRenewalOnItsOwnContext(t, h)
}

func TestKubeletToken_ConcurrentCallersDuringARenewalMakeOneRequest(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	h := newHangingTokens(t, &tokenAPI{now: clock.now})
	token := newKubeletToken(h, "costfluent", "costfluent-k8s-agent-kubelet", clock.now)
	if _, err := token.get(context.Background()); err != nil {
		t.Fatal(err)
	}

	h.hang()
	clock.advance(55 * time.Minute)
	const callers = 16
	var wg sync.WaitGroup
	got := make([]string, callers)
	errs := make([]error, callers)
	for i := range callers {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			got[i], errs[i] = token.get(ctx)
		})
	}
	wg.Wait()
	for i := range callers {
		if got[i] != "metrics-token-1" || errs[i] != nil {
			t.Fatalf("caller %d: get = %q, %v; want the still valid token", i, got[i], errs[i])
		}
	}
	h.awaitCalls(t, 2)
	if n := h.calls.Load(); n != 2 {
		t.Fatalf("token requests = %d, want the first and one renewal for all %d callers", n, callers)
	}
	assertRenewalOnItsOwnContext(t, h)
}

func TestKubeletToken_ARenewalThatLaterSucceedsReplacesTheToken(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	h := newHangingTokens(t, &tokenAPI{now: clock.now})
	token := newKubeletToken(h, "costfluent", "costfluent-k8s-agent-kubelet", clock.now)
	if _, err := token.get(context.Background()); err != nil {
		t.Fatal(err)
	}

	h.hang()
	clock.advance(55 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if got, err := token.get(ctx); got != "metrics-token-1" || err != nil {
		t.Fatalf("get during the renewal = %q, %v; want the still valid token", got, err)
	}
	h.awaitCalls(t, 2)
	h.letThrough()
	settle(token)
	if got, err := token.get(context.Background()); got != "metrics-token-2" || err != nil {
		t.Fatalf("get after the renewal = %q, %v; want the renewed token", got, err)
	}
	clock.advance(30 * time.Minute)
	if got, err := token.get(context.Background()); got != "metrics-token-2" || err != nil || h.calls.Load() != 2 {
		t.Fatalf("30 min after the renewal: get = %q, %v after %d requests; want the renewed token reused", got, err, h.calls.Load())
	}
}

func TestKubeletToken_WithoutAValidTokenACallerWaitsOnlyItsOwnContext(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	h := newHangingTokens(t, &tokenAPI{now: clock.now})
	token := newKubeletToken(h, "costfluent", "costfluent-k8s-agent-kubelet", clock.now)

	h.hang()
	const callers = 4
	var wg sync.WaitGroup
	errs := make([]error, callers)
	took := make([]time.Duration, callers)
	for i := range callers {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			began := time.Now()
			var got string
			got, errs[i] = token.get(ctx)
			took[i] = time.Since(began)
			if got != "" {
				errs[i] = fmt.Errorf("got token %q", got)
			}
		})
	}
	wg.Wait()
	for i := range callers {
		var tokenErr *collector.TokenError
		if !errors.As(errs[i], &tokenErr) || !errors.Is(errs[i], context.DeadlineExceeded) {
			t.Errorf("caller %d: err = %v, want a *collector.TokenError for its own deadline", i, errs[i])
		}
		// The deadline is 100 ms; the bound leaves room for a loaded race run, and is far below
		// the renewal's own timeout.
		if took[i] > 3*time.Second {
			t.Errorf("caller %d waited %v, want at most its own 100 ms deadline", i, took[i])
		}
	}
	h.awaitCalls(t, 1)
	if n := h.calls.Load(); n != 1 {
		t.Fatalf("token requests = %d, want one for all %d callers", n, callers)
	}
	assertRenewalOnItsOwnContext(t, h)

	h.letThrough()
	settle(token)
	if got, err := token.get(context.Background()); got != "metrics-token-1" || err != nil {
		t.Fatalf("get after the request went through = %q, %v; want the minted token", got, err)
	}
}

// settle waits for the token request in flight, if any, to finish.
func settle(k *kubeletToken) {
	k.mu.Lock()
	done := k.inFlight
	k.mu.Unlock()
	if done != nil {
		<-done
	}
}
