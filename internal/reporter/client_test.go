package reporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func TestClient_ProxyCarriesReportTraffic(t *testing.T) {
	var proxied atomic.Value
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied.Store(r.Method + " " + r.URL.String())
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":"kr_proxy","clusterId":"c","status":"accepted"}`))
	}))
	defer proxy.Close()
	proxyURL, _ := url.Parse(proxy.URL)
	endpoint, _ := url.Parse("http://reports.example.invalid/api/")

	res := NewClient(endpoint, "t", "v", proxyURL).Send(context.Background(), []byte("x"))
	if res.Outcome != Accepted || res.ID != "kr_proxy" {
		t.Fatalf("result = %+v, want accepted through the proxy", res)
	}
	if got := proxied.Load(); got != "POST http://reports.example.invalid/api/v1/kubernetes/reports" {
		t.Fatalf("proxy saw %v, want the absolute report URL with the endpoint's path kept", got)
	}
}

func TestClient_AcceptedWithoutAReadableBodyHasNoID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html>ok</html>"))
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	res := NewClient(u, "t", "v", nil).Send(context.Background(), []byte("x"))
	if res.Outcome != Accepted || res.Status != http.StatusOK || res.ID != "" || res.Message != "" {
		t.Fatalf("result = %+v, want accepted with no id", res)
	}
}

func TestClient_InvalidEndpointIsRetriedNotPanicked(t *testing.T) {
	u := &url.URL{Scheme: "http", Host: "bad host"}
	res := NewClient(u, "t", "v", nil).Send(context.Background(), []byte("x"))
	if res.Outcome != Retry || !strings.HasPrefix(res.Message, "building request:") {
		t.Fatalf("result = %+v", res)
	}
}

func TestClient_CancelledContextIsRetried(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := NewClient(u, "t", "v", nil).Send(ctx, []byte("x"))
	if res.Outcome != Retry || !strings.Contains(res.Message, "context canceled") {
		t.Fatalf("result = %+v, want a retry naming the cancellation", res)
	}
	if hits.Load() != 0 {
		t.Fatal("a cancelled send reached the API")
	}
}

func TestClient_ErrorMessages(t *testing.T) {
	long := strings.Repeat("a", 400)
	cases := []struct {
		name, body, want string
	}{
		{"problem with title and detail", `{"title":"Conflict","detail":"window overlaps","status":409}`, "Conflict: window overlaps"},
		{"problem with title only", `{"title":"Gone","status":410}`, "Gone"},
		{"problem with detail only", `{"detail":"cluster deleted","status":410}`, "cluster deleted"},
		{"problem without title or detail", `{"status":409}`, `{"status":409}`},
		{"plain text", "  upstream timed out \n", "upstream timed out"},
		{"malformed JSON", `{"title":`, `{"title":`},
		{"long body truncated", long, long[:300]},
		{"long reasons truncated", `{"title":"t","errors":[{"name":"n","reason":"` + long + `"}]}`, ("t: " + long)[:300]},
		{"empty body", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			u, _ := url.Parse(srv.URL)
			res := NewClient(u, "t", "v", nil).Send(context.Background(), []byte("x"))
			if res.Outcome != Rejected || res.Message != tc.want {
				t.Fatalf("result = %+v, want message %q", res, tc.want)
			}
		})
	}
}

func TestClient_RefusalNamesTheRefusedFields(t *testing.T) {
	const title = `"title":"One or more validation errors occurred","status":400,"code":"VALIDATION_FAILED","retryable":false`
	cases := []struct {
		name, errors, message string
		size                  bool
	}{
		{"too many pods", `[{"name":"report.pods","reason":"A report carries at most 50000 pods."}]`,
			"One or more validation errors occurred: A report carries at most 50000 pods.", true},
		{"too many nodes, Pascal-cased", `[{"name":"Report.Nodes","reason":"A report carries at most 1000 nodes."}]`,
			"One or more validation errors occurred: A report carries at most 1000 nodes.", true},
		{"a pod's own field", `[{"name":"report.pods[3].containers","reason":"A pod carries at most 64 containers."}]`,
			"One or more validation errors occurred: A pod carries at most 64 containers.", false},
		{"two reasons", `[{"name":"report.clusterId","reason":"Required."},{"name":"report.windowEnd","reason":"After start."}]`,
			"One or more validation errors occurred: Required.; After start.", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{` + title + `,"errors":` + tc.errors + `}`))
			}))
			defer srv.Close()
			u, _ := url.Parse(srv.URL)
			res := NewClient(u, "t", "v", nil).Send(context.Background(), []byte("x"))
			if res.Outcome != Rejected || res.Message != tc.message {
				t.Fatalf("result = %+v, want message %q", res, tc.message)
			}
			advice := Advice(res)
			if got := strings.Contains(advice, "more nodes or pods than Costfluent collects"); got != tc.size {
				t.Fatalf("Advice = %q, size advice %v, want %v", advice, got, tc.size)
			}
			if got := strings.Contains(advice, "upgrade the agent"); got == tc.size {
				t.Fatalf("Advice = %q; only a refusal that is not about size says to upgrade the agent", advice)
			}
		})
	}
}

func TestAdvice_NamesTheNextStepPerStatus(t *testing.T) {
	cases := map[int]string{
		http.StatusUnauthorized:          "the token is not valid",
		http.StatusForbidden:             "the token is not this cluster's token",
		http.StatusConflict:              "run one agent per cluster",
		http.StatusGone:                  "add it again under Kubernetes, Clusters",
		http.StatusBadRequest:            "upgrade the agent",
		http.StatusRequestEntityTooLarge: "larger than the API accepts; it is dropped",
		http.StatusTooManyRequests:       "retrying with backoff",
		0:                                "retrying with backoff",
	}
	for status, want := range cases {
		if got := Advice(Result{Status: status}); !strings.Contains(got, want) {
			t.Errorf("Advice(%d) = %q, want it to contain %q", status, got, want)
		}
	}
}
