package reporter

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"go.uber.org/zap"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/storage"
)

func sampleReport(hour int) v1.Report {
	start := time.Date(2026, 9, 23, hour, 0, 0, 0, time.UTC)
	return v1.Report{SchemaVersion: 1, ClusterID: "pve-dev", WindowStart: start, WindowEnd: start.Add(time.Hour), Nodes: []v1.Node{{Name: "n"}}}
}

func TestClient_SendsGzipBearerReportAndClassifiesStatus(t *testing.T) {
	cases := []struct {
		status int
		want   Outcome
	}{
		{http.StatusAccepted, Accepted},
		{http.StatusBadRequest, Rejected},
		{http.StatusUnauthorized, Unauthorized},
		{http.StatusForbidden, Unauthorized},
		{http.StatusConflict, Rejected},
		{http.StatusGone, Rejected},
		{http.StatusRequestEntityTooLarge, Retry},
		{http.StatusTooManyRequests, Retry},
		{http.StatusInternalServerError, Retry},
		{http.StatusServiceUnavailable, Retry},
	}
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/kubernetes/reports" {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
				}
				if r.Header.Get("Authorization") != "Bearer cf_org_x" || r.Header.Get("Content-Encoding") != "gzip" ||
					r.Header.Get("Content-Type") != "application/json" || r.Header.Get("User-Agent") != "costfluent-k8s-agent/0.2.0" {
					t.Errorf("headers = %v", r.Header)
				}
				zr, err := gzip.NewReader(r.Body)
				if err != nil {
					t.Fatalf("body is not gzip: %v", err)
				}
				dec := json.NewDecoder(zr)
				dec.DisallowUnknownFields()
				var got v1.Report
				if err := dec.Decode(&got); err != nil || got.ClusterID != "pve-dev" {
					t.Errorf("body = %+v %v", got, err)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				if tc.status == http.StatusAccepted {
					_ = json.NewEncoder(w).Encode(v1.ReportAccepted{ID: "kr_1", ClusterID: "pve-dev", Status: "accepted"})
				} else {
					_ = json.NewEncoder(w).Encode(v1.Problem{Title: "Refused", Detail: "because", Status: tc.status})
				}
			}))
			defer srv.Close()
			u, _ := url.Parse(srv.URL + "/")
			body, err := storage.Encode(sampleReport(10))
			if err != nil {
				t.Fatal(err)
			}
			res := NewClient(u, "cf_org_x", "0.2.0", nil).Send(context.Background(), body)
			if res.Outcome != tc.want || res.Status != tc.status {
				t.Fatalf("result = %+v, want outcome %v", res, tc.want)
			}
			if tc.want == Accepted && res.ID != "kr_1" {
				t.Errorf("id = %q", res.ID)
			}
			if tc.want != Accepted && res.Message != "Refused: because" {
				t.Errorf("message = %q", res.Message)
			}
		})
	}
}

func TestClient_NetworkErrorIsRetried(t *testing.T) {
	u, _ := url.Parse("http://127.0.0.1:1")
	if res := NewClient(u, "t", "v", nil).Send(context.Background(), []byte("x")); res.Outcome != Retry {
		t.Fatalf("result = %+v", res)
	}
}

type scripted struct {
	results []Result
	calls   int
}

func (s *scripted) Send(context.Context, []byte) Result {
	r := s.results[min(s.calls, len(s.results)-1)]
	s.calls++
	return r
}

func newBuffer(t *testing.T, hours ...int) *storage.Buffer {
	t.Helper()
	b, err := storage.Open(t.TempDir(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hours {
		if err := b.Put(sampleReport(h)); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

func length(t *testing.T, b *storage.Buffer) int {
	n, err := b.Len()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSender_DrainsAcceptedAndDropsRejected(t *testing.T) {
	b := newBuffer(t, 1, 2, 3)
	client := &scripted{results: []Result{{Outcome: Accepted}, {Outcome: Rejected, Status: 409}, {Outcome: Accepted}}}
	NewSender(b, client, zap.NewNop()).Drain(context.Background(), false)
	if client.calls != 3 || length(t, b) != 0 {
		t.Fatalf("calls = %d, buffered = %d", client.calls, length(t, b))
	}
}

func TestSender_UnauthorizedKeepsReportsAndStopsSending(t *testing.T) {
	b := newBuffer(t, 1, 2)
	client := &scripted{results: []Result{{Outcome: Unauthorized, Status: 401}}}
	s := NewSender(b, client, zap.NewNop())
	s.Drain(context.Background(), false)
	s.Drain(context.Background(), true)
	if client.calls != 1 || length(t, b) != 2 {
		t.Fatalf("calls = %d, buffered = %d; a 401 must neither retry nor drop", client.calls, length(t, b))
	}
}

func TestSender_RetryBacksOffUntilForced(t *testing.T) {
	b := newBuffer(t, 1)
	client := &scripted{results: []Result{{Outcome: Retry, Status: 503}, {Outcome: Accepted}}}
	s := NewSender(b, client, zap.NewNop())
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	s.Drain(context.Background(), false)
	s.Drain(context.Background(), false)
	if client.calls != 1 || length(t, b) != 1 {
		t.Fatalf("calls = %d; the second drain must wait for the backoff", client.calls)
	}
	now = now.Add(minBackoff)
	s.Drain(context.Background(), false)
	if client.calls != 2 || length(t, b) != 0 {
		t.Fatalf("calls = %d, buffered = %d after the backoff", client.calls, length(t, b))
	}
}
