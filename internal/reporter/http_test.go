package reporter

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/storage"
)

func sampleReport(hour int) v1.Report {
	start := time.Now().UTC().Truncate(time.Hour).Add(time.Duration(hour-24) * time.Hour)
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
		{http.StatusRequestEntityTooLarge, Rejected},
		{http.StatusUnprocessableEntity, Retry},
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
	if got := length(t, b); got != len(hours) {
		t.Fatalf("buffered %d of %d test reports", got, len(hours))
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
	s, c, _ := observedSender(t, b, client)
	s.Drain(context.Background(), false)
	s.Drain(context.Background(), false)
	if client.calls != 1 || length(t, b) != 1 {
		t.Fatalf("calls = %d; the second drain must wait for the backoff", client.calls)
	}
	c.Advance(minBackoff)
	s.Drain(context.Background(), false)
	if client.calls != 2 || length(t, b) != 0 {
		t.Fatalf("calls = %d, buffered = %d after the backoff", client.calls, length(t, b))
	}
}

func TestClient_ReadsRetryAfterAsSecondsOrADate(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		status int
		header string
		want   time.Duration
	}{
		{"seconds on 429", http.StatusTooManyRequests, "120", 2 * time.Minute},
		{"a date on 429", http.StatusTooManyRequests, now.Add(90 * time.Second).Format(http.TimeFormat), 90 * time.Second},
		{"seconds on 503", http.StatusServiceUnavailable, "7", 7 * time.Second},
		{"a date in the past", http.StatusTooManyRequests, now.Add(-time.Minute).Format(http.TimeFormat), 0},
		{"negative seconds", http.StatusTooManyRequests, "-5", 0},
		{"unreadable", http.StatusTooManyRequests, "soon", 0},
		{"absent", http.StatusTooManyRequests, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.header != "" {
					w.Header().Set("Retry-After", tc.header)
				}
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()
			u, _ := url.Parse(srv.URL)
			c := NewClient(u, "t", "v", nil)
			c.now = func() time.Time { return now }
			res := c.Send(context.Background(), []byte("x"))
			if res.Outcome != Retry || res.RetryAfter != tc.want {
				t.Fatalf("result = %+v, want Retry after %s", res, tc.want)
			}
		})
	}
}

func TestSender_ATooLargeReportDoesNotBlockLaterOnes(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	b := newBuffer(t, 1, 2)
	s, _, _ := observedSender(t, b, NewClient(u, "t", "v", nil))

	s.Drain(context.Background(), false)

	if calls.Load() != 2 || length(t, b) != 0 {
		t.Fatalf("calls = %d, buffered = %d; a 413 must drop that report and let the next one through", calls.Load(), length(t, b))
	}
}

func TestClient_ReadsTheAPIsTimeAndHowFarTheClockIsAheadFromDate(t *testing.T) {
	serverNow := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		node   time.Time
		noDate bool
		ahead  time.Duration
	}{
		{"clock 7 minutes ahead", serverNow.Add(7 * time.Minute), false, 7 * time.Minute},
		{"clocks agree", serverNow, false, 0},
		{"clock behind", serverNow.Add(-10 * time.Minute), false, 0},
		{"no Date", serverNow.Add(7 * time.Minute), true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.noDate {
					w.Header()["Date"] = nil
				} else {
					w.Header().Set("Date", serverNow.Format(http.TimeFormat))
				}
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"status":400,"errors":[{"name":"report.windowEnd","reason":"window_end is in the future."}]}`))
			}))
			defer srv.Close()
			u, _ := url.Parse(srv.URL)
			c := NewClient(u, "t", "v", nil)
			c.now = func() time.Time { return tc.node }

			res := c.Send(context.Background(), []byte("x"))

			wantTime := serverNow
			if tc.noDate {
				wantTime = time.Time{}
			}
			if res.Outcome != Rejected || !res.ServerTime.Equal(wantTime) || res.ClockAhead != tc.ahead {
				t.Fatalf("result = %+v, want Rejected (the sender decides on a retry) at %s, %s ahead", res, wantTime, tc.ahead)
			}
		})
	}
}

func TestSender_KeepsAReportRefusedAsEndingInTheFutureUntilTheAPIsClockPassesItsEnd(t *testing.T) {
	end := sampleReport(1).WindowEnd
	cases := []struct {
		name   string
		field  string
		date   time.Time // the response's Date; zero sends none
		node   time.Time // this node's clock
		kept   bool
		advice string
	}{
		{"clock 7 minutes ahead", "report.windowEnd", end.Add(-time.Hour), end.Add(-time.Hour + 7*time.Minute), true, "clock is 7m0s ahead"},
		{"clock 7 minutes ahead, Pascal-cased", "Report.WindowEnd", end.Add(-time.Hour), end.Add(-time.Hour + 7*time.Minute), true, "clock is 7m0s ahead"},
		{"clock fixed since the window was built", "report.windowEnd", end.Add(-time.Hour), end.Add(-time.Hour), true, "kept and retried"},
		{"clock stepped behind since", "report.windowEnd", end.Add(-time.Hour), end.Add(-time.Hour - 10*time.Minute), true, "kept and retried"},
		{"window ends at the API's time, clock 3 minutes ahead", "report.windowEnd", end, end.Add(3 * time.Minute), false, "upgrade the agent"},
		{"window ended before the API's time", "report.windowEnd", end.Add(time.Minute), end.Add(time.Minute), false, "upgrade the agent"},
		{"another field refused", "report.nodes", end.Add(-time.Hour), end.Add(-time.Hour + 7*time.Minute), false, "more nodes or pods"},
		{"no Date to compare the window with", "report.windowEnd", time.Time{}, end.Add(-time.Hour + 7*time.Minute), false, "upgrade the agent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) > 1 {
					w.WriteHeader(http.StatusAccepted)
					return
				}
				if tc.date.IsZero() {
					w.Header()["Date"] = nil
				} else {
					w.Header().Set("Date", tc.date.Format(http.TimeFormat))
				}
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"status":400,"errors":[{"name":"` + tc.field + `","reason":"refused"}]}`))
			}))
			defer srv.Close()
			u, _ := url.Parse(srv.URL)
			client := NewClient(u, "t", "v", nil)
			client.now = func() time.Time { return tc.node }
			b := newBuffer(t, 1)
			s, c, logs := observedSender(t, b, client)

			s.Drain(context.Background(), false)

			if got := length(t, b); calls.Load() != 1 || got != map[bool]int{true: 1, false: 0}[tc.kept] {
				t.Fatalf("calls = %d, buffered = %d, want the report kept: %v", calls.Load(), got, tc.kept)
			}
			if logs.FilterMessageSnippet(tc.advice).Len() != 1 {
				t.Fatalf("logs = %v, want one naming %q", logs.All(), tc.advice)
			}
			if !tc.kept {
				return
			}
			if logs.FilterField(zap.Duration("retry_in", minBackoff)).Len() != 1 || logs.FilterMessageSnippet("dropped").Len() != 0 {
				t.Fatalf("logs = %v; want one retry and no drop", logs.All())
			}
			c.Advance(minBackoff)
			s.Drain(context.Background(), false)
			if calls.Load() != 2 || length(t, b) != 0 {
				t.Fatalf("calls = %d, buffered = %d; the kept report must be sent after the backoff", calls.Load(), length(t, b))
			}
		})
	}
}

// A cluster token never expires, so one that worked until now is refused with 401 because it was
// revoked or its creator's account was disabled or deleted; the advice says so and how to replace it.
func TestAdvice_UnauthorizedSaysWhyATokenStopsAndHowToReplaceIt(t *testing.T) {
	got := Advice(Result{Status: http.StatusUnauthorized})
	for _, want := range []string{"revoked", "disabled or deleted", "Mint token on the cluster", "revoke the old one"} {
		if !strings.Contains(got, want) {
			t.Errorf("Advice(401) = %q, want it to contain %q", got, want)
		}
	}
}
