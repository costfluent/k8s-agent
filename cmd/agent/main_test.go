package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/collector"
	"github.com/costfluent/k8s-agent/internal/config"
	"github.com/costfluent/k8s-agent/internal/reporter"
	"github.com/costfluent/k8s-agent/internal/storage"
)

func TestAgent_ShutdownClosesTheWindowAndFlushesOnAFreshContext(t *testing.T) {
	var mu sync.Mutex
	var received []v1.Report
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("gzip: %v", err)
			return
		}
		var rep v1.Report
		if err := json.NewDecoder(zr).Decode(&rep); err != nil {
			t.Errorf("decode: %v", err)
			return
		}
		mu.Lock()
		received = append(received, rep)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	dir := t.TempDir()
	logger := zap.NewNop()
	buffer, err := storage.Open(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, _ := url.Parse(srv.URL)
	interval := 20 * time.Millisecond
	var polls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	a := &agent{
		interval:     interval,
		snapshotPath: filepath.Join(dir, snapshotFile),
		aggregator:   collector.NewAggregator("pve-dev", "test", "id", interval, time.Now()),
		buffer:       buffer,
		sender:       reporter.NewSender(buffer, reporter.NewClient(endpoint, "t", "test", nil), logger),
		collect: func(_ context.Context, now time.Time) (collector.Observation, error) {
			if polls.Add(1) == 5 {
				cancel()
			}
			return collector.Observation{Time: now, Nodes: []v1.Node{{Name: "n1"}}}, nil
		},
		now:         time.Now,
		ready:       new(atomic.Bool),
		logger:      logger,
		lastSuccess: new(atomic.Int64),
	}
	if err := os.WriteFile(a.snapshotPath, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() { a.run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the agent did not stop")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || received[0].ClusterID != "pve-dev" || received[0].Nodes[0].ObservedSeconds <= 0 {
		t.Fatalf("received = %+v; the open window must be sent at shutdown", received)
	}
	if n, _ := buffer.Len(); n != 0 {
		t.Fatalf("buffered = %d after the flush", n)
	}
	if _, err := os.Stat(a.snapshotPath); !os.IsNotExist(err) {
		t.Fatalf("the snapshot must be removed after a clean shutdown: %v", err)
	}
	if !a.ready.Load() {
		t.Fatal("readiness was never set")
	}
}

func TestMux_ServesHealthBeforeReady(t *testing.T) {
	var ready atomic.Bool
	srv := httptest.NewServer(newMux(&ready, func() error { return nil }))
	defer srv.Close()
	get := func(path string) int {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if get("/healthz") != 200 || get("/readyz") != 503 || get("/metrics") != 200 {
		t.Fatal("unexpected status before the first poll")
	}
	ready.Store(true)
	if get("/readyz") != 200 {
		t.Fatal("readyz after the first poll")
	}
}

func TestApplyMemoryLimit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		limit      int64
		goMemLimit string
		want       int64
	}{
		{"the container's limit sets the soft limit", 256 << 20, "", 192 << 20},
		{"GOMEMLIMIT wins", 256 << 20, "300MiB", 0},
		{"no container limit, no soft limit", 0, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var set int64
			lookup := func(string) (string, bool) { return tc.goMemLimit, tc.goMemLimit != "" }
			got := applyMemoryLimit(&config.Config{MemoryLimitBytes: tc.limit}, lookup, func(n int64) int64 { set = n; return 0 })
			if got != tc.want || set != tc.want {
				t.Fatalf("applied %d (set %d), want %d", got, set, tc.want)
			}
		})
	}
}

func TestAgent_HealthyUntilPollsStopForLongerThanALiveAgentTakes(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		interval time.Duration
		since    time.Duration
		healthy  bool
	}{
		{"a poll a minute ago", time.Minute, time.Minute, true},
		{"four intervals ago", time.Minute, 4 * time.Minute, true},
		{"a slow first poll at a short interval", 5 * time.Second, 9 * time.Minute, true},
		{"just past the ten minute floor", 5 * time.Second, 10*time.Minute + time.Second, false},
		{"just past five intervals", 5 * time.Minute, 25*time.Minute + time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &agent{interval: tc.interval, now: func() time.Time { return now }, lastSuccess: new(atomic.Int64)}
			a.lastSuccess.Store(now.Add(-tc.since).UnixNano())
			if err := a.healthy(); (err == nil) != tc.healthy {
				t.Fatalf("healthy() = %v, want healthy %v", err, tc.healthy)
			}
		})
	}
}

func TestMux_HealthzReportsWhyTheAgentIsUnhealthy(t *testing.T) {
	var ready atomic.Bool
	srv := httptest.NewServer(newMux(&ready, func() error { return errors.New("no successful poll since 11:00") }))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(body), "no successful poll since 11:00") {
		t.Fatalf("healthz = %d %q, want 503 with the reason", resp.StatusCode, body)
	}
}

func TestAgent_PollRecordsItsSuccess(t *testing.T) {
	start := recentHour()
	a, _ := newTestAgent(t, &fakeSender{}, start)
	a.collect = func(_ context.Context, now time.Time) (collector.Observation, error) {
		return collector.Observation{Time: now, Nodes: []v1.Node{{Name: "n"}}, Duration: time.Second}, nil
	}
	a.poll(context.Background())
	if got := a.lastSuccess.Load(); got != start.UnixNano() {
		t.Fatalf("lastSuccess = %v, want the poll time", time.Unix(0, got))
	}
}
