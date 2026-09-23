package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/collector"
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
		now:    time.Now,
		ready:  new(atomic.Bool),
		logger: logger,
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
	srv := httptest.NewServer(newMux(&ready))
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
