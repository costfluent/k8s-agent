package storage

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/metrics"
)

var base = time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)

func report(hour int) v1.Report {
	start := base.Add(time.Duration(hour) * time.Hour)
	return v1.Report{SchemaVersion: 1, ClusterID: "c", WindowStart: start, WindowEnd: start.Add(time.Hour), Nodes: []v1.Node{{Name: "n"}}}
}

func TestBuffer_ReturnsOldestFirstAndRemoves(t *testing.T) {
	b, err := Open(t.TempDir(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	b.now = func() time.Time { return base.Add(10 * time.Hour) }
	for _, h := range []int{3, 1, 2} {
		if err := b.Put(report(h)); err != nil {
			t.Fatal(err)
		}
	}
	e, ok, err := b.Oldest()
	if err != nil || !ok || !e.WindowStart.Equal(base.Add(time.Hour)) || len(e.Body) == 0 {
		t.Fatalf("oldest = %+v %v %v", e, ok, err)
	}
	if err := b.Remove(e.Name); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Len(); n != 2 {
		t.Fatalf("len = %d", n)
	}
	if got := testutil.ToFloat64(metrics.BufferReports); got != 2 {
		t.Fatalf("buffer gauge = %v", got)
	}
}

func TestBuffer_DropsOldestBeyondAgeAndSize(t *testing.T) {
	b, err := Open(t.TempDir(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	b.now = func() time.Time { return base.Add(100 * time.Hour) }
	ageBefore := testutil.ToFloat64(metrics.BufferDropped.WithLabelValues("age"))
	if err := b.Put(report(1)); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Len(); n != 0 {
		t.Fatalf("a report older than 96h was kept")
	}
	if testutil.ToFloat64(metrics.BufferDropped.WithLabelValues("age"))-ageBefore != 1 {
		t.Fatal("the age drop was not counted")
	}

	for h := 10; h < 14; h++ {
		if err := b.Put(report(h)); err != nil {
			t.Fatal(err)
		}
	}
	e, _, _ := b.Oldest()
	b.maxBytes = int64(len(e.Body)) * 5 / 2
	sizeBefore := testutil.ToFloat64(metrics.BufferDropped.WithLabelValues("size"))
	if err := b.Put(report(14)); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Len(); n != 2 {
		t.Fatalf("len after the size cap = %d", n)
	}
	if testutil.ToFloat64(metrics.BufferDropped.WithLabelValues("size"))-sizeBefore != 3 {
		t.Fatal("size drops were not counted")
	}
	if e, _, _ := b.Oldest(); !e.WindowStart.Equal(base.Add(13 * time.Hour)) {
		t.Fatalf("the newest reports must survive: oldest is %s", e.WindowStart)
	}
}

func TestOpen_FailsOnAnUnwritableDataDir(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("permissions are not enforced for this user")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := Open(filepath.Join(dir, "data"), zap.NewNop()); err == nil {
		t.Fatal("expected an error for a read-only data directory")
	}
}

func TestInstanceID_IsCreatedOnceAndKept(t *testing.T) {
	dir := t.TempDir()
	first, err := InstanceID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(first) {
		t.Fatalf("not a v4 UUID: %q", first)
	}
	second, err := InstanceID(dir)
	if err != nil || second != first {
		t.Fatalf("second = %q %v", second, err)
	}
}
