package storage

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
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
	if err != nil || !ok || !e.WindowStart.Equal(base.Add(time.Hour)) || !e.WindowEnd.Equal(base.Add(2*time.Hour)) || len(e.Body) == 0 {
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

func TestClusterID_IsKeptAndReplaced(t *testing.T) {
	dir := t.TempDir()
	if id, err := ClusterID(dir); err != nil || id != "" {
		t.Fatalf("before any save: id = %q, %v; want none", id, err)
	}
	if err := SaveClusterID(dir, "pve-dev"); err != nil {
		t.Fatal(err)
	}
	if id, err := ClusterID(dir); err != nil || id != "pve-dev" {
		t.Fatalf("id = %q, %v; want pve-dev", id, err)
	}
	if err := SaveClusterID(dir, "pve-prod"); err != nil {
		t.Fatal(err)
	}
	if id, err := ClusterID(dir); err != nil || id != "pve-prod" {
		t.Fatalf("id = %q, %v; want the replacement pve-prod", id, err)
	}
	if leftovers, _ := filepath.Glob(filepath.Join(dir, ".cluster-id*")); len(leftovers) != 0 {
		t.Fatalf("saving left temporary files behind: %v", leftovers)
	}
}

func TestClusterID_Edges(t *testing.T) {
	t.Run("blank file is no id", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "cluster-id"), []byte(" \n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if id, err := ClusterID(dir); err != nil || id != "" {
			t.Fatalf("id = %q, %v; want none", id, err)
		}
	})
	t.Run("unreadable path", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "cluster-id"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := ClusterID(dir); err == nil || !strings.Contains(err.Error(), "reading") {
			t.Fatalf("err = %v, want the read failure", err)
		}
	})
	t.Run("unwritable directory", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing")
		if err := SaveClusterID(missing, "pve-dev"); err == nil || !strings.Contains(err.Error(), "writing") {
			t.Fatalf("err = %v, want the write failure", err)
		}
	})
	t.Run("irreplaceable file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "cluster-id", "occupied"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := SaveClusterID(dir, "pve-dev"); err == nil || !strings.Contains(err.Error(), "writing") {
			t.Fatalf("err = %v, want the replace failure", err)
		}
		if leftovers, _ := filepath.Glob(filepath.Join(dir, ".cluster-id*")); len(leftovers) != 0 {
			t.Fatalf("a failed save left temporary files behind: %v", leftovers)
		}
	})
}

func TestBuffer_HoldsOnlyTheWindowsItBuffered(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	b.now = func() time.Time { return base.Add(10 * time.Hour) }
	if err := b.Put(report(2)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		start time.Time
		want  bool
	}{
		{base.Add(2 * time.Hour), true},
		{base.Add(2 * time.Hour).In(time.FixedZone("CEST", 2*60*60)), true},
		{base.Add(time.Hour), false},
		{base.Add(3 * time.Hour), false},
	} {
		if got, err := b.Holds(tc.start); err != nil || got != tc.want {
			t.Errorf("Holds(%s) = %v, %v; want %v", tc.start, got, err, tc.want)
		}
	}
	if err := os.RemoveAll(filepath.Join(dir, bufferDir)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Holds(base.Add(2 * time.Hour)); err == nil {
		t.Fatal("an unreadable buffer must be reported, not read as empty")
	}
}
