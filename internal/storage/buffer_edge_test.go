package storage

import (
	"compress/gzip"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	v1 "github.com/costfluent/k8s-agent/api/v1"
)

func openAt(t *testing.T, dataDir string, now time.Time) (*Buffer, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	b, err := Open(dataDir, zap.New(core))
	if err != nil {
		t.Fatal(err)
	}
	b.now = func() time.Time { return now }
	return b, logs
}

func skipWithoutPermissions(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("permissions are not enforced for this user")
	}
}

func TestEncode_IsGzipJSONOfTheReport(t *testing.T) {
	body, err := Encode(report(2))
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("body is not gzip: %v", err)
	}
	var got v1.Report
	if err := json.NewDecoder(zr).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ClusterID != "c" || !got.WindowStart.Equal(base.Add(2*time.Hour)) || len(got.Nodes) != 1 {
		t.Fatalf("decoded = %+v", got)
	}
}

func TestPut_RefusesAReportJSONCannotEncode(t *testing.T) {
	b, _ := openAt(t, t.TempDir(), base.Add(10*time.Hour))
	r := report(1)
	r.Nodes[0].CapacityCPUCores = math.NaN()
	if err := b.Put(r); err == nil || !strings.Contains(err.Error(), "encoding report") {
		t.Fatalf("err = %v, want an encoding error", err)
	}
	if n, _ := b.Len(); n != 0 {
		t.Fatalf("len = %d; nothing may be buffered for an unencodable report", n)
	}
}

func TestPut_NamesFilesSoLexicalOrderIsWindowOrder(t *testing.T) {
	dataDir := t.TempDir()
	b, _ := openAt(t, dataDir, base.Add(10*time.Hour))
	if err := b.Put(report(3)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "buffer", "20260923T030000Z_20260923T040000Z.json.gz")); err != nil {
		t.Fatalf("report file not named by its window: %v", err)
	}
	// A report for the same window replaces the earlier one rather than duplicating it.
	if err := b.Put(report(3)); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Len(); n != 1 {
		t.Fatalf("len = %d after re-buffering one window", n)
	}
}

func TestPut_FailsWhenTheBufferDirectoryIsGone(t *testing.T) {
	dataDir := t.TempDir()
	b, _ := openAt(t, dataDir, base.Add(10*time.Hour))
	if err := os.RemoveAll(filepath.Join(dataDir, "buffer")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(report(1)); err == nil || !strings.Contains(err.Error(), "buffering report") {
		t.Fatalf("err = %v", err)
	}
	if _, _, err := b.Oldest(); err == nil || !strings.Contains(err.Error(), "listing buffer") {
		t.Fatalf("Oldest err = %v, want the listing failure", err)
	}
	if _, err := b.Len(); err == nil {
		t.Fatal("Len must report the listing failure")
	}
}

func TestPut_FailsWhenTheTargetNameIsTaken(t *testing.T) {
	dataDir := t.TempDir()
	b, _ := openAt(t, dataDir, base.Add(10*time.Hour))
	blocker := filepath.Join(dataDir, "buffer", "20260923T010000Z_20260923T020000Z.json.gz")
	if err := os.MkdirAll(filepath.Join(blocker, "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(report(1)); err == nil || !strings.Contains(err.Error(), "buffering report") {
		t.Fatalf("err = %v, want the rename failure", err)
	}
}

func TestList_IgnoresFilesThatAreNotReports(t *testing.T) {
	dataDir := t.TempDir()
	b, logs := openAt(t, dataDir, base.Add(10*time.Hour))
	if err := b.Put(report(2)); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(dataDir, "buffer")
	for name, isDir := range map[string]bool{
		"notes.txt": false,
		".20260923T000000Z_20260923T010000Z.json.gz.tmp": false,
		".20260923T000000Z_20260923T010000Z.json.gz":     false,
		"garbage_name.json.gz":                           false,
		"20260923T000000Z_20260923T010000Z.json.gz":      true,
	} {
		var err error
		if isDir {
			err = os.Mkdir(filepath.Join(dir, name), 0o700)
		} else {
			err = os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	e, ok, err := b.Oldest()
	if err != nil || !ok || !e.WindowStart.Equal(base.Add(2*time.Hour)) {
		t.Fatalf("oldest = %+v %v %v, want the only real report", e, ok, err)
	}
	if n, _ := b.Len(); n != 1 {
		t.Fatalf("len = %d", n)
	}
	warned := logs.FilterMessage("ignoring a buffer file with an unexpected name").All()
	if len(warned) == 0 || warned[0].ContextMap()["file"] != "garbage_name.json.gz" {
		t.Fatalf("warnings = %+v, want the unparseable name logged", warned)
	}
}

func TestOldest_EmptyBuffer(t *testing.T) {
	b, _ := openAt(t, t.TempDir(), base)
	e, ok, err := b.Oldest()
	if err != nil || ok || e.Name != "" {
		t.Fatalf("oldest = %+v %v %v, want nothing", e, ok, err)
	}
}

func TestOldest_ReportsAnUnreadableReport(t *testing.T) {
	dataDir := t.TempDir()
	b, _ := openAt(t, dataDir, base.Add(10*time.Hour))
	link := filepath.Join(dataDir, "buffer", "20260923T050000Z_20260923T060000Z.json.gz")
	if err := os.Symlink(filepath.Join(dataDir, "nowhere"), link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, _, err := b.Oldest(); err == nil || !strings.Contains(err.Error(), "reading buffered report") {
		t.Fatalf("err = %v", err)
	}
}

func TestRemove_IgnoresAMissingReportButNotAFailure(t *testing.T) {
	dataDir := t.TempDir()
	b, _ := openAt(t, dataDir, base.Add(10*time.Hour))
	if err := b.Remove("20260923T010000Z_20260923T020000Z.json.gz"); err != nil {
		t.Fatalf("removing a report already gone must succeed: %v", err)
	}
	busy := filepath.Join(dataDir, "buffer", "busy")
	if err := os.MkdirAll(filepath.Join(busy, "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := b.Remove("busy"); err == nil || !strings.Contains(err.Error(), "removing buffered report busy") {
		t.Fatalf("err = %v", err)
	}
}

func TestEnforce_ReportsAReportItCannotDrop(t *testing.T) {
	skipWithoutPermissions(t)
	dataDir := t.TempDir()
	b, _ := openAt(t, dataDir, base.Add(2*time.Hour))
	if err := b.Put(report(1)); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(dataDir, "buffer")
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	b.now = func() time.Time { return base.Add(200 * time.Hour) }
	if err := b.Remove("20260923T090000Z_20260923T100000Z.json.gz"); err == nil || !strings.Contains(err.Error(), "dropping buffered report") {
		t.Fatalf("err = %v, want the failed age drop reported", err)
	}
}

func TestOpen_FailsWhenTheBufferCannotBeListed(t *testing.T) {
	skipWithoutPermissions(t)
	dataDir := t.TempDir()
	dir := filepath.Join(dataDir, "buffer")
	if err := os.Mkdir(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := Open(dataDir, zap.NewNop()); err == nil || !strings.Contains(err.Error(), "listing buffer") {
		t.Fatalf("err = %v, want the listing failure at start", err)
	}
}

func TestOpen_EnforcesTheCapsOnWhatAnEarlierRunLeft(t *testing.T) {
	dataDir := t.TempDir()
	b, _ := openAt(t, dataDir, base.Add(10*time.Hour))
	for _, h := range []int{1, 2} {
		if err := b.Put(report(h)); err != nil {
			t.Fatal(err)
		}
	}
	// The real clock is weeks past these windows, so the restart drops both by age.
	reopened, err := Open(dataDir, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := reopened.Len(); n != 0 {
		t.Fatalf("len = %d, want reports older than %s dropped at start", n, DefaultMaxAge)
	}
}

func TestInstanceID_Edges(t *testing.T) {
	t.Run("blank file is replaced", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "instance-id")
		if err := os.WriteFile(path, []byte(" \n"), 0o600); err != nil {
			t.Fatal(err)
		}
		id, err := InstanceID(dir)
		if err != nil || len(id) != 36 {
			t.Fatalf("id = %q %v", id, err)
		}
		data, _ := os.ReadFile(path)
		if string(data) != id+"\n" {
			t.Fatalf("file = %q, want the new id persisted", data)
		}
	})
	t.Run("existing id is trimmed", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "instance-id"), []byte("  kept-id \n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if id, err := InstanceID(dir); err != nil || id != "kept-id" {
			t.Fatalf("id = %q %v", id, err)
		}
	})
	t.Run("unreadable path", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "instance-id"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := InstanceID(dir); err == nil || !strings.Contains(err.Error(), "reading") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unwritable directory", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing")
		if _, err := InstanceID(missing); err == nil || !strings.Contains(err.Error(), "writing") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("ids are unique", func(t *testing.T) {
		a, _ := InstanceID(t.TempDir())
		b, _ := InstanceID(t.TempDir())
		if a == "" || a == b {
			t.Fatalf("ids %q and %q must differ", a, b)
		}
	})
}
