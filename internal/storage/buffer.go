// Package storage keeps closed reports on disk until the API accepts them, and the agent's
// persistent identity.
package storage

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/metrics"
)

const (
	DefaultMaxAge   = 96 * time.Hour
	DefaultMaxBytes = 50 << 20

	bufferDir      = "buffer"
	suffix         = ".json.gz"
	nameTimeLayout = "20060102T150405Z"
	instanceIDFile = "instance-id"
	clusterIDFile  = "cluster-id"
)

// Entry is one buffered report, already gzip-encoded for the wire.
type Entry struct {
	Name        string
	WindowStart time.Time
	WindowEnd   time.Time
	Body        []byte
}

// Buffer is a directory of gzip reports, named so that lexical order is window order.
type Buffer struct {
	dir      string
	maxAge   time.Duration
	maxBytes int64
	now      func() time.Time
	logger   *zap.Logger
	mu       sync.Mutex
}

// Open prepares dataDir/buffer and fails when the directory cannot be written, so a read-only or
// unmounted volume is an error at start rather than a silent loss an hour later.
func Open(dataDir string, logger *zap.Logger) (*Buffer, error) {
	dir := filepath.Join(dataDir, bufferDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("data directory %s is not writable: %w", dataDir, err)
	}
	probe, err := os.CreateTemp(dir, ".probe-*")
	if err != nil {
		return nil, fmt.Errorf("data directory %s is not writable: %w", dataDir, err)
	}
	_ = probe.Close()
	if err := os.Remove(probe.Name()); err != nil {
		return nil, fmt.Errorf("data directory %s: removing probe: %w", dataDir, err)
	}
	b := &Buffer{dir: dir, maxAge: DefaultMaxAge, maxBytes: DefaultMaxBytes, now: time.Now, logger: logger.Named("buffer")}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.enforceLocked(); err != nil {
		return nil, err
	}
	return b, nil
}

// Put stores a closed report and then drops the oldest reports beyond the caps.
func (b *Buffer) Put(r v1.Report) error {
	body, err := Encode(r)
	if err != nil {
		return err
	}
	name := r.WindowStart.UTC().Format(nameTimeLayout) + "_" + r.WindowEnd.UTC().Format(nameTimeLayout) + suffix
	b.mu.Lock()
	defer b.mu.Unlock()
	tmp := filepath.Join(b.dir, "."+name+".tmp")
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return fmt.Errorf("buffering report %s: %w", name, err)
	}
	if err := os.Rename(tmp, filepath.Join(b.dir, name)); err != nil {
		return fmt.Errorf("buffering report %s: %w", name, err)
	}
	return b.enforceLocked()
}

// Holds reports whether a report for the window starting at start is buffered.
func (b *Buffer) Holds(start time.Time) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	files, err := b.listLocked()
	if err != nil {
		return false, err
	}
	for _, f := range files {
		if f.start.Equal(start) {
			return true, nil
		}
	}
	return false, nil
}

// Encode is the wire body: gzip JSON.
func Encode(r v1.Report) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if err := v1.WriteJSON(zw, r); err != nil {
		return nil, fmt.Errorf("encoding report: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("compressing report: %w", err)
	}
	return buf.Bytes(), nil
}

// Oldest returns the earliest buffered report.
func (b *Buffer) Oldest() (Entry, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	files, err := b.listLocked()
	if err != nil || len(files) == 0 {
		return Entry{}, false, err
	}
	f := files[0]
	body, err := os.ReadFile(filepath.Join(b.dir, f.name))
	if err != nil {
		return Entry{}, false, fmt.Errorf("reading buffered report %s: %w", f.name, err)
	}
	return Entry{Name: f.name, WindowStart: f.start, WindowEnd: f.end, Body: body}, true, nil
}

// Remove deletes a sent or refused report.
func (b *Buffer) Remove(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := os.Remove(filepath.Join(b.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing buffered report %s: %w", name, err)
	}
	return b.enforceLocked()
}

// Len returns the number of buffered reports.
func (b *Buffer) Len() (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	files, err := b.listLocked()
	return len(files), err
}

type file struct {
	name  string
	start time.Time
	end   time.Time
	size  int64
}

func parseName(name string) (time.Time, time.Time, error) {
	startText, endText, _ := strings.Cut(strings.TrimSuffix(name, suffix), "_")
	start, err := time.Parse(nameTimeLayout, startText)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	end, err := time.Parse(nameTimeLayout, endText)
	return start, end, err
}

func (b *Buffer) listLocked() ([]file, error) {
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return nil, fmt.Errorf("listing buffer: %w", err)
	}
	var files []file
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, suffix) || strings.HasPrefix(name, ".") {
			continue
		}
		start, end, err := parseName(name)
		if err != nil {
			b.logger.Warn("ignoring a buffer file with an unexpected name", zap.String("file", name))
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, fmt.Errorf("reading buffer entry %s: %w", name, err)
		}
		files = append(files, file{name: name, start: start, end: end, size: info.Size()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })
	return files, nil
}

func (b *Buffer) enforceLocked() error {
	files, err := b.listLocked()
	if err != nil {
		return err
	}
	var total int64
	for _, f := range files {
		total += f.size
	}
	cutoff := b.now().Add(-b.maxAge)
	for len(files) > 0 {
		f := files[0]
		reason := ""
		switch {
		case f.start.Before(cutoff):
			reason = "age"
		case total > b.maxBytes:
			reason = "size"
		}
		if reason == "" {
			break
		}
		if err := os.Remove(filepath.Join(b.dir, f.name)); err != nil {
			return fmt.Errorf("dropping buffered report %s: %w", f.name, err)
		}
		metrics.BufferDropped.WithLabelValues(reason).Inc()
		b.logger.Warn("dropped the oldest buffered report", zap.String("file", f.name), zap.String("reason", reason))
		total -= f.size
		files = files[1:]
	}
	metrics.BufferReports.Set(float64(len(files)))
	metrics.BufferBytes.Set(float64(total))
	return nil
}

// InstanceID returns the UUID in dataDir/instance-id, creating it on first use. It names this
// agent installation to the API, which refuses a second instance reporting the same cluster.
func InstanceID(dataDir string) (string, error) {
	path := filepath.Join(dataDir, instanceIDFile)
	data, err := os.ReadFile(path) //nolint:gosec // path is under the agent's own data directory
	if err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("reading %s: %w", path, err)
	}
	id, err := newUUID()
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("writing %s: %w", path, err)
	}
	return id, nil
}

// ClusterID returns the cluster id SaveClusterID kept in dataDir/cluster-id, or "" when none is
// kept.
func ClusterID(dataDir string) (string, error) {
	path := filepath.Join(dataDir, clusterIDFile)
	data, err := os.ReadFile(path) //nolint:gosec // path is under the agent's own data directory
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", path, err)
	}
	return strings.TrimSpace(string(data)), nil
}

// SaveClusterID keeps the cluster the API token names, so a restart while the API is unreachable
// reports for it. It replaces the file whole, so a crash never leaves a truncated id behind; the
// one agent a volume belongs to is the only writer of its temporary file.
func SaveClusterID(dataDir, id string) error {
	path := filepath.Join(dataDir, clusterIDFile)
	tmp := filepath.Join(dataDir, "."+clusterIDFile+".tmp")
	if err := os.WriteFile(tmp, []byte(id+"\n"), 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

func newUUID() (string, error) {
	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		return "", fmt.Errorf("generating instance id: %w", err)
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16]), nil
}
