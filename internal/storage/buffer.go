package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/metrics"
)

const (
	maxBufferSize    = 50 * 1024 * 1024 // 50MB max buffer
	maxBufferAge     = 24 * time.Hour
	stateFileName    = "state.json"
	reportsDir       = "pending"
)

// Buffer provides persistent storage for pending reports
type Buffer struct {
	mu      sync.RWMutex
	dataDir string
	logger  *zap.Logger
}

// State tracks agent state for recovery
type State struct {
	LastReportTime    time.Time `json:"last_report_time"`
	LastHeartbeatTime time.Time `json:"last_heartbeat_time"`
	PendingReportIDs  []string  `json:"pending_report_ids"`
}

// NewBuffer creates a new persistent buffer
func NewBuffer(dataDir string, logger *zap.Logger) (*Buffer, error) {
	reportsPath := filepath.Join(dataDir, reportsDir)
	if err := os.MkdirAll(reportsPath, 0755); err != nil {
		return nil, fmt.Errorf("creating buffer directory: %w", err)
	}

	return &Buffer{
		dataDir: dataDir,
		logger:  logger.Named("buffer"),
	}, nil
}

// SaveReport persists a report to disk
func (b *Buffer) SaveReport(report *v1.MetricsReport) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Generate unique ID based on timestamp
	reportID := fmt.Sprintf("report_%d", report.ReportEnd.UnixNano())
	filename := filepath.Join(b.dataDir, reportsDir, reportID+".json")

	data, err := json.Marshal(report)
	if err != nil {
		return "", fmt.Errorf("marshaling report: %w", err)
	}

	// Check buffer size limits
	if err := b.enforceBufferLimits(int64(len(data))); err != nil {
		b.logger.Warn("buffer limit exceeded, dropping oldest reports", zap.Error(err))
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return "", fmt.Errorf("writing report: %w", err)
	}

	b.logger.Debug("saved report to buffer",
		zap.String("id", reportID),
		zap.Int("bytes", len(data)))

	return reportID, nil
}

// LoadReport reads a report from disk
func (b *Buffer) LoadReport(reportID string) (*v1.MetricsReport, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	filename := filepath.Join(b.dataDir, reportsDir, reportID+".json")

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("reading report: %w", err)
	}

	var report v1.MetricsReport
	if err := json.Unmarshal(data, &report); err != nil {
		return nil, fmt.Errorf("unmarshaling report: %w", err)
	}

	return &report, nil
}

// DeleteReport removes a report from disk
func (b *Buffer) DeleteReport(reportID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	filename := filepath.Join(b.dataDir, reportsDir, reportID+".json")

	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting report: %w", err)
	}

	b.logger.Debug("deleted report from buffer", zap.String("id", reportID))
	return nil
}

// ListPendingReports returns IDs of all buffered reports
func (b *Buffer) ListPendingReports() ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	reportsPath := filepath.Join(b.dataDir, reportsDir)

	entries, err := os.ReadDir(reportsPath)
	if err != nil {
		return nil, fmt.Errorf("reading reports directory: %w", err)
	}

	var ids []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) == ".json" {
			ids = append(ids, name[:len(name)-5]) // Remove .json extension
		}
	}

	// Sort by timestamp (oldest first)
	sort.Strings(ids)

	return ids, nil
}

// SaveState persists agent state
func (b *Buffer) SaveState(state *State) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	filename := filepath.Join(b.dataDir, stateFileName)

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling state: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("writing state: %w", err)
	}

	return nil
}

// LoadState reads persisted agent state
func (b *Buffer) LoadState() (*State, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	filename := filepath.Join(b.dataDir, stateFileName)

	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return &State{}, nil
		}
		return nil, fmt.Errorf("reading state: %w", err)
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshaling state: %w", err)
	}

	return &state, nil
}

// enforceBufferLimits removes old reports if buffer is too large
func (b *Buffer) enforceBufferLimits(newReportSize int64) error {
	reportsPath := filepath.Join(b.dataDir, reportsDir)

	entries, err := os.ReadDir(reportsPath)
	if err != nil {
		return err
	}

	// Calculate total size
	var totalSize int64
	var files []struct {
		name    string
		size    int64
		modTime time.Time
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		totalSize += info.Size()
		files = append(files, struct {
			name    string
			size    int64
			modTime time.Time
		}{
			name:    entry.Name(),
			size:    info.Size(),
			modTime: info.ModTime(),
		})
	}

	// If adding new report would exceed limit, remove oldest
	if totalSize+newReportSize > maxBufferSize {
		// Sort by modification time (oldest first)
		sort.Slice(files, func(i, j int) bool {
			return files[i].modTime.Before(files[j].modTime)
		})

		// Remove files until we have space
		for _, f := range files {
			if totalSize+newReportSize <= maxBufferSize {
				break
			}

			filename := filepath.Join(reportsPath, f.name)
			if err := os.Remove(filename); err != nil {
				continue
			}

			totalSize -= f.size
			b.logger.Warn("removed old report due to buffer limit",
				zap.String("file", f.name),
				zap.Int64("freed_bytes", f.size))
		}
	}

	// Also remove reports older than maxBufferAge
	cutoff := time.Now().Add(-maxBufferAge)
	for _, f := range files {
		if f.modTime.Before(cutoff) {
			filename := filepath.Join(reportsPath, f.name)
			if err := os.Remove(filename); err != nil {
				continue
			}
			b.logger.Warn("removed expired report",
				zap.String("file", f.name),
				zap.Time("modTime", f.modTime))
		}
	}

	return nil
}

// BufferStats returns buffer statistics
func (b *Buffer) BufferStats() (count int, totalBytes int64, oldestTime time.Time) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	reportsPath := filepath.Join(b.dataDir, reportsDir)

	entries, err := os.ReadDir(reportsPath)
	if err != nil {
		return 0, 0, time.Time{}
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}

		count++
		totalBytes += info.Size()

		if oldestTime.IsZero() || info.ModTime().Before(oldestTime) {
			oldestTime = info.ModTime()
		}
	}

	return count, totalBytes, oldestTime
}

// UpdateMetrics updates prometheus metrics for buffer state
func (b *Buffer) UpdateMetrics() {
	count, totalBytes, _ := b.BufferStats()
	metrics.UpdateBuffer(count, totalBytes)
}
