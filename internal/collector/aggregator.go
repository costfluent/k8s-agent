package collector

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/metrics"
)

// FirstWindow is how long the window opened at a fresh start runs, so an install shows a report
// within minutes instead of at the next clock hour.
const FirstWindow = 2 * time.Minute

// Observation is one poll: the nodes, the running pods, and their usage.
type Observation struct {
	Time  time.Time
	Nodes []v1.Node
	Pods  []PodInfo
	Usage map[ContainerKey]ContainerUsage
	// Unscraped names the nodes whose kubelet scrape failed: their usage is unknown, so the poll
	// did not observe them or their pods.
	Unscraped map[string]bool
	// Duration is how long the poll took to collect.
	Duration time.Duration
}

type podKey struct{ namespace, name, node string }

type podAccum struct {
	pod        v1.Pod
	containers []v1.Container
}

// container returns the accumulator for the named container, adding it on first sight. Pods have
// few containers, so a scan is cheaper than a map per pod.
func (acc *podAccum) container(name string) *v1.Container {
	for i := range acc.containers {
		if acc.containers[i].Name == name {
			return &acc.containers[i]
		}
	}
	acc.containers = append(acc.containers, v1.Container{Name: name})
	return &acc.containers[len(acc.containers)-1]
}

// Aggregator integrates observations into seconds-weighted sums per window. It is not safe for
// concurrent use.
type Aggregator struct {
	clusterID, version, instanceID string
	interval                       time.Duration

	start, end   time.Time
	lastSample   time.Time
	lastDuration time.Duration
	nodes        map[string]*v1.Node
	pods         map[podKey]*podAccum
}

// NewAggregator opens the short first window at now. interval is the polling interval, used to
// bound the time one observation may stand for.
func NewAggregator(clusterID, version, instanceID string, interval time.Duration, now time.Time) *Aggregator {
	a := &Aggregator{clusterID: clusterID, version: version, instanceID: instanceID, interval: interval}
	now = now.UTC()
	a.open(now, minTime(now.Add(FirstWindow), nextHour(now)))
	return a
}

// Window returns the open window's bounds.
func (a *Aggregator) Window() (time.Time, time.Time) { return a.start, a.end }

func (a *Aggregator) open(start, end time.Time) {
	a.start, a.end = start, end
	a.nodes = map[string]*v1.Node{}
	a.pods = map[podKey]*podAccum{}
}

// Observe adds one poll and returns the windows it closed. The first observation only sets the
// clock: an observation stands for the time since the previous one.
func (a *Aggregator) Observe(obs Observation) []v1.Report {
	now := obs.Time.UTC()
	if a.lastSample.IsZero() {
		a.lastSample, a.lastDuration = maxTime(now, a.start), obs.Duration
		return a.closeUntil(now)
	}
	elapsed := now.Sub(a.lastSample)
	if elapsed <= 0 {
		return nil
	}
	metrics.ElapsedSeconds.Add(elapsed.Seconds())
	// A slow poll delays the next one, and that time was observed. A gap longer than the last poll
	// plus an interval, and than three intervals, is polls that failed or were missed: time the agent
	// did not observe, which counts as one interval.
	if elapsed > max(3*a.interval, a.lastDuration+a.interval) {
		elapsed = a.interval
	}
	metrics.CreditedSeconds.Add(elapsed.Seconds())
	a.lastSample, a.lastDuration = now, obs.Duration

	var closed []v1.Report
	for from := now.Add(-elapsed); from.Before(now); {
		if !from.Before(a.end) {
			closed = append(closed, a.closeUntil(from)...)
		}
		to := minTime(now, a.end)
		a.apply(obs, to.Sub(maxTime(from, a.start)).Seconds())
		from = to
	}
	return append(closed, a.closeUntil(now)...)
}

// closeUntil closes the open window while t is at or past its end.
func (a *Aggregator) closeUntil(t time.Time) []v1.Report {
	var closed []v1.Report
	for !t.Before(a.end) {
		if r, ok := a.report(a.end); ok {
			closed = append(closed, r)
		}
		start := maxTime(a.end, t.Truncate(time.Hour))
		a.open(start, nextHour(start))
	}
	return closed
}

// Finish closes the open window at now, for a shutdown. It returns false when the window saw
// nothing.
func (a *Aggregator) Finish(now time.Time) (v1.Report, bool) {
	end := minTime(now.UTC(), a.end)
	r, ok := a.report(end)
	a.open(end, nextHour(end))
	return r, ok
}

func (a *Aggregator) apply(obs Observation, seconds float64) {
	if seconds <= 0 {
		return
	}
	for _, n := range obs.Nodes {
		if obs.Unscraped[n.Name] {
			continue
		}
		acc, ok := a.nodes[n.Name]
		observed := 0.0
		if ok {
			observed = acc.ObservedSeconds
		}
		node := n
		node.ObservedSeconds = observed + seconds
		a.nodes[n.Name] = &node
	}
	for _, p := range obs.Pods {
		if obs.Unscraped[p.Node] {
			continue
		}
		key := podKey{p.Namespace, p.Name, p.Node}
		acc, ok := a.pods[key]
		if !ok {
			acc = &podAccum{containers: make([]v1.Container, 0, len(p.Specs))}
			a.pods[key] = acc
		}
		acc.pod = p.Pod
		acc.pod.Containers = nil
		for _, spec := range p.Specs {
			c := acc.container(spec.Name)
			c.CPURequestCores = spec.CPURequestCores
			c.MemoryRequestBytes = spec.MemoryRequestBytes
			c.GPURequest = spec.GPURequest

			u := obs.Usage[ContainerKey{p.Namespace, p.Name, spec.Name}]
			cpu := 0.0
			if u.HasCPU {
				cpu = u.CPUCores
			}
			mem := float64(u.MemoryBytes)
			c.ObservedSeconds += seconds
			c.CPUUsageCoreSeconds += cpu * seconds
			c.MemoryUsageByteSeconds += mem * seconds
			c.CPUAllocatedCoreSeconds += max(spec.CPURequestCores, cpu) * seconds
			c.MemoryAllocatedByteSeconds += max(float64(spec.MemoryRequestBytes), mem) * seconds
			c.CPUUsagePeakCores = max(c.CPUUsagePeakCores, cpu)
			c.MemoryUsagePeakBytes = max(c.MemoryUsagePeakBytes, u.MemoryBytes)
		}
	}
}

func (a *Aggregator) report(end time.Time) (v1.Report, bool) {
	r := v1.Report{
		SchemaVersion:   v1.SchemaVersion,
		ClusterID:       a.clusterID,
		AgentVersion:    a.version,
		AgentInstanceID: a.instanceID,
		WindowStart:     a.start,
		WindowEnd:       end,
		Nodes:           make([]v1.Node, 0, len(a.nodes)),
		Pods:            make([]v1.Pod, 0, len(a.pods)),
	}
	for _, n := range a.nodes {
		r.Nodes = append(r.Nodes, *n)
	}
	sort.Slice(r.Nodes, func(i, j int) bool { return r.Nodes[i].Name < r.Nodes[j].Name })
	for _, acc := range a.pods {
		p := acc.pod
		p.Containers = slices.Clone(acc.containers)
		sort.Slice(p.Containers, func(i, j int) bool { return p.Containers[i].Name < p.Containers[j].Name })
		r.Pods = append(r.Pods, p)
	}
	sort.Slice(r.Pods, func(i, j int) bool {
		x, y := r.Pods[i], r.Pods[j]
		if x.Namespace != y.Namespace {
			return x.Namespace < y.Namespace
		}
		if x.Name != y.Name {
			return x.Name < y.Name
		}
		return x.Node < y.Node
	})
	return r, len(r.Nodes) > 0 && end.After(a.start)
}

type snapshot struct {
	LastSample time.Time `json:"last_sample"`
	Report     v1.Report `json:"report"`
}

// SaveSnapshot writes the open window to path atomically, so a crash loses at most the time since.
// The report streams to the file, so the encoded window is never held in memory whole.
func (a *Aggregator) SaveSnapshot(path string) error {
	r, _ := a.report(a.end)
	tmp := path + ".tmp"
	if err := writeSnapshot(tmp, a.lastSample, r); err != nil {
		return fmt.Errorf("writing window snapshot: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replacing window snapshot: %w", err)
	}
	return nil
}

func writeSnapshot(path string, lastSample time.Time, r v1.Report) (err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // path is under the agent's own data directory
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	w := bufio.NewWriterSize(f, 64<<10)
	sample, err := json.Marshal(lastSample)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, `{"last_sample":%s,"report":`, sample); err != nil {
		return err
	}
	if err := v1.WriteJSON(w, r); err != nil {
		return err
	}
	if _, err := w.WriteString("}"); err != nil {
		return err
	}
	return w.Flush()
}

// RestoreSnapshot loads the window saved at path. A window still open at now is resumed; one that
// closed while the agent was down is returned as a report. The file is removed either way.
func (a *Aggregator) RestoreSnapshot(path string, now time.Time) (*v1.Report, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is under the agent's own data directory
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading window snapshot: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return nil, fmt.Errorf("removing window snapshot: %w", err)
	}
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("decoding window snapshot %s: %w", filepath.Base(path), err)
	}
	r := s.Report
	if r.ClusterID != a.clusterID || len(r.Nodes) == 0 {
		return nil, nil
	}
	if !now.UTC().Before(r.WindowEnd) {
		r.AgentVersion = a.version
		r.AgentInstanceID = a.instanceID
		return &r, nil
	}

	a.open(r.WindowStart, r.WindowEnd)
	a.lastSample = time.Time{}
	for i := range r.Nodes {
		n := r.Nodes[i]
		a.nodes[n.Name] = &n
	}
	for _, p := range r.Pods {
		acc := &podAccum{pod: p, containers: p.Containers}
		acc.pod.Containers = nil
		a.pods[podKey{p.Namespace, p.Name, p.Node}] = acc
	}
	return nil, nil
}

func nextHour(t time.Time) time.Time { return t.Truncate(time.Hour).Add(time.Hour) }

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
