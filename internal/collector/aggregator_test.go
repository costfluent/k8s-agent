package collector

import (
	"math"
	"path/filepath"
	"testing"
	"time"

	v1 "github.com/costfluent/k8s-agent/api/v1"
)

var t0 = time.Date(2026, 9, 23, 10, 17, 0, 0, time.UTC)

func obsAt(at time.Time, nodes []string, pods []PodInfo, usage map[ContainerKey]ContainerUsage) Observation {
	o := Observation{Time: at, Pods: pods, Usage: usage}
	for _, n := range nodes {
		o.Nodes = append(o.Nodes, v1.Node{Name: n, CapacityCPUCores: 4})
	}
	return o
}

func webPod(node string) PodInfo {
	return PodInfo{
		Pod:   v1.Pod{Namespace: "shop", Name: "web-0", Node: node, ControllerKind: "StatefulSet", Controller: "web"},
		Specs: []ContainerSpec{{Name: "app", CPURequestCores: 0.5, MemoryRequestBytes: 100}, {Name: "sidecar", CPURequestCores: 0.1, MemoryRequestBytes: 10}},
	}
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func findPod(r v1.Report, name, node string) *v1.Pod {
	for i := range r.Pods {
		if r.Pods[i].Name == name && r.Pods[i].Node == node {
			return &r.Pods[i]
		}
	}
	return nil
}

func TestAggregator_AllocatesMaxOfRequestAndUsage(t *testing.T) {
	a := NewAggregator("c", "0.2.0", "id", 10*time.Second, t0)
	start, end := a.Window()
	if !start.Equal(t0) || !end.Equal(t0.Add(FirstWindow)) {
		t.Fatalf("first window = %s..%s", start, end)
	}
	usage := map[ContainerKey]ContainerUsage{
		{"shop", "web-0", "app"}:     {CPUCores: 0.8, HasCPU: true, MemoryBytes: 50},
		{"shop", "web-0", "sidecar"}: {CPUCores: 0.05, HasCPU: true, MemoryBytes: 40},
	}
	pods := []PodInfo{webPod("n1")}
	if closed := a.Observe(obsAt(t0, []string{"n1"}, pods, usage)); len(closed) != 0 {
		t.Fatalf("the first observation closed %d windows", len(closed))
	}
	for i := 1; i <= 12; i++ {
		closed := a.Observe(obsAt(t0.Add(time.Duration(i)*10*time.Second), []string{"n1"}, pods, usage))
		if i < 12 && len(closed) != 0 {
			t.Fatalf("tick %d closed a window early", i)
		}
		if i == 12 {
			if len(closed) != 1 {
				t.Fatalf("the two-minute window did not close: %d", len(closed))
			}
			r := closed[0]
			if !r.WindowStart.Equal(t0) || !r.WindowEnd.Equal(t0.Add(2*time.Minute)) || r.SchemaVersion != 1 || r.AgentInstanceID != "id" {
				t.Fatalf("report header = %+v", r)
			}
			p := findPod(r, "web-0", "n1")
			if p == nil || len(p.Containers) != 2 {
				t.Fatalf("pod = %+v", p)
			}
			app, side := p.Containers[0], p.Containers[1]
			if !near(app.ObservedSeconds, 120) || !near(app.CPUUsageCoreSeconds, 96) || !near(app.CPUAllocatedCoreSeconds, 96) {
				t.Errorf("app cpu = %+v", app)
			}
			if !near(app.MemoryAllocatedByteSeconds, 100*120) || !near(app.MemoryUsageByteSeconds, 50*120) || app.MemoryUsagePeakBytes != 50 {
				t.Errorf("app memory = %+v", app)
			}
			if !near(side.CPUAllocatedCoreSeconds, 0.1*120) || !near(side.MemoryAllocatedByteSeconds, 40*120) || !near(side.CPUUsagePeakCores, 0.05) {
				t.Errorf("sidecar = %+v", side)
			}
			if !near(r.Nodes[0].ObservedSeconds, 120) {
				t.Errorf("node observed = %v", r.Nodes[0].ObservedSeconds)
			}
		}
	}
	start, end = a.Window()
	if !start.Equal(t0.Add(2*time.Minute)) || !end.Equal(time.Date(2026, 9, 23, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("second window = %s..%s", start, end)
	}
}

func TestAggregator_ChurnAndNodeJoinLeave(t *testing.T) {
	a := NewAggregator("c", "v", "id", 60*time.Second, t0)
	a.Observe(obsAt(t0, []string{"n1"}, []PodInfo{webPod("n1")}, nil))
	a.Observe(obsAt(t0.Add(60*time.Second), []string{"n1", "n2"}, []PodInfo{webPod("n1")}, nil))
	closed := a.Observe(obsAt(t0.Add(120*time.Second), []string{"n2"}, []PodInfo{webPod("n2")}, nil))
	if len(closed) != 1 {
		t.Fatalf("closed = %d", len(closed))
	}
	r := closed[0]
	if len(r.Nodes) != 2 || !near(r.Nodes[0].ObservedSeconds, 60) || !near(r.Nodes[1].ObservedSeconds, 120) {
		t.Errorf("nodes = %+v", r.Nodes)
	}
	on1, on2 := findPod(r, "web-0", "n1"), findPod(r, "web-0", "n2")
	if on1 == nil || on2 == nil || !near(on1.Containers[0].ObservedSeconds, 60) || !near(on2.Containers[0].ObservedSeconds, 60) {
		t.Fatalf("a pod that moved node must appear once per node: %+v %+v", on1, on2)
	}
	if !near(on1.Containers[0].CPUAllocatedCoreSeconds, 30) || on1.Containers[0].CPUUsageCoreSeconds != 0 {
		t.Errorf("without usage the request is allocated: %+v", on1.Containers[0])
	}
}

func TestAggregator_SplitsAnObservationAtTheHourAndNeverCrossesMidnight(t *testing.T) {
	late := time.Date(2026, 9, 23, 23, 59, 0, 0, time.UTC)
	a := NewAggregator("c", "v", "id", 60*time.Second, late)
	if _, end := a.Window(); !end.Equal(time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("the first window crosses midnight: ends %s", end)
	}
	a.Observe(obsAt(late.Add(30*time.Second), []string{"n1"}, nil, nil))
	closed := a.Observe(obsAt(late.Add(90*time.Second), []string{"n1"}, nil, nil))
	if len(closed) != 1 || !near(closed[0].Nodes[0].ObservedSeconds, 30) {
		t.Fatalf("closed = %+v", closed)
	}
	r, ok := a.Finish(late.Add(90 * time.Second))
	if !ok || !r.WindowStart.Equal(time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)) || !near(r.Nodes[0].ObservedSeconds, 30) {
		t.Fatalf("after midnight = %+v %v", r, ok)
	}
}

func TestAggregator_GapCountsAsOneInterval(t *testing.T) {
	a := NewAggregator("c", "v", "id", 60*time.Second, t0.Truncate(time.Hour).Add(time.Minute))
	a.Observe(obsAt(t0, []string{"n1"}, nil, nil))
	a.Observe(obsAt(t0.Add(20*time.Minute), []string{"n1"}, nil, nil))
	r, ok := a.Finish(t0.Add(20 * time.Minute))
	if !ok || !near(r.Nodes[0].ObservedSeconds, 60) {
		t.Fatalf("a 20 minute gap = %+v", r.Nodes)
	}
}

func TestAggregator_SnapshotRestoresAnOpenWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "window.json")
	a := NewAggregator("c", "v", "id", 60*time.Second, t0)
	a.Observe(obsAt(t0, []string{"n1"}, []PodInfo{webPod("n1")}, nil))
	a.Observe(obsAt(t0.Add(time.Minute), []string{"n1"}, []PodInfo{webPod("n1")}, nil))
	if err := a.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}

	b := NewAggregator("c", "v", "id", 60*time.Second, t0.Add(90*time.Second))
	closed, err := b.RestoreSnapshot(path, t0.Add(90*time.Second))
	if err != nil || closed != nil {
		t.Fatalf("restore = %v %v", closed, err)
	}
	start, end := b.Window()
	if !start.Equal(t0) || !end.Equal(t0.Add(FirstWindow)) {
		t.Fatalf("restored window = %s..%s", start, end)
	}
	b.Observe(obsAt(t0.Add(100*time.Second), []string{"n1"}, []PodInfo{webPod("n1")}, nil))
	closed2 := b.Observe(obsAt(t0.Add(120*time.Second), []string{"n1"}, []PodInfo{webPod("n1")}, nil))
	if len(closed2) != 1 || !near(closed2[0].Nodes[0].ObservedSeconds, 80) {
		t.Fatalf("restored sums = %+v", closed2)
	}
	if c := findPod(closed2[0], "web-0", "n1"); c == nil || !near(c.Containers[0].ObservedSeconds, 80) {
		t.Fatalf("restored pod = %+v", c)
	}
}

func TestAggregator_SnapshotOfAClosedWindowBecomesAReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "window.json")
	a := NewAggregator("c", "v", "id", 60*time.Second, t0)
	a.Observe(obsAt(t0, []string{"n1"}, nil, nil))
	a.Observe(obsAt(t0.Add(time.Minute), []string{"n1"}, nil, nil))
	if err := a.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}
	later := t0.Add(3 * time.Hour)
	b := NewAggregator("c", "v2", "id", 60*time.Second, later)
	closed, err := b.RestoreSnapshot(path, later)
	if err != nil || closed == nil || !closed.WindowEnd.Equal(t0.Add(FirstWindow)) || closed.AgentVersion != "v2" {
		t.Fatalf("closed = %+v %v", closed, err)
	}
	if start, _ := b.Window(); !start.Equal(later) {
		t.Fatalf("a closed snapshot must not replace the fresh window: %s", start)
	}
}
