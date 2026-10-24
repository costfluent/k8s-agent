package collector

import (
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/metrics"
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

// credited runs two observations elapsed apart, the first taking took to collect, and returns the
// seconds the window was credited and the change in the elapsed and credited counters.
func credited(t *testing.T, interval, took, elapsed time.Duration) (float64, float64, float64) {
	t.Helper()
	start := t0.Truncate(time.Hour).Add(5 * time.Minute)
	a := NewAggregator("c", "v", "id", interval, start)
	elapsedBefore, creditedBefore := testutil.ToFloat64(metrics.ElapsedSeconds), testutil.ToFloat64(metrics.CreditedSeconds)
	first := obsAt(start, []string{"n1"}, nil, nil)
	first.Duration = took
	a.Observe(first)
	a.Observe(obsAt(start.Add(elapsed), []string{"n1"}, nil, nil))
	r, ok := a.Finish(start.Add(elapsed))
	if !ok {
		t.Fatal("the window saw nothing")
	}
	return r.Nodes[0].ObservedSeconds, testutil.ToFloat64(metrics.ElapsedSeconds) - elapsedBefore, testutil.ToFloat64(metrics.CreditedSeconds) - creditedBefore
}

func TestAggregator_MissedPollsCountAsOneInterval(t *testing.T) {
	got, elapsed, creditedSeconds := credited(t, time.Minute, time.Second, 20*time.Minute)
	if !near(got, 60) || !near(elapsed, 1200) || !near(creditedSeconds, 60) {
		t.Fatalf("a 20 minute gap after a 1 s poll credited %v (counters: elapsed %v, credited %v), want one interval", got, elapsed, creditedSeconds)
	}
}

func TestAggregator_SlowPollIsCreditedInFull(t *testing.T) {
	for _, tc := range []struct {
		name          string
		took, elapsed time.Duration
		want          float64
	}{
		{"a 75 s poll at a 5 s interval", 75 * time.Second, 80 * time.Second, 80},
		{"right at the poll plus one interval", 30 * time.Second, 35 * time.Second, 35},
		{"within three intervals of a quick poll", time.Second, 15 * time.Second, 15},
		{"past the slow poll plus one interval", 75 * time.Second, 81 * time.Second, 5},
		{"a slow poll then missed ones", 75 * time.Second, 10 * time.Minute, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, elapsed, creditedSeconds := credited(t, 5*time.Second, tc.took, tc.elapsed)
			if !near(got, tc.want) || !near(elapsed, tc.elapsed.Seconds()) || !near(creditedSeconds, tc.want) {
				t.Fatalf("credited %v of %v (counters: elapsed %v, credited %v), want %v", got, tc.elapsed, elapsed, creditedSeconds, tc.want)
			}
		})
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

func apiPod(node string) PodInfo {
	return PodInfo{
		Pod:   v1.Pod{Namespace: "shop", Name: "api-0", Node: node},
		Specs: []ContainerSpec{{Name: "app", CPURequestCores: 0.5, MemoryRequestBytes: 100}},
	}
}

func TestAggregator_LeavesAnUnscrapedNodeAndItsPodsOutOfThatInterval(t *testing.T) {
	a := NewAggregator("c", "v", "id", 10*time.Second, t0)
	pods := []PodInfo{webPod("n1"), apiPod("n2")}
	both := map[ContainerKey]ContainerUsage{
		{"shop", "web-0", "app"}: {CPUCores: 0.8, HasCPU: true, MemoryBytes: 50},
		{"shop", "api-0", "app"}: {CPUCores: 0.8, HasCPU: true, MemoryBytes: 50},
	}
	onlyN1 := map[ContainerKey]ContainerUsage{{"shop", "web-0", "app"}: both[ContainerKey{"shop", "web-0", "app"}]}
	for i, failed := range []bool{false, false, true, true, false, false} {
		o := obsAt(t0.Add(time.Duration(i)*10*time.Second), []string{"n1", "n2"}, pods, both)
		if failed {
			o.Usage, o.Unscraped = onlyN1, map[string]bool{"n2": true}
		}
		a.Observe(o)
	}
	r, ok := a.Finish(t0.Add(50 * time.Second))
	if !ok || len(r.Nodes) != 2 {
		t.Fatalf("report = %+v %v", r, ok)
	}
	if !near(r.Nodes[0].ObservedSeconds, 50) || !near(r.Nodes[1].ObservedSeconds, 30) {
		t.Errorf("node seconds = %v, %v; want 50 for the scraped node and 30 for the one that failed two polls", r.Nodes[0].ObservedSeconds, r.Nodes[1].ObservedSeconds)
	}
	api := findPod(r, "api-0", "n2")
	if api == nil {
		t.Fatal("the unscraped node's pod is missing")
	}
	if c := api.Containers[0]; !near(c.ObservedSeconds, 30) || !near(c.CPUUsageCoreSeconds, 0.8*30) || !near(c.CPUAllocatedCoreSeconds, 0.8*30) || !near(c.MemoryUsageByteSeconds, 50*30) || !near(c.MemoryAllocatedByteSeconds, 100*30) {
		t.Errorf("unscraped node's container = %+v, want only the scraped 30 s, at its measured usage", c)
	}
	web := findPod(r, "web-0", "n1")
	if web == nil {
		t.Fatal("the scraped node's pod is missing")
	}
	if app := web.Containers[0]; !near(app.ObservedSeconds, 50) || !near(app.CPUUsageCoreSeconds, 0.8*50) || !near(app.CPUAllocatedCoreSeconds, 0.8*50) {
		t.Errorf("scraped node's container = %+v, want every second at its measured usage", app)
	}
	if side := web.Containers[1]; !near(side.ObservedSeconds, 50) || side.CPUUsageCoreSeconds != 0 || !near(side.CPUAllocatedCoreSeconds, 0.1*50) {
		t.Errorf("a container missing from a successful scrape = %+v, want its request for every second", side)
	}
}

func TestAggregator_WhenEveryScrapeFailsNothingIsObserved(t *testing.T) {
	all := map[string]bool{"n1": true, "n2": true}
	usage := map[ContainerKey]ContainerUsage{
		{"shop", "web-0", "app"}: {CPUCores: 0.8, HasCPU: true, MemoryBytes: 50},
		{"shop", "api-0", "app"}: {CPUCores: 0.8, HasCPU: true, MemoryBytes: 50},
	}
	poll := func(at time.Duration, failed bool) Observation {
		o := obsAt(t0.Add(at), []string{"n1", "n2"}, []PodInfo{webPod("n1"), apiPod("n2")}, usage)
		if failed {
			o.Usage, o.Unscraped = nil, all
		}
		return o
	}

	a := NewAggregator("c", "v", "id", 10*time.Second, t0)
	for i := range 4 {
		a.Observe(poll(time.Duration(i)*10*time.Second, i > 0))
	}
	if r, ok := a.Finish(t0.Add(30 * time.Second)); ok {
		t.Errorf("a window in which every scrape failed was reported: %+v", r)
	}

	b := NewAggregator("c", "v", "id", 10*time.Second, t0)
	for i := range 5 {
		b.Observe(poll(time.Duration(i)*10*time.Second, i == 1 || i == 2))
	}
	r, ok := b.Finish(t0.Add(40 * time.Second))
	if !ok || len(r.Nodes) != 2 || !near(r.Nodes[0].ObservedSeconds, 20) || !near(r.Nodes[1].ObservedSeconds, 20) {
		t.Fatalf("after recovery nodes = %+v %v, want the 20 scraped seconds each", r.Nodes, ok)
	}
	for _, p := range r.Pods {
		if c := p.Containers[0]; !near(c.ObservedSeconds, 20) || !near(c.CPUUsageCoreSeconds, 0.8*20) {
			t.Errorf("%s/%s = %+v, want the 20 scraped seconds at measured usage", p.Name, c.Name, c)
		}
	}
}
