package v1

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

const atLimitsGolden = "testdata/report-at-limits.json.gz"

// fullMap holds MaxMapEntries entries, each with a key and a value at the API's length caps. A
// key has a label key's longest shape: a 253-character DNS prefix, a slash and a 63-character name.
func fullMap() map[string]string {
	prefix := strings.Repeat(strings.Repeat("a", 61)+".", 4) + "e.com"
	m := make(map[string]string, MaxMapEntries)
	for i := range MaxMapEntries {
		m[fmt.Sprintf("%s/k%062d", prefix, i)] = fmt.Sprintf("%0*d", MaxMapValueLength, i)
	}
	return m
}

// atLimitsReport is a report exactly at every cap: MaxNodes nodes, MaxPods pods, a pod with
// MaxContainersPerPod containers, and full label and annotation maps. Names are as long as
// Kubernetes allows.
func atLimitsReport() Report {
	start := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	full := fullMap()
	r := Report{
		SchemaVersion:   SchemaVersion,
		ClusterID:       "pve-dev",
		AgentVersion:    "0.2.0",
		AgentInstanceID: "6f1c1d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f",
		WindowStart:     start,
		WindowEnd:       start.Add(time.Hour),
		Nodes:           make([]Node, 0, MaxNodes),
		Pods:            make([]Pod, 0, MaxPods),
	}
	for i := range MaxNodes {
		r.Nodes = append(r.Nodes, Node{
			Name:                   fmt.Sprintf("node-%04d", i),
			InstanceType:           "Standard_D4s_v5",
			Region:                 "westeurope",
			Zone:                   "westeurope-1",
			ObservedSeconds:        3600,
			CapacityCPUCores:       4,
			CapacityMemoryBytes:    17179869184,
			AllocatableCPUCores:    3.86,
			AllocatableMemoryBytes: 13958643712,
		})
	}
	first := &r.Nodes[0]
	first.Name = fmt.Sprintf("node-%0248d", 0)
	first.ProviderID = "azure:///" + strings.Repeat("x", 1024-len("azure:///"))
	first.InstanceType = strings.Repeat("i", 63)
	first.Region = strings.Repeat("r", 63)
	first.Zone = strings.Repeat("z", 63)
	first.Labels = full
	first.Annotations = full

	for i := range MaxPods {
		r.Pods = append(r.Pods, Pod{
			Namespace:      fmt.Sprintf("team-%02d", i%50),
			Name:           fmt.Sprintf("web-%05d", i),
			Node:           r.Nodes[i%MaxNodes].Name,
			ControllerKind: "Deployment",
			Controller:     "web",
			Containers: []Container{{
				Name:                       "web",
				ObservedSeconds:            3600,
				CPURequestCores:            0.1,
				MemoryRequestBytes:         134217728,
				CPUUsageCoreSeconds:        36,
				MemoryUsageByteSeconds:     241591910400,
				CPUAllocatedCoreSeconds:    360,
				MemoryAllocatedByteSeconds: 483183820800,
				CPUUsagePeakCores:          0.02,
				MemoryUsagePeakBytes:       100663296,
			}},
		})
	}
	pod := &r.Pods[0]
	pod.Namespace = fmt.Sprintf("ns-%060d", 0)
	pod.Name = fmt.Sprintf("web-%0249d", 0)
	pod.ControllerKind = "StatefulSet"
	pod.Controller = fmt.Sprintf("web-%0249d", 0)
	pod.Labels = full
	pod.Annotations = full
	pod.NamespaceLabels = full
	pod.Containers = make([]Container, 0, MaxContainersPerPod)
	for i := range MaxContainersPerPod {
		pod.Containers = append(pod.Containers, Container{Name: fmt.Sprintf("c-%061d", i), ObservedSeconds: 3600})
	}
	return r
}

func TestReport_AtLimitsMatchesGoldenFile(t *testing.T) {
	got, err := json.Marshal(atLimitsReport())
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')

	if *update {
		var buf bytes.Buffer
		zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := zw.Write(got); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(atLimitsGolden, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	f, err := os.Open(atLimitsGolden)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	want, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s drifted from the caps; run go test ./api/v1 -update once the Costfluent API accepts the change", atLimitsGolden)
	}
}

func TestReport_CheckSizeAcceptsTheCapsAndRefusesOneMore(t *testing.T) {
	if err := atLimitsReport().CheckSize(); err != nil {
		t.Fatalf("CheckSize at the caps = %v, want nil", err)
	}
	for _, tc := range []struct {
		name string
		grow func(*Report)
		want string
	}{
		{"one node more", func(r *Report) { r.Nodes = append(r.Nodes, Node{Name: "extra"}) }, "1001 nodes; the API accepts at most 1000"},
		{"one pod more", func(r *Report) { r.Pods = append(r.Pods, Pod{Name: "extra"}) }, "50001 pods; the API accepts at most 50000"},
		{"one container more", func(r *Report) {
			p := &r.Pods[len(r.Pods)-1]
			p.Containers = append(p.Containers, make([]Container, MaxContainersPerPod)...)
		}, "pod " + fmt.Sprintf("team-%02d/web-%05d", (MaxPods-1)%50, MaxPods-1) + " has 101 containers; the API accepts at most 100"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := atLimitsReport()
			tc.grow(&r)
			if err := r.CheckSize(); err == nil || err.Error() != tc.want {
				t.Fatalf("CheckSize = %v, want %q", err, tc.want)
			}
		})
	}
}
