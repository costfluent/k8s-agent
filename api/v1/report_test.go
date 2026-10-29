package v1

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata")

func goldenReport() Report {
	start := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	return Report{
		SchemaVersion:   SchemaVersion,
		ClusterID:       "pve-dev",
		AgentVersion:    "0.2.0",
		AgentInstanceID: "6f1c1d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f",
		WindowStart:     start,
		WindowEnd:       start.Add(time.Hour),
		Nodes: []Node{
			{
				Name:                   "aks-system-12345678-vmss000000",
				ProviderID:             "azure:///subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/mc_rg_aks_westeurope/providers/Microsoft.Compute/virtualMachineScaleSets/aks-system-12345678-vmss/virtualMachines/0",
				InstanceType:           "Standard_D4s_v5",
				Region:                 "westeurope",
				Zone:                   "westeurope-1",
				Spot:                   false,
				ObservedSeconds:        3600,
				CapacityCPUCores:       4,
				CapacityMemoryBytes:    17179869184,
				CapacityGPU:            0,
				AllocatableCPUCores:    3.86,
				AllocatableMemoryBytes: 13958643712,
				Labels: map[string]string{
					"kubernetes.io/os":                 "linux",
					"node.kubernetes.io/instance-type": "Standard_D4s_v5",
				},
			},
			{
				Name:                   "talos-worker-1",
				Spot:                   false,
				ObservedSeconds:        1800,
				CapacityCPUCores:       8,
				CapacityMemoryBytes:    34359738368,
				CapacityGPU:            1,
				GPUResources:           map[string]int{ResourceNVIDIAGPU: 1},
				AllocatableCPUCores:    7.9,
				AllocatableMemoryBytes: 33285996544,
				Annotations: map[string]string{
					AnnotationVCPUHourlyRate:  "0.021",
					AnnotationRAMGBHourlyRate: "0.003",
					AnnotationGPUHourlyRate:   "0.9",
				},
			},
		},
		Pods: []Pod{
			{
				Namespace:      "kube-system",
				Name:           "coredns-7db6d8ff4d-abcde",
				Node:           "aks-system-12345678-vmss000000",
				ControllerKind: "Deployment",
				Controller:     "coredns",
				Labels:         map[string]string{"k8s-app": "kube-dns"},
				Containers: []Container{
					{
						Name:                       "coredns",
						ObservedSeconds:            3600,
						CPURequestCores:            0.1,
						MemoryRequestBytes:         73400320,
						CPUUsageCoreSeconds:        14.4,
						MemoryUsageByteSeconds:     90596966400,
						CPUAllocatedCoreSeconds:    360,
						MemoryAllocatedByteSeconds: 264241152000,
						CPUUsagePeakCores:          0.012,
						MemoryUsagePeakBytes:       26214400,
					},
				},
			},
			{
				Namespace:       "ml",
				Name:            "trainer-0",
				Node:            "talos-worker-1",
				ControllerKind:  "StatefulSet",
				Controller:      "trainer",
				Labels:          map[string]string{"app": "trainer", "team": "research"},
				Annotations:     map[string]string{"owner": "research@example.com"},
				NamespaceLabels: map[string]string{"cost-center": "rnd"},
				Claims:          []string{"data-trainer-0"},
				Containers: []Container{
					{
						Name:                       "trainer",
						ObservedSeconds:            1800,
						CPURequestCores:            2,
						MemoryRequestBytes:         4294967296,
						GPURequest:                 1,
						GPURequests:                map[string]int{ResourceNVIDIAGPU: 1},
						CPUUsageCoreSeconds:        4500,
						MemoryUsageByteSeconds:     5153960755200,
						CPUAllocatedCoreSeconds:    4500,
						MemoryAllocatedByteSeconds: 7730941132800,
						CPUUsagePeakCores:          3.1,
						MemoryUsagePeakBytes:       3221225472,
					},
					{
						Name:                       "sidecar",
						ObservedSeconds:            1800,
						CPURequestCores:            0.05,
						MemoryRequestBytes:         33554432,
						CPUUsageCoreSeconds:        9,
						MemoryUsageByteSeconds:     28991029248,
						CPUAllocatedCoreSeconds:    90,
						MemoryAllocatedByteSeconds: 60397977600,
						CPUUsagePeakCores:          0.01,
						MemoryUsagePeakBytes:       20971520,
					},
				},
			},
		},
		Volumes: []Volume{
			{
				Name:            "pvc-cf675ad5-0d5a-4a4c-9a39-3a3b8f6c1d2e",
				CapacityBytes:   107374182400,
				StorageClass:    "managed-csi",
				CSIDriver:       "disk.csi.azure.com",
				VolumeHandle:    "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/mc_rg_aks_westeurope/providers/Microsoft.Compute/disks/pvc-cf675ad5-0d5a-4a4c-9a39-3a3b8f6c1d2e",
				Phase:           "Bound",
				ClaimNamespace:  "ml",
				ClaimName:       "data-trainer-0",
				ObservedSeconds: 3600,
				ClaimLabels:     map[string]string{"app": "trainer"},
			},
			{
				Name:                    "pvc-0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d",
				CapacityBytes:           10737418240,
				StorageClass:            "local-path",
				CSIDriver:               "rancher.io/local-path",
				Phase:                   "Released",
				ObservedSeconds:         3600,
				StorageClassAnnotations: map[string]string{AnnotationStorageGBHourlyRate: "0.0001"},
			},
		},
	}
}

func TestReport_MatchesGoldenFile(t *testing.T) {
	got, err := json.MarshalIndent(goldenReport(), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')

	if *update {
		if err := os.WriteFile("testdata/report.json", got, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	want, err := os.ReadFile("testdata/report.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("report.json drifted from the contract; run go test ./api/v1 -update once the Costfluent API accepts the change\n got: %s", got)
	}
}

func TestReport_GoldenFileRoundTrips(t *testing.T) {
	raw, err := os.ReadFile("testdata/report.json")
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var r Report
	if err := dec.Decode(&r); err != nil {
		t.Fatal(err)
	}
	if r.SchemaVersion != SchemaVersion || len(r.Nodes) != 2 || len(r.Pods) != 2 || len(r.Volumes) != 2 {
		t.Fatalf("unexpected golden report: %+v", r)
	}
}

func TestIsGPUResource_MatchesTheFourFamiliesOnly(t *testing.T) {
	for name, want := range map[string]bool{
		"nvidia.com/gpu":         true,
		"nvidia.com/gpu.shared":  true,
		"amd.com/gpu":            true,
		"nvidia.com/mig-1g.5gb":  true,
		"nvidia.com/mig-3g.40gb": true,
		"nvidia.com/mig-":        false,
		"nvidia.com/gpu.memory":  false,
		"gpu.intel.com/i915":     false,
		"cpu":                    false,
	} {
		if got := IsGPUResource(name); got != want {
			t.Errorf("IsGPUResource(%q) = %v, want %v", name, got, want)
		}
	}
}
