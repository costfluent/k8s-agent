package collector

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/config"
	"github.com/costfluent/k8s-agent/internal/metrics"
)

// persistentVolume is a volume of class in phase, its claim referenced as a volume keeps it after
// the claim is gone, so a Released volume still names one.
func persistentVolume(name, class string, phase corev1.PersistentVolumePhase, claim string, source corev1.PersistentVolumeSource) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{"pv.kubernetes.io/provisioned-by": "x"},
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kube-controller-manager"}}, Finalizers: []string{"kubernetes.io/pv-protection"}},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			PersistentVolumeSource:        source,
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              class,
		},
		Status: corev1.PersistentVolumeStatus{Phase: phase},
	}
	if ns, name, ok := strings.Cut(claim, "/"); ok {
		pv.Spec.ClaimRef = &corev1.ObjectReference{Kind: "PersistentVolumeClaim", Namespace: ns, Name: name, UID: "claim-uid"}
	}
	return pv
}

func csi(driver, handle string) corev1.PersistentVolumeSource {
	return corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: driver, VolumeHandle: handle, FSType: "ext4",
		VolumeAttributes: map[string]string{"storage.kubernetes.io/csiProvisionerIdentity": "1-2-3"}}}
}

func betaClass(pv *corev1.PersistentVolume, class string) *corev1.PersistentVolume {
	pv.Annotations[corev1.BetaStorageClassAnnotation] = class
	return pv
}

func storageClass(name string, annotations map[string]string) *storagev1.StorageClass {
	return &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations}, Provisioner: "example.com/csi",
		Parameters: map[string]string{"type": "gp3"}}
}

// volumesOf waits for the storage informers' first sync and returns the volumes a poll reads.
func volumesOf(t *testing.T, m *Metadata) []v1.Volume {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if vols, ok := m.Volumes(context.Background()); ok {
			return vols
		}
		if time.Now().After(deadline) {
			t.Fatal("the storage informers never synced")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func volumeNamed(vols []v1.Volume, name string) *v1.Volume {
	for i := range vols {
		if vols[i].Name == name {
			return &vols[i]
		}
	}
	return nil
}

func TestStorage_ReportsEveryVolumeWithItsClassHandleAndClaim(t *testing.T) {
	const (
		ebsHandle   = "vol-0a1b2c3d4e5f60718"
		azureHandle = "/subscriptions/0000/resourceGroups/mc_rg/providers/Microsoft.Compute/disks/pvc-cf675ad5"
		pdHandle    = "projects/acme/zones/europe-west1-b/disks/pvc-9e1f"
	)
	m := startMetadata(t, &config.Config{},
		persistentVolume("pvc-ebs", "gp3", corev1.VolumeBound, "shop/data-web-0", csi("ebs.csi.aws.com", ebsHandle)),
		persistentVolume("pvc-azure", "managed-csi", corev1.VolumeReleased, "shop/gone", csi("disk.csi.azure.com", azureHandle)),
		persistentVolume("pvc-gke", "standard-rwo", corev1.VolumeAvailable, "", csi("pd.csi.storage.gke.io", pdHandle)),
		persistentVolume("intree-ebs", "gp2", corev1.VolumeBound, "shop/old-ebs",
			corev1.PersistentVolumeSource{AWSElasticBlockStore: &corev1.AWSElasticBlockStoreVolumeSource{VolumeID: "vol-0intree"}}),
		persistentVolume("intree-azure", "default", corev1.VolumeBound, "shop/old-azure",
			corev1.PersistentVolumeSource{AzureDisk: &corev1.AzureDiskVolumeSource{DiskName: "old", DataDiskURI: azureHandle + "-old"}}),
		persistentVolume("intree-gce", "standard", corev1.VolumeBound, "shop/old-gce",
			corev1.PersistentVolumeSource{GCEPersistentDisk: &corev1.GCEPersistentDiskVolumeSource{PDName: "old-disk"}}),
		persistentVolume("pvc-local", "local-path", corev1.VolumeBound, "db/data",
			corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/opt/local-path-provisioner/pvc-local"}}),
		persistentVolume("pvc-overlong", "x", corev1.VolumeBound, "db/x", csi("ebs.csi.aws.com", "vol-"+strings.Repeat("h", v1.MaxVolumeHandleLength))),
		persistentVolume("pvc-nfs", "nfs-csi", corev1.VolumeBound, "db/shared", csi("nfs.csi.k8s.io", "nfs-01.corp.internal#exports/team#pvc-nfs#")),
		betaClass(persistentVolume("pvc-beta", "", corev1.VolumeBound, "db/old", corev1.PersistentVolumeSource{}), "slow"),
	)

	vols := volumesOf(t, m)

	const tenGiB = 10 << 30
	want := []v1.Volume{
		{Name: "intree-azure", CapacityBytes: tenGiB, StorageClass: "default", CSIDriver: "kubernetes.io/azure-disk", VolumeHandle: azureHandle + "-old", Phase: "Bound", ClaimNamespace: "shop", ClaimName: "old-azure"},
		{Name: "intree-ebs", CapacityBytes: tenGiB, StorageClass: "gp2", CSIDriver: "kubernetes.io/aws-ebs", VolumeHandle: "vol-0intree", Phase: "Bound", ClaimNamespace: "shop", ClaimName: "old-ebs"},
		{Name: "intree-gce", CapacityBytes: tenGiB, StorageClass: "standard", CSIDriver: "kubernetes.io/gce-pd", VolumeHandle: "old-disk", Phase: "Bound", ClaimNamespace: "shop", ClaimName: "old-gce"},
		{Name: "pvc-azure", CapacityBytes: tenGiB, StorageClass: "managed-csi", CSIDriver: "disk.csi.azure.com", VolumeHandle: azureHandle, Phase: "Released"},
		// A volume from before storageClassName names its class in the beta annotation.
		{Name: "pvc-beta", CapacityBytes: tenGiB, StorageClass: "slow", Phase: "Bound", ClaimNamespace: "db", ClaimName: "old"},
		{Name: "pvc-ebs", CapacityBytes: tenGiB, StorageClass: "gp3", CSIDriver: "ebs.csi.aws.com", VolumeHandle: ebsHandle, Phase: "Bound", ClaimNamespace: "shop", ClaimName: "data-web-0"},
		{Name: "pvc-gke", CapacityBytes: tenGiB, StorageClass: "standard-rwo", CSIDriver: "pd.csi.storage.gke.io", VolumeHandle: pdHandle, Phase: "Available"},
		{Name: "pvc-local", CapacityBytes: tenGiB, StorageClass: "local-path", Phase: "Bound", ClaimNamespace: "db", ClaimName: "data"},
		// Only a cloud disk's handle leaves the cluster: an NFS one names internal servers and shares.
		{Name: "pvc-nfs", CapacityBytes: tenGiB, StorageClass: "nfs-csi", CSIDriver: "nfs.csi.k8s.io", Phase: "Bound", ClaimNamespace: "db", ClaimName: "shared"},
		// A handle past the API's cap is left out, so the volume falls to its class's rate.
		{Name: "pvc-overlong", CapacityBytes: tenGiB, StorageClass: "x", CSIDriver: "ebs.csi.aws.com", Phase: "Bound", ClaimNamespace: "db", ClaimName: "x"},
	}
	if !reflect.DeepEqual(vols, want) {
		t.Fatalf("volumes =\n%+v\nwant, in name order,\n%+v", vols, want)
	}

	a := NewAggregator("c", "0.4.0", "id", 10*time.Second, t0)
	for i := range 3 {
		obs := obsAt(t0.Add(time.Duration(i)*10*time.Second), []string{"n1"}, nil, nil)
		obs.Volumes = vols
		a.Observe(obs)
	}
	r, ok := a.Finish(t0.Add(20 * time.Second))
	if !ok || len(r.Volumes) != len(want) {
		t.Fatalf("report volumes = %+v, want %d", r.Volumes, len(want))
	}
	for i, v := range r.Volumes {
		if v.Name != want[i].Name || !near(v.ObservedSeconds, 20) {
			t.Errorf("volume %d = %s observed %vs, want %s observed over both 10s polls", i, v.Name, v.ObservedSeconds, want[i].Name)
		}
	}
}

func TestStorage_ForwardsOnlyTheClassRateAnnotation(t *testing.T) {
	m := startMetadata(t, &config.Config{},
		storageClass("local-path", map[string]string{
			v1.AnnotationStorageGBHourlyRate:                   "0.0002",
			"storageclass.kubernetes.io/is-default-class":      "true",
			"kubectl.kubernetes.io/last-applied-configuration": `{"secret":"do-not-send"}`,
		}),
		storageClass("gp3", map[string]string{"storageclass.kubernetes.io/is-default-class": "false"}),
		persistentVolume("pvc-local", "local-path", corev1.VolumeBound, "db/data", corev1.PersistentVolumeSource{}),
		persistentVolume("pvc-ebs", "gp3", corev1.VolumeBound, "db/logs", csi("ebs.csi.aws.com", "vol-1")),
		persistentVolume("pvc-orphan", "deleted-class", corev1.VolumeAvailable, "", corev1.PersistentVolumeSource{}),
	)

	vols := volumesOf(t, m)

	if got := volumeNamed(vols, "pvc-local").StorageClassAnnotations; !reflect.DeepEqual(got, map[string]string{v1.AnnotationStorageGBHourlyRate: "0.0002"}) {
		t.Errorf("local-path volume class annotations = %v, want only the rate", got)
	}
	for _, name := range []string{"pvc-ebs", "pvc-orphan"} {
		if got := volumeNamed(vols, name).StorageClassAnnotations; got != nil {
			t.Errorf("%s class annotations = %v, want none: its class has no rate", name, got)
		}
	}
	for _, obj := range m.storage.classes.List() {
		c := obj.(*storagev1.StorageClass)
		if len(c.Annotations) > 1 || c.Parameters != nil || c.Provisioner != "" {
			t.Errorf("the cached class %s keeps what the agent never sends: %+v", c.Name, c)
		}
	}
}

func TestCachePod_KeepsTheClaimsItMounts(t *testing.T) {
	claim := func(volume, name string) corev1.Volume {
		return corev1.Volume{Name: volume, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name}}}
	}
	ephemeral := func(volume string) corev1.Volume {
		return corev1.Volume{Name: volume, VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{}}}
	}
	configMap := corev1.Volume{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{}}}
	many := make([]corev1.Volume, 0, 40)
	var first32 []string
	for i := 39; i >= 0; i-- {
		many = append(many, claim(fmt.Sprintf("v%d", i), fmt.Sprintf("claim-%02d", i)))
	}
	for i := range v1.MaxClaimsPerPod {
		first32 = append(first32, fmt.Sprintf("claim-%02d", i))
	}
	longName := strings.Repeat("p", v1.MaxNameLength-2)

	for _, tc := range []struct {
		name, pod string
		volumes   []corev1.Volume
		want      []string
	}{
		{"claims and a generic ephemeral volume, in name order", "web-0",
			[]corev1.Volume{configMap, ephemeral("scratch"), claim("data", "data-web-0"), claim("data-again", "data-web-0")},
			[]string{"data-web-0", "web-0-scratch"}},
		{"the first claims by name past the cap", "web-0", many, first32},
		{"no claims", "web-0", []corev1.Volume{configMap}, nil},
		{"an ephemeral claim whose name is past the API's cap", longName, []corev1.Volume{ephemeral("scratch")}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &Metadata{cfg: &config.Config{}}
			p := runningPod("shop", tc.pod, "n1", nil, nil, nil, corev1.Container{Name: "app"})
			p.Spec.Volumes = tc.volumes
			if got := m.cachePod(p).Claims; !reflect.DeepEqual(got, tc.want) {
				t.Errorf("claims = %#v, want %#v", got, tc.want)
			}
		})
	}

	p := runningPod("shop", "web-0", "n1", nil, nil, nil, corev1.Container{Name: "app"})
	p.Spec.Volumes = []corev1.Volume{claim("data", "data-web-0")}
	m := startMetadata(t, &config.Config{}, p)
	pods, err := m.Pods(context.Background())
	if err != nil || len(pods) != 1 || !reflect.DeepEqual(pods[0].Claims, []string{"data-web-0"}) {
		t.Fatalf("pods = %+v, %v; want web-0 sending its claim", pods, err)
	}
}

func TestStorage_ClaimLabelsOnlyWhenCollected(t *testing.T) {
	objects := []interface{ GetName() string }{
		persistentVolume("pvc-data", "gp3", corev1.VolumeBound, "shop/data", csi("ebs.csi.aws.com", "vol-1")),
		persistentVolume("pvc-big", "gp3", corev1.VolumeBound, "shop/big", csi("ebs.csi.aws.com", "vol-2")),
		persistentVolume("pvc-released", "gp3", corev1.VolumeReleased, "shop/data", csi("ebs.csi.aws.com", "vol-3")),
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "data", Labels: map[string]string{"team": "core"},
			Annotations: map[string]string{"volume.kubernetes.io/selected-node": "n1"}}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "big", Labels: manyLabels(200)}},
	}
	claimLists := func(client *fake.Clientset) int {
		n := 0
		for _, a := range client.Actions() {
			if a.GetVerb() == "list" && a.GetResource().Resource == "persistentvolumeclaims" {
				n++
			}
		}
		return n
	}

	t.Run("off", func(t *testing.T) {
		m, client := startMetadataWith(t, &config.Config{}, nil, objects...)
		for _, v := range volumesOf(t, m) {
			if v.ClaimLabels != nil {
				t.Errorf("%s carries claim labels %v with claim labels off", v.Name, v.ClaimLabels)
			}
		}
		if n := claimLists(client); n != 0 {
			t.Errorf("the agent listed claims %d times with claim labels off", n)
		}
	})

	t.Run("on", func(t *testing.T) {
		before := dropped(t, "claim_labels")
		m, client := startMetadataWith(t, &config.Config{CollectPVCLabels: true}, nil, objects...)
		vols := volumesOf(t, m)
		again := volumesOf(t, m)

		if got := volumeNamed(vols, "pvc-data").ClaimLabels; !reflect.DeepEqual(got, map[string]string{"team": "core"}) {
			t.Errorf("pvc-data claim labels = %v", got)
		}
		big := volumeNamed(vols, "pvc-big").ClaimLabels
		assertFitsTheAPI(t, "claim labels", big)
		if len(big) != v1.MaxMapEntries {
			t.Errorf("pvc-big kept %d claim labels, want the first %d", len(big), v1.MaxMapEntries)
		}
		if got := volumeNamed(vols, "pvc-released").ClaimLabels; got != nil {
			t.Errorf("a Released volume carries its old claim's labels %v", got)
		}
		if !reflect.DeepEqual(again, vols) {
			t.Error("a second poll within the refresh reads other labels")
		}
		if d := dropped(t, "claim_labels") - before; d != 72 {
			t.Errorf("claim_labels drops = %v, want 72: counted once when the claims are listed", d)
		}
		if n := claimLists(client); n != 1 {
			t.Errorf("claims listed %d times in two polls, want once per refresh", n)
		}
	})
}

func TestMetadata_StorageForbidden_StillReportsNodesAndPods(t *testing.T) {
	var lists atomic.Int32
	forbid := func(client *fake.Clientset) {
		client.PrependReactor("list", "persistentvolumes", func(clienttesting.Action) (bool, runtime.Object, error) {
			lists.Add(1)
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "persistentvolumes"}, "",
				errors.New(`User "system:serviceaccount:costfluent:agent" cannot list resource "persistentvolumes"`))
		})
	}
	core, logs := observer.New(zap.WarnLevel)
	client := fake.NewClientset()
	forbid(client)
	for _, obj := range []runtime.Object{richNode("n1"), richPod(0), persistentVolume("pvc-1", "gp3", corev1.VolumeBound, "shop/data", csi("ebs.csi.aws.com", "vol-1"))} {
		if err := client.Tracker().Add(obj); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m := NewMetadata(client, &config.Config{}, zap.New(core))
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start = %v, want it to return once nodes and pods sync", err)
	}

	for lists.Load() < 2 || testutil.ToFloat64(metrics.StorageUnreadable) != 1 {
		select {
		case <-ctx.Done():
			t.Fatalf("after %d volume lists the unreadable gauge is %v, want 1", lists.Load(), testutil.ToFloat64(metrics.StorageUnreadable))
		case <-time.After(10 * time.Millisecond):
		}
	}
	vols, ok := m.Volumes(ctx)
	if ok || vols != nil {
		t.Fatalf("volumes = %v, %v; want none while they cannot be read", vols, ok)
	}
	nodes, err := m.Nodes()
	if err != nil || len(nodes) != 1 {
		t.Fatalf("nodes = %v, %v", nodes, err)
	}
	pods, err := m.Pods(ctx)
	if err != nil || len(pods) != 1 {
		t.Fatalf("pods = %v, %v", pods, err)
	}
	a := NewAggregator("c", "0.4.0", "id", 10*time.Second, t0)
	for i := range 2 {
		obs := Observation{Time: t0.Add(time.Duration(i) * 10 * time.Second), Nodes: []v1.Node{ConvertNode(nodes[0])}, Pods: pods, Volumes: vols}
		a.Observe(obs)
	}
	r, ok := a.Finish(t0.Add(10 * time.Second))
	if !ok || len(r.Nodes) != 1 || len(r.Pods) != 1 || r.Volumes != nil {
		t.Fatalf("report = %d nodes, %d pods, volumes %v; want the node and pod and no volumes", len(r.Nodes), len(r.Pods), r.Volumes)
	}
	if n := logs.FilterMessageSnippet("persistentvolumes").Len(); n != 1 {
		t.Errorf("%d warnings over %d failed lists, want one: %v", n, lists.Load(), logs.All())
	}
}

func TestStorage_RecoversWhenItsListsSucceedAgain(t *testing.T) {
	var classLists atomic.Int32
	failFirstClassList := func(client *fake.Clientset) {
		client.PrependReactor("list", "storageclasses", func(clienttesting.Action) (bool, runtime.Object, error) {
			if classLists.Add(1) == 1 {
				return true, nil, apierrors.NewServiceUnavailable("the API server is restarting")
			}
			return false, nil, nil
		})
		client.PrependReactor("list", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "persistentvolumeclaims"}, "", errors.New("RBAC"))
		})
	}
	core, logs := observer.New(zap.InfoLevel)
	client := fake.NewClientset()
	failFirstClassList(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m := NewMetadata(client, &config.Config{CollectPVCLabels: true}, zap.New(core))
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}

	if vols := volumesOf(t, m); len(vols) != 0 {
		t.Fatalf("volumes = %+v, want none in an empty cluster", vols)
	}
	if got := testutil.ToFloat64(metrics.StorageUnreadable); got != 0 || logs.FilterMessageSnippet("listing storageclasses again").Len() != 1 {
		t.Errorf("unreadable gauge = %v and logs %v after the class list succeeded again", got, logs.All())
	}
	if logs.FilterMessageSnippet("listing persistent volume claims failed").Len() != 1 {
		t.Errorf("a refused claim list was not logged: %v", logs.All())
	}

	pv := persistentVolume("pvc-new", "gp3", corev1.VolumeBound, "shop/data", csi("ebs.csi.aws.com", "vol-new"))
	if err := client.Tracker().Add(pv); err != nil {
		t.Fatal(err)
	}
	for {
		if obj, ok, err := m.storage.volumes.GetByKey("pvc-new"); err == nil && ok {
			if _, ok := obj.(*cachedVolume); !ok {
				t.Fatalf("a volume delivered by the watch was cached as %T", obj)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the watched volume never reached the cache")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if vols := volumesOf(t, m); len(vols) != 1 || vols[0].ClaimName != "data" || vols[0].ClaimLabels != nil {
		t.Errorf("volumes = %+v, want pvc-new without labels, since its claim could not be listed", vols)
	}
}

func TestCachedVolume_CopiesAsTheInformerNeeds(t *testing.T) {
	v := cacheVolume(persistentVolume("pvc-1", "gp3", corev1.VolumeBound, "shop/data", csi("ebs.csi.aws.com", "vol-1")))
	c := v.DeepCopyObject().(*cachedVolume)
	if !reflect.DeepEqual(c, v) || c == v || v.GetObjectKind() == nil || v.GetObjectMeta().GetName() != "pvc-1" {
		t.Fatalf("DeepCopyObject = %+v, want an equal, separate volume named pvc-1", c)
	}
	list := &cachedVolumeList{Items: []*cachedVolume{v}}
	if l := list.DeepCopyObject().(*cachedVolumeList); len(l.Items) != 1 || &l.Items[0] == &list.Items[0] || list.GetObjectKind() == nil {
		t.Fatal("cachedVolumeList.DeepCopyObject must copy the item slice")
	}
}
