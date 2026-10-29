package collector

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	v1 "github.com/costfluent/k8s-agent/api/v1"
	"github.com/costfluent/k8s-agent/internal/config"
	"github.com/costfluent/k8s-agent/internal/metrics"
)

const (
	claimLabelsRefresh = 5 * time.Minute

	resourceVolumes = "persistentvolumes"
	resourceClasses = "storageclasses"

	// The in-tree plugins' names, sent as the CSI driver of a volume of their source.
	inTreeAWSEBS    = "kubernetes.io/aws-ebs"
	inTreeAzureDisk = "kubernetes.io/azure-disk"
	inTreeGCEPD     = "kubernetes.io/gce-pd"
)

// cloudDiskDrivers are the drivers whose volume handle is a cloud disk's id, which Costfluent
// matches against that cloud's bill. Any other driver's handle stays in the cluster: an NFS or SMB
// handle names internal servers and shares, and nothing reads it.
var cloudDiskDrivers = map[string]bool{
	"ebs.csi.aws.com":       true,
	"disk.csi.azure.com":    true,
	"pd.csi.storage.gke.io": true,
	inTreeAWSEBS:            true,
	inTreeAzureDisk:         true,
	inTreeGCEPD:             true,
}

// classAnnotations are the only storage class annotations the agent keeps and sends.
var classAnnotations = []string{v1.AnnotationStorageGBHourlyRate}

// storage reads persistent volumes and storage classes from informers that sync beside the node
// and pod ones, never holding them back: a ClusterRole without storage leaves volumes out of the
// reports and nothing else. Claims are listed only for their labels, when the owner opts in.
type storage struct {
	client kubernetes.Interface
	cfg    *config.Config
	logger *zap.Logger

	volumes, classes cache.Store
	synced           []cache.InformerSynced

	mu sync.Mutex
	// failing holds each storage resource whose last list failed.
	failing map[string]bool

	// claimsMu is held through a claim list, apart from mu, which the informers take.
	claimsMu    sync.Mutex
	claimLabels map[string]map[string]string
	claimsAt    time.Time
}

func newStorage(client kubernetes.Interface, cfg *config.Config, logger *zap.Logger) *storage {
	return &storage{client: client, cfg: cfg, logger: logger, failing: map[string]bool{}}
}

// cachedVolume is all the agent keeps of a persistent volume. Its strings are shared with the
// volumes built from it, so it is never modified once stored.
type cachedVolume struct {
	Name                    string
	CapacityBytes           int64
	StorageClass, CSIDriver string
	VolumeHandle, Phase     string
	ClaimNamespace          string
	ClaimName               string
}

func (c *cachedVolume) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

func (c *cachedVolume) DeepCopyObject() runtime.Object {
	cp := *c
	return &cp
}

// GetObjectMeta gives the informer the name it keys the volume by.
func (c *cachedVolume) GetObjectMeta() metav1.Object { return &metav1.ObjectMeta{Name: c.Name} }

// cachedVolumeList is a page-built volume list the reflector stores item by item.
type cachedVolumeList struct {
	metav1.ListMeta
	Items []*cachedVolume
}

func (l *cachedVolumeList) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

func (l *cachedVolumeList) DeepCopyObject() runtime.Object {
	c := *l
	c.Items = slices.Clone(l.Items)
	return &c
}

// start runs the volume and class informers until ctx ends, without waiting for them to sync.
func (s *storage) start(ctx context.Context) error {
	core, storageAPI := s.client.CoreV1(), s.client.StorageV1()
	volumes := cache.NewSharedIndexInformer(pagedListWatch{&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
			items, rv, err := listPages(ctx, opts, func(ctx context.Context, opts metav1.ListOptions) ([]corev1.PersistentVolume, metav1.ListMeta, error) {
				l, err := core.PersistentVolumes().List(ctx, opts)
				if err != nil {
					return nil, metav1.ListMeta{}, err
				}
				return l.Items, l.ListMeta, nil
			}, cacheVolume)
			s.listed(ctx, resourceVolumes, err)
			return &cachedVolumeList{ListMeta: metav1.ListMeta{ResourceVersion: rv}, Items: items}, err
		},
		WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			return core.PersistentVolumes().Watch(ctx, opts)
		},
	}}, &corev1.PersistentVolume{}, 0, cache.Indexers{})
	classes := cache.NewSharedIndexInformer(pagedListWatch{&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
			items, rv, err := listPages(ctx, opts, func(ctx context.Context, opts metav1.ListOptions) ([]storagev1.StorageClass, metav1.ListMeta, error) {
				l, err := storageAPI.StorageClasses().List(ctx, opts)
				if err != nil {
					return nil, metav1.ListMeta{}, err
				}
				return l.Items, l.ListMeta, nil
			}, func(c *storagev1.StorageClass) storagev1.StorageClass {
				stripClass(c)
				return *c
			})
			s.listed(ctx, resourceClasses, err)
			return &storagev1.StorageClassList{ListMeta: metav1.ListMeta{ResourceVersion: rv}, Items: items}, err
		},
		WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			return storageAPI.StorageClasses().Watch(ctx, opts)
		},
	}}, &storagev1.StorageClass{}, 0, cache.Indexers{})
	if err := volumes.SetTransform(func(obj any) (any, error) {
		if pv, ok := obj.(*corev1.PersistentVolume); ok {
			return cacheVolume(pv), nil
		}
		return obj, nil
	}); err != nil {
		return fmt.Errorf("persistent volume informer: %w", err)
	}
	if err := classes.SetTransform(func(obj any) (any, error) {
		if c, ok := obj.(*storagev1.StorageClass); ok {
			stripClass(c)
		}
		return obj, nil
	}); err != nil {
		return fmt.Errorf("storage class informer: %w", err)
	}
	s.volumes, s.classes = volumes.GetStore(), classes.GetStore()
	s.synced = []cache.InformerSynced{volumes.HasSynced, classes.HasSynced}
	go volumes.Run(ctx.Done())
	go classes.Run(ctx.Done())
	return nil
}

// listed records whether a storage list succeeded, warning once when a resource turns unreadable
// rather than at every retry of the reflector.
func (s *storage) listed(ctx context.Context, resource string, err error) {
	if err != nil && ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch was := s.failing[resource]; {
	case err != nil && !was:
		s.logger.Warn("cannot list "+resource+"; reports leave persistent volumes out until it can. Check that the agent's ClusterRole grants list and watch on it",
			zap.Error(err))
	case err == nil && was:
		s.logger.Info("listing " + resource + " again; reports carry persistent volumes")
	}
	s.failing[resource] = err != nil
	unreadable := 0.0
	for _, failing := range s.failing {
		if failing {
			unreadable = 1
		}
	}
	metrics.StorageUnreadable.Set(unreadable)
}

// readable reports whether both informers have synced and their last lists succeeded.
func (s *storage) readable() bool {
	if s.volumes == nil {
		return false
	}
	for _, synced := range s.synced {
		if !synced() {
			return false
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, failing := range s.failing {
		if failing {
			return false
		}
	}
	return true
}

// read returns every persistent volume, sorted by name, and false when storage cannot be read.
func (s *storage) read(ctx context.Context) ([]v1.Volume, bool) {
	if !s.readable() {
		return nil, false
	}
	labels := s.claimLabelsFor(ctx)
	cached := s.volumes.List()
	out := make([]v1.Volume, 0, len(cached))
	for _, obj := range cached {
		c, ok := obj.(*cachedVolume)
		if !ok {
			s.logger.Error("the persistent volume cache holds an unexpected type; reporting no volumes", zap.String("type", fmt.Sprintf("%T", obj)))
			return nil, false
		}
		v := v1.Volume{
			Name:           c.Name,
			CapacityBytes:  c.CapacityBytes,
			StorageClass:   c.StorageClass,
			CSIDriver:      c.CSIDriver,
			VolumeHandle:   c.VolumeHandle,
			Phase:          c.Phase,
			ClaimNamespace: c.ClaimNamespace,
			ClaimName:      c.ClaimName,
		}
		if c.StorageClass != "" {
			if obj, ok, _ := s.classes.GetByKey(c.StorageClass); ok {
				if class, ok := obj.(*storagev1.StorageClass); ok {
					v.StorageClassAnnotations = fitMap(class.Annotations, "storage_class_annotations")
				}
			}
		}
		if c.ClaimName != "" {
			v.ClaimLabels = labels[c.ClaimNamespace+"/"+c.ClaimName]
		}
		out = append(out, v)
	}
	slices.SortFunc(out, func(a, b v1.Volume) int { return strings.Compare(a.Name, b.Name) })
	return out, true
}

type claimKeyLabels struct {
	key    string
	labels map[string]string
}

// claimLabelsFor returns each claim's labels within the API's map caps, keyed namespace/name,
// listed at most every claimLabelsRefresh. Nil unless the owner opted in.
func (s *storage) claimLabelsFor(ctx context.Context) map[string]map[string]string {
	if !s.cfg.CollectPVCLabels {
		return nil
	}
	s.claimsMu.Lock()
	defer s.claimsMu.Unlock()
	if s.claimLabels != nil && time.Since(s.claimsAt) < claimLabelsRefresh {
		return s.claimLabels
	}
	items, _, err := listPages(ctx, metav1.ListOptions{}, func(ctx context.Context, opts metav1.ListOptions) ([]corev1.PersistentVolumeClaim, metav1.ListMeta, error) {
		l, err := s.client.CoreV1().PersistentVolumeClaims("").List(ctx, opts)
		if err != nil {
			return nil, metav1.ListMeta{}, err
		}
		return l.Items, l.ListMeta, nil
	}, func(c *corev1.PersistentVolumeClaim) claimKeyLabels {
		return claimKeyLabels{key: c.Namespace + "/" + c.Name, labels: fitMap(c.Labels, "claim_labels")}
	})
	if err != nil {
		s.logger.Warn("listing persistent volume claims failed; keeping the previous claim labels", zap.Error(err))
		return s.claimLabels
	}
	byClaim := make(map[string]map[string]string, len(items))
	for _, c := range items {
		if c.labels != nil {
			byClaim[c.key] = c.labels
		}
	}
	s.claimLabels, s.claimsAt = byClaim, time.Now()
	return byClaim
}

// cacheVolume keeps what the agent sends of a persistent volume. Only a cloud disk's handle is
// kept, and one past the API's cap is left out, so the volume is priced by its class rather than
// refusing the report.
func cacheVolume(pv *corev1.PersistentVolume) *cachedVolume {
	driver, handle := volumeSource(&pv.Spec.PersistentVolumeSource)
	if !cloudDiskDrivers[driver] || utf16Len(handle) > v1.MaxVolumeHandleLength {
		handle = ""
	}
	class := pv.Spec.StorageClassName
	if beta, ok := pv.Annotations[corev1.BetaStorageClassAnnotation]; ok {
		class = beta
	}
	c := &cachedVolume{
		Name:          pv.Name,
		CapacityBytes: bytesOf(pv.Spec.Capacity.Storage()),
		StorageClass:  intern(class),
		CSIDriver:     intern(driver),
		VolumeHandle:  handle,
		Phase:         intern(string(pv.Status.Phase)),
	}
	if ref := pv.Spec.ClaimRef; ref != nil && pv.Status.Phase == corev1.VolumeBound {
		c.ClaimNamespace, c.ClaimName = intern(ref.Namespace), ref.Name
	}
	return c
}

// volumeSource names a volume's CSI driver and handle, or for an in-tree cloud disk its plugin and
// disk id. Any other source has neither.
func volumeSource(src *corev1.PersistentVolumeSource) (string, string) {
	switch {
	case src.CSI != nil:
		return src.CSI.Driver, src.CSI.VolumeHandle
	case src.AWSElasticBlockStore != nil:
		return inTreeAWSEBS, src.AWSElasticBlockStore.VolumeID
	case src.AzureDisk != nil:
		return inTreeAzureDisk, src.AzureDisk.DataDiskURI
	case src.GCEPersistentDisk != nil:
		return inTreeGCEPD, src.GCEPersistentDisk.PDName
	}
	return "", ""
}

// stripClass cuts a storage class down, in place, to its name and rate annotation.
func stripClass(c *storagev1.StorageClass) {
	*c = storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: c.Name, Annotations: keepKeys(c.Annotations, classAnnotations)}}
}
