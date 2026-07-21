package driver

import (
	"context"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

const startupAttachmentSyncTimeout = 30 * time.Second

type startupFencingVolume struct {
	datasetName  string
	dataset      *truenas.Dataset
	shareType    ShareType
	volumeID     string
	records      map[string]publicationRecord
	deferReasons []string
}

// reconcilePublishedAttachments is the rolling-upgrade bridge from the old
// statically-authorized transport model to durable per-volume publication
// records. Informer caches are fully synced before the snapshot is used, and
// every attached VolumeAttachment is folded into its volume before any backend
// allowlist is changed. Grouping first is important in strict mode: applying
// one attachment at a time could transiently fence another live node.
func (d *Driver) reconcilePublishedAttachments(ctx context.Context) error {
	if d.config == nil || !d.config.Fencing.Enabled() {
		return nil
	}
	if d.eventRecorder == nil || d.eventRecorder.clientset == nil {
		if d.config.Fencing.Mode == FencingModeStrict {
			return fmt.Errorf("strict fencing requires Kubernetes access for startup VolumeAttachment reconciliation")
		}
		klog.Warning("Startup fencing reconciliation skipped because Kubernetes access is unavailable; additive static allowlists remain in place")
		return nil
	}

	factory := informers.NewSharedInformerFactory(d.eventRecorder.clientset, 0)
	attachments := factory.Storage().V1().VolumeAttachments()
	persistentVolumes := factory.Core().V1().PersistentVolumes()
	csiNodes := factory.Storage().V1().CSINodes()
	nodes := factory.Core().V1().Nodes()
	// SharedInformerFactory only starts informers that have already been
	// materialized. Obtain each underlying informer before Start; doing this
	// lazily inside WaitForCacheSync would wait forever on informers that were
	// registered after the factory took its startup snapshot.
	attachmentInformer := attachments.Informer()
	persistentVolumeInformer := persistentVolumes.Informer()
	csiNodeInformer := csiNodes.Informer()
	nodeInformer := nodes.Informer()
	stopCh := make(chan struct{})
	defer close(stopCh)
	factory.Start(stopCh)
	if !cache.WaitForCacheSync(
		ctx.Done(),
		attachmentInformer.HasSynced,
		persistentVolumeInformer.HasSynced,
		csiNodeInformer.HasSynced,
		nodeInformer.HasSynced,
	) {
		return fmt.Errorf("timed out syncing Kubernetes caches for startup fencing reconciliation")
	}

	volumeAttachments, err := attachments.Lister().List(labels.Everything())
	if err != nil {
		return fmt.Errorf("list VolumeAttachments from synced cache: %w", err)
	}
	volumes := make(map[string]*startupFencingVolume)
	attachmentCount := 0
	for _, attachment := range volumeAttachments {
		if attachment.Spec.Attacher != d.name || !attachment.Status.Attached || attachment.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		pv, getErr := persistentVolumes.Lister().Get(*attachment.Spec.Source.PersistentVolumeName)
		if getErr != nil {
			return fmt.Errorf("resolve PersistentVolume %s for VolumeAttachment %s: %w",
				*attachment.Spec.Source.PersistentVolumeName, attachment.Name, getErr)
		}
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != d.name || pv.Spec.CSI.VolumeHandle == "" {
			continue
		}

		identity := NodeIdentity{Name: attachment.Spec.NodeName, Legacy: true}
		if csiNode, nodeErr := csiNodes.Lister().Get(attachment.Spec.NodeName); nodeErr == nil {
			for _, driver := range csiNode.Spec.Drivers {
				if driver.Name != d.name {
					continue
				}
				if parsed, parseErr := parseNodeIdentity(driver.NodeID); parseErr == nil {
					identity = mergeNodeIdentity(identity, parsed)
				}
				break
			}
		}
		// The VolumeAttachment node name is the durable Kubernetes key even if a
		// malformed or stale CSINode happens to advertise another display name.
		identity.Name = attachment.Spec.NodeName
		if node, nodeErr := nodes.Lister().Get(attachment.Spec.NodeName); nodeErr == nil {
			for _, address := range node.Status.Addresses {
				if ip := net.ParseIP(address.Address); ip != nil {
					identity.IPs = append(identity.IPs, ip)
				}
			}
			identity.IPs = canonicalNodeIPs(identity.IPs)
		}

		volumeID := pv.Spec.CSI.VolumeHandle
		volume := volumes[volumeID]
		if volume == nil {
			datasetName, nameErr := d.datasetForID(volumeID)
			if nameErr != nil {
				return fmt.Errorf("resolve attached volume %s: %w", volumeID, nameErr)
			}
			dataset, datasetErr := d.truenasClient.DatasetGet(ctx, datasetName)
			if datasetErr != nil {
				if truenas.IsNotFoundError(datasetErr) {
					return fmt.Errorf("attached volume %s has no backend dataset", volumeID)
				}
				return fmt.Errorf("read attached volume %s: %w", volumeID, datasetErr)
			}
			records, recordsErr := publicationRecordsFromDataset(dataset)
			if recordsErr != nil {
				return fmt.Errorf("read publication records for attached volume %s: %w", volumeID, recordsErr)
			}
			volume = &startupFencingVolume{
				datasetName: datasetName,
				dataset:     dataset,
				shareType:   shareTypeForPublishedVolume(dataset, pv.Spec.CSI.VolumeAttributes),
				volumeID:    volumeID,
				records:     records,
			}
			volumes[volumeID] = volume
		}

		if identityErr := validateIdentityForProtocol(identity, volume.shareType); identityErr != nil {
			if d.config.Fencing.Mode == FencingModeStrict {
				return fmt.Errorf("cannot reconcile attached volume %s on node %s in strict mode: %w",
					volumeID, attachment.Spec.NodeName, identityErr)
			}
			klog.Warningf("Startup fencing reconciliation could not add attached volume %s node %s: %v; additive static allowlists remain in place",
				volumeID, attachment.Spec.NodeName, identityErr)
			volume.deferReasons = append(volume.deferReasons,
				fmt.Sprintf("node %s has no enforceable transport identity", attachment.Spec.NodeName))
		}
		mode, readonly := accessModeForPersistentVolume(pv)
		record, recordErr := newPublicationRecord(identity, mode, readonly)
		if recordErr != nil {
			return fmt.Errorf("encode attached node %s identity: %w", attachment.Spec.NodeName, recordErr)
		}
		volume.records[publicationPropertyKey(identity.Name)] = record
		attachmentCount++
	}

	volumeIDs := make([]string, 0, len(volumes))
	for volumeID := range volumes {
		volumeIDs = append(volumeIDs, volumeID)
	}
	sort.Strings(volumeIDs)
	// Preflight every grouped volume before the first backend write. Two live
	// VolumeAttachments for a single-node PV can occur briefly during teardown;
	// strict mode must fail rather than authorize both, while additive mode keeps
	// legacy access intact and defers tightening until Kubernetes converges.
	for _, volumeID := range volumeIDs {
		volume := volumes[volumeID]
		for key := range volume.records {
			record := volume.records[key]
			if record.State != publicationStatePublished {
				continue
			}
			if compatibilityErr := validatePublicationCompatibility(volume.records, record); compatibilityErr != nil {
				if d.config.Fencing.Mode == FencingModeStrict {
					return fmt.Errorf("cannot reconcile attached volume %s in strict mode: %w", volumeID, compatibilityErr)
				}
				volume.deferReasons = append(volume.deferReasons, compatibilityErr.Error())
				break
			}
		}
	}
	for _, volumeID := range volumeIDs {
		volume := volumes[volumeID]
		keys := make([]string, 0, len(volume.records))
		for key := range volume.records {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := storePublicationRecord(ctx, d.truenasClient, volume.dataset, volume.datasetName, key, volume.records[key]); err != nil {
				return fmt.Errorf("persist startup attachment for volume %s: %w", volumeID, err)
			}
		}
		if len(volume.deferReasons) > 0 {
			// Do not partially tighten a volume while even one live attachment is
			// represented by a legacy node_id. The placeholder records block later
			// publishes from changing the allowlist until those nodes re-register.
			klog.Warningf("Startup fencing reconciliation deferred backend changes for volume %s: %v",
				volumeID, volume.deferReasons)
			continue
		}
		if err := d.ensureShareExists(ctx, volume.dataset, volume.datasetName, volume.volumeID, volume.shareType); err != nil {
			return fmt.Errorf("ensure share for startup attachment %s: %w", volumeID, err)
		}
		if err := d.applyBackendFence(ctx, volume.dataset, volume.datasetName, volume.shareType, volume.records); err != nil {
			return fmt.Errorf("enforce startup attachment fence for volume %s: %w", volumeID, err)
		}
	}
	klog.Infof("Startup fencing reconciliation completed: %d attached publication(s) across %d volume(s)", attachmentCount, len(volumes))
	return nil
}

func accessModeForPersistentVolume(pv *corev1.PersistentVolume) (csi.VolumeCapability_AccessMode_Mode, bool) {
	if pv == nil {
		return csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false
	}
	for _, mode := range pv.Spec.AccessModes {
		if mode == corev1.ReadWriteMany {
			return csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, false
		}
	}
	for _, mode := range pv.Spec.AccessModes {
		if mode == corev1.ReadOnlyMany {
			return csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY, true
		}
	}
	for _, mode := range pv.Spec.AccessModes {
		if mode == corev1.ReadWriteOncePod {
			return csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER, false
		}
	}
	return csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false
}

func (d *Driver) runStartupAttachmentReconcile() error {
	ctx, cancel := context.WithTimeout(context.Background(), startupAttachmentSyncTimeout)
	defer cancel()
	return d.reconcilePublishedAttachments(ctx)
}
