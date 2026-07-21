package driver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const startupReconcileWorkers = 4

var (
	startupReconcileInitialBackoff = 5 * time.Second
	startupReconcileMaxBackoff     = time.Minute
)

type startupPublication struct {
	identity NodeIdentity
	mode     csi.VolumeCapability_AccessMode_Mode
	readonly bool
}

type startupFencingVolume struct {
	volumeID         string
	volumeAttributes map[string]string
	publications     []startupPublication
}

// reconcilePublishedAttachments is the rolling-upgrade bridge from static
// transport authorization to durable per-volume publication records. It first
// takes a Kubernetes-only snapshot, then reconciles independent volumes with a
// four-worker bound. The TrueNAS client has ten request slots, so this leaves
// capacity for live CSI calls while startup convergence runs in the background.
func (d *Driver) reconcilePublishedAttachments(ctx context.Context) error {
	if d.config == nil || !d.config.Fencing.Enabled() {
		return nil
	}
	if d.eventRecorder == nil || d.eventRecorder.clientset == nil {
		return fmt.Errorf("fencing startup reconciliation requires Kubernetes client access")
	}
	clientset := d.eventRecorder.clientset

	pvList, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list PersistentVolumes for startup fencing reconciliation: %w", err)
	}
	attachmentList, err := clientset.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list VolumeAttachments for startup fencing reconciliation: %w", err)
	}
	csiNodeList, err := clientset.StorageV1().CSINodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list CSINodes for startup fencing reconciliation: %w", err)
	}
	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list Nodes for startup fencing reconciliation: %w", err)
	}

	pvs := make(map[string]*corev1.PersistentVolume, len(pvList.Items))
	for i := range pvList.Items {
		pvs[pvList.Items[i].Name] = &pvList.Items[i]
	}
	csiNodes := make(map[string]*storagev1.CSINode, len(csiNodeList.Items))
	for i := range csiNodeList.Items {
		csiNodes[csiNodeList.Items[i].Name] = &csiNodeList.Items[i]
	}
	nodes := make(map[string]*corev1.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		nodes[nodeList.Items[i].Name] = &nodeList.Items[i]
	}

	volumes := make(map[string]*startupFencingVolume)
	var collectionErrors []error
	attachmentCount := 0
	for i := range attachmentList.Items {
		attachment := &attachmentList.Items[i]
		if attachment.Spec.Attacher != d.name || !attachment.Status.Attached ||
			!attachment.DeletionTimestamp.IsZero() || attachment.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		pvName := *attachment.Spec.Source.PersistentVolumeName
		pv := pvs[pvName]
		if pv == nil {
			collectionErrors = append(collectionErrors, fmt.Errorf(
				"volume attachment %s references missing PersistentVolume %s", attachment.Name, pvName))
			continue
		}
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != d.name || pv.Spec.CSI.VolumeHandle == "" {
			continue
		}

		identity := startupNodeIdentity(d.name, attachment.Spec.NodeName, csiNodes[attachment.Spec.NodeName], nodes[attachment.Spec.NodeName])
		mode, readonly := accessModeForPersistentVolume(pv)
		volumeID := pv.Spec.CSI.VolumeHandle
		volume := volumes[volumeID]
		if volume == nil {
			volume = &startupFencingVolume{
				volumeID:         volumeID,
				volumeAttributes: pv.Spec.CSI.VolumeAttributes,
			}
			volumes[volumeID] = volume
		}
		volume.publications = append(volume.publications, startupPublication{
			identity: identity,
			mode:     mode,
			readonly: readonly,
		})
		attachmentCount++
	}

	volumeIDs := make([]string, 0, len(volumes))
	for volumeID := range volumes {
		volumeIDs = append(volumeIDs, volumeID)
	}
	sort.Strings(volumeIDs)
	jobs := make(chan *startupFencingVolume)
	results := make(chan error, len(volumeIDs))
	workerCount := startupReconcileWorkers
	if len(volumeIDs) < workerCount {
		workerCount = len(volumeIDs)
	}
	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for volume := range jobs {
				results <- d.reconcileStartupFencingVolume(ctx, volume)
			}
		}()
	}
	for _, volumeID := range volumeIDs {
		jobs <- volumes[volumeID]
	}
	close(jobs)
	workers.Wait()
	close(results)
	for result := range results {
		if result != nil {
			collectionErrors = append(collectionErrors, result)
		}
	}
	if len(collectionErrors) > 0 {
		return errors.Join(collectionErrors...)
	}
	klog.Infof("Startup fencing reconciliation converged: %d attached publication(s) across %d volume(s)",
		attachmentCount, len(volumes))
	return nil
}

func startupNodeIdentity(
	driverName, nodeName string,
	csiNode *storagev1.CSINode,
	node *corev1.Node,
) NodeIdentity {
	identity := NodeIdentity{Name: nodeName, Legacy: true}
	if csiNode != nil {
		for _, driver := range csiNode.Spec.Drivers {
			if driver.Name != driverName {
				continue
			}
			if parsed, err := parseNodeIdentity(driver.NodeID); err == nil {
				identity = mergeNodeIdentity(identity, parsed)
			}
			break
		}
	}
	// VolumeAttachment.spec.nodeName is the durable Kubernetes identity even if
	// a stale or malformed CSINode advertises another display name.
	identity.Name = nodeName
	if node != nil {
		for _, address := range node.Status.Addresses {
			if ip := net.ParseIP(address.Address); ip != nil {
				identity.IPs = append(identity.IPs, ip)
			}
		}
		identity.IPs = canonicalNodeIPs(identity.IPs)
	}
	return identity
}

func (d *Driver) reconcileStartupFencingVolume(ctx context.Context, volume *startupFencingVolume) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lockKey := "volume:" + volume.volumeID
	if !d.acquireOperationLock(lockKey) {
		return fmt.Errorf("startup reconcile volume %s: live CSI operation is in progress", volume.volumeID)
	}
	defer d.releaseOperationLock(lockKey)

	// The initial list only schedules work. Rebuild the current attachment set
	// after taking the same per-volume lock as ControllerPublish/Unpublish. A VA
	// with a deletion timestamp is already in the unpublish path and must never be
	// re-granted from the stale startup snapshot.
	currentVolume, err := d.currentStartupFencingVolume(ctx, volume.volumeID)
	if err != nil {
		return fmt.Errorf("refresh attached volume %s: %w", volume.volumeID, err)
	}
	if len(currentVolume.publications) == 0 {
		return nil
	}
	volume = currentVolume
	datasetName, err := d.datasetForID(volume.volumeID)
	if err != nil {
		return fmt.Errorf("resolve attached volume %s: %w", volume.volumeID, err)
	}

	// The dataset and its publication properties must be read only after the
	// volume lock is held; startup now runs concurrently with the served CSI API.
	dataset, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return fmt.Errorf("read attached volume %s: %w", volume.volumeID, err)
	}
	records, err := publicationRecordsFromDataset(dataset)
	if err != nil {
		return fmt.Errorf("read publication records for attached volume %s: %w", volume.volumeID, err)
	}
	shareType := shareTypeForPublishedVolume(dataset, volume.volumeAttributes)
	compatibilityRecords := make(map[string]publicationRecord, len(records)+len(volume.publications))
	for key := range records {
		compatibilityRecords[key] = records[key]
	}
	desired := make(map[string]publicationRecord)
	deferred := false
	for _, publication := range volume.publications {
		isDeferred, identityErr := d.validateOrDeferFencingIdentity(publication.identity, shareType)
		if identityErr != nil {
			return fmt.Errorf("cannot reconcile attached volume %s on node %s: %w",
				volume.volumeID, publication.identity.Name, identityErr)
		}
		record, recordErr := newPublicationRecord(publication.identity, publication.mode, publication.readonly)
		if recordErr != nil {
			return fmt.Errorf("encode attached node %s identity: %w", publication.identity.Name, recordErr)
		}
		if compatibilityErr := validatePublicationCompatibility(compatibilityRecords, record); compatibilityErr != nil {
			// Transient dual-VA states are normal during migration. Both modes retry
			// this volume; strict readiness remains false, but the process stays up.
			return fmt.Errorf("startup fencing for volume %s has not converged: %w", volume.volumeID, compatibilityErr)
		}
		if compatibilityErr := d.validateBackendSingleNodeCompatibility(
			ctx, dataset, datasetName, shareType, publication.identity, compatibilityRecords, publication.mode,
		); compatibilityErr != nil {
			return fmt.Errorf("startup fencing for volume %s has not converged: %w", volume.volumeID, compatibilityErr)
		}
		key := publicationPropertyKey(publication.identity.Name)
		previous, hasPrevious := records[key]
		if ownershipErr := d.populateAdditiveGrantOwnership(
			ctx, dataset, datasetName, shareType, publication.identity,
			previous, hasPrevious, isDeferred, &record,
		); ownershipErr != nil {
			return fmt.Errorf("classify startup grant ownership for volume %s on node %s: %w",
				volume.volumeID, publication.identity.Name, ownershipErr)
		}
		compatibilityRecords[key] = record
		records[key] = record
		desired[key] = record
		if isDeferred {
			// Persist publication ownership for every captured VA, including legacy
			// identities. applyBackendFence skips only the deferred identity in
			// additive mode, so enforceable peers can still converge while the
			// preserved static policy carries the legacy node.
			deferred = true
			continue
		}
	}
	for key := range desired {
		record := desired[key]
		if err := storePublicationRecord(ctx, d.truenasClient, dataset, datasetName, key, record); err != nil {
			return fmt.Errorf("persist startup attachment for volume %s: %w", volume.volumeID, err)
		}
		d.stalePublicationRecordsSeen.Delete(stalePublicationObservationKey(datasetName, key))
	}
	if len(desired) > 0 {
		if err := d.ensureShareExists(ctx, dataset, datasetName, volume.volumeID, shareType); err != nil {
			return fmt.Errorf("ensure share for startup attachment %s: %w", volume.volumeID, err)
		}
		if err := d.applyBackendFence(ctx, dataset, datasetName, shareType, records); err != nil {
			return fmt.Errorf("enforce startup attachment fence for volume %s: %w", volume.volumeID, err)
		}
	}
	if deferred {
		return fmt.Errorf("%w: startup fencing for volume %s is waiting for node identity re-registration",
			errFenceDeferred, volume.volumeID)
	}
	return nil
}

func (d *Driver) currentStartupFencingVolume(ctx context.Context, volumeID string) (*startupFencingVolume, error) {
	result := &startupFencingVolume{volumeID: volumeID}
	clientset := d.eventRecorder.clientset
	pvList, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list PersistentVolumes: %w", err)
	}
	pvs := make(map[string]*corev1.PersistentVolume)
	for i := range pvList.Items {
		pv := &pvList.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.name && pv.Spec.CSI.VolumeHandle == volumeID {
			pvs[pv.Name] = pv
		}
	}
	attachmentList, err := clientset.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list VolumeAttachments: %w", err)
	}
	type currentAttachment struct {
		nodeName string
		pv       *corev1.PersistentVolume
	}
	current := make([]currentAttachment, 0)
	for i := range attachmentList.Items {
		attachment := &attachmentList.Items[i]
		if attachment.Spec.Attacher != d.name || !attachment.Status.Attached ||
			!attachment.DeletionTimestamp.IsZero() || attachment.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		pv := pvs[*attachment.Spec.Source.PersistentVolumeName]
		if pv == nil {
			continue
		}
		current = append(current, currentAttachment{nodeName: attachment.Spec.NodeName, pv: pv})
	}
	if len(current) == 0 {
		return result, nil
	}
	csiNodeList, err := clientset.StorageV1().CSINodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list CSINodes: %w", err)
	}
	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list Nodes: %w", err)
	}
	csiNodes := make(map[string]*storagev1.CSINode, len(csiNodeList.Items))
	for i := range csiNodeList.Items {
		csiNodes[csiNodeList.Items[i].Name] = &csiNodeList.Items[i]
	}
	nodes := make(map[string]*corev1.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		nodes[nodeList.Items[i].Name] = &nodeList.Items[i]
	}
	for _, attachment := range current {
		identity := startupNodeIdentity(d.name, attachment.nodeName, csiNodes[attachment.nodeName], nodes[attachment.nodeName])
		mode, readonly := accessModeForPersistentVolume(attachment.pv)
		result.volumeAttributes = attachment.pv.Spec.CSI.VolumeAttributes
		result.publications = append(result.publications, startupPublication{identity: identity, mode: mode, readonly: readonly})
	}
	return result, nil
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

func (d *Driver) runStartupAttachmentReconcile(parent context.Context) error {
	timeout, err := d.config.Fencing.StartupReconcileTimeoutDuration()
	if err != nil {
		return fmt.Errorf("invalid fencing.startupReconcileTimeout: %w", err)
	}
	if timeout <= 0 {
		return fmt.Errorf("fencing.startupReconcileTimeout must be positive")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return d.reconcilePublishedAttachments(ctx)
}

func (d *Driver) startStartupAttachmentReconcile() {
	if d.config == nil || !d.config.Fencing.Enabled() {
		return
	}
	signal := d.startupAttachmentReconcileSignal()
	ctx, cancel := context.WithCancel(context.Background())
	d.startupReconcileCancel = cancel
	d.startupReconcileWg.Add(1)
	go func() {
		defer d.startupReconcileWg.Done()
		backoff := startupReconcileInitialBackoff
		waitForSignal := false
		for {
			if waitForSignal {
				// A converged additive controller stays idle. A later publish that
				// must defer fencing signals this channel, allowing retry without a
				// permanent cluster-wide polling and backend-write loop.
				select {
				case <-signal:
					backoff = startupReconcileInitialBackoff
					waitForSignal = false
				case <-ctx.Done():
					return
				}
			}
			err := d.runStartupAttachmentReconcile(ctx)
			if err == nil {
				if d.config.Fencing.Mode == FencingModeStrict {
					d.ready.Store(true)
					return
				}
				waitForSignal = true
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if d.config.Fencing.Mode == FencingModeStrict {
				d.ready.Store(false)
			}
			klog.Warningf("Background startup fencing reconciliation incomplete; retrying in %v: %v", backoff, err)
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
			backoff *= 2
			if backoff > startupReconcileMaxBackoff {
				backoff = startupReconcileMaxBackoff
			}
		}
	}()
}

func (d *Driver) startupAttachmentReconcileSignal() <-chan struct{} {
	d.startupReconcileOnce.Do(func() {
		d.startupReconcileSignal = make(chan struct{}, 1)
	})
	return d.startupReconcileSignal
}

func (d *Driver) requestStartupAttachmentReconcile() {
	d.startupAttachmentReconcileSignal()
	select {
	case d.startupReconcileSignal <- struct{}{}:
	default:
		// A pending signal already covers this deferral.
	}
}

func (d *Driver) stopStartupAttachmentReconcile() {
	if d.startupReconcileCancel != nil {
		d.startupReconcileCancel()
		d.startupReconcileWg.Wait()
	}
}
