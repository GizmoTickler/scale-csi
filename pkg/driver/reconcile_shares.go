package driver

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// nfsShareCommentDatasetName extracts the backing dataset name from a CSI-managed
// NFS share comment of the form "truenas-csi (<driverName>): <datasetName>". The
// boolean is false when the comment is not a CSI share comment for THIS driver
// instance, so foreign shares are never classified or touched.
func (d *Driver) nfsShareCommentDatasetName(comment string) (string, bool) {
	prefix := "truenas-csi (" + d.name + "): "
	if !strings.HasPrefix(comment, prefix) {
		return "", false
	}
	datasetName := strings.TrimSpace(strings.TrimPrefix(comment, prefix))
	if datasetName == "" {
		return "", false
	}
	return datasetName, true
}

// iscsiExtentCommentDatasetName extracts the backing dataset name from a
// CSI-managed iSCSI extent comment of the form "truenas-csi: <datasetName>".
// Unlike the NFS share comment, the iSCSI extent comment does NOT embed the
// driver instance name, so driver-instance scoping is enforced separately by
// requiring the derived dataset to live under the configured parent dataset (see
// datasetUnderParent). The boolean is false when the comment is not a CSI extent
// comment at all, so foreign extents are never classified or touched.
func iscsiExtentCommentDatasetName(comment string) (string, bool) {
	const prefix = "truenas-csi: "
	if !strings.HasPrefix(comment, prefix) {
		return "", false
	}
	datasetName := strings.TrimSpace(strings.TrimPrefix(comment, prefix))
	if datasetName == "" {
		return "", false
	}
	return datasetName, true
}

// zvolReferenceDatasetName extracts the backing dataset name from a zvol device
// reference of the form "zvol/<datasetName>" (tolerating a leading /dev/ or /).
// This is the authoritative, non-lossy backreference carried by NVMe-oF
// namespaces; the lossy subsystem NAME is never used to decide deletion.
func zvolReferenceDatasetName(devicePath string) (string, bool) {
	reference := normalizedZvolReference(devicePath)
	if !strings.HasPrefix(reference, "zvol/") {
		return "", false
	}
	datasetName := strings.TrimPrefix(reference, "zvol/")
	if datasetName == "" {
		return "", false
	}
	return datasetName, true
}

// shareOrphanLivePV reports whether the volume backing a share orphan still has
// a live PersistentVolume. Such a share is anomalous (absent dataset under a live
// PV) and must be surfaced, never swept.
func shareOrphanLivePV(kubeState *kubernetesReconcileState, volumeID string) bool {
	if kubeState == nil {
		return false
	}
	_, live := kubeState.volumeHandles[volumeID]
	return live
}

// detectOrphanedShares finds CSI-managed backend shares (NFS, iSCSI, NVMe-oF)
// whose backing dataset is confirmed absent. DeleteVolume removes the share
// before the dataset, so a share that outlives its dataset is residue from an
// interrupted delete; sweeping it keeps that residue from being silently
// permanent. A share still referenced by a live PersistentVolume is never
// classified: an absent dataset under a live PV is anomalous and must be
// surfaced, not "fixed" by deleting the share. Detection is read-only; deletion
// happens in the guarded delete phase. Each protocol is detected independently so
// a listing failure in one cannot leak the others' orphans.
func (d *Driver) detectOrphanedShares(ctx context.Context, kubeState *kubernetesReconcileState, report *ReconcileReport) {
	d.detectOrphanedNFSShares(ctx, kubeState, report)
	d.detectOrphanedISCSIShares(ctx, kubeState, report)
	d.detectOrphanedNVMeoFShares(ctx, kubeState, report)
	sort.Slice(report.OrphanShares, func(i, j int) bool { return report.OrphanShares[i].ID < report.OrphanShares[j].ID })
	report.OrphanShareCount = len(report.OrphanShares)
}

func (d *Driver) detectOrphanedNFSShares(ctx context.Context, kubeState *kubernetesReconcileState, report *ReconcileReport) {
	shares, err := d.truenasClient.NFSShareList(ctx)
	if err != nil {
		RecordReconcileFailure("list_backend_shares")
		klog.Warningf("Orphan reconcile: failed to list NFS shares for orphan detection: %v", err)
		return
	}
	for _, share := range shares {
		if share == nil {
			continue
		}
		datasetName, ok := d.nfsShareCommentDatasetName(share.Comment)
		if !ok {
			continue
		}
		volumeID := path.Base(datasetName)
		if kubeState != nil {
			if _, live := kubeState.volumeHandles[volumeID]; live {
				continue
			}
		}
		if _, getErr := d.truenasClient.DatasetGet(ctx, datasetName); getErr == nil {
			continue // dataset still present: the share is not orphaned
		} else if !truenas.IsNotFoundError(getErr) {
			klog.Warningf("Orphan reconcile: skipping NFS share %d orphan check for %s: dataset lookup failed: %v", share.ID, datasetName, getErr)
			continue
		}
		report.OrphanShares = append(report.OrphanShares, ReconcileObject{
			ID:             datasetName,
			BackendID:      strconv.Itoa(share.ID),
			SourceVolumeID: volumeID,
			Protocol:       ShareTypeNFS,
		})
	}
}

func (d *Driver) detectOrphanedISCSIShares(ctx context.Context, kubeState *kubernetesReconcileState, report *ReconcileReport) {
	extents, err := d.truenasClient.ISCSIExtentList(ctx)
	if err != nil {
		RecordReconcileFailure("list_backend_shares")
		klog.Warningf("Orphan reconcile: failed to list iSCSI extents for orphan detection: %v", err)
		return
	}
	for _, extent := range extents {
		if extent == nil {
			continue
		}
		// The extent comment is the authoritative, non-lossy backreference to the
		// dataset; the lossy extent NAME is never used for classification.
		datasetName, ok := iscsiExtentCommentDatasetName(extent.Comment)
		if !ok {
			continue
		}
		if !d.datasetUnderParent(datasetName) {
			continue // foreign driver instance or non-CSI dataset
		}
		volumeID := path.Base(datasetName)
		if shareOrphanLivePV(kubeState, volumeID) {
			continue
		}
		if _, getErr := d.truenasClient.DatasetGet(ctx, datasetName); getErr == nil {
			continue // dataset still present: the share is not orphaned
		} else if !truenas.IsNotFoundError(getErr) {
			klog.Warningf("Orphan reconcile: skipping iSCSI extent %d orphan check for %s: dataset lookup failed: %v", extent.ID, datasetName, getErr)
			continue
		}
		report.OrphanShares = append(report.OrphanShares, ReconcileObject{
			ID:             datasetName,
			BackendID:      strconv.Itoa(extent.ID),
			SourceVolumeID: volumeID,
			Protocol:       ShareTypeISCSI,
		})
	}
}

func (d *Driver) detectOrphanedNVMeoFShares(ctx context.Context, kubeState *kubernetesReconcileState, report *ReconcileReport) {
	subsystems, err := d.truenasClient.NVMeoFSubsystemList(ctx)
	if err != nil {
		RecordReconcileFailure("list_backend_shares")
		klog.Warningf("Orphan reconcile: failed to list NVMe-oF subsystems for orphan detection: %v", err)
		return
	}
	// Fetch every namespace in ONE query and group client-side by subsystem
	// instead of issuing NVMeoFNamespaceListBySubsystem per subsystem (~N round
	// trips per pass). Each namespace carries its SubsystemID, so the grouping is
	// lossless; the DevicePath backreference logic below is unchanged.
	allNamespaces, err := d.truenasClient.NVMeoFNamespaceList(ctx)
	if err != nil {
		RecordReconcileFailure("list_backend_shares")
		klog.Warningf("Orphan reconcile: failed to list NVMe-oF namespaces for orphan detection: %v", err)
		return
	}
	namespacesBySubsystem := make(map[int][]*truenas.NVMeoFNamespace, len(allNamespaces))
	for _, namespace := range allNamespaces {
		if namespace == nil {
			continue
		}
		namespacesBySubsystem[namespace.SubsystemID] = append(namespacesBySubsystem[namespace.SubsystemID], namespace)
	}
	for _, subsys := range subsystems {
		if subsys == nil {
			continue
		}
		// The namespace DevicePath (zvol/<dataset>) is the authoritative
		// backreference; the subsystem NAME is lossy and never used to decide
		// deletion. A subsystem with no namespace resolving to a dataset under the
		// parent is foreign and skipped.
		namespaces := namespacesBySubsystem[subsys.ID]
		for _, namespace := range namespaces {
			if namespace == nil {
				continue
			}
			datasetName, ok := zvolReferenceDatasetName(namespace.DevicePath)
			if !ok {
				continue
			}
			if !d.datasetUnderParent(datasetName) {
				continue
			}
			volumeID := path.Base(datasetName)
			if shareOrphanLivePV(kubeState, volumeID) {
				continue
			}
			if _, getErr := d.truenasClient.DatasetGet(ctx, datasetName); getErr == nil {
				continue // dataset still present: the share is not orphaned
			} else if !truenas.IsNotFoundError(getErr) {
				klog.Warningf("Orphan reconcile: skipping NVMe-oF subsystem %d orphan check for %s: dataset lookup failed: %v", subsys.ID, datasetName, getErr)
				continue
			}
			report.OrphanShares = append(report.OrphanShares, ReconcileObject{
				ID:             datasetName,
				BackendID:      strconv.Itoa(subsys.ID),
				SourceVolumeID: volumeID,
				Protocol:       ShareTypeNVMeoF,
			})
			// A CSI subsystem maps to a single dataset, so classify at most once
			// per subsystem even if extra namespaces are present.
			break
		}
	}
}

// deleteOrphanedShares removes shares detected by detectOrphanedShares, bounded
// by the per-run deletion cap. Each share's dataset absence is re-confirmed
// immediately before mutation so a dataset recreated after detection is never
// orphaned out from under a live volume. Cleanup is routed to the correct
// backend objects by the orphan's Protocol.
func (d *Driver) deleteOrphanedShares(ctx context.Context, report *ReconcileReport, maxPerRun int) {
	for i := range report.OrphanShares {
		orphan := &report.OrphanShares[i]
		if maxPerRun > 0 && len(report.DeletedShares) >= maxPerRun {
			break
		}
		// TOCTOU guard: re-confirm the dataset is still absent immediately before
		// mutating backend state, regardless of protocol.
		if _, getErr := d.truenasClient.DatasetGet(ctx, orphan.ID); getErr == nil || !truenas.IsNotFoundError(getErr) {
			d.recordReconcileSkip(report, "share", orphan.ID, "dataset reappeared or lookup failed before delete")
			continue
		}
		switch orphan.Protocol {
		case ShareTypeISCSI:
			d.deleteOrphanedISCSIShare(ctx, report, *orphan)
		case ShareTypeNVMeoF:
			d.deleteOrphanedNVMeoFShare(ctx, report, *orphan)
		default: // ShareTypeNFS (and any unset value) retains the legacy NFS path.
			d.deleteOrphanedNFSShare(ctx, report, *orphan)
		}
	}
}

func (d *Driver) deleteOrphanedNFSShare(ctx context.Context, report *ReconcileReport, orphan ReconcileObject) {
	shareID, err := strconv.Atoi(orphan.BackendID)
	if err != nil || shareID <= 0 {
		return
	}
	if delErr := d.truenasClient.NFSShareDelete(ctx, shareID); delErr != nil && !truenas.IsNotFoundError(delErr) {
		d.recordReconcileObjectFailure("share", orphan.BackendID, delErr)
		return
	}
	report.DeletedShares = append(report.DeletedShares, orphan.ID)
	klog.Infof("Orphan reconcile: deleted orphaned NFS share %d (dataset %s absent)", shareID, orphan.ID)
}

func (d *Driver) deleteOrphanedISCSIShare(ctx context.Context, report *ReconcileReport, orphan ReconcileObject) {
	shareName := d.iscsiShareName(orphan.SourceVolumeID)
	target, err := d.truenasClient.ISCSITargetFindByName(ctx, shareName)
	if err != nil && !truenas.IsNotFoundError(err) {
		d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("find iSCSI target %s: %w", shareName, err))
		return
	}
	extent, err := d.truenasClient.ISCSIExtentFindByName(ctx, shareName)
	if err != nil && !truenas.IsNotFoundError(err) {
		d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("find iSCSI extent %s: %w", shareName, err))
		return
	}
	// Canonical teardown also removes the per-volume fencing initiator group. The
	// dataset is gone, so resolve it by its ownership comment rather than a stored
	// property ID; sweeping must delete the same object set or one initiator group
	// leaks per swept volume.
	var initiatorGroup *truenas.ISCSIInitiator
	if d.config.Fencing.Enabled() {
		initiatorGroup, err = d.resolveFencingInitiatorGroup(ctx, nil, orphan.ID)
		if err != nil {
			d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("find iSCSI initiator group for %s: %w", orphan.ID, err))
			return
		}
	}
	if target == nil && extent == nil && initiatorGroup == nil {
		report.DeletedShares = append(report.DeletedShares, orphan.ID)
		klog.Infof("Orphan reconcile: orphaned iSCSI share for dataset %s already absent", orphan.ID)
		return
	}
	if target != nil && extent != nil {
		association, findErr := d.truenasClient.ISCSITargetExtentFind(ctx, target.ID, extent.ID)
		if findErr != nil && !truenas.IsNotFoundError(findErr) {
			d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("find iSCSI target-extent for %s: %w", shareName, findErr))
			return
		}
		if association != nil {
			if delErr := d.truenasClient.ISCSITargetExtentDelete(ctx, association.ID, true); delErr != nil && !truenas.IsNotFoundError(delErr) {
				d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("delete iSCSI target-extent %d: %w", association.ID, delErr))
				return
			}
		}
	}
	if extent != nil {
		if delErr := d.truenasClient.ISCSIExtentDelete(ctx, extent.ID, false, true); delErr != nil && !truenas.IsNotFoundError(delErr) {
			d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("delete iSCSI extent %d: %w", extent.ID, delErr))
			return
		}
	}
	if target != nil {
		if delErr := d.truenasClient.ISCSITargetDelete(ctx, target.ID, true); delErr != nil && !truenas.IsNotFoundError(delErr) {
			d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("delete iSCSI target %d: %w", target.ID, delErr))
			return
		}
	}
	if initiatorGroup != nil {
		if delErr := d.truenasClient.ISCSIInitiatorDelete(ctx, initiatorGroup.ID); delErr != nil && !truenas.IsNotFoundError(delErr) {
			d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("delete iSCSI initiator group %d: %w", initiatorGroup.ID, delErr))
			return
		}
	}
	// Best-effort debounced service reload mirrors the share create/delete path so
	// initiators stop seeing the removed target promptly.
	if d.serviceReloadDebouncer != nil {
		if reloadErr := d.serviceReloadDebouncer.RequestReload(ctx, "iscsitarget"); reloadErr != nil {
			klog.Warningf("Orphan reconcile: iSCSI service reload after sweeping %s failed (non-fatal): %v", orphan.ID, reloadErr)
		}
	}
	report.DeletedShares = append(report.DeletedShares, orphan.ID)
	klog.Infof("Orphan reconcile: deleted orphaned iSCSI share for dataset %s (name %s)", orphan.ID, shareName)
}

func (d *Driver) deleteOrphanedNVMeoFShare(ctx context.Context, report *ReconcileReport, orphan ReconcileObject) {
	subsysName := d.nvmeSubsystemName(orphan.ID)
	subsys, err := d.truenasClient.NVMeoFSubsystemFindByName(ctx, subsysName)
	if err != nil && !truenas.IsNotFoundError(err) {
		d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("find NVMe-oF subsystem %s: %w", subsysName, err))
		return
	}
	if subsys == nil {
		report.DeletedShares = append(report.DeletedShares, orphan.ID)
		klog.Infof("Orphan reconcile: orphaned NVMe-oF share for dataset %s already absent", orphan.ID)
		return
	}
	namespaces, err := d.truenasClient.NVMeoFNamespaceListBySubsystem(ctx, subsys.ID)
	if err != nil && !truenas.IsNotFoundError(err) {
		d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("list NVMe-oF namespaces for subsystem %d: %w", subsys.ID, err))
		return
	}
	for _, namespace := range namespaces {
		if namespace == nil {
			continue
		}
		if delErr := d.truenasClient.NVMeoFNamespaceDelete(ctx, namespace.ID); delErr != nil && !truenas.IsNotFoundError(delErr) {
			d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("delete NVMe-oF namespace %d: %w", namespace.ID, delErr))
			return
		}
	}
	// Canonical teardown removes the port-subsystem associations before the
	// subsystem; sweeping must do the same or the subsystem delete can fail every
	// pass (or dangle) while the association remains.
	associations, assocErr := d.truenasClient.NVMeoFPortSubsysList(ctx)
	if assocErr != nil && !truenas.IsNotFoundError(assocErr) {
		d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("list NVMe-oF port-subsystem associations for subsystem %d: %w", subsys.ID, assocErr))
		return
	}
	for _, association := range truenas.NVMeoFPortSubsysFilterBySubsystem(associations, subsys.ID) {
		if association == nil {
			continue
		}
		if delErr := d.truenasClient.NVMeoFPortSubsysDelete(ctx, association.ID); delErr != nil && !truenas.IsNotFoundError(delErr) {
			d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("delete NVMe-oF port-subsystem %d: %w", association.ID, delErr))
			return
		}
	}
	if delErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); delErr != nil && !truenas.IsNotFoundError(delErr) {
		d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("delete NVMe-oF subsystem %d: %w", subsys.ID, delErr))
		return
	}
	report.DeletedShares = append(report.DeletedShares, orphan.ID)
	klog.Infof("Orphan reconcile: deleted orphaned NVMe-oF share for dataset %s (subsystem %s)", orphan.ID, subsysName)
}
