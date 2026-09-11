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
// NFS share comment of the form "scale-csi (<driverName>): <datasetName>". The
// legacy "truenas-csi (<driverName>): " spelling written by releases before the
// namespace rename is accepted on read forever: share comments are only
// rewritten when a share is recreated, so pre-rename shares keep the old
// comment for their whole life. The boolean is false when the comment is not a
// CSI share comment for THIS driver instance, so foreign shares are never
// classified or touched.
func (d *Driver) nfsShareCommentDatasetName(comment string) (string, bool) {
	for _, prefix := range []string{"scale-csi (" + d.name + "): ", "truenas-csi (" + d.name + "): "} {
		if !strings.HasPrefix(comment, prefix) {
			continue
		}
		datasetName := strings.TrimSpace(strings.TrimPrefix(comment, prefix))
		if datasetName == "" {
			return "", false
		}
		return datasetName, true
	}
	return "", false
}

// iscsiExtentCommentDatasetName extracts the backing dataset name from a
// CSI-managed iSCSI extent comment of the form "scale-csi: <datasetName>"
// (legacy "truenas-csi: <datasetName>" comments from releases before the
// namespace rename are accepted on read forever — extent comments are only
// rewritten when an extent is recreated). Unlike the NFS share comment, the
// iSCSI extent comment does NOT embed the driver instance name, so
// driver-instance scoping is enforced separately by requiring the derived
// dataset to live under the configured parent dataset (see datasetUnderParent).
// The boolean is false when the comment is not a CSI extent comment at all, so
// foreign extents are never classified or touched.
func iscsiExtentCommentDatasetName(comment string) (string, bool) {
	for _, prefix := range []string{"scale-csi: ", "truenas-csi: "} {
		if !strings.HasPrefix(comment, prefix) {
			continue
		}
		datasetName := strings.TrimSpace(strings.TrimPrefix(comment, prefix))
		if datasetName == "" {
			return "", false
		}
		return datasetName, true
	}
	return "", false
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
		// NFS was the only protocol missing this guard. The share comment does
		// embed the driver instance name, so this is not the sole scoping check
		// the way it is for iSCSI and NVMe-oF — but the comment is attacker- and
		// operator-writable free text on a shared appliance, and a dataset path
		// outside this instance's configured parent is one this instance must
		// never sweep regardless of what the comment claims. Matches
		// detectOrphanedISCSIShares and detectOrphanedNVMeoFShares exactly.
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
//
// deletedCount is the running total ALREADY spent by deleteDetectedOrphans
// (snapshots, volumes, tombstones, spent-restores, remnants) for this pass
// (C8): shares share that ONE per-run deletion budget rather than policing
// their own separate len(report.DeletedShares) counter, which let a pass with
// maxPerRun=1000 destroy up to 1900 objects when both counters independently
// maxed out. A cap refusal here is also now recorded via recordReconcileSkip
// with deletionCapReasonPrefix, matching every other cap path — previously an
// orphaned-share backlog that exceeded the cap every night left
// scale_csi_tombstone_reap_last_skipped_on_cap at 0 forever while shares leaked.
//
// currentState is the Kubernetes state RE-LISTED immediately before mutation by
// runReconcileDeletePhase. Shares were the only delete path not given it: they
// were gated solely on the detection-time snapshot plus the dataset-absence
// re-check below. That re-check does cover the common case — a PV created after
// detection brings its dataset with it — but not a PV bound to a volume handle
// whose dataset is genuinely absent, which is exactly the anomaly
// shareOrphanLivePV exists to surface rather than sweep. The gate is a map
// lookup and can only ever make the sweep more conservative.
func (d *Driver) deleteOrphanedShares(ctx context.Context, report *ReconcileReport, currentState *kubernetesReconcileState, deletedCount, maxPerRun int) {
	for i := range report.OrphanShares {
		orphan := &report.OrphanShares[i]
		if maxPerRun > 0 && deletedCount >= maxPerRun {
			d.recordReconcileSkip(
				report,
				"share",
				orphan.ID,
				fmt.Sprintf("%s (maxPerRun=%d)", deletionCapReasonPrefix, maxPerRun),
			)
			continue
		}
		// TOCTOU guard: a PersistentVolume that appeared since detection makes
		// this share live again. An absent dataset under a live PV is anomalous
		// and must be surfaced, never "fixed" by deleting the share.
		if shareOrphanLivePV(currentState, orphan.SourceVolumeID) {
			d.recordReconcileSkip(report, "share", orphan.ID, "a live PersistentVolume appeared for this volume before delete")
			continue
		}
		// TOCTOU guard: re-confirm the dataset is still absent immediately before
		// mutating backend state, regardless of protocol.
		if _, getErr := d.truenasClient.DatasetGet(ctx, orphan.ID); getErr == nil || !truenas.IsNotFoundError(getErr) {
			d.recordReconcileSkip(report, "share", orphan.ID, "dataset reappeared or lookup failed before delete")
			continue
		}
		before := len(report.DeletedShares)
		switch orphan.Protocol {
		case ShareTypeISCSI:
			d.deleteOrphanedISCSIShare(ctx, report, *orphan)
		case ShareTypeNVMeoF:
			d.deleteOrphanedNVMeoFShare(ctx, report, *orphan)
		default: // ShareTypeNFS (and any unset value) retains the legacy NFS path.
			d.deleteOrphanedNFSShare(ctx, report, *orphan)
		}
		if len(report.DeletedShares) > before {
			deletedCount++
		}
	}
}

func (d *Driver) deleteOrphanedNFSShare(ctx context.Context, report *ReconcileReport, orphan ReconcileObject) {
	shareID, err := strconv.Atoi(orphan.BackendID)
	if err != nil || shareID <= 0 {
		// (C8) Match the iSCSI/NVMe-oF siblings: every failure is recorded, none
		// return silently. A malformed BackendID here previously left no log, no
		// skip, and no object failure — the object simply vanished from the pass
		// with nothing to show for it.
		if err == nil {
			err = fmt.Errorf("non-positive share ID %d", shareID)
		}
		d.recordReconcileObjectFailure("share", orphan.BackendID, fmt.Errorf("parse NFS share ID %q: %w", orphan.BackendID, err))
		return
	}
	if delErr := d.truenasClient.NFSShareDelete(ctx, shareID); delErr != nil && !truenas.IsNotFoundError(delErr) {
		d.recordReconcileObjectFailure("share", orphan.BackendID, delErr)
		return
	}
	report.DeletedShares = append(report.DeletedShares, orphan.ID)
	klog.Infof("Orphan reconcile: deleted orphaned NFS share %d (dataset %s absent)", shareID, orphan.ID)
}

// resolveOrphanISCSIExtent resolves the extent the CLASSIFIER saw, by the ID it
// recorded in orphan.BackendID, rather than re-deriving a name from config.
//
// The deleter used to look the extent up by iscsiShareName(SourceVolumeID). That
// name is a function of iscsi.nameSuffix, so any change to that config between
// the sweep that created the object and the sweep that deletes it resolved
// NOTHING — and the deleter then logged "already absent", appended the orphan to
// DeletedShares as a success, and recorded no failure metric, while the real
// orphan it had just classified from the live listing leaked forever with
// nothing in logs or metrics to show for it. BackendID came from that listing
// and is immune to config drift.
//
// The name lookup survives only as the fallback for an orphan carrying no
// usable BackendID.
func (d *Driver) resolveOrphanISCSIExtent(ctx context.Context, orphan ReconcileObject, shareName string) (*truenas.ISCSIExtent, error) {
	if extentID, err := strconv.Atoi(orphan.BackendID); err == nil && extentID > 0 {
		// The by-ID getters report absence as a "not found" error rather than a
		// nil result; an absent classified ID means the extent is genuinely gone.
		extent, getErr := d.truenasClient.ISCSIExtentGet(ctx, extentID)
		if getErr != nil {
			if truenas.IsNotFoundError(getErr) {
				return nil, nil
			}
			return nil, fmt.Errorf("get iSCSI extent %d: %w", extentID, getErr)
		}
		return extent, nil
	}
	extent, findErr := d.truenasClient.ISCSIExtentFindByName(ctx, shareName)
	if findErr != nil && !truenas.IsNotFoundError(findErr) {
		return nil, fmt.Errorf("find iSCSI extent %s: %w", shareName, findErr)
	}
	return extent, nil
}

func (d *Driver) deleteOrphanedISCSIShare(ctx context.Context, report *ReconcileReport, orphan ReconcileObject) {
	shareName := d.iscsiShareName(orphan.SourceVolumeID)
	extent, err := d.resolveOrphanISCSIExtent(ctx, orphan, shareName)
	if err != nil {
		d.recordReconcileObjectFailure("share", orphan.ID, err)
		return
	}
	// Reach the target through the target-extent ASSOCIATION when the extent
	// resolved: that relationship is stored on the appliance and, like BackendID,
	// survives a nameSuffix change. The derived-name lookup is only the fallback
	// for an extent that is already gone.
	var target *truenas.ISCSITarget
	var association *truenas.ISCSITargetExtent
	if extent != nil {
		associations, findErr := d.truenasClient.ISCSITargetExtentFindByExtent(ctx, extent.ID)
		if findErr != nil && !truenas.IsNotFoundError(findErr) {
			d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("find iSCSI target-extent for extent %d: %w", extent.ID, findErr))
			return
		}
		for _, candidate := range associations {
			if candidate != nil {
				association = candidate
				break
			}
		}
	}
	if association != nil {
		target, err = d.truenasClient.ISCSITargetGet(ctx, association.Target)
		if err != nil && !truenas.IsNotFoundError(err) {
			d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("get iSCSI target %d: %w", association.Target, err))
			return
		}
	}
	if target == nil {
		target, err = d.truenasClient.ISCSITargetFindByName(ctx, shareName)
		if err != nil && !truenas.IsNotFoundError(err) {
			d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("find iSCSI target %s: %w", shareName, err))
			return
		}
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
	if association != nil {
		if delErr := d.truenasClient.ISCSITargetExtentDelete(ctx, association.ID, true); delErr != nil && !truenas.IsNotFoundError(delErr) {
			d.recordReconcileObjectFailure("share", orphan.ID, fmt.Errorf("delete iSCSI target-extent %d: %w", association.ID, delErr))
			return
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

// resolveOrphanNVMeoFSubsystem is the NVMe-oF counterpart of
// resolveOrphanISCSIExtent: use the subsystem ID the classifier recorded in
// orphan.BackendID instead of re-deriving nvmeSubsystemName, which depends on
// both nvmeof.namePrefix and nvmeof.nameSuffix. See that function for the
// false-"already absent" leak this closes.
func (d *Driver) resolveOrphanNVMeoFSubsystem(ctx context.Context, orphan ReconcileObject, subsysName string) (*truenas.NVMeoFSubsystem, error) {
	if subsysID, err := strconv.Atoi(orphan.BackendID); err == nil && subsysID > 0 {
		subsys, getErr := d.truenasClient.NVMeoFSubsystemGet(ctx, subsysID)
		if getErr != nil {
			if truenas.IsNotFoundError(getErr) {
				return nil, nil
			}
			return nil, fmt.Errorf("get NVMe-oF subsystem %d: %w", subsysID, getErr)
		}
		return subsys, nil
	}
	subsys, findErr := d.truenasClient.NVMeoFSubsystemFindByName(ctx, subsysName)
	if findErr != nil && !truenas.IsNotFoundError(findErr) {
		return nil, fmt.Errorf("find NVMe-oF subsystem %s: %w", subsysName, findErr)
	}
	return subsys, nil
}

func (d *Driver) deleteOrphanedNVMeoFShare(ctx context.Context, report *ReconcileReport, orphan ReconcileObject) {
	subsysName := d.nvmeSubsystemName(orphan.ID)
	subsys, err := d.resolveOrphanNVMeoFSubsystem(ctx, orphan, subsysName)
	if err != nil {
		d.recordReconcileObjectFailure("share", orphan.ID, err)
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
