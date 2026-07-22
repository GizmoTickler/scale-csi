package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

const (
	reconcileListPageSize                    = 100
	staleRecordMassAbsenceThreshold          = 2
	defaultControllerReconcileInterval       = time.Hour
	defaultControllerReconcileMinOrphanAge   = 24 * time.Hour
	replicationJobReasonMissingMarker        = "missing_marker"
	replicationJobReasonMissingSourceDataset = "missing_source_dataset"
)

var (
	volumeSnapshotContentGVR = schema.GroupVersionResource{
		Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents",
	}
	volumeSnapshotGVR = schema.GroupVersionResource{
		Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots",
	}
)

// ReconcileOptions controls one orphan reconcile pass.
type ReconcileOptions struct {
	// Delete enables guarded cleanup. It must only be set by the separately
	// gated run-once entrypoint; the periodic controller pass always uses false.
	Delete bool

	// MinOrphanAge prevents in-flight or rebuild-transient resources from being
	// classified for cleanup. A non-positive value falls back to the config.
	MinOrphanAge time.Duration
}

// ReconcileObject describes a managed backend resource detected as orphaned.
type ReconcileObject struct {
	ID             string
	BackendID      string
	KubernetesName string
	PVName         string
	SourceVolumeID string
	CreatedAt      time.Time
	Age            time.Duration
	Bytes          int64
}

// SpentRestoreSnapshot describes a VolSync restore-destination snapshot whose
// intermediate source PVC is no longer Bound.
type SpentRestoreSnapshot struct {
	Namespace           string
	Name                string
	ContentName         string
	SnapshotHandle      string
	SourcePVC           string
	SourcePVCPhase      corev1.PersistentVolumeClaimPhase
	CreationTime        time.Time
	Age                 time.Duration
	BackendSnapshotID   string
	ClassifiedAt        time.Time
	SourcePVCWasMissing bool
}

// ReconcileActionFailure records a guarded cleanup that was skipped or refused.
type ReconcileActionFailure struct {
	Kind   string
	ID     string
	Reason string
}

// ReconcileReport is the complete detection and optional cleanup result.
type ReconcileReport struct {
	OrphanVolumeCount          int
	OrphanSnapshotCount        int
	OrphanShareCount           int
	SpentRestoreSnapshotCount  int
	TombstoneSnapshotCount     int
	OrphanVolumeBytes          int64
	OrphanSnapshotBytes        int64
	TombstoneSnapshotBytes     int64
	OrphanVolumes              []ReconcileObject
	OrphanSnapshots            []ReconcileObject
	OrphanShares               []ReconcileObject
	SpentRestoreSnapshots      []SpentRestoreSnapshot
	TombstoneSnapshots         []ReconcileObject
	DeletedVolumes             []string
	DeletedSnapshots           []string
	DeletedShares              []string
	DeletedSpentRestoreObjects []string
	DeletedTombstones          []string
	SkippedDeletes             []ReconcileActionFailure
	DeleteEnabled              bool
}

type snapshotContentState struct {
	name           string
	snapshotHandle string
}

type kubernetesReconcileState struct {
	volumeHandles                  map[string]struct{}
	volumeHandlesByPV              map[string]string
	liveVolumeAttachments          map[string]struct{}
	volumeAttachmentCount          int
	snapshotHandles                map[string]struct{}
	handlelessSnapshotContentNames []string
	snapshotContentsByRef          map[string]snapshotContentState
	snapshotContentsByName         map[string]snapshotContentState
	volumeSnapshots                []unstructured.Unstructured
	pvcs                           map[string]*corev1.PersistentVolumeClaim
}

type stalePublicationObservation struct {
	FirstMissing time.Time
	UpdatedAt    string
	State        string
	EncodedID    string
}

func newStalePublicationObservation(now time.Time, record publicationRecord) stalePublicationObservation {
	return stalePublicationObservation{
		FirstMissing: now,
		UpdatedAt:    record.UpdatedAt,
		State:        record.State,
		EncodedID:    record.EncodedID,
	}
}

func (observation stalePublicationObservation) matches(record publicationRecord) bool {
	return observation.UpdatedAt == record.UpdatedAt &&
		observation.State == record.State &&
		observation.EncodedID == record.EncodedID
}

// ReconcileOrphans detects managed TrueNAS resources that no longer have a
// matching Kubernetes object. Backend deletion, when explicitly enabled, is
// routed exclusively through the existing guarded CSI delete methods.
func (d *Driver) ReconcileOrphans(ctx context.Context, opts ReconcileOptions) (ReconcileReport, error) {
	// The exported run-once path is also used by the chart's orphan-GC CronJob.
	// It must never become a second fencing writer beside the live controller;
	// stale publication revocation is exclusive to the controller loop below.
	d.runReplicationJobSweep(ctx)
	return d.reconcileOrphans(ctx, opts, false)
}

func (d *Driver) reconcileOrphans(ctx context.Context, opts ReconcileOptions, reconcileStalePublications bool) (report ReconcileReport, retErr error) {
	report = ReconcileReport{DeleteEnabled: opts.Delete}
	defer func() {
		sort.Slice(report.OrphanVolumes, func(i, j int) bool { return report.OrphanVolumes[i].ID < report.OrphanVolumes[j].ID })
		sort.Slice(report.OrphanSnapshots, func(i, j int) bool { return report.OrphanSnapshots[i].ID < report.OrphanSnapshots[j].ID })
		sort.Slice(report.SpentRestoreSnapshots, func(i, j int) bool {
			left := report.SpentRestoreSnapshots[i].Namespace + "/" + report.SpentRestoreSnapshots[i].Name
			right := report.SpentRestoreSnapshots[j].Namespace + "/" + report.SpentRestoreSnapshots[j].Name
			return left < right
		})
		sort.Slice(report.TombstoneSnapshots, func(i, j int) bool { return report.TombstoneSnapshots[i].ID < report.TombstoneSnapshots[j].ID })
		report.OrphanVolumeCount = len(report.OrphanVolumes)
		report.OrphanSnapshotCount = len(report.OrphanSnapshots)
		report.SpentRestoreSnapshotCount = len(report.SpentRestoreSnapshots)
		report.TombstoneSnapshotCount = len(report.TombstoneSnapshots)
		// Publish even a partial pass so a single malformed object cannot freeze
		// the last visible inventory indefinitely.
		SetOrphanReconcileMetrics(report)
		if retErr == nil {
			RecordReconcileSuccess(time.Now())
		}
	}()
	if d.config == nil || d.truenasClient == nil {
		RecordReconcileFailure("configuration")
		return report, fmt.Errorf("driver configuration and TrueNAS client are required")
	}
	minOrphanAge, err := d.reconcileMinOrphanAge(opts.MinOrphanAge)
	if err != nil {
		RecordReconcileFailure("configuration")
		return report, err
	}

	datasets, err := d.listAllManagedDatasets(ctx)
	if err != nil {
		RecordReconcileFailure("list_backend_volumes")
		return report, fmt.Errorf("list managed backend volumes: %w", err)
	}
	snapshots, tombstones, err := d.listAllManagedSnapshots(ctx)
	if err != nil {
		RecordReconcileFailure("list_backend_snapshots")
		return report, fmt.Errorf("list managed backend snapshots: %w", err)
	}
	kubeState, err := d.loadKubernetesReconcileState(ctx)
	if err != nil {
		RecordReconcileFailure("load_kubernetes_state")
		return report, err
	}
	if reconcileStalePublications && d.config.Fencing.Enabled() {
		d.reconcileStalePublicationRecords(ctx, datasets, kubeState, time.Now())
	}

	// The parent dataset carries the driver's durable bookkeeping: in-flight
	// creation markers and the tombstone ledger. Reading it can fail without
	// aborting the pass — tombstone reaping then simply stays empty (fail-safe;
	// no ledger, no reaping) and the sweeps are skipped.
	var ledger map[string]tombstoneLedgerEntry
	parentDataset, parentErr := d.truenasClient.DatasetGet(ctx, d.parentDatasetName())
	if parentErr != nil {
		d.recordReconcileObjectFailure("parent_bookkeeping", d.parentDatasetName(), parentErr)
	} else {
		ledger = tombstoneLedgerFromDataset(parentDataset)
		// Bookkeeping hygiene runs on every pass regardless of opts.Delete: these
		// properties are driver-internal provenance records, not user data, and
		// each removal requires proof of staleness gathered live below.
		d.sweepStaleInflightMarkers(ctx, parentDataset, time.Now(), minOrphanAge)
		d.sweepOrphanedTombstoneLedger(ctx, ledger, time.Now(), minOrphanAge)
	}

	now := time.Now()
	managedBackendVolumeCount := 0
	for _, ds := range datasets {
		if ds == nil || ds.UserProperties[PropManagedResource].Value != "true" {
			continue
		}
		managedBackendVolumeCount++
		volumeID := path.Base(ds.Name)
		if _, live := kubeState.volumeHandles[volumeID]; live {
			continue
		}
		createdAt, age, eligible := reconcileAge(now, ds.GetCreationTime(), minOrphanAge)
		if !eligible {
			if ds.GetCreationTime() <= 0 {
				klog.Warningf("Orphan reconcile: skipping managed volume %s because its creation time is unavailable", ds.Name)
			}
			continue
		}
		pvName := ds.UserProperties[PropCSIVolumeName].Value
		if pvName == "" || pvName == "-" {
			pvName = volumeID
		}
		item := ReconcileObject{
			ID:        volumeID,
			BackendID: ds.Name,
			PVName:    pvName,
			CreatedAt: createdAt,
			Age:       age,
			Bytes:     ds.GetUsedBytes(),
		}
		report.OrphanVolumes = append(report.OrphanVolumes, item)
		report.OrphanVolumeBytes += item.Bytes
	}

	managedBackendSnapshotCount := 0
	for _, snap := range snapshots {
		if !isCSISnapshot(snap) {
			continue
		}
		managedBackendSnapshotCount++
		snapshotHandle, ok := reconcileSnapshotHandle(snap)
		if !ok {
			klog.Warningf("Orphan reconcile: skipping managed snapshot %s because its CSI handle cannot be derived", snap.ID)
			continue
		}
		if _, live := kubeState.snapshotHandles[snapshotHandle]; live {
			continue
		}
		createdAt, age, eligible := reconcileAge(now, snap.GetCreationTime(), minOrphanAge)
		if !eligible {
			if snap.GetCreationTime() <= 0 {
				klog.Warningf("Orphan reconcile: skipping managed snapshot %s because its creation time is unavailable", snap.ID)
			}
			continue
		}
		sourceVolumeID := snap.UserProperties[PropCSISnapshotSourceVolumeID].Value
		if sourceVolumeID == "" || sourceVolumeID == "-" {
			sourceVolumeID = path.Base(snap.Dataset)
		}
		item := ReconcileObject{
			ID:             snapshotHandle,
			BackendID:      snap.ID,
			KubernetesName: snap.UserProperties[PropCSISnapshotName].Value,
			SourceVolumeID: sourceVolumeID,
			CreatedAt:      createdAt,
			Age:            age,
			Bytes:          snap.GetSnapshotSize(),
		}
		report.OrphanSnapshots = append(report.OrphanSnapshots, item)
		report.OrphanSnapshotBytes += item.Bytes
	}

	// Tombstone-named snapshots are the driver's own deferred-delete markers. On
	// backends without ZFS deferred destroy (TrueNAS 26.0) they cannot be removed
	// until their last restored clone is gone, and the tombstone rename released
	// their CSI identity, so the CSI-snapshot orphan pass above never sees them.
	// Classification requires a matching parent-dataset ledger entry: the name
	// shape alone is NOT provenance — a user snapshot may legitimately be named
	// *-csi-deleted-<n> and must never be counted, reaped, or even reported.
	for _, snap := range tombstones {
		entry, recorded := ledger[tombstoneLedgerKey(snap.ID)]
		if !recorded || entry.Snapshot != snap.ID {
			continue
		}
		// The ledger's recorded immutable creation time must match the observed
		// snapshot: a stale entry never authorizes classifying (or later reaping)
		// a DIFFERENT object recreated at the same full ID.
		if entry.CreatedAt <= 0 || entry.CreatedAt != snap.GetCreationTime() {
			continue
		}
		if snapshotIsLiveCSIObjectWithTombstoneShapedName(snap) {
			klog.Warningf("Orphan reconcile: skipping %s — it carries live CSI snapshot identity despite a tombstone-shaped name and ledger entry", snap.ID)
			continue
		}
		createdAt, age, eligible := reconcileAge(now, snap.GetCreationTime(), minOrphanAge)
		if !eligible {
			if snap.GetCreationTime() <= 0 {
				klog.Warningf("Orphan reconcile: skipping tombstone snapshot %s because its creation time is unavailable", snap.ID)
			}
			continue
		}
		sourceVolumeID := ""
		if snap.Dataset != "" {
			sourceVolumeID = path.Base(snap.Dataset)
		}
		item := ReconcileObject{
			ID:             snap.ID,
			BackendID:      snap.ID,
			SourceVolumeID: sourceVolumeID,
			CreatedAt:      createdAt,
			Age:            age,
			Bytes:          snap.GetSnapshotSize(),
		}
		report.TombstoneSnapshots = append(report.TombstoneSnapshots, item)
		report.TombstoneSnapshotBytes += item.Bytes
	}

	// Spent-restore classification always runs (read-only detection). It is NOT
	// gated on the global zfs.detachedVolumesFromSnapshots flag: a StorageClass
	// may opt into snapshotRestoreMode=detached while that global default stays
	// clone, and gating on the global flag alone would leak that class's spent
	// volsync restore snapshots (never reaped). Deletion remains gated by
	// opts.Delete and the per-object revalidation in deleteDetectedOrphans.
	report.SpentRestoreSnapshots = d.classifySpentRestoreSnapshots(ctx, now, kubeState)
	if ctxErr := ctx.Err(); ctxErr != nil {
		RecordReconcileFailure("spent_restore_classification")
		return report, ctxErr
	}

	// Orphaned-share detection (a share whose dataset is gone) always runs so the
	// residue is visible even in dry-run; deletion stays gated by opts.Delete.
	d.detectOrphanedShares(ctx, kubeState, &report)
	if ctxErr := ctx.Err(); ctxErr != nil {
		RecordReconcileFailure("orphan_share_detection")
		return report, ctxErr
	}

	logAction := "[DRY RUN] would delete"
	if opts.Delete {
		logAction = "will attempt guarded delete of"
	}
	for _, orphan := range report.OrphanSnapshots {
		klog.Infof("Orphan reconcile: %s managed snapshot %s (backend=%s age=%v bytes=%d)",
			logAction, orphan.ID, orphan.BackendID, orphan.Age, orphan.Bytes)
	}
	for _, orphan := range report.OrphanVolumes {
		klog.Infof("Orphan reconcile: %s managed volume %s (backend=%s pv=%s age=%v bytes=%d)",
			logAction, orphan.ID, orphan.BackendID, orphan.PVName, orphan.Age, orphan.Bytes)
	}
	for _, orphan := range report.OrphanShares {
		klog.Infof("Orphan reconcile: %s orphaned share %s (backend=%s volume=%s)",
			logAction, orphan.ID, orphan.BackendID, orphan.SourceVolumeID)
	}
	for _, tombstone := range report.TombstoneSnapshots {
		klog.Infof("Orphan reconcile: %s released deferred-delete tombstone %s (age=%v)",
			logAction, tombstone.ID, tombstone.Age)
	}
	for i := range report.SpentRestoreSnapshots {
		spent := &report.SpentRestoreSnapshots[i]
		spentAction := logAction
		if spent.Age <= minOrphanAge {
			spentAction = "classified (creation-age gate not yet met):"
		}
		klog.Infof("Orphan reconcile: %s spent restore VolumeSnapshot %s/%s (content=%s sourcePVC=%s phase=%s)",
			spentAction, spent.Namespace, spent.Name, spent.ContentName, spent.SourcePVC, spent.SourcePVCPhase)
	}

	if !opts.Delete {
		return report, nil
	}
	if len(kubeState.volumeHandles) == 0 && managedBackendVolumeCount > 0 {
		RecordReconcileFailure("safety_brake")
		return report, fmt.Errorf(
			"refusing to GC: zero live PVs for driver but %d managed backend volumes exist — cluster rebuild in progress?",
			managedBackendVolumeCount,
		)
	}
	snapshotDeleteBlockReason := snapshotDeletePassBlockReason(kubeState, managedBackendSnapshotCount)

	// Re-list Kubernetes state immediately before mutation so a newly created PV
	// or snapshot binding cannot be deleted using a stale detection snapshot.
	currentState, err := d.loadKubernetesReconcileState(ctx)
	if err != nil {
		RecordReconcileFailure("revalidate_kubernetes_state")
		return report, fmt.Errorf("revalidate Kubernetes state before delete: %w", err)
	}
	if len(currentState.volumeHandles) == 0 && managedBackendVolumeCount > 0 {
		RecordReconcileFailure("safety_brake")
		return report, fmt.Errorf(
			"refusing to GC: zero live PVs for driver but %d managed backend volumes exist — cluster rebuild in progress?",
			managedBackendVolumeCount,
		)
	}
	if reason := snapshotDeletePassBlockReason(currentState, managedBackendSnapshotCount); reason != "" {
		snapshotDeleteBlockReason = reason
	}
	if err := d.deleteDetectedOrphans(
		ctx,
		&report,
		currentState,
		minOrphanAge,
		d.config.Reconcile.Delete.MaxPerRun,
		snapshotDeleteBlockReason,
	); err != nil {
		RecordReconcileFailure("delete")
		return report, err
	}
	d.deleteOrphanedShares(ctx, &report, d.config.Reconcile.Delete.MaxPerRun)
	return report, nil
}

func reconcileSnapshotHandle(snapshot *truenas.Snapshot) (string, bool) {
	if snapshot == nil {
		return "", false
	}
	if snapshotHandle, ok := extractSnapshotName(snapshot.ID); ok {
		return snapshotHandle, true
	}
	if snapshot.Name != "" {
		return snapshot.Name, true
	}
	return "", false
}

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

// detectOrphanedShares finds CSI-managed backend shares whose backing dataset is
// confirmed absent. DeleteVolume removes the share before the dataset, so a share
// that outlives its dataset is residue from an interrupted delete; sweeping it
// keeps that residue from being silently permanent. A share still referenced by a
// live PersistentVolume is never classified: an absent dataset under a live PV is
// anomalous and must be surfaced, not "fixed" by deleting the share. Detection is
// read-only; deletion happens in the guarded delete phase.
func (d *Driver) detectOrphanedShares(ctx context.Context, kubeState *kubernetesReconcileState, report *ReconcileReport) {
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
		})
	}
	sort.Slice(report.OrphanShares, func(i, j int) bool { return report.OrphanShares[i].ID < report.OrphanShares[j].ID })
	report.OrphanShareCount = len(report.OrphanShares)
}

// deleteOrphanedShares removes shares detected by detectOrphanedShares, bounded
// by the per-run deletion cap. Each share's dataset absence is re-confirmed
// immediately before mutation so a dataset recreated after detection is never
// orphaned out from under a live volume.
func (d *Driver) deleteOrphanedShares(ctx context.Context, report *ReconcileReport, maxPerRun int) {
	for _, orphan := range report.OrphanShares {
		if maxPerRun > 0 && len(report.DeletedShares) >= maxPerRun {
			break
		}
		shareID, err := strconv.Atoi(orphan.BackendID)
		if err != nil || shareID <= 0 {
			continue
		}
		if _, getErr := d.truenasClient.DatasetGet(ctx, orphan.ID); getErr == nil || !truenas.IsNotFoundError(getErr) {
			d.recordReconcileSkip(report, "share", orphan.BackendID, "dataset reappeared or lookup failed before delete")
			continue
		}
		if delErr := d.truenasClient.NFSShareDelete(ctx, shareID); delErr != nil && !truenas.IsNotFoundError(delErr) {
			d.recordReconcileObjectFailure("share", orphan.BackendID, delErr)
			continue
		}
		report.DeletedShares = append(report.DeletedShares, orphan.ID)
		klog.Infof("Orphan reconcile: deleted orphaned NFS share %d (dataset %s absent)", shareID, orphan.ID)
	}
}

func snapshotDeletePassBlockReason(state *kubernetesReconcileState, managedBackendSnapshotCount int) string {
	if state == nil {
		return "Kubernetes snapshot state is unavailable"
	}
	if len(state.handlelessSnapshotContentNames) > 0 {
		return fmt.Sprintf(
			"%d driver VolumeSnapshotContents have no readable status.snapshotHandle",
			len(state.handlelessSnapshotContentNames),
		)
	}
	if len(state.snapshotHandles) == 0 && managedBackendSnapshotCount > 0 {
		return fmt.Sprintf(
			"zero live VolumeSnapshotContents for driver but %d managed backend snapshots exist — cluster rebuild in progress?",
			managedBackendSnapshotCount,
		)
	}
	return ""
}

func (d *Driver) reconcileMinOrphanAge(requested time.Duration) (time.Duration, error) {
	if requested > 0 {
		return requested, nil
	}
	if d.config == nil {
		return 0, fmt.Errorf("driver config is unavailable")
	}
	minAge, err := d.config.Reconcile.MinOrphanAgeDuration()
	if err != nil || minAge <= 0 {
		if err != nil {
			return 0, fmt.Errorf("parse reconcile minimum orphan age: %w", err)
		}
		return 0, fmt.Errorf("reconcile minimum orphan age must be positive")
	}
	return minAge, nil
}

func reconcileAge(now time.Time, creationUnix int64, minAge time.Duration) (time.Time, time.Duration, bool) {
	if creationUnix <= 0 {
		return time.Time{}, 0, false
	}
	createdAt := time.Unix(creationUnix, 0)
	age := now.Sub(createdAt)
	return createdAt, age, age > minAge
}

// listAllManagedDatasets returns every CSI-managed dataset below the configured
// parent. On TrueNAS 26.0 it prefers the path-scoped zfs.resource.query read
// (DatasetQueryByParent), which avoids pool.dataset.query's full-system
// user-property materialization, and filters the managed_resource user property
// client-side. If the resource API is unavailable OR the call errors, it falls
// back to the paginated pool.dataset.query loop so reconciliation never fails on
// the migration. See the LIVE-PROBE GATE on DatasetQueryByParent: the resource
// shape is modeled, not yet live-verified, and this fallback is what makes the
// migration safe to ship behind detection.
func (d *Driver) listAllManagedDatasets(ctx context.Context) ([]*truenas.Dataset, error) {
	resourceDatasets, err := d.truenasClient.DatasetQueryByParent(ctx, d.config.ZFS.DatasetParentName)
	if err == nil {
		managed := make([]*truenas.Dataset, 0, len(resourceDatasets))
		for _, ds := range resourceDatasets {
			if datasetUserProperty(ds, PropManagedResource) == "true" {
				managed = append(managed, ds)
			}
		}
		return managed, nil
	}
	klog.Warningf("Managed-dataset listing via zfs.resource.query failed; falling back to pool.dataset.query: %v", err)
	return d.listAllManagedDatasetsPaged(ctx)
}

// listAllManagedDatasetsPaged is the legacy paginated pool.dataset.query path,
// retained as the safe fallback for listAllManagedDatasets.
func (d *Driver) listAllManagedDatasetsPaged(ctx context.Context) ([]*truenas.Dataset, error) {
	var datasets []*truenas.Dataset
	for offset := 0; ; offset += reconcileListPageSize {
		page, err := d.truenasClient.DatasetList(ctx, d.config.ZFS.DatasetParentName, reconcileListPageSize, offset)
		if err != nil {
			return nil, err
		}
		datasets = append(datasets, page...)
		if len(page) < reconcileListPageSize {
			return datasets, nil
		}
	}
}

// listAllManagedSnapshots pages the backend once and partitions the parent's
// snapshots into live CSI-managed snapshots and the driver's own tombstone-named
// deferred-delete markers. Tombstones are never CSI snapshots (their identity is
// stripped at rename), so they must be gathered separately for GC.
func (d *Driver) listAllManagedSnapshots(ctx context.Context) (managed, tombstones []*truenas.Snapshot, err error) {
	for offset := 0; ; offset += reconcileListPageSize {
		page, err := d.truenasClient.SnapshotListAll(ctx, d.config.ZFS.DatasetParentName, reconcileListPageSize, offset)
		if err != nil {
			return nil, nil, err
		}
		for _, snap := range page {
			switch {
			case isCSISnapshot(snap):
				managed = append(managed, snap)
			case isSnapshotTombstone(snap):
				tombstones = append(tombstones, snap)
			}
		}
		if len(page) < reconcileListPageSize {
			return managed, tombstones, nil
		}
	}
}

func (d *Driver) kubernetesReconcileClients() (
	clientset kubernetes.Interface,
	dynamicClient dynamic.Interface,
	err error,
) {
	if d.eventRecorder == nil || d.eventRecorder.clientset == nil || d.eventRecorder.dynamicClient == nil {
		return nil, nil, fmt.Errorf("kubernetes clients are unavailable; orphan reconcile requires in-cluster client access")
	}
	return d.eventRecorder.clientset, d.eventRecorder.dynamicClient, nil
}

func (d *Driver) loadKubernetesReconcileState(ctx context.Context) (*kubernetesReconcileState, error) {
	clientset, dynamicClient, err := d.kubernetesReconcileClients()
	if err != nil {
		return nil, err
	}
	state := &kubernetesReconcileState{
		volumeHandles:          make(map[string]struct{}),
		volumeHandlesByPV:      make(map[string]string),
		liveVolumeAttachments:  make(map[string]struct{}),
		snapshotHandles:        make(map[string]struct{}),
		snapshotContentsByRef:  make(map[string]snapshotContentState),
		snapshotContentsByName: make(map[string]snapshotContentState),
		pvcs:                   make(map[string]*corev1.PersistentVolumeClaim),
	}

	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list PersistentVolumes: %w", err)
	}
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.name && pv.Spec.CSI.VolumeHandle != "" {
			state.volumeHandles[pv.Spec.CSI.VolumeHandle] = struct{}{}
			state.volumeHandlesByPV[pv.Name] = pv.Spec.CSI.VolumeHandle
		}
	}
	if d.config.Fencing.Enabled() {
		attachments, listErr := clientset.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
		if listErr != nil {
			return nil, fmt.Errorf("list VolumeAttachments: %w", listErr)
		}
		for i := range attachments.Items {
			attachment := &attachments.Items[i]
			if attachment.Spec.Attacher != d.name || attachment.Spec.Source.PersistentVolumeName == nil {
				continue
			}
			state.volumeAttachmentCount++
			pvName := *attachment.Spec.Source.PersistentVolumeName
			volumeHandle := state.volumeHandlesByPV[pvName]
			if volumeHandle == "" {
				// CSI PV names normally equal CreateVolumeRequest.name. This fallback
				// makes a temporarily missing PV object a conservative live grant.
				volumeHandle = pvName
			}
			state.liveVolumeAttachments[volumeAttachmentKey(volumeHandle, attachment.Spec.NodeName)] = struct{}{}
		}
	}

	contents, err := dynamicClient.Resource(volumeSnapshotContentGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list VolumeSnapshotContents: %w", err)
	}
	for i := range contents.Items {
		content := &contents.Items[i]
		driverName, found, nestedErr := unstructured.NestedString(content.Object, "spec", "driver")
		if nestedErr != nil {
			d.recordReconcileObjectFailure("snapshot_content_classification", content.GetName(), nestedErr)
			continue
		}
		if !found {
			continue
		}
		if driverName != d.name {
			continue
		}
		handle, found, nestedErr := unstructured.NestedString(content.Object, "status", "snapshotHandle")
		if nestedErr != nil || !found || handle == "" {
			klog.Warningf(
				"Orphan reconcile: skipping driver VolumeSnapshotContent %s because status.snapshotHandle is unavailable",
				content.GetName(),
			)
			state.handlelessSnapshotContentNames = append(state.handlelessSnapshotContentNames, content.GetName())
			continue
		}
		state.snapshotHandles[handle] = struct{}{}
		namespace, _, namespaceErr := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "namespace")
		name, _, nameErr := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "name")
		if namespaceErr != nil || nameErr != nil {
			nestedErr := namespaceErr
			if nestedErr == nil {
				nestedErr = nameErr
			}
			d.recordReconcileObjectFailure("snapshot_content_classification", content.GetName(), nestedErr)
			continue
		}
		contentState := snapshotContentState{
			name: content.GetName(), snapshotHandle: handle,
		}
		state.snapshotContentsByName[content.GetName()] = contentState
		if namespace != "" && name != "" {
			state.snapshotContentsByRef[namespacedName(namespace, name)] = contentState
		}
	}

	// VolumeSnapshots and PVCs are always loaded so spent-restore classification
	// can run regardless of the global detached flag (see reconcileOrphans).
	volumeSnapshots, err := dynamicClient.Resource(volumeSnapshotGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list VolumeSnapshots: %w", err)
	}
	state.volumeSnapshots = volumeSnapshots.Items
	pvcs, err := clientset.CoreV1().PersistentVolumeClaims(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list PersistentVolumeClaims: %w", err)
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		state.pvcs[namespacedName(pvc.Namespace, pvc.Name)] = pvc
	}
	return state, nil
}

func (d *Driver) classifySpentRestoreSnapshots(
	ctx context.Context,
	now time.Time,
	state *kubernetesReconcileState,
) []SpentRestoreSnapshot {
	if state == nil {
		return nil
	}
	spent := make([]SpentRestoreSnapshot, 0)
	for i := range state.volumeSnapshots {
		snapshot := &state.volumeSnapshots[i]
		matched, _ := path.Match("volsync-*-dst-dest*", snapshot.GetName())
		if !matched {
			continue
		}
		content, ok := state.snapshotContentsByRef[namespacedName(snapshot.GetNamespace(), snapshot.GetName())]
		if !ok {
			boundContent, found, nestedErr := unstructured.NestedString(snapshot.Object, "status", "boundVolumeSnapshotContentName")
			if nestedErr != nil {
				d.recordReconcileObjectFailure("spent_restore_classification", snapshot.GetNamespace()+"/"+snapshot.GetName(), nestedErr)
				continue
			}
			if !found || boundContent == "" {
				continue
			}
			content, ok = state.snapshotContentsByName[boundContent]
		}
		if !ok {
			continue
		}
		sourcePVC, found, nestedErr := unstructured.NestedString(snapshot.Object, "spec", "source", "persistentVolumeClaimName")
		if nestedErr != nil {
			d.recordReconcileObjectFailure("spent_restore_classification", snapshot.GetNamespace()+"/"+snapshot.GetName(), nestedErr)
			continue
		}
		if !found || sourcePVC == "" {
			continue
		}
		pvc, exists := state.pvcs[namespacedName(snapshot.GetNamespace(), sourcePVC)]
		if exists && pvc.Status.Phase == corev1.ClaimBound {
			continue
		}
		backendSnapshot, backendErr := d.findBackendSnapshotForHandle(ctx, content.snapshotHandle)
		if backendErr != nil {
			d.recordReconcileObjectFailure("spent_restore_classification", content.snapshotHandle, backendErr)
			continue
		}
		if backendSnapshot == nil || !isCSISnapshot(backendSnapshot) {
			continue
		}
		createdAt := snapshot.GetCreationTimestamp().Time
		backendCreatedAt := time.Unix(backendSnapshot.GetCreationTime(), 0)
		if createdAt.IsZero() || backendSnapshot.GetCreationTime() <= 0 {
			d.recordReconcileObjectFailure("spent_restore_classification", backendSnapshot.ID,
				fmt.Errorf("snapshot creation time is unavailable"))
			continue
		}
		// TrueNAS 26.0 cannot mutate user properties on an existing snapshot:
		// zfs.resource.snapshot.update is absent and pool.snapshot.update silently
		// drops them. Use the later of the Kubernetes and backend creation times as
		// a durable, monotonic, write-free age origin. Clock skew can only delay GC.
		// Classification remains immediate for observability; guarded deletion
		// revalidates this origin and applies minOrphanAge immediately before GC.
		ageOrigin := laterTime(createdAt, backendCreatedAt)
		age := now.Sub(ageOrigin)
		phase := corev1.PersistentVolumeClaimPhase("")
		if exists {
			phase = pvc.Status.Phase
		}
		spent = append(spent, SpentRestoreSnapshot{
			Namespace:           snapshot.GetNamespace(),
			Name:                snapshot.GetName(),
			ContentName:         content.name,
			SnapshotHandle:      content.snapshotHandle,
			SourcePVC:           sourcePVC,
			SourcePVCPhase:      phase,
			CreationTime:        createdAt,
			Age:                 age,
			BackendSnapshotID:   backendSnapshot.ID,
			ClassifiedAt:        ageOrigin,
			SourcePVCWasMissing: !exists,
		})
	}
	return spent
}

func (d *Driver) findBackendSnapshotForHandle(ctx context.Context, handle string) (*truenas.Snapshot, error) {
	if strings.Contains(handle, "@") {
		snapshot, err := d.truenasClient.SnapshotGet(ctx, handle)
		if err != nil {
			if truenas.IsNotFoundError(err) {
				return nil, nil
			}
			return nil, err
		}
		return snapshot, nil
	}
	return d.truenasClient.SnapshotFindByName(ctx, d.config.ZFS.DatasetParentName, handle)
}

func laterTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func (d *Driver) deleteDetectedOrphans(
	ctx context.Context,
	report *ReconcileReport,
	currentState *kubernetesReconcileState,
	minOrphanAge time.Duration,
	maxPerRun int,
	snapshotDeleteBlockReason string,
) error {
	deletedCount := 0
	deletionCapReached := func(kind, id string) bool {
		if deletedCount < maxPerRun {
			return false
		}
		d.recordReconcileSkip(
			report,
			kind,
			id,
			fmt.Sprintf("deletion cap reached (maxPerRun=%d)", maxPerRun),
		)
		return true
	}

	if snapshotDeleteBlockReason != "" {
		klog.Errorf("Orphan reconcile: snapshot deletion pass skipped: %s", snapshotDeleteBlockReason)
		for _, orphan := range report.OrphanSnapshots {
			d.recordReconcileSkip(report, "snapshot", orphan.ID, snapshotDeleteBlockReason)
		}
	} else {
		for _, orphan := range report.OrphanSnapshots {
			if err := ctx.Err(); err != nil {
				return err
			}
			if deletionCapReached("snapshot", orphan.ID) {
				continue
			}
			if _, live := currentState.snapshotHandles[orphan.ID]; live {
				d.recordReconcileSkip(report, "snapshot", orphan.ID, "snapshot handle became live during revalidation")
				continue
			}
			if safe, reason := d.revalidateOrphanSnapshot(ctx, orphan, minOrphanAge); !safe {
				d.recordReconcileSkip(report, "snapshot", orphan.ID, reason)
				continue
			}
			if safe, reason := d.hardRecheckSnapshotContentAbsent(ctx, orphan); !safe {
				d.recordReconcileSkip(report, "snapshot", orphan.ID, reason)
				continue
			}
			klog.Infof("Orphan reconcile: deleting managed snapshot %s through guarded DeleteSnapshot", orphan.ID)
			if _, err := d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: orphan.ID}); err != nil {
				d.recordReconcileSkip(report, "snapshot", orphan.ID, err.Error())
				continue
			}
			report.DeletedSnapshots = append(report.DeletedSnapshots, orphan.ID)
			deletedCount++
		}
	}

	for _, orphan := range report.OrphanVolumes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if deletionCapReached("volume", orphan.ID) {
			continue
		}
		if _, live := currentState.volumeHandles[orphan.ID]; live {
			d.recordReconcileSkip(report, "volume", orphan.ID, "volume handle became live during revalidation")
			continue
		}
		if safe, reason := d.revalidateOrphanVolume(ctx, orphan, minOrphanAge); !safe {
			d.recordReconcileSkip(report, "volume", orphan.ID, reason)
			continue
		}
		if safe, reason := d.hardRecheckPersistentVolumeAbsent(ctx, orphan); !safe {
			d.recordReconcileSkip(report, "volume", orphan.ID, reason)
			continue
		}
		klog.Infof("Orphan reconcile: deleting managed volume %s through guarded DeleteVolume", orphan.ID)
		if _, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: orphan.ID}); err != nil {
			d.recordReconcileSkip(report, "volume", orphan.ID, err.Error())
			continue
		}
		report.DeletedVolumes = append(report.DeletedVolumes, orphan.ID)
		deletedCount++
	}

	for _, tombstone := range report.TombstoneSnapshots {
		if err := ctx.Err(); err != nil {
			return err
		}
		if deletionCapReached("tombstone-snapshot", tombstone.ID) {
			continue
		}
		reaped, reason := d.reapTombstoneSnapshot(ctx, tombstone, minOrphanAge)
		if !reaped {
			d.recordReconcileSkip(report, "tombstone-snapshot", tombstone.ID, reason)
			continue
		}
		report.DeletedTombstones = append(report.DeletedTombstones, tombstone.ID)
		deletedCount++
	}

	// Spent-restore deletion is not gated on the global detached flag (a
	// detached-opt-in StorageClass must be reapable even when the global default
	// is clone). This pass only runs under opts.Delete, and each object is
	// revalidated before deletion below.
	clientset, dynamicClient, err := d.kubernetesReconcileClients()
	if err != nil {
		return err
	}
	for i := range report.SpentRestoreSnapshots {
		detected := &report.SpentRestoreSnapshots[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		key := namespacedName(detected.Namespace, detected.Name)
		if deletionCapReached("spent-restore-snapshot", key) {
			continue
		}
		spent, safe, reason := d.revalidateSpentRestoreSnapshot(
			ctx, clientset, dynamicClient, *detected, minOrphanAge,
		)
		if !safe {
			d.recordReconcileSkip(report, "spent-restore-snapshot", key, reason)
			continue
		}
		klog.Infof("Orphan reconcile: deleting spent restore VolumeSnapshot %s", key)
		err := dynamicClient.Resource(volumeSnapshotGVR).Namespace(spent.Namespace).Delete(ctx, spent.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			d.recordReconcileSkip(report, "spent-restore-snapshot", key, err.Error())
			continue
		}
		report.DeletedSpentRestoreObjects = append(report.DeletedSpentRestoreObjects, key)
		deletedCount++
	}
	return nil
}

// The broad list used for classification is necessarily a sample. These live
// GETs occur after backend identity revalidation and immediately before the CSI
// delete call. Any object returned under the expected name is a veto, without
// trusting its current fields; a concurrent binder must always win the race.
func (d *Driver) hardRecheckPersistentVolumeAbsent(ctx context.Context, orphan ReconcileObject) (safe bool, reason string) {
	clientset, _, err := d.kubernetesReconcileClients()
	if err != nil {
		return false, err.Error()
	}
	name := orphan.PVName
	if name == "" {
		name = orphan.ID
	}
	// Also close the legacy-name gap for volumes whose durable csi_volume_name
	// does not equal the actual PV metadata name.
	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Sprintf("final PersistentVolume handle recheck failed: %v", err)
	}
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.name && pv.Spec.CSI.VolumeHandle == orphan.ID {
			return false, fmt.Sprintf("PersistentVolume %s appeared for handle %s during final live recheck", pv.Name, orphan.ID)
		}
	}
	// Keep the required object-specific GET as the final API operation before
	// the caller invokes DeleteVolume.
	if _, err := clientset.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{}); err == nil {
		return false, fmt.Sprintf("PersistentVolume %s appeared during final live recheck", name)
	} else if !apierrors.IsNotFound(err) {
		return false, fmt.Sprintf("final PersistentVolume %s recheck failed: %v", name, err)
	}
	return true, ""
}

func (d *Driver) hardRecheckSnapshotContentAbsent(ctx context.Context, orphan ReconcileObject) (safe bool, reason string) {
	_, dynamicClient, err := d.kubernetesReconcileClients()
	if err != nil {
		return false, err.Error()
	}
	name := orphan.KubernetesName
	if name == "" {
		name = orphan.ID
	}
	contents, err := dynamicClient.Resource(volumeSnapshotContentGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Sprintf("final VolumeSnapshotContent handle recheck failed: %v", err)
	}
	for i := range contents.Items {
		content := &contents.Items[i]
		driverName, _, _ := unstructured.NestedString(content.Object, "spec", "driver")
		handle, _, _ := unstructured.NestedString(content.Object, "status", "snapshotHandle")
		if driverName == d.name && handle == orphan.ID {
			return false, fmt.Sprintf("VolumeSnapshotContent %s appeared for handle %s during final live recheck", content.GetName(), orphan.ID)
		}
	}
	// Keep the required object-specific GET as the final API operation before
	// the caller invokes DeleteSnapshot.
	if _, err := dynamicClient.Resource(volumeSnapshotContentGVR).Get(ctx, name, metav1.GetOptions{}); err == nil {
		return false, fmt.Sprintf("VolumeSnapshotContent %s appeared during final live recheck", name)
	} else if !apierrors.IsNotFound(err) {
		return false, fmt.Sprintf("final VolumeSnapshotContent %s recheck failed: %v", name, err)
	}
	return true, ""
}

// snapshotIsLiveCSIObjectWithTombstoneShapedName is the identity belt on top of
// the ledger: it detects a LIVE CSI snapshot whose user-chosen name merely looks
// like a tombstone (its recorded CSI name sanitizes to its own current short
// name), e.g. a snapshot literally created as "backup-csi-deleted-2024". Such an
// object could only meet the ledger gate through a stale entry at an identical
// recreated full ID, and must never be reaped. It shares the exact identity
// predicate the global tombstone classification uses (identity beats name
// shape), so classification and reaping can never diverge.
//
// Deliberately NOT "skip whenever csi_snapshot_name is present": on TrueNAS 26.0
// the post-rename property strip is a silent no-op (no API mutates properties on
// an existing snapshot), so the driver's OWN tombstones still carry their
// original identity properties — their recorded name is the pre-tombstone CSI
// name and does not match the tombstone-shaped current name. A bare presence
// check would make the reaper permanently inert on the exact backend the leak
// repair exists for.
func snapshotIsLiveCSIObjectWithTombstoneShapedName(snap *truenas.Snapshot) bool {
	return snapshotCarriesLiveCSIIdentity(snap)
}

// reapTombstoneSnapshot removes a driver-created deferred-delete tombstone once
// its last restored clone is gone. On TrueNAS 26.0 zfs.resource.snapshot.destroy
// has no defer semantics, so DeleteSnapshot's post-tombstone destroy leaves the
// tombstone behind whenever a live clone still depends on it; this reaps it
// exactly when the dependency is finally released. Destruction requires, all
// re-proven under the source volume lock immediately before the delete:
//   - a matching parent-dataset ledger entry whose recorded immutable creation
//     time matches the observed snapshot (driver provenance — neither the name
//     shape nor a stale entry over a recreated same-ID object authorizes a reap);
//   - no live CSI snapshot identity (belt against stale-ledger name collisions);
//   - the source dataset locally stamped by THIS driver instance;
//   - unchanged creation identity and satisfied age gate.
//
// A snapshot that still has clones is a benign skip, not a failure. After a
// successful reap the ledger entry is removed (best-effort; the sweep retires
// leftovers).
func (d *Driver) reapTombstoneSnapshot(
	ctx context.Context,
	tombstone ReconcileObject,
	minOrphanAge time.Duration,
) (reaped bool, reason string) {
	if tombstone.SourceVolumeID == "" {
		return false, "tombstone snapshot has no resolvable source volume"
	}
	lockKey := "volume:" + tombstone.SourceVolumeID
	if !d.acquireOperationLock(lockKey) {
		return false, "source volume operation is in progress"
	}
	defer d.releaseOperationLock(lockKey)

	snapshot, err := d.truenasClient.SnapshotGet(ctx, tombstone.BackendID)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			// Already gone (ZFS reclaimed it, or a peer reaped it): the operation's
			// goal is met, so treat it as reaped for reporting and retire the entry.
			d.removeTombstoneLedgerEntry(ctx, tombstone.BackendID)
			return true, ""
		}
		return false, fmt.Sprintf("tombstone snapshot revalidation failed: %v", err)
	}
	if !isSnapshotTombstone(snapshot) || snapshot.ID != tombstone.BackendID {
		return false, "backend snapshot is no longer the detected tombstone"
	}
	if snapshotIsLiveCSIObjectWithTombstoneShapedName(snapshot) {
		return false, "snapshot carries live CSI identity; refusing to reap"
	}
	// Re-prove driver provenance from a fresh parent read under the lock.
	parent, parentErr := d.truenasClient.DatasetGet(ctx, d.parentDatasetName())
	if parentErr != nil {
		return false, fmt.Sprintf("tombstone ledger revalidation failed: %v", parentErr)
	}
	entry, recorded := tombstoneLedgerFromDataset(parent)[tombstoneLedgerKey(snapshot.ID)]
	if !recorded || entry.Snapshot != snapshot.ID {
		return false, "no tombstone ledger entry proves driver provenance"
	}
	if entry.CreatedAt <= 0 || entry.CreatedAt != snapshot.GetCreationTime() {
		return false, "tombstone ledger creation identity does not match the observed snapshot"
	}
	// The tombstone must sit on a dataset this driver instance owns.
	sourceDataset, dsErr := d.truenasClient.DatasetGet(ctx, snapshot.Dataset)
	if dsErr != nil {
		return false, fmt.Sprintf("tombstone source dataset revalidation failed: %v", dsErr)
	}
	if !datasetHasLocalUserProperty(sourceDataset, PropDriverInstanceID, d.driverInstanceID()) {
		return false, "tombstone source dataset does not carry this driver instance's ownership stamp"
	}
	createdAt, _, eligible := reconcileAge(time.Now(), snapshot.GetCreationTime(), minOrphanAge)
	if !eligible || !createdAt.Equal(tombstone.CreatedAt) {
		return false, "tombstone creation identity or age changed"
	}
	if err := d.truenasClient.SnapshotDelete(ctx, snapshot.ID, false, false); err != nil {
		if truenas.IsNotFoundError(err) {
			d.removeTombstoneLedgerEntry(ctx, snapshot.ID)
			return true, ""
		}
		var cloneErr *truenas.ErrSnapshotHasClones
		if errors.As(err, &cloneErr) {
			return false, "tombstone snapshot still has dependent clones"
		}
		return false, fmt.Sprintf("failed to reap tombstone snapshot: %v", err)
	}
	d.removeTombstoneLedgerEntry(ctx, snapshot.ID)
	klog.Infof("Orphan reconcile: reaped released deferred-delete tombstone %s", snapshot.ID)
	return true, ""
}

// sweepStaleInflightMarkers retires in-flight creation markers that can no
// longer gate a recovery: their dataset is gone, or it now carries a local
// ownership stamp (creation completed; only the marker delete was lost). Both
// conditions are proven live and only after a generous age bound so an active
// multi-minute detached copy is never raced. Markers from other driver
// instances, other versions, or with unusable timestamps are left alone;
// corrupt (unparseable) payloads in our namespace are retired because nothing
// can ever act on them.
func (d *Driver) sweepStaleInflightMarkers(ctx context.Context, parent *truenas.Dataset, now time.Time, minAge time.Duration) {
	if parent == nil {
		return
	}
	staleKeys := make([]string, 0)
	for key, property := range parent.UserProperties {
		if !strings.HasPrefix(key, PropInflightMarkerPrefix) || !isLocalUserPropertySource(property.Source) {
			continue
		}
		var marker inflightMarker
		if err := json.Unmarshal([]byte(property.Value), &marker); err != nil {
			klog.Warningf("Retiring corrupt in-flight marker %s: %v", key, err)
			staleKeys = append(staleKeys, key)
			continue
		}
		if marker.Version != inflightMarkerVersion || marker.Instance != d.driverInstanceID() || marker.Dataset == "" {
			continue
		}
		startedAt, parseErr := time.Parse(time.RFC3339Nano, marker.StartedAt)
		if parseErr != nil || now.Sub(startedAt) <= minAge {
			continue
		}
		dataset, err := d.truenasClient.DatasetGet(ctx, marker.Dataset)
		if err != nil {
			if truenas.IsNotFoundError(err) {
				staleKeys = append(staleKeys, key)
			} else {
				d.recordReconcileObjectFailure("inflight_marker_sweep", marker.Dataset, err)
			}
			continue
		}
		if owner, ok := datasetUserPropertyProjection(dataset, PropDriverInstanceID); ok && isLocalUserPropertySource(owner.Source) {
			staleKeys = append(staleKeys, key)
		}
		// Dataset exists but is still unstamped: the marker remains the only proof
		// that lets a retry recover the remnant — keep it.
	}
	if len(staleKeys) == 0 {
		return
	}
	sort.Strings(staleKeys)
	if err := d.truenasClient.DatasetRemoveUserProperties(ctx, d.parentDatasetName(), staleKeys); err != nil {
		d.recordReconcileObjectFailure("inflight_marker_sweep", d.parentDatasetName(), err)
		return
	}
	klog.Infof("Orphan reconcile: retired %d stale in-flight creation markers", len(staleKeys))
}

// sweepOrphanedReplicationJobs aborts only active replication.run_onetime jobs
// whose target is strictly below this driver's configured parent dataset. A
// matching copy marker protects legitimate in-flight work unless a live backend
// read proves that its source dataset is gone. The target dataset is never used
// as an abort trigger: a live detached copy (replication.run_onetime with
// only_from_scratch) deliberately has no target until 'zfs receive' materializes
// it, so target absence is expected mid-copy. The source, by contrast, is present
// throughout a legitimate copy, so its absence unambiguously marks an abandoned
// job. Marker reads happen after the job list, preserving the marker-before-launch
// ordering used by CreateVolume and avoiding a stale-parent-read race with a newly
// launched job.
func (d *Driver) sweepOrphanedReplicationJobs(ctx context.Context) error {
	if d.config == nil || d.truenasClient == nil {
		return fmt.Errorf("driver configuration and TrueNAS client are required")
	}
	jobs, err := d.truenasClient.ReplicationJobList(ctx)
	if err != nil {
		return err
	}
	parentName := d.parentDatasetName()
	inScope := make([]*truenas.ReplicationJob, 0, len(jobs))
	for _, job := range jobs {
		if job != nil && datasetStrictlyBelowParent(job.TargetDataset, parentName) {
			inScope = append(inScope, job)
		}
	}
	if len(inScope) == 0 {
		return nil
	}

	parent, err := d.truenasClient.DatasetGet(ctx, parentName)
	if err != nil {
		// Without the parent read, marker absence is not proven. Fail closed and
		// leave every job untouched.
		return fmt.Errorf("read parent dataset before replication job sweep: %w", err)
	}
	markers := d.liveCopyMarkers(parent)

	for _, job := range inScope {
		reason := ""
		if _, marked := markers[job.TargetDataset]; !marked {
			reason = replicationJobReasonMissingMarker
		} else {
			// The target dataset is deliberately absent until 'zfs receive'
			// materializes it, so only a missing SOURCE proves the job is abandoned.
			for _, sourceDataset := range job.SourceDatasets {
				missing, conclusive := d.replicationJobDatasetMissing(ctx, sourceDataset, job.ID, "source")
				if !conclusive {
					reason = ""
					break
				}
				if missing {
					reason = replicationJobReasonMissingSourceDataset
					break
				}
			}
		}
		if reason == "" {
			continue
		}
		if err := d.truenasClient.ReplicationJobAbort(ctx, job.ID, reason); err != nil {
			d.recordReconcileObjectFailure("replication_job_abort", fmt.Sprint(job.ID), err)
			continue
		}
		klog.Warningf("Replication job sweep aborted driver-owned job id=%d target=%s reason=%s", job.ID, job.TargetDataset, reason)
	}
	return nil
}

func (d *Driver) liveCopyMarkers(parent *truenas.Dataset) map[string]struct{} {
	markers := make(map[string]struct{})
	if parent == nil {
		return markers
	}
	for key, property := range parent.UserProperties {
		if !strings.HasPrefix(key, PropInflightMarkerPrefix) || !isLocalUserPropertySource(property.Source) {
			continue
		}
		var marker inflightMarker
		if err := json.Unmarshal([]byte(property.Value), &marker); err != nil {
			continue
		}
		if marker.Version != inflightMarkerVersion || marker.Instance != d.driverInstanceID() ||
			marker.Mode != inflightModeCopy || !datasetStrictlyBelowParent(marker.Dataset, d.parentDatasetName()) ||
			key != inflightMarkerKey(path.Base(marker.Dataset)) {
			continue
		}
		markers[marker.Dataset] = struct{}{}
	}
	return markers
}

func datasetStrictlyBelowParent(dataset, parent string) bool {
	dataset = strings.TrimSuffix(strings.TrimSpace(dataset), "/")
	parent = strings.TrimSuffix(strings.TrimSpace(parent), "/")
	return dataset != "" && parent != "" && strings.HasPrefix(dataset, parent+"/")
}

func (d *Driver) replicationJobDatasetMissing(ctx context.Context, dataset string, jobID int64, role string) (missing, conclusive bool) {
	if strings.TrimSpace(dataset) == "" {
		return false, false
	}
	if _, err := d.truenasClient.DatasetGet(ctx, dataset); err == nil {
		return false, true
	} else if truenas.IsNotFoundError(err) {
		return true, true
	} else {
		d.recordReconcileObjectFailure("replication_job_dataset_check", fmt.Sprintf("%d/%s/%s", jobID, role, dataset), err)
		return false, false
	}
}

func (d *Driver) runReplicationJobSweep(ctx context.Context) {
	if err := d.sweepOrphanedReplicationJobs(ctx); err != nil && ctx.Err() == nil {
		RecordReconcileFailure("replication_job_sweep")
		klog.Errorf("Replication job sweep failed: %v", err)
	}
}

// sweepOrphanedTombstoneLedger retires ledger entries whose snapshot no longer
// exists — either the crash window between ledger write and rename (the
// tombstone was never created) or a reap/reclaim whose entry removal was lost.
// Age-gated on the recorded rename time so an in-progress DeleteSnapshot is
// never raced. Swept entries are also dropped from the in-memory map so the
// same pass cannot classify against them.
func (d *Driver) sweepOrphanedTombstoneLedger(ctx context.Context, ledger map[string]tombstoneLedgerEntry, now time.Time, minAge time.Duration) {
	staleKeys := make([]string, 0)
	for key, entry := range ledger {
		if _, err := d.truenasClient.SnapshotGet(ctx, entry.Snapshot); err == nil {
			continue
		} else if !truenas.IsNotFoundError(err) {
			d.recordReconcileObjectFailure("tombstone_ledger_sweep", entry.Snapshot, err)
			continue
		}
		if renamedAt, parseErr := time.Parse(time.RFC3339Nano, entry.RenamedAt); parseErr == nil && now.Sub(renamedAt) <= minAge {
			continue
		}
		staleKeys = append(staleKeys, key)
	}
	if len(staleKeys) == 0 {
		return
	}
	sort.Strings(staleKeys)
	if err := d.truenasClient.DatasetRemoveUserProperties(ctx, d.parentDatasetName(), staleKeys); err != nil {
		d.recordReconcileObjectFailure("tombstone_ledger_sweep", d.parentDatasetName(), err)
		return
	}
	for _, key := range staleKeys {
		delete(ledger, key)
	}
	klog.Infof("Orphan reconcile: retired %d orphaned tombstone ledger entries", len(staleKeys))
}

func (d *Driver) revalidateOrphanVolume(
	ctx context.Context,
	orphan ReconcileObject,
	minOrphanAge time.Duration,
) (safe bool, reason string) {
	dataset, err := d.truenasClient.DatasetGet(ctx, orphan.BackendID)
	if err != nil {
		return false, fmt.Sprintf("backend volume revalidation failed: %v", err)
	}
	if dataset == nil || dataset.UserProperties[PropManagedResource].Value != "true" {
		return false, "backend volume is no longer CSI-managed"
	}
	if path.Base(dataset.Name) != orphan.ID {
		return false, "backend volume identity changed"
	}
	createdAt, _, eligible := reconcileAge(time.Now(), dataset.GetCreationTime(), minOrphanAge)
	if !eligible || !createdAt.Equal(orphan.CreatedAt) {
		return false, "backend volume creation identity or age changed"
	}
	return true, ""
}

func (d *Driver) revalidateOrphanSnapshot(
	ctx context.Context,
	orphan ReconcileObject,
	minOrphanAge time.Duration,
) (safe bool, reason string) {
	snapshot, err := d.truenasClient.SnapshotGet(ctx, orphan.BackendID)
	if err != nil {
		return false, fmt.Sprintf("backend snapshot revalidation failed: %v", err)
	}
	snapshotHandle, ok := reconcileSnapshotHandle(snapshot)
	if !isCSISnapshot(snapshot) || !ok || snapshotHandle != orphan.ID {
		return false, "backend snapshot is no longer the detected CSI-managed object"
	}
	createdAt, _, eligible := reconcileAge(time.Now(), snapshot.GetCreationTime(), minOrphanAge)
	if !eligible || !createdAt.Equal(orphan.CreatedAt) {
		return false, "backend snapshot creation identity or age changed"
	}
	return true, ""
}

func (d *Driver) revalidateSpentRestoreSnapshot(
	ctx context.Context,
	clientset kubernetes.Interface,
	dynamicClient dynamic.Interface,
	detected SpentRestoreSnapshot,
	minOrphanAge time.Duration,
) (spent SpentRestoreSnapshot, safe bool, reason string) {
	snapshot, err := dynamicClient.Resource(volumeSnapshotGVR).Namespace(detected.Namespace).Get(
		ctx, detected.Name, metav1.GetOptions{},
	)
	if err != nil {
		return SpentRestoreSnapshot{}, false, fmt.Sprintf("VolumeSnapshot revalidation failed: %v", err)
	}
	sourcePVC, found, nestedErr := unstructured.NestedString(
		snapshot.Object, "spec", "source", "persistentVolumeClaimName",
	)
	if nestedErr != nil || !found || sourcePVC == "" || sourcePVC != detected.SourcePVC {
		return SpentRestoreSnapshot{}, false, "VolumeSnapshot source PVC identity changed"
	}
	contentName, found, nestedErr := unstructured.NestedString(
		snapshot.Object, "status", "boundVolumeSnapshotContentName",
	)
	if nestedErr != nil || !found || contentName == "" || contentName != detected.ContentName {
		return SpentRestoreSnapshot{}, false, "VolumeSnapshotContent binding changed"
	}
	content, err := dynamicClient.Resource(volumeSnapshotContentGVR).Get(ctx, contentName, metav1.GetOptions{})
	if err != nil {
		return SpentRestoreSnapshot{}, false, fmt.Sprintf("VolumeSnapshotContent revalidation failed: %v", err)
	}
	driverName, _, driverErr := unstructured.NestedString(content.Object, "spec", "driver")
	referenceNamespace, _, namespaceErr := unstructured.NestedString(
		content.Object, "spec", "volumeSnapshotRef", "namespace",
	)
	referenceName, _, nameErr := unstructured.NestedString(
		content.Object, "spec", "volumeSnapshotRef", "name",
	)
	if driverErr != nil || namespaceErr != nil || nameErr != nil || driverName != d.name ||
		referenceNamespace != detected.Namespace || referenceName != detected.Name {
		return SpentRestoreSnapshot{}, false, "VolumeSnapshotContent is no longer bound to this driver's restore snapshot"
	}
	handle, found, handleErr := unstructured.NestedString(content.Object, "status", "snapshotHandle")
	if handleErr != nil || !found || handle == "" || handle != detected.SnapshotHandle {
		return SpentRestoreSnapshot{}, false, "VolumeSnapshotContent handle changed"
	}

	pvc, err := clientset.CoreV1().PersistentVolumeClaims(detected.Namespace).Get(
		ctx, sourcePVC, metav1.GetOptions{},
	)
	missing := apierrors.IsNotFound(err)
	if err != nil && !missing {
		return SpentRestoreSnapshot{}, false, fmt.Sprintf("source PVC revalidation failed: %v", err)
	}
	if err == nil && pvc.Status.Phase == corev1.ClaimBound {
		return SpentRestoreSnapshot{}, false, "source PVC became Bound during revalidation"
	}
	createdAt := snapshot.GetCreationTimestamp().Time
	if createdAt.IsZero() || !createdAt.Equal(detected.CreationTime) {
		return SpentRestoreSnapshot{}, false, "snapshot creation identity changed or has not exceeded the minimum orphan age"
	}
	backendSnapshot, backendErr := d.findBackendSnapshotForHandle(ctx, handle)
	if backendErr != nil || backendSnapshot == nil || backendSnapshot.ID != detected.BackendSnapshotID {
		return SpentRestoreSnapshot{}, false, fmt.Sprintf("backend spent-restore snapshot revalidation failed: %v", backendErr)
	}
	if backendSnapshot.GetCreationTime() <= 0 {
		return SpentRestoreSnapshot{}, false, "backend spent-restore snapshot creation time is unavailable"
	}
	// This is intentionally write-free on TrueNAS 26.0; existing snapshots have
	// no API that can persist a user property. Recompute the same conservative
	// age origin and require both object identities to remain unchanged.
	ageOrigin := laterTime(createdAt, time.Unix(backendSnapshot.GetCreationTime(), 0))
	if !ageOrigin.Equal(detected.ClassifiedAt) {
		return SpentRestoreSnapshot{}, false, "spent-restore snapshot creation identity changed"
	}
	age := time.Since(ageOrigin)
	if age <= minOrphanAge {
		return SpentRestoreSnapshot{}, false, "spent-restore snapshot creation age has not exceeded the minimum orphan age"
	}
	phase := corev1.PersistentVolumeClaimPhase("")
	if pvc != nil {
		phase = pvc.Status.Phase
	}
	return SpentRestoreSnapshot{
		Namespace:           detected.Namespace,
		Name:                detected.Name,
		ContentName:         contentName,
		SnapshotHandle:      handle,
		SourcePVC:           sourcePVC,
		SourcePVCPhase:      phase,
		CreationTime:        createdAt,
		Age:                 age,
		BackendSnapshotID:   backendSnapshot.ID,
		ClassifiedAt:        ageOrigin,
		SourcePVCWasMissing: missing,
	}, true, ""
}

func volumeAttachmentKey(volumeID, nodeName string) string {
	return volumeID + "\x00" + nodeName
}

func stalePublicationObservationKey(datasetName, propertyKey string) string {
	return datasetName + "\x00" + propertyKey
}

func publicationPropertyCount(datasets []*truenas.Dataset) int {
	count := 0
	for _, dataset := range datasets {
		if dataset == nil {
			continue
		}
		for key, property := range dataset.UserProperties {
			if strings.HasPrefix(key, publicationPropertyPrefix) &&
				isLocalUserPropertySource(property.Source) {
				count++
			}
		}
	}
	return count
}

// reconcileStalePublicationRecords repairs the operator force-finalizer escape
// hatch. A finalizer-removed VolumeAttachment never reaches external-attacher's
// normal ControllerUnpublishVolume call, so its durable record otherwise blocks
// SINGLE_NODE volumes forever. Absence must be continuous for the configured
// grace period and is proved again under the same per-volume lock used by CSI.
func (d *Driver) reconcileStalePublicationRecords(
	ctx context.Context,
	datasets []*truenas.Dataset,
	state *kubernetesReconcileState,
	now time.Time,
) {
	if state == nil {
		return
	}
	recordCount := publicationPropertyCount(datasets)
	if state.volumeAttachmentCount == 0 && recordCount >= staleRecordMassAbsenceThreshold {
		// A zero-result VA list while several backend records exist is the shape of
		// an etcd restore or informer/API discontinuity, not evidence for mass
		// revocation. Restart every observation's grace window after recovery.
		d.stalePublicationRecordsSeen.Range(func(key, _ interface{}) bool {
			d.stalePublicationRecordsSeen.Delete(key)
			return true
		})
		RecordFencingStaleDeferred()
		klog.Warningf("Stale fencing record reconcile deferred: VolumeAttachment list is empty while %d records exist (brake threshold=%d)",
			recordCount, staleRecordMassAbsenceThreshold)
		return
	}
	grace, err := d.config.Fencing.StaleRecordGracePeriodDuration()
	if err != nil || grace <= 0 {
		d.recordReconcileObjectFailure("stale_publication_configuration", "fencing.staleRecordGracePeriod", err)
		return
	}
	for _, dataset := range datasets {
		if dataset == nil {
			continue
		}
		records, parseErr := publicationRecordsFromDataset(dataset)
		if parseErr != nil {
			d.recordReconcileObjectFailure("stale_publication_classification", dataset.Name, parseErr)
			continue
		}
		volumeID := path.Base(dataset.Name)
		for propertyKey := range records {
			record := records[propertyKey]
			observationKey := stalePublicationObservationKey(dataset.Name, propertyKey)
			if _, live := state.liveVolumeAttachments[volumeAttachmentKey(volumeID, record.Node)]; live {
				d.stalePublicationRecordsSeen.Delete(observationKey)
				continue
			}
			firstMissing := now
			if record.State != publicationStateRemoving {
				candidate := newStalePublicationObservation(now, record)
				actual, loaded := d.stalePublicationRecordsSeen.LoadOrStore(observationKey, candidate)
				observation, valid := actual.(stalePublicationObservation)
				if !loaded || !valid || !observation.matches(record) {
					d.stalePublicationRecordsSeen.Store(observationKey, candidate)
					observation = candidate
				}
				firstMissing = observation.FirstMissing
				if now.Sub(firstMissing) < grace {
					continue
				}
			}
			revoked, err := d.revokeStalePublicationRecord(ctx, dataset.Name, volumeID, propertyKey, record, recordCount)
			if err != nil {
				d.recordReconcileObjectFailure("stale_publication_cleanup", volumeID+"/"+record.Node, err)
				continue
			}
			d.stalePublicationRecordsSeen.Delete(observationKey)
			if !revoked {
				continue
			}
			klog.Infof("Stale fencing record reconcile revoked volume=%s node=%s after continuous absence since %s",
				volumeID, record.Node, firstMissing.UTC().Format(time.RFC3339))
		}
	}
}

func (d *Driver) revokeStalePublicationRecord(
	ctx context.Context,
	datasetName, volumeID, propertyKey string,
	detected publicationRecord,
	recordCount int,
) (bool, error) {
	lockKey := "volume:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return false, fmt.Errorf("volume operation is in progress")
	}
	defer d.releaseOperationLock(lockKey)

	dataset, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return false, fmt.Errorf("fresh dataset read: %w", err)
	}
	records, err := publicationRecordsFromDataset(dataset)
	if err != nil {
		return false, fmt.Errorf("fresh publication record read: %w", err)
	}
	current, exists := records[propertyKey]
	if !exists || !samePublicationRecordGeneration(current, detected) {
		// The grant was removed or republished while the outer pass waited for
		// the volume lock. Never apply an old grace decision to a new generation.
		return false, nil
	}
	live, attachmentCount, err := d.liveVolumeAttachmentExists(ctx, volumeID, current.Node)
	if err != nil {
		return false, err
	}
	if live {
		return false, nil
	}
	if attachmentCount == 0 && recordCount >= staleRecordMassAbsenceThreshold {
		RecordFencingStaleDeferred()
		return false, fmt.Errorf("mass-absence brake engaged during final VolumeAttachment recheck")
	}
	nodeID := current.EncodedID
	if nodeID == "" {
		nodeID = current.Node
	}
	shareType := shareTypeForPublishedVolume(dataset, nil)
	if err := d.unpublishFencedVolume(ctx, dataset, datasetName, shareType, nodeID); err != nil {
		return false, fmt.Errorf("revoke backend grant and publication record: %w", err)
	}
	return true, nil
}

func (d *Driver) liveVolumeAttachmentExists(ctx context.Context, volumeID, nodeName string) (exists bool, attachmentCount int, retErr error) {
	clientset, _, err := d.kubernetesReconcileClients()
	if err != nil {
		return false, 0, err
	}
	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, 0, fmt.Errorf("final PersistentVolume list for attachment recheck: %w", err)
	}
	handlesByPV := make(map[string]string)
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.name {
			handlesByPV[pv.Name] = pv.Spec.CSI.VolumeHandle
		}
	}
	attachments, err := clientset.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, 0, fmt.Errorf("final VolumeAttachment list: %w", err)
	}
	count := 0
	for i := range attachments.Items {
		attachment := &attachments.Items[i]
		if attachment.Spec.Attacher != d.name || attachment.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		count++
		pvName := *attachment.Spec.Source.PersistentVolumeName
		if (handlesByPV[pvName] == volumeID || pvName == volumeID) && attachment.Spec.NodeName == nodeName {
			return true, count, nil
		}
	}
	return false, count, nil
}

func (d *Driver) recordReconcileObjectFailure(phase, id string, err error) {
	RecordReconcileFailure(phase)
	if err == nil {
		err = fmt.Errorf("unknown error")
	}
	klog.Errorf("Reconcile object skipped phase=%s id=%s: %v", phase, id, err)
}

func (d *Driver) recordReconcileSkip(report *ReconcileReport, kind, id, reason string) {
	klog.Warningf("Orphan reconcile: guarded delete skipped kind=%s id=%s: %s", kind, id, reason)
	report.SkippedDeletes = append(report.SkippedDeletes, ReconcileActionFailure{Kind: kind, ID: id, Reason: reason})
}

func namespacedName(namespace, name string) string {
	return strings.TrimSuffix(namespace, "/") + "/" + strings.TrimPrefix(name, "/")
}

func (d *Driver) startOrphanReconcile() {
	if d.config == nil {
		klog.Info("Orphan reconcile detection disabled because configuration is unavailable")
		return
	}
	interval, err := d.config.Reconcile.IntervalDuration()
	if strings.TrimSpace(d.config.Reconcile.Interval) == "" {
		interval, err = defaultControllerReconcileInterval, nil
	}
	if err != nil || interval <= 0 {
		klog.Errorf("Controller reconciliation disabled due to invalid interval %q: %v", d.config.Reconcile.Interval, err)
		return
	}
	minAge := defaultControllerReconcileMinOrphanAge
	if d.config.Reconcile.Enabled || d.config.Fencing.Enabled() {
		minAge, err = d.config.Reconcile.MinOrphanAgeDuration()
		if strings.TrimSpace(d.config.Reconcile.MinOrphanAge) == "" {
			minAge, err = defaultControllerReconcileMinOrphanAge, nil
		}
		if err != nil || minAge <= 0 {
			klog.Errorf("Controller reconciliation disabled due to invalid minimum orphan age %q: %v", d.config.Reconcile.MinOrphanAge, err)
			return
		}
	}
	cadence := interval
	if d.config.Fencing.Enabled() {
		grace, graceErr := d.config.Fencing.StaleRecordGracePeriodDuration()
		if graceErr != nil || grace <= 0 {
			klog.Errorf("Controller reconciliation disabled due to invalid fencing stale-record grace %q: %v",
				d.config.Fencing.StaleRecordGracePeriod, graceErr)
			return
		}
		cadence = controllerReconcileCadence(interval, grace)
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.reconcileCancel = cancel
	d.reconcileWg.Add(1)
	go func() {
		defer d.reconcileWg.Done()
		klog.Infof("Controller reconciliation started: interval=%v cadence=%v minOrphanAge=%v replicationJobSweep=true orphanDetection=%t staleFencingRecords=%t delete=false",
			interval, cadence, minAge, d.config.Reconcile.Enabled, d.config.Fencing.Enabled())
		run := func() {
			d.runReplicationJobSweep(ctx)
			if !d.config.Reconcile.Enabled && !d.config.Fencing.Enabled() {
				return
			}
			report, reconcileErr := d.reconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: minAge}, true)
			if reconcileErr != nil && ctx.Err() == nil {
				klog.Errorf("Orphan reconcile detection failed: %v", reconcileErr)
				return
			}
			if reconcileErr == nil {
				klog.Infof("Orphan reconcile detection complete: volumes=%d snapshots=%d spentRestoreSnapshots=%d tombstones=%d",
					report.OrphanVolumeCount, report.OrphanSnapshotCount, report.SpentRestoreSnapshotCount, report.TombstoneSnapshotCount)
			}
		}

		// Populate metrics immediately rather than leaving them unknown until the
		// first interval elapses.
		run()
		ticker := time.NewTicker(cadence)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				run()
			case <-ctx.Done():
				klog.Info("Orphan reconcile detection stopped")
				return
			}
		}
	}()
}

func controllerReconcileCadence(orphanInterval, staleGrace time.Duration) time.Duration {
	if staleGrace > 0 && staleGrace < orphanInterval {
		return staleGrace
	}
	return orphanInterval
}

func (d *Driver) stopOrphanReconcile() {
	if d.reconcileCancel != nil {
		d.reconcileCancel()
		d.reconcileWg.Wait()
	}
}
