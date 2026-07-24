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
	// deletionCapReasonPrefix marks skips caused purely by the per-run deletion
	// cap so reporting can separate backlog pressure from real guard refusals.
	deletionCapReasonPrefix = "deletion cap reached"
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
	// Protocol identifies the share protocol for share orphans (NFS, iSCSI,
	// NVMe-oF) so the guarded delete phase can route cleanup to the correct
	// backend objects. It is only meaningful for entries in OrphanShares.
	Protocol ShareType
	// remnantNonce carries the in-flight marker nonce observed when a remnant
	// orphan was classified. The guarded destroy re-fetches the marker live and
	// refuses to act unless the nonce is identical, so a fresh create attempt
	// that rewrites the marker between detection and deletion is never raced.
	// Only meaningful for entries in RemnantVolumes.
	remnantNonce string
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
	RemnantVolumeCount         int
	OrphanVolumeBytes          int64
	OrphanSnapshotBytes        int64
	TombstoneSnapshotBytes     int64
	OrphanVolumes              []ReconcileObject
	OrphanSnapshots            []ReconcileObject
	OrphanShares               []ReconcileObject
	SpentRestoreSnapshots      []SpentRestoreSnapshot
	TombstoneSnapshots         []ReconcileObject
	RemnantVolumes             []ReconcileObject
	DeletedVolumes             []string
	DeletedSnapshots           []string
	DeletedShares              []string
	DeletedSpentRestoreObjects []string
	DeletedTombstones          []string
	DeletedRemnants            []string
	AdoptedStamps              []string
	SkippedDeletes             []ReconcileActionFailure
	DeleteEnabled              bool
	AdoptedStampCount          int
}

// CapSkippedDeletes counts guarded deletes that were skipped only because the
// per-run deletion cap was already spent. The remainder of SkippedDeletes are
// guard refusals that need operator attention.
func (r *ReconcileReport) CapSkippedDeletes() int {
	count := 0
	for i := range r.SkippedDeletes {
		if strings.HasPrefix(r.SkippedDeletes[i].Reason, deletionCapReasonPrefix) {
			count++
		}
	}
	return count
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
		sort.Slice(report.RemnantVolumes, func(i, j int) bool { return report.RemnantVolumes[i].ID < report.RemnantVolumes[j].ID })
		sort.Strings(report.AdoptedStamps)
		report.OrphanVolumeCount = len(report.OrphanVolumes)
		report.OrphanSnapshotCount = len(report.OrphanSnapshots)
		report.SpentRestoreSnapshotCount = len(report.SpentRestoreSnapshots)
		report.TombstoneSnapshotCount = len(report.TombstoneSnapshots)
		report.RemnantVolumeCount = len(report.RemnantVolumes)
		report.AdoptedStampCount = len(report.AdoptedStamps)
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
	kubeState, err := d.loadKubernetesReconcileState(ctx, minOrphanAge)
	if err != nil {
		RecordReconcileFailure("load_kubernetes_state")
		return report, err
	}
	if reconcileStalePublications && d.config.Fencing.Enabled() {
		d.reconcileStalePublicationRecords(ctx, datasets, kubeState, time.Now())
	}

	// The driver's durable bookkeeping — in-flight creation markers and the
	// tombstone ledger — lives on the parent dataset, and (Fix 4b, when enabled)
	// also on a dedicated bookkeeping child dataset. Reading can fail without
	// aborting the pass — tombstone reaping then simply stays empty (fail-safe;
	// no ledger, no reaping) and the sweeps are skipped.
	var ledger map[string]tombstoneLedgerEntry
	bookkeepingReadable := false
	parentDataset, parentErr := d.truenasClient.DatasetGet(ctx, d.parentDatasetName())
	if parentErr != nil {
		d.recordReconcileObjectFailure("parent_bookkeeping", d.parentDatasetName(), parentErr)
	} else {
		bookkeepingReadable = true
		ledger = tombstoneLedgerFromDataset(parentDataset)
		// Bookkeeping hygiene runs on every pass regardless of opts.Delete: these
		// properties are driver-internal provenance records, not user data, and
		// each removal requires proof of staleness gathered live below.
		d.sweepStaleInflightMarkers(ctx, parentDataset, time.Now(), minOrphanAge)
	}
	if d.bookkeepingEnabled() {
		// Migrate any parent-side bookkeeping toward the dedicated child dataset
		// (idempotent; parent removal is gated by CleanupParent). Runs before the
		// dual-read merge so a freshly migrated entry is visible from the child.
		d.migrateParentBookkeeping(ctx, parentDataset)
		bookkeeping, bkErr := d.truenasClient.DatasetGet(ctx, d.bookkeepingDatasetName())
		if bkErr != nil && !truenas.IsNotFoundError(bkErr) {
			d.recordReconcileObjectFailure("bookkeeping", d.bookkeepingDatasetName(), bkErr)
		} else {
			bookkeepingReadable = true
			// Dual-read: merge the child's ledger entries over the parent's. Keys
			// are content-hashed snapshot IDs, so a migrated entry is identical in
			// both locations and the merge is a lossless union.
			for key, entry := range tombstoneLedgerFromDataset(bookkeeping) {
				if ledger == nil {
					ledger = make(map[string]tombstoneLedgerEntry)
				}
				ledger[key] = entry
			}
			d.sweepStaleInflightMarkers(ctx, bookkeeping, time.Now(), minOrphanAge)
		}
	}
	if bookkeepingReadable {
		d.sweepOrphanedTombstoneLedger(ctx, ledger, listedSnapshotIDs(snapshots, tombstones), time.Now(), minOrphanAge)
	}

	// Remnant-orphan detection (always-on; deletion stays gated by opts.Delete).
	// A remnant is an unstamped dataset whose in-flight creation marker survived
	// a controller crash and whose same-name CreateVolume retry is never coming
	// (VolSync mints a new PVC UID on failure). It runs after the stale-marker
	// sweep so a marker the sweep just retired is re-read from the backend, and
	// every gate below re-validates live state, so the pass-snapshot the markers
	// came from can never authorize a destroy on its own.
	d.classifyRemnantOrphans(ctx, time.Now(), minOrphanAge, &report)
	if ctxErr := ctx.Err(); ctxErr != nil {
		RecordReconcileFailure("remnant_orphan_classification")
		return report, ctxErr
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
		// The listing strips property source, so an inherited managed_resource — a
		// user dataset nested under a live CSI volume — is indistinguishable from a
		// local stamp here. Re-fetch the candidate with source and require a LOCAL
		// managed_resource: only then did this driver create the dataset. The
		// candidate set is small (already filtered to non-live, age-eligible
		// datasets), which bounds the extra API cost.
		localManaged, getErr := d.datasetHasLocalManagedResource(ctx, ds.Name)
		if getErr != nil {
			d.recordReconcileObjectFailure("orphan_volume_classify", ds.Name, getErr)
			continue
		}
		if !localManaged {
			klog.V(4).Infof("Orphan reconcile: skipping %s because managed_resource is inherited, not local", ds.Name)
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

	// Spent-restore classification is read-only detection, gated on the
	// VolSync-specific reconcile.spentRestore.enabled flag (default true). It is
	// NOT gated on the global zfs.detachedVolumesFromSnapshots flag: a
	// StorageClass may opt into snapshotRestoreMode=detached while that global
	// default stays clone, and gating on the global flag alone would leak that
	// class's spent volsync restore snapshots (never reaped). When disabled,
	// orphan volume/snapshot detection, orphaned-share detection, and tombstone
	// sweeping still run. Deletion remains gated by opts.Delete and the
	// per-object revalidation in deleteDetectedOrphans.
	if d.config.Reconcile.SpentRestore.EnabledOrDefault() {
		report.SpentRestoreSnapshots = d.classifySpentRestoreSnapshots(ctx, now, kubeState, &report)
		if ctxErr := ctx.Err(); ctxErr != nil {
			RecordReconcileFailure("spent_restore_classification")
			return report, ctxErr
		}
	}

	// Orphaned-share detection (a share whose dataset is gone) always runs so the
	// residue is visible even in dry-run; deletion stays gated by opts.Delete.
	d.detectOrphanedShares(ctx, kubeState, &report)
	if ctxErr := ctx.Err(); ctxErr != nil {
		RecordReconcileFailure("orphan_share_detection")
		return report, ctxErr
	}

	// Legacy stamp adoption (always-on; NOT gated by opts.Delete). Stamps
	// driver_instance_id onto migration-era datasets that predate the v1.2.21
	// ownership stamp so the delete-mode tombstone reaper — which refuses any
	// source dataset lacking this instance's stamp — can finally act on their
	// tombstones. It runs before tombstone sweeping so a freshly adopted source
	// unblocks reaping in the SAME pass.
	d.adoptLegacyOwnershipStamps(ctx, datasets, &report, d.config.Reconcile.Delete.MaxPerRun)
	if ctxErr := ctx.Err(); ctxErr != nil {
		RecordReconcileFailure("stamp_adoption")
		return report, ctxErr
	}

	logAction := "[DRY RUN] would delete"
	if opts.Delete {
		logAction = "will attempt guarded delete of"
	}
	for i := range report.OrphanSnapshots {
		orphan := &report.OrphanSnapshots[i]
		klog.Infof("Orphan reconcile: %s managed snapshot %s (backend=%s age=%v bytes=%d)",
			logAction, orphan.ID, orphan.BackendID, orphan.Age, orphan.Bytes)
	}
	for i := range report.OrphanVolumes {
		orphan := &report.OrphanVolumes[i]
		klog.Infof("Orphan reconcile: %s managed volume %s (backend=%s pv=%s age=%v bytes=%d)",
			logAction, orphan.ID, orphan.BackendID, orphan.PVName, orphan.Age, orphan.Bytes)
	}
	for i := range report.OrphanShares {
		orphan := &report.OrphanShares[i]
		klog.Infof("Orphan reconcile: %s orphaned share %s (backend=%s volume=%s protocol=%s)",
			logAction, orphan.ID, orphan.BackendID, orphan.SourceVolumeID, orphan.Protocol)
	}
	for i := range report.TombstoneSnapshots {
		tombstone := &report.TombstoneSnapshots[i]
		klog.Infof("Orphan reconcile: %s released deferred-delete tombstone %s (age=%v)",
			logAction, tombstone.ID, tombstone.Age)
	}
	for i := range report.RemnantVolumes {
		remnant := &report.RemnantVolumes[i]
		klog.Infof("Orphan reconcile: %s remnant orphan volume %s (backend=%s age=%v bytes=%d)",
			logAction, remnant.ID, remnant.BackendID, remnant.Age, remnant.Bytes)
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
	currentState, err := d.loadKubernetesReconcileState(ctx, minOrphanAge)
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

// datasetUnderParent reports whether datasetName lives below the configured
// parent dataset. It is the driver-instance scoping guard for block-protocol
// share orphans whose backreference (iSCSI extent comment, NVMe-oF namespace
// device path) does not itself carry the driver instance name: only datasets
// this driver instance owns may be classified and swept.
func (d *Driver) datasetUnderParent(datasetName string) bool {
	return strings.HasPrefix(datasetName, d.parentDatasetName()+"/")
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
	for _, subsys := range subsystems {
		if subsys == nil {
			continue
		}
		namespaces, nsErr := d.truenasClient.NVMeoFNamespaceListBySubsystem(ctx, subsys.ID)
		if nsErr != nil {
			klog.Warningf("Orphan reconcile: skipping NVMe-oF subsystem %d orphan check: namespace listing failed: %v", subsys.ID, nsErr)
			continue
		}
		// The namespace DevicePath (zvol/<dataset>) is the authoritative
		// backreference; the subsystem NAME is lossy and never used to decide
		// deletion. A subsystem with no namespace resolving to a dataset under the
		// parent is foreign and skipped.
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

// listAllManagedSnapshots fetches the parent's snapshots in a SINGLE round trip
// and partitions them in memory into live CSI-managed snapshots and the driver's
// own tombstone-named deferred-delete markers. Tombstones are never CSI snapshots
// (their identity is stripped at rename), so they must be gathered separately for
// GC.
//
// The previous implementation re-called SnapshotListAll once per page. On TrueNAS
// 26.0 the zfs.resource.snapshot.query backend offers no server-side filter or
// pagination, so every call transferred (and re-sorted) the ENTIRE snapshot set
// just to slice one page out — O(N²) wire volume per reconcile pass. Fetching the
// full set once (limit=0) and partitioning in memory collapses that to a single
// transfer; sorting/paging is then pure memory work.
func (d *Driver) listAllManagedSnapshots(ctx context.Context) (managed, tombstones []*truenas.Snapshot, err error) {
	all, err := d.truenasClient.SnapshotListAll(ctx, d.config.ZFS.DatasetParentName, 0, 0)
	if err != nil {
		return nil, nil, err
	}
	for _, snap := range all {
		switch {
		case isCSISnapshot(snap):
			managed = append(managed, snap)
		case isSnapshotTombstone(snap):
			tombstones = append(tombstones, snap)
		}
	}
	return managed, tombstones, nil
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

func (d *Driver) loadKubernetesReconcileState(ctx context.Context, minOrphanAge time.Duration) (*kubernetesReconcileState, error) {
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
			// Pre-provisioned contents declare their backend handle in
			// spec.source before the snapshotter ever populates status; that
			// handle is authoritative and keeps the content a live grant.
			sourceHandle, sourceFound, sourceErr := unstructured.NestedString(
				content.Object, "spec", "source", "snapshotHandle",
			)
			switch {
			case sourceErr == nil && sourceFound && sourceHandle != "":
				handle = sourceHandle
			case !content.GetCreationTimestamp().Time.IsZero() &&
				time.Since(content.GetCreationTimestamp().Time) < minOrphanAge:
				// Dynamic content mid-creation (e.g. the nightly GC run racing
				// VolSync's hourly snapshot schedule). Its backend snapshot
				// cannot be older than the content object, and every guarded
				// snapshot delete re-proves age >= minOrphanAge immediately
				// before destroy, so content younger than the gate cannot own
				// a delete-eligible backend snapshot. Skip it without failing
				// the whole snapshot deletion pass closed. The safety margin
				// of this carve-out is minOrphanAge minus TrueNAS<->kube-API
				// clock skew: it is enormous at the 24h default but shrinks if
				// an operator tunes minOrphanAge down to minutes.
				klog.V(2).Infof(
					"Orphan reconcile: ignoring in-flight driver VolumeSnapshotContent %s (age %v < min orphan age %v, status.snapshotHandle not yet populated)",
					content.GetName(), time.Since(content.GetCreationTimestamp().Time).Round(time.Second), minOrphanAge,
				)
				continue
			default:
				klog.Warningf(
					"Orphan reconcile: skipping driver VolumeSnapshotContent %s because status.snapshotHandle is unavailable",
					content.GetName(),
				)
				state.handlelessSnapshotContentNames = append(state.handlelessSnapshotContentNames, content.GetName())
				continue
			}
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

// spentRestoreDeferredPVCPhase reports whether a source PVC that EXISTS in the
// given phase indicates an incomplete or stalled restore whose backend snapshot
// must NOT be reaped. Pending and Lost (and any unknown/empty phase) defer
// conservatively; Bound does not (the restore completed) and neither does
// "Released" (the restore's PV was already let go — torn down), so in both those
// cases the snapshot is genuinely spent. A missing PVC is handled by the caller.
func spentRestoreDeferredPVCPhase(phase corev1.PersistentVolumeClaimPhase) bool {
	switch phase {
	case corev1.ClaimBound:
		return false
	case corev1.ClaimPending, corev1.ClaimLost:
		return true
	}
	// "Released" is a PV phase sometimes observed on a lingering PVC after its PV
	// was released; it means the restore is torn down, so the snapshot is spent.
	if phase == corev1.PersistentVolumeClaimPhase("Released") {
		return false
	}
	return true
}

func (d *Driver) classifySpentRestoreSnapshots(
	ctx context.Context,
	now time.Time,
	state *kubernetesReconcileState,
	report *ReconcileReport,
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
		if exists {
			if pvc.Status.Phase == corev1.ClaimBound {
				// Restore completed: the backend snapshot is spent and reclaimable.
				continue
			}
			if spentRestoreDeferredPVCPhase(pvc.Status.Phase) {
				// A PVC that exists in Pending, Lost, or an unknown phase means the
				// restore is incomplete or stalled. The backend snapshot still backs
				// it, so classifying it as spent could destroy an in-flight restore.
				// Defer with an operator-visible reason. (A Released PVC means the
				// restore's PV was already released — torn down — so it stays spent.)
				klog.V(2).Infof("Orphan reconcile: deferring spent-restore snapshot %s/%s: source PVC %s is %s",
					snapshot.GetNamespace(), snapshot.GetName(), sourcePVC, pvc.Status.Phase)
				d.recordReconcileSkip(report, "spent-restore-snapshot", namespacedName(snapshot.GetNamespace(), snapshot.GetName()),
					fmt.Sprintf("source PVC %s is %s (restore incomplete)", sourcePVC, pvc.Status.Phase))
				continue
			}
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
			fmt.Sprintf("%s (maxPerRun=%d)", deletionCapReasonPrefix, maxPerRun),
		)
		return true
	}

	if snapshotDeleteBlockReason != "" {
		klog.Errorf("Orphan reconcile: snapshot deletion pass skipped: %s", snapshotDeleteBlockReason)
		for i := range report.OrphanSnapshots {
			d.recordReconcileSkip(report, "snapshot", report.OrphanSnapshots[i].ID, snapshotDeleteBlockReason)
		}
	} else {
		for i := range report.OrphanSnapshots {
			orphan := &report.OrphanSnapshots[i]
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
			if safe, reason := d.revalidateOrphanSnapshot(ctx, *orphan, minOrphanAge); !safe {
				d.recordReconcileSkip(report, "snapshot", orphan.ID, reason)
				continue
			}
			if safe, reason := d.hardRecheckSnapshotContentAbsent(ctx, *orphan); !safe {
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

	for i := range report.OrphanVolumes {
		orphan := &report.OrphanVolumes[i]
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
		if safe, reason := d.revalidateOrphanVolume(ctx, *orphan, minOrphanAge); !safe {
			d.recordReconcileSkip(report, "volume", orphan.ID, reason)
			continue
		}
		if safe, reason := d.hardRecheckPersistentVolumeAbsent(ctx, *orphan); !safe {
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

	for i := range report.TombstoneSnapshots {
		tombstone := &report.TombstoneSnapshots[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		if deletionCapReached("tombstone-snapshot", tombstone.ID) {
			continue
		}
		reaped, reason := d.reapTombstoneSnapshot(ctx, *tombstone, minOrphanAge)
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

	// Remnant-orphan destroy shares the per-run deletion cap with every other
	// guarded delete above. Each remnant re-proves its marker nonce, unstamped
	// dataset, origin binding, and Kubernetes absence live immediately before the
	// non-recursive destroy.
	for i := range report.RemnantVolumes {
		remnant := &report.RemnantVolumes[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		if deletionCapReached("remnant-volume", remnant.ID) {
			continue
		}
		destroyed, reason := d.destroyRemnantOrphan(ctx, *remnant)
		if !destroyed {
			d.recordReconcileSkip(report, "remnant-volume", remnant.ID, reason)
			continue
		}
		report.DeletedRemnants = append(report.DeletedRemnants, remnant.ID)
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
	// Re-prove driver provenance from a fresh dual-location read under the lock.
	// Relocation (bookkeeping.enabled) writes new ledger entries to the dedicated
	// child dataset and cleanupParent removes migrated ones from the parent, so a
	// valid entry may live in either location. A parent-only read here would
	// permanently refuse to reap any tombstone whose entry exists only on the
	// child (and, once cleanupParent runs, every tombstone).
	parent, parentErr := d.truenasClient.DatasetGet(ctx, d.parentDatasetName())
	if parentErr != nil {
		return false, fmt.Sprintf("tombstone ledger revalidation failed: %v", parentErr)
	}
	ledger := tombstoneLedgerFromDataset(parent)
	if d.bookkeepingEnabled() {
		bookkeeping, bkErr := d.truenasClient.DatasetGet(ctx, d.bookkeepingDatasetName())
		if bkErr != nil {
			// An absent child is legitimate (no entry migrated or written yet);
			// any other read failure must fail closed, not silently downgrade to
			// a parent-only decision.
			if !truenas.IsNotFoundError(bkErr) {
				return false, fmt.Sprintf("tombstone ledger revalidation failed: %v", bkErr)
			}
		} else {
			for key, childEntry := range tombstoneLedgerFromDataset(bookkeeping) {
				ledger[key] = childEntry
			}
		}
	}
	entry, recorded := ledger[tombstoneLedgerKey(snapshot.ID)]
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
	if err := d.removeBookkeepingProperties(ctx, staleKeys); err != nil {
		d.recordReconcileObjectFailure("inflight_marker_sweep", d.parentDatasetName(), err)
		return
	}
	klog.Infof("Orphan reconcile: retired %d stale in-flight creation markers", len(staleKeys))
}

// localInflightMarkers extracts every parseable LOCAL in-flight creation marker
// from a dataset, keyed by property key. Version and instance filtering is left
// to the caller (matching sweepStaleInflightMarkers), so foreign or newer-version
// markers are visible to the same guards that ignore them.
func localInflightMarkers(ds *truenas.Dataset) map[string]*inflightMarker {
	markers := make(map[string]*inflightMarker)
	if ds == nil {
		return markers
	}
	for key, property := range ds.UserProperties {
		if !strings.HasPrefix(key, PropInflightMarkerPrefix) || !isLocalUserPropertySource(property.Source) {
			continue
		}
		var marker inflightMarker
		if err := json.Unmarshal([]byte(property.Value), &marker); err != nil {
			klog.Warningf("Ignoring unparseable in-flight marker %s: %v", key, err)
			continue
		}
		markers[key] = &marker
	}
	return markers
}

// validVolumeIDLeaf mirrors datasetForID's identity rules for a single path
// leaf: it must be non-empty, not a path traversal component, and contain no
// path separator. It guards the marker-derived volume ID before it is used to
// build dataset paths or property keys.
func validVolumeIDLeaf(id string) bool {
	return id != "" && !strings.ContainsAny(id, "/") && id != "." && id != ".."
}

// datasetHasLocalOwnershipStamp reports whether a dataset carries a LOCAL
// driver-instance or managed_resource ownership property. A dataset with either
// stamp is owned (creation completed or explicitly adopted) and is therefore NOT
// an unstamped in-flight remnant. Inherited values are not proof of ownership.
func datasetHasLocalOwnershipStamp(ds *truenas.Dataset) bool {
	if ds == nil {
		return false
	}
	if owner, ok := datasetUserPropertyProjection(ds, PropDriverInstanceID); ok && isLocalUserPropertySource(owner.Source) {
		return true
	}
	return datasetHasLocalUserProperty(ds, PropManagedResource, "true")
}

// remnantHasNoKubernetesReference live-lists PersistentVolumes and
// VolumeAttachments (NOT informer caches) and reports whether any object owned
// by this driver references volumeID. It is the classification and pre-destroy
// hard-recheck for remnant orphans, mirroring liveVolumeAttachmentExists.
func (d *Driver) remnantHasNoKubernetesReference(ctx context.Context, volumeID string) (safe bool, reason string) {
	clientset, _, err := d.kubernetesReconcileClients()
	if err != nil {
		return false, err.Error()
	}
	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Sprintf("live PersistentVolume list for remnant recheck: %v", err)
	}
	handlesByPV := make(map[string]string)
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != d.name {
			continue
		}
		handlesByPV[pv.Name] = pv.Spec.CSI.VolumeHandle
		if pv.Spec.CSI.VolumeHandle == volumeID {
			return false, fmt.Sprintf("PersistentVolume %s references remnant volume %s", pv.Name, volumeID)
		}
	}
	// Volume names derive from the PVC UID (the CreateVolume name), and a PV's
	// spec.csi.volumeHandle equals that name. A remnant has no PV because
	// provisioning never completed, and a Pending PVC that would retry this exact
	// volume re-enters CreateVolume and recovers the remnant through its marker
	// (recoverInFlightContentSourceRemnant) rather than binding a competing PV —
	// so a missing PV is sufficient proof no claim references the remnant, and no
	// separate PVC scan is needed. A VolumeAttachment, by contrast, can outlive a
	// deleted PV (operator force-finalizer), so it is rechecked live as well.
	attachments, err := clientset.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Sprintf("live VolumeAttachment list for remnant recheck: %v", err)
	}
	for i := range attachments.Items {
		attachment := &attachments.Items[i]
		if attachment.Spec.Attacher != d.name || attachment.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		pvName := *attachment.Spec.Source.PersistentVolumeName
		if handlesByPV[pvName] == volumeID || pvName == volumeID {
			return false, fmt.Sprintf("VolumeAttachment %s references remnant volume %s", attachment.Name, volumeID)
		}
	}
	return true, ""
}

// classifyRemnantOrphans detects remnant orphans: unstamped datasets whose
// in-flight creation marker survived a controller crash and whose same-name
// CreateVolume retry is never coming (VolSync mints a NEW PVC UID on failure, so
// the marker — not a retry — is the only thing that can ever act on the remnant).
// Each candidate must satisfy, all proven live: a valid local marker for THIS
// instance whose dataset sits strictly under the CSI parent (datasetForID-style
// validation), marker age beyond minOrphanAge, an existing UNSTAMPED dataset, and
// no referencing Kubernetes object. Detection is read-only; the guarded destroy
// runs in deleteDetectedOrphans under opts.Delete. A stamped dataset is left to
// the stale-marker sweep (marker retirement) and the orphan-volume pass (dataset
// reclamation) — this phase never touches it.
func (d *Driver) classifyRemnantOrphans(ctx context.Context, now time.Time, minOrphanAge time.Duration, report *ReconcileReport) {
	parentDataset, err := d.truenasClient.DatasetGet(ctx, d.parentDatasetName())
	if err != nil {
		d.recordReconcileObjectFailure("remnant_orphan_classify", d.parentDatasetName(), err)
		return
	}
	markers := localInflightMarkers(parentDataset)
	if d.bookkeepingEnabled() {
		bookkeeping, bkErr := d.truenasClient.DatasetGet(ctx, d.bookkeepingDatasetName())
		if bkErr != nil && !truenas.IsNotFoundError(bkErr) {
			d.recordReconcileObjectFailure("remnant_orphan_classify", d.bookkeepingDatasetName(), bkErr)
			return
		}
		// Dual-read merge: the same marker carries the same content-hashed key in
		// both locations, so the union is lossless and a migrated marker is seen
		// regardless of which dataset still holds it.
		for key, marker := range localInflightMarkers(bookkeeping) {
			markers[key] = marker
		}
	}
	parentName := d.parentDatasetName()
	for key, marker := range markers {
		if marker.Version != inflightMarkerVersion || marker.Instance != d.driverInstanceID() || marker.Dataset == "" {
			continue
		}
		volumeID := path.Base(marker.Dataset)
		if !datasetStrictlyBelowParent(marker.Dataset, parentName) || !validVolumeIDLeaf(volumeID) || key != inflightMarkerKey(volumeID) {
			continue
		}
		if volumeID == bookkeepingDatasetLeaf {
			// Belt-and-suspenders: the bookkeeping dataset is never a volume and
			// must never be classified, whatever properties it carries.
			continue
		}
		startedAt, parseErr := time.Parse(time.RFC3339Nano, marker.StartedAt)
		if parseErr != nil || now.Sub(startedAt) <= minOrphanAge {
			continue
		}
		dataset, getErr := d.truenasClient.DatasetGet(ctx, marker.Dataset)
		if getErr != nil {
			if truenas.IsNotFoundError(getErr) {
				// Dataset gone: nothing to destroy; the stale-marker sweep retires
				// the marker. Not a remnant orphan.
				continue
			}
			d.recordReconcileObjectFailure("remnant_orphan_classify", marker.Dataset, getErr)
			continue
		}
		if datasetHasLocalOwnershipStamp(dataset) {
			klog.V(4).Infof("Orphan reconcile: skipping remnant candidate %s because it carries a local ownership stamp", marker.Dataset)
			continue
		}
		if safe, k8sReason := d.remnantHasNoKubernetesReference(ctx, volumeID); !safe {
			klog.V(2).Infof("Orphan reconcile: skipping remnant candidate %s: %s", marker.Dataset, k8sReason)
			continue
		}
		report.RemnantVolumes = append(report.RemnantVolumes, ReconcileObject{
			ID:           volumeID,
			BackendID:    marker.Dataset,
			PVName:       volumeID,
			CreatedAt:    startedAt,
			Age:          now.Sub(startedAt),
			Bytes:        dataset.GetUsedBytes(),
			remnantNonce: marker.Nonce,
		})
	}
}

// destroyRemnantOrphan removes a classified remnant orphan under opts.Delete.
// Immediately before the non-recursive destroy it re-proves, live: the marker is
// still present with an identical nonce (a rewritten marker means a fresh create
// owns the dataset now), the dataset still exists and is unstamped, the dataset's
// actual ZFS origin matches the marker (clone mode) or is empty (detached copy),
// and no Kubernetes object references the volume. The destroy is NON-recursive
// with force=false so any child dataset or snapshot under the remnant FAILS the
// delete (fail-safe) and is surfaced as a skip reason rather than destroyed
// silently. On success the marker is retired from both bookkeeping locations and
// a Warning event is recorded so operators see the reap.
func (d *Driver) destroyRemnantOrphan(ctx context.Context, remnant ReconcileObject) (destroyed bool, reason string) {
	volumeID := remnant.ID
	// Serialize against any concurrent CSI operation on the same volume name —
	// in particular a same-name CreateVolume RESUME, whose marker keeps the
	// original StartedAt (aged past the gate) and nonce, defeating the age and
	// nonce guards; the per-volume lock is the discriminator the sibling
	// reconcile delete paths rely on.
	lockKey := "volume:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return false, "volume operation is in progress"
	}
	defer d.releaseOperationLock(lockKey)
	marker, err := d.readInflightMarker(ctx, volumeID)
	if err != nil {
		return false, fmt.Sprintf("remnant marker revalidation failed: %v", err)
	}
	if marker == nil || marker.Instance != d.driverInstanceID() || marker.Dataset != remnant.BackendID {
		return false, "in-flight marker is no longer present for this remnant"
	}
	if marker.Nonce != remnant.remnantNonce {
		return false, "in-flight marker nonce changed (a new create attempt owns the remnant)"
	}
	dataset, err := d.truenasClient.DatasetGet(ctx, marker.Dataset)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			// Already gone: the goal is met; retire the marker and report success.
			d.deleteInflightMarker(ctx, volumeID)
			return true, ""
		}
		return false, fmt.Sprintf("remnant dataset revalidation failed: %v", err)
	}
	if datasetHasLocalOwnershipStamp(dataset) {
		return false, "remnant dataset became stamped (creation completed)"
	}
	actualOrigin := datasetOriginSnapshotID(dataset)
	switch marker.Mode {
	case inflightModeClone:
		if marker.Origin == "" || actualOrigin != marker.Origin {
			return false, fmt.Sprintf("remnant origin %q does not match marker origin %q", actualOrigin, marker.Origin)
		}
	case inflightModeCopy:
		if actualOrigin != "" {
			return false, fmt.Sprintf("detached-copy remnant has unexpected origin %q", actualOrigin)
		}
	default:
		return false, fmt.Sprintf("remnant marker has unrecognized mode %q", marker.Mode)
	}
	if safe, k8sReason := d.remnantHasNoKubernetesReference(ctx, volumeID); !safe {
		return false, k8sReason
	}
	if delErr := d.truenasClient.DatasetDelete(ctx, marker.Dataset, false, false); delErr != nil {
		if truenas.IsNotFoundError(delErr) {
			d.deleteInflightMarker(ctx, volumeID)
			return true, ""
		}
		return false, fmt.Sprintf("guarded remnant destroy refused: %v", delErr)
	}
	d.deleteInflightMarker(ctx, volumeID)
	d.recordWarningEvent(volumeEventRef(volumeID), EventReasonRemnantOrphanReaped,
		fmt.Sprintf("Reaped remnant orphan dataset %s (origin %q, mode %s) left unstamped after a crashed create",
			marker.Dataset, actualOrigin, marker.Mode))
	klog.Infof("Orphan reconcile: destroyed remnant orphan volume %s (backend=%s origin=%q)", volumeID, marker.Dataset, actualOrigin)
	return true, ""
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
	if d.bookkeepingEnabled() {
		// Dual-read: copy markers may live on the dedicated bookkeeping dataset.
		// A marker in either location protects its in-flight job from being aborted.
		bookkeeping, bkErr := d.truenasClient.DatasetGet(ctx, d.bookkeepingDatasetName())
		if bkErr != nil && !truenas.IsNotFoundError(bkErr) {
			return fmt.Errorf("read bookkeeping dataset before replication job sweep: %w", bkErr)
		}
		for dataset := range d.liveCopyMarkers(bookkeeping) {
			markers[dataset] = struct{}{}
		}
	}

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
//
// listedIDs is the set of snapshot IDs observed in THIS pass's single snapshot
// listing. Existence is resolved against it first so the sweep no longer issues
// a full SnapshotGet per ledger entry (each of which re-transferred the entire
// parent snapshot payload on TrueNAS 26.0). A live SnapshotGet confirms only the
// rare entry absent from the listing, preserving the exact not-found-vs-error
// semantics without the per-entry payload cost.
func (d *Driver) sweepOrphanedTombstoneLedger(ctx context.Context, ledger map[string]tombstoneLedgerEntry, listedIDs map[string]struct{}, now time.Time, minAge time.Duration) {
	staleKeys := make([]string, 0)
	for key, entry := range ledger {
		if _, listed := listedIDs[entry.Snapshot]; listed {
			// Tombstone present in this pass's listing: its provenance must stay so
			// the reaper can still reclaim it. Never age out a live tombstone's entry.
			continue
		}
		// Absent from the listing: confirm with a live lookup so a transient
		// listing gap can never retire a live tombstone's provenance.
		if _, err := d.truenasClient.SnapshotGet(ctx, entry.Snapshot); err == nil {
			continue
		} else if !truenas.IsNotFoundError(err) {
			d.recordReconcileObjectFailure("tombstone_ledger_sweep", entry.Snapshot, err)
			continue
		}
		// The snapshot is genuinely gone: age-gate so an in-progress DeleteSnapshot
		// (crash window between ledger write and rename) is never raced.
		if renamedAt, parseErr := time.Parse(time.RFC3339Nano, entry.RenamedAt); parseErr == nil && now.Sub(renamedAt) <= minAge {
			continue
		}
		staleKeys = append(staleKeys, key)
	}
	if len(staleKeys) == 0 {
		return
	}
	sort.Strings(staleKeys)
	if err := d.removeBookkeepingProperties(ctx, staleKeys); err != nil {
		d.recordReconcileObjectFailure("tombstone_ledger_sweep", d.parentDatasetName(), err)
		return
	}
	for _, key := range staleKeys {
		delete(ledger, key)
	}
	klog.Infof("Orphan reconcile: retired %d orphaned tombstone ledger entries", len(staleKeys))
}

// listedSnapshotIDs builds the set of full snapshot IDs present in a reconcile
// pass's snapshot listing (CSI-managed snapshots plus tombstone markers).
func listedSnapshotIDs(snapshots, tombstones []*truenas.Snapshot) map[string]struct{} {
	ids := make(map[string]struct{}, len(snapshots)+len(tombstones))
	for _, snap := range snapshots {
		if snap != nil {
			ids[snap.ID] = struct{}{}
		}
	}
	for _, snap := range tombstones {
		if snap != nil {
			ids[snap.ID] = struct{}{}
		}
	}
	return ids
}

// datasetHasLocalManagedResource re-fetches datasetName with property source and
// reports whether managed_resource="true" is stamped LOCALLY on it. The reconcile
// listing does not carry property source, so a dataset that merely inherits
// managed_resource from a CSI-managed ancestor (e.g. a user dataset nested under
// a live volume) must be re-fetched to avoid classifying it as a managed orphan.
func (d *Driver) datasetHasLocalManagedResource(ctx context.Context, datasetName string) (bool, error) {
	dataset, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return false, err
	}
	return datasetHasLocalUserProperty(dataset, PropManagedResource, "true"), nil
}

// adoptLegacyOwnershipStamps stamps driver_instance_id onto legacy managed
// datasets that provably belong to this cluster's Bound volumes but predate the
// v1.2.21 ownership stamp (the 2026-07-23 04:00Z incident: the reaper refused
// every age-eligible migration-era tombstone because its source dataset carried
// LOCAL managed_resource + csi_volume_name but NO instance stamp, so the ledger
// entries and -csi-deleted- snapshots accumulated forever).
//
// This is a WRITE that runs in detection mode (it is NOT gated by opts.Delete).
// That is safe and deliberate: it only adds provenance to datasets that are
// provably this cluster's Bound volumes (a live Bound PV of THIS driver
// references them, and they carry this driver's own LOCAL managed_resource +
// csi_volume_name); it deletes nothing; and it is required for the delete-mode
// reaper to ever act on legacy tombstones. Adoptions are capped at maxPerRun
// per pass as a blast-radius bound.
//
// Absolute rule: an existing driver_instance_id of ANY source (local,
// inherited, or foreign) is NEVER overwritten — a dataset stamped by another
// driver instance sharing the pool must not be hijacked.
//
// Residual: a legacy dataset that is NOT currently Bound is never adopted, so
// its tombstones stay refused (fail-safe); operators can bind it or clean it up
// manually.
func (d *Driver) adoptLegacyOwnershipStamps(ctx context.Context, datasets []*truenas.Dataset, report *ReconcileReport, maxPerRun int) {
	boundHandles, ok := d.liveBoundVolumeHandles(ctx)
	if !ok {
		// Fail-safe: a PV-list error or an empty view of this driver's PVs is an
		// API discontinuity, not evidence that adoption is safe — adopt nothing.
		return
	}
	parentName := d.parentDatasetName()
	for _, ds := range datasets {
		if maxPerRun > 0 && len(report.AdoptedStamps) >= maxPerRun {
			break
		}
		if ds == nil {
			continue
		}
		volumeID := path.Base(ds.Name)
		if !datasetStrictlyBelowParent(ds.Name, parentName) || !validVolumeIDLeaf(volumeID) || volumeID == bookkeepingDatasetLeaf {
			continue
		}
		// Source-bearing re-read (batch-12 DatasetGet pattern): the listing strips
		// property source, so it is never trusted for the ownership/source checks.
		candidate, err := d.truenasClient.DatasetGet(ctx, ds.Name)
		if err != nil {
			if !truenas.IsNotFoundError(err) {
				d.recordReconcileObjectFailure("stamp_adoption", ds.Name, err)
			}
			continue
		}
		// LOCAL managed_resource AND LOCAL csi_volume_name matching the dataset
		// leaf prove this driver created the dataset (a same-name CreateVolume
		// derives the leaf from the PVC UID, which is also the PV volumeHandle).
		if !datasetHasLocalUserProperty(candidate, PropManagedResource, "true") {
			continue
		}
		if !datasetHasLocalUserProperty(candidate, PropCSIVolumeName, volumeID) {
			continue
		}
		// Absolute rule: never overwrite an existing instance stamp of any source.
		if _, present := datasetUserPropertyProjection(candidate, PropDriverInstanceID); present {
			continue
		}
		// A live Bound PV of THIS driver must reference the volume.
		if _, bound := boundHandles[volumeID]; !bound {
			continue
		}
		adopted, adoptErr := d.writeAndVerifyAdoptionStamp(ctx, ds.Name, volumeID)
		if adoptErr != nil {
			d.recordReconcileObjectFailure("stamp_adoption", ds.Name, adoptErr)
			continue
		}
		if !adopted {
			continue
		}
		report.AdoptedStamps = append(report.AdoptedStamps, volumeID)
		klog.Infof("Orphan reconcile: adopted ownership stamp on legacy volume %s", volumeID)
	}
}

// liveBoundVolumeHandles live-lists PersistentVolumes (clientset List, NOT
// informer caches) and returns the volume handles referenced by a Bound PV of
// THIS driver. The boolean is false when the list fails or when NO PV references
// this driver at all — the standard fail-safe shared with the remnant classifier:
// an API discontinuity (or an empty view of the driver's PVs) is not evidence
// that adoption is safe, so the caller adopts nothing this pass.
func (d *Driver) liveBoundVolumeHandles(ctx context.Context) (map[string]struct{}, bool) {
	clientset, _, err := d.kubernetesReconcileClients()
	if err != nil {
		return nil, false
	}
	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		d.recordReconcileObjectFailure("stamp_adoption_pv_list", d.name, err)
		return nil, false
	}
	handles := make(map[string]struct{})
	driverPVCount := 0
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != d.name {
			continue
		}
		driverPVCount++
		if pv.Status.Phase == corev1.VolumeBound {
			handles[pv.Spec.CSI.VolumeHandle] = struct{}{}
		}
	}
	if driverPVCount == 0 {
		return nil, false
	}
	return handles, true
}

// writeAndVerifyAdoptionStamp persists driver_instance_id through the proven
// stampAndMirror user-property write path used at create time, then verifies the
// write with a source-bearing re-read before reporting it adopted. It serializes
// on the per-volume lock and re-proves the stamp is still absent immediately
// before writing, so a concurrent create or peer that stamped the dataset between
// the detection read and the write is never overwritten (the absolute rule). It
// returns adopted=false (no error) when the dataset is already stamped — a
// write-free no-op.
func (d *Driver) writeAndVerifyAdoptionStamp(ctx context.Context, datasetName, volumeID string) (bool, error) {
	lockKey := "volume:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return false, fmt.Errorf("volume operation in progress for %s", volumeID)
	}
	defer d.releaseOperationLock(lockKey)
	fresh, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return false, fmt.Errorf("adoption stamp revalidation failed: %w", err)
	}
	if _, present := datasetUserPropertyProjection(fresh, PropDriverInstanceID); present {
		return false, nil
	}
	if stampErr := stampAndMirror(ctx, d.truenasClient, fresh, datasetName, map[string]string{
		PropDriverInstanceID: d.driverInstanceID(),
	}); stampErr != nil {
		return false, stampErr
	}
	reread, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return false, fmt.Errorf("re-read dataset after adoption stamp: %w", err)
	}
	if !datasetHasLocalUserProperty(reread, PropDriverInstanceID, d.driverInstanceID()) {
		return false, fmt.Errorf("adoption stamp did not persist with a local source on %s", datasetName)
	}
	return true, nil
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
	// A dataset that only inherits managed_resource (source != "local") was never
	// created by this driver; revalidation must not bless it for deletion.
	if !datasetHasLocalUserProperty(dataset, PropManagedResource, "true") {
		return false, "backend volume managed_resource is inherited, not local"
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
	if err == nil {
		if pvc.Status.Phase == corev1.ClaimBound {
			return SpentRestoreSnapshot{}, false, "source PVC became Bound during revalidation"
		}
		if spentRestoreDeferredPVCPhase(pvc.Status.Phase) {
			// A PVC that exists in Pending, Lost, or an unknown phase means the
			// restore is incomplete or stalled; the backend snapshot still backs it
			// and must not be reaped. (Released stays spent: the PV was let go.)
			return SpentRestoreSnapshot{}, false, fmt.Sprintf("source PVC %s is %s (restore incomplete)", sourcePVC, pvc.Status.Phase)
		}
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

// datasetHasPublicationRecordKeys reports whether the dataset carries at least one
// publication_* user-property KEY. The TrueNAS 26.0 zfs.resource.query listing
// exposes these keys but NOT their source (user_properties come back as a flat
// string map), so key presence is the cheap, source-independent pre-filter used to
// decide which datasets need a source-bearing re-fetch before their records can be
// classified. It is deliberately over-inclusive — a clone inherits the source
// volume's publication_* keys — but the source-authoritative parse that follows the
// re-fetch narrows back to only this dataset's own (local) records.
func datasetHasPublicationRecordKeys(dataset *truenas.Dataset) bool {
	if dataset == nil {
		return false
	}
	for key := range dataset.UserProperties {
		if strings.HasPrefix(key, publicationPropertyPrefix) {
			return true
		}
	}
	return false
}

// publicationPropertyCount counts publication_* user-property KEYS across the
// listing. It deliberately ignores property source: the zfs.resource.query listing
// is flat/sourceless on TrueNAS 26.0, so source is unavailable here. The count only
// feeds the mass-absence brake — a heuristic that defers all revocation when the
// VolumeAttachment list looks empty while several records exist — where counting
// clone-inherited keys as well is safe: it biases the brake toward deferral
// (inaction) rather than mass revocation. Source-authoritative classification
// happens later, per candidate dataset, after a source-bearing re-fetch.
func publicationPropertyCount(datasets []*truenas.Dataset) int {
	count := 0
	for _, dataset := range datasets {
		if dataset == nil {
			continue
		}
		for key := range dataset.UserProperties {
			if strings.HasPrefix(key, publicationPropertyPrefix) {
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
		// The zfs.resource.query listing returns user_properties as a flat,
		// SOURCELESS map on TrueNAS 26.0, but publicationRecordsFromDataset must
		// distinguish a dataset's own (source=="local") records from clone-inherited
		// ones by source: run against the listing directly, it would skip every
		// record and silently disable this repair. Pre-filter cheaply on
		// publication_* KEY presence (the flat read still exposes keys) and, for the
		// few candidates whose listing came from the sourceless resource path,
		// re-fetch through the source-bearing pool.dataset.query read (DatasetGet)
		// before classifying. A source-bearing listing — the pool.dataset.query
		// fallback — is already authoritative and is used as-is. This costs a
		// handful of DatasetGets (one per sourceless dataset that carries a
		// publication record), not one per managed dataset.
		recordSource := dataset
		if dataset.ResourceQuery && datasetHasPublicationRecordKeys(dataset) {
			sourceBearing, getErr := d.truenasClient.DatasetGet(ctx, dataset.Name)
			if getErr != nil {
				d.recordReconcileObjectFailure("stale_publication_classification", dataset.Name, getErr)
				continue
			}
			recordSource = sourceBearing
		}
		records, parseErr := publicationRecordsFromDataset(recordSource)
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
				klog.Infof("Orphan reconcile detection complete: volumes=%d snapshots=%d spentRestoreSnapshots=%d tombstones=%d remnants=%d adoptedStamps=%d",
					report.OrphanVolumeCount, report.OrphanSnapshotCount, report.SpentRestoreSnapshotCount, report.TombstoneSnapshotCount, report.RemnantVolumeCount, report.AdoptedStampCount)
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
