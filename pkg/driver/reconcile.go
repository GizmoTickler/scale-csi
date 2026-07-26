package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	datasetGetByNamesBatchBudget             = 32 * 1024
	datasetGetByNamesEnvelopeHeadroom        = 1024
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
	// tombstoneScanFallback marks a tombstone discovered by the scan fallback
	// (reconcile.tombstoneReaper.scanFallback) rather than the ledger-driven
	// path. The reaper requires retained creation-time snapshot identity, an
	// exact driver rename-algorithm match, and fresh absence from BOTH ledgers
	// before deleting it. Only meaningful for entries in TombstoneSnapshots.
	tombstoneScanFallback bool
}

// ReconcileActionFailure records a guarded cleanup that was skipped or refused.
type ReconcileActionFailure struct {
	Kind   string
	ID     string
	Reason string
}

// ReconcileReport is the complete detection and optional cleanup result.
type ReconcileReport struct {
	OrphanVolumeCount            int
	OrphanSnapshotCount          int
	OrphanShareCount             int
	SpentRestoreSnapshotCount    int
	TombstoneSnapshotCount       int
	ManualRecoveryTombstoneCount int
	RemnantVolumeCount           int
	OrphanVolumeBytes            int64
	OrphanSnapshotBytes          int64
	TombstoneSnapshotBytes       int64
	OrphanVolumes                []ReconcileObject
	OrphanSnapshots              []ReconcileObject
	OrphanShares                 []ReconcileObject
	SpentRestoreSnapshots        []SpentRestoreSnapshot
	TombstoneSnapshots           []ReconcileObject
	ManualRecoveryTombstones     []ReconcileObject
	RemnantVolumes               []ReconcileObject
	DeletedVolumes               []string
	DeletedSnapshots             []string
	DeletedShares                []string
	DeletedSpentRestoreObjects   []string
	DeletedTombstones            []string
	DeletedRemnants              []string
	AdoptedStamps                []string
	SkippedDeletes               []ReconcileActionFailure
	DeleteEnabled                bool
	AdoptedStampCount            int
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
		sort.Slice(report.ManualRecoveryTombstones, func(i, j int) bool {
			return report.ManualRecoveryTombstones[i].ID < report.ManualRecoveryTombstones[j].ID
		})
		sort.Slice(report.RemnantVolumes, func(i, j int) bool { return report.RemnantVolumes[i].ID < report.RemnantVolumes[j].ID })
		sort.Strings(report.AdoptedStamps)
		report.OrphanVolumeCount = len(report.OrphanVolumes)
		report.OrphanSnapshotCount = len(report.OrphanSnapshots)
		report.SpentRestoreSnapshotCount = len(report.SpentRestoreSnapshots)
		report.TombstoneSnapshotCount = len(report.TombstoneSnapshots)
		report.ManualRecoveryTombstoneCount = len(report.ManualRecoveryTombstones)
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

	ledger, parentDataset, remnantBookkeeping := d.readBookkeepingState(ctx, minOrphanAge, snapshots, tombstones)

	// Remnant-orphan detection (always-on; deletion stays gated by opts.Delete).
	// A remnant is an unstamped dataset whose in-flight creation marker survived
	// a controller crash and whose same-name CreateVolume retry is never coming
	// (VolSync mints a new PVC UID on failure). It runs after the stale-marker
	// sweep; safety does NOT depend on the classifier seeing the post-sweep parent
	// read — every per-marker gate below re-validates live state, and
	// destroyRemnantOrphan re-reads the marker under the per-volume lock before any
	// destroy — so reusing the pass's parent/bookkeeping reads is safe.
	d.classifyRemnantOrphans(ctx, time.Now(), minOrphanAge, parentDataset, remnantBookkeeping, &report)
	if ctxErr := ctx.Err(); ctxErr != nil {
		RecordReconcileFailure("remnant_orphan_classification")
		return report, ctxErr
	}

	now := time.Now()
	managedBackendVolumeCount := d.classifyOrphanVolumes(ctx, now, datasets, kubeState, minOrphanAge, &report)
	managedBackendSnapshotCount := d.classifyOrphanSnapshots(now, snapshots, kubeState, minOrphanAge, &report)
	d.classifyTombstones(now, tombstones, ledger, minOrphanAge, &report)

	// Scan fallback (reconcile.tombstoneReaper.scanFallback, default off) runs
	// independently of the strict ledger backlog. It reuses this pass's already
	// fetched tombstone slice, deduplicates strict candidates, and authorizes only
	// snapshots carrying retained creation-time identity that exactly proves the
	// driver's rename output. Unprovable lookalikes are manual-recovery inventory,
	// never delete candidates.
	if d.config.Reconcile.TombstoneReaper.ScanFallback.EnabledOrDefault() {
		d.detectTombstonesByScanFallback(ctx, now, tombstones, ledger, minOrphanAge, &report)
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
		report.SpentRestoreSnapshots = d.classifySpentRestoreSnapshots(ctx, now, kubeState, snapshots, &report)
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

	logReconcilePlan(&report, opts.Delete, minOrphanAge)

	if err := d.runReconcileDeletePhase(ctx, opts, &report, kubeState, minOrphanAge, parentDataset, managedBackendVolumeCount, managedBackendSnapshotCount); err != nil {
		return report, err
	}
	return report, nil
}

// readBookkeepingState performs the dual-read bookkeeping prologue: it reads the
// parent dataset's durable bookkeeping — in-flight creation markers and the
// tombstone ledger — and, when the dedicated bookkeeping child dataset is
// enabled (Fix 4b), merges the child's ledger over the parent's. Reading can
// fail without aborting the pass — tombstone reaping then simply stays empty
// (fail-safe; no ledger, no reaping) and the sweeps are skipped. It runs the
// always-on bookkeeping hygiene sweeps and returns the merged ledger plus the
// parent/bookkeeping datasets threaded into the remnant classifier. Extracted
// verbatim from reconcileOrphans (Batch 18 R2).
func (d *Driver) readBookkeepingState(ctx context.Context, minOrphanAge time.Duration, snapshots, tombstones []*truenas.Snapshot) (map[string]tombstoneLedgerEntry, *truenas.Dataset, *truenas.Dataset) {
	var ledger map[string]tombstoneLedgerEntry
	bookkeepingReadable := false
	var remnantBookkeeping *truenas.Dataset
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
		bookkeepingDataset, bkErr := d.truenasClient.DatasetGet(ctx, d.bookkeepingDatasetName())
		if bkErr != nil && !truenas.IsNotFoundError(bkErr) {
			d.recordReconcileObjectFailure("bookkeeping", d.bookkeepingDatasetName(), bkErr)
		} else {
			bookkeepingReadable = true
			// Dual-read: merge the child's ledger entries over the parent's. Keys
			// are content-hashed snapshot IDs, so a migrated entry is identical in
			// both locations and the merge is a lossless union.
			for key, entry := range tombstoneLedgerFromDataset(bookkeepingDataset) {
				if ledger == nil {
					ledger = make(map[string]tombstoneLedgerEntry)
				}
				ledger[key] = entry
			}
			d.sweepStaleInflightMarkers(ctx, bookkeepingDataset, time.Now(), minOrphanAge)
		}
		// Thread the datasets this pass already read into the remnant classifier so
		// it does not re-fetch them (see classifyRemnantOrphans). parentDataset is
		// non-nil when its read above succeeded; bookkeepingDataset is non-nil when
		// the bookkeeping read succeeded (nil for NotFound/error → the classifier
		// re-reads to tell those apart, preserving its historical behavior).
		remnantBookkeeping = bookkeepingDataset
	}
	if bookkeepingReadable {
		d.sweepOrphanedTombstoneLedger(ctx, ledger, listedSnapshotIDs(snapshots, tombstones), time.Now(), minOrphanAge)
	}
	return ledger, parentDataset, remnantBookkeeping
}

// classifyOrphanVolumes appends age-eligible, non-live managed datasets whose
// managed_resource stamp is local to report.OrphanVolumes and returns the count
// of managed backend volumes observed. Extracted verbatim from reconcileOrphans
// (Batch 18 R2).
func (d *Driver) classifyOrphanVolumes(ctx context.Context, now time.Time, datasets []*truenas.Dataset, kubeState *kubernetesReconcileState, minOrphanAge time.Duration, report *ReconcileReport) int {
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
	return managedBackendVolumeCount
}

// classifyOrphanSnapshots appends age-eligible, non-live CSI snapshots to
// report.OrphanSnapshots and returns the count of managed backend snapshots
// observed. Extracted verbatim from reconcileOrphans (Batch 18 R2).
func (d *Driver) classifyOrphanSnapshots(now time.Time, snapshots []*truenas.Snapshot, kubeState *kubernetesReconcileState, minOrphanAge time.Duration, report *ReconcileReport) int {
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
	return managedBackendSnapshotCount
}

// classifyTombstones appends ledger-proven, age-eligible tombstone snapshots to
// report.TombstoneSnapshots. Tombstone-named snapshots are the driver's own
// deferred-delete markers. On backends without ZFS deferred destroy (TrueNAS
// 26.0) they cannot be removed until their last restored clone is gone, and the
// tombstone rename released their CSI identity, so the CSI-snapshot orphan pass
// never sees them. Classification requires a matching parent-dataset ledger
// entry: the name shape alone is NOT provenance — a user snapshot may
// legitimately be named *-csi-deleted-<n> and must never be counted, reaped, or
// even reported. Extracted verbatim from reconcileOrphans (Batch 18 R2).
func (d *Driver) classifyTombstones(now time.Time, tombstones []*truenas.Snapshot, ledger map[string]tombstoneLedgerEntry, minOrphanAge time.Duration, report *ReconcileReport) {
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
}

// logReconcilePlan emits the per-object dry-run/delete-intent log lines for a
// completed classification pass. Extracted verbatim from reconcileOrphans
// (Batch 18 R2).
func logReconcilePlan(report *ReconcileReport, deleteEnabled bool, minOrphanAge time.Duration) {
	logAction := "[DRY RUN] would delete"
	if deleteEnabled {
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
	for i := range report.ManualRecoveryTombstones {
		tombstone := &report.ManualRecoveryTombstones[i]
		klog.Warningf("Orphan reconcile: manual recovery required for unproven tombstone-shaped snapshot %s (age=%v); it will not be deleted",
			tombstone.ID, tombstone.Age)
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
}

// runReconcileDeletePhase is the delete-gate tail of reconcileOrphans. It is a
// no-op returning nil when opts.Delete is false; otherwise it re-validates the
// safety brakes, re-lists Kubernetes state immediately before mutation, and
// invokes the guarded deleters. Extracted verbatim from reconcileOrphans
// (Batch 18 R2).
func (d *Driver) runReconcileDeletePhase(ctx context.Context, opts ReconcileOptions, report *ReconcileReport, kubeState *kubernetesReconcileState, minOrphanAge time.Duration, parentDataset *truenas.Dataset, managedBackendVolumeCount, managedBackendSnapshotCount int) error {
	if !opts.Delete {
		return nil
	}
	if len(kubeState.volumeHandles) == 0 && managedBackendVolumeCount > 0 {
		RecordReconcileFailure("safety_brake")
		return fmt.Errorf(
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
		return fmt.Errorf("revalidate Kubernetes state before delete: %w", err)
	}
	if len(currentState.volumeHandles) == 0 && managedBackendVolumeCount > 0 {
		RecordReconcileFailure("safety_brake")
		return fmt.Errorf(
			"refusing to GC: zero live PVs for driver but %d managed backend volumes exist — cluster rebuild in progress?",
			managedBackendVolumeCount,
		)
	}
	if reason := snapshotDeletePassBlockReason(currentState, managedBackendSnapshotCount); reason != "" {
		snapshotDeleteBlockReason = reason
	}
	if err := d.deleteDetectedOrphans(
		ctx,
		report,
		currentState,
		minOrphanAge,
		d.config.Reconcile.Delete.MaxPerRun,
		snapshotDeleteBlockReason,
		parentDataset,
	); err != nil {
		RecordReconcileFailure("delete")
		return err
	}
	d.deleteOrphanedShares(ctx, report, d.config.Reconcile.Delete.MaxPerRun)
	return nil
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

// datasetUnderParent reports whether datasetName lives below the configured
// parent dataset. It is the driver-instance scoping guard for block-protocol
// share orphans whose backreference (iSCSI extent comment, NVMe-oF namespace
// device path) does not itself carry the driver instance name: only datasets
// this driver instance owns may be classified and swept.
func (d *Driver) datasetUnderParent(datasetName string) bool {
	return strings.HasPrefix(datasetName, d.parentDatasetName()+"/")
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
	parent *truenas.Dataset,
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

	retire := &tombstoneRetirementBatch{}
	for i := range report.TombstoneSnapshots {
		tombstone := &report.TombstoneSnapshots[i]
		if err := ctx.Err(); err != nil {
			retire.flush(ctx, d, parent)
			return err
		}
		if deletionCapReached("tombstone-snapshot", tombstone.ID) {
			continue
		}
		reaped, reason := d.reapTombstoneSnapshot(ctx, *tombstone, minOrphanAge, retire)
		if !reaped {
			d.recordReconcileSkip(report, "tombstone-snapshot", tombstone.ID, reason)
			continue
		}
		report.DeletedTombstones = append(report.DeletedTombstones, tombstone.ID)
		deletedCount++
	}
	// Batch-remove the retired tombstone ledger entries now (one size-bounded
	// remove per location) instead of per-reap. Best-effort; the orphan-ledger
	// sweep retires anything this fails to remove.
	retire.flush(ctx, d, parent)

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

func datasetStrictlyBelowParent(dataset, parent string) bool {
	dataset = strings.TrimSuffix(strings.TrimSpace(dataset), "/")
	parent = strings.TrimSuffix(strings.TrimSpace(parent), "/")
	return dataset != "" && parent != "" && strings.HasPrefix(dataset, parent+"/")
}

func datasetHasUserProperty(dataset *truenas.Dataset, key string) bool {
	if dataset == nil {
		return false
	}
	_, present := dataset.UserProperties[key]
	return present
}

// bookkeepingDatasetReads holds the raw results of the parent-GET and (optional)
// bookkeeping-child-GET that the dual-read bookkeeping sites share. It carries
// BOTH datasets and BOTH raw errors so each caller keeps its own explicit
// fail-open/fail-closed policy and its own parse/merge: this helper makes no
// policy decision and performs no IsNotFoundError classification itself.
type bookkeepingDatasetReads struct {
	parent    *truenas.Dataset
	parentErr error
	child     *truenas.Dataset
	childErr  error
}

// readBookkeepingDatasets issues the parent-dataset GET and, when readChild is
// true, the bookkeeping-child GET, parent-first (matching the callers' historical
// order). readChild lets each caller keep its exact child-read gate — e.g.
// bookkeepingEnabled(), or that OR a scan-fallback path — so no extra API call is
// issued when the child read was previously skipped. A skipped child read leaves
// child nil and childErr nil, which callers distinguish from a successful read via
// the child != nil check.
func (d *Driver) readBookkeepingDatasets(ctx context.Context, readChild bool) bookkeepingDatasetReads {
	var reads bookkeepingDatasetReads
	reads.parent, reads.parentErr = d.truenasClient.DatasetGet(ctx, d.parentDatasetName())
	if readChild {
		reads.child, reads.childErr = d.truenasClient.DatasetGet(ctx, d.bookkeepingDatasetName())
	}
	return reads
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

// chunkDatasetNames splits names so the typed ["id","in",names] filter remains
// below the 32 KiB request budget, leaving fixed headroom for the JSON-RPC
// envelope and query options. Dataset names are bounded by ZFS, so a single
// entry cannot consume the budget by itself.
func chunkDatasetNames(names []string, budget int) [][]string {
	if len(names) == 0 || budget <= datasetGetByNamesEnvelopeHeadroom {
		return nil
	}
	var chunks [][]string
	var current []string
	for _, name := range names {
		candidate := append(append([]string(nil), current...), name)
		encoded, err := json.Marshal(candidate)
		if err != nil {
			continue
		}
		if len(current) > 0 && len(encoded)+datasetGetByNamesEnvelopeHeadroom > budget {
			chunks = append(chunks, current)
			current = []string{name}
			continue
		}
		current = candidate
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}

// datasetGetByNamesChunked merges successful source-bearing reads and marks only
// the names in failed chunks unavailable. One oversized/transient request can
// no longer disable stale-publication classification for the entire pass.
func (d *Driver) datasetGetByNamesChunked(
	ctx context.Context,
	names []string,
) (result map[string]*truenas.Dataset, failed map[string]struct{}) {
	result = make(map[string]*truenas.Dataset, len(names))
	failed = make(map[string]struct{})
	for i, chunk := range chunkDatasetNames(names, datasetGetByNamesBatchBudget) {
		datasets, err := d.truenasClient.DatasetGetByNames(ctx, chunk)
		if err != nil {
			d.recordReconcileObjectFailure("stale_publication_classification", fmt.Sprintf("batch-%d", i+1), err)
			for _, name := range chunk {
				failed[name] = struct{}{}
			}
			if ctx.Err() != nil {
				break
			}
			continue
		}
		for name, dataset := range datasets {
			result[name] = dataset
		}
	}
	return result, failed
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
	// Hoist the empty-string check ahead of the parse: an empty raw value means
	// "use the default", so parsing it (only to discard the result) is wasted. A
	// non-empty value is parsed and validated. Behavior is unchanged because the
	// defaults are positive.
	var err error
	interval := defaultControllerReconcileInterval
	if strings.TrimSpace(d.config.Reconcile.Interval) != "" {
		interval, err = d.config.Reconcile.IntervalDuration()
	}
	if err != nil || interval <= 0 {
		klog.Errorf("Controller reconciliation disabled due to invalid interval %q: %v", d.config.Reconcile.Interval, err)
		return
	}
	minAge := defaultControllerReconcileMinOrphanAge
	if d.config.Reconcile.Enabled || d.config.Fencing.Enabled() {
		if strings.TrimSpace(d.config.Reconcile.MinOrphanAge) != "" {
			minAge, err = d.config.Reconcile.MinOrphanAgeDuration()
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
				klog.Infof("Orphan reconcile detection complete: volumes=%d snapshots=%d spentRestoreSnapshots=%d tombstones=%d manualRecoveryTombstones=%d remnants=%d adoptedStamps=%d",
					report.OrphanVolumeCount, report.OrphanSnapshotCount, report.SpentRestoreSnapshotCount, report.TombstoneSnapshotCount,
					report.ManualRecoveryTombstoneCount, report.RemnantVolumeCount, report.AdoptedStampCount)
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
