package driver

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// tombstoneScanFallbackLimit bounds the number of provenance-proven fallback
// candidates processed in one pass. The snapshot transfer itself is shared with
// listAllManagedSnapshots; this is deliberately not a fixed first-page slice,
// which could permanently starve candidates later in the listing.
const tombstoneScanFallbackLimit = 500

// detectTombstonesByScanFallback classifies ledger-less tombstones from the
// pass's existing snapshot slice. Name shape and source-volume ownership are
// insufficient provenance: a delete candidate must retain both the original CSI
// snapshot name and this driver's instance identity, and its current short name
// must exactly equal snapshotTombstoneName(dataset, sanitizedOriginal, nonce).
// A ledger entry of any shape excludes the fallback path; mismatches fail closed.
func (d *Driver) detectTombstonesByScanFallback(
	ctx context.Context,
	now time.Time,
	scanned []*truenas.Snapshot,
	ledger map[string]tombstoneLedgerEntry,
	minOrphanAge time.Duration,
	report *ReconcileReport,
) {
	strictIDs := make(map[string]struct{}, len(report.TombstoneSnapshots))
	for i := range report.TombstoneSnapshots {
		strictIDs[report.TombstoneSnapshots[i].BackendID] = struct{}{}
	}
	processed := 0
	for _, snap := range scanned {
		if snap == nil || !isSnapshotTombstone(snap) {
			continue
		}
		if _, strict := strictIDs[snap.ID]; strict {
			continue
		}
		if snapshotIsLiveCSIObjectWithTombstoneShapedName(snap) {
			klog.Warningf("Orphan reconcile: scan fallback skipping %s — it carries live CSI snapshot identity despite a tombstone-shaped name", snap.ID)
			continue
		}
		createdAt, age, eligible := reconcileAge(now, snap.GetCreationTime(), minOrphanAge)
		if !eligible {
			continue
		}
		sourceVolumeID := path.Base(snap.Dataset)
		item := ReconcileObject{
			ID:             snap.ID,
			BackendID:      snap.ID,
			SourceVolumeID: sourceVolumeID,
			CreatedAt:      createdAt,
			Age:            age,
			Bytes:          snap.GetSnapshotSize(),
		}
		_, ledgerPresent := ledger[tombstoneLedgerKey(snap.ID)]
		provenanceSafe := !ledgerPresent && snapshotMatchesRetainedTombstoneIdentity(snap, d.driverInstanceID())
		if provenanceSafe {
			sourceDataset, dsErr := d.truenasClient.DatasetGet(ctx, snap.Dataset)
			if dsErr != nil {
				d.recordReconcileObjectFailure("tombstone_scan_fallback", snap.ID, dsErr)
				provenanceSafe = false
			} else if !datasetHasLocalUserProperty(sourceDataset, PropDriverInstanceID, d.driverInstanceID()) {
				provenanceSafe = false
			} else if sourceDatasetMasksTombstoneInheritance(sourceDataset) {
				// The source dataset itself carries csi_snapshot_name, so on TrueNAS
				// 26.0 the snapshot's retained identity may be INHERITED from the
				// dataset rather than written at CreateSnapshot — not proof of a
				// driver tombstone. Route to manual recovery; never reap.
				provenanceSafe = false
			}
		}
		if !provenanceSafe {
			report.ManualRecoveryTombstones = append(report.ManualRecoveryTombstones, item)
			klog.Warningf("Orphan reconcile: tombstone-shaped snapshot %s lacks safe scan-fallback provenance; manual recovery required", snap.ID)
			continue
		}
		if processed >= tombstoneScanFallbackLimit {
			continue
		}
		processed++
		item.tombstoneScanFallback = true
		report.TombstoneSnapshots = append(report.TombstoneSnapshots, item)
		report.TombstoneSnapshotBytes += item.Bytes
	}
}

// snapshotMatchesRetainedTombstoneIdentity proves that a renamed snapshot is
// exactly one this driver could have produced from the identity properties
// written atomically at CreateSnapshot time. The suffix nonce is parsed and fed
// back through the production rename algorithm; accepting a mere name prefix
// would make manual lookalikes destructive.
//
// This proves identity from the snapshot's own retained property VALUES. On
// TrueNAS 26.0 the snapshot resource path returns user_properties as a flat,
// SOURCELESS string map, so a source=="local" requirement here would break every
// real reap and must NOT be imposed; the dataset-side inheritance guard
// (sourceDatasetMasksTombstoneInheritance) covers the sourceless-26.0 vector
// where these values could be inherited from the source dataset rather than
// written on the snapshot. Where the snapshot's UserProperty.Source IS populated
// (legacy path), an inherited/foreign source proves the value was not written on
// this snapshot at CreateSnapshot, so reject it.
func snapshotMatchesRetainedTombstoneIdentity(snap *truenas.Snapshot, instanceID string) bool {
	if snap == nil || instanceID == "" {
		return false
	}
	original, hasOriginal := snap.UserProperties[PropCSISnapshotName]
	instance, hasInstance := snap.UserProperties[PropDriverInstanceID]
	if !hasOriginal || original.Value == "" || original.Value == "-" ||
		!hasInstance || instance.Value != instanceID {
		return false
	}
	if original.Source != "" && !isLocalUserPropertySource(original.Source) {
		return false
	}
	if instance.Source != "" && !isLocalUserPropertySource(instance.Source) {
		return false
	}
	currentName := snapshotShortName(snap)
	marker := strings.LastIndex(currentName, snapshotTombstoneMarker)
	if marker <= 0 {
		return false
	}
	nonce, err := strconv.ParseInt(currentName[marker+len(snapshotTombstoneMarker):], 10, 64)
	if err != nil || nonce <= 0 {
		return false
	}
	return currentName == snapshotTombstoneName(snap.Dataset, sanitizeVolumeID(original.Value), nonce)
}

// sourceDatasetMasksTombstoneInheritance reports whether the tombstone's source
// dataset itself carries the csi_snapshot_name identity property. The driver
// never stamps csi_snapshot_name on a dataset — it is a snapshot-level property
// written at CreateSnapshot — so its presence on the source dataset means a
// snapshot under it can INHERIT that identity. On TrueNAS 26.0, where the
// snapshot resource path returns sourceless user_properties, an inherited
// csi_snapshot_name is indistinguishable by value from a driver-written one, so
// retained snapshot identity is no longer proof of a driver tombstone. Callers
// on the identity-only (scan-fallback) reap path must route such candidates to
// manual recovery instead of reaping them.
func sourceDatasetMasksTombstoneInheritance(sourceDataset *truenas.Dataset) bool {
	return datasetHasUserProperty(sourceDataset, PropCSISnapshotName)
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
// successful reap the ledger entry is retired into the pass-level batch (flushed
// at pass end; best-effort — the sweep retires leftovers).
func (d *Driver) reapTombstoneSnapshot(
	ctx context.Context,
	tombstone ReconcileObject,
	minOrphanAge time.Duration,
	retire *tombstoneRetirementBatch,
) (reaped bool, reason string) {
	if tombstone.SourceVolumeID == "" {
		return false, "tombstone snapshot has no resolvable source volume"
	}
	lockKey := volumeLockKey(tombstone.SourceVolumeID)
	if !d.acquireOperationLock(lockKey) {
		return false, "source volume operation is in progress"
	}
	defer d.releaseOperationLock(lockKey)

	snapshot, err := d.truenasClient.SnapshotGet(ctx, tombstone.BackendID)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			// Already gone (ZFS reclaimed it, or a peer reaped it): the operation's
			// goal is met, so treat it as reaped for reporting and retire the entry.
			retire.add(tombstone.BackendID)
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
	ledgerKey := tombstoneLedgerKey(snapshot.ID)
	reads := d.readBookkeepingDatasets(ctx, d.bookkeepingEnabled() || tombstone.tombstoneScanFallback)
	if reads.parentErr != nil {
		return false, fmt.Sprintf("tombstone ledger revalidation failed: %v", reads.parentErr)
	}
	parentLedger := tombstoneLedgerFromDataset(reads.parent)
	parentEntry, parentRecorded := parentLedger[ledgerKey]
	parentPropertyPresent := datasetHasUserProperty(reads.parent, ledgerKey)
	var childEntry tombstoneLedgerEntry
	var childRecorded, childPropertyPresent bool
	if reads.childErr != nil {
		// An absent child is legitimate (no entry migrated or written yet);
		// any other read failure must fail closed, not silently downgrade to
		// a parent-only decision.
		if !truenas.IsNotFoundError(reads.childErr) {
			return false, fmt.Sprintf("tombstone ledger revalidation failed: %v", reads.childErr)
		}
	} else if reads.child != nil {
		childEntry, childRecorded = tombstoneLedgerFromDataset(reads.child)[ledgerKey]
		childPropertyPresent = datasetHasUserProperty(reads.child, ledgerKey)
	}
	if tombstone.tombstoneScanFallback {
		// Fallback is exclusively for a genuinely absent ledger. A raw property
		// at either location — including an unparseable or creation-mismatched
		// entry — fails closed and can never be downgraded to relaxed provenance.
		if parentPropertyPresent || childPropertyPresent {
			return false, "tombstone ledger entry is present; scan fallback requires fresh absence from parent and child"
		}
		if !snapshotMatchesRetainedTombstoneIdentity(snapshot, d.driverInstanceID()) {
			return false, "retained snapshot identity does not prove the driver tombstone rename"
		}
	} else {
		ledgerProvenance := func(entry tombstoneLedgerEntry, recorded bool) bool {
			return recorded && entry.Snapshot == snapshot.ID && entry.CreatedAt > 0 &&
				entry.CreatedAt == snapshot.GetCreationTime()
		}
		if !ledgerProvenance(parentEntry, parentRecorded) && !ledgerProvenance(childEntry, childRecorded) {
			if !parentRecorded && !childRecorded {
				return false, "no tombstone ledger entry proves driver provenance"
			}
			return false, "tombstone ledger creation identity does not match the observed snapshot"
		}
	}
	// The tombstone must sit on a dataset this driver instance owns. For
	// scan-fallback tombstones without a ledger entry this ownership stamp is the
	// positive provenance that authorizes the reap.
	sourceDataset, dsErr := d.truenasClient.DatasetGet(ctx, snapshot.Dataset)
	if dsErr != nil {
		return false, fmt.Sprintf("tombstone source dataset revalidation failed: %v", dsErr)
	}
	if !datasetHasLocalUserProperty(sourceDataset, PropDriverInstanceID, d.driverInstanceID()) {
		return false, "tombstone source dataset does not carry this driver instance's ownership stamp"
	}
	if tombstone.tombstoneScanFallback && sourceDatasetMasksTombstoneInheritance(sourceDataset) {
		// Scan-fallback provenance rests on the snapshot's retained identity values
		// alone. On TrueNAS 26.0 (sourceless snapshot properties) a source dataset
		// that carries csi_snapshot_name could have that identity inherited rather
		// than driver-written, so refuse the reap. The ledger-proven path is
		// unaffected: a matching ledger entry with creation-time identity is
		// authoritative provenance that inheritance cannot forge.
		return false, "tombstone source dataset carries csi_snapshot_name; retained identity may be inherited, not driver-written"
	}
	createdAt, _, eligible := reconcileAge(time.Now(), snapshot.GetCreationTime(), minOrphanAge)
	if !eligible || !createdAt.Equal(tombstone.CreatedAt) {
		return false, "tombstone creation identity or age changed"
	}
	hasDependentClones, cloneErr := d.truenasClient.DatasetHasDependentClones(ctx, snapshot.Dataset)
	if cloneErr != nil {
		return false, fmt.Sprintf("tombstone dependent-clone preflight failed: %v", cloneErr)
	}
	if hasDependentClones {
		return false, "tombstone snapshot still has dependent clones"
	}
	if err := d.truenasClient.SnapshotDelete(ctx, snapshot.ID, false, false); err != nil {
		if truenas.IsNotFoundError(err) {
			retire.add(snapshot.ID)
			return true, ""
		}
		var cloneErr *truenas.ErrSnapshotHasClones
		if errors.As(err, &cloneErr) {
			return false, "tombstone snapshot still has dependent clones"
		}
		return false, fmt.Sprintf("failed to reap tombstone snapshot: %v", err)
	}
	retire.add(snapshot.ID)
	klog.Infof("Orphan reconcile: reaped released deferred-delete tombstone %s", snapshot.ID)
	return true, ""
}

// tombstoneRetirementBatch accumulates the full snapshot IDs whose tombstone
// ledger entries a reconcile pass retired, so their bookkeeping property removals
// are batched into one size-bounded user_properties_update remove per location at
// pass end instead of one removeBookkeepingProperties (parent + child = 2 RTTs)
// per reaped tombstone (~384/night). The snapshot destroys themselves still happen
// per-reap, in order, under the source-volume lock; only the post-destroy property
// removal is deferred. A failed batch removal leaves its entries for the
// orphan-ledger sweep to retire later — the same failure mode as today.
type tombstoneRetirementBatch struct {
	snapshotIDs []string
}

func (b *tombstoneRetirementBatch) add(fullSnapshotID string) {
	if b == nil {
		return
	}
	b.snapshotIDs = append(b.snapshotIDs, fullSnapshotID)
}

// flush removes the accumulated ledger entries. parent is the pass's parent
// dataset read (may be nil if that read failed): when non-nil, the parent removal
// is restricted to the retired keys the parent actually carries and skipped
// entirely when it carries none (the fully-migrated steady state where the parent
// is drained), saving one wasted RTT per retirement; when nil, all keys are
// removed from the parent conservatively (the historical behavior). Removals are
// chunked under the TrueNAS 26.0 64 kB WebSocket inbound limit and are
// best-effort per chunk.
func (b *tombstoneRetirementBatch) flush(ctx context.Context, d *Driver, parent *truenas.Dataset) {
	if b == nil || len(b.snapshotIDs) == 0 {
		return
	}
	keys := make([]string, 0, len(b.snapshotIDs))
	for _, snapshotID := range b.snapshotIDs {
		keys = append(keys, tombstoneLedgerKey(snapshotID))
	}
	// Parent removal first (preserving the historical parent-before-child order),
	// restricted to keys the pass's parent read carried when that read succeeded.
	parentKeys := keys
	if parent != nil {
		parentKeys = nil
		for _, key := range keys {
			if property, ok := parent.UserProperties[key]; ok && isLocalUserPropertySource(property.Source) {
				parentKeys = append(parentKeys, key)
			}
		}
	}
	if len(parentKeys) > 0 {
		for _, chunk := range chunkKeyList(parentKeys, bookkeepingMigrationBatchBudget) {
			if err := d.truenasClient.DatasetRemoveUserProperties(ctx, d.parentDatasetName(), chunk); err != nil {
				klog.Warningf("Failed to batch-remove %d tombstone ledger entries from parent (sweep will retire them): %v", len(chunk), err)
			}
		}
	}
	if d.bookkeepingEnabled() {
		for _, chunk := range chunkKeyList(keys, bookkeepingMigrationBatchBudget) {
			if err := d.truenasClient.DatasetRemoveUserProperties(ctx, d.bookkeepingDatasetName(), chunk); err != nil {
				d.noteBookkeepingWriteFailure(d.bookkeepingDatasetName(), err)
				if !truenas.IsNotFoundError(err) {
					klog.Warningf("Failed to batch-remove %d tombstone ledger entries from bookkeeping dataset (sweep will retire them): %v", len(chunk), err)
				}
			}
		}
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
