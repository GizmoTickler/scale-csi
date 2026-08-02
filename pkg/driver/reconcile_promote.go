package driver

import (
	"context"
	"fmt"
	"sort"
	"time"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// reconcilePromoteRestoredClones is the GF2/E3 background step that frees
// clone-restored volumes from their origin-snapshot pin. A `snapshotRestoreMode:
// clone` restore pins its CSI source snapshot as the ZFS origin forever, which
// keeps the source snapshot (and its source volume) from ever being reclaimed.
// Promoting the clone inverts the dependency so the tombstone reaper can finally
// reclaim the source snapshot and the source volume becomes destroyable (P3).
//
// Gated on zfs.promoteRestoredClones (default false).
//
// GF2-fix summary — this step previously (a) never ran on the preferred TrueNAS
// 26.0 reconcile path, (b) permanently stranded any tombstone the promote
// migrated, (c) ignored snapshots other than the origin that ZFS also migrates,
// and (d) decided eligibility from a cached in-memory tally with no lock. All
// four are addressed below:
//
//	B3 — the pass's dataset listing (zfs.resource.query) returns SOURCELESS user
//	     properties, so a source=="local" ownership test can never pass on it.
//	     The listing is now used only to nominate candidates; each candidate is
//	     re-read with a source-bearing DatasetGet under the lock, where the
//	     strict local-stamp check is applied.
//	H3 — eligibility is re-proven under the per-volume operation lock (both the
//	     clone's and the origin source volume's, acquired in a deterministic
//	     order) using the AUTHORITATIVE SnapshotDependentClones query, which sees
//	     unmanaged and inherited clones the managed slice never contained.
//	H1 — promote migrates EVERY snapshot older-or-equal to the origin. The step
//	     refuses outright when that set contains any other LIVE CSI snapshot,
//	     whose backend ID would silently change (making its DeleteSnapshot lie).
//	B2 — for each tombstone in the migrating set, the ledger entry is RE-KEYED to
//	     the post-promote ID *before* the promote, so the reaper's provenance
//	     follows the snapshot instead of being retired as "already gone".
//
// GF2-fix2 supplies the missing third of that inventory. listAllManagedSnapshots
// partitions the parent's snapshots into CSI snapshots, tombstones and UNOWNED
// (foreign snapshots plus this driver's own task-created scheduled snapshots,
// which carry no CSI properties). The unowned bucket was DROPPED at this call
// site, so H1 analyzed an incomplete migration set while pool.dataset.promote
// moves every older-or-equal snapshot regardless of who owns it. An unowned
// snapshot could migrate onto the restored clone unseen, where it is stranded
// under the wrong volume — or destroyed with it once
// destroyForeignSnapshotsOnDelete is enabled. All three buckets are now indexed
// and promoteRestoredClone refuses when any unowned snapshot would migrate.
func (d *Driver) reconcilePromoteRestoredClones(
	ctx context.Context,
	datasets []*truenas.Dataset,
	snapshots, tombstones, unowned []*truenas.Snapshot,
	ledger map[string]tombstoneLedgerEntry,
	report *ReconcileReport,
) {
	if !d.config.ZFS.PromoteRestoredClones {
		return
	}

	// Index every snapshot the pass observed by its dataset — ALL THREE buckets,
	// because ZFS migrates by createtxg, not by ownership — so the migrating-set
	// analysis needs no extra listing and can never be silently partial.
	byDataset := make(map[string][]*truenas.Snapshot)
	for _, bucket := range [][]*truenas.Snapshot{snapshots, tombstones, unowned} {
		for _, snap := range bucket {
			if snap != nil {
				byDataset[snap.Dataset] = append(byDataset[snap.Dataset], snap)
			}
		}
	}

	for _, listed := range datasets {
		if ctx.Err() != nil {
			return
		}
		// CANDIDATE nomination from the (possibly sourceless) pass listing. Value
		// matches only — every authoritative check happens after the fresh read.
		if listed == nil ||
			datasetUserProperty(listed, PropDriverInstanceID) != d.driverInstanceID() ||
			datasetUserProperty(listed, PropVolumeContentSourceType) != "snapshot" ||
			datasetOriginSnapshotID(listed) == "" {
			continue
		}
		migratedOldIDs, reason := d.promoteRestoredClone(ctx, listed.Name, byDataset, ledger)
		if reason != "" {
			klog.V(4).Infof("GF2/E3: skipping promote of %s — %s", listed.Name, reason)
			continue
		}
		report.PromotedCloneCount++
		// The pass classified tombstones BEFORE this promote, so any migrated
		// tombstone's entry still names its pre-migration id. Drop those: reaping
		// by the old id would resolve NotFound and be reported as a successful
		// reap while the snapshot lives on at its new id. Its re-keyed ledger
		// entry makes it a proper candidate on the next pass.
		dropMigratedTombstoneCandidates(report, migratedOldIDs)
	}
}

func dropMigratedTombstoneCandidates(report *ReconcileReport, migratedOldIDs []string) {
	if len(migratedOldIDs) == 0 || len(report.TombstoneSnapshots) == 0 {
		return
	}
	migrated := make(map[string]struct{}, len(migratedOldIDs))
	for _, id := range migratedOldIDs {
		migrated[id] = struct{}{}
	}
	kept := report.TombstoneSnapshots[:0]
	for i := range report.TombstoneSnapshots {
		if _, moved := migrated[report.TombstoneSnapshots[i].BackendID]; moved {
			report.TombstoneSnapshotBytes -= report.TombstoneSnapshots[i].Bytes
			continue
		}
		kept = append(kept, report.TombstoneSnapshots[i])
	}
	report.TombstoneSnapshots = kept
}

// promoteRestoredClone re-proves every eligibility gate under the operation
// locks and promotes exactly one clone. On success it returns the pre-migration
// ids of every snapshot ZFS moved (skipReason == ""); otherwise a human-readable
// skip reason. Nothing here trusts the caller's cached view.
func (d *Driver) promoteRestoredClone(
	ctx context.Context,
	datasetName string,
	snapshotsByDataset map[string][]*truenas.Snapshot,
	ledger map[string]tombstoneLedgerEntry,
) (migratedOldIDs []string, skipReason string) {
	cloneVolumeID := datasetVolumeID(datasetName)

	// Fresh, source-bearing read BEFORE the lock decision so the origin we lock
	// against is the live one.
	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return nil, fmt.Sprintf("candidate revalidation failed: %v", err)
	}
	origin := datasetOriginSnapshotID(ds)
	if origin == "" {
		return nil, "already independent (promoted earlier or never a clone)"
	}
	sourceDataset, _, ok := splitSnapshotID(origin)
	if !ok {
		return nil, fmt.Sprintf("origin %q is not a parseable snapshot id", origin)
	}
	sourceVolumeID := datasetVolumeID(sourceDataset)

	// Deterministic lock ordering across the two volumes involved, so promote can
	// never deadlock against a concurrent CreateVolume/DeleteVolume/DeleteSnapshot
	// or the tombstone reaper (all of which take the per-volume lock).
	lockKeys := sortedLockKeys(cloneVolumeID, sourceVolumeID)
	acquired := make([]string, 0, len(lockKeys))
	defer func() {
		for i := len(acquired) - 1; i >= 0; i-- {
			d.releaseOperationLock(acquired[i])
		}
	}()
	for _, key := range lockKeys {
		if !d.acquireOperationLock(key) {
			return nil, "a volume operation is in progress"
		}
		acquired = append(acquired, key)
	}

	// Re-read under the lock and apply the STRICT ownership stamp (source-bearing
	// read, so an inherited value can never masquerade as local ownership).
	ds, err = d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return nil, fmt.Sprintf("candidate revalidation under lock failed: %v", err)
	}
	if !datasetHasLocalUserProperty(ds, PropDriverInstanceID, d.driverInstanceID()) {
		return nil, "not locally stamped by this driver instance"
	}
	if datasetLocalUserProperty(ds, PropVolumeContentSourceType) != "snapshot" {
		return nil, "not a snapshot-restored clone"
	}
	if origin = datasetOriginSnapshotID(ds); origin == "" {
		return nil, "origin pin released between reads"
	}
	if lockedSource, _, ok := splitSnapshotID(origin); !ok || lockedSource != sourceDataset {
		return nil, "origin changed between reads"
	}

	// AUTHORITATIVE sole-dependent gate (H3): ask the backend which datasets are
	// clones of this exact snapshot, not the pass's managed-dataset slice. An
	// unmanaged or foreign sibling under the CSI parent is therefore counted, and
	// promoting would have re-parented it onto a CSI-managed dataset.
	dependents, err := d.truenasClient.SnapshotDependentClones(ctx, origin)
	if err != nil {
		return nil, fmt.Sprintf("authoritative dependent-clone query failed: %v", err)
	}
	if len(dependents) != 1 || dependents[0] != datasetName {
		return nil, fmt.Sprintf("origin %s has %d dependent clones (%v); promoting would re-parent them (R3)", origin, len(dependents), dependents)
	}

	// CORROBORATE THE INVENTORY BEFORE TRUSTING IT (GF2-fix3/B1-g).
	//
	// The migrating set is only as good as the snapshot listing it is computed
	// from, and SnapshotListAll returns ([]*Snapshot, error) with no total, page
	// token or completeness marker: a truncated-but-nil-error result is
	// indistinguishable from a complete one, and an older UNOWNED snapshot missing
	// from it is silently re-parented by pool.dataset.promote. "No error" is not
	// completeness.
	//
	// So completeness is established POSITIVELY, from a second independent
	// authoritative inventory: a fresh, dataset-scoped SnapshotList taken here,
	// under the lock, through a different query shape than the pass's recursive
	// parent walk. The two must agree EXACTLY on membership. They disagree when
	// either listing was truncated, and they also disagree when the source
	// dataset's snapshots changed since the pass — both are cases where the
	// migration analysis would be made on a view that is not the live one, and
	// both REFUSE. Promotion is a background, opt-in, always-retryable step; a
	// skipped pass costs nothing and a wrong answer corrupts snapshot identity.
	fresh, err := d.truenasClient.SnapshotList(ctx, sourceDataset)
	if err != nil {
		// Metered under the SAME reason as a membership disagreement (GF2-fix4/F4):
		// both are "the corroborating inventory did not establish completeness", and
		// leaving this arm unmetered made the counter under-report exactly the
		// backend-unreachable case an operator most needs to see.
		RecordClonePromoteRefused("uncorroborated_snapshot_inventory")
		return nil, fmt.Sprintf("corroborating snapshot inventory for %s failed; refusing to promote on an unprovable migration set: %v", sourceDataset, err)
	}
	candidates, reason := corroboratedMigrationCandidates(snapshotsByDataset[sourceDataset], fresh, sourceDataset)
	if reason != "" {
		RecordClonePromoteRefused("uncorroborated_snapshot_inventory")
		return nil, reason
	}

	// Determine the MIGRATING SET: promote moves the origin and every snapshot of
	// the source dataset that is older-or-equal to it (P3).
	migrating, reason := migratingSnapshots(candidates, origin)
	if reason != "" {
		return nil, reason
	}

	// H1: refuse when a live CSI VolumeSnapshot would migrate. Its backend ID
	// would change under Kubernetes' feet: SnapshotGet(old id) 404s, so
	// DeleteSnapshot would report SUCCESS while the snapshot persists forever.
	//
	// GF2-fix2: the same refusal now covers UNOWNED snapshots — foreign ones and
	// the driver's own task-created scheduled snapshots. Migrating either is
	// destructive of meaning: a foreign snapshot lands under a volume its owner
	// never picked (and is destroyed with it if destroyForeignSnapshotsOnDelete is
	// on), and a scheduled snapshot's name stops proving out against the new
	// dataset's leaf and schema, permanently wedging the clone's own DeleteVolume
	// behind the foreign guard. Tombstones are the only non-CSI class that may
	// migrate, because their ledger provenance is explicitly re-keyed below.
	for _, snap := range migrating {
		switch {
		case isCSISnapshot(snap):
			RecordClonePromoteRefused("live_csi_snapshot_would_migrate")
			return nil, fmt.Sprintf("promote would migrate live CSI snapshot %s off %s (its backend id would change)", snap.ID, sourceDataset)
		case isSnapshotTombstone(snap):
			// Provenance is carried across the id migration below.
		default:
			RecordClonePromoteRefused("unowned_snapshot_would_migrate")
			return nil, fmt.Sprintf("promote would migrate non-CSI snapshot %s off %s (it would be stranded under the promoted volume, or destroyed with it when destroyForeignSnapshotsOnDelete is enabled)", snap.ID, sourceDataset)
		}
	}

	// B2: carry tombstone provenance ACROSS the ID migration BEFORE promoting.
	// Writing the new key first is the crash-safe order: an entry whose snapshot
	// does not (yet) exist is exactly what the age-gated orphan-ledger sweep
	// retires, whereas a migrated tombstone with no entry is unreapable forever.
	migratedKeys, err := d.rekeyMigratingTombstoneLedger(ctx, migrating, datasetName, ledger)
	if err != nil {
		return nil, fmt.Sprintf("tombstone ledger re-key before promote failed: %v", err)
	}

	// Capture the pre-migration ids BEFORE the promote: the snapshot objects are
	// moved by ZFS (and mutated in place by the fidelity mock), so reading
	// snap.ID afterwards would yield the post-migration id.
	migratedOldIDs = make([]string, 0, len(migrating))
	staleLedgerKeys := make([]string, 0, len(migrating))
	for _, snap := range migrating {
		migratedOldIDs = append(migratedOldIDs, snap.ID)
		if isSnapshotTombstone(snap) {
			key := tombstoneLedgerKey(snap.ID)
			if _, recorded := ledger[key]; recorded {
				staleLedgerKeys = append(staleLedgerKeys, key)
			}
		}
	}

	if err := d.truenasClient.DatasetPromote(ctx, datasetName); err != nil {
		RecordClonePromoted(err)
		klog.Warningf("GF2/E3: failed to promote clone-restored volume %s (will retry next pass): %v", datasetName, err)
		// Roll the speculative ledger entries back; if this fails the sweep
		// retires them once their (never-created) snapshot is confirmed absent.
		if len(migratedKeys) > 0 {
			if rbErr := d.removeBookkeepingProperties(ctx, migratedKeys); rbErr != nil {
				klog.Warningf("GF2/E3: failed to roll back %d speculative tombstone ledger entries after a failed promote (the sweep will retire them): %v", len(migratedKeys), rbErr)
			}
		}
		return nil, fmt.Sprintf("promote failed: %v", err)
	}
	RecordClonePromoted(nil)

	// Retire the pre-migration ledger keys. Best-effort: their snapshots are gone
	// from the old IDs, so the age-gated sweep retires any leftover.
	d.retireMigratedTombstoneKeys(ctx, staleLedgerKeys, ledger)

	klog.Infof("GF2/E3: promoted clone-restored volume %s; origin snapshot %s pin released (%d snapshot(s) migrated, ledger re-keyed)",
		datasetName, origin, len(migrating))
	return migratedOldIDs, ""
}

// corroboratedMigrationCandidates cross-checks the reconcile pass's view of a
// dataset's snapshots against a second, independently obtained authoritative
// listing of the SAME dataset, and returns the snapshots the migration analysis
// may be run on (GF2-fix3/B1-g).
//
// Neither listing can prove its own completeness — the backend returns a bare
// slice with no total and no page token — so completeness is established the
// only way an unmarked API allows: two inventories obtained through DIFFERENT
// query shapes (a recursive parent walk vs. a dataset-scoped query) must agree
// exactly on membership. A truncation in either one, in either direction, breaks
// the agreement and REFUSES. The fresh listing is what is returned, because it is
// the one taken under the lock.
//
// Any nil entry, any id that is not on this dataset, and an empty fresh listing
// (the origin must be in it) are refusals too: each means the inventory the
// promote would reason about is not the inventory that exists.
func corroboratedMigrationCandidates(passView, fresh []*truenas.Snapshot, sourceDataset string) (candidates []*truenas.Snapshot, refusal string) {
	freshIDs := make(map[string]struct{}, len(fresh))
	candidates = make([]*truenas.Snapshot, 0, len(fresh))
	for _, snap := range fresh {
		if snap == nil {
			return nil, fmt.Sprintf("the corroborating snapshot inventory for %s contains a nil entry; refusing to promote on an unprovable migration set", sourceDataset)
		}
		if snap.Dataset != sourceDataset {
			return nil, fmt.Sprintf("the corroborating snapshot inventory for %s returned snapshot %s from another dataset; refusing to promote on an unprovable migration set", sourceDataset, snap.ID)
		}
		if _, dup := freshIDs[snap.ID]; dup {
			return nil, fmt.Sprintf("the corroborating snapshot inventory for %s lists %s twice; refusing to promote on an unprovable migration set", sourceDataset, snap.ID)
		}
		freshIDs[snap.ID] = struct{}{}
		candidates = append(candidates, snap)
	}
	if len(candidates) == 0 {
		return nil, fmt.Sprintf("the corroborating snapshot inventory for %s is empty although the pass observed %d snapshot(s) there; refusing to promote on an unprovable migration set", sourceDataset, len(passView))
	}
	seen := make(map[string]struct{}, len(passView))
	for _, snap := range passView {
		if snap == nil || snap.Dataset != sourceDataset {
			continue
		}
		seen[snap.ID] = struct{}{}
		if _, ok := freshIDs[snap.ID]; !ok {
			return nil, fmt.Sprintf("snapshot %s was observed by this reconcile pass but is absent from the corroborating inventory of %s; refusing to promote on an unprovable migration set", snap.ID, sourceDataset)
		}
	}
	for id := range freshIDs {
		if _, ok := seen[id]; !ok {
			return nil, fmt.Sprintf("snapshot %s is present on %s but was absent from this reconcile pass's inventory (a truncated or stale listing); refusing to promote on an unprovable migration set", id, sourceDataset)
		}
	}
	return candidates, ""
}

// migratingSnapshots returns the snapshots ZFS will move onto the promoted clone
// — the origin plus every snapshot of the same dataset that is older-or-equal by
// createtxg (P3). When the origin's createtxg is unavailable the analysis cannot
// be made and the caller must NOT promote: a wrong answer here silently corrupts
// a live snapshot's identity, so this fails closed.
func migratingSnapshots(candidates []*truenas.Snapshot, origin string) (migrating []*truenas.Snapshot, refusal string) {
	var originSnap *truenas.Snapshot
	for _, snap := range candidates {
		if snap != nil && snap.ID == origin {
			originSnap = snap
			break
		}
	}
	if originSnap == nil {
		return nil, fmt.Sprintf("origin snapshot %s was not observed in this pass; refusing to promote without the migration set", origin)
	}
	if originSnap.CreateTXG == 0 {
		return nil, fmt.Sprintf("origin snapshot %s exposes no createtxg; refusing to promote without a provable migration set", origin)
	}
	migrating = []*truenas.Snapshot{originSnap}
	for _, snap := range candidates {
		if snap == nil || snap.ID == origin {
			continue
		}
		if snap.CreateTXG == 0 {
			return nil, fmt.Sprintf("snapshot %s exposes no createtxg; refusing to promote without a provable migration set", snap.ID)
		}
		if snap.CreateTXG <= originSnap.CreateTXG {
			migrating = append(migrating, snap)
		}
	}
	sort.Slice(migrating, func(i, j int) bool { return migrating[i].ID < migrating[j].ID })
	return migrating, ""
}

// rekeyMigratingTombstoneLedger writes a ledger entry at each migrating
// tombstone's POST-promote id, preserving the creation identity that authorizes
// the reap. Returns the newly written keys so a failed promote can roll them
// back.
func (d *Driver) rekeyMigratingTombstoneLedger(
	ctx context.Context,
	migrating []*truenas.Snapshot,
	promotedDataset string,
	ledger map[string]tombstoneLedgerEntry,
) ([]string, error) {
	var written []string
	for _, snap := range migrating {
		if !isSnapshotTombstone(snap) {
			continue
		}
		entry, ok := ledger[tombstoneLedgerKey(snap.ID)]
		if !ok {
			// No provenance to carry: the scan fallback (identity-based) is this
			// tombstone's only route, and it re-proves identity on the NEW dataset,
			// which is driver-owned. Nothing to re-key.
			continue
		}
		migratedID := promotedDataset + "@" + snapshotShortName(snap)
		migrated := entry
		migrated.Snapshot = migratedID
		migrated.Dataset = promotedDataset
		migrated.RenamedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if err := d.writeTombstoneLedgerEntry(ctx, migrated); err != nil {
			// Fail closed: without provenance at the new id the tombstone would be
			// unreapable forever, which is exactly the leak this fix closes.
			if len(written) > 0 {
				if rbErr := d.removeBookkeepingProperties(ctx, written); rbErr != nil {
					klog.Warningf("GF2/E3: failed to roll back %d partial ledger re-keys: %v", len(written), rbErr)
				}
			}
			return nil, err
		}
		key := tombstoneLedgerKey(migratedID)
		written = append(written, key)
		ledger[key] = migrated
	}
	return written, nil
}

// retireMigratedTombstoneKeys removes the pre-migration ledger keys once the
// promote landed. Best-effort by design: a leftover entry whose snapshot no
// longer exists at that id is precisely what sweepOrphanedTombstoneLedger
// retires.
func (d *Driver) retireMigratedTombstoneKeys(ctx context.Context, stale []string, ledger map[string]tombstoneLedgerEntry) {
	if len(stale) == 0 {
		return
	}
	sort.Strings(stale)
	if err := d.removeBookkeepingProperties(ctx, stale); err != nil {
		klog.Warningf("GF2/E3: failed to retire %d pre-promote tombstone ledger entries (the sweep will retire them): %v", len(stale), err)
		return
	}
	for _, key := range stale {
		delete(ledger, key)
	}
}

// sortedLockKeys returns the deduplicated per-volume lock keys for the volumes
// a promote touches, in a stable order so two concurrent promotes can never
// acquire them in opposite orders.
func sortedLockKeys(volumeIDs ...string) []string {
	seen := make(map[string]struct{}, len(volumeIDs))
	keys := make([]string, 0, len(volumeIDs))
	for _, id := range volumeIDs {
		if id == "" {
			continue
		}
		key := volumeLockKey(id)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// splitSnapshotID splits "dataset@name".
func splitSnapshotID(snapshotID string) (dataset, name string, ok bool) {
	for i := len(snapshotID) - 1; i >= 0; i-- {
		if snapshotID[i] == '@' {
			return snapshotID[:i], snapshotID[i+1:], i > 0 && i < len(snapshotID)-1
		}
	}
	return "", "", false
}
