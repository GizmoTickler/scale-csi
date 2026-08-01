package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func originProp(snapshotID string) truenas.DatasetProperty {
	return truenas.DatasetProperty{Value: snapshotID, Parsed: snapshotID, Rawvalue: snapshotID, Source: "LOCAL"}
}

// seedCloneRestoredVolume registers a driver-owned snapshot-restored clone dataset
// whose ZFS origin is originSnapshotID.
func seedCloneRestoredVolume(client *truenas.MockClient, d *Driver, name, originSnapshotID string) *truenas.Dataset {
	ds := &truenas.Dataset{
		ID:     "pool/parent/" + name,
		Name:   "pool/parent/" + name,
		Type:   "FILESYSTEM",
		Origin: originProp(originSnapshotID),
		UserProperties: map[string]truenas.UserProperty{
			PropManagedResource:         {Value: "true", Source: "local"},
			PropDriverInstanceID:        {Value: d.driverInstanceID(), Source: "local"},
			PropVolumeContentSourceType: {Value: "snapshot", Source: "local"},
		},
	}
	client.Datasets[ds.Name] = ds
	return ds
}

// resourceQueryProjection reproduces what the PREFERRED TrueNAS 26.0 reconcile
// listing (zfs.resource.query -> DatasetQueryByParent) actually hands the
// promote step: user properties with NO per-property source. Any check that
// requires source=="local" is unsatisfiable on this shape — the reason E3 was a
// production no-op (GF2-fix/B3).
func resourceQueryProjection(ds *truenas.Dataset) *truenas.Dataset {
	projected := *ds
	projected.ResourceQuery = true
	projected.UserProperties = make(map[string]truenas.UserProperty, len(ds.UserProperties))
	for key, property := range ds.UserProperties {
		property.Source = ""
		projected.UserProperties[key] = property
	}
	return &projected
}

func promoteReport() *ReconcileReport { return &ReconcileReport{} }

// seedOriginTombstone creates the snapshot a clone-restored volume is actually
// pinned to in production: the TOMBSTONE of a deleted CSI source snapshot.
//
// The fixtures previously passed a property-less snapshot in the CSI-snapshot
// bucket, which listAllManagedSnapshots can never produce (a snapshot with no
// CSI identity lands in the UNOWNED bucket). That mislabeling is precisely what
// hid the dropped-unowned-bucket defect this round fixes, so the fixtures now
// place every snapshot in the bucket the production partition would.
func seedOriginTombstone(t *testing.T, client *truenas.MockClient, dataset, name string) *truenas.Snapshot {
	t.Helper()
	snap, err := client.SnapshotCreate(context.Background(), dataset, name+snapshotTombstoneMarker+"1", nil)
	require.NoError(t, err)
	require.True(t, isSnapshotTombstone(snap), "fixture must be a real tombstone")
	return snap
}

func TestMockDatasetPromoteMigratesEveryOlderOrEqualSnapshot(t *testing.T) {
	client := truenas.NewMockClient()
	ctx := context.Background()

	// s1 is OLDER than the origin s2; s3 is newer.
	_, err := client.SnapshotCreate(ctx, "pool/parent/source", "s1", nil)
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, "pool/parent/source", "s2", nil)
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, "pool/parent/source", "s3", nil)
	require.NoError(t, err)
	client.Datasets["pool/parent/source"] = &truenas.Dataset{Name: "pool/parent/source"}
	client.Datasets["pool/parent/clone-a"] = &truenas.Dataset{Name: "pool/parent/clone-a", Origin: originProp("pool/parent/source@s2")}
	client.Datasets["pool/parent/clone-b"] = &truenas.Dataset{Name: "pool/parent/clone-b", Origin: originProp("pool/parent/source@s2")}

	require.NoError(t, client.DatasetPromote(ctx, "pool/parent/clone-a"))

	assert.Equal(t, "", datasetOriginSnapshotID(client.Datasets["pool/parent/clone-a"]), "the promoted clone is independent")
	// The origin AND the older sibling snapshot moved; the newer one stayed.
	for _, migrated := range []string{"pool/parent/clone-a@s1", "pool/parent/clone-a@s2"} {
		_, err = client.SnapshotGet(ctx, migrated)
		require.NoError(t, err, "%s must live under the promoted clone", migrated)
	}
	_, err = client.SnapshotGet(ctx, "pool/parent/source@s1")
	assert.True(t, truenas.IsNotFoundError(err), "the OLDER snapshot migrates too — real promote moves every older-or-equal snapshot")
	_, err = client.SnapshotGet(ctx, "pool/parent/source@s3")
	assert.NoError(t, err, "a snapshot newer than the origin stays on the source")
	// Sibling clone re-parented, and the SOURCE itself becomes a clone (P3).
	assert.Equal(t, "pool/parent/clone-a@s2", datasetOriginSnapshotID(client.Datasets["pool/parent/clone-b"]))
	assert.Equal(t, "pool/parent/clone-a@s2", datasetOriginSnapshotID(client.Datasets["pool/parent/source"]),
		"promote inverts the dependency: the source becomes a clone of the promoted dataset")
}

// B3 — the step must actually execute on the PREFERRED TrueNAS 26.0 reconcile
// path, whose dataset projection carries no per-property source. Reverting the
// candidate/re-read split makes the strict source=="local" test run against the
// sourceless projection and this promotes nothing.
func TestReconcilePromoteRunsOnResourceQueryProjection(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	d.config.ZFS.PromoteRestoredClones = true

	snap := seedOriginTombstone(t, client, "pool/parent/source", "snap")
	client.Datasets["pool/parent/source"] = &truenas.Dataset{Name: "pool/parent/source"}
	clone := seedCloneRestoredVolume(client, d, "restored", snap.ID)

	report := promoteReport()
	d.reconcilePromoteRestoredClones(ctx,
		[]*truenas.Dataset{resourceQueryProjection(clone)},
		nil, []*truenas.Snapshot{snap}, nil, map[string]tombstoneLedgerEntry{}, report)

	assert.Equal(t, 1, report.PromotedCloneCount, "promote must run on the sourceless resource-query projection")
	assert.Contains(t, client.DatasetPromoteCalls, "pool/parent/restored")
	assert.Equal(t, "", datasetOriginSnapshotID(client.Datasets["pool/parent/restored"]))
}

// H3 — the sole-dependent gate must be AUTHORITATIVE. An UNMANAGED sibling clone
// (never in the driver's managed-dataset slice) must block the promote; the old
// in-memory tally over the reconcile slice could not see it and re-parented it.
func TestReconcilePromoteRefusesUnmanagedSiblingClone(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	d.config.ZFS.PromoteRestoredClones = true

	snap := seedOriginTombstone(t, client, "pool/parent/source", "snap")
	client.Datasets["pool/parent/source"] = &truenas.Dataset{Name: "pool/parent/source"}
	clone := seedCloneRestoredVolume(client, d, "restored", snap.ID)
	// An admin's restore-test clone of the same origin, outside driver management.
	client.Datasets["pool/parent/admin-restore-test"] = &truenas.Dataset{
		Name:   "pool/parent/admin-restore-test",
		Origin: originProp(snap.ID),
	}

	report := promoteReport()
	// The driver's own slice contains ONLY the managed clone, exactly as the
	// managed_resource filter produces in production.
	d.reconcilePromoteRestoredClones(ctx, []*truenas.Dataset{clone},
		nil, []*truenas.Snapshot{snap}, nil, map[string]tombstoneLedgerEntry{}, report)

	assert.Equal(t, 0, report.PromotedCloneCount, "an unmanaged sibling clone must block the promote")
	assert.Empty(t, client.DatasetPromoteCalls)
	assert.Equal(t, snap.ID, datasetOriginSnapshotID(client.Datasets["pool/parent/admin-restore-test"]),
		"the unmanaged clone must NOT be re-parented onto a CSI-managed dataset")
}

// H1 — promote migrates every snapshot older-or-equal to the origin, so an
// unrelated LIVE CSI VolumeSnapshot on the source would silently change backend
// id and its DeleteSnapshot would then report success while it persisted. The
// step must refuse.
func TestReconcilePromoteRefusesWhenLiveCSISnapshotWouldMigrate(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	d.config.ZFS.PromoteRestoredClones = true

	// s1: an unrelated live CSI VolumeSnapshot, OLDER than the restore origin.
	older, err := client.SnapshotCreate(ctx, "pool/parent/source", "s1", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "user-snapshot",
		PropCSISnapshotSourceVolumeID: "source",
	})
	require.NoError(t, err)
	origin := seedOriginTombstone(t, client, "pool/parent/source", "s2")
	client.Datasets["pool/parent/source"] = &truenas.Dataset{Name: "pool/parent/source"}
	clone := seedCloneRestoredVolume(client, d, "restored", origin.ID)

	report := promoteReport()
	d.reconcilePromoteRestoredClones(ctx, []*truenas.Dataset{clone},
		[]*truenas.Snapshot{older}, []*truenas.Snapshot{origin}, nil, map[string]tombstoneLedgerEntry{}, report)

	assert.Equal(t, 0, report.PromotedCloneCount, "a live CSI snapshot in the migrating set must refuse the promote")
	assert.Empty(t, client.DatasetPromoteCalls)
	got, err := client.SnapshotGet(ctx, older.ID)
	require.NoError(t, err, "the unrelated live CSI snapshot keeps its backend id")
	assert.Equal(t, "pool/parent/source", got.Dataset)
}

// GF2-fix2 (new HIGH) — the promote step analyzed only the CSI-snapshot and
// tombstone buckets, while pool.dataset.promote migrates EVERY snapshot
// older-or-equal to the origin. A foreign/unowned snapshot on the source
// therefore migrated onto the restored clone unseen by the H1 refusal gate: it
// ends up stranded under a volume its owner never chose, and is destroyed with
// that volume once destroyForeignSnapshotsOnDelete is enabled.
//
// REVERT-PROOF: passing the unowned bucket is the fix. On 03d37b8 the promote
// call site drops it, the migrating set contains only the origin tombstone, the
// promote SUCCEEDS, and the foreign snapshot is silently re-parented onto
// pool/parent/restored — so both the PromotedCloneCount assertion and the
// "stays on the source dataset" assertion below fail. Verified by running this
// scenario on a 03d37b8 worktree against the pre-fix SIX-argument call site
// (which has no unowned parameter at all, so the foreign snapshot simply is not
// passed — exactly what production did): all three assertions failed and the
// snapshot's dataset came back as pool/parent/restored.
func TestReconcilePromoteRefusesWhenUnownedSnapshotWouldMigrate(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	d.config.ZFS.PromoteRestoredClones = true

	// An operator's own snapshot, OLDER than the restore origin, with no CSI
	// identity — exactly what listAllManagedSnapshots puts in the unowned bucket.
	foreign, err := client.SnapshotCreate(ctx, "pool/parent/source", "admin-preupgrade", nil)
	require.NoError(t, err)
	require.False(t, isCSISnapshot(foreign))
	require.False(t, isSnapshotTombstone(foreign))
	origin := seedOriginTombstone(t, client, "pool/parent/source", "snap")
	client.Datasets["pool/parent/source"] = &truenas.Dataset{Name: "pool/parent/source"}
	clone := seedCloneRestoredVolume(client, d, "restored", origin.ID)

	report := promoteReport()
	d.reconcilePromoteRestoredClones(ctx, []*truenas.Dataset{clone},
		nil, []*truenas.Snapshot{origin}, []*truenas.Snapshot{foreign}, map[string]tombstoneLedgerEntry{}, report)

	assert.Equal(t, 0, report.PromotedCloneCount, "an unowned snapshot in the migrating set must refuse the promote")
	assert.Empty(t, client.DatasetPromoteCalls)
	got, err := client.SnapshotGet(ctx, foreign.ID)
	require.NoError(t, err, "the operator's snapshot must not be re-parented")
	assert.Equal(t, "pool/parent/source", got.Dataset)
}

// The complete-inventory fix must not be satisfiable by simply never promoting:
// a source carrying only NEWER unowned snapshots (which ZFS does not migrate)
// must still promote.
func TestReconcilePromoteStillRunsWhenUnownedSnapshotIsNewerThanOrigin(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	d.config.ZFS.PromoteRestoredClones = true

	origin := seedOriginTombstone(t, client, "pool/parent/source", "snap")
	newer, err := client.SnapshotCreate(ctx, "pool/parent/source", "admin-after", nil)
	require.NoError(t, err)
	require.Greater(t, newer.CreateTXG, origin.CreateTXG, "fixture: the foreign snapshot is newer")
	client.Datasets["pool/parent/source"] = &truenas.Dataset{Name: "pool/parent/source"}
	clone := seedCloneRestoredVolume(client, d, "restored", origin.ID)

	report := promoteReport()
	d.reconcilePromoteRestoredClones(ctx, []*truenas.Dataset{clone},
		nil, []*truenas.Snapshot{origin}, []*truenas.Snapshot{newer}, map[string]tombstoneLedgerEntry{}, report)

	assert.Equal(t, 1, report.PromotedCloneCount, "a NEWER unowned snapshot does not migrate and must not block")
	assert.Contains(t, client.DatasetPromoteCalls, "pool/parent/restored")
}

func TestReconcilePromoteSkipsSharedOriginSiblings(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	d.config.ZFS.PromoteRestoredClones = true

	snap := seedOriginTombstone(t, client, "pool/parent/source", "snap")
	client.Datasets["pool/parent/source"] = &truenas.Dataset{Name: "pool/parent/source"}
	cloneA := seedCloneRestoredVolume(client, d, "restored-a", snap.ID)
	cloneB := seedCloneRestoredVolume(client, d, "restored-b", snap.ID)

	report := promoteReport()
	d.reconcilePromoteRestoredClones(ctx, []*truenas.Dataset{cloneA, cloneB},
		nil, []*truenas.Snapshot{snap}, nil, map[string]tombstoneLedgerEntry{}, report)

	assert.Equal(t, 0, report.PromotedCloneCount, "siblings sharing an origin are NOT promoted (R3 ordering rule)")
	assert.Empty(t, client.DatasetPromoteCalls)
}

func TestReconcilePromoteNoOpWhenDisabled(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	// PromoteRestoredClones defaults to false.

	snap := seedOriginTombstone(t, client, "pool/parent/source", "snap")
	clone := seedCloneRestoredVolume(client, d, "restored", snap.ID)

	report := promoteReport()
	d.reconcilePromoteRestoredClones(ctx, []*truenas.Dataset{clone},
		nil, []*truenas.Snapshot{snap}, nil, map[string]tombstoneLedgerEntry{}, report)

	assert.Equal(t, 0, report.PromotedCloneCount, "promote is off by default")
	assert.Empty(t, client.DatasetPromoteCalls)
	assert.NotEqual(t, "", datasetOriginSnapshotID(client.Datasets["pool/parent/restored"]), "the origin pin is untouched when disabled")
}

// B2 — the headline blocker: after a promote migrates a tombstone, its ledger
// provenance must FOLLOW it. Before the fix the reaper resolved the OLD id,
// retired the entry as "already gone", and the migrated tombstone was invisible
// and unreapable forever with the (default-off) scan fallback disabled.
func TestPromoteCarriesTombstoneLedgerProvenanceAcrossMigration(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	d.config.ZFS.PromoteRestoredClones = true
	require.False(t, d.config.Reconcile.TombstoneReaper.ScanFallback.EnabledOrDefault(),
		"precondition: the scan fallback is off, so the ledger is the ONLY provenance")

	oldTombstoneID := bookkeepingTombstone(t, d, client)
	restored := client.Datasets["pool/parent/restored"]
	require.Equal(t, oldTombstoneID, datasetOriginSnapshotID(restored), "precondition: the clone pins the tombstone")
	// Make the clone look like a snapshot-restored volume owned by this instance.
	require.NoError(t, client.DatasetSetUserProperty(ctx, restored.Name, PropDriverInstanceID, d.driverInstanceID()))
	require.NoError(t, client.DatasetSetUserProperty(ctx, restored.Name, PropVolumeContentSourceType, "snapshot"))

	tombstone, err := client.SnapshotGet(ctx, oldTombstoneID)
	require.NoError(t, err)
	listedClone, err := client.DatasetGet(ctx, restored.Name)
	require.NoError(t, err)

	parent, err := client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	ledger := tombstoneLedgerFromDataset(parent)
	require.Contains(t, ledger, tombstoneLedgerKey(oldTombstoneID), "precondition: the pre-promote entry exists")

	report := promoteReport()
	d.reconcilePromoteRestoredClones(ctx, []*truenas.Dataset{resourceQueryProjection(listedClone)},
		nil, []*truenas.Snapshot{tombstone}, nil, ledger, report)
	require.Equal(t, 1, report.PromotedCloneCount)

	newTombstoneID := "pool/parent/restored@" + snapshotShortName(tombstone)
	_, err = client.SnapshotGet(ctx, newTombstoneID)
	require.NoError(t, err, "the tombstone migrated onto the promoted clone")

	// The promote made the SOURCE volume destroyable (P3) — that is the whole
	// point of the feature. Destroy it, then the migrated tombstone's last
	// dependency is gone and it must be REAPABLE from its re-keyed provenance.
	require.NoError(t, client.DatasetDelete(ctx, "pool/parent/source", false, true))

	parent, err = client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	require.Contains(t, tombstoneLedgerFromDataset(parent), tombstoneLedgerKey(newTombstoneID),
		"the ledger entry followed the snapshot to its new id")

	retire := &tombstoneRetirementBatch{}
	reaped, reason := d.reapTombstoneSnapshot(ctx, tombstoneReconcileObject(t, client, newTombstoneID), time.Hour, retire)
	assert.True(t, reaped, "the migrated tombstone must be reapable, got: %s", reason)
	_, err = client.SnapshotGet(ctx, newTombstoneID)
	assert.True(t, truenas.IsNotFoundError(err), "the migrated tombstone is actually drained")
}
