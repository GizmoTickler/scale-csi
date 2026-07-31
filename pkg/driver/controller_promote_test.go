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

func TestMockDatasetPromoteMigratesOriginAndReparentsSiblings(t *testing.T) {
	client := truenas.NewMockClient()
	ctx := context.Background()

	_, err := client.SnapshotCreate(ctx, "pool/parent/source", "snap", nil)
	require.NoError(t, err)
	// Two sibling clones of source@snap.
	client.Datasets["pool/parent/clone-a"] = &truenas.Dataset{Name: "pool/parent/clone-a", Origin: originProp("pool/parent/source@snap")}
	client.Datasets["pool/parent/clone-b"] = &truenas.Dataset{Name: "pool/parent/clone-b", Origin: originProp("pool/parent/source@snap")}

	require.NoError(t, client.DatasetPromote(ctx, "pool/parent/clone-a"))

	// The promoted clone is independent.
	assert.Equal(t, "", datasetOriginSnapshotID(client.Datasets["pool/parent/clone-a"]))
	// The origin snapshot migrated onto the promoted clone.
	_, err = client.SnapshotGet(ctx, "pool/parent/source@snap")
	assert.True(t, truenas.IsNotFoundError(err), "the origin snapshot no longer lives under source")
	migrated, err := client.SnapshotGet(ctx, "pool/parent/clone-a@snap")
	require.NoError(t, err)
	assert.Equal(t, "pool/parent/clone-a", migrated.Dataset)
	// The sibling clone is re-parented onto the migrated snapshot.
	assert.Equal(t, "pool/parent/clone-a@snap", datasetOriginSnapshotID(client.Datasets["pool/parent/clone-b"]))
}

func TestReconcilePromoteRestoredClonesSoleDependent(t *testing.T) {
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	d.config.ZFS.PromoteRestoredClones = true

	_, err := client.SnapshotCreate(context.Background(), "pool/parent/source", "snap", nil)
	require.NoError(t, err)
	clone := seedCloneRestoredVolume(client, d, "restored", "pool/parent/source@snap")

	report := &ReconcileReport{}
	d.reconcilePromoteRestoredClones(context.Background(), []*truenas.Dataset{clone}, report)

	assert.Equal(t, 1, report.PromotedCloneCount, "a sole-dependent clone-restored volume is promoted")
	assert.Contains(t, client.DatasetPromoteCalls, "pool/parent/restored")
	assert.Equal(t, "", datasetOriginSnapshotID(client.Datasets["pool/parent/restored"]), "promote clears the origin pin")
}

func TestReconcilePromoteSkipsSharedOriginSiblings(t *testing.T) {
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	d.config.ZFS.PromoteRestoredClones = true

	_, err := client.SnapshotCreate(context.Background(), "pool/parent/source", "snap", nil)
	require.NoError(t, err)
	cloneA := seedCloneRestoredVolume(client, d, "restored-a", "pool/parent/source@snap")
	cloneB := seedCloneRestoredVolume(client, d, "restored-b", "pool/parent/source@snap")

	report := &ReconcileReport{}
	d.reconcilePromoteRestoredClones(context.Background(), []*truenas.Dataset{cloneA, cloneB}, report)

	assert.Equal(t, 0, report.PromotedCloneCount, "siblings sharing an origin are NOT promoted (R3 ordering rule)")
	assert.Empty(t, client.DatasetPromoteCalls)
}

func TestReconcilePromoteNoOpWhenDisabled(t *testing.T) {
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	// PromoteRestoredClones defaults to false.

	_, err := client.SnapshotCreate(context.Background(), "pool/parent/source", "snap", nil)
	require.NoError(t, err)
	clone := seedCloneRestoredVolume(client, d, "restored", "pool/parent/source@snap")

	report := &ReconcileReport{}
	d.reconcilePromoteRestoredClones(context.Background(), []*truenas.Dataset{clone}, report)

	assert.Equal(t, 0, report.PromotedCloneCount, "promote is off by default")
	assert.Empty(t, client.DatasetPromoteCalls)
	assert.NotEqual(t, "", datasetOriginSnapshotID(client.Datasets["pool/parent/restored"]), "the origin pin is untouched when disabled")
}

// TestReaperRetiresOrphanedLedgerEntryAfterPromote covers the R3 ledger
// self-heal: promoting a clone migrates its origin (tombstone) snapshot onto the
// clone, so the ledger entry keyed by the OLD tombstone id no longer resolves.
// The reaper must retire that orphaned entry as "already gone" rather than wedge.
func TestReaperRetiresOrphanedLedgerEntryAfterPromote(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)

	tombstoneID := bookkeepingTombstone(t, d, client) // source@<name>-csi-deleted-<n>, ledger entry written
	// The restored clone's origin is the tombstone (the rename re-pointed it).
	restored := client.Datasets["pool/parent/restored"]
	require.NotNil(t, restored)
	require.Equal(t, tombstoneID, datasetOriginSnapshotID(restored), "precondition: the clone pins the tombstone")

	// Promote the clone: the tombstone migrates onto the clone, orphaning the
	// ledger entry keyed by the old tombstone id.
	require.NoError(t, client.DatasetPromote(ctx, "pool/parent/restored"))
	_, err := client.SnapshotGet(ctx, tombstoneID)
	require.True(t, truenas.IsNotFoundError(err), "the tombstone migrated away from its old id")

	// Reap by the OLD id: SnapshotGet no longer resolves it, so the reaper must
	// retire the orphaned ledger entry as already-gone rather than refuse forever.
	// Build the object manually (the helper requires a live snapshot lookup).
	tombstone := ReconcileObject{
		ID:             tombstoneID,
		BackendID:      tombstoneID,
		SourceVolumeID: "source",
	}
	retire := &tombstoneRetirementBatch{}
	reaped, reason := d.reapTombstoneSnapshot(ctx, tombstone, time.Hour, retire)
	assert.True(t, reaped, "the orphaned ledger entry is retired as already-gone after promote, got: %s", reason)
}
