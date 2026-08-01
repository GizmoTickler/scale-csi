package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// newHoldTestDriver builds a controller Driver backed by a MockClient with the
// GF2/E1 deletion-proof hold feature enabled.
func newHoldTestDriver(t *testing.T, client truenas.ClientInterface) *Driver {
	t.Helper()
	return &Driver{
		name: "csi.scale.io",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
				HoldCSISnapshots:  true,
			},
			NFS: NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
}

func createHoldSourceDataset(t *testing.T, client *truenas.MockClient, name string) {
	t.Helper()
	_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/" + name, Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
}

func TestCreateSnapshotPlacesHoldWhenEnabled(t *testing.T) {
	client := truenas.NewMockClient()
	d := newHoldTestDriver(t, client)
	createHoldSourceDataset(t, client, "hold-source")

	resp, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "held-snap", SourceVolumeId: "hold-source"})
	require.NoError(t, err)
	require.True(t, resp.GetSnapshot().GetReadyToUse())

	snapshotID := "pool/parent/hold-source@" + resp.GetSnapshot().GetSnapshotId()
	held, err := client.SnapshotIsHeld(context.Background(), snapshotID)
	require.NoError(t, err)
	assert.True(t, held, "CreateSnapshot must place a deletion-proof hold when zfs.holdCsiSnapshots is set")
	assert.Equal(t, 1, client.SnapshotHoldCalls)
}

func TestCreateSnapshotNoHoldWhenDisabled(t *testing.T) {
	client := truenas.NewMockClient()
	d := newHoldTestDriver(t, client)
	d.config.ZFS.HoldCSISnapshots = false
	createHoldSourceDataset(t, client, "nohold-source")

	resp, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "unheld-snap", SourceVolumeId: "nohold-source"})
	require.NoError(t, err)

	snapshotID := "pool/parent/nohold-source@" + resp.GetSnapshot().GetSnapshotId()
	held, err := client.SnapshotIsHeld(context.Background(), snapshotID)
	require.NoError(t, err)
	assert.False(t, held, "no hold is placed when the feature is off (default)")
	assert.Equal(t, 0, client.SnapshotHoldCalls, "the default path makes no hold call")
}

// holdFailureMock fails every SnapshotHold so the non-fatal degradation path can
// be exercised: the snapshot must still become ReadyToUse.
type holdFailureMock struct {
	*truenas.MockClient
}

func (m *holdFailureMock) SnapshotHold(ctx context.Context, snapshotID string) error {
	return fmt.Errorf("backend hiccup: hold unavailable")
}

func TestCreateSnapshotHoldFailureIsNonFatal(t *testing.T) {
	client := &holdFailureMock{MockClient: truenas.NewMockClient()}
	d := newHoldTestDriver(t, client)
	createHoldSourceDataset(t, client.MockClient, "holdfail-source")

	resp, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "holdfail-snap", SourceVolumeId: "holdfail-source"})
	require.NoError(t, err, "a hold failure must not fail CreateSnapshot")
	assert.True(t, resp.GetSnapshot().GetReadyToUse(), "the snapshot degrades to unprotected but stays ReadyToUse")
}

func TestDeleteSnapshotReleasesHoldBeforeDestroy(t *testing.T) {
	client := truenas.NewMockClient()
	d := newHoldTestDriver(t, client)
	createHoldSourceDataset(t, client, "hold-del-source")

	_, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "hold-del-snap", SourceVolumeId: "hold-del-source"})
	require.NoError(t, err)
	snapshotID := "pool/parent/hold-del-source@hold-del-snap"
	held, err := client.SnapshotIsHeld(context.Background(), snapshotID)
	require.NoError(t, err)
	require.True(t, held, "precondition: the snapshot is held after create")

	// If DeleteSnapshot did not release first, the mock destroy would EBUSY on the
	// held snapshot and the delete would fail. Success proves release-before-destroy.
	_, err = d.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "hold-del-snap"})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, client.SnapshotReleaseCalls, 1, "DeleteSnapshot must release the hold before destroy")
	_, err = client.SnapshotGet(context.Background(), snapshotID)
	assert.True(t, truenas.IsNotFoundError(err), "the released snapshot is destroyed")
}

// TestForeignDeleteBlockedByHold proves the protection half of E1: an actor that
// calls destroy directly (a box-wide periodic-task prune, an admin) hits EBUSY on
// a held snapshot. Only the driver's release-first paths can remove it.
func TestForeignDeleteBlockedByHold(t *testing.T) {
	client := truenas.NewMockClient()
	d := newHoldTestDriver(t, client)
	createHoldSourceDataset(t, client, "foreign-source")

	_, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "foreign-snap", SourceVolumeId: "foreign-source"})
	require.NoError(t, err)
	snapshotID := "pool/parent/foreign-source@foreign-snap"

	err = client.SnapshotDelete(context.Background(), snapshotID, false, false)
	require.Error(t, err, "a foreign destroy of a held snapshot must be blocked")
	assert.False(t, truenas.IsNotFoundError(err), "the snapshot must survive the blocked foreign delete")

	_, err = client.SnapshotGet(context.Background(), snapshotID)
	assert.NoError(t, err, "the held snapshot is still present after the blocked foreign delete")
}

// TestReaperReleasesHoldBeforeReap covers the R1 wedge: a tombstone that reaches
// the reaper still held (feature toggled on mid-life, or a crash between rename
// and DeleteSnapshot's release) must be released then reaped, never EBUSY-wedged.
func TestReaperReleasesHoldBeforeReap(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	d.config.ZFS.HoldCSISnapshots = true

	tombstoneID := bookkeepingTombstone(t, d, client)

	// Simulate a hold that outlived the tombstone rename (P1: holds survive rename).
	require.NoError(t, client.SnapshotHold(ctx, tombstoneID))
	held, err := client.SnapshotIsHeld(ctx, tombstoneID)
	require.NoError(t, err)
	require.True(t, held, "precondition: the tombstone is held when it reaches the reaper")

	// Remove the restored clone so the dependent-clone gate passes.
	require.NoError(t, client.DatasetDelete(ctx, "pool/parent/restored", false, true))

	retire := &tombstoneRetirementBatch{}
	reaped, reason := d.reapTombstoneSnapshot(ctx, tombstoneReconcileObject(t, client, tombstoneID), time.Hour, retire)
	assert.True(t, reaped, "release-before-reap must let the reaper destroy a held tombstone, got refusal: %s", reason)

	held, err = client.SnapshotIsHeld(ctx, tombstoneID)
	require.NoError(t, err)
	assert.False(t, held, "the reaper releases the hold before destroy")
	_, err = client.SnapshotGet(ctx, tombstoneID)
	assert.True(t, truenas.IsNotFoundError(err), "the held tombstone is reaped, not wedged")
}

// H4 — the rollback wedge. Holds are backend state that outlives the flag that
// placed them, so with zfs.holdCsiSnapshots turned back OFF every previously
// held CSI snapshot became undeletable: SnapshotDelete returned EBUSY, which is
// neither NotFound nor has-clones, so DeleteSnapshot answered codes.Internal and
// external-snapshotter retried forever (and the source volume's DeleteVolume was
// blocked behind it). Release must run whenever a hold is actually present,
// regardless of the flag.
func TestDeleteSnapshotSucceedsOnHeldSnapshotAfterFlagDisabled(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newHoldTestDriver(t, client)
	createHoldSourceDataset(t, client, "rollback-source")

	_, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "rollback-snap", SourceVolumeId: "rollback-source"})
	require.NoError(t, err)
	snapshotID := "pool/parent/rollback-source@rollback-snap"
	held, err := client.SnapshotIsHeld(ctx, snapshotID)
	require.NoError(t, err)
	require.True(t, held, "precondition: the snapshot was held while the feature was on")

	// The operator rolls the Helm value back to false. The hold remains.
	d.config.ZFS.HoldCSISnapshots = false

	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "rollback-snap"})
	require.NoError(t, err, "disabling the feature must never wedge an already-held snapshot")
	_, err = client.SnapshotGet(ctx, snapshotID)
	assert.True(t, truenas.IsNotFoundError(err), "the held snapshot is released and destroyed")
}

// H4 — the recursive-destroy half of the same wedge (codex E1 finding #3): ZFS
// refuses the WHOLE recursive dataset destroy with EBUSY when any snapshot
// beneath it is held, and the per-snapshot release sites never see those
// snapshots. A held driver tombstone therefore made its volume undeletable.
func TestDeleteVolumeReleasesHeldDriverTombstoneUnderRecursiveDestroy(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	d.config.ZFS.HoldCSISnapshots = true
	d.config.ZFS.DestroyForeignSnapshotsOnDelete = true

	tombstoneID := bookkeepingTombstone(t, d, client)
	require.NoError(t, client.SnapshotHold(ctx, tombstoneID))
	// Drop the restored clone so only the held tombstone blocks the destroy.
	require.NoError(t, client.DatasetDelete(ctx, "pool/parent/restored", false, true))

	// The rollback case again: the flag is off by the time the volume is deleted.
	d.config.ZFS.HoldCSISnapshots = false

	_, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source"})
	require.NoError(t, err, "a held driver tombstone must not make its volume undeletable")
	assert.NotContains(t, client.Datasets, "pool/parent/source")
}

// R2 — the release recovery must never strip a hold the driver cannot prove it
// owns. A FOREIGN held snapshot keeps the recursive destroy correctly refused.
func TestDeleteVolumeDoesNotReleaseForeignHeldSnapshot(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newHoldTestDriver(t, client)
	d.config.ZFS.DestroyForeignSnapshotsOnDelete = true
	createHoldSourceDataset(t, client, "foreign-held")

	_, err := client.SnapshotCreate(ctx, "pool/parent/foreign-held", "admin-backup", nil)
	require.NoError(t, err)
	require.NoError(t, client.SnapshotHold(ctx, "pool/parent/foreign-held@admin-backup"))

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "foreign-held"})
	require.Error(t, err, "a foreign hold must keep the destroy refused")

	held, err := client.SnapshotIsHeld(ctx, "pool/parent/foreign-held@admin-backup")
	require.NoError(t, err)
	assert.True(t, held, "the driver must never release a foreign snapshot's hold")
	_, err = client.SnapshotGet(ctx, "pool/parent/foreign-held@admin-backup")
	assert.NoError(t, err, "the foreign snapshot survives")
}
