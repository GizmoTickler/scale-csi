package driver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// tombstoneIDFor finds the tombstone that the driver renamed FROM snapshotName.
// findTombstoneID returns whichever tombstone comes first, which cannot tell two
// tombstones on one dataset apart — exactly what this regression needs.
func tombstoneIDFor(t *testing.T, client *truenas.MockClient, sourceDataset, snapshotName string) string {
	t.Helper()
	snaps, err := client.SnapshotList(context.Background(), sourceDataset)
	require.NoError(t, err)
	prefix := sourceDataset + "@" + snapshotName + "-csi-deleted-"
	for _, snap := range snaps {
		if isSnapshotTombstone(snap) && strings.HasPrefix(snap.ID, prefix) {
			return snap.ID
		}
	}
	return ""
}

// tombstoneOnSource reproduces how a tombstone actually arises: a backup mounts
// a CLONE of the snapshot, DeleteSnapshot therefore cannot destroy it and defers
// instead, leaving a retained tombstone. releaseClone then destroys that clone,
// modeling the backup finishing — which is the state a later reconcile pass is
// supposed to reap.
func tombstoneOnSource(
	t *testing.T,
	d *Driver,
	client *truenas.MockClient,
	sourceDataset, snapshotName, cloneInto string,
	releaseClone bool,
) string {
	t.Helper()
	ctx := context.Background()
	snapshot, err := client.SnapshotCreate(ctx, sourceDataset, snapshotName, map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           snapshotName,
		PropCSISnapshotSourceVolumeID: "source",
	})
	require.NoError(t, err)
	snapshot.Properties["creation"] = map[string]interface{}{
		"parsed": float64(time.Now().Add(-48 * time.Hour).Unix()),
	}
	require.NoError(t, client.SnapshotClone(ctx, snapshot.ID, cloneInto))
	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snapshotName})
	require.NoError(t, err)
	tombstoneID := tombstoneIDFor(t, client, sourceDataset, snapshotName)
	require.NotEmpty(t, tombstoneID, "snapshot %s must be retained as a tombstone", snapshotName)
	if releaseClone {
		require.NoError(t, client.DatasetDelete(ctx, cloneInto, false, true))
	}
	return tombstoneID
}

// Regression (live 2026-09-17, downloads/qbittorrent): reapTombstoneSnapshot
// gated the destroy of ONE tombstone on DatasetHasDependentClones, which answers
// the dataset-wide question "is any snapshot of this dataset cloned". That is
// the right question when deleting a whole volume and the wrong one here, so a
// single live clone vetoed every tombstone on the dataset.
//
// It is not a corner case. A volume under hourly clone-based backup (kopiur and
// VolSync both mount a CLONE of the snapshot they read) has a clone present for
// part of every hour, so any reconcile pass overlapping a backup refused that
// dataset wholesale. Live: 12 tombstones refused on one PVC with zero clones of
// their own, oldest 13h46m, while sibling datasets reaped 96 in the same pass.
//
// Pre-fix this test fails on the second assertion: the uncloned tombstone is
// refused with "tombstone snapshot still has dependent clones" because its
// SIBLING is cloned.
func TestReapTombstoneIgnoresClonesOfSiblingSnapshots(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	client.NoDeferredSnapshotDestroy = true
	mustCreateParentDataset(t, client)

	source := addReconcileDataset(client, "source", time.Now().Add(-72*time.Hour), true, testGiB)
	require.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropDriverInstanceID, d.driverInstanceID()))

	// An earlier hourly backup that has FINISHED: its clone is gone, so this
	// tombstone is free to reap.
	freeTombstone := tombstoneOnSource(t, d, client, source.Name, "snap-hourly", "pool/parent/restore-hourly", true)
	// The backup running right now on the same volume: its clone is still live.
	clonedTombstone := tombstoneOnSource(t, d, client, source.Name, "snap-backup", "pool/parent/restore-backup", false)
	require.NotEqual(t, clonedTombstone, freeTombstone)

	clones, err := client.SnapshotDependentClones(ctx, freeTombstone)
	require.NoError(t, err)
	require.Empty(t, clones, "precondition: nothing is cloned from the free tombstone")

	retire := &tombstoneRetirementBatch{}

	// The cloned tombstone must still be refused — the fix must not weaken this.
	reaped, reason := d.reapTombstoneSnapshot(ctx, tombstoneReconcileObject(t, client, clonedTombstone), time.Hour, retire)
	assert.False(t, reaped, "a tombstone whose OWN snapshot is cloned must stay")
	assert.Equal(t, "tombstone snapshot still has dependent clones", reason)

	// The uncloned sibling must reap despite that live clone on the dataset.
	reaped, reason = d.reapTombstoneSnapshot(ctx, tombstoneReconcileObject(t, client, freeTombstone), time.Hour, retire)
	assert.True(t, reaped, "a sibling's clone must not veto this tombstone, got refusal: %s", reason)

	retire.flush(ctx, d, nil)
	_, err = client.SnapshotGet(ctx, freeTombstone)
	assert.True(t, truenas.IsNotFoundError(err), "the reaped tombstone is destroyed")
	_, err = client.SnapshotGet(ctx, clonedTombstone)
	assert.NoError(t, err, "the cloned tombstone survives")
}
