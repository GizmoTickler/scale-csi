package driver

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// boundReconcilePV is reconcilePV with an explicit Bound phase: stamp adoption
// (gate 4) only trusts a live Bound PV of this driver.
func boundReconcilePV(volumeHandle, driverName string) *corev1.PersistentVolume {
	pv := reconcilePV(volumeHandle, driverName)
	pv.Status.Phase = corev1.VolumeBound
	return pv
}

// setupLegacyTombstone reproduces the 2026-07-23 04:00Z production condition: a
// migration-era source dataset that carries LOCAL managed_resource +
// csi_volume_name but NO driver_instance_id stamp, plus an aged deferred-delete
// tombstone (with its parent-dataset ledger entry) whose source the reaper
// therefore refuses. The clone that forced the tombstone to be retained is left
// in place; callers delete it when they want the reap to succeed.
func setupLegacyTombstone(t *testing.T, d *Driver, client *truenas.MockClient, volumeID string) string {
	t.Helper()
	ctx := context.Background()
	client.NoDeferredSnapshotDestroy = true
	mustCreateParentDataset(t, client)

	source := addReconcileDataset(client, volumeID, time.Now().Add(-72*time.Hour), true, testGiB)
	source.Mountpoint = "/mnt/pool/parent/" + volumeID
	// Deliberately NO PropDriverInstanceID stamp: this is the legacy condition.
	snapshot, err := client.SnapshotCreate(ctx, source.Name, "snap-1", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "snap-1",
		PropCSISnapshotSourceVolumeID: volumeID,
	})
	require.NoError(t, err)
	snapshot.Properties["creation"] = map[string]interface{}{"parsed": float64(time.Now().Add(-48 * time.Hour).Unix())}
	require.NoError(t, client.SnapshotClone(ctx, snapshot.ID, "pool/parent/restored"))

	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "snap-1"})
	require.NoError(t, err, "DeleteSnapshot must succeed on an unstamped legacy source")

	tombstoneID := findTombstoneID(t, client, source.Name)
	require.NotEmpty(t, tombstoneID, "the tombstone is retained while its clone is live")
	parent, err := client.DatasetGet(ctx, "pool/parent")
	require.NoError(t, err)
	require.Contains(t, tombstoneLedgerFromDataset(parent), tombstoneLedgerKey(tombstoneID),
		"the tombstone must have driver provenance in the parent ledger")
	return tombstoneID
}

// tombstoneReconcileObject rebuilds the ReconcileObject the classifier would
// produce for a tombstone, so reapTombstoneSnapshot can be exercised directly
// (before any adoption pass runs).
func tombstoneReconcileObject(t *testing.T, client *truenas.MockClient, tombstoneID string) ReconcileObject {
	t.Helper()
	snap, err := client.SnapshotGet(context.Background(), tombstoneID)
	require.NoError(t, err)
	return ReconcileObject{
		ID:             snap.ID,
		BackendID:      snap.ID,
		SourceVolumeID: snap.Dataset[len("pool/parent/"):],
		CreatedAt:      time.Unix(snap.GetCreationTime(), 0),
	}
}

// TestReconcileAdoptsLegacyStampAndReapsRefusedTombstone is the end-to-end
// regression for the 04:00Z incident: the reaper refuses the legacy source's
// tombstone with the exact ownership-stamp reason, then the SAME reconcile pass
// adopts the stamp and reaps the tombstone.
func TestReconcileAdoptsLegacyStampAndReapsRefusedTombstone(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	tombstoneID := setupLegacyTombstone(t, d, client, "source")

	// Pre-fix behavior: the reaper refuses the unstamped legacy source's
	// tombstone with the exact ownership-stamp reason (and would refuse it
	// forever, leaking the ledger entry and snapshot).
	tombstone := tombstoneReconcileObject(t, client, tombstoneID)
	reaped, reason := d.reapTombstoneSnapshot(ctx, tombstone, time.Hour)
	assert.False(t, reaped)
	assert.Equal(t, "tombstone source dataset does not carry this driver instance's ownership stamp", reason)
	_, err := client.SnapshotGet(ctx, tombstoneID)
	require.NoError(t, err, "the refused tombstone is still present")

	// Release the clone so the reap can succeed once the stamp is adopted.
	require.NoError(t, client.DatasetDelete(ctx, "pool/parent/restored", false, true))

	// Post-fix: the SAME pass adopts the stamp and reaps the tombstone.
	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Contains(t, report.AdoptedStamps, "source")
	assert.Equal(t, 1, report.AdoptedStampCount)
	assert.Contains(t, report.DeletedTombstones, tombstoneID)

	source, err := client.DatasetGet(ctx, "pool/parent/source")
	require.NoError(t, err)
	assert.True(t, datasetHasLocalUserProperty(source, PropDriverInstanceID, d.driverInstanceID()),
		"the legacy source now carries this instance's local ownership stamp")
	_, err = client.SnapshotGet(ctx, tombstoneID)
	assert.True(t, truenas.IsNotFoundError(err), "the released tombstone is reaped in the same pass")
	parent, err := client.DatasetGet(ctx, "pool/parent")
	require.NoError(t, err)
	assert.NotContains(t, tombstoneLedgerFromDataset(parent), tombstoneLedgerKey(tombstoneID),
		"the ledger entry is retired with the reaped tombstone")
}

// TestReconcileStampAdoptionRequiresBoundPV: a legacy dataset referenced only by
// a non-Bound PV is never adopted, and its tombstone stays refused (fail-safe).
func TestReconcileStampAdoptionRequiresBoundPV(t *testing.T) {
	ctx := context.Background()
	pv := reconcilePV("source", "csi.scale.io") // phase != Bound
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	tombstoneID := setupLegacyTombstone(t, d, client, "source")
	require.NoError(t, client.DatasetDelete(ctx, "pool/parent/restored", false, true))

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.AdoptedStamps, "a non-Bound PV must not authorize adoption")
	assert.Empty(t, report.DeletedTombstones, "the tombstone stays refused while the source is unstamped")

	source, err := client.DatasetGet(ctx, "pool/parent/source")
	require.NoError(t, err)
	assert.False(t, datasetHasLocalUserProperty(source, PropDriverInstanceID, d.driverInstanceID()))
	_, err = client.SnapshotGet(ctx, tombstoneID)
	assert.NoError(t, err, "the tombstone is still present")
}

// TestReconcileStampAdoptionNeverOverwritesForeignStamp: a dataset already
// stamped by ANOTHER driver instance is left untouched even when a Bound PV of
// this driver references it (a second driver sharing the pool is never
// hijacked).
func TestReconcileStampAdoptionNeverOverwritesForeignStamp(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	mustCreateParentDataset(t, client)
	source := addReconcileDataset(client, "source", time.Now().Add(-72*time.Hour), true, testGiB)
	require.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropDriverInstanceID, "another-controller"))

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.AdoptedStamps)

	fresh, err := client.DatasetGet(ctx, source.Name)
	require.NoError(t, err)
	assert.Equal(t, "another-controller", datasetUserProperty(fresh, PropDriverInstanceID),
		"the foreign instance stamp must be unchanged")
}

// TestReconcileStampAdoptionSkipsInheritedManagedResource: a dataset that only
// inherits managed_resource (source != local) was never created by this driver
// and is never adopted, even with a matching Bound PV.
func TestReconcileStampAdoptionSkipsInheritedManagedResource(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("nested", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	mustCreateParentDataset(t, client)
	dataset := addReconcileDataset(client, "nested", time.Now().Add(-72*time.Hour), true, testGiB)
	// Demote managed_resource to inherited: the listing still sees value "true",
	// but the source-bearing re-read must reject it.
	dataset.UserProperties[PropManagedResource] = truenas.UserProperty{Value: "true", Source: "inherited from pool/parent"}

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.AdoptedStamps)

	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	assert.Empty(t, datasetUserProperty(fresh, PropDriverInstanceID))
}

// TestReconcileStampAdoptionIgnoresOtherDriverPV: adoption requires a Bound PV of
// THIS driver. A volume referenced only by another driver's PV (while this
// driver has other PVs, so the empty-list fail-safe is not what stops it) is
// never adopted.
func TestReconcileStampAdoptionIgnoresOtherDriverPV(t *testing.T) {
	ctx := context.Background()
	// This driver has a Bound PV (fail-safe passes) but NOT for "source"; "source"
	// is referenced only by another driver's Bound PV.
	ourPV := boundReconcilePV("other-vol", "csi.scale.io")
	foreignPV := boundReconcilePV("source", "other.driver.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{ourPV, foreignPV}, nil)
	mustCreateParentDataset(t, client)
	source := addReconcileDataset(client, "source", time.Now().Add(-72*time.Hour), true, testGiB)

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.AdoptedStamps)

	fresh, err := client.DatasetGet(ctx, source.Name)
	require.NoError(t, err)
	assert.Empty(t, datasetUserProperty(fresh, PropDriverInstanceID))
}

// TestReconcileStampAdoptionEmptyPVListFailSafe: with no PVs at all for this
// driver, the live PV list is an API discontinuity, not evidence — adopt nothing.
func TestReconcileStampAdoptionEmptyPVListFailSafe(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil) // zero PVs
	mustCreateParentDataset(t, client)
	source := addReconcileDataset(client, "source", time.Now().Add(-72*time.Hour), true, testGiB)

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.AdoptedStamps, "empty PV list for the driver must adopt nothing")

	fresh, err := client.DatasetGet(ctx, source.Name)
	require.NoError(t, err)
	assert.Empty(t, datasetUserProperty(fresh, PropDriverInstanceID))
}

// TestReconcileStampAdoptionAlreadyStampedNoOp: a dataset this driver already
// stamped is skipped write-free (idempotent, cheap when nothing qualifies).
func TestReconcileStampAdoptionAlreadyStampedNoOp(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	mustCreateParentDataset(t, client)
	source := addReconcileDataset(client, "source", time.Now().Add(-72*time.Hour), true, testGiB)
	require.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropDriverInstanceID, d.driverInstanceID()))
	writesBefore := client.SetUserPropertiesCallCount()

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.AdoptedStamps)
	assert.Equal(t, writesBefore, client.SetUserPropertiesCallCount(),
		"an already-stamped dataset must not trigger a write")
}

// TestReconcileStampAdoptionRespectsCap: adoptions per pass are bounded by
// maxPerRun as a blast-radius bound.
func TestReconcileStampAdoptionRespectsCap(t *testing.T) {
	ctx := context.Background()
	objects := make([]runtime.Object, 0, 3)
	for i := 0; i < 3; i++ {
		objects = append(objects, boundReconcilePV("cap-"+string(rune('0'+i)), "csi.scale.io"))
	}
	d, client := newReconcileTestDriver(t, false, objects, nil)
	d.config.Reconcile.Delete.MaxPerRun = 2
	mustCreateParentDataset(t, client)
	for i := 0; i < 3; i++ {
		addReconcileDataset(client, "cap-"+string(rune('0'+i)), time.Now().Add(-72*time.Hour), true, testGiB)
	}

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Len(t, report.AdoptedStamps, 2, "adoptions are capped at maxPerRun per pass")
}

// TestReconcileStampAdoptionSkipsInheritedInstanceStamp: an INHERITED
// driver_instance_id (any source, not just local/foreign-local) must block
// adoption and be left untouched — spec Item 1 rule 3. (Opus batch-16 note:
// the code handled this; this test makes the rule self-documenting.)
func TestReconcileStampAdoptionSkipsInheritedInstanceStamp(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("inherited-stamp", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	mustCreateParentDataset(t, client)
	dataset := addReconcileDataset(client, "inherited-stamp", time.Now().Add(-72*time.Hour), true, testGiB)
	dataset.UserProperties[PropDriverInstanceID] = truenas.UserProperty{Value: "some-other-install", Source: "inherited from pool/parent"}

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.AdoptedStamps)

	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	prop := fresh.UserProperties[PropDriverInstanceID]
	assert.Equal(t, "some-other-install", prop.Value, "inherited instance stamp must be untouched")
	assert.NotEqual(t, "local", prop.Source, "stamp must not have been re-written as local")
}
