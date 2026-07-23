package driver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// createRemnantClone creates an unstamped clone of a tombstone-named snapshot on
// a fresh (unmanaged) source dataset and returns the clone's ZFS origin snapshot
// ID. The clone is the remnant-orphan candidate: it carries no local ownership
// stamp (a clone inherits source properties with the origin-snapshot source, never
// "local"), exactly the shape the controller OOM crash loop manufactured.
func createRemnantClone(t *testing.T, client *truenas.MockClient, sourceID, cloneID, snapshotName string) string {
	t.Helper()
	ctx := context.Background()
	source := addReconcileDataset(client, sourceID, time.Now().Add(-72*time.Hour), false, 100)
	snap, err := client.SnapshotCreate(ctx, source.Name, snapshotName, nil)
	require.NoError(t, err)
	cloneName := "pool/parent/" + cloneID
	require.NoError(t, client.SnapshotClone(ctx, snap.ID, cloneName))
	return snap.ID
}

// writeRemnantMarker writes an in-flight creation marker for a remnant clone.
func writeRemnantMarker(t *testing.T, d *Driver, volumeID, origin, mode, instance, nonce string, startedAt time.Time) {
	t.Helper()
	marker := inflightMarker{
		Version:    inflightMarkerVersion,
		Instance:   instance,
		Dataset:    "pool/parent/" + volumeID,
		Mode:       mode,
		SourceType: "snapshot",
		SourceID:   "snap-src",
		Origin:     origin,
		Protocol:   "nfs",
		Nonce:      nonce,
		StartedAt:  startedAt.UTC().Format(time.RFC3339Nano),
	}
	require.NoError(t, d.writeInflightMarker(context.Background(), marker))
}

// TestReconcileRemnantOrphanZombieLifecycle reproduces the 2026-07-23 live
// incident end to end: a clone created between `zfs clone` and the ownership
// stamp, whose marker survived the crash and whose same-name CreateVolume retry
// never comes. It is NOT classified before minOrphanAge, IS classified after,
// is destroyed only under opts.Delete, retires its marker, and releases the
// tombstone origin snapshot's last clone.
func TestReconcileRemnantOrphanZombieLifecycle(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)

	originSnapID := createRemnantClone(t, client, "tombstone-origin", "remnant-vol", "snap-csi-deleted-1")
	writeRemnantMarker(t, d, "remnant-vol", originSnapID, inflightModeClone, d.driverInstanceID(), "nonce-zombie",
		time.Now().Add(-2*time.Hour))

	// The clone is the origin snapshot's only dependent clone before the reap.
	hasClones, err := client.DatasetHasDependentClones(ctx, "pool/parent/tombstone-origin")
	require.NoError(t, err)
	require.True(t, hasClones, "the tombstone origin snapshot starts with the remnant as its clone")

	// Before minOrphanAge the marker is still recovery-relevant: not classified.
	young, err := d.ReconcileOrphans(ctx, ReconcileOptions{MinOrphanAge: 24 * time.Hour})
	require.NoError(t, err)
	assert.Empty(t, young.RemnantVolumes, "a remnant younger than minOrphanAge must not be classified")

	// After minOrphanAge it is classified, but detection alone never destroys.
	detected, err := d.ReconcileOrphans(ctx, ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	require.Len(t, detected.RemnantVolumes, 1)
	assert.Equal(t, "remnant-vol", detected.RemnantVolumes[0].ID)
	assert.Equal(t, "pool/parent/remnant-vol", detected.RemnantVolumes[0].BackendID)
	assert.Empty(t, detected.DeletedRemnants, "detection without opts.Delete must not destroy")
	_, err = client.DatasetGet(ctx, "pool/parent/remnant-vol")
	require.NoError(t, err, "detection alone leaves the remnant dataset in place")

	// Under opts.Delete the remnant is destroyed and its marker retired.
	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, []string{"remnant-vol"}, report.DeletedRemnants)
	_, err = client.DatasetGet(ctx, "pool/parent/remnant-vol")
	require.Error(t, err, "the guarded destroy removes the remnant dataset")
	require.True(t, truenas.IsNotFoundError(err))

	goneMarker, err := d.readInflightMarker(ctx, "remnant-vol")
	require.NoError(t, err)
	assert.Nil(t, goneMarker, "the marker is retired once the remnant is destroyed")

	hasClones, err = client.DatasetHasDependentClones(ctx, "pool/parent/tombstone-origin")
	require.NoError(t, err)
	assert.False(t, hasClones, "destroying the remnant releases the tombstone origin snapshot's last clone")
	_, err = client.SnapshotGet(ctx, originSnapID)
	require.NoError(t, err, "the tombstone origin snapshot itself survives the non-recursive destroy")
}

// TestReconcileRemnantOrphanSkipsStampedDataset proves a stamped dataset is never
// a remnant orphan: the existing stale-marker sweep retires the marker (the OLD
// path) and the remnant phase leaves both dataset and marker-classification alone.
func TestReconcileRemnantOrphanSkipsStampedDataset(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)

	originSnapID := createRemnantClone(t, client, "tombstone-origin", "stamped-vol", "snap-csi-deleted-1")
	// Creation completed: the dataset carries a local driver-instance stamp, only
	// the marker delete was lost.
	require.NoError(t, client.DatasetSetUserProperty(ctx, "pool/parent/stamped-vol", PropDriverInstanceID, d.driverInstanceID()))
	writeRemnantMarker(t, d, "stamped-vol", originSnapID, inflightModeClone, d.driverInstanceID(), "nonce-stamped",
		time.Now().Add(-48*time.Hour))

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.RemnantVolumes, "a stamped dataset is never classified as a remnant orphan")
	assert.Empty(t, report.DeletedRemnants)

	retired, err := d.readInflightMarker(ctx, "stamped-vol")
	require.NoError(t, err)
	assert.Nil(t, retired, "the stale-marker sweep (old path) retires the stamped dataset's marker")
	_, err = client.DatasetGet(ctx, "pool/parent/stamped-vol")
	require.NoError(t, err, "the stamped dataset survives")
}

// TestReconcileRemnantOrphanIgnoresForeignInstanceMarker proves a marker written
// by another driver instance is neither classified nor retired.
func TestReconcileRemnantOrphanIgnoresForeignInstanceMarker(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)

	originSnapID := createRemnantClone(t, client, "tombstone-origin", "foreign-vol", "snap-csi-deleted-1")
	writeRemnantMarker(t, d, "foreign-vol", originSnapID, inflightModeClone, "some-other-instance", "nonce-foreign",
		time.Now().Add(-48*time.Hour))

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.RemnantVolumes, "a foreign-instance marker is never classified")
	assert.Empty(t, report.DeletedRemnants)

	parent, err := client.DatasetGet(ctx, "pool/parent")
	require.NoError(t, err)
	_, present := parent.UserProperties[inflightMarkerKey("foreign-vol")]
	assert.True(t, present, "the foreign-instance marker is left untouched")
	_, err = client.DatasetGet(ctx, "pool/parent/foreign-vol")
	require.NoError(t, err, "the foreign remnant dataset survives")
}

// TestReconcileRemnantOrphanOriginMismatchSkips proves the identity binding: a
// clone whose actual ZFS origin does not match the marker's recorded origin is
// classified but never destroyed.
func TestReconcileRemnantOrphanOriginMismatchSkips(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)

	createRemnantClone(t, client, "tombstone-origin", "mismatch-vol", "snap-csi-deleted-1")
	// The marker records a DIFFERENT origin than the clone actually has.
	writeRemnantMarker(t, d, "mismatch-vol", "pool/parent/elsewhere@other-snap", inflightModeClone, d.driverInstanceID(), "nonce-mismatch",
		time.Now().Add(-48*time.Hour))

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	require.Len(t, report.RemnantVolumes, 1, "origin is a deletion guard, not a classification gate")
	assert.Empty(t, report.DeletedRemnants)
	require.NotEmpty(t, report.SkippedDeletes)
	assert.Equal(t, "remnant-volume", report.SkippedDeletes[0].Kind)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "origin")
	_, err = client.DatasetGet(ctx, "pool/parent/mismatch-vol")
	require.NoError(t, err, "an origin mismatch is never destroyed")
}

// TestReconcileRemnantOrphanLivePVNotClassified proves a remnant referenced by a
// live PersistentVolume is never classified.
func TestReconcileRemnantOrphanLivePVNotClassified(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("remnant-vol", "csi.scale.io")}, nil)
	mustCreateParentDataset(t, client)

	originSnapID := createRemnantClone(t, client, "tombstone-origin", "remnant-vol", "snap-csi-deleted-1")
	writeRemnantMarker(t, d, "remnant-vol", originSnapID, inflightModeClone, d.driverInstanceID(), "nonce-live-pv",
		time.Now().Add(-48*time.Hour))

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.RemnantVolumes, "a remnant referenced by a live PV is never classified")
	assert.Empty(t, report.DeletedRemnants)
	_, err = client.DatasetGet(ctx, "pool/parent/remnant-vol")
	require.NoError(t, err, "the live-PV remnant dataset survives")
}

// stampRemnantOnDestroyClient models an ownership stamp landing on the remnant
// between classification and deletion. The stale-marker sweep, the classifier,
// and the pre-destroy revalidation each fetch the remnant dataset once, in that
// order; stamping on the third fetch makes classification see an unstamped
// dataset while the guarded destroy sees a stamped one.
type stampRemnantOnDestroyClient struct {
	*truenas.MockClient
	datasetName string
	gets        int
}

func (c *stampRemnantOnDestroyClient) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	if name == c.datasetName {
		c.gets++
		if c.gets >= 3 {
			if err := c.DatasetSetUserProperty(ctx, name, PropDriverInstanceID, "late-instance"); err != nil {
				return nil, err
			}
		}
	}
	return c.MockClient.DatasetGet(ctx, name)
}

// TestReconcileRemnantOrphanStampAppearsPreDestroy proves the pre-destroy
// re-fetch veto: a stamp that appears after classification skips the destroy.
func TestReconcileRemnantOrphanStampAppearsPreDestroy(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)

	originSnapID := createRemnantClone(t, client, "tombstone-origin", "remnant-vol", "snap-csi-deleted-1")
	writeRemnantMarker(t, d, "remnant-vol", originSnapID, inflightModeClone, d.driverInstanceID(), "nonce-race",
		time.Now().Add(-48*time.Hour))

	wrapper := &stampRemnantOnDestroyClient{MockClient: client, datasetName: "pool/parent/remnant-vol"}
	d.truenasClient = wrapper

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	require.Len(t, report.RemnantVolumes, 1, "classification saw the dataset before the stamp landed")
	assert.Empty(t, report.DeletedRemnants)
	require.NotEmpty(t, report.SkippedDeletes)
	assert.Equal(t, "remnant-volume", report.SkippedDeletes[0].Kind)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "stamped")
	_, err = client.DatasetGet(ctx, "pool/parent/remnant-vol")
	require.NoError(t, err, "the dataset survives the vetoed destroy")
}

// TestReconcileRemnantOrphanDeletionCapRespected proves remnant destroys count
// against the shared per-run deletion cap.
func TestReconcileRemnantOrphanDeletionCapRespected(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("live-volume", "csi.scale.io")}, nil)
	mustCreateParentDataset(t, client)
	d.config.Reconcile.Delete.MaxPerRun = 2

	// A live managed volume keeps the safety brake satisfied without consuming the
	// deletion cap.
	addReconcileDataset(client, "live-volume", time.Now().Add(-72*time.Hour), true, 100)

	for _, volumeID := range []string{"remnant-a", "remnant-b", "remnant-c"} {
		origin := createRemnantClone(t, client, "origin-"+volumeID, volumeID, "snap-csi-deleted-1")
		writeRemnantMarker(t, d, volumeID, origin, inflightModeClone, d.driverInstanceID(), "nonce-"+volumeID,
			time.Now().Add(-48*time.Hour))
	}

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	require.Len(t, report.RemnantVolumes, 3)
	assert.Len(t, report.DeletedRemnants, 2, "only maxPerRun remnants are destroyed")

	capSkips := 0
	for _, skipped := range report.SkippedDeletes {
		if skipped.Kind == "remnant-volume" && strings.Contains(skipped.Reason, "deletion cap reached") {
			capSkips++
		}
	}
	assert.Equal(t, 1, capSkips, "the third remnant is skipped for the deletion cap")
}

// TestReconcileRemnantOrphanDualReadBookkeeping proves the remnant phase reads
// markers from the dedicated bookkeeping child dataset (dual-read) when the
// relocation is enabled, and retires the marker from the child on destroy.
func TestReconcileRemnantOrphanDualReadBookkeeping(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)
	enableBookkeeping(d)

	originSnapID := createRemnantClone(t, client, "tombstone-origin", "remnant-vol", "snap-csi-deleted-1")
	writeRemnantMarker(t, d, "remnant-vol", originSnapID, inflightModeClone, d.driverInstanceID(), "nonce-child",
		time.Now().Add(-48*time.Hour))

	// The marker lives on the bookkeeping child, not the parent.
	child, err := client.DatasetGet(ctx, d.bookkeepingDatasetName())
	require.NoError(t, err)
	_, inChild := child.UserProperties[inflightMarkerKey("remnant-vol")]
	require.True(t, inChild, "the relocated marker is written to the bookkeeping child")

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, []string{"remnant-vol"}, report.DeletedRemnants, "the child-side marker is discovered via dual-read")
	_, err = client.DatasetGet(ctx, "pool/parent/remnant-vol")
	require.Error(t, err, "the remnant dataset is destroyed")

	goneMarker, err := d.readInflightMarker(ctx, "remnant-vol")
	require.NoError(t, err)
	assert.Nil(t, goneMarker, "the marker is retired from the child on destroy")
}

// D1 regression (Opus batch-15 finding): the remnant destroy must serialize on
// the per-volume operation lock — a held lock (same-name CreateVolume resume in
// flight) must skip the destroy, dataset untouched.
func TestReconcileRemnantOrphanSkipsWhileVolumeOperationInProgress(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)

	createRemnantClone(t, client, "tombstone-origin", "locked-vol", "snap-csi-deleted-1")
	writeRemnantMarker(t, d, "locked-vol", "pool/parent/tombstone-origin@snap-csi-deleted-1", inflightModeClone, d.driverInstanceID(), "nonce-locked",
		time.Now().Add(-48*time.Hour))

	// Simulate a same-name CreateVolume resume holding the per-volume lock.
	lockKey := "volume:locked-vol"
	require.True(t, d.acquireOperationLock(lockKey))
	defer d.releaseOperationLock(lockKey)

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	require.Len(t, report.RemnantVolumes, 1)
	assert.Empty(t, report.DeletedRemnants, "destroy must skip while the volume operation lock is held")
	require.NotEmpty(t, report.SkippedDeletes)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "in progress")
	_, err = client.DatasetGet(ctx, "pool/parent/locked-vol")
	require.NoError(t, err, "dataset must survive a lock-contended destroy")
}
