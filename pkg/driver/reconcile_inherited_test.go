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

// addNestedForeignDataset models a user/operator-created child dataset nested
// INSIDE a CSI-managed volume (e.g. `zfs create pool/parent/<vol>/nested`). ZFS
// user properties inherit to descendants, so the child presents the parent's
// managed_resource=true and csi_volume_name stamps with a NON-local source (the
// origin name, never "local"). The reconcile listing strips source, so only a
// source-bearing re-fetch (DatasetGet) can distinguish it from a managed volume.
func addNestedForeignDataset(client *truenas.MockClient, parentVolumeID, childName string, createdAt time.Time) *truenas.Dataset {
	inheritedFrom := "pool/parent/" + parentVolumeID + "@inherit"
	dataset := &truenas.Dataset{
		ID:       "pool/parent/" + parentVolumeID + "/" + childName,
		Name:     "pool/parent/" + parentVolumeID + "/" + childName,
		Type:     "FILESYSTEM",
		Creation: truenas.DatasetProperty{Parsed: float64(createdAt.Unix())},
		Used:     truenas.DatasetProperty{Parsed: float64(1000)},
		UserProperties: map[string]truenas.UserProperty{
			PropManagedResource: {Value: "true", Source: inheritedFrom},
			PropCSIVolumeName:   {Value: parentVolumeID, Source: inheritedFrom},
		},
	}
	client.Datasets[dataset.Name] = dataset
	return dataset
}

// A nested user dataset under a LIVE volume inherits managed_resource=true but
// was never created by the driver. It must not be classified as a CSI orphan.
func TestReconcileOrphansIgnoresInheritedManagedResource(t *testing.T) {
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("live-volume", "csi.scale.io")}, nil,
	)
	old := time.Now().Add(-48 * time.Hour)
	addReconcileDataset(client, "live-volume", old, true, 100)
	addNestedForeignDataset(client, "live-volume", "nested", old)

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.OrphanVolumes, "an inherited managed_resource must not classify a nested dataset as an orphan")
	assert.Empty(t, report.DeletedVolumes)
}

// Under delete mode a nested inherited dataset must fail revalidation (source !=
// local), never reach DeleteVolume, and leave the real dataset untouched — while
// a genuine orphan alongside it is still deleted.
func TestReconcileOrphansInheritedManagedResourceNotRevalidatedOrDeleted(t *testing.T) {
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("anchor-volume", "csi.scale.io")}, nil,
	)
	d.config.Reconcile.Delete.MaxPerRun = 5
	old := time.Now().Add(-48 * time.Hour)
	addReconcileDataset(client, "anchor-volume", old, true, 50) // live PV keeps the zero-PV safety brake open
	addReconcileDataset(client, "old-volume", old, true, 100)   // genuine orphan (no PV)
	addNestedForeignDataset(client, "old-volume", "nested", old)

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{
		Delete: true, MinOrphanAge: time.Hour,
	})
	require.NoError(t, err)

	assert.Contains(t, report.DeletedVolumes, "old-volume", "the genuine orphan must still be deleted")
	assert.NotContains(t, report.DeletedVolumes, "nested", "the inherited dataset must not be reported deleted")
	for _, orphan := range report.OrphanVolumes {
		assert.NotEqual(t, "nested", orphan.ID, "the inherited dataset must not be classified")
	}

	_, getErr := client.DatasetGet(context.Background(), "pool/parent/old-volume/nested")
	assert.NoError(t, getErr, "the real nested dataset must survive reconcile")
	for _, call := range client.DatasetDeleteCalls {
		assert.NotEqual(t, "pool/parent/old-volume/nested", call.Name,
			"DeleteVolume must never operate on the inherited dataset path")
	}
}

// Phantom orphans used to consume a maxPerRun slot every pass. Here two phantoms
// nest under a genuine orphan (so their inherited csi_volume_name points at a
// gone PV and they pass the hard recheck pre-fix) and sort before a second
// genuine orphan. With a cap of 2, the phantoms burn both slots pre-fix and the
// second genuine orphan is never deleted; post-fix the phantoms are never
// classified, so the genuine orphan claims a slot.
func TestReconcileOrphansInheritedManagedResourceDoesNotConsumeDeleteSlot(t *testing.T) {
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("live-volume", "csi.scale.io")}, nil,
	)
	d.config.Reconcile.Delete.MaxPerRun = 2
	old := time.Now().Add(-48 * time.Hour)
	addReconcileDataset(client, "live-volume", old, true, 50) // live PV keeps the zero-PV safety brake open
	addReconcileDataset(client, "aaa-gone", old, true, 100)   // genuine orphan parent (no PV)
	addNestedForeignDataset(client, "aaa-gone", "nested-1", old)
	addNestedForeignDataset(client, "aaa-gone", "nested-2", old)
	addReconcileDataset(client, "zzz-orphan", old, true, 100) // genuine orphan (no PV)

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{
		Delete: true, MinOrphanAge: time.Hour,
	})
	require.NoError(t, err)

	assert.Contains(t, report.DeletedVolumes, "zzz-orphan",
		"the genuine orphan must claim a slot the phantoms no longer consume")
	assert.NotContains(t, report.DeletedVolumes, "nested-1")
	assert.NotContains(t, report.DeletedVolumes, "nested-2")

	_, getErr := client.DatasetGet(context.Background(), "pool/parent/aaa-gone/nested-1")
	assert.NoError(t, getErr, "the real nested dataset must survive reconcile")
}
