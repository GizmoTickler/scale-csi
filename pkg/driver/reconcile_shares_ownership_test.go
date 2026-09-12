package driver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// iscsiTargetNames returns the names of every iSCSI target currently in the
// mock, so a test can assert survival without depending on IDs.
func iscsiTargetNames(t *testing.T, client *truenas.MockClient) []string {
	t.Helper()
	targets, err := client.ISCSITargetList(context.Background())
	require.NoError(t, err)
	names := make([]string, 0, len(targets))
	for _, target := range targets {
		names = append(names, target.Name)
	}
	return names
}

// iscsiExtentNames returns the names of every iSCSI extent currently in the mock.
func iscsiExtentNames(t *testing.T, client *truenas.MockClient) []string {
	t.Helper()
	extents, err := client.ISCSIExtentList(context.Background())
	require.NoError(t, err)
	names := make([]string, 0, len(extents))
	for _, extent := range extents {
		names = append(names, extent.Name)
	}
	return names
}

// TestOrphanShareSweepRefusesToDeleteSharedISCSITarget proves the sweep will not
// destroy a target it did not create.
//
// Since 5a5843c the deleter reaches the target through the target-extent
// ASSOCIATION instead of ISCSITargetFindByName(shareName), with no name or
// ownership check and force=true. One orphaned CSI extent parked on an
// operator's shared target therefore destroyed that target and took every live
// extent on it offline. Here the orphaned extent's association points at a
// target named "shared-storage-target" that also carries a live, non-CSI
// extent: the target and the foreign extent must survive.
//
// Round 8: the refusal now retains the orphan's OWN extent and association too.
// Sweeping them while retaining the target destroyed the extent comment, which
// is the only handle detectOrphanedISCSIShares rediscovers the orphan by, so the
// refused target leaked permanently and invisibly. See
// TestOrphanShareSweepRetainsRefusedISCSIOrphanAcrossPasses.
func TestOrphanShareSweepRefusesToDeleteSharedISCSITarget(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	sharedTarget, err := client.ISCSITargetCreate(ctx, "shared-storage-target", "", "ISCSI", nil)
	require.NoError(t, err)
	// The orphan: a CSI extent (authoritative comment backreference) whose
	// dataset is gone, associated with the operator's shared target.
	orphanExtent, err := client.ISCSIExtentCreate(ctx, d.iscsiShareName("gone-volume"),
		"zvol/pool/parent/gone-volume", "truenas-csi: pool/parent/gone-volume", 512, true, "SSD")
	require.NoError(t, err)
	orphanAssoc, err := client.ISCSITargetExtentCreate(ctx, sharedTarget.ID, orphanExtent.ID, 0)
	require.NoError(t, err)
	// A live extent belonging to somebody else, on the same target.
	foreignExtent, err := client.ISCSIExtentCreate(ctx, "vmware-datastore-01",
		"zvol/tank/vmware/datastore-01", "hand-built VMware LUN", 512, true, "SSD")
	require.NoError(t, err)
	_, err = client.ISCSITargetExtentCreate(ctx, sharedTarget.ID, foreignExtent.ID, 1)
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Contains(t, iscsiTargetNames(t, client), "shared-storage-target",
		"a target this driver never created must survive the sweep of one orphaned extent on it")
	assert.Contains(t, iscsiExtentNames(t, client), "vmware-datastore-01",
		"a live foreign extent on the shared target must survive")
	assert.Contains(t, iscsiExtentNames(t, client), d.iscsiShareName("gone-volume"),
		"the orphan's own extent must be RETAINED: it is the only handle the classifier rediscovers this orphan by")

	remainingAssoc, err := client.ISCSITargetExtentGet(ctx, orphanAssoc.ID)
	require.NoError(t, err)
	assert.NotNil(t, remainingAssoc, "the orphan's own target-extent association must be retained with it")
	assert.Empty(t, report.DeletedShares, "a retained orphan must not be reported as deleted")

	require.Len(t, report.SkippedDeletes, 1)
	assert.Equal(t, "iscsi_target", report.SkippedDeletes[0].Kind)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "did not create",
		"the refusal must be recorded in the report, not swallowed")
}

// TestOrphanShareSweepRefusesToDeleteMultiLUNISCSITarget proves the name check
// alone is not enough. This target IS named exactly what the driver would name
// it for the orphaned volume, so a pure target.Name == shareName gate passes —
// but an operator has since hung a second, live extent off it. Force-deleting
// the target would take that LUN offline, so the target must be retained even
// though its name matches.
func TestOrphanShareSweepRefusesToDeleteMultiLUNISCSITarget(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	shareName := d.iscsiShareName("gone-volume")
	target, err := client.ISCSITargetCreate(ctx, shareName, "", "ISCSI", nil)
	require.NoError(t, err)
	orphanExtent, err := client.ISCSIExtentCreate(ctx, shareName,
		"zvol/pool/parent/gone-volume", "truenas-csi: pool/parent/gone-volume", 512, true, "SSD")
	require.NoError(t, err)
	_, err = client.ISCSITargetExtentCreate(ctx, target.ID, orphanExtent.ID, 0)
	require.NoError(t, err)
	// A second LUN an operator added to the driver's target by hand.
	extraExtent, err := client.ISCSIExtentCreate(ctx, "operator-added-lun",
		"zvol/tank/manual/lun1", "added by hand", 512, true, "SSD")
	require.NoError(t, err)
	_, err = client.ISCSITargetExtentCreate(ctx, target.ID, extraExtent.ID, 1)
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Contains(t, iscsiTargetNames(t, client), shareName,
		"a multi-LUN target must survive even when its name matches this driver's naming")
	assert.Contains(t, iscsiExtentNames(t, client), "operator-added-lun",
		"the operator's second LUN must survive")
	assert.Contains(t, iscsiExtentNames(t, client), shareName,
		"the orphan's own extent must be RETAINED so a later pass can rediscover and retry it")
	assert.Empty(t, report.DeletedShares, "a retained orphan must not be reported as deleted")

	require.Len(t, report.SkippedDeletes, 1)
	assert.Equal(t, "iscsi_target", report.SkippedDeletes[0].Kind)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "still carries extent")
}
