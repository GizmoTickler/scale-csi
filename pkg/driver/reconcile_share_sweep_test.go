package driver

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// TestOrphanShareSweepISCSIDeletesFencingInitiatorGroup proves the iSCSI sweep
// removes the same object set as canonical teardown: target, extent, association
// AND the per-volume fencing initiator group. Pre-fix the sweep left one initiator
// group behind per swept volume.
func TestOrphanShareSweepISCSIDeletesFencingInitiatorGroup(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)
	d.config.Fencing.Mode = FencingModeStrict

	// Orphaned iSCSI share (dataset gone) with its per-volume fencing initiator
	// group, identified by the ownership comment canonical teardown matches on.
	createISCSIShareFixture(t, ctx, client, d, "gone-volume", "pool/parent/gone-volume")
	orphanGroup, err := client.ISCSIInitiatorCreate(ctx, "scale-csi fencing: pool/parent/gone-volume")
	require.NoError(t, err)
	// A decoy group owned by another volume must survive the sweep.
	decoyGroup, err := client.ISCSIInitiatorCreate(ctx, "scale-csi fencing: pool/parent/other-volume")
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)
	require.Len(t, report.DeletedShares, 1)

	goneTarget, err := client.ISCSITargetFindByName(ctx, d.iscsiShareName("gone-volume"))
	require.NoError(t, err)
	assert.Nil(t, goneTarget, "the orphaned target must be deleted")

	groupIDs := iscsiInitiatorGroupIDs(t, client)
	assert.NotContains(t, groupIDs, orphanGroup.ID, "the orphaned volume's initiator group must be deleted")
	assert.Contains(t, groupIDs, decoyGroup.ID, "an unrelated initiator group must survive")
}

// iscsiInitiatorGroupIDs returns the set of initiator group IDs currently in the
// mock. The mock pre-seeds an "allow-all" group, so tests assert membership rather
// than exact counts.
func iscsiInitiatorGroupIDs(t *testing.T, client *truenas.MockClient) []int {
	t.Helper()
	groups, err := client.ISCSIInitiatorList(context.Background())
	require.NoError(t, err)
	ids := make([]int, 0, len(groups))
	for _, group := range groups {
		ids = append(ids, group.ID)
	}
	return ids
}

// TestOrphanShareSweepISCSIInitiatorGroupGatedOnFencing proves the sweep only
// reaches for the initiator group when fencing is enabled (matching canonical
// teardown), so a fencing-off deployment pays no extra lookup.
func TestOrphanShareSweepISCSIInitiatorGroupGatedOnFencing(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client) // fencing off by default

	createISCSIShareFixture(t, ctx, client, d, "gone-volume", "pool/parent/gone-volume")
	// Even if a group with the ownership comment exists, fencing-off must not
	// touch it (canonical teardown leaves it too).
	group, err := client.ISCSIInitiatorCreate(ctx, "scale-csi fencing: pool/parent/gone-volume")
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)
	require.Len(t, report.DeletedShares, 1)

	groupIDs := iscsiInitiatorGroupIDs(t, client)
	assert.Contains(t, groupIDs, group.ID, "fencing-off sweep must leave the initiator group untouched")
}

// nvmeSweepAssociationMock returns a configured port-subsystem association table
// and records deletion calls, since the base mock's association methods are
// stateless stubs.
type nvmeSweepAssociationMock struct {
	*truenas.MockClient
	associations       []*truenas.NVMeoFPortSubsys
	associationDeletes []int
}

func (m *nvmeSweepAssociationMock) NVMeoFPortSubsysList(ctx context.Context) ([]*truenas.NVMeoFPortSubsys, error) {
	return m.associations, nil
}

func (m *nvmeSweepAssociationMock) NVMeoFPortSubsysDelete(ctx context.Context, id int) error {
	m.associationDeletes = append(m.associationDeletes, id)
	return m.MockClient.NVMeoFPortSubsysDelete(ctx, id)
}

// TestOrphanShareSweepNVMeoFDeletesPortSubsystemAssociation proves the NVMe-oF
// sweep removes the port-subsystem association before the subsystem, matching
// canonical teardown. Pre-fix the sweep skipped the association, so the subsystem
// delete could fail every pass or dangle.
func TestOrphanShareSweepNVMeoFDeletesPortSubsystemAssociation(t *testing.T) {
	ctx := context.Background()
	base := truenas.NewMockClient()
	mock := &nvmeSweepAssociationMock{MockClient: base}
	d := newOrphanShareSweepDriver(base)
	d.truenasClient = mock

	// Orphaned NVMe-oF share (dataset gone).
	goneSubsys, err := base.NVMeoFSubsystemCreate(ctx, d.nvmeSubsystemName("pool/parent/gone-volume"), true, nil)
	require.NoError(t, err)
	_, err = base.NVMeoFNamespaceCreate(ctx, goneSubsys.ID, "zvol/pool/parent/gone-volume", "ZVOL")
	require.NoError(t, err)
	// A live subsystem's association must NOT be deleted.
	liveSubsys, err := base.NVMeoFSubsystemCreate(ctx, d.nvmeSubsystemName("pool/parent/live-volume"), true, nil)
	require.NoError(t, err)
	mock.associations = []*truenas.NVMeoFPortSubsys{
		{ID: 11, PortID: 1, SubsysID: goneSubsys.ID},
		{ID: 12, PortID: 1, SubsysID: liveSubsys.ID},
	}

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)
	assert.Equal(t, "pool/parent/gone-volume", report.OrphanShares[0].ID)

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)
	require.Len(t, report.DeletedShares, 1)

	assert.Contains(t, mock.associationDeletes, 11, "the orphaned subsystem's port association must be deleted")
	assert.NotContains(t, mock.associationDeletes, 12, "a live subsystem's port association must survive")

	remaining, err := base.NVMeoFSubsystemFindByName(ctx, d.nvmeSubsystemName("pool/parent/gone-volume"))
	require.NoError(t, err)
	assert.Nil(t, remaining, "the orphaned subsystem must be deleted")
}

// TestDeleteOrphanedSharesSharesDeletionCapWithOtherOrphanKinds is the
// regression test for the first half of C8: deleteOrphanedShares used to
// police its own independent len(report.DeletedShares) counter against
// maxPerRun instead of the SAME deletedCount every other orphan kind
// (snapshots, volumes, tombstones, spent-restores, remnants) shares in
// deleteDetectedOrphans, so a pass could destroy up to 2x maxPerRun objects.
// Here deletedCount is passed in already AT the cap (as if
// deleteDetectedOrphans already spent the whole budget elsewhere this pass),
// so deleteOrphanedShares must skip every share and record a cap skip for
// each — not silently delete up to maxPerRun MORE.
func TestDeleteOrphanedSharesSharesDeletionCapWithOtherOrphanKinds(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
		},
		truenasClient: client,
	}
	_, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: "/mnt/pool/parent/gone-volume", Comment: "truenas-csi (org.scale.csi.nfs): pool/parent/gone-volume", Enabled: true,
	})
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)

	// deletedCount=2, maxPerRun=2: the budget is already fully spent by other
	// orphan kinds before deleteOrphanedShares ever runs.
	d.deleteOrphanedShares(ctx, &report, kubeState, 2, 2)

	assert.Empty(t, report.DeletedShares, "the shared per-run cap must block every share once other kinds already spent it")
	require.Len(t, report.SkippedDeletes, 1)
	assert.Equal(t, "share", report.SkippedDeletes[0].Kind)
	assert.Contains(t, report.SkippedDeletes[0].Reason, deletionCapReasonPrefix,
		"a cap-blocked share skip must be recorded with the shared cap-skip reason prefix (feeds CapSkippedDeletes/scale_csi_tombstone_reap_last_skipped_on_cap)")

	shares, err := client.NFSShareList(ctx)
	require.NoError(t, err)
	assert.Len(t, shares, 1, "the orphaned share must survive when the shared cap is already spent")
}

// TestDeleteOrphanedNFSShareRecordsFailureOnMalformedBackendID is the
// regression test for the second half of C8: a malformed BackendID used to
// return silently — no log, no skip, no object failure — while the iSCSI and
// NVMe-oF siblings record every failure through recordReconcileObjectFailure.
func TestDeleteOrphanedNFSShareRecordsFailureOnMalformedBackendID(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
		},
		truenasClient: client,
	}
	report := ReconcileReport{}
	before := testutil.ToFloat64(reconcileFailuresTotal.WithLabelValues("share"))

	d.deleteOrphanedNFSShare(ctx, &report, ReconcileObject{ID: "pool/parent/malformed", BackendID: "not-a-number"})

	assert.Empty(t, report.DeletedShares, "a malformed BackendID must not be treated as a successful delete")
	assert.Equal(t, before+1, testutil.ToFloat64(reconcileFailuresTotal.WithLabelValues("share")),
		"a malformed BackendID must be recorded as a failure like every other NFS/iSCSI/NVMe-oF share cleanup error")
}
