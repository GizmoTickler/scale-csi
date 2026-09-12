package driver

import (
	"context"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// newLiveNFSVolumeWithForgedComment provisions the shape the round-seven NFS
// guard could not tell from an orphan: a LIVE CSI volume — dataset present,
// share exporting that dataset's own mountpoint — whose comment names an ABSENT
// sibling under the same parent.
//
// Every live CSI NFS volume on the appliance has exactly this shape apart from
// the comment, and the comment is the one field an operator (or anything that
// can reach the share API) can retype. Grounded against nas01: the parent
// dataset flashstor/scale-csi is mounted at /mnt/flashstor/scale-csi and each
// child inherits, so a CSI volume exports precisely its own dataset mountpoint
// and nothing else.
func newLiveNFSVolumeWithForgedComment(t *testing.T, ctx context.Context, client *truenas.MockClient) *truenas.NFSShare {
	t.Helper()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	live, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/pvc-live", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.Equal(t, "/mnt/pool/parent/pvc-live", live.Mountpoint)

	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path:    live.Mountpoint,
		Comment: "scale-csi (org.scale.csi.test): pool/parent/pvc-gone",
		Enabled: true,
	})
	require.NoError(t, err)
	return share
}

// TestDetectOrphanedNFSSharesRefusesAShareExportingALiveSiblingVolume is the
// regression for the round-seven NFS proof that admitted every live CSI volume.
//
// That guard required only that each exported path sit SOMEWHERE UNDER the
// parent dataset's mountpoint. Every live CSI NFS volume exports such a path, so
// the proof held for all of them: write "scale-csi (<driver>):
// pool/parent/pvc-gone" onto the share of the live volume pvc-live and the
// claimed dataset is absent, no PersistentVolume is bound to the CLAIM's volume
// ID, and the exported path passes the prefix test — so the live volume's export
// is classified and deleted. The commit that introduced it called the exported
// path "a field an operator cannot retarget by typing in a comment box"; it is
// retargetable by writing the comment on a different share inside the parent,
// which is where every CSI volume lives.
//
// The proof must be EQUALITY: the share must export exactly the path the claimed
// dataset resolves to under the parent's real mountpoint.
func TestDetectOrphanedNFSSharesRefusesAShareExportingALiveSiblingVolume(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	newLiveNFSVolumeWithForgedComment(t, ctx, client)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)

	assert.Empty(t, report.OrphanShares,
		"a share exporting a LIVE volume's mountpoint must never be classified because its comment names an absent sibling")

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Contains(t, nfsSharePaths(t, client), "/mnt/pool/parent/pvc-live",
		"the live volume's export must survive a forged comment")
}

// TestDeleteOrphanedNFSShareRefusesAShareExportingALiveSiblingVolume is the same
// defect at the delete-time re-proof, reached directly with an already
// classified orphan. The re-proof reran the identical prefix test, so it caught
// nothing the detector had already waved through: it re-confirmed that a share
// exporting a live volume was "under the parent" and deleted it.
func TestDeleteOrphanedNFSShareRefusesAShareExportingALiveSiblingVolume(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	share := newLiveNFSVolumeWithForgedComment(t, ctx, client)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{OrphanShares: []ReconcileObject{{
		ID:             "pool/parent/pvc-gone",
		BackendID:      strconv.Itoa(share.ID),
		SourceVolumeID: "pvc-gone",
		Protocol:       ShareTypeNFS,
	}}}

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Empty(t, report.DeletedShares, "the live volume's export must not be deleted on a forged claim")
	require.Len(t, report.SkippedDeletes, 1)
	assert.Equal(t, "share", report.SkippedDeletes[0].Kind)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "rather than exactly")
	assert.Contains(t, nfsSharePaths(t, client), "/mnt/pool/parent/pvc-live",
		"the live volume's export must survive the delete-time re-proof too")
}

// newLiveISCSIVolumeWithForgedComment is the iSCSI counterpart: a LIVE volume's
// extent (disk zvol/pool/parent/pvc-live, dataset present) whose comment claims
// an absent sibling. The extent's own disk reference is the authoritative
// backreference — createISCSIShareForDataset writes exactly "zvol/<dataset>" —
// and the classifier ignored it in favor of the comment.
func newLiveISCSIVolumeWithForgedComment(t *testing.T, ctx context.Context, client *truenas.MockClient, d *Driver) *truenas.ISCSIExtent {
	t.Helper()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/pvc-live", Type: "VOLUME"})
	require.NoError(t, err)

	shareName := d.iscsiShareName("pvc-live")
	target, err := client.ISCSITargetCreate(ctx, shareName, "", "ISCSI", nil)
	require.NoError(t, err)
	extent, err := client.ISCSIExtentCreate(ctx, shareName, "zvol/pool/parent/pvc-live",
		"scale-csi: pool/parent/pvc-gone", 512, true, "SSD")
	require.NoError(t, err)
	_, err = client.ISCSITargetExtentCreate(ctx, target.ID, extent.ID, 0)
	require.NoError(t, err)
	return extent
}

// TestDetectOrphanedISCSISharesRefusesAnExtentBackedByALiveSiblingVolume is the
// iSCSI half of the same defect. detectOrphanedISCSIShares called the extent
// comment "the authoritative, non-lossy backreference" and keyed classification
// on it, while extent.Disk — "zvol/<dataset>", written by the driver and stored
// by the appliance — sat unread on the very object about to be force-deleted.
// A comment naming an absent sibling therefore handed a live volume's extent,
// association and target to the sweep.
func TestDetectOrphanedISCSISharesRefusesAnExtentBackedByALiveSiblingVolume(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	newLiveISCSIVolumeWithForgedComment(t, ctx, client, d)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)

	assert.Empty(t, report.OrphanShares,
		"an extent whose disk reference points at a LIVE dataset must never be classified from its comment alone")

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Contains(t, iscsiExtentNames(t, client), d.iscsiShareName("pvc-live"),
		"the live volume's extent must survive a forged comment")
	assert.Contains(t, iscsiTargetNames(t, client), d.iscsiShareName("pvc-live"),
		"the live volume's target must survive a forged comment")
}

// TestDeleteOrphanedISCSIShareRevalidatesTheExtentAtDeleteTime pins the iSCSI
// delete-time re-proof, which did not exist: the deleter resolved the extent by
// the row ID a whole detection pass ago and force-deleted it, its association
// and its target without re-reading either half of the ownership proof. Here the
// classified BackendID now names a live volume's extent.
func TestDeleteOrphanedISCSIShareRevalidatesTheExtentAtDeleteTime(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	extent := newLiveISCSIVolumeWithForgedComment(t, ctx, client, d)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{OrphanShares: []ReconcileObject{{
		ID:             "pool/parent/pvc-gone",
		BackendID:      strconv.Itoa(extent.ID),
		SourceVolumeID: "pvc-gone",
		Protocol:       ShareTypeISCSI,
	}}}

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Empty(t, report.DeletedShares)
	require.Len(t, report.SkippedDeletes, 1)
	assert.Equal(t, "share", report.SkippedDeletes[0].Kind)
	assert.Contains(t, iscsiExtentNames(t, client), d.iscsiShareName("pvc-live"),
		"the live volume's extent must survive a stale classification")
	assert.Contains(t, iscsiTargetNames(t, client), d.iscsiShareName("pvc-live"),
		"the live volume's target must survive a stale classification")
}

// TestOrphanShareSweepRetainsRefusedISCSIOrphanAcrossPasses is the two-pass
// probe for the leak the round-seven sole-occupancy gate introduced.
//
// When the gate refused a target, pass one still deleted the extent. The
// extent's comment is the ONLY handle detectOrphanedISCSIShares rediscovers the
// orphan by, so pass two reported zero orphans AND zero skips, and the
// driver-created target stayed on the appliance forever with nothing in the
// report, the logs or the metrics to show for it. A refusal must retain the
// whole orphan so every later pass re-detects it and re-records the refusal.
func TestOrphanShareSweepRetainsRefusedISCSIOrphanAcrossPasses(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	sharedTarget, err := client.ISCSITargetCreate(ctx, "shared-storage-target", "", "ISCSI", nil)
	require.NoError(t, err)
	orphanExtent, err := client.ISCSIExtentCreate(ctx, d.iscsiShareName("gone-volume"),
		"zvol/pool/parent/gone-volume", "truenas-csi: pool/parent/gone-volume", 512, true, "SSD")
	require.NoError(t, err)
	_, err = client.ISCSITargetExtentCreate(ctx, sharedTarget.ID, orphanExtent.ID, 0)
	require.NoError(t, err)
	foreignExtent, err := client.ISCSIExtentCreate(ctx, "vmware-datastore-01",
		"zvol/tank/vmware/datastore-01", "hand-built VMware LUN", 512, true, "SSD")
	require.NoError(t, err)
	_, err = client.ISCSITargetExtentCreate(ctx, sharedTarget.ID, foreignExtent.ID, 1)
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}

	for pass := 1; pass <= 2; pass++ {
		report := ReconcileReport{}
		d.detectOrphanedShares(ctx, kubeState, &report)
		require.Lenf(t, report.OrphanShares, 1,
			"pass %d must still discover the refused orphan: retaining the extent is what keeps it discoverable", pass)

		d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

		assert.Emptyf(t, report.DeletedShares, "pass %d must not report a refused orphan as deleted", pass)
		require.Lenf(t, report.SkippedDeletes, 1, "pass %d must record the refusal", pass)
		assert.Equal(t, "iscsi_target", report.SkippedDeletes[0].Kind)
		assert.Containsf(t, iscsiExtentNames(t, client), d.iscsiShareName("gone-volume"),
			"pass %d must retain the orphan's extent", pass)
		assert.Containsf(t, iscsiTargetNames(t, client), "shared-storage-target",
			"pass %d must retain the foreign target", pass)
	}
}

// iscsiOccupancyErrorMock fails only the sole-occupancy lookup. Finding the
// association (and therefore the target) still works, so the sweep reaches the
// gate's transient-error arm rather than never seeing a target at all.
type iscsiOccupancyErrorMock struct {
	*truenas.MockClient
}

func (m *iscsiOccupancyErrorMock) ISCSITargetExtentFindByTarget(_ context.Context, _ int) ([]*truenas.ISCSITargetExtent, error) {
	return nil, assert.AnError
}

// TestOrphanShareSweepRetainsISCSIOrphanWhenOccupancyIsUnknown covers the
// transient arm of the same leak. When the occupancy lookup failed the code
// recorded an object failure, then carried on and deleted the extent anyway AND
// appended the orphan to DeletedShares — so one object was reported as both
// failed and deleted, while the target it refused to touch leaked with its
// discovery handle destroyed. An unproven occupancy must retain everything.
func TestOrphanShareSweepRetainsISCSIOrphanWhenOccupancyIsUnknown(t *testing.T) {
	ctx := context.Background()
	base := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(base)
	d.truenasClient = &iscsiOccupancyErrorMock{MockClient: base}

	createISCSIShareFixture(t, ctx, base, d, "gone-volume", "pool/parent/gone-volume")

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)

	failuresBefore := testutil.ToFloat64(reconcileFailuresTotal.WithLabelValues("share"))
	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Empty(t, report.DeletedShares,
		"an orphan whose target occupancy is unknown must not be reported as deleted as well as failed")
	require.Len(t, report.SkippedDeletes, 1)
	assert.Equal(t, "iscsi_target", report.SkippedDeletes[0].Kind)
	assert.Equal(t, failuresBefore+1, testutil.ToFloat64(reconcileFailuresTotal.WithLabelValues("share")),
		"the transient lookup failure must still be counted once")
	assert.Contains(t, iscsiExtentNames(t, base), d.iscsiShareName("gone-volume"),
		"the extent must be retained so a later pass can retry the occupancy proof")
}

// nvmeoFNamespaceDevicePaths returns the device path of every namespace still on
// a subsystem, so a test can assert survival without depending on IDs.
func nvmeoFNamespaceDevicePaths(t *testing.T, client *truenas.MockClient, subsysID int) []string {
	t.Helper()
	namespaces, err := client.NVMeoFNamespaceListBySubsystem(context.Background(), subsysID)
	require.NoError(t, err)
	paths := make([]string, 0, len(namespaces))
	for _, namespace := range namespaces {
		paths = append(paths, namespace.DevicePath)
	}
	return paths
}

// TestDeleteOrphanedNVMeoFShareRefusesASubsystemCarryingAnUnownedNamespace is the
// regression for the protocol that carries 48 of the 51 live volumes.
//
// deleteOrphanedNVMeoFShare deleted EVERY namespace on the subsystem and then
// the subsystem itself, with no ownership or sole-occupancy check at all —
// exactly the defect round seven called a blocker for iSCSI. Detection only ever
// proves the ONE namespace whose DevicePath backreferences the orphan's dataset;
// a live sibling volume or a hand-built non-CSI namespace on the same subsystem
// was destroyed on no evidence whatsoever. That the live appliance happens to
// run one namespace per subsystem today is a property of the current topology,
// not a guarantee of the design.
func TestDeleteOrphanedNVMeoFShareRefusesASubsystemCarryingAnUnownedNamespace(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	// A live CSI sibling: dataset present.
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/pvc-live", Type: "VOLUME"})
	require.NoError(t, err)

	subsys, err := client.NVMeoFSubsystemCreate(ctx, d.nvmeSubsystemName("pool/parent/gone-volume"), true, nil)
	require.NoError(t, err)
	_, err = client.NVMeoFNamespaceCreate(ctx, subsys.ID, "zvol/pool/parent/gone-volume", "ZVOL")
	require.NoError(t, err)
	_, err = client.NVMeoFNamespaceCreate(ctx, subsys.ID, "zvol/pool/parent/pvc-live", "ZVOL")
	require.NoError(t, err)
	_, err = client.NVMeoFNamespaceCreate(ctx, subsys.ID, "zvol/tank/vmware/lun1", "ZVOL")
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)
	require.Equal(t, "pool/parent/gone-volume", report.OrphanShares[0].ID)

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Empty(t, report.DeletedShares, "a subsystem carrying unproven namespaces must not be reported as swept")
	require.Len(t, report.SkippedDeletes, 1)
	assert.Equal(t, "nvmeof_subsystem", report.SkippedDeletes[0].Kind)

	remaining := nvmeoFNamespaceDevicePaths(t, client, subsys.ID)
	assert.Contains(t, remaining, "zvol/pool/parent/pvc-live",
		"a live sibling volume's namespace must never be deleted by another volume's orphan sweep")
	assert.Contains(t, remaining, "zvol/tank/vmware/lun1",
		"a non-CSI namespace on the same subsystem must never be deleted")
	assert.Contains(t, remaining, "zvol/pool/parent/gone-volume",
		"the orphan's own namespace must be retained too: it is the handle the classifier rediscovers it by")

	survivor, err := client.NVMeoFSubsystemGet(ctx, subsys.ID)
	require.NoError(t, err)
	assert.NotNil(t, survivor, "the subsystem must be retained rather than force-deleted on no proof")
}

// TestDeleteOrphanedNVMeoFShareRefusesASubsystemItCanNoLongerProveItOwns covers
// the ownership half of the same gate. Between detection and mutation the
// namespace that proved the subsystem was ours disappears, and nvmeof.nameSuffix
// drifts so the derived name no longer matches either. Nothing then connects the
// recorded row ID to this driver — appliance row IDs are reusable — so the
// subsystem must be retained, not force-deleted.
func TestDeleteOrphanedNVMeoFShareRefusesASubsystemItCanNoLongerProveItOwns(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	subsys, err := client.NVMeoFSubsystemCreate(ctx, d.nvmeSubsystemName("pool/parent/gone-volume"), true, nil)
	require.NoError(t, err)
	namespace, err := client.NVMeoFNamespaceCreate(ctx, subsys.ID, "zvol/pool/parent/gone-volume", "ZVOL")
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)

	require.NoError(t, client.NVMeoFNamespaceDelete(ctx, namespace.ID))
	d.config.NVMeoF.NameSuffix = "-rev2"

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Empty(t, report.DeletedShares)
	require.Len(t, report.SkippedDeletes, 1)
	assert.Equal(t, "nvmeof_subsystem", report.SkippedDeletes[0].Kind)

	survivor, err := client.NVMeoFSubsystemGet(ctx, subsys.ID)
	require.NoError(t, err)
	assert.NotNil(t, survivor, "an unprovable subsystem must be retained, not force-deleted on a reusable row ID")
}
