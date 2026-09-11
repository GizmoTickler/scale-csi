package driver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// nfsSharePaths returns the exported Path of every NFS share in the mock.
func nfsSharePaths(t *testing.T, client *truenas.MockClient) []string {
	t.Helper()
	shares, err := client.NFSShareList(context.Background())
	require.NoError(t, err)
	paths := make([]string, 0, len(shares))
	for _, share := range shares {
		paths = append(paths, share.Path)
	}
	return paths
}

// TestDetectOrphanedNFSSharesValidatesTheExportedPathNotTheComment proves the
// NFS scoping guard validates the OBJECT being deleted rather than the claim
// written on it.
//
// The guard applied datasetUnderParent to a dataset name parsed out of
// share.Comment — "attacker- and operator-writable free text", by the code's
// own description — and then deleted by share.ID. So anyone who could set a
// share comment could point the sweep at any share on the appliance: write
// "scale-csi (<driver>): pool/parent/<anything-absent>" on a share exporting
// /mnt/tank/finance and the next of the four daily passes deleted it.
func TestDetectOrphanedNFSSharesValidatesTheExportedPathNotTheComment(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	// A share for somebody else's data, carrying a forged CSI comment whose
	// claimed dataset sits under this driver's parent and does not exist.
	_, err = client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path:    "/mnt/tank/finance",
		Comment: "scale-csi (org.scale.csi.test): pool/parent/not-a-real-volume",
		Enabled: true,
	})
	require.NoError(t, err)
	// A genuine orphan, to prove the guard is not a blanket disable.
	_, err = client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path:    "/mnt/pool/parent/gone-volume",
		Comment: "scale-csi (org.scale.csi.test): pool/parent/gone-volume",
		Enabled: true,
	})
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)

	require.Len(t, report.OrphanShares, 1, "only the share whose EXPORTED PATH is under the parent may be classified")
	assert.Equal(t, "pool/parent/gone-volume", report.OrphanShares[0].ID)

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Contains(t, nfsSharePaths(t, client), "/mnt/tank/finance",
		"a share outside the parent dataset must survive however its comment is written")
	assert.NotContains(t, nfsSharePaths(t, client), "/mnt/pool/parent/gone-volume",
		"the genuine orphan must still be swept")
}

// TestDetectOrphanedNFSSharesDerivesTheParentMountpointFromTheAppliance proves
// the path check reads the parent dataset's REAL mountpoint instead of assuming
// the conventional /mnt/<dataset> string. Here the parent is mounted at
// /exports/k8s, so:
//
//   - a share exported from /exports/k8s IS in scope (an assumed /mnt/pool/parent
//     prefix would have refused it and leaked the orphan forever), and
//   - a share exported from the conventional-looking /mnt/pool/parent/... is NOT
//     (an assumed prefix would have swept somebody else's share).
func TestDetectOrphanedNFSSharesDerivesTheParentMountpointFromTheAppliance(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	parent, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.Equal(t, "/mnt/pool/parent", parent.Mountpoint)
	client.Datasets["pool/parent"].Mountpoint = "/exports/k8s"

	_, err = client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path:    "/exports/k8s/gone-volume",
		Comment: "scale-csi (org.scale.csi.test): pool/parent/gone-volume",
		Enabled: true,
	})
	require.NoError(t, err)
	// Exported from the conventional path the parent is NOT actually mounted at.
	_, err = client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path:    "/mnt/pool/parent/decoy-volume",
		Comment: "scale-csi (org.scale.csi.test): pool/parent/decoy-volume",
		Enabled: true,
	})
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)

	require.Len(t, report.OrphanShares, 1)
	assert.Equal(t, "pool/parent/gone-volume", report.OrphanShares[0].ID)

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Contains(t, nfsSharePaths(t, client), "/mnt/pool/parent/decoy-volume",
		"a share outside the parent's real mountpoint must survive")
	assert.NotContains(t, nfsSharePaths(t, client), "/exports/k8s/gone-volume",
		"the orphan under the parent's real mountpoint must be swept")
}

// TestDetectOrphanedNFSSharesRefusesMultiPathExportLeavingTheParent proves the
// path check is "every exported path", not "any". NFSShareDelete removes the
// whole share, so a multi-path export that publishes one CSI volume alongside
// /mnt/tank/finance takes the finance export down with it.
func TestDetectOrphanedNFSSharesRefusesMultiPathExportLeavingTheParent(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path:    "/mnt/pool/parent/gone-volume",
		Comment: "scale-csi (org.scale.csi.test): pool/parent/gone-volume",
		Enabled: true,
	})
	require.NoError(t, err)
	client.NFSShares[share.ID].Paths = []string{"/mnt/pool/parent/gone-volume", "/mnt/tank/finance"}

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)

	assert.Empty(t, report.OrphanShares,
		"a share that also exports a path outside the parent must never be classified: deleting it by ID takes that path down too")

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)
	remaining, err := client.NFSShareList(ctx)
	require.NoError(t, err)
	assert.Len(t, remaining, 1, "the multi-path share must survive")
}

// TestDeleteOrphanedNFSShareRevalidatesTheShareAtDeleteTime proves the guarded
// delete phase re-runs the scoping proof against the object as it exists at
// mutation time, instead of deleting an ID a detection pass classified earlier.
// Here the share carrying the classified ID has been retargeted at
// /mnt/tank/finance between detection and delete; the sweep must refuse it and
// record a skip.
func TestDeleteOrphanedNFSShareRevalidatesTheShareAtDeleteTime(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path:    "/mnt/pool/parent/gone-volume",
		Comment: "scale-csi (org.scale.csi.test): pool/parent/gone-volume",
		Enabled: true,
	})
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)

	// The object behind the classified ID changes before the delete phase runs.
	client.NFSShares[share.ID].Path = "/mnt/tank/finance"

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Empty(t, report.DeletedShares, "a retargeted share must not be deleted on a stale classification")
	require.Len(t, report.SkippedDeletes, 1)
	assert.Equal(t, "share", report.SkippedDeletes[0].Kind)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "not under the parent dataset mountpoint")
	assert.Contains(t, nfsSharePaths(t, client), "/mnt/tank/finance", "the retargeted share must survive")
}
