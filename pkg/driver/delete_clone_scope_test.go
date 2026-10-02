package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// Batch 4.1: DeleteVolume's dependent-clone check is scoped to exactly the
// volume's own snapshots. These tests pin both directions of that scope: a
// clone of one of the volume's own snapshots refuses the delete before the
// share is touched (wherever the clone lives), and nothing else — a clone of a
// sibling volume's snapshot, even one whose name extends this volume's name —
// vetoes it.

func newCloneScopeDriver(client truenas.ClientInterface, destroyForeign bool) *Driver {
	return &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName:               "pool/parent",
				DestroyForeignSnapshotsOnDelete: destroyForeign,
			},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "1.2.3.4"},
		},
		truenasClient: client,
	}
}

func addCloneScopeVolume(t *testing.T, client *controllerCallCountingMock, name string) *truenas.NFSShare {
	t.Helper()
	ctx := context.Background()
	ds, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/" + name, Type: "FILESYSTEM"})
	require.NoError(t, err)
	ds.Mountpoint = "/mnt/pool/parent/" + name
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: ds.Mountpoint})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, ds.Name, PropNFSShareID, fmt.Sprint(share.ID)))
	return share
}

func addCloneOf(t *testing.T, client *controllerCallCountingMock, cloneName, origin string) {
	t.Helper()
	clone, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{Name: cloneName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	client.Datasets[clone.Name].Origin = truenas.DatasetProperty{Value: origin, Parsed: origin, Rawvalue: origin}
}

func TestDeleteVolumeWithoutOwnSnapshotsSkipsTheOriginScan(t *testing.T) {
	client := newControllerCallCountingMock()
	addCloneScopeVolume(t, client, "vol-scope")
	// A sibling whose name extends this volume's, with a live clone of one of
	// its snapshots (an hourly backup clone, say). It is not this volume's
	// dependency and must neither be queried for nor veto the delete.
	addCloneScopeVolume(t, client, "vol-scope-2")
	_, err := client.SnapshotCreate(context.Background(), "pool/parent/vol-scope-2", "backup", nil)
	require.NoError(t, err)
	addCloneOf(t, client, "pool/parent/backup-clone", "pool/parent/vol-scope-2@backup")

	_, err = newCloneScopeDriver(client, false).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-scope"})
	require.NoError(t, err)
	assert.Equal(t, 0, client.dependentCloneQueries,
		"a volume with no snapshots of its own cannot have a dependent clone; the parent-wide origin scan must not run")
	_, getErr := client.MockClient.DatasetGet(context.Background(), "pool/parent/vol-scope")
	assert.True(t, truenas.IsNotFoundError(getErr), "the volume must be deleted")
}

func TestDeleteVolumeOwnSnapshotCloneOutsideParentRefusesBeforeShareDeletion(t *testing.T) {
	client := newControllerCallCountingMock()
	share := addCloneScopeVolume(t, client, "vol-own")
	_, err := client.SnapshotCreate(context.Background(), "pool/parent/vol-own", "backup", nil)
	require.NoError(t, err)
	// The clone lives outside the CSI parent (live: clones of one dataset's
	// snapshots in another parent are normal on TrueNAS, e.g. boot environments).
	addCloneOf(t, client, "pool/elsewhere/backup-clone", "pool/parent/vol-own@backup")

	_, err = newCloneScopeDriver(client, true).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-own"})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	assert.Contains(t, status.Convert(err).Message(), "dependent clones")
	assert.Equal(t, 1, client.dependentCloneQueries)
	remaining, shareErr := client.NFSShareGet(context.Background(), share.ID)
	require.NoError(t, shareErr)
	assert.NotNil(t, remaining, "the refusal must come before the share is deleted")
}

func TestDeleteVolumeSiblingCloneDoesNotVetoAVolumeWithSnapshots(t *testing.T) {
	client := newControllerCallCountingMock()
	addCloneScopeVolume(t, client, "vol-snap")
	addCloneScopeVolume(t, client, "vol-snap-2")
	// This volume has an uncloned foreign snapshot (destroyed with it, by opt-in).
	_, err := client.SnapshotCreate(context.Background(), "pool/parent/vol-snap", "foreign", nil)
	require.NoError(t, err)
	// The sibling's snapshot is cloned.
	_, err = client.SnapshotCreate(context.Background(), "pool/parent/vol-snap-2", "backup", nil)
	require.NoError(t, err)
	addCloneOf(t, client, "pool/parent/backup-clone", "pool/parent/vol-snap-2@backup")

	_, err = newCloneScopeDriver(client, true).DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-snap"})
	require.NoError(t, err)
	// With snapshots present, the origin scan is still the authority: once up
	// front, and once more after the non-recursive delete fails on the snapshot.
	assert.Equal(t, 2, client.dependentCloneQueries, "with snapshots present, the origin scan is still the authority")
	_, getErr := client.MockClient.DatasetGet(context.Background(), "pool/parent/vol-snap")
	assert.True(t, truenas.IsNotFoundError(getErr), "a sibling's clone must not veto this volume's delete")
}
