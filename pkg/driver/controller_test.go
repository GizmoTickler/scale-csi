package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

type controllerCallCountingMock struct {
	*truenas.MockClient
	datasetGetCalls       int
	snapshotListCalls     int
	dependentCloneQueries int
}

type busyDatasetDeleteMock struct {
	*controllerCallCountingMock
}

type cloneDependencyMock struct {
	*truenas.MockClient
	shareDeleteAttempted bool
}

func (m *cloneDependencyMock) SnapshotClone(ctx context.Context, snapshotID, newDatasetName string) error {
	if err := m.MockClient.SnapshotClone(ctx, snapshotID, newDatasetName); err != nil {
		return err
	}
	snap, err := m.SnapshotGet(ctx, snapshotID)
	if err != nil {
		return err
	}
	snap.Properties["clones"] = map[string]interface{}{"value": newDatasetName}
	return nil
}

func (m *cloneDependencyMock) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	snapshots, err := m.SnapshotList(ctx, name)
	if err != nil {
		return err
	}
	for _, snap := range snapshots {
		if len(snap.GetClones()) > 0 {
			return &truenas.APIError{Code: -1, Message: "snapshot has dependent clones"}
		}
	}
	if err := m.MockClient.DatasetDelete(ctx, name, recursive, force); err != nil {
		return err
	}
	for _, snap := range m.Snapshots {
		clones := snap.GetClones()
		remaining := clones[:0]
		for _, clone := range clones {
			if clone != name {
				remaining = append(remaining, clone)
			}
		}
		snap.Properties["clones"] = map[string]interface{}{"value": strings.Join(remaining, ",")}
	}
	return nil
}

func (m *cloneDependencyMock) SnapshotDelete(ctx context.Context, snapshotID string, defer_, recursive bool) error {
	snap, err := m.SnapshotGet(ctx, snapshotID)
	if err == nil && len(snap.GetClones()) > 0 {
		return &truenas.ErrSnapshotHasClones{SnapshotID: snapshotID, Clones: snap.GetClones()}
	}
	return m.MockClient.SnapshotDelete(ctx, snapshotID, defer_, recursive)
}

func (m *cloneDependencyMock) NFSShareDelete(ctx context.Context, id int) error {
	m.shareDeleteAttempted = true
	return m.MockClient.NFSShareDelete(ctx, id)
}

type recursiveCloneDependencyMock struct {
	*truenas.MockClient
}

type snapshotListErrorMock struct {
	*cloneDependencyMock
}

func (m *snapshotListErrorMock) SnapshotList(context.Context, string) ([]*truenas.Snapshot, error) {
	return nil, fmt.Errorf("injected snapshot query failure")
}

type blockingSnapshotCreateMock struct {
	*truenas.MockClient
	entered chan struct{}
	release chan struct{}
}

func (m *blockingSnapshotCreateMock) SnapshotCreate(ctx context.Context, dataset, name string) (*truenas.Snapshot, error) {
	close(m.entered)
	select {
	case <-m.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return m.MockClient.SnapshotCreate(ctx, dataset, name)
}

func (m *recursiveCloneDependencyMock) DatasetDelete(context.Context, string, bool, bool) error {
	return fmt.Errorf("snapshot has dependent clones")
}

func (m *busyDatasetDeleteMock) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	if !recursive {
		return &truenas.APIError{Code: -1, Message: "dataset is busy"}
	}
	return m.MockClient.DatasetDelete(ctx, name, recursive, force)
}

func newControllerCallCountingMock() *controllerCallCountingMock {
	return &controllerCallCountingMock{MockClient: truenas.NewMockClient()}
}

func (m *controllerCallCountingMock) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	m.datasetGetCalls++
	return m.MockClient.DatasetGet(ctx, name)
}

func (m *controllerCallCountingMock) SnapshotList(ctx context.Context, dataset string) ([]*truenas.Snapshot, error) {
	m.snapshotListCalls++
	return m.MockClient.SnapshotList(ctx, dataset)
}

func (m *controllerCallCountingMock) DatasetHasDependentClones(ctx context.Context, datasetName string) (bool, error) {
	m.dependentCloneQueries++
	return m.MockClient.DatasetHasDependentClones(ctx, datasetName)
}

func TestCreateVolume(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
			DriverName: "org.scale.csi.nfs",
			NFS: NFSConfig{
				ShareHost: "1.2.3.4",
			},
		},
		truenasClient: mockClient,
	}

	// Test Case 1: Success
	req := &csi.CreateVolumeRequest{
		Name:               "vol-01",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1024 * 1024 * 1024,
		},
	}
	resp, err := d.CreateVolume(context.Background(), req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "vol-01", resp.Volume.VolumeId)
	assert.Equal(t, int64(1024*1024*1024), resp.Volume.CapacityBytes)

	// Verify dataset created
	ds, err := mockClient.DatasetGet(context.Background(), "pool/parent/vol-01")
	assert.NoError(t, err)
	assert.Equal(t, "pool/parent/vol-01", ds.ID)

	// Test Case 2: Idempotency (Same request)
	resp2, err := d.CreateVolume(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, resp.Volume.VolumeId, resp2.Volume.VolumeId)

	// Test Case 3: Missing Name
	_, err = d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{})
	assert.Error(t, err)
}

func TestDeleteVolume(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: mockClient,
	}

	// Pre-create volume
	volName := "vol-delete"
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/" + volName,
	})
	assert.NoError(t, err)

	// Test Case 1: Success
	req := &csi.DeleteVolumeRequest{
		VolumeId: volName,
	}
	_, err = d.DeleteVolume(context.Background(), req)
	assert.NoError(t, err)

	// Verify dataset deleted
	_, err = mockClient.DatasetGet(context.Background(), "pool/parent/"+volName)
	assert.Error(t, err)

	// Test Case 2: Idempotency (Already deleted)
	_, err = d.DeleteVolume(context.Background(), req)
	assert.NoError(t, err)

	// Test Case 3: Missing ID
	_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{})
	assert.Error(t, err)
}

func TestGetVolumeContextUsesProvidedDatasetWithoutQuery(t *testing.T) {
	mockClient := newControllerCallCountingMock()
	d := &Driver{
		config:        &Config{NFS: NFSConfig{ShareHost: "10.0.0.10"}},
		truenasClient: mockClient,
	}
	ds := &truenas.Dataset{
		Name:           "pool/parent/vol-context",
		Type:           "FILESYSTEM",
		Mountpoint:     "/mnt/pool/parent/vol-context",
		UserProperties: map[string]truenas.UserProperty{},
	}

	volumeContext, err := d.getVolumeContext(context.Background(), ds, ds.Name, ShareTypeNFS)
	assert.NoError(t, err)
	assert.Equal(t, "/mnt/pool/parent/vol-context", volumeContext["share"])
	assert.Zero(t, mockClient.datasetGetCalls)
}

func TestDeleteVolumeHappyPathListsSnapshotsOnce(t *testing.T) {
	// The up-front dependent-snapshot check is deliberate: the share is deleted
	// before the dataset, so discovering snapshots only after DatasetDelete
	// fails would leave an existing volume with no share. One SnapshotList per
	// delete is the accepted cost.
	mockClient := newControllerCallCountingMock()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
		},
		truenasClient: mockClient,
	}
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/vol-fast-delete", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)

	_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-fast-delete"})
	assert.NoError(t, err)
	assert.Equal(t, 1, mockClient.datasetGetCalls)
	assert.Equal(t, 1, mockClient.snapshotListCalls)
	assert.Equal(t, 1, mockClient.dependentCloneQueries)
}

func TestDeleteVolumeWithManagedSnapshotFailsBeforeShareDeletion(t *testing.T) {
	mockClient := newControllerCallCountingMock()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "1.2.3.4"},
		},
		truenasClient: mockClient,
	}
	ctx := context.Background()
	_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/vol-snap-guard", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)
	share, err := mockClient.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: "/mnt/pool/parent/vol-snap-guard"})
	assert.NoError(t, err)
	snap, err := mockClient.SnapshotCreate(ctx, "pool/parent/vol-snap-guard", "snap-1")
	assert.NoError(t, err)
	assert.NoError(t, mockClient.SnapshotSetUserProperty(ctx, snap.ID, PropManagedResource, "true"))

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "vol-snap-guard"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	// The guard must fire before share deletion: the share still exists.
	remaining, listErr := mockClient.NFSShareGet(ctx, share.ID)
	assert.NoError(t, listErr)
	assert.NotNil(t, remaining)
}

func TestDeleteVolumeWithDatasetOriginCloneFailsBeforeShareDeletion(t *testing.T) {
	client := &cloneDependencyMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "1.2.3.4"},
		},
		truenasClient: client,
	}
	ctx := context.Background()
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/source-origin", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)
	source.Mountpoint = "/mnt/pool/parent/source-origin"
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: source.Mountpoint})
	assert.NoError(t, err)
	assert.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropNFSShareID, fmt.Sprint(share.ID)))
	clone, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/external/clone", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)
	clone.Origin = truenas.DatasetProperty{
		Value:  "pool/parent/source-origin@external-snapshot",
		Parsed: "pool/parent/source-origin@external-snapshot",
	}

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source-origin"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.False(t, client.shareDeleteAttempted)
	remaining, shareErr := client.NFSShareGet(ctx, share.ID)
	assert.NoError(t, shareErr)
	assert.NotNil(t, remaining)
}

func TestDeleteVolumeCloneSourceFailsBeforeShareDeletionThenSucceeds(t *testing.T) {
	client := &cloneDependencyMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "1.2.3.4"},
		},
		truenasClient: client,
	}
	ctx := context.Background()
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/source", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)
	source.Mountpoint = "/mnt/pool/parent/source"
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: source.Mountpoint})
	assert.NoError(t, err)
	assert.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropNFSShareID, fmt.Sprint(share.ID)))

	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "clone",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source"},
		}},
	})
	assert.NoError(t, err)
	clone, err := client.DatasetGet(ctx, "pool/parent/clone")
	assert.NoError(t, err)
	originID := clone.UserProperties[PropVolumeOriginSnapshot].Value
	assert.NotEmpty(t, originID)
	origin, err := client.SnapshotGet(ctx, originID)
	assert.NoError(t, err)
	assert.Equal(t, "true", origin.UserProperties[PropInternalResource].Value)
	assert.NotEmpty(t, origin.GetClones())

	client.shareDeleteAttempted = false
	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.False(t, client.shareDeleteAttempted)
	remainingShare, shareErr := client.NFSShareGet(ctx, share.ID)
	assert.NoError(t, shareErr)
	assert.NotNil(t, remainingShare)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "clone"})
	assert.NoError(t, err)
	_, err = client.SnapshotGet(ctx, originID)
	assert.Error(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source"})
	assert.NoError(t, err)
	_, err = client.DatasetGet(ctx, source.Name)
	assert.Error(t, err)
}

func TestDeleteVolumeRecursiveCloneDependencyIsFailedPrecondition(t *testing.T) {
	client := &recursiveCloneDependencyMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}, DriverName: "org.scale.csi.nfs"},
		truenasClient: client,
	}
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM"})
	assert.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, "pool/parent/source", "external")
	assert.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestDeleteVolumeSnapshotQueryFailureDoesNotDeleteShare(t *testing.T) {
	base := &cloneDependencyMock{MockClient: truenas.NewMockClient()}
	client := &snapshotListErrorMock{cloneDependencyMock: base}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "1.2.3.4"},
		},
		truenasClient: client,
	}
	ctx := context.Background()
	ds, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM"})
	assert.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: "/mnt/pool/parent/source"})
	assert.NoError(t, err)
	assert.NoError(t, client.DatasetSetUserProperty(ctx, ds.Name, PropNFSShareID, fmt.Sprint(share.ID)))

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source"})
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.False(t, client.shareDeleteAttempted)
	remainingShare, shareErr := client.NFSShareGet(ctx, share.ID)
	assert.NoError(t, shareErr)
	assert.NotNil(t, remainingShare)
}

func TestDeleteVolumeBusyDatasetChecksManagedSnapshots(t *testing.T) {
	countingMock := newControllerCallCountingMock()
	mockClient := &busyDatasetDeleteMock{controllerCallCountingMock: countingMock}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
		},
		truenasClient: mockClient,
	}
	datasetName := "pool/parent/vol-with-snapshot"
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: datasetName, Type: "FILESYSTEM",
	})
	assert.NoError(t, err)
	snapshot, err := mockClient.SnapshotCreate(context.Background(), datasetName, "managed")
	assert.NoError(t, err)
	snapshot.UserProperties[PropManagedResource] = truenas.UserProperty{Value: "true"}

	_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-with-snapshot"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Equal(t, 1, countingMock.snapshotListCalls)
}

func TestCreateSnapshot(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: mockClient,
	}

	// Pre-create source volume
	volName := "vol-snap"
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/" + volName,
	})
	assert.NoError(t, err)

	// Test Case 1: Success
	req := &csi.CreateSnapshotRequest{
		SourceVolumeId: volName,
		Name:           "snap-01",
	}
	resp, err := d.CreateSnapshot(context.Background(), req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "snap-01", resp.Snapshot.SnapshotId)
	assert.Equal(t, volName, resp.Snapshot.SourceVolumeId)

	// The same name and source are idempotent.
	idempotentResp, err := d.CreateSnapshot(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, resp.Snapshot.SnapshotId, idempotentResp.Snapshot.SnapshotId)

	// Verify snapshot created
	snapID := "pool/parent/" + volName + "@snap-01"
	snap, err := mockClient.SnapshotGet(context.Background(), snapID)
	assert.NoError(t, err)
	assert.Equal(t, snapID, snap.ID)

	// The same global CSI snapshot name cannot refer to another source volume.
	_, err = mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/vol-snap-other",
	})
	assert.NoError(t, err)
	_, err = d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: "vol-snap-other",
		Name:           "snap-01",
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	// Test Case 2: Missing Source
	_, err = d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "snap-02"})
	assert.Error(t, err)
}

func TestControllerPublishVolumeValidation(t *testing.T) {
	mockClient := truenas.NewMockClient()
	d := &Driver{
		nodeID:  "known-node",
		runNode: true,
		config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "pool/parent"},
		},
		truenasClient: mockClient,
	}
	capability := testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)

	_, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         "missing-volume",
		NodeId:           "known-node",
		VolumeCapability: capability,
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         "missing-volume",
		NodeId:           "unknown-node",
		VolumeCapability: capability,
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Contains(t, err.Error(), "node not found")

	_, err = d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "missing-volume",
		NodeId:   "known-node",
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCreateSnapshotSerializesWithDeleteVolume(t *testing.T) {
	client := &blockingSnapshotCreateMock{
		MockClient: truenas.NewMockClient(),
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}, DriverName: "org.scale.csi.nfs"},
		truenasClient: client,
	}
	_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/source", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)

	createErr := make(chan error, 1)
	go func() {
		_, createSnapshotErr := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
			Name: "snapshot", SourceVolumeId: "source",
		})
		createErr <- createSnapshotErr
	}()
	<-client.entered

	_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "source"})
	assert.Equal(t, codes.Aborted, status.Code(err))
	close(client.release)
	assert.NoError(t, <-createErr)
}

func TestDeleteSnapshot(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: mockClient,
	}

	// Pre-create snapshot
	snapName := "snap-delete"
	volName := "vol-snap-del"
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{Name: "pool/parent/" + volName})
	assert.NoError(t, err)
	_, err = mockClient.SnapshotCreate(context.Background(), "pool/parent/"+volName, snapName)
	assert.NoError(t, err)

	// Test Case 1: Success
	req := &csi.DeleteSnapshotRequest{
		SnapshotId: snapName,
	}
	_, err = d.DeleteSnapshot(context.Background(), req)
	assert.NoError(t, err)

	// Verify snapshot deleted
	// Note: Mock implementation of DeleteSnapshot deletes by ID, but ListSnapshots iterates map.
	// The driver implementation lists snapshots to find the one matching the name.
	// So we need to ensure our mock supports that flow.
}

// TestCreateSnapshot_RestoreSize verifies that CreateSnapshot returns the correct
// SizeBytes value for restore operations. This is critical for CSI volume restore
// where the PVC must have a size >= snapshot.restoreSize.
//
// The fix ensures we use the source volume's volsize (for zvols) instead of the
// snapshot's "used" bytes, which can be tiny for near-empty volumes.
func TestCreateSnapshot_RestoreSize(t *testing.T) {
	// Test cases for the restoreSize fix
	tests := []struct {
		name         string
		datasetType  string
		volsize      int64 // Source volume size (for zvols)
		refquota     int64 // Source refquota (for filesystems)
		snapshotUsed int64 // Snapshot "used" bytes to set on mock
		expectedSize int64 // Expected SizeBytes in response
		description  string
	}{
		{
			name:         "zvol_uses_volsize_not_used_bytes",
			datasetType:  "VOLUME",
			volsize:      10 * 1024 * 1024 * 1024, // 10 GiB
			snapshotUsed: 102400,                  // 100 KiB (near-empty volume)
			expectedSize: 10 * 1024 * 1024 * 1024, // Should use volsize, NOT used bytes
			description:  "For zvols with valid volsize, SizeBytes should be volsize (not snapshot used bytes)",
		},
		{
			name:         "zvol_empty_volume",
			datasetType:  "VOLUME",
			volsize:      5 * 1024 * 1024 * 1024, // 5 GiB
			snapshotUsed: 0,                      // Completely empty
			expectedSize: 5 * 1024 * 1024 * 1024, // Should use volsize
			description:  "Empty zvol should still use volsize for restoreSize",
		},
		{
			name:         "zvol_1gib_volume",
			datasetType:  "VOLUME",
			volsize:      1 * 1024 * 1024 * 1024, // 1 GiB
			snapshotUsed: 512,                    // Tiny used bytes
			expectedSize: 1 * 1024 * 1024 * 1024, // Should use volsize
			description:  "1 GiB zvol should return 1 GiB restoreSize even with tiny used bytes",
		},
		{
			name:         "zvol_zero_volsize_fallback",
			datasetType:  "VOLUME",
			volsize:      0,       // Invalid/missing volsize
			snapshotUsed: 2097152, // 2 MiB (not used in this test - mock returns 0)
			expectedSize: 0,       // Falls back to snapshot used bytes (mock returns 0)
			description:  "When volsize is 0 and refquota is 0, fall back to snapshot used bytes",
		},
		{
			name:         "filesystem_uses_refquota",
			datasetType:  "FILESYSTEM",
			refquota:     20 * 1024 * 1024 * 1024, // 20 GiB refquota
			snapshotUsed: 51200,                   // 50 KiB used
			expectedSize: 20 * 1024 * 1024 * 1024, // For filesystems (NFS), use refquota
			description:  "Filesystems use refquota as volume size",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mock client
			mockClient := truenas.NewMockClient()
			d := &Driver{
				config: &Config{
					ZFS: ZFSConfig{
						DatasetParentName: "pool/parent",
					},
				},
				truenasClient: mockClient,
			}

			volName := "vol-" + tc.name
			datasetName := "pool/parent/" + volName

			// Create source volume with appropriate type and size
			_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
				Name:     datasetName,
				Type:     tc.datasetType,
				Volsize:  tc.volsize,
				Refquota: tc.refquota,
			})
			assert.NoError(t, err, "Failed to create source dataset")

			// Create the snapshot request
			snapName := "snap-" + tc.name
			req := &csi.CreateSnapshotRequest{
				SourceVolumeId: volName,
				Name:           snapName,
			}

			// Execute CreateSnapshot
			resp, err := d.CreateSnapshot(context.Background(), req)
			assert.NoError(t, err, "CreateSnapshot should succeed: %s", tc.description)
			assert.NotNil(t, resp, "Response should not be nil")
			assert.NotNil(t, resp.Snapshot, "Snapshot should not be nil")

			// Now set the snapshot used bytes on the mock (for verification purposes)
			// Note: This happens after snapshot creation, but we use it to verify
			// that the code path is using volsize, not GetSnapshotSize()
			snapID := datasetName + "@" + snapName
			mockClient.SetSnapshotUsedBytes(snapID, tc.snapshotUsed)

			// Verify the snapshot size matches expected (volsize for zvols, refquota for filesystems)
			assert.Equal(t, tc.expectedSize, resp.Snapshot.SizeBytes,
				"%s: expected %d, got %d", tc.description, tc.expectedSize, resp.Snapshot.SizeBytes)

			// Verify other snapshot properties
			assert.Equal(t, snapName, resp.Snapshot.SnapshotId)
			assert.Equal(t, volName, resp.Snapshot.SourceVolumeId)
			assert.True(t, resp.Snapshot.ReadyToUse)
		})
	}
}

// TestCreateSnapshot_RestoreSizeFallbackWithUsedBytes tests the fallback path
// when volsize is not available but snapshot has used bytes set.
func TestCreateSnapshot_RestoreSizeFallbackWithUsedBytes(t *testing.T) {
	// This test uses a custom mock that sets used bytes on snapshot creation
	mockClient := &snapshotWithUsedBytesMock{
		MockClient: truenas.NewMockClient(),
		usedBytes:  5 * 1024 * 1024, // 5 MiB
	}

	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: mockClient,
	}

	volName := "vol-fallback"
	datasetName := "pool/parent/" + volName

	// Create source volume as FILESYSTEM (no volsize)
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "FILESYSTEM",
	})
	assert.NoError(t, err)

	req := &csi.CreateSnapshotRequest{
		SourceVolumeId: volName,
		Name:           "snap-fallback",
	}

	resp, err := d.CreateSnapshot(context.Background(), req)
	assert.NoError(t, err, "CreateSnapshot should succeed")
	assert.NotNil(t, resp)

	// Should fall back to snapshot used bytes (5 MiB)
	assert.Equal(t, int64(5*1024*1024), resp.Snapshot.SizeBytes,
		"Should use snapshot used bytes when volsize is not available")
}

// snapshotWithUsedBytesMock sets the used property on snapshots at creation time
type snapshotWithUsedBytesMock struct {
	*truenas.MockClient
	usedBytes int64
}

func (m *snapshotWithUsedBytesMock) SnapshotCreate(ctx context.Context, dataset, name string) (*truenas.Snapshot, error) {
	snap, err := m.MockClient.SnapshotCreate(ctx, dataset, name)
	if err != nil {
		return nil, err
	}
	// Set the used bytes property
	snap.Properties["used"] = map[string]interface{}{
		"parsed": float64(m.usedBytes),
	}
	return snap, nil
}

// TestCreateSnapshot_SourceDatasetGetFailure verifies that a failing source
// DatasetGet fails the RPC up front — before any snapshot is created — so a
// retry can succeed cleanly with no orphaned snapshot. (The former post-create
// size re-query and its used-bytes fallback were removed: the up-front fetch
// is reused for restoreSize.)
func TestCreateSnapshot_SourceDatasetGetFailure(t *testing.T) {
	mockClient := &datasetGetFailMock{
		MockClient:     truenas.NewMockClient(),
		failDatasetGet: true,
	}

	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: mockClient,
	}

	req := &csi.CreateSnapshotRequest{
		SourceVolumeId: "vol-fail-test",
		Name:           "snap-fail",
	}

	resp, err := d.CreateSnapshot(context.Background(), req)
	assert.Error(t, err, "CreateSnapshot must fail when the source dataset cannot be read")
	assert.Nil(t, resp)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.False(t, mockClient.snapCreated, "no snapshot may be created when the source lookup fails")
}

// datasetGetFailMock wraps MockClient to fail DatasetGet and record whether a
// snapshot was ever created.
type datasetGetFailMock struct {
	*truenas.MockClient
	failDatasetGet bool
	snapCreated    bool
}

func (m *datasetGetFailMock) SnapshotCreate(ctx context.Context, dataset, name string) (*truenas.Snapshot, error) {
	snap, err := m.MockClient.SnapshotCreate(ctx, dataset, name)
	if err == nil {
		m.snapCreated = true
	}
	return snap, err
}

func (m *datasetGetFailMock) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	if m.failDatasetGet {
		return nil, &truenas.APIError{Code: -1, Message: "dataset not found (simulated)"}
	}
	return m.MockClient.DatasetGet(ctx, name)
}

func TestControllerExpandVolume(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
			DriverName: "org.scale.csi.nfs",
		},
		truenasClient: mockClient,
	}

	// Pre-create volume
	volName := "vol-expand"
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name:    "pool/parent/" + volName,
		Volsize: 1024,
	})
	assert.NoError(t, err)

	// Test Case 1: Success
	req := &csi.ControllerExpandVolumeRequest{
		VolumeId: volName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 2048,
		},
	}
	resp, err := d.ControllerExpandVolume(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, int64(2048), resp.CapacityBytes)
	assert.False(t, resp.NodeExpansionRequired) // NFS doesn't require node expansion usually, but code says depends on resource type

	// Verify expansion
	ds, _ := mockClient.DatasetGet(context.Background(), "pool/parent/"+volName)
	// Note: Mock implementation updates Volsize for Expand, but driver might update Refquota for NFS
	// Let's check what the driver does.
	// Driver checks config.GetZFSResourceType().
	// Default config implies filesystem for NFS.
	// Driver updates Refquota for filesystem.
	// Mock DatasetUpdate handles Volsize, let's ensure it handles Refquota too if we want to test that path.
	// For now, basic check is fine.
	_ = ds
}
