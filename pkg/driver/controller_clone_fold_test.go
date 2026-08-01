package driver

import (
	"context"
	"errors"
	"path"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

type cloneFoldCaptureClient struct {
	*truenas.MockClient
	foldUpdates        []*truenas.DatasetUpdateParams
	datasetExpandCalls int
	datasetDeleteCalls int
}

func isSnapshotCloneFold(params *truenas.DatasetUpdateParams) bool {
	if params == nil {
		return false
	}
	foundType := false
	foundID := false
	for _, update := range params.UserPropertiesUpdate {
		switch update.Key {
		case PropVolumeContentSourceType:
			foundType = true
		case PropVolumeContentSourceID:
			foundID = true
		}
	}
	return foundType || foundID
}

func cloneDatasetUpdateParams(params *truenas.DatasetUpdateParams) *truenas.DatasetUpdateParams {
	if params == nil {
		return nil
	}
	cloned := *params
	cloned.UserPropertiesUpdate = append([]truenas.UserPropertyUpdate(nil), params.UserPropertiesUpdate...)
	return &cloned
}

func (m *cloneFoldCaptureClient) DatasetUpdate(
	ctx context.Context,
	name string,
	params *truenas.DatasetUpdateParams,
) (*truenas.Dataset, error) {
	if isSnapshotCloneFold(params) {
		m.foldUpdates = append(m.foldUpdates, cloneDatasetUpdateParams(params))
	}
	return m.MockClient.DatasetUpdate(ctx, name, params)
}

func (m *cloneFoldCaptureClient) DatasetExpand(ctx context.Context, name string, newSize int64) error {
	m.datasetExpandCalls++
	return m.MockClient.DatasetExpand(ctx, name, newSize)
}

func (m *cloneFoldCaptureClient) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	m.datasetDeleteCalls++
	return m.MockClient.DatasetDelete(ctx, name, recursive, force)
}

type partialCloneFoldResponseClient struct {
	*cloneFoldCaptureClient
}

func (m *partialCloneFoldResponseClient) DatasetUpdate(
	ctx context.Context,
	name string,
	params *truenas.DatasetUpdateParams,
) (*truenas.Dataset, error) {
	if !isSnapshotCloneFold(params) {
		return m.cloneFoldCaptureClient.DatasetUpdate(ctx, name, params)
	}
	m.foldUpdates = append(m.foldUpdates, cloneDatasetUpdateParams(params))
	partial := *params
	partial.UserPropertiesUpdate = nil
	for _, update := range params.UserPropertiesUpdate {
		if update.Key == PropVolumeContentSourceType {
			partial.UserPropertiesUpdate = []truenas.UserPropertyUpdate{update}
			break
		}
	}
	return m.MockClient.DatasetUpdate(ctx, name, &partial)
}

type recoveryWinsCloneCleanupRaceClient struct {
	*cloneFoldCaptureClient
	owner string
	raced bool
}

func (m *recoveryWinsCloneCleanupRaceClient) DatasetUpdate(
	ctx context.Context,
	name string,
	params *truenas.DatasetUpdateParams,
) (*truenas.Dataset, error) {
	if !isSnapshotCloneFold(params) || m.raced {
		return m.cloneFoldCaptureClient.DatasetUpdate(ctx, name, params)
	}
	m.raced = true
	m.foldUpdates = append(m.foldUpdates, cloneDatasetUpdateParams(params))
	partial := *params
	partial.UserPropertiesUpdate = nil
	for _, update := range params.UserPropertiesUpdate {
		if update.Key == PropVolumeContentSourceType {
			partial.UserPropertiesUpdate = []truenas.UserPropertyUpdate{update}
			break
		}
	}
	response, err := m.MockClient.DatasetUpdate(ctx, name, &partial)
	if err != nil {
		return nil, err
	}
	if err := m.DatasetSetUserProperty(ctx, name, PropDriverInstanceID, m.owner); err != nil {
		return nil, err
	}
	// Return the partial response from our failed update. The peer's ownership
	// stamp is visible only to the guarded cleanup's fresh DatasetGet.
	return response, nil
}

func newSnapshotCloneFoldDriver(client truenas.ClientInterface, quotas bool) *Driver {
	return &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName:   "pool/parent",
				DatasetEnableQuotas: quotas,
				ZvolReadyTimeout:    1,
			},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
}

func snapshotCloneRequest(name, snapshotID, protocol string, capacity int64) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:               name,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: capacity},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": protocol},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapshotID},
		}},
	}
}

func seedSnapshotCloneSource(
	t *testing.T,
	client *truenas.MockClient,
	datasetType string,
	capacity int64,
) *truenas.Snapshot {
	t.Helper()
	mustCreateParentDataset(t, client)
	params := &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: datasetType}
	if datasetType == "VOLUME" {
		params.Volsize = capacity
	} else {
		params.Refquota = capacity
	}
	_, err := client.DatasetCreate(context.Background(), params)
	require.NoError(t, err)
	snapshot, err := client.SnapshotCreate(context.Background(), params.Name, "snap-1", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "snap-1",
		PropCSISnapshotSourceVolumeID: "source",
	})
	require.NoError(t, err)
	return snapshot
}

func TestSnapshotCloneFoldCrashStatesRecoverThroughExistingArm(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		applyFolded              bool
		wantOwnershipBeforeRetry bool
	}{
		{name: "crash immediately before merged update"},
		// Sprint 3 (L2a): the merged update now carries ownership atomically, so the
		// old "crash after merged update before ownership" state no longer exists.
		// The surviving post-merged-write crash point is "after the merged update,
		// before marker retire": ownership+content-source are durable and the marker
		// is still present, and a retry recovers through the existing-volume arm.
		{name: "crash after merged update before marker retire", applyFolded: true, wantOwnershipBeforeRetry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client := truenas.NewMockClient()
			d := newSnapshotCloneFoldDriver(client, true)
			snapshot := seedSnapshotCloneSource(t, client, "FILESYSTEM", testGiB)
			request := snapshotCloneRequest("restored", "snap-1", "nfs", 2*testGiB)
			marker, err := d.newInflightMarker("pool/parent/restored", request.GetVolumeContentSource(), ShareTypeNFS)
			require.NoError(t, err)
			marker.Origin = snapshot.ID
			require.NoError(t, d.writeInflightMarker(ctx, marker))
			require.NoError(t, client.SnapshotClone(ctx, snapshot.ID, marker.Dataset))

			if tc.applyFolded {
				_, err = d.setAndVerifyDatasetProps(ctx, marker.Dataset, 2*testGiB, map[string]string{
					PropVolumeContentSourceType: "snapshot",
					PropVolumeContentSourceID:   "snap-1",
					PropDriverInstanceID:        d.driverInstanceID(),
				})
				require.NoError(t, err)
			}
			beforeRetry, err := client.DatasetGet(ctx, marker.Dataset)
			require.NoError(t, err)
			require.Equal(t, tc.wantOwnershipBeforeRetry, datasetHasLocalOwnershipStamp(beforeRetry),
				"the simulated crash point must carry exactly the expected ownership state")

			response, err := d.CreateVolume(ctx, request)
			require.NoError(t, err)
			require.NotNil(t, response)
			assert.Equal(t, "snap-1", response.GetVolume().GetContentSource().GetSnapshot().GetSnapshotId())
			recovered, err := client.DatasetGet(ctx, marker.Dataset)
			require.NoError(t, err)
			assert.True(t, datasetHasLocalUserProperty(recovered, PropDriverInstanceID, d.driverInstanceID()))
			assert.True(t, datasetHasLocalUserProperty(recovered, PropVolumeContentSourceType, "snapshot"))
			assert.True(t, datasetHasLocalUserProperty(recovered, PropVolumeContentSourceID, "snap-1"))
			consumed, err := d.readInflightMarker(ctx, "restored")
			require.NoError(t, err)
			assert.Nil(t, consumed)
		})
	}
}

func TestSnapshotCloneFoldPartialResponseIsFatalAndGuardCleaned(t *testing.T) {
	ctx := context.Background()
	base := &cloneFoldCaptureClient{MockClient: truenas.NewMockClient()}
	client := &partialCloneFoldResponseClient{cloneFoldCaptureClient: base}
	d := newSnapshotCloneFoldDriver(client, true)
	seedSnapshotCloneSource(t, client.MockClient, "FILESYSTEM", testGiB)

	_, err := d.CreateVolume(ctx, snapshotCloneRequest("restored", "snap-1", "nfs", 2*testGiB))
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Len(t, client.foldUpdates, 1)
	assert.Equal(t, 1, client.datasetDeleteCalls, "the still-matching unstamped attempt must be cleaned")
	_, getErr := client.DatasetGet(ctx, "pool/parent/restored")
	assert.True(t, truenas.IsNotFoundError(getErr), "partial content-source state must not wedge a destination")
	marker, markerErr := d.readInflightMarker(ctx, "restored")
	require.NoError(t, markerErr)
	assert.Nil(t, marker, "successful guarded cleanup must retire the marker")
}

func TestSnapshotCloneFoldCleanupLosesToPeerRecovery(t *testing.T) {
	ctx := context.Background()
	base := &cloneFoldCaptureClient{MockClient: truenas.NewMockClient()}
	d := newSnapshotCloneFoldDriver(base, true)
	client := &recoveryWinsCloneCleanupRaceClient{
		cloneFoldCaptureClient: base,
		owner:                  d.driverInstanceID(),
	}
	d.truenasClient = client
	seedSnapshotCloneSource(t, client.MockClient, "FILESYSTEM", testGiB)

	_, err := d.CreateVolume(ctx, snapshotCloneRequest("restored", "snap-1", "nfs", 2*testGiB))
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
	assert.True(t, client.raced)
	assert.Zero(t, client.datasetDeleteCalls, "the cleanup loser must never delete the peer-owned clone")
	clone, getErr := client.DatasetGet(ctx, "pool/parent/restored")
	require.NoError(t, getErr)
	assert.True(t, datasetHasLocalUserProperty(clone, PropDriverInstanceID, d.driverInstanceID()))
}

func TestSnapshotCloneFoldInvalidStoredContentSourceAlreadyExists(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newSnapshotCloneFoldDriver(client, false)
	seedSnapshotCloneSource(t, client, "FILESYSTEM", testGiB)
	request := snapshotCloneRequest("restored", "snap-1", "nfs", testGiB)
	_, err := d.CreateVolume(ctx, request)
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, "pool/parent/restored", PropVolumeContentSourceID, "different-snapshot"))

	_, err = d.CreateVolume(ctx, request)
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	_, getErr := client.DatasetGet(ctx, "pool/parent/restored")
	require.NoError(t, getErr, "the AlreadyExists content-source gate must not delete the volume")
}

func TestSnapshotCloneFoldQuotaAndZvolPaths(t *testing.T) {
	tests := []struct {
		name              string
		datasetType       string
		protocol          ShareType
		quotas            bool
		wantRefquota      bool
		wantDatasetExpand int
	}{
		{
			name:         "quota-enabled filesystem",
			datasetType:  "FILESYSTEM",
			protocol:     ShareTypeNFS,
			quotas:       true,
			wantRefquota: true,
		},
		{
			name:        "quota-disabled filesystem",
			datasetType: "FILESYSTEM",
			protocol:    ShareTypeNFS,
		},
		{
			name:              "zvol keeps DatasetExpand",
			datasetType:       "VOLUME",
			protocol:          ShareTypeISCSI,
			wantDatasetExpand: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client := &cloneFoldCaptureClient{MockClient: truenas.NewMockClient()}
			d := newSnapshotCloneFoldDriver(client, tc.quotas)
			seedSnapshotCloneSource(t, client.MockClient, tc.datasetType, testGiB)
			source := snapshotCloneRequest("restored", "snap-1", string(tc.protocol), 2*testGiB).GetVolumeContentSource()

			created, _, err := d.handleVolumeContentSource(
				ctx,
				"pool/parent/restored",
				"restored",
				source,
				2*testGiB,
				tc.protocol,
				false,
				nil,
			)
			require.NoError(t, err)
			require.NotNil(t, created)
			writtenMarker, markerErr := d.readInflightMarker(ctx, "restored")
			require.NoError(t, markerErr)
			require.NotNil(t, writtenMarker, "the clone must write a durable in-flight marker before mutating")
			require.Len(t, client.foldUpdates, 1, "quota and content source must share exactly one update")
			fold := client.foldUpdates[0]
			if tc.wantRefquota {
				assert.Equal(t, 2*testGiB, fold.Refquota)
				assert.Equal(t, 2*testGiB, datasetPropertyBytes(created.Refquota))
				assert.True(t, isLocalUserPropertySource(created.Refquota.Source))
			} else {
				assert.Nil(t, fold.Refquota)
			}
			assert.Equal(t, tc.wantDatasetExpand, client.datasetExpandCalls)
			properties := make(map[string]string)
			for _, update := range fold.UserPropertiesUpdate {
				properties[update.Key] = update.Value
			}
			// Sprint 3 (L2a): the ownership stamp folds INTO the merged content-source
			// update, so all three keys persist in one atomic pool.dataset.update.
			assert.Equal(t, map[string]string{
				PropVolumeContentSourceID:   "snap-1",
				PropVolumeContentSourceType: "snapshot",
				PropDriverInstanceID:        d.driverInstanceID(),
			}, properties)
			if tc.datasetType == "VOLUME" {
				assert.Equal(t, 2*testGiB, datasetPropertyBytes(created.Volsize))
			}
		})
	}
}

func TestGuardedSnapshotCloneCleanupRefusesMarkerIdentityChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, context.Context, *Driver, *truenas.MockClient, inflightMarker)
	}{
		{
			name: "marker absent",
			mutate: func(t *testing.T, ctx context.Context, d *Driver, _ *truenas.MockClient, marker inflightMarker) {
				t.Helper()
				d.deleteInflightMarker(ctx, path.Base(marker.Dataset))
			},
		},
		{
			name: "marker nonce changed",
			mutate: func(t *testing.T, ctx context.Context, d *Driver, _ *truenas.MockClient, marker inflightMarker) {
				t.Helper()
				marker.Nonce = "different-attempt"
				require.NoError(t, d.writeInflightMarker(ctx, marker))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client := truenas.NewMockClient()
			d := newSnapshotCloneFoldDriver(client, false)
			snapshot := seedSnapshotCloneSource(t, client, "FILESYSTEM", testGiB)
			source := snapshotCloneRequest("restored", "snap-1", "nfs", testGiB).GetVolumeContentSource()
			marker, err := d.newInflightMarker("pool/parent/restored", source, ShareTypeNFS)
			require.NoError(t, err)
			marker.Origin = snapshot.ID
			require.NoError(t, d.writeInflightMarker(ctx, marker))
			require.NoError(t, client.SnapshotClone(ctx, snapshot.ID, marker.Dataset))
			tc.mutate(t, ctx, d, client, marker)

			err = d.guardedCleanupFailedSnapshotClone(ctx, marker.Dataset, &marker, errors.New("injected failure"))
			assert.Equal(t, codes.Aborted, status.Code(err))
			_, getErr := client.DatasetGet(ctx, marker.Dataset)
			require.NoError(t, getErr, "changed cleanup identity must preserve the dataset")
		})
	}
}
