package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

const (
	testGiB = int64(1024 * 1024 * 1024)
)

func newComplianceTestDriver(client truenas.ClientInterface) *Driver {
	return &Driver{
		name: "org.scale.csi.test",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
			NFS: NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
}

func testVolumeCapability(mode csi.VolumeCapability_AccessMode_Mode) *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
	}
}

func TestCreateVolumeExistingVolumeCompatibility(t *testing.T) {
	tests := []struct {
		name             string
		protocol         string
		datasetType      string
		existingCapacity int64
		requiredBytes    int64
		limitBytes       int64
	}{
		{
			name:             "block request conflicts with filesystem",
			protocol:         "iscsi",
			datasetType:      "FILESYSTEM",
			existingCapacity: testGiB,
			requiredBytes:    testGiB,
		},
		{
			name:             "NFS request conflicts with block volume",
			protocol:         "nfs",
			datasetType:      "VOLUME",
			existingCapacity: testGiB,
			requiredBytes:    testGiB,
		},
		{
			name:             "existing capacity is below required",
			protocol:         "nfs",
			datasetType:      "FILESYSTEM",
			existingCapacity: testGiB,
			requiredBytes:    2 * testGiB,
		},
		{
			name:             "existing capacity exceeds limit",
			protocol:         "nfs",
			datasetType:      "FILESYSTEM",
			existingCapacity: 3 * testGiB,
			requiredBytes:    testGiB,
			limitBytes:       2 * testGiB,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := truenas.NewMockClient()
			dataset := &truenas.Dataset{
				ID:             "pool/parent/existing-volume",
				Name:           "pool/parent/existing-volume",
				Type:           tc.datasetType,
				UserProperties: make(map[string]truenas.UserProperty),
			}
			if tc.datasetType == "VOLUME" {
				dataset.Volsize = truenas.DatasetProperty{Parsed: float64(tc.existingCapacity)}
			} else {
				dataset.Refquota = truenas.DatasetProperty{Parsed: float64(tc.existingCapacity)}
			}
			mockClient.Datasets[dataset.Name] = dataset

			driver := newComplianceTestDriver(mockClient)
			_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:               "existing-volume",
				VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: tc.requiredBytes,
					LimitBytes:    tc.limitBytes,
				},
				Parameters: map[string]string{"protocol": tc.protocol},
			})

			require.Error(t, err)
			assert.Equal(t, codes.AlreadyExists, status.Code(err))
		})
	}
}

func TestCreateVolumeRejectsUnknownExplicitProtocol(t *testing.T) {
	driver := newComplianceTestDriver(truenas.NewMockClient())

	_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "invalid-protocol",
		Parameters:         map[string]string{"protocol": "smb"},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
	})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "smb")
	assert.Contains(t, err.Error(), "nfs, iscsi, nvmeof")
}

func TestCreateVolumeRequiresCapabilities(t *testing.T) {
	driver := newComplianceTestDriver(truenas.NewMockClient())

	_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{Name: "missing-capabilities"})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "volume capabilities")
}

func TestValidateVolumeCapabilitiesRejectsMultiNodeForBlockVolume(t *testing.T) {
	mockClient := truenas.NewMockClient()
	mockClient.Datasets["pool/parent/block-volume"] = &truenas.Dataset{
		ID:             "pool/parent/block-volume",
		Name:           "pool/parent/block-volume",
		Type:           "VOLUME",
		UserProperties: make(map[string]truenas.UserProperty),
	}
	driver := newComplianceTestDriver(mockClient)

	response, err := driver.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: "block-volume",
		VolumeCapabilities: []*csi.VolumeCapability{
			testVolumeCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
		},
	})

	require.NoError(t, err)
	assert.Nil(t, response.Confirmed)
	assert.Contains(t, response.Message, "requires NFS")
}

func TestValidateVolumeCapabilitiesDatasetErrors(t *testing.T) {
	request := &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: "missing-volume",
		VolumeCapabilities: []*csi.VolumeCapability{
			testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
		},
	}

	t.Run("not found", func(t *testing.T) {
		driver := newComplianceTestDriver(truenas.NewMockClient())
		_, err := driver.ValidateVolumeCapabilities(context.Background(), request)
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("transient backend error", func(t *testing.T) {
		mockClient := truenas.NewMockClient()
		mockClient.InjectError = errors.New("temporary backend failure")
		driver := newComplianceTestDriver(mockClient)
		_, err := driver.ValidateVolumeCapabilities(context.Background(), request)
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
	})
}

type cloneExpandErrorClient struct {
	*truenas.MockClient
	expandCalls int
}

func (m *cloneExpandErrorClient) DatasetExpand(context.Context, string, int64) error {
	m.expandCalls++
	return errors.New("injected expand failure")
}

func TestCreateVolumeCloneExpansionFailure(t *testing.T) {
	tests := []struct {
		name          string
		contentSource func(*truenas.MockClient) *csi.VolumeContentSource
	}{
		{
			name: "snapshot clone",
			contentSource: func(mockClient *truenas.MockClient) *csi.VolumeContentSource {
				_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
					Name: "pool/parent/source", Type: "VOLUME", Volsize: testGiB,
				})
				require.NoError(t, err)
				_, err = mockClient.SnapshotCreate(context.Background(), "pool/parent/source", "source-snapshot")
				require.NoError(t, err)
				return &csi.VolumeContentSource{
					Type: &csi.VolumeContentSource_Snapshot{
						Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "source-snapshot"},
					},
				}
			},
		},
		{
			name: "volume clone",
			contentSource: func(mockClient *truenas.MockClient) *csi.VolumeContentSource {
				_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
					Name: "pool/parent/source", Type: "VOLUME", Volsize: testGiB,
				})
				require.NoError(t, err)
				return &csi.VolumeContentSource{
					Type: &csi.VolumeContentSource_Volume{
						Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source"},
					},
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &cloneExpandErrorClient{MockClient: truenas.NewMockClient()}
			driver := newComplianceTestDriver(client)
			source := tc.contentSource(client.MockClient)

			_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:                "clone-target",
				CapacityRange:       &csi.CapacityRange{RequiredBytes: 2 * testGiB},
				Parameters:          map[string]string{"protocol": "iscsi"},
				VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
				VolumeContentSource: source,
			})

			require.Error(t, err)
			assert.Equal(t, codes.Internal, status.Code(err))
			assert.Equal(t, 1, client.expandCalls)
		})
	}
}

type cloneWaitErrorClient struct {
	*truenas.MockClient
	datasetDeletes  []string
	snapshotDeletes []string
}

func (m *cloneWaitErrorClient) WaitForZvolReady(context.Context, string, time.Duration) (*truenas.Dataset, error) {
	return nil, errors.New("injected readiness failure")
}

func (m *cloneWaitErrorClient) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	m.datasetDeletes = append(m.datasetDeletes, name)
	return m.MockClient.DatasetDelete(ctx, name, recursive, force)
}

func (m *cloneWaitErrorClient) SnapshotDelete(ctx context.Context, snapshotID string, defer_, recursive bool) error {
	m.snapshotDeletes = append(m.snapshotDeletes, snapshotID)
	return m.MockClient.SnapshotDelete(ctx, snapshotID, defer_, recursive)
}

func TestCreateVolumeCloneReadinessFailure(t *testing.T) {
	tests := []struct {
		name                string
		source              *csi.VolumeContentSource
		wantSnapshotCleanup bool
	}{
		{
			name: "snapshot clone",
			source: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "source-snapshot"},
			}},
		},
		{
			name: "volume clone",
			source: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source"},
			}},
			wantSnapshotCleanup: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &cloneWaitErrorClient{MockClient: truenas.NewMockClient()}
			_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
				Name: "pool/parent/source", Type: "VOLUME", Volsize: testGiB,
			})
			require.NoError(t, err)
			if tc.source.GetSnapshot() != nil {
				_, err = client.SnapshotCreate(context.Background(), "pool/parent/source", "source-snapshot")
				require.NoError(t, err)
			}

			driver := newComplianceTestDriver(client)
			_, err = driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:                "clone-target",
				CapacityRange:       &csi.CapacityRange{RequiredBytes: testGiB},
				Parameters:          map[string]string{"protocol": "iscsi"},
				VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
				VolumeContentSource: tc.source,
			})

			require.Error(t, err)
			assert.Equal(t, codes.Internal, status.Code(err))
			assert.Equal(t, []string{"pool/parent/clone-target"}, client.datasetDeletes)
			if tc.wantSnapshotCleanup {
				require.Len(t, client.snapshotDeletes, 1)
				assert.Contains(t, client.snapshotDeletes[0], "pool/parent/source@clone-source-")
			} else {
				assert.Empty(t, client.snapshotDeletes, "a user-provided source snapshot must never be deleted")
			}
		})
	}
}

func TestCreateVolumeNFSCloneSetsRefquota(t *testing.T) {
	mockClient := truenas.NewMockClient()
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	_, err = mockClient.SnapshotCreate(context.Background(), "pool/parent/source", "source-snapshot")
	require.NoError(t, err)

	driver := newComplianceTestDriver(mockClient)
	driver.config.ZFS.DatasetEnableQuotas = true
	requestedCapacity := 20 * testGiB
	_, err = driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "nfs-clone-target",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: requestedCapacity},
		Parameters:         map[string]string{"protocol": "nfs"},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "source-snapshot"},
		}},
	})
	require.NoError(t, err)

	clone, err := mockClient.DatasetGet(context.Background(), "pool/parent/nfs-clone-target")
	require.NoError(t, err)
	assert.Equal(t, float64(requestedCapacity), clone.Refquota.Parsed)
}

type quotaUpdateErrorClient struct {
	*truenas.MockClient
}

func (m *quotaUpdateErrorClient) DatasetUpdate(context.Context, string, *truenas.DatasetUpdateParams) (*truenas.Dataset, error) {
	return nil, errors.New("injected quota update failure")
}

func TestCreateVolumeNFSCloneQuotaFailure(t *testing.T) {
	client := &quotaUpdateErrorClient{MockClient: truenas.NewMockClient()}
	_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	_, err = client.SnapshotCreate(context.Background(), "pool/parent/source", "source-snapshot")
	require.NoError(t, err)

	driver := newComplianceTestDriver(client)
	driver.config.ZFS.DatasetEnableQuotas = true
	_, err = driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "nfs-clone-target",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		Parameters:         map[string]string{"protocol": "nfs"},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "source-snapshot"},
		}},
	})

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestCreateVolumeCloneMissingSourceVolume(t *testing.T) {
	driver := newComplianceTestDriver(truenas.NewMockClient())

	_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "clone-target",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "missing-source"},
		}},
	})

	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Contains(t, err.Error(), "source volume not found")
}

type expandTrackingClient struct {
	*truenas.MockClient
	expandCalls int
}

func (m *expandTrackingClient) DatasetExpand(ctx context.Context, name string, newSize int64) error {
	m.expandCalls++
	return m.MockClient.DatasetExpand(ctx, name, newSize)
}

func TestControllerExpandVolumeShrinkIsNoOp(t *testing.T) {
	client := &expandTrackingClient{MockClient: truenas.NewMockClient()}
	_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/block-volume", Type: "VOLUME", Volsize: 2 * testGiB,
	})
	require.NoError(t, err)
	driver := newComplianceTestDriver(client)

	response, err := driver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "block-volume",
		CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
	})

	require.NoError(t, err)
	assert.Equal(t, 2*testGiB, response.CapacityBytes)
	// Node expansion must still be requested for zvols: a retried expand can
	// arrive after the controller resize succeeded but before the node grew
	// the filesystem.
	assert.True(t, response.NodeExpansionRequired)
	assert.Zero(t, client.expandCalls)
}

func TestControllerExpandVolumeRejectsRequiredAboveLimit(t *testing.T) {
	driver := newComplianceTestDriver(truenas.NewMockClient())

	_, err := driver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: "block-volume",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 2 * testGiB,
			LimitBytes:    testGiB,
		},
	})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

type directSnapshotLookupClient struct {
	*truenas.MockClient
	listCalls int
}

func (m *directSnapshotLookupClient) SnapshotListAll(context.Context, string, int, int) ([]*truenas.Snapshot, error) {
	m.listCalls++
	return nil, nil
}

func TestListSnapshotsByIDUsesDirectLookup(t *testing.T) {
	client := &directSnapshotLookupClient{MockClient: truenas.NewMockClient()}
	client.Snapshots["pool/parent/source@snapshot-outside-page"] = &truenas.Snapshot{
		ID:      "pool/parent/source@snapshot-outside-page",
		Name:    "snapshot-outside-page",
		Dataset: "pool/parent/source",
		UserProperties: map[string]truenas.UserProperty{
			PropManagedResource:           {Value: "true"},
			PropCSISnapshotSourceVolumeID: {Value: "source"},
		},
	}
	driver := newComplianceTestDriver(client)

	response, err := driver.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SnapshotId:    "snapshot-outside-page",
		MaxEntries:    1,
		StartingToken: "not-a-page-token",
	})

	require.NoError(t, err)
	require.Len(t, response.Entries, 1)
	assert.Equal(t, "snapshot-outside-page", response.Entries[0].Snapshot.SnapshotId)
	assert.Empty(t, response.NextToken)
	assert.Zero(t, client.listCalls)
}

func TestControllerGetVolumeDatasetErrors(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		driver := newComplianceTestDriver(truenas.NewMockClient())
		_, err := driver.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: "missing"})
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("transient backend error", func(t *testing.T) {
		mockClient := truenas.NewMockClient()
		mockClient.InjectError = errors.New("temporary backend failure")
		driver := newComplianceTestDriver(mockClient)
		_, err := driver.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: "volume"})
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
	})
}
