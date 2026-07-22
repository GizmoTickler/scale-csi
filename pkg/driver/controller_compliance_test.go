package driver

import (
	"context"
	"errors"
	"fmt"
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

func TestCreateVolumeRejectsExistingBlockProtocolMismatch(t *testing.T) {
	tests := []struct {
		name              string
		requestedProtocol string
		storedProperty    string
		storedProtocol    string
	}{
		{
			name:              "existing iSCSI volume rejects NVMe-oF request",
			requestedProtocol: "nvmeof",
			storedProperty:    PropISCSITargetExtentID,
			storedProtocol:    "iscsi",
		},
		{
			name:              "existing NVMe-oF volume rejects iSCSI request",
			requestedProtocol: "iscsi",
			storedProperty:    PropNVMeoFNamespaceID,
			storedProtocol:    "nvmeof",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := truenas.NewMockClient()
			mockClient.Datasets["pool/parent/existing-volume"] = &truenas.Dataset{
				ID:      "pool/parent/existing-volume",
				Name:    "pool/parent/existing-volume",
				Type:    "VOLUME",
				Volsize: truenas.DatasetProperty{Parsed: float64(testGiB)},
				UserProperties: map[string]truenas.UserProperty{
					tc.storedProperty: {Value: "42"},
				},
			}
			driver := newComplianceTestDriver(mockClient)

			_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
				Name:               "existing-volume",
				Parameters:         map[string]string{"protocol": tc.requestedProtocol},
				VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			})

			require.Error(t, err)
			assert.Equal(t, codes.AlreadyExists, status.Code(err))
			assert.Contains(t, err.Error(), "protocol "+tc.storedProtocol)
			assert.Contains(t, err.Error(), "requested "+tc.requestedProtocol)
		})
	}
}

func TestCreateVolumeExistingBlockProtocolMatchIsIdempotent(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
	}{
		{name: "iSCSI on iSCSI", protocol: "iscsi"},
		{name: "NVMe-oF on NVMe-oF", protocol: "nvmeof"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mockClient := truenas.NewMockClient()
			driver := newComplianceTestDriver(mockClient)
			driver.config.ISCSI.TargetPortal = "192.0.2.10:3260"
			driver.config.NVMeoF = NVMeoFConfig{
				Transport:             "TCP",
				TransportAddress:      "192.0.2.20",
				TransportServiceID:    4420,
				SubsystemAllowAnyHost: true,
			}
			driver.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(context.Context, string) error {
				return nil
			})
			t.Cleanup(driver.serviceReloadDebouncer.Stop)

			request := &csi.CreateVolumeRequest{
				Name:               "existing-block-volume",
				Parameters:         map[string]string{"protocol": tc.protocol},
				VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			}
			created, err := driver.CreateVolume(ctx, request)
			require.NoError(t, err)

			retry, err := driver.CreateVolume(ctx, request)
			require.NoError(t, err)
			assert.Equal(t, created.GetVolume().GetVolumeId(), retry.GetVolume().GetVolumeId())
			assert.Equal(t, testGiB, retry.GetVolume().GetCapacityBytes())
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

func TestCreateVolumeRequiresProtocolForMultiProtocolDriver(t *testing.T) {
	driver := newComplianceTestDriver(truenas.NewMockClient())
	driver.config.NFS.Enabled = true
	driver.config.ISCSI.Enabled = true

	_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "ambiguous-protocol",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
	})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), `StorageClass parameter "protocol" is required`)
	assert.Contains(t, err.Error(), "nfs, iscsi, nvmeof")
}

func TestCreateVolumeRejectsDisabledExplicitProtocol(t *testing.T) {
	driver := newComplianceTestDriver(truenas.NewMockClient())
	driver.config.NFS.Enabled = true
	driver.config.ISCSI.Enabled = true

	_, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "disabled-protocol",
		Parameters:         map[string]string{"protocol": "nvmeof"},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
	})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), `StorageClass parameter "protocol"`)
	assert.Contains(t, err.Error(), "nvmeof")
	assert.Contains(t, err.Error(), "enabled options are: nfs, iscsi")
	assert.NotContains(t, err.Error(), "nfs, iscsi, nvmeof")
}

func TestCreateVolumeKeepsSingleProtocolFallback(t *testing.T) {
	driver := newComplianceTestDriver(truenas.NewMockClient())
	driver.config.NFS.Enabled = true

	response, err := driver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "single-protocol",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
	})

	require.NoError(t, err)
	assert.Equal(t, "single-protocol", response.GetVolume().GetVolumeId())
}

func TestGetShareTypeUsesSoleEnabledProtocol(t *testing.T) {
	cfg := &Config{
		DriverName: "csi.scale.io",
		ISCSI:      ISCSIConfig{Enabled: true},
	}
	assert.Equal(t, ShareTypeISCSI, cfg.GetShareType(nil))
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

func TestValidateVolumeCapabilitiesRejectsBlockAccessTypeForFilesystem(t *testing.T) {
	mockClient := truenas.NewMockClient()
	mockClient.Datasets["pool/parent/nfs-volume"] = &truenas.Dataset{
		ID:             "pool/parent/nfs-volume",
		Name:           "pool/parent/nfs-volume",
		Type:           "FILESYSTEM",
		UserProperties: make(map[string]truenas.UserProperty),
	}
	driver := newComplianceTestDriver(mockClient)

	response, err := driver.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: "nfs-volume",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})

	require.NoError(t, err)
	assert.Nil(t, response.Confirmed)
	assert.Contains(t, response.Message, "block access type")
	assert.Contains(t, response.Message, "filesystem")
}

func TestCreateVolumeExistingResponseIncludesAccessibleTopology(t *testing.T) {
	ctx := context.Background()
	mockClient := truenas.NewMockClient()
	_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/existing-volume", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	driver := newComplianceTestDriver(mockClient)
	require.NoError(t, mockClient.DatasetSetUserProperty(ctx, "pool/parent/existing-volume", PropDriverInstanceID, driver.driverInstanceID()))
	driver.config.ZFS.DatasetEnableQuotas = true
	driver.config.Node.Topology = TopologyConfig{Enabled: true, Zone: "zone-a", Region: "region-a"}

	response, err := driver.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "existing-volume",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
	})

	require.NoError(t, err)
	require.Len(t, response.GetVolume().GetAccessibleTopology(), 1)
	assert.Equal(t, "zone-a", response.GetVolume().GetAccessibleTopology()[0].GetSegments()["topology.kubernetes.io/zone"])
	assert.Equal(t, "region-a", response.GetVolume().GetAccessibleTopology()[0].GetSegments()["topology.kubernetes.io/region"])
}

func TestCreateVolumeQuotaLessNFSUsesStoredRequestedCapacity(t *testing.T) {
	ctx := context.Background()
	mockClient := truenas.NewMockClient()
	driver := newComplianceTestDriver(mockClient)
	requested := 2 * testGiB
	request := &csi.CreateVolumeRequest{
		Name:               "quota-less-volume",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: requested, LimitBytes: requested},
	}

	response, err := driver.CreateVolume(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, requested, response.GetVolume().GetCapacityBytes())
	dataset, err := mockClient.DatasetGet(ctx, "pool/parent/quota-less-volume")
	require.NoError(t, err)
	assert.Equal(t, "2147483648", datasetUserProperty(dataset, PropRequestedSizeBytes))
	dataset.Available = truenas.DatasetProperty{Parsed: float64(100 * testGiB)}
	assert.Equal(t, requested, driver.getDatasetCapacity(dataset))

	retryResponse, err := driver.CreateVolume(ctx, request)
	require.NoError(t, err, "pool availability must not violate the original volume capacity limit")
	assert.Equal(t, requested, retryResponse.GetVolume().GetCapacityBytes())
}

func TestControllerExpandVolumeQuotaLessNFSUpdatesStoredRequestedCapacity(t *testing.T) {
	ctx := context.Background()
	mockClient := truenas.NewMockClient()
	driver := newComplianceTestDriver(mockClient)

	createRequest := &csi.CreateVolumeRequest{
		Name:               "quota-less-expand-volume",
		Parameters:         map[string]string{"protocol": "nfs"},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 2 * testGiB},
	}
	_, err := driver.CreateVolume(ctx, createRequest)
	require.NoError(t, err)

	expandResponse, err := driver.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      createRequest.Name,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 5 * testGiB},
	})
	require.NoError(t, err)
	assert.Equal(t, 5*testGiB, expandResponse.GetCapacityBytes())
	assert.False(t, expandResponse.GetNodeExpansionRequired())

	getResponse, err := driver.ControllerGetVolume(ctx, &csi.ControllerGetVolumeRequest{VolumeId: createRequest.Name})
	require.NoError(t, err)
	assert.Equal(t, 5*testGiB, getResponse.GetVolume().GetCapacityBytes())
	repeatExpandResponse, err := driver.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      createRequest.Name,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 5 * testGiB},
	})
	require.NoError(t, err)
	assert.Equal(t, 5*testGiB, repeatExpandResponse.GetCapacityBytes())
	assert.False(t, repeatExpandResponse.GetNodeExpansionRequired())

	dataset, err := mockClient.DatasetGet(ctx, "pool/parent/"+createRequest.Name)
	require.NoError(t, err)
	assert.Equal(t, "5368709120", datasetUserProperty(dataset, PropRequestedSizeBytes))

	createRequest.CapacityRange = &csi.CapacityRange{RequiredBytes: 5 * testGiB}
	retryResponse, err := driver.CreateVolume(ctx, createRequest)
	require.NoError(t, err)
	assert.Equal(t, 5*testGiB, retryResponse.GetVolume().GetCapacityBytes())
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
				_, err = mockClient.SnapshotCreate(context.Background(), "pool/parent/source", "source-snapshot", nil)
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
			mustCreateParentDataset(t, client.MockClient)
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
			mustCreateParentDataset(t, client.MockClient)
			_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
				Name: "pool/parent/source", Type: "VOLUME", Volsize: testGiB,
			})
			require.NoError(t, err)
			if tc.source.GetSnapshot() != nil {
				_, err = client.SnapshotCreate(context.Background(), "pool/parent/source", "source-snapshot", nil)
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
	mustCreateParentDataset(t, mockClient)
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	_, err = mockClient.SnapshotCreate(context.Background(), "pool/parent/source", "source-snapshot", nil)
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

func TestCreateVolumeFromSnapshotDetachedGate(t *testing.T) {
	for _, test := range []struct {
		name     string
		detached bool
	}{
		{name: "legacy clone remains the default", detached: false},
		{name: "detached copy is independent", detached: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			client := truenas.NewMockClient()
			mustCreateParentDataset(t, client)
			source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
				Name: "pool/parent/restore-intermediate", Type: "FILESYSTEM",
				UserProperties: []truenas.UserPropertyUpdate{
					{Key: PropCSIVolumeName, Value: "restore-intermediate"},
					{Key: PropVolumeContentSourceType, Value: "volume"},
					{Key: PropVolumeContentSourceID, Value: "stale-source"},
					{Key: PropVolumeOriginSnapshot, Value: "pool/parent/other@clone-source"},
					{Key: PropRequestedSizeBytes, Value: "1234"},
				},
			})
			require.NoError(t, err)
			source.Available = truenas.DatasetProperty{Parsed: float64(100 * testGiB)}
			snapshot, err := client.SnapshotCreate(ctx, source.Name, "restore-point", nil)
			require.NoError(t, err)

			driver := newComplianceTestDriver(client)
			driver.config.ZFS.DetachedVolumesFromSnapshots = test.detached
			driver.config.ZFS.DatasetEnableQuotas = false
			request := &csi.CreateVolumeRequest{
				Name:               "restored-volume",
				CapacityRange:      &csi.CapacityRange{RequiredBytes: 2 * testGiB},
				Parameters:         map[string]string{"protocol": "nfs"},
				VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
				VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "restore-point"},
				}},
			}

			response, err := driver.CreateVolume(ctx, request)
			require.NoError(t, err)
			assert.Equal(t, 2*testGiB, response.GetVolume().GetCapacityBytes())
			target, err := client.DatasetGet(ctx, "pool/parent/restored-volume")
			require.NoError(t, err)

			if test.detached {
				assert.Empty(t, datasetOriginSnapshotID(target))
				assert.Equal(t, "restored-volume", datasetUserProperty(target, PropCSIVolumeName))
				assert.Equal(t, "snapshot", datasetUserProperty(target, PropVolumeContentSourceType))
				assert.Equal(t, "restore-point", datasetUserProperty(target, PropVolumeContentSourceID))
				assert.Equal(t, "-", datasetUserProperty(target, PropVolumeOriginSnapshot))
				assert.Equal(t, "2147483648", datasetUserProperty(target, PropRequestedSizeBytes))

				// A durable content-source mismatch is not safe to repair or infer:
				// CreateVolume must reject it without mutating the stored identity.
				require.NoError(t, client.DatasetSetUserProperties(ctx, target.Name, map[string]string{
					PropCSIVolumeName:           "wrong-name",
					PropVolumeContentSourceType: "volume",
					PropVolumeContentSourceID:   "wrong-source",
					PropRequestedSizeBytes:      "1",
				}))
				_, retryErr := driver.CreateVolume(ctx, request)
				require.Error(t, retryErr)
				assert.Equal(t, codes.AlreadyExists, status.Code(retryErr))
				target, err = client.DatasetGet(ctx, target.Name)
				require.NoError(t, err)
				assert.Equal(t, "volume", datasetUserProperty(target, PropVolumeContentSourceType))
				assert.Equal(t, "wrong-source", datasetUserProperty(target, PropVolumeContentSourceID))

				// With a compatible durable source, retry remains idempotent and may
				// normalize other stale CSI bookkeeping properties.
				require.NoError(t, client.DatasetSetUserProperties(ctx, target.Name, map[string]string{
					PropVolumeContentSourceType: "snapshot",
					PropVolumeContentSourceID:   "restore-point",
				}))
				retry, retryErr := driver.CreateVolume(ctx, request)
				require.NoError(t, retryErr)
				assert.Equal(t, "restore-point", retry.GetVolume().GetContentSource().GetSnapshot().GetSnapshotId())
				target, err = client.DatasetGet(ctx, target.Name)
				require.NoError(t, err)
				assert.Equal(t, "restored-volume", datasetUserProperty(target, PropCSIVolumeName))
				assert.Equal(t, "snapshot", datasetUserProperty(target, PropVolumeContentSourceType))
				assert.Equal(t, "restore-point", datasetUserProperty(target, PropVolumeContentSourceID))
				assert.Equal(t, "2147483648", datasetUserProperty(target, PropRequestedSizeBytes))

				hasClones, cloneErr := client.DatasetHasDependentClones(ctx, source.Name)
				require.NoError(t, cloneErr)
				assert.False(t, hasClones)
				require.NoError(t, client.SnapshotDelete(ctx, snapshot.ID, false, false))
				require.NoError(t, client.DatasetDelete(ctx, source.Name, false, true))
			} else {
				assert.Equal(t, snapshot.ID, datasetOriginSnapshotID(target))
				hasClones, cloneErr := client.DatasetHasDependentClones(ctx, source.Name)
				require.NoError(t, cloneErr)
				assert.True(t, hasClones)
				require.Error(t, client.SnapshotDelete(ctx, snapshot.ID, false, false))
			}
		})
	}
}

func TestCreateVolumeDetachedSnapshotCopyNormalizesZvolCapacityAndIdentity(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	mustCreateParentDataset(t, client)
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/block-intermediate", Type: "VOLUME", Volsize: testGiB, Refreservation: testGiB,
		UserProperties: []truenas.UserPropertyUpdate{
			{Key: PropCSIVolumeName, Value: "block-intermediate"},
			{Key: PropVolumeContentSourceType, Value: "volume"},
			{Key: PropVolumeContentSourceID, Value: "stale-source"},
		},
	})
	require.NoError(t, err)
	snapshot, err := client.SnapshotCreate(ctx, source.Name, "block-restore", nil)
	require.NoError(t, err)

	driver := newComplianceTestDriver(client)
	driver.config.ZFS.DetachedVolumesFromSnapshots = true
	driver.config.ZFS.ZvolEnableReservation = true
	driver.config.ZFS.ZvolReadyTimeout = 1
	driver.config.ISCSI = ISCSIConfig{
		TargetPortal:      "192.0.2.10:3260",
		ExtentBlocksize:   512,
		ExtentRpm:         "SSD",
		DeviceWaitTimeout: 1,
	}
	driver.serviceReloadDebouncer = NewServiceReloadDebouncer(0, client.ServiceReload)
	t.Cleanup(driver.serviceReloadDebouncer.Stop)

	response, err := driver.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "restored-block-volume",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		Parameters:         map[string]string{"protocol": "iscsi"},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "block-restore"},
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, 2*testGiB, response.GetVolume().GetCapacityBytes())

	target, err := client.DatasetGet(ctx, "pool/parent/restored-block-volume")
	require.NoError(t, err)
	assert.Empty(t, datasetOriginSnapshotID(target))
	assert.Equal(t, float64(2*testGiB), target.Volsize.Parsed)
	assert.Equal(t, float64(2*testGiB), target.Refreservation.Parsed)
	assert.Equal(t, "restored-block-volume", datasetUserProperty(target, PropCSIVolumeName))
	assert.Equal(t, "snapshot", datasetUserProperty(target, PropVolumeContentSourceType))
	assert.Equal(t, "block-restore", datasetUserProperty(target, PropVolumeContentSourceID))

	require.NoError(t, client.SnapshotDelete(ctx, snapshot.ID, false, false))
	require.NoError(t, client.DatasetDelete(ctx, source.Name, false, true))
}

type transferredSnapshotCleanupTrackingClient struct {
	*truenas.MockClient
	destroyedSnapshots []string
}

func (m *transferredSnapshotCleanupTrackingClient) DestroyReplicatedTargetSnapshot(
	ctx context.Context,
	targetDataset string,
	snapshotShortName string,
) error {
	m.destroyedSnapshots = append(m.destroyedSnapshots, targetDataset+"@"+snapshotShortName)
	return m.MockClient.DestroyReplicatedTargetSnapshot(ctx, targetDataset, snapshotShortName)
}

func TestCreateVolumeDetachedSnapshotExistingCopyClearsInheritedShareIdentity(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/source-with-share", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	sourceShare, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: source.Mountpoint})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropNFSShareID, fmt.Sprint(sourceShare.ID)))
	snapshot, err := client.SnapshotCreate(ctx, source.Name, "existing-copy-snapshot", nil)
	require.NoError(t, err)
	require.NoError(t, client.CopyDatasetFromSnapshotLocal(
		ctx, source.Name, snapshot.Name, "pool/parent/existing-copy",
	))
	// A retry may resume only when the existing copy already has the durable
	// provenance written by the original request. CreateVolume must never infer
	// these properties from a later source-bearing request.
	require.NoError(t, client.DatasetSetUserProperties(ctx, "pool/parent/existing-copy", map[string]string{
		PropVolumeContentSourceType: "snapshot",
		PropVolumeContentSourceID:   snapshot.Name,
	}))
	transferredSnapshot, err := client.SnapshotCreate(ctx, "pool/parent/existing-copy", snapshot.Name, nil)
	require.NoError(t, err)

	trackingClient := &transferredSnapshotCleanupTrackingClient{MockClient: client}
	driver := newComplianceTestDriver(trackingClient)
	require.NoError(t, client.DatasetSetUserProperty(ctx, "pool/parent/existing-copy", PropDriverInstanceID, driver.driverInstanceID()))
	driver.config.ZFS.DetachedVolumesFromSnapshots = true
	driver.config.ZFS.DatasetEnableQuotas = true
	response, err := driver.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "existing-copy",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"protocol": "nfs"},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapshot.Name},
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, snapshot.Name, response.GetVolume().GetContentSource().GetSnapshot().GetSnapshotId())
	assert.Equal(t, []string{transferredSnapshot.ID}, trackingClient.destroyedSnapshots)
	_, err = client.SnapshotGet(ctx, transferredSnapshot.ID)
	require.Error(t, err)

	target, err := client.DatasetGet(ctx, "pool/parent/existing-copy")
	require.NoError(t, err)
	targetShareID := datasetUserProperty(target, PropNFSShareID)
	assert.NotEqual(t, fmt.Sprint(sourceShare.ID), targetShareID)
	assert.NotEqual(t, "-", targetShareID)
	remainingSourceShare, err := client.NFSShareGet(ctx, sourceShare.ID)
	require.NoError(t, err)
	assert.NotNil(t, remainingSourceShare)

	_, err = driver.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "existing-copy"})
	require.NoError(t, err)
	_, err = client.DatasetGet(ctx, target.Name)
	assert.True(t, truenas.IsNotFoundError(err))
}

type detachedCopyErrorClient struct {
	*truenas.MockClient
}

func (m *detachedCopyErrorClient) CopyDatasetFromSnapshotLocal(
	ctx context.Context,
	_, _, targetDataset string,
) error {
	_, err := m.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: targetDataset, Type: "FILESYSTEM"})
	if err != nil {
		return err
	}
	return errors.New("injected copy failure after partial receive")
}

func TestCreateVolumeDetachedSnapshotCopyFailureCleansPartialTarget(t *testing.T) {
	ctx := context.Background()
	client := &detachedCopyErrorClient{MockClient: truenas.NewMockClient()}
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/partial-copy-source", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, source.Name, "partial-copy-snapshot", nil)
	require.NoError(t, err)

	driver := newComplianceTestDriver(client)
	driver.config.ZFS.DetachedVolumesFromSnapshots = true
	_, err = driver.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "partial-copy-target",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"protocol": "nfs"},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "partial-copy-snapshot"},
		}},
	})
	require.Equal(t, codes.Internal, status.Code(err))

	_, err = client.DatasetGet(ctx, "pool/parent/partial-copy-target")
	assert.True(t, truenas.IsNotFoundError(err))
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
	_, err = client.SnapshotCreate(context.Background(), "pool/parent/source", "source-snapshot", nil)
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

func TestControllerGetVolumeOmitsUnadvertisedVolumeCondition(t *testing.T) {
	mockClient := truenas.NewMockClient()
	mockClient.Datasets["pool/parent/volume"] = &truenas.Dataset{
		ID:             "pool/parent/volume",
		Name:           "pool/parent/volume",
		Type:           "FILESYSTEM",
		UserProperties: make(map[string]truenas.UserProperty),
	}
	driver := newComplianceTestDriver(mockClient)

	response, err := driver.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: "volume"})

	require.NoError(t, err)
	assert.Nil(t, response.GetStatus())
}
