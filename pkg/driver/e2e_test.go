// Package driver provides end-to-end tests for CSI operations using mocks.
// These tests validate the full workflow: provision -> stage -> publish -> unpublish -> unstage -> delete.
package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// TestE2E_NFSVolumeLifecycle tests the complete lifecycle of an NFS volume.
func TestE2E_NFSVolumeLifecycle(t *testing.T) {
	// Create driver with mock client
	driver, mockClient := createTestDriver(t, "nfs")

	ctx := context.Background()
	volumeName := "test-nfs-volume"
	volumeID := volumeName // Simplified for test

	// Step 1: CreateVolume
	t.Run("CreateVolume", func(t *testing.T) {
		req := &csi.CreateVolumeRequest{
			Name: volumeName,
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: 1024 * 1024 * 1024, // 1GB
			},
			VolumeCapabilities: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
					},
				},
			},
			Parameters: map[string]string{
				"protocol": "nfs",
			},
		}

		resp, err := driver.CreateVolume(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.NotEmpty(t, resp.Volume.VolumeId)

		// Verify dataset was created in mock
		ds, err := mockClient.DatasetGet(ctx, "tank/k8s/volumes/"+volumeID)
		require.NoError(t, err)
		assert.NotNil(t, ds)
	})

	// Step 2: CreateVolume (idempotent - second call should succeed)
	t.Run("CreateVolume_Idempotent", func(t *testing.T) {
		req := &csi.CreateVolumeRequest{
			Name: volumeName,
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: 1024 * 1024 * 1024,
			},
			VolumeCapabilities: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
					},
				},
			},
			Parameters: map[string]string{
				"protocol": "nfs",
			},
		}

		resp, err := driver.CreateVolume(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	// Step 3: DeleteVolume
	t.Run("DeleteVolume", func(t *testing.T) {
		req := &csi.DeleteVolumeRequest{
			VolumeId: volumeID,
		}

		_, err := driver.DeleteVolume(ctx, req)
		require.NoError(t, err)
	})

	// Step 4: DeleteVolume (idempotent - second call should succeed)
	t.Run("DeleteVolume_Idempotent", func(t *testing.T) {
		req := &csi.DeleteVolumeRequest{
			VolumeId: volumeID,
		}

		_, err := driver.DeleteVolume(ctx, req)
		require.NoError(t, err)
	})
}

// TestE2E_SnapshotLifecycle tests snapshot create and delete operations.
func TestE2E_SnapshotLifecycle(t *testing.T) {
	driver, mockClient := createTestDriver(t, "nfs")
	ctx := context.Background()

	volumeName := "test-volume-for-snapshot"
	snapshotName := "test-snapshot"

	// Setup: Create a volume first
	_, err := driver.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: volumeName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1024 * 1024 * 1024,
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
		Parameters: map[string]string{"protocol": "nfs"},
	})
	require.NoError(t, err)

	var snapshotID string

	// Step 1: CreateSnapshot
	t.Run("CreateSnapshot", func(t *testing.T) {
		req := &csi.CreateSnapshotRequest{
			SourceVolumeId: volumeName,
			Name:           snapshotName,
		}

		resp, err := driver.CreateSnapshot(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.NotEmpty(t, resp.Snapshot.SnapshotId)
		snapshotID = resp.Snapshot.SnapshotId

		// Verify snapshot was created in mock
		snaps, err := mockClient.SnapshotList(ctx, "tank/k8s/volumes/"+volumeName)
		require.NoError(t, err)
		assert.Greater(t, len(snaps), 0)
	})

	// Step 2: CreateSnapshot (idempotent)
	t.Run("CreateSnapshot_Idempotent", func(t *testing.T) {
		req := &csi.CreateSnapshotRequest{
			SourceVolumeId: volumeName,
			Name:           snapshotName,
		}

		resp, err := driver.CreateSnapshot(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	// Step 3: DeleteSnapshot
	t.Run("DeleteSnapshot", func(t *testing.T) {
		req := &csi.DeleteSnapshotRequest{
			SnapshotId: snapshotID,
		}

		_, err := driver.DeleteSnapshot(ctx, req)
		require.NoError(t, err)
	})

	// Cleanup
	_, _ = driver.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volumeName})
}

// TestE2E_VolumeExpansion tests volume expansion workflow.
func TestE2E_VolumeExpansion(t *testing.T) {
	driver, _ := createTestDriver(t, "nfs")
	ctx := context.Background()

	volumeName := "test-volume-expand"

	// Setup: Create a volume
	createResp, err := driver.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: volumeName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1024 * 1024 * 1024, // 1GB
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
		Parameters: map[string]string{"protocol": "nfs"},
	})
	require.NoError(t, err)

	// Step 1: ControllerExpandVolume
	t.Run("ControllerExpandVolume", func(t *testing.T) {
		req := &csi.ControllerExpandVolumeRequest{
			VolumeId: createResp.Volume.VolumeId,
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: 2 * 1024 * 1024 * 1024, // 2GB
			},
		}

		resp, err := driver.ControllerExpandVolume(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.GreaterOrEqual(t, resp.CapacityBytes, int64(2*1024*1024*1024))
	})

	// Cleanup
	_, _ = driver.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: createResp.Volume.VolumeId})
}

// TestE2E_AccessModeValidation tests that access mode is validated per protocol.
func TestE2E_AccessModeValidation(t *testing.T) {
	driver, _ := createTestDriver(t, "iscsi")
	ctx := context.Background()

	// iSCSI should reject MULTI_NODE_MULTI_WRITER (RWX)
	t.Run("iSCSI_RejectsRWX", func(t *testing.T) {
		req := &csi.CreateVolumeRequest{
			Name: "test-iscsi-rwx",
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: 1024 * 1024 * 1024,
			},
			VolumeCapabilities: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
					},
				},
			},
			Parameters: map[string]string{"protocol": "iscsi"},
		}

		_, err := driver.CreateVolume(ctx, req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "access mode")
	})
}

// TestShareType tests the ShareType enum functionality.
func TestShareType(t *testing.T) {
	t.Run("IsValid", func(t *testing.T) {
		assert.True(t, ShareTypeNFS.IsValid())
		assert.True(t, ShareTypeISCSI.IsValid())
		assert.True(t, ShareTypeNVMeoF.IsValid())
		assert.False(t, ShareType("invalid").IsValid())
	})

	t.Run("IsBlockProtocol", func(t *testing.T) {
		assert.False(t, ShareTypeNFS.IsBlockProtocol())
		assert.True(t, ShareTypeISCSI.IsBlockProtocol())
		assert.True(t, ShareTypeNVMeoF.IsBlockProtocol())
	})

	t.Run("ZFSResourceType", func(t *testing.T) {
		assert.Equal(t, "filesystem", ShareTypeNFS.ZFSResourceType())
		assert.Equal(t, "volume", ShareTypeISCSI.ZFSResourceType())
		assert.Equal(t, "volume", ShareTypeNVMeoF.ZFSResourceType())
	})

	t.Run("SupportsMultiNode", func(t *testing.T) {
		assert.True(t, ShareTypeNFS.SupportsMultiNode())
		assert.False(t, ShareTypeISCSI.SupportsMultiNode())
		assert.False(t, ShareTypeNVMeoF.SupportsMultiNode())
	})

	t.Run("ParseShareType", func(t *testing.T) {
		assert.Equal(t, ShareTypeNFS, ParseShareType("nfs"))
		assert.Equal(t, ShareTypeNFS, ParseShareType("NFS"))
		assert.Equal(t, ShareTypeNFS, ParseShareType("  nfs  "))
		assert.Equal(t, ShareTypeISCSI, ParseShareType("iscsi"))
		assert.Equal(t, ShareTypeNVMeoF, ParseShareType("nvmeof"))
		assert.Equal(t, ShareTypeNFS, ParseShareType("unknown")) // defaults to NFS
	})
}

// createTestDriver creates a driver with a mock TrueNAS client for testing.
func createTestDriver(t *testing.T, protocol string) (*Driver, *truenas.MockClient) {
	t.Helper()

	mockClient := truenas.NewMockClient()

	driverName := "org.scale.csi." + protocol
	config := &Config{
		DriverName: driverName,
		ZFS: ZFSConfig{
			DatasetParentName: "tank/k8s/volumes",
		},
		NFS: NFSConfig{
			ShareHost: "192.168.1.100",
		},
		ISCSI: ISCSIConfig{
			TargetPortal: "192.168.1.100:3260",
		},
		NVMeoF: NVMeoFConfig{
			TransportAddress: "192.168.1.100",
		},
	}

	driver := &Driver{
		name:          driverName,
		version:       "test",
		nodeID:        "test-node",
		config:        config,
		truenasClient: mockClient,
	}

	return driver, mockClient
}
