package driver

import (
	"context"
	"os"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// newTestNodeDriver creates a minimal driver configured for node testing.
func newTestNodeDriver(shareType ShareType) *Driver {
	cfg := &Config{
		DriverName: "org.scale.csi.nfs",
		ZFS: ZFSConfig{
			DatasetParentName: "pool/test",
		},
		NFS: NFSConfig{
			ShareHost: "192.168.1.100",
		},
		ISCSI: ISCSIConfig{
			TargetPortal:      "192.168.1.100:3260",
			DeviceWaitTimeout: 60,
		},
		NVMeoF: NVMeoFConfig{
			TransportAddress:   "192.168.1.100",
			TransportServiceID: 4420,
			Transport:          "tcp",
			DeviceWaitTimeout:  60,
		},
		Node: NodeConfig{
			SessionCleanupDelay: 100,
		},
		SessionGC: SessionGCConfig{
			Interval: 0, // Disabled for tests
		},
	}

	// Set driver name based on share type
	switch shareType {
	case ShareTypeNFS:
		cfg.DriverName = "org.scale.csi.nfs"
	case ShareTypeISCSI:
		cfg.DriverName = "org.scale.csi.iscsi"
	case ShareTypeNVMeoF:
		cfg.DriverName = "org.scale.csi.nvmeof"
	}

	return &Driver{
		name:          cfg.DriverName,
		version:       "test",
		nodeID:        "test-node-1",
		config:        cfg,
		truenasClient: truenas.NewMockClient(),
	}
}

func TestNodeGetCapabilities(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)

	resp, err := d.NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotEmpty(t, resp.Capabilities)

	// Verify expected capabilities
	capTypes := make(map[csi.NodeServiceCapability_RPC_Type]bool)
	for _, cap := range resp.Capabilities {
		if rpc := cap.GetRpc(); rpc != nil {
			capTypes[rpc.Type] = true
		}
	}

	assert.True(t, capTypes[csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME], "should have STAGE_UNSTAGE_VOLUME")
	assert.True(t, capTypes[csi.NodeServiceCapability_RPC_GET_VOLUME_STATS], "should have GET_VOLUME_STATS")
	assert.True(t, capTypes[csi.NodeServiceCapability_RPC_EXPAND_VOLUME], "should have EXPAND_VOLUME")
	assert.True(t, capTypes[csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER], "should have SINGLE_NODE_MULTI_WRITER")
	assert.True(t, capTypes[csi.NodeServiceCapability_RPC_VOLUME_CONDITION], "should have VOLUME_CONDITION")
}

func TestNodeGetInfo(t *testing.T) {
	t.Run("BasicInfo", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		d.nodeID = "worker-node-1"

		resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "worker-node-1", resp.NodeId)
		assert.Nil(t, resp.AccessibleTopology, "no topology should be set when disabled")
	})

	t.Run("WithTopology", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		d.nodeID = "worker-node-2"
		d.config.Node.Topology.Enabled = true
		d.config.Node.Topology.Zone = "zone-a"
		d.config.Node.Topology.Region = "us-west"

		resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "worker-node-2", resp.NodeId)
		require.NotNil(t, resp.AccessibleTopology)
		assert.Equal(t, "zone-a", resp.AccessibleTopology.Segments["topology.kubernetes.io/zone"])
		assert.Equal(t, "us-west", resp.AccessibleTopology.Segments["topology.kubernetes.io/region"])
	})

	t.Run("WithCustomLabels", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		d.config.Node.Topology.Enabled = true
		d.config.Node.Topology.CustomLabels = map[string]string{
			"custom.topology.io/rack": "rack-1",
		}

		resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})

		require.NoError(t, err)
		require.NotNil(t, resp.AccessibleTopology)
		assert.Equal(t, "rack-1", resp.AccessibleTopology.Segments["custom.topology.io/rack"])
	})
}

func TestNodeStageVolume_Validation(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)

	t.Run("MissingVolumeID", func(t *testing.T) {
		req := &csi.NodeStageVolumeRequest{
			StagingTargetPath: "/var/lib/kubelet/staging/vol1",
			VolumeContext:     map[string]string{"server": "nas", "share": "/data"},
		}

		_, err := d.NodeStageVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "volume ID")
	})

	t.Run("MissingStagingPath", func(t *testing.T) {
		req := &csi.NodeStageVolumeRequest{
			VolumeId:      "vol-1",
			VolumeContext: map[string]string{"server": "nas", "share": "/data"},
		}

		_, err := d.NodeStageVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "staging target path")
	})

	t.Run("MissingVolumeContext", func(t *testing.T) {
		req := &csi.NodeStageVolumeRequest{
			VolumeId:          "vol-1",
			StagingTargetPath: "/var/lib/kubelet/staging/vol1",
		}

		_, err := d.NodeStageVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "volume context")
	})

	t.Run("UnknownProtocolDefaultsToNFS", func(t *testing.T) {
		// Unknown protocols default to NFS (safe default per ParseShareType)
		// So the error will be NFS-specific validation error
		req := &csi.NodeStageVolumeRequest{
			VolumeId:          "vol-1",
			StagingTargetPath: "/tmp/csi-test-staging",
			VolumeContext: map[string]string{
				"node_attach_driver": "unknown-protocol",
				// Missing server and share - should fail NFS validation
			},
		}

		_, err := d.NodeStageVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		// Falls back to NFS and fails NFS validation
		assert.Contains(t, st.Message(), "NFS server and share are required")
	})
}

func TestNodeUnstageVolume_Validation(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)

	t.Run("MissingVolumeID", func(t *testing.T) {
		req := &csi.NodeUnstageVolumeRequest{
			StagingTargetPath: "/var/lib/kubelet/staging/vol1",
		}

		_, err := d.NodeUnstageVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "volume ID")
	})

	t.Run("MissingStagingPath", func(t *testing.T) {
		req := &csi.NodeUnstageVolumeRequest{
			VolumeId: "vol-1",
		}

		_, err := d.NodeUnstageVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "staging target path")
	})
}

func TestNodePublishVolume_Validation(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)

	t.Run("MissingVolumeID", func(t *testing.T) {
		req := &csi.NodePublishVolumeRequest{
			TargetPath:        "/var/lib/kubelet/pods/xxx/volumes/csi",
			StagingTargetPath: "/var/lib/kubelet/staging/vol1",
		}

		_, err := d.NodePublishVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "volume ID")
	})

	t.Run("MissingTargetPath", func(t *testing.T) {
		req := &csi.NodePublishVolumeRequest{
			VolumeId:          "vol-1",
			StagingTargetPath: "/var/lib/kubelet/staging/vol1",
		}

		_, err := d.NodePublishVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "target path")
	})
}

func TestNodeUnpublishVolume_Validation(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)

	t.Run("MissingVolumeID", func(t *testing.T) {
		req := &csi.NodeUnpublishVolumeRequest{
			TargetPath: "/var/lib/kubelet/pods/xxx/volumes/csi",
		}

		_, err := d.NodeUnpublishVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "volume ID")
	})

	t.Run("MissingTargetPath", func(t *testing.T) {
		req := &csi.NodeUnpublishVolumeRequest{
			VolumeId: "vol-1",
		}

		_, err := d.NodeUnpublishVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "target path")
	})
}

func TestNodeGetVolumeStats_Validation(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)

	t.Run("MissingVolumeID", func(t *testing.T) {
		req := &csi.NodeGetVolumeStatsRequest{
			VolumePath: "/var/lib/kubelet/pods/xxx/volumes/csi",
		}

		_, err := d.NodeGetVolumeStats(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "volume ID")
	})

	t.Run("MissingVolumePath", func(t *testing.T) {
		req := &csi.NodeGetVolumeStatsRequest{
			VolumeId: "vol-1",
		}

		_, err := d.NodeGetVolumeStats(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "volume path")
	})

	t.Run("NonExistentPath", func(t *testing.T) {
		req := &csi.NodeGetVolumeStatsRequest{
			VolumeId:   "vol-1",
			VolumePath: "/nonexistent/path/that/should/not/exist",
		}

		_, err := d.NodeGetVolumeStats(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
		assert.Contains(t, st.Message(), "not found")
	})
}

func TestNodeExpandVolume_Validation(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)

	t.Run("MissingVolumeID", func(t *testing.T) {
		req := &csi.NodeExpandVolumeRequest{
			VolumePath: "/var/lib/kubelet/pods/xxx/volumes/csi",
		}

		_, err := d.NodeExpandVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "volume ID")
	})
}

func TestStageNFSVolume_Validation(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)

	t.Run("NilVolumeContext", func(t *testing.T) {
		err := d.stageNFSVolume(context.Background(), nil, "/staging")

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("MissingServer", func(t *testing.T) {
		err := d.stageNFSVolume(context.Background(), map[string]string{
			"share": "/data",
		}, "/staging")

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "server")
	})

	t.Run("MissingShare", func(t *testing.T) {
		err := d.stageNFSVolume(context.Background(), map[string]string{
			"server": "nas.example.com",
		}, "/staging")

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "share")
	})
}

func TestStageISCSIVolume_Validation(t *testing.T) {
	d := newTestNodeDriver(ShareTypeISCSI)

	t.Run("NilVolumeContext", func(t *testing.T) {
		err := d.stageISCSIVolume(context.Background(), nil, "/staging", nil)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("MissingPortal", func(t *testing.T) {
		err := d.stageISCSIVolume(context.Background(), map[string]string{
			"iqn": "iqn.2005-10.org.freenas.ctl:vol1",
		}, "/staging", nil)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "portal")
	})

	t.Run("MissingIQN", func(t *testing.T) {
		err := d.stageISCSIVolume(context.Background(), map[string]string{
			"portal": "192.168.1.100:3260",
		}, "/staging", nil)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "IQN")
	})

	t.Run("InvalidLUN", func(t *testing.T) {
		err := d.stageISCSIVolume(context.Background(), map[string]string{
			"portal": "192.168.1.100:3260",
			"iqn":    "iqn.2005-10.org.freenas.ctl:vol1",
			"lun":    "invalid",
		}, "/staging", nil)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "LUN")
	})
}

func TestStageNVMeoFVolume_Validation(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNVMeoF)

	t.Run("NilVolumeContext", func(t *testing.T) {
		err := d.stageNVMeoFVolume(context.Background(), nil, "/staging", nil)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("MissingNQN", func(t *testing.T) {
		err := d.stageNVMeoFVolume(context.Background(), map[string]string{
			"address": "192.168.1.100",
		}, "/staging", nil)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "NQN")
	})

	t.Run("MissingAddress", func(t *testing.T) {
		err := d.stageNVMeoFVolume(context.Background(), map[string]string{
			"nqn": "nqn.2011-06.com.truenas:vol1",
		}, "/staging", nil)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "address")
	})
}

func TestCreateSymlinkAtomic(t *testing.T) {
	t.Run("NonExistentTarget", func(t *testing.T) {
		// Create a symlink to a non-existent file (allowed by symlink semantics)
		tmpDir := t.TempDir()
		linkPath := tmpDir + "/testlink"
		target := "/dev/nonexistent"

		err := createSymlinkAtomic(target, linkPath)
		require.NoError(t, err)

		// Verify symlink was created
		actualTarget, err := readSymlink(linkPath)
		require.NoError(t, err)
		assert.Equal(t, target, actualTarget)
	})

	t.Run("SymlinkAlreadyCorrect", func(t *testing.T) {
		tmpDir := t.TempDir()
		linkPath := tmpDir + "/testlink"
		target := "/dev/test"

		// Create initial symlink
		err := createSymlinkAtomic(target, linkPath)
		require.NoError(t, err)

		// Call again with same target - should succeed
		err = createSymlinkAtomic(target, linkPath)
		require.NoError(t, err)

		// Verify symlink is still correct
		actualTarget, err := readSymlink(linkPath)
		require.NoError(t, err)
		assert.Equal(t, target, actualTarget)
	})

	t.Run("SymlinkPointsToWrongTarget", func(t *testing.T) {
		tmpDir := t.TempDir()
		linkPath := tmpDir + "/testlink"
		oldTarget := "/dev/old"
		newTarget := "/dev/new"

		// Create initial symlink pointing to old target
		err := createSymlinkAtomic(oldTarget, linkPath)
		require.NoError(t, err)

		// Update to new target
		err = createSymlinkAtomic(newTarget, linkPath)
		require.NoError(t, err)

		// Verify symlink now points to new target
		actualTarget, err := readSymlink(linkPath)
		require.NoError(t, err)
		assert.Equal(t, newTarget, actualTarget)
	})
}

// readSymlink is a helper to read symlink target
func readSymlink(path string) (string, error) {
	return os.Readlink(path)
}

func TestOperationLocking(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)

	t.Run("AcquireAndRelease", func(t *testing.T) {
		key := "test-lock-1"

		// First acquire should succeed
		assert.True(t, d.acquireOperationLock(key))

		// Second acquire should fail
		assert.False(t, d.acquireOperationLock(key))

		// Release
		d.releaseOperationLock(key)

		// Now acquire should succeed again
		assert.True(t, d.acquireOperationLock(key))
		d.releaseOperationLock(key)
	})

	t.Run("DifferentKeysIndependent", func(t *testing.T) {
		key1 := "test-lock-a"
		key2 := "test-lock-b"

		assert.True(t, d.acquireOperationLock(key1))
		assert.True(t, d.acquireOperationLock(key2))

		d.releaseOperationLock(key1)
		d.releaseOperationLock(key2)
	})
}

func TestCleanupOrphanedSessionByVolumeID(t *testing.T) {
	// This tests the method exists and doesn't panic with various configs
	// Actual cleanup is hard to test without mocking util functions

	t.Run("NFSDriver_NoOp", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)

		// Should be a no-op for NFS (no sessions to clean)
		d.cleanupOrphanedSessionByVolumeID(context.Background(), "test-vol")
	})

	t.Run("ISCSIDriver", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeISCSI)
		d.config.ISCSI.NameSuffix = "-cluster"

		// Should attempt to find and clean iSCSI session
		// Will log "no active session found" since no real sessions exist
		d.cleanupOrphanedSessionByVolumeID(context.Background(), "test-vol")
	})

	t.Run("NVMeoFDriver", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNVMeoF)
		d.config.NVMeoF.NamePrefix = "k8s-"
		d.config.NVMeoF.NameSuffix = "-prod"

		// Should attempt to find and clean NVMe-oF session
		d.cleanupOrphanedSessionByVolumeID(context.Background(), "test-vol")
	})
}

func TestNodeStageVolume_ConcurrentProtection(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)
	volumeID := "concurrent-test-vol"

	// Simulate first operation holding the lock
	lockKey := "node-stage:" + volumeID
	acquired := d.acquireOperationLock(lockKey)
	require.True(t, acquired)
	defer d.releaseOperationLock(lockKey)

	// Second attempt should get Aborted
	req := &csi.NodeStageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: "/staging",
		VolumeContext:     map[string]string{"server": "nas", "share": "/data"},
	}

	_, err := d.NodeStageVolume(context.Background(), req)

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Aborted, st.Code())
	assert.Contains(t, st.Message(), "operation already in progress")
}

func TestNodePublishVolume_ConcurrentProtection(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)
	volumeID := "concurrent-publish-vol"

	// Simulate first operation holding the lock
	lockKey := "node-publish:" + volumeID
	acquired := d.acquireOperationLock(lockKey)
	require.True(t, acquired)
	defer d.releaseOperationLock(lockKey)

	req := &csi.NodePublishVolumeRequest{
		VolumeId:          volumeID,
		TargetPath:        "/target",
		StagingTargetPath: "/staging",
	}

	_, err := d.NodePublishVolume(context.Background(), req)

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Aborted, st.Code())
}

func TestNodeUnstageVolume_ConcurrentProtection(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)
	volumeID := "concurrent-unstage-vol"

	// Simulate first operation holding the lock
	lockKey := "node-unstage:" + volumeID
	acquired := d.acquireOperationLock(lockKey)
	require.True(t, acquired)
	defer d.releaseOperationLock(lockKey)

	req := &csi.NodeUnstageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: "/staging",
	}

	_, err := d.NodeUnstageVolume(context.Background(), req)

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Aborted, st.Code())
}

func TestNodeUnpublishVolume_ConcurrentProtection(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)
	volumeID := "concurrent-unpublish-vol"

	// Simulate first operation holding the lock
	lockKey := "node-unpublish:" + volumeID
	acquired := d.acquireOperationLock(lockKey)
	require.True(t, acquired)
	defer d.releaseOperationLock(lockKey)

	req := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: "/target",
	}

	_, err := d.NodeUnpublishVolume(context.Background(), req)

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Aborted, st.Code())
}
