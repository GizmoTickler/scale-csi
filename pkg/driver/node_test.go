package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"github.com/GizmoTickler/scale-csi/pkg/util"
)

const fakeNodeCommandScript = `#!/bin/sh
name="$(basename "$0")"
if [ -n "$FAKE_NODE_COMMAND_LOG" ]; then
	printf '%s %s\n' "$name" "$*" >> "$FAKE_NODE_COMMAND_LOG"
fi
case "$name" in
	findmnt)
		if [ -n "$FAKE_NODE_MOUNT_STATE_FILE" ] && [ -f "$FAKE_NODE_MOUNT_STATE_FILE" ]; then
			case " $* " in
				*" SOURCE,FSTYPE,OPTIONS "*)
					if [ -n "$FAKE_NODE_FINDMNT_INFO" ]; then
						printf '%s' "$FAKE_NODE_FINDMNT_INFO"
					elif [ -f "$FAKE_NODE_MOUNT_STATE_FILE.info" ]; then
						cat "$FAKE_NODE_MOUNT_STATE_FILE.info"
					else
						printf '%s' "${FAKE_NODE_FINDMNT_OUTPUT:-/dev/null} ext4 rw"
					fi
					exit 0
					;;
			esac
			printf '%s' "${FAKE_NODE_FINDMNT_OUTPUT:-mounted}"
			exit 0
		fi
		if [ -n "$FAKE_NODE_FINDMNT_OUTPUT" ]; then
			printf '%s' "$FAKE_NODE_FINDMNT_OUTPUT"
			exit 0
		fi
		exit 1
		;;
	blkid)
		if [ -n "$FAKE_NODE_BLKID_OUTPUT" ]; then
			printf '%s' "$FAKE_NODE_BLKID_OUTPUT"
			exit 0
		fi
		exit 2
		;;
	nvme)
		if [ "$1 $2" = "list-subsys -o" ] && [ -n "$FAKE_NODE_NVME_LIST_SUBSYS_OUTPUT" ]; then
			printf '%s' "$FAKE_NODE_NVME_LIST_SUBSYS_OUTPUT"
		fi
		exit 0
		;;
	iscsiadm)
		if [ "$1 $2" = "-m session" ] && [ -n "$FAKE_NODE_ISCSI_SESSION_OUTPUT" ]; then
			printf '%s' "$FAKE_NODE_ISCSI_SESSION_OUTPUT"
			exit 0
		fi
		exit 97
		;;
	mount)
		if [ -n "$FAKE_NODE_MOUNT_STATE_FILE" ]; then
			: > "$FAKE_NODE_MOUNT_STATE_FILE"
			previous=""
			source=""
			fstype="none"
			options="rw"
			next_fstype=false
			next_options=false
			mount_options=""
			for arg in "$@"; do
				if [ "$next_fstype" = true ]; then fstype="$arg"; next_fstype=false; continue; fi
				if [ "$next_options" = true ]; then mount_options="$arg"; next_options=false; continue; fi
				if [ "$arg" = "-t" ]; then next_fstype=true; continue; fi
				if [ "$arg" = "-o" ]; then next_options=true; continue; fi
				previous="$source"
				source="$arg"
			done
			case ",$mount_options," in *,ro,*) options="ro" ;; esac
			if [ "$source" = "$last" ]; then source="$previous"; fi
			if printf '%s' "$mount_options" | grep -q 'remount'; then
				if [ -f "$FAKE_NODE_MOUNT_STATE_FILE.info" ]; then
					source="$(awk '{print $1}' "$FAKE_NODE_MOUNT_STATE_FILE.info")"
					fstype="$(awk '{print $2}' "$FAKE_NODE_MOUNT_STATE_FILE.info")"
				fi
			fi
			printf '%s %s %s' "$source" "$fstype" "$options" > "$FAKE_NODE_MOUNT_STATE_FILE.info"
		fi
		exit 0
		;;
	umount)
		if [ -n "$FAKE_NODE_MOUNT_STATE_FILE" ]; then
			rm -f "$FAKE_NODE_MOUNT_STATE_FILE"
			rm -f "$FAKE_NODE_MOUNT_STATE_FILE.info"
		fi
		exit 0
		;;
	*) exit 0 ;;
esac
`

func installFakeNodeCommands(t *testing.T, commands ...string) string {
	t.Helper()
	binDir := t.TempDir()
	for _, command := range commands {
		path := filepath.Join(binDir, command)
		require.NoError(t, os.WriteFile(path, []byte(fakeNodeCommandScript), 0o750))
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return binDir
}

func readNodeCommandLog(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return ""
	}
	require.NoError(t, err)
	return string(content)
}

// newTestNodeDriver creates a minimal driver configured for node testing.
func newTestNodeDriver(shareType ShareType) *Driver {
	cfg := &Config{
		DriverName: "org.scale.csi.nfs",
		ZFS: ZFSConfig{
			DatasetParentName: "pool/test",
		},
		NFS: NFSConfig{
			ShareHost: "192.0.2.100",
		},
		ISCSI: ISCSIConfig{
			TargetPortal:      "192.0.2.100:3260",
			DeviceWaitTimeout: 60,
		},
		NVMeoF: NVMeoFConfig{
			TransportAddress:   "192.0.2.100",
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

	encodedNodeID, err := encodeNodeIdentity(NodeIdentity{Name: "test-node-1"})
	if err != nil {
		panic(err)
	}
	return &Driver{
		name:          cfg.DriverName,
		version:       "test",
		nodeID:        "test-node-1",
		encodedNodeID: encodedNodeID,
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
		d.encodedNodeID, _ = encodeNodeIdentity(NodeIdentity{Name: d.nodeID})

		resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})

		require.NoError(t, err)
		require.NotNil(t, resp)
		identity, parseErr := parseNodeIdentity(resp.NodeId)
		require.NoError(t, parseErr)
		assert.Equal(t, "worker-node-1", identity.Name)
		assert.Nil(t, resp.AccessibleTopology, "no topology should be set when disabled")
		assert.Zero(t, resp.MaxVolumesPerNode, "zero should preserve the unlimited/unset behavior")
	})

	t.Run("WithMaxVolumesPerNode", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeISCSI)
		d.config.Node.MaxVolumesPerNode = 32

		resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})

		require.NoError(t, err)
		assert.Equal(t, int64(32), resp.MaxVolumesPerNode)
	})

	t.Run("WithTopology", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		d.nodeID = "worker-node-2"
		d.encodedNodeID, _ = encodeNodeIdentity(NodeIdentity{Name: d.nodeID})
		d.config.Node.Topology.Enabled = true
		d.config.Node.Topology.Zone = "zone-a"
		d.config.Node.Topology.Region = "us-west"

		resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})

		require.NoError(t, err)
		require.NotNil(t, resp)
		identity, parseErr := parseNodeIdentity(resp.NodeId)
		require.NoError(t, parseErr)
		assert.Equal(t, "worker-node-2", identity.Name)
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
			VolumeCapability:  testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
		}

		_, err := d.NodeStageVolume(context.Background(), req)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "volume context")
	})

	t.Run("MissingVolumeCapability", func(t *testing.T) {
		req := &csi.NodeStageVolumeRequest{
			VolumeId:          "vol-1",
			StagingTargetPath: t.TempDir(),
			VolumeContext:     map[string]string{"server": "nas", "share": "/data"},
		}

		_, err := d.NodeStageVolume(context.Background(), req)

		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "volume capability")
	})

	t.Run("UnknownProtocolDefaultsToNFS", func(t *testing.T) {
		// Unknown protocols default to NFS (safe default per ParseShareType)
		// So the error will be NFS-specific validation error
		req := &csi.NodeStageVolumeRequest{
			VolumeId:          "vol-1",
			StagingTargetPath: "/tmp/csi-test-staging",
			VolumeCapability:  testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
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

func TestNodeStageUnstageVolume_BlockRoundTrip(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "iscsiadm")
	originalConnect := iscsiConnectWithSessions
	originalGetInfo := nodeGetISCSIInfo
	t.Cleanup(func() {
		iscsiConnectWithSessions = originalConnect
		nodeGetISCSIInfo = originalGetInfo
	})
	iscsiConnectWithSessions = func(context.Context, string, string, int, *util.ISCSIConnectOptions, []util.ISCSISessionInfo) (string, error) {
		return "/dev/null", nil
	}
	nodeGetISCSIInfo = func(string) (string, string, error) {
		return "192.0.2.10:3260", "iqn.test:block-volume", nil
	}

	d := newTestNodeDriver(ShareTypeISCSI)
	stagingPath := filepath.Join(t.TempDir(), "staging", "volume-device")
	// Kubelet pre-creates staging_target_path as a directory before invoking
	// NodeStageVolume, including for raw-block volumes.
	require.NoError(t, os.MkdirAll(stagingPath, 0o750))
	req := &csi.NodeStageVolumeRequest{
		VolumeId:          "block-volume",
		StagingTargetPath: stagingPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
		VolumeContext: map[string]string{
			"node_attach_driver": "iscsi",
			"portal":             "192.0.2.10:3260",
			"iqn":                "iqn.test:block-volume",
			"lun":                "0",
		},
	}

	_, err := d.NodeStageVolume(context.Background(), req)
	require.NoError(t, err)
	info, err := os.Lstat(stagingPath)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink, "block staging path must be a symlink, not a directory")
	target, err := os.Readlink(stagingPath)
	require.NoError(t, err)
	assert.Equal(t, "/dev/null", target)

	// Re-staging an already-correct symlink must be an idempotent no-op.
	_, err = d.NodeStageVolume(context.Background(), req)
	require.NoError(t, err)
	restagedInfo, err := os.Lstat(stagingPath)
	require.NoError(t, err)
	assert.True(t, os.SameFile(info, restagedInfo), "correct staging symlink must not be replaced")

	_, err = d.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId: "block-volume", StagingTargetPath: stagingPath,
	})
	require.NoError(t, err)
	_, err = os.Lstat(stagingPath)
	assert.True(t, os.IsNotExist(err), "unstage must remove the block staging symlink")
}

func TestNodeStageVolume_BlockRejectsNonEmptyStagingDirectory(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "iscsiadm")
	originalConnect := iscsiConnectWithSessions
	t.Cleanup(func() { iscsiConnectWithSessions = originalConnect })
	iscsiConnectWithSessions = func(context.Context, string, string, int, *util.ISCSIConnectOptions, []util.ISCSISessionInfo) (string, error) {
		return "/dev/null", nil
	}

	d := newTestNodeDriver(ShareTypeISCSI)
	stagingPath := filepath.Join(t.TempDir(), "staging", "volume-device")
	require.NoError(t, os.MkdirAll(stagingPath, 0o750))
	sentinelPath := filepath.Join(stagingPath, "kubelet-data")
	require.NoError(t, os.WriteFile(sentinelPath, []byte("preserve me"), 0o600))

	_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "block-volume",
		StagingTargetPath: stagingPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
		VolumeContext: map[string]string{
			"node_attach_driver": "iscsi",
			"portal":             "192.0.2.10:3260",
			"iqn":                "iqn.test:block-volume",
			"lun":                "0",
		},
	})

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "failed to remove existing directory")
	assert.Contains(t, err.Error(), "directory must be empty")
	content, readErr := os.ReadFile(sentinelPath)
	require.NoError(t, readErr)
	assert.Equal(t, "preserve me", string(content), "non-empty staging directory contents must not be deleted")
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

func TestNodePublishVolumeExistingTargetCompatibility(t *testing.T) {
	originalIsMounted := nodeIsMounted
	originalGetMountInfo := nodeGetMountInfo
	originalListMountInfo := nodeListMountInfo
	t.Cleanup(func() {
		nodeIsMounted = originalIsMounted
		nodeGetMountInfo = originalGetMountInfo
		nodeListMountInfo = originalListMountInfo
	})
	nodeIsMounted = func(string) (bool, error) { return true, nil }
	nodeListMountInfo = func() ([]util.MountInfo, error) { return nil, nil }

	mountCapability := func(mode csi.VolumeCapability_AccessMode_Mode) *csi.VolumeCapability {
		return &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
		}
	}

	t.Run("CompatibleRepeatedPublishSucceeds", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		stagingPath := t.TempDir()
		targetPath := t.TempDir()
		nodeGetMountInfo = func(path string) (util.MountInfo, error) {
			return util.MountInfo{Source: "server:/share", Target: path, FSType: "ext4", ReadOnly: false}, nil
		}
		req := &csi.NodePublishVolumeRequest{
			VolumeId:          "volume-a",
			StagingTargetPath: stagingPath,
			TargetPath:        targetPath,
			VolumeCapability:  mountCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER),
		}

		_, err := d.NodePublishVolume(context.Background(), req)
		require.NoError(t, err)
		_, err = d.NodePublishVolume(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("DifferentBackingSourceReturnsAlreadyExists", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		stagingPath := t.TempDir()
		targetPath := t.TempDir()
		nodeGetMountInfo = func(path string) (util.MountInfo, error) {
			source := "server:/requested"
			if path == targetPath {
				source = "server:/existing"
			}
			return util.MountInfo{Source: source, Target: path, FSType: "ext4"}, nil
		}

		_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
			VolumeId:          "volume-a",
			StagingTargetPath: stagingPath,
			TargetPath:        targetPath,
			VolumeCapability:  mountCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER),
		})
		require.Error(t, err)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
	})

	t.Run("DifferentReadonlyStateReturnsAlreadyExists", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		stagingPath := t.TempDir()
		targetPath := t.TempDir()
		nodeGetMountInfo = func(path string) (util.MountInfo, error) {
			return util.MountInfo{Source: "server:/share", Target: path, FSType: "ext4", ReadOnly: false}, nil
		}

		_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
			VolumeId:          "volume-a",
			StagingTargetPath: stagingPath,
			TargetPath:        targetPath,
			Readonly:          true,
			VolumeCapability:  mountCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER),
		})
		require.Error(t, err)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
	})

	t.Run("DifferentCapabilityReturnsAlreadyExists", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		stagingPath := t.TempDir()
		targetPath := t.TempDir()
		nodeGetMountInfo = func(path string) (util.MountInfo, error) {
			return util.MountInfo{Source: "server:/share", Target: path, FSType: "ext4"}, nil
		}
		first := &csi.NodePublishVolumeRequest{
			VolumeId:          "volume-a",
			StagingTargetPath: stagingPath,
			TargetPath:        targetPath,
			VolumeCapability:  mountCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER),
		}
		_, err := d.NodePublishVolume(context.Background(), first)
		require.NoError(t, err)

		second := proto.Clone(first).(*csi.NodePublishVolumeRequest)
		second.VolumeCapability = mountCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY)
		_, err = d.NodePublishVolume(context.Background(), second)
		require.Error(t, err)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
	})

	t.Run("DifferentVolumeReturnsAlreadyExists", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		stagingPath := t.TempDir()
		targetPath := t.TempDir()
		nodeGetMountInfo = func(path string) (util.MountInfo, error) {
			return util.MountInfo{Source: "server:/share", Target: path, FSType: "ext4"}, nil
		}
		first := &csi.NodePublishVolumeRequest{
			VolumeId:          "volume-a",
			StagingTargetPath: stagingPath,
			TargetPath:        targetPath,
			VolumeCapability:  mountCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER),
		}
		_, err := d.NodePublishVolume(context.Background(), first)
		require.NoError(t, err)

		second := proto.Clone(first).(*csi.NodePublishVolumeRequest)
		second.VolumeId = "volume-b"
		_, err = d.NodePublishVolume(context.Background(), second)
		require.Error(t, err)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
	})

	t.Run("DifferentAccessTypeReturnsAlreadyExists", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeISCSI)
		stagingPath := filepath.Join(t.TempDir(), "staged-device")
		require.NoError(t, os.Symlink("/dev/null", stagingPath))
		targetPath := t.TempDir()
		nodeGetMountInfo = func(path string) (util.MountInfo, error) {
			return util.MountInfo{Source: "/dev/null", Target: path, FSType: "devtmpfs"}, nil
		}

		_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
			VolumeId:          "volume-a",
			StagingTargetPath: stagingPath,
			TargetPath:        targetPath,
			VolumeCapability: &csi.VolumeCapability{
				AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER},
			},
		})
		require.Error(t, err)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
	})
}

func TestNodePublishVolumeSingleWriterRejectsDifferentTarget(t *testing.T) {
	originalIsMounted := nodeIsMounted
	originalGetMountInfo := nodeGetMountInfo
	originalListMountInfo := nodeListMountInfo
	t.Cleanup(func() {
		nodeIsMounted = originalIsMounted
		nodeGetMountInfo = originalGetMountInfo
		nodeListMountInfo = originalListMountInfo
	})

	d := newTestNodeDriver(ShareTypeNFS)
	stagingPath := t.TempDir()
	existingTarget := t.TempDir()
	newTarget := filepath.Join(t.TempDir(), "new-target")
	capability := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER},
	}
	signature, err := nodeCapabilityForRequest(capability)
	require.NoError(t, err)
	d.storePublicationRecord(nodeMountRecord{
		VolumeID:       "volume-a",
		TargetPath:     existingTarget,
		ExpectedSource: "server:/share",
		LiveSource:     "server:/share",
		Capability:     signature,
	})
	nodeIsMounted = func(path string) (bool, error) { return path == existingTarget, nil }
	nodeGetMountInfo = func(path string) (util.MountInfo, error) {
		return util.MountInfo{Source: "server:/share", Target: path, FSType: "nfs4"}, nil
	}
	nodeListMountInfo = func() ([]util.MountInfo, error) { return nil, nil }

	_, err = d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "volume-a",
		StagingTargetPath: stagingPath,
		TargetPath:        newTarget,
		VolumeCapability:  capability,
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), existingTarget)
}

func TestNodePublishVolumeRebuildsSingleWriterStateFromMountTable(t *testing.T) {
	originalIsMounted := nodeIsMounted
	originalGetMountInfo := nodeGetMountInfo
	originalListMountInfo := nodeListMountInfo
	t.Cleanup(func() {
		nodeIsMounted = originalIsMounted
		nodeGetMountInfo = originalGetMountInfo
		nodeListMountInfo = originalListMountInfo
	})

	d := newTestNodeDriver(ShareTypeNFS)
	stagingPath := t.TempDir()
	newTarget := filepath.Join(t.TempDir(), "new-target")
	existingTarget := "/var/lib/kubelet/pods/existing/volumes/kubernetes.io~csi/pvc/mount"
	nodeIsMounted = func(string) (bool, error) { return false, nil }
	nodeGetMountInfo = func(path string) (util.MountInfo, error) {
		return util.MountInfo{Source: "server:/share", Target: path, FSType: "nfs4"}, nil
	}
	nodeListMountInfo = func() ([]util.MountInfo, error) {
		return []util.MountInfo{{Source: "server:/share", Target: existingTarget, FSType: "nfs4"}}, nil
	}

	_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "volume-a",
		StagingTargetPath: stagingPath,
		TargetPath:        newTarget,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER},
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), existingTarget)
}

func TestNodeStageVolumeExistingTargetCompatibility(t *testing.T) {
	originalIsMounted := nodeIsMounted
	originalGetMountInfo := nodeGetMountInfo
	t.Cleanup(func() {
		nodeIsMounted = originalIsMounted
		nodeGetMountInfo = originalGetMountInfo
	})
	nodeIsMounted = func(string) (bool, error) { return true, nil }

	newRequest := func(stagingPath string) *csi.NodeStageVolumeRequest {
		return &csi.NodeStageVolumeRequest{
			VolumeId:          "volume-a",
			StagingTargetPath: stagingPath,
			VolumeContext: map[string]string{
				"node_attach_driver": "nfs",
				"server":             "server",
				"share":              "/share",
			},
			VolumeCapability: &csi.VolumeCapability{
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER},
			},
		}
	}

	t.Run("CompatibleRepeatedStageSucceeds", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		stagingPath := t.TempDir()
		nodeGetMountInfo = func(path string) (util.MountInfo, error) {
			return util.MountInfo{Source: "server:/share", Target: path, FSType: "nfs4"}, nil
		}
		req := newRequest(stagingPath)

		_, err := d.NodeStageVolume(context.Background(), req)
		require.NoError(t, err)
		_, err = d.NodeStageVolume(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("DifferentBackingSourceReturnsAlreadyExists", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		stagingPath := t.TempDir()
		nodeGetMountInfo = func(path string) (util.MountInfo, error) {
			return util.MountInfo{Source: "server:/other", Target: path, FSType: "nfs4"}, nil
		}

		_, err := d.NodeStageVolume(context.Background(), newRequest(stagingPath))
		require.Error(t, err)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
	})

	t.Run("DifferentVolumeReturnsAlreadyExists", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		stagingPath := t.TempDir()
		nodeGetMountInfo = func(path string) (util.MountInfo, error) {
			return util.MountInfo{Source: "server:/share", Target: path, FSType: "nfs4"}, nil
		}
		first := newRequest(stagingPath)
		_, err := d.NodeStageVolume(context.Background(), first)
		require.NoError(t, err)

		second := proto.Clone(first).(*csi.NodeStageVolumeRequest)
		second.VolumeId = "volume-b"
		_, err = d.NodeStageVolume(context.Background(), second)
		require.Error(t, err)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
	})

	t.Run("DifferentAccessTypeReturnsAlreadyExists", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNFS)
		stagingPath := t.TempDir()
		nodeGetMountInfo = func(path string) (util.MountInfo, error) {
			return util.MountInfo{Source: "server:/share", Target: path, FSType: "nfs4"}, nil
		}
		req := newRequest(stagingPath)
		req.VolumeCapability.AccessType = &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}

		_, err := d.NodeStageVolume(context.Background(), req)
		require.Error(t, err)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
	})
}

func TestNodePublishUnpublishVolume_BlockRoundTrip(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "mount", "umount")
	originalGetInfo := nodeGetISCSIInfo
	defer func() { nodeGetISCSIInfo = originalGetInfo }()
	nodeGetISCSIInfo = func(string) (string, string, error) {
		return "192.0.2.10:3260", "iqn.2026-07.example:block-vol", nil
	}
	logPath := filepath.Join(t.TempDir(), "commands.log")
	mountStatePath := filepath.Join(t.TempDir(), "mounted")
	t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
	t.Setenv("FAKE_NODE_MOUNT_STATE_FILE", mountStatePath)

	stagingPath := filepath.Join(t.TempDir(), "staged-device")
	require.NoError(t, os.Symlink("/dev/null", stagingPath))
	targetPath := filepath.Join(t.TempDir(), "pod", "volume")
	d := newTestNodeDriver(ShareTypeISCSI)
	req := &csi.NodePublishVolumeRequest{
		VolumeId:          "block-vol",
		StagingTargetPath: stagingPath,
		TargetPath:        targetPath,
		Readonly:          true,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		},
	}

	_, err := d.NodePublishVolume(context.Background(), req)
	require.NoError(t, err)
	info, err := os.Stat(targetPath)
	require.NoError(t, err)
	assert.True(t, info.Mode().IsRegular())
	assert.Equal(t, os.FileMode(0o640), info.Mode().Perm())

	_, err = d.NodePublishVolume(context.Background(), req)
	require.NoError(t, err)
	commands := readNodeCommandLog(t, logPath)
	assert.Equal(t, 1, strings.Count(commands, "mount -o bind,ro /dev/null "+targetPath))
	assert.Equal(t, 1, strings.Count(commands, "mount -o remount,bind,ro "+targetPath))

	_, err = d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "block-vol",
		TargetPath: targetPath,
	})
	require.NoError(t, err)
	_, err = os.Lstat(targetPath)
	assert.True(t, os.IsNotExist(err))
	assert.Contains(t, readNodeCommandLog(t, logPath), "umount "+targetPath)
}

func TestNodePublishVolumeRawBlockRejectsWrongDeviceOwner(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "mount")
	originalGetInfo := nodeGetISCSIInfo
	defer func() { nodeGetISCSIInfo = originalGetInfo }()
	nodeGetISCSIInfo = func(string) (string, string, error) {
		return "192.0.2.10:3260", "iqn.2026-07.example:another-volume", nil
	}
	logPath := filepath.Join(t.TempDir(), "commands.log")
	t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)

	stagingPath := filepath.Join(t.TempDir(), "staged-device")
	require.NoError(t, os.Symlink("/dev/null", stagingPath))
	targetPath := filepath.Join(t.TempDir(), "pod", "volume")
	d := newTestNodeDriver(ShareTypeISCSI)

	_, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "block-vol",
		StagingTargetPath: stagingPath,
		TargetPath:        targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "another-volume")
	assert.Contains(t, err.Error(), "block-vol")
	assert.NotContains(t, readNodeCommandLog(t, logPath), "mount -o bind")
	_, statErr := os.Lstat(targetPath)
	assert.True(t, os.IsNotExist(statErr), "ownership must be checked before creating the target file")
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

		resp, err := d.NodeGetVolumeStats(context.Background(), req)

		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestNodeGetVolumeStats_BlockMode(t *testing.T) {
	originalResolver := resolveNodeStatsDevice
	originalDeviceSize := getNodeDeviceSize
	originalStat := nodeStatsStat
	originalSysfsRoot := nodeStatsSysfsRoot
	t.Cleanup(func() {
		resolveNodeStatsDevice = originalResolver
		getNodeDeviceSize = originalDeviceSize
		nodeStatsStat = originalStat
		nodeStatsSysfsRoot = originalSysfsRoot
	})
	resolveNodeStatsDevice = nodeStatsDevice
	getNodeDeviceSize = nodeStatsDeviceSize

	volumePath := filepath.Join(t.TempDir(), "published-block-device")
	require.NoError(t, os.WriteFile(volumePath, nil, 0o640))
	nodeStatsStat = func(path string) (uint32, uint64, error) {
		assert.Equal(t, volumePath, path)
		return unix.S_IFBLK, unix.Mkdev(8, 1), nil
	}
	nodeStatsSysfsRoot = t.TempDir()
	sizeDir := filepath.Join(nodeStatsSysfsRoot, "dev", "block", "8:1")
	require.NoError(t, os.MkdirAll(sizeDir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(sizeDir, "size"), []byte("16777216\n"), 0o640))

	d := newTestNodeDriver(ShareTypeISCSI)
	resp, err := d.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId: "block-vol", VolumePath: volumePath,
	})
	require.NoError(t, err)
	require.Len(t, resp.Usage, 1)
	assert.Equal(t, int64(8<<30), resp.Usage[0].Total)
	assert.Equal(t, csi.VolumeUsage_BYTES, resp.Usage[0].Unit)
	require.NotNil(t, resp.VolumeCondition)
	assert.False(t, resp.VolumeCondition.Abnormal)
}

func TestNodeStatsDeviceSizeRejectsSectorOverflow(t *testing.T) {
	originalSysfsRoot := nodeStatsSysfsRoot
	t.Cleanup(func() { nodeStatsSysfsRoot = originalSysfsRoot })
	nodeStatsSysfsRoot = t.TempDir()
	sizeDir := filepath.Join(nodeStatsSysfsRoot, "dev", "block", "8:1")
	require.NoError(t, os.MkdirAll(sizeDir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(sizeDir, "size"), []byte("9223372036854775807\n"), 0o640))

	size, err := nodeStatsDeviceSize("8:1")
	require.Error(t, err)
	assert.Zero(t, size)
	assert.Contains(t, err.Error(), "exceeds int64 bytes")
}

func TestNodeGetVolumeStats_FilesystemModeUnchanged(t *testing.T) {
	originalResolver := resolveNodeStatsDevice
	originalFilesystemStats := getNodeFilesystemStats
	t.Cleanup(func() {
		resolveNodeStatsDevice = originalResolver
		getNodeFilesystemStats = originalFilesystemStats
	})
	resolveNodeStatsDevice = func(string) (string, bool, error) { return "", false, nil }
	getNodeFilesystemStats = func(string) (*util.FilesystemStats, error) {
		return &util.FilesystemStats{
			TotalBytes: 100, AvailableBytes: 40, UsedBytes: 60,
			TotalInodes: 10, AvailableInodes: 4, UsedInodes: 6,
		}, nil
	}

	d := newTestNodeDriver(ShareTypeNFS)
	resp, err := d.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId: "fs-vol", VolumePath: "/pods/fs-vol",
	})
	require.NoError(t, err)
	require.Len(t, resp.Usage, 2)
	assert.Equal(t, int64(100), resp.Usage[0].Total)
	assert.Equal(t, int64(10), resp.Usage[1].Total)
	assert.False(t, resp.VolumeCondition.Abnormal)
}

func TestNodeGetVolumeStats_StatFailureIsAbnormal(t *testing.T) {
	originalResolver := resolveNodeStatsDevice
	t.Cleanup(func() { resolveNodeStatsDevice = originalResolver })
	resolveNodeStatsDevice = func(string) (string, bool, error) {
		return "", false, errors.New("injected stat failure")
	}

	d := newTestNodeDriver(ShareTypeNFS)
	resp, err := d.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
		VolumeId: "fs-vol", VolumePath: "/pods/fs-vol",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.VolumeCondition)
	assert.True(t, resp.VolumeCondition.Abnormal)
	assert.Contains(t, resp.VolumeCondition.Message, "injected stat failure")
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

	t.Run("MissingVolumePath", func(t *testing.T) {
		_, err := d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{VolumeId: "vol-1"})

		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "volume path")
	})

	t.Run("MissingPathOnHost", func(t *testing.T) {
		_, err := d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
			VolumeId:   "vol-1",
			VolumePath: filepath.Join(t.TempDir(), "missing"),
		})

		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestNodeExpandVolumeRescansBeforeFilesystemResize(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "nvme", "blkid", "resize2fs")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
	t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/nvme3n7\n")
	t.Setenv("FAKE_NODE_BLKID_OUTPUT", "ext4\n")

	d := newTestNodeDriver(ShareTypeNFS) // Device detection must support mixed protocols.
	originalDeviceSize := nodeGetDeviceSize
	t.Cleanup(func() { nodeGetDeviceSize = originalDeviceSize })
	sizeReads := 0
	nodeGetDeviceSize = func(string) (int64, error) {
		sizeReads++
		if sizeReads == 1 {
			return 1 << 30, nil
		}
		return 3 << 30, nil
	}
	volumePath := t.TempDir()
	stagingPath := t.TempDir()
	resp, err := d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:          "vol-1",
		VolumePath:        volumePath,
		StagingTargetPath: stagingPath,
		CapacityRange:     &csi.CapacityRange{RequiredBytes: 2 << 30},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3<<30), resp.CapacityBytes, "the response must report the actual device capacity")

	commands := readNodeCommandLog(t, logPath)
	rescanIndex := strings.Index(commands, "nvme ns-rescan /dev/nvme3")
	resizeIndex := strings.Index(commands, "resize2fs /dev/nvme3n7")
	require.GreaterOrEqual(t, rescanIndex, 0)
	require.Greater(t, resizeIndex, rescanIndex)
}

func TestNodeExpandVolumeRawBlockRescansWithoutFilesystemResize(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "nvme", "blkid", "resize2fs", "xfs_growfs", "btrfs")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
	t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/nvme3n7\n")

	d := newTestNodeDriver(ShareTypeNFS)
	originalDeviceSize := nodeGetDeviceSize
	originalGetNVMeInfo := nodeGetNVMeInfo
	originalPollInterval := nodeDeviceSizePollInterval
	t.Cleanup(func() {
		nodeGetDeviceSize = originalDeviceSize
		nodeGetNVMeInfo = originalGetNVMeInfo
		nodeDeviceSizePollInterval = originalPollInterval
	})
	nodeDeviceSizePollInterval = time.Millisecond
	sizeReads := 0
	nodeGetDeviceSize = func(string) (int64, error) {
		sizeReads++
		if sizeReads <= 2 {
			return 2 << 30, nil
		}
		return 4 << 30, nil
	}
	nodeGetNVMeInfo = func(string) (string, error) { return "nqn.test:block-vol", nil }
	resp, err := d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:          "block-vol",
		VolumePath:        t.TempDir(),
		StagingTargetPath: t.TempDir(),
		CapacityRange:     &csi.CapacityRange{RequiredBytes: 4 << 30},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(4<<30), resp.CapacityBytes)
	assert.Equal(t, 3, sizeReads, "device size must be polled until the asynchronous rescan settles")

	commands := readNodeCommandLog(t, logPath)
	assert.Contains(t, commands, "nvme ns-rescan /dev/nvme3")
	assert.NotContains(t, commands, "blkid ")
	assert.NotContains(t, commands, "resize2fs ")
	assert.NotContains(t, commands, "xfs_growfs ")
	assert.NotContains(t, commands, "btrfs ")
}

func TestNodeExpandVolumeReturnsErrorWhenActualSizeIsBelowTarget(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "nvme")
	t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/nvme3n7\n")
	originalDeviceSize := nodeGetDeviceSize
	originalGetNVMeInfo := nodeGetNVMeInfo
	originalPollTimeout := nodeDeviceSizePollTimeout
	originalPollInterval := nodeDeviceSizePollInterval
	t.Cleanup(func() {
		nodeGetDeviceSize = originalDeviceSize
		nodeGetNVMeInfo = originalGetNVMeInfo
		nodeDeviceSizePollTimeout = originalPollTimeout
		nodeDeviceSizePollInterval = originalPollInterval
	})
	nodeDeviceSizePollTimeout = 5 * time.Millisecond
	nodeDeviceSizePollInterval = time.Millisecond
	nodeGetDeviceSize = func(string) (int64, error) { return 2 << 30, nil }
	nodeGetNVMeInfo = func(string) (string, error) { return "nqn.test:block-vol", nil }

	d := newTestNodeDriver(ShareTypeNVMeoF)
	resp, err := d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:          "block-vol",
		VolumePath:        t.TempDir(),
		StagingTargetPath: t.TempDir(),
		CapacityRange:     &csi.CapacityRange{RequiredBytes: 4 << 30},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		},
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "did not settle")
}

func TestNodeExpandVolumeRejectsRawBlockDeviceOwnedByAnotherVolume(t *testing.T) {
	installFakeNodeCommands(t, "findmnt")
	t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/sdz\n")
	originalGetInfo := nodeGetISCSIInfo
	originalRescan := nodeISCSIRescan
	t.Cleanup(func() {
		nodeGetISCSIInfo = originalGetInfo
		nodeISCSIRescan = originalRescan
	})
	nodeGetISCSIInfo = func(string) (string, string, error) {
		return "192.0.2.10:3260", "iqn.2005-10.org.freenas.ctl:another-volume", nil
	}
	rescanCalls := 0
	nodeISCSIRescan = func(context.Context, string, string) error {
		rescanCalls++
		return nil
	}

	d := newTestNodeDriver(ShareTypeISCSI)
	_, err := d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:          "this-volume",
		VolumePath:        t.TempDir(),
		StagingTargetPath: t.TempDir(),
		CapacityRange:     &csi.CapacityRange{RequiredBytes: 4 << 30},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Zero(t, rescanCalls)
}

func TestStageNFSVolume_Validation(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)

	t.Run("NilVolumeContext", func(t *testing.T) {
		err := d.stageNFSVolume(context.Background(), nil, "/staging", nil)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("MissingServer", func(t *testing.T) {
		err := d.stageNFSVolume(context.Background(), map[string]string{
			"share": "/data",
		}, "/staging", nil)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "server")
	})

	t.Run("MissingShare", func(t *testing.T) {
		err := d.stageNFSVolume(context.Background(), map[string]string{
			"server": "nas.example.com",
		}, "/staging", nil)

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
			"portal": "192.0.2.100:3260",
		}, "/staging", nil)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "IQN")
	})

	t.Run("InvalidLUN", func(t *testing.T) {
		err := d.stageISCSIVolume(context.Background(), map[string]string{
			"portal": "192.0.2.100:3260",
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

func TestBlockBackedFilesystemStagePassesNormalizedMountFlags(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "iscsiadm", "nvme")
	originalISCSIConnect := iscsiConnectWithSessions
	originalNVMeConnect := nvmeConnectWithSubsystems
	originalFormatAndMount := nodeFormatAndMount
	t.Cleanup(func() {
		iscsiConnectWithSessions = originalISCSIConnect
		nvmeConnectWithSubsystems = originalNVMeConnect
		nodeFormatAndMount = originalFormatAndMount
	})

	var gotFS string
	var gotFlags []string
	nodeFormatAndMount = func(_ context.Context, _, _, fsType string, flags []string) error {
		gotFS = fsType
		gotFlags = append([]string(nil), flags...)
		return nil
	}
	capability := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{
		FsType:     "XFS",
		MountFlags: []string{" noatime ", "discard", "noatime", ""},
	}}}

	t.Run("iSCSI", func(t *testing.T) {
		gotFS, gotFlags = "", nil
		iscsiConnectWithSessions = func(context.Context, string, string, int, *util.ISCSIConnectOptions, []util.ISCSISessionInfo) (string, error) {
			return "/dev/sdz", nil
		}
		d := newTestNodeDriver(ShareTypeISCSI)
		err := d.stageISCSIVolume(context.Background(), map[string]string{
			"portal": "192.0.2.10:3260", "iqn": "iqn.test:volume", "lun": "0",
		}, filepath.Join(t.TempDir(), "stage"), capability)
		require.NoError(t, err)
		assert.Equal(t, "xfs", gotFS)
		assert.Equal(t, []string{"noatime", "discard", "nouuid"}, gotFlags)
	})

	t.Run("NVMeoF", func(t *testing.T) {
		gotFS, gotFlags = "", nil
		nvmeConnectWithSubsystems = func(context.Context, string, string, *util.NVMeoFConnectOptions, []util.NVMeSubsystem) (string, error) {
			return "/dev/nvme9n1", nil
		}
		d := newTestNodeDriver(ShareTypeNVMeoF)
		err := d.stageNVMeoFVolume(context.Background(), map[string]string{
			"nqn": "nqn.test:volume", "transport": "tcp", "address": "192.0.2.20", "port": "4420",
		}, filepath.Join(t.TempDir(), "stage"), capability)
		require.NoError(t, err)
		assert.Equal(t, "xfs", gotFS)
		assert.Equal(t, []string{"noatime", "discard", "nouuid"}, gotFlags)
	})

	t.Run("deduplicates operator supplied nouuid", func(t *testing.T) {
		flags := volumeMountFlagsForFS(&csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{MountFlags: []string{"NOUUID"}},
		}}, "XFS")
		assert.Equal(t, []string{"NOUUID"}, flags)
	})

	t.Run("ext4 unchanged", func(t *testing.T) {
		flags := volumeMountFlagsForFS(&csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{MountFlags: []string{"discard"}},
		}}, "ext4")
		assert.Equal(t, []string{"discard"}, flags)
	})
}

func TestNodeStageVolumeNFSXFSExcludesNouuid(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "mount")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)

	d := newTestNodeDriver(ShareTypeNFS)
	_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "nfs-xfs-volume",
		StagingTargetPath: filepath.Join(t.TempDir(), "stage"),
		VolumeContext: map[string]string{
			"node_attach_driver": "nfs",
			"server":             "192.0.2.30",
			"share":              "/mnt/pool/clone",
		},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{
				FsType: "XFS", MountFlags: []string{"noatime"},
			}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	})
	require.NoError(t, err)
	log := readNodeCommandLog(t, logPath)
	assert.Contains(t, log, "mount -t nfs -o nfsvers=4,noatime 192.0.2.30:/mnt/pool/clone")
	assert.NotContains(t, log, "nouuid")
}

func TestStageISCSIVolumeRejectsMultipathOwnedDeviceBeforeMount(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "iscsiadm")
	originalConnect := iscsiConnectWithSessions
	originalCheck := nodeCheckISCSIMultipath
	originalFormatAndMount := nodeFormatAndMount
	t.Cleanup(func() {
		iscsiConnectWithSessions = originalConnect
		nodeCheckISCSIMultipath = originalCheck
		nodeFormatAndMount = originalFormatAndMount
	})
	iscsiConnectWithSessions = func(context.Context, string, string, int, *util.ISCSIConnectOptions, []util.ISCSISessionInfo) (string, error) {
		return "/dev/sdz", nil
	}
	nodeCheckISCSIMultipath = func(devicePath string) error {
		return errors.New("iSCSI device " + devicePath + " is claimed by dm-multipath; iSCSI multipath is unsupported")
	}
	formatCalls := 0
	nodeFormatAndMount = func(context.Context, string, string, string, []string) error {
		formatCalls++
		return nil
	}

	d := newTestNodeDriver(ShareTypeISCSI)
	err := d.stageISCSIVolume(context.Background(), map[string]string{
		"portal": "192.0.2.10:3260", "iqn": "iqn.test:volume", "lun": "0",
	}, filepath.Join(t.TempDir(), "stage"), &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "claimed by dm-multipath")
	assert.Zero(t, formatCalls)
}

func TestStageBlockProtocolVolumesAreIdempotentWhenMounted(t *testing.T) {
	t.Run("iSCSI", func(t *testing.T) {
		installFakeNodeCommands(t, "findmnt", "iscsiadm")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
		t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/sda "+t.TempDir()+"\n")

		stagingPath := t.TempDir()
		d := newTestNodeDriver(ShareTypeISCSI)
		err := d.stageISCSIVolume(context.Background(), map[string]string{
			"portal": "192.0.2.100:3260",
			"iqn":    "iqn.test:vol1",
		}, stagingPath, &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		})
		require.NoError(t, err)
		assert.NotContains(t, readNodeCommandLog(t, logPath), "iscsiadm ")
	})

	t.Run("NVMe-oF", func(t *testing.T) {
		installFakeNodeCommands(t, "findmnt", "nvme")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
		t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/nvme2n1 "+t.TempDir()+"\n")

		stagingPath := t.TempDir()
		d := newTestNodeDriver(ShareTypeNVMeoF)
		err := d.stageNVMeoFVolume(context.Background(), map[string]string{
			"nqn":     "nqn.test:vol1",
			"address": "192.0.2.100",
		}, stagingPath, &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		})
		require.NoError(t, err)
		assert.NotContains(t, readNodeCommandLog(t, logPath), "nvme ")
	})
}

func TestStageBlockProtocolVolumesListSessionsOnce(t *testing.T) {
	t.Run("iSCSI", func(t *testing.T) {
		installFakeNodeCommands(t, "findmnt", "iscsiadm")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
		t.Setenv("FAKE_NODE_ISCSI_SESSION_OUTPUT", "tcp: [1] 192.0.2.100:3260,1 iqn.test:other (non-flash)\n")

		originalConnect := iscsiConnectWithSessions
		iscsiConnectWithSessions = func(_ context.Context, _, _ string, _ int, _ *util.ISCSIConnectOptions, sessions []util.ISCSISessionInfo) (string, error) {
			require.Len(t, sessions, 1)
			return "/dev/null", nil
		}
		defer func() { iscsiConnectWithSessions = originalConnect }()

		stagingPath := filepath.Join(t.TempDir(), "staging")
		d := newTestNodeDriver(ShareTypeISCSI)
		err := d.stageISCSIVolume(context.Background(), map[string]string{
			"portal": "192.0.2.100:3260",
			"iqn":    "iqn.test:vol1",
		}, stagingPath, &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		})
		require.NoError(t, err)
		assert.Equal(t, 1, strings.Count(readNodeCommandLog(t, logPath), "iscsiadm -m session"))
	})

	t.Run("NVMe-oF", func(t *testing.T) {
		installFakeNodeCommands(t, "findmnt", "nvme")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
		t.Setenv("FAKE_NODE_NVME_LIST_SUBSYS_OUTPUT", `{"Subsystems":[{"NQN":"nqn.test:other","Name":"nvme2"}]}`)

		originalConnect := nvmeConnectWithSubsystems
		nvmeConnectWithSubsystems = func(_ context.Context, _, _ string, _ *util.NVMeoFConnectOptions, subsystems []util.NVMeSubsystem) (string, error) {
			require.Len(t, subsystems, 1)
			return "/dev/null", nil
		}
		defer func() { nvmeConnectWithSubsystems = originalConnect }()

		stagingPath := filepath.Join(t.TempDir(), "staging")
		d := newTestNodeDriver(ShareTypeNVMeoF)
		err := d.stageNVMeoFVolume(context.Background(), map[string]string{
			"nqn":     "nqn.test:vol1",
			"address": "192.0.2.100",
		}, stagingPath, &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		})
		require.NoError(t, err)
		assert.Equal(t, 1, strings.Count(readNodeCommandLog(t, logPath), "nvme list-subsys"))
	})
}

func TestStageBlockProtocolVolumesAreIdempotentWithLiveSymlink(t *testing.T) {
	t.Run("iSCSI", func(t *testing.T) {
		installFakeNodeCommands(t, "findmnt", "iscsiadm")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
		t.Setenv("FAKE_NODE_ISCSI_SESSION_OUTPUT", "tcp: [1] 192.0.2.100:3260,1 iqn.test:vol1 (non-flash)\n")

		originalInfo := getISCSIInfoFromDeviceWithSessions
		getISCSIInfoFromDeviceWithSessions = func(devicePath string, sessions []util.ISCSISessionInfo) (string, string, error) {
			return "192.0.2.100:3260", "iqn.test:vol1", nil
		}
		defer func() { getISCSIInfoFromDeviceWithSessions = originalInfo }()

		stagingPath := filepath.Join(t.TempDir(), "staging")
		require.NoError(t, os.Symlink("/dev/null", stagingPath))

		d := newTestNodeDriver(ShareTypeISCSI)
		err := d.stageISCSIVolume(context.Background(), map[string]string{
			"portal": "192.0.2.100:3260",
			"iqn":    "iqn.test:vol1",
		}, stagingPath, &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		})
		require.NoError(t, err)

		commands := readNodeCommandLog(t, logPath)
		assert.Equal(t, 1, strings.Count(commands, "iscsiadm -m session"))
		assert.NotContains(t, commands, "--logout")
	})

	t.Run("NVMe-oF", func(t *testing.T) {
		installFakeNodeCommands(t, "findmnt", "nvme")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
		t.Setenv("FAKE_NODE_NVME_LIST_SUBSYS_OUTPUT", `{"Subsystems":[{"NQN":"nqn.test:vol1","Name":"nvme2","Paths":[{"Name":"nvme2"}]}]}`)

		originalInfo := getNVMeInfoFromDevice
		getNVMeInfoFromDevice = func(devicePath string) (string, error) {
			return "nqn.test:vol1", nil
		}
		defer func() { getNVMeInfoFromDevice = originalInfo }()

		stagingPath := filepath.Join(t.TempDir(), "staging")
		require.NoError(t, os.Symlink("/dev/null", stagingPath))

		d := newTestNodeDriver(ShareTypeNVMeoF)
		err := d.stageNVMeoFVolume(context.Background(), map[string]string{
			"nqn":     "nqn.test:vol1",
			"address": "192.0.2.100",
		}, stagingPath, &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		})
		require.NoError(t, err)

		commands := readNodeCommandLog(t, logPath)
		assert.Equal(t, 1, strings.Count(commands, "nvme list-subsys"))
		assert.NotContains(t, commands, "nvme disconnect")
	})
}

func TestStageRetryDoesNotDisconnectLiveDeviceOnIdentityFailure(t *testing.T) {
	t.Run("iSCSI", func(t *testing.T) {
		installFakeNodeCommands(t, "findmnt", "iscsiadm")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
		t.Setenv("FAKE_NODE_ISCSI_SESSION_OUTPUT", "tcp: [1] 192.0.2.100:3260,1 iqn.test:vol1 (non-flash)\n")

		originalInfo := getISCSIInfoFromDeviceWithSessions
		originalConnect := iscsiConnectWithSessions
		t.Cleanup(func() {
			getISCSIInfoFromDeviceWithSessions = originalInfo
			iscsiConnectWithSessions = originalConnect
		})
		getISCSIInfoFromDeviceWithSessions = func(string, []util.ISCSISessionInfo) (string, string, error) {
			return "", "", errors.New("transient sysfs miss")
		}
		iscsiConnectWithSessions = func(context.Context, string, string, int, *util.ISCSIConnectOptions, []util.ISCSISessionInfo) (string, error) {
			return "/dev/null", nil
		}

		stagingPath := filepath.Join(t.TempDir(), "staging")
		require.NoError(t, os.Symlink("/dev/null", stagingPath))
		d := newTestNodeDriver(ShareTypeISCSI)
		err := d.stageISCSIVolume(context.Background(), map[string]string{
			"portal": "192.0.2.100:3260", "iqn": "iqn.test:vol1",
		}, stagingPath, &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		})
		require.NoError(t, err)
		assert.NotContains(t, readNodeCommandLog(t, logPath), "--logout")
	})

	t.Run("NVMe-oF", func(t *testing.T) {
		installFakeNodeCommands(t, "findmnt", "nvme")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
		t.Setenv("FAKE_NODE_NVME_LIST_SUBSYS_OUTPUT", `{"Subsystems":[{"NQN":"nqn.test:vol1","Name":"nvme2","Paths":[{"Name":"nvme2"}]}]}`)

		originalInfo := getNVMeInfoFromDevice
		originalConnect := nvmeConnectWithSubsystems
		t.Cleanup(func() {
			getNVMeInfoFromDevice = originalInfo
			nvmeConnectWithSubsystems = originalConnect
		})
		getNVMeInfoFromDevice = func(string) (string, error) {
			return "", errors.New("transient sysfs miss")
		}
		nvmeConnectWithSubsystems = func(context.Context, string, string, *util.NVMeoFConnectOptions, []util.NVMeSubsystem) (string, error) {
			return "/dev/null", nil
		}

		stagingPath := filepath.Join(t.TempDir(), "staging")
		require.NoError(t, os.Symlink("/dev/null", stagingPath))
		d := newTestNodeDriver(ShareTypeNVMeoF)
		err := d.stageNVMeoFVolume(context.Background(), map[string]string{
			"nqn": "nqn.test:vol1", "address": "192.0.2.100",
		}, stagingPath, &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		})
		require.NoError(t, err)
		assert.NotContains(t, readNodeCommandLog(t, logPath), "nvme disconnect")
	})
}

func TestWaitForSessionCleanupReturnsWhenSessionDisappears(t *testing.T) {
	calls := 0
	start := time.Now()
	err := waitForSessionCleanup(context.Background(), time.Second, func() (bool, error) {
		calls++
		return calls == 1, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
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
			"address": "192.0.2.100",
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
		before, err := os.Lstat(linkPath)
		require.NoError(t, err)

		// Call again with same target - should succeed
		err = createSymlinkAtomic(target, linkPath)
		require.NoError(t, err)
		after, err := os.Lstat(linkPath)
		require.NoError(t, err)
		assert.True(t, os.SameFile(before, after), "correct symlink must be left in place")

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

	t.Run("RegularFileIsReplaced", func(t *testing.T) {
		tmpDir := t.TempDir()
		linkPath := filepath.Join(tmpDir, "testlink")
		target := "/dev/test"
		require.NoError(t, os.WriteFile(linkPath, []byte("stale"), 0o600))

		err := createSymlinkAtomic(target, linkPath)
		require.NoError(t, err)

		info, err := os.Lstat(linkPath)
		require.NoError(t, err)
		assert.NotZero(t, info.Mode()&os.ModeSymlink)
		actualTarget, err := os.Readlink(linkPath)
		require.NoError(t, err)
		assert.Equal(t, target, actualTarget)
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
		d.cleanupOrphanedSessionByVolumeID(context.Background(), "test-vol", ShareTypeNFS)
	})

	t.Run("ISCSIDriver", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeISCSI)
		d.config.ISCSI.NameSuffix = "-cluster"

		// Should attempt to find and clean iSCSI session
		// Will log "no active session found" since no real sessions exist
		d.cleanupOrphanedSessionByVolumeID(context.Background(), "test-vol", ShareTypeISCSI)
	})

	t.Run("NVMeoFDriver", func(t *testing.T) {
		d := newTestNodeDriver(ShareTypeNVMeoF)
		d.config.NVMeoF.NamePrefix = "k8s-"
		d.config.NVMeoF.NameSuffix = "-prod"

		// Should attempt to find and clean NVMe-oF session
		d.cleanupOrphanedSessionByVolumeID(context.Background(), "test-vol", ShareTypeNVMeoF)
	})
}

func TestCleanupOrphanedSessionUsesOnlyConfiguredProtocol(t *testing.T) {
	installFakeNodeCommands(t, "iscsiadm", "nvme")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)

	d := newTestNodeDriver(ShareTypeISCSI)
	d.cleanupOrphanedSessionByVolumeID(context.Background(), "test-vol", ShareTypeISCSI)
	log := readNodeCommandLog(t, logPath)
	assert.Contains(t, log, "iscsiadm -m session")
	assert.NotContains(t, log, "nvme list-subsys")

	require.NoError(t, os.Remove(logPath))
	d.cleanupOrphanedSessionByVolumeID(context.Background(), "test-vol", ShareTypeNVMeoF)
	log = readNodeCommandLog(t, logPath)
	assert.Contains(t, log, "nvme list-subsys")
	assert.NotContains(t, log, "iscsiadm -m session")
}

func TestNodeUnstageBlockSymlinkDoesNotTrustDeviceSession(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "iscsiadm", "nvme")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)

	originalGetInfo := nodeGetNVMeInfo
	originalDisconnect := nodeNVMeDisconnect
	defer func() {
		nodeGetNVMeInfo = originalGetInfo
		nodeNVMeDisconnect = originalDisconnect
	}()
	disconnectCalls := 0
	deviceInfoCalls := 0
	nodeGetNVMeInfo = func(string) (string, error) {
		deviceInfoCalls++
		return "nqn.test:another-volume", nil
	}
	nodeNVMeDisconnect = func(string) error {
		disconnectCalls++
		return nil
	}

	stagingPath := filepath.Join(t.TempDir(), "staging")
	require.NoError(t, os.Symlink("/dev/nvme9n1", stagingPath))
	d := newTestNodeDriver(ShareTypeNVMeoF)
	_, err := d.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId: "test-volume", StagingTargetPath: stagingPath,
	})
	require.NoError(t, err)
	assert.Zero(t, deviceInfoCalls, "the stale symlink device must not be used to derive a session")
	assert.Zero(t, disconnectCalls, "a different volume's NVMe session must not be disconnected")
	log := readNodeCommandLog(t, logPath)
	assert.Contains(t, log, "nvme list-subsys")
	assert.NotContains(t, log, "iscsiadm -m session")
}

func TestNodeUnstageOrphanScanProtocolSelection(t *testing.T) {
	t.Run("KnownISCSIDeviceProbesOnlyISCSI", func(t *testing.T) {
		installFakeNodeCommands(t, "findmnt", "iscsiadm", "nvme")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)

		originalGetInfo := nodeGetISCSIInfo
		defer func() { nodeGetISCSIInfo = originalGetInfo }()
		nodeGetISCSIInfo = func(string) (string, string, error) {
			return "", "", errors.New("info lookup failed")
		}

		stagingPath := filepath.Join(t.TempDir(), "staging")
		require.NoError(t, os.Symlink("/dev/sdz", stagingPath))
		d := newTestNodeDriver(ShareTypeNVMeoF)
		_, err := d.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
			VolumeId: "test-volume", StagingTargetPath: stagingPath,
		})
		require.NoError(t, err)
		log := readNodeCommandLog(t, logPath)
		assert.Contains(t, log, "iscsiadm -m session")
		assert.NotContains(t, log, "nvme list-subsys")
	})

	t.Run("KnownNVMeDeviceProbesOnlyNVMe", func(t *testing.T) {
		installFakeNodeCommands(t, "findmnt", "iscsiadm", "nvme")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)

		originalGetInfo := nodeGetNVMeInfo
		defer func() { nodeGetNVMeInfo = originalGetInfo }()
		nodeGetNVMeInfo = func(string) (string, error) {
			return "", errors.New("info lookup failed")
		}

		stagingPath := filepath.Join(t.TempDir(), "staging")
		require.NoError(t, os.Symlink("/dev/nvme9n1", stagingPath))
		d := newTestNodeDriver(ShareTypeISCSI)
		_, err := d.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
			VolumeId: "test-volume", StagingTargetPath: stagingPath,
		})
		require.NoError(t, err)
		log := readNodeCommandLog(t, logPath)
		assert.Contains(t, log, "nvme list-subsys")
		assert.NotContains(t, log, "iscsiadm -m session")
	})

	t.Run("EmptyDeviceProbesBothProtocols", func(t *testing.T) {
		installFakeNodeCommands(t, "findmnt", "iscsiadm", "nvme")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)

		d := newTestNodeDriver(ShareTypeISCSI)
		_, err := d.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
			VolumeId: "test-volume", StagingTargetPath: filepath.Join(t.TempDir(), "missing"),
		})
		require.NoError(t, err)
		log := readNodeCommandLog(t, logPath)
		assert.Contains(t, log, "iscsiadm -m session")
		assert.Contains(t, log, "nvme list-subsys")
	})
}

func TestCleanupOrphanedISCSISessionUsesProtocolShareName(t *testing.T) {
	installFakeNodeCommands(t, "iscsiadm")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)

	d := newTestNodeDriver(ShareTypeISCSI)
	d.config.ISCSI.NameSuffix = "-Cluster_Name!"
	volumeID := "pvc-11111111-2222-4333-8444-555555555555"
	targetName := d.iscsiShareName(volumeID)
	assert.Equal(t, protocolShareName(volumeID+d.config.ISCSI.NameSuffix), targetName)
	iqn := "iqn.2005-10.org.freenas.ctl:" + targetName
	t.Setenv("FAKE_NODE_ISCSI_SESSION_OUTPUT", "tcp: [1] 192.0.2.100:3260,1 "+iqn+" (non-flash)\n")

	d.cleanupOrphanedSessionByVolumeID(context.Background(), volumeID, ShareTypeISCSI)
	log := readNodeCommandLog(t, logPath)
	assert.Contains(t, log, "-T "+iqn)
	assert.LessOrEqual(t, len(targetName), 64)
}

func TestNodeStageVolume_ConcurrentProtection(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)
	volumeID := "concurrent-test-vol"

	// Simulate first operation holding the lock
	lockKey := nodeVolumeLockKey(volumeID)
	acquired := d.acquireOperationLock(lockKey)
	require.True(t, acquired)
	defer d.releaseOperationLock(lockKey)

	// Second attempt should get Aborted
	req := &csi.NodeStageVolumeRequest{
		VolumeId:          volumeID,
		StagingTargetPath: "/staging",
		VolumeContext:     map[string]string{"server": "nas", "share": "/data"},
		VolumeCapability:  testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
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
	lockKey := nodeVolumeLockKey(volumeID)
	acquired := d.acquireOperationLock(lockKey)
	require.True(t, acquired)
	defer d.releaseOperationLock(lockKey)

	req := &csi.NodePublishVolumeRequest{
		VolumeId:          volumeID,
		TargetPath:        "/target",
		StagingTargetPath: "/staging",
		VolumeCapability:  testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
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
	lockKey := nodeVolumeLockKey(volumeID)
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
	lockKey := nodeVolumeLockKey(volumeID)
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

func TestNodeExpandVolume_ConcurrentProtection(t *testing.T) {
	d := newTestNodeDriver(ShareTypeNFS)
	volumeID := "concurrent-expand-vol"

	lockKey := nodeVolumeLockKey(volumeID)
	require.True(t, d.acquireOperationLock(lockKey))
	defer d.releaseOperationLock(lockKey)

	_, err := d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:   volumeID,
		VolumePath: t.TempDir(),
	})
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
}
