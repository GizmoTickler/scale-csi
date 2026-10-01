package driver

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

// A replayed raw-block NodePublishVolume after a node-plugin restart (which an
// upgrade is) has no in-memory record, and the mount table shows the bound
// device node's source as devtmpfs ("udev[/sda]"), never the device. Comparing
// that with the staged device refused every such replay with AlreadyExists; the
// target is identified by its device number instead.
func TestRawBlockPublishReplayComparesDeviceNumbers(t *testing.T) {
	target := filepath.Join(t.TempDir(), "pv")
	require.NoError(t, os.WriteFile(target, nil, 0o600))
	capability, err := nodeCapabilityForRequest(&csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	})
	require.NoError(t, err)
	req := &csi.NodePublishVolumeRequest{VolumeId: "pvc-1", TargetPath: target}

	origInfo, origStat := nodeGetMountInfo, nodeStatsStat
	t.Cleanup(func() { nodeGetMountInfo, nodeStatsStat = origInfo, origStat })
	nodeGetMountInfo = func(string) (util.MountInfo, error) {
		return util.MountInfo{Source: "udev[/sda]", FSType: "devtmpfs", Options: []string{"rw"}}, nil
	}
	devices := map[string]uint64{target: 7, "/dev/sda": 7, "/dev/sdb": 9}
	nodeStatsStat = func(path string) (uint32, uint64, error) {
		rdev, ok := devices[path]
		if !ok {
			return 0, 0, os.ErrNotExist
		}
		return unix.S_IFBLK | 0o660, rdev, nil
	}

	d := newTestNodeDriver(ShareTypeNVMeoF)
	require.NoError(t, d.validateExistingPublication(req, capability, "/dev/sda"), "after a restart: no record")
	require.NoError(t, d.validateExistingPublication(req, capability, "/dev/sda"), "and with the record it stored")

	d = newTestNodeDriver(ShareTypeNVMeoF)
	err = d.validateExistingPublication(req, capability, "/dev/sdb")
	assert.Equal(t, codes.AlreadyExists, status.Code(err), "a target bound to another device: %v", err)

	// A fresh publish records the staged device, not the devtmpfs source.
	d = newTestNodeDriver(ShareTypeNVMeoF)
	d.rememberPublication(req, capability, "/dev/sda")
	require.NoError(t, d.validateExistingPublication(req, capability, "/dev/sda"))
}
