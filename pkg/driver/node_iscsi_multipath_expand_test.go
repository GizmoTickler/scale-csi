package driver

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An iSCSI multipath map grows only when multipathd is told to resize it, and
// only to the size of its smallest path: expansion must rescan every path of
// the map, then resize the map.
func TestNodeExpandVolumeISCSIMultipathRescansEveryPathAndResizesTheMap(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "blkid", "resize2fs", "xfs_growfs", "btrfs")
	t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/dm-3\n")

	originals := struct {
		size     func(string) (int64, error)
		info     func(string) (string, string, error)
		rescan   func(context.Context, string, string) error
		paths    func(string) (string, []string, bool, error)
		resize   func(context.Context, string) error
		interval time.Duration
		timeout  time.Duration
	}{nodeGetDeviceSize, nodeGetISCSIInfo, nodeISCSIRescan, nodeMultipathPaths, nodeMultipathResize, nodeDeviceSizePollInterval, nodeDeviceSizePollTimeout}
	t.Cleanup(func() {
		nodeGetDeviceSize, nodeGetISCSIInfo, nodeISCSIRescan = originals.size, originals.info, originals.rescan
		nodeMultipathPaths, nodeMultipathResize = originals.paths, originals.resize
		nodeDeviceSizePollInterval, nodeDeviceSizePollTimeout = originals.interval, originals.timeout
	})
	nodeDeviceSizePollInterval = time.Millisecond
	nodeDeviceSizePollTimeout = 50 * time.Millisecond

	const iqn = "iqn.2005-10.org.freenas.ctl:block-vol"
	portals := map[string]string{
		"/dev/dm-3": "192.0.2.30:3260", // the map resolves to its first path
		"/dev/sdb":  "192.0.2.30:3260",
		"/dev/sdc":  "192.0.2.31:3260",
	}
	nodeGetISCSIInfo = func(device string) (string, string, error) { return portals[device], iqn, nil }
	nodeMultipathPaths = func(device string) (string, []string, bool, error) {
		if device != "/dev/dm-3" {
			return "", nil, false, nil
		}
		return "mpatha", []string{"/dev/sdb", "/dev/sdc"}, true, nil
	}
	rescanned := map[string]bool{}
	nodeISCSIRescan = func(_ context.Context, portal, _ string) error {
		rescanned[portal] = true
		return nil
	}
	var resized []string
	nodeMultipathResize = func(_ context.Context, name string) error {
		if len(rescanned) == 2 {
			resized = append(resized, name)
		}
		return nil
	}
	nodeGetDeviceSize = func(string) (int64, error) {
		if len(resized) > 0 {
			return 4 << 30, nil
		}
		return 2 << 30, nil
	}

	d := newTestNodeDriver(ShareTypeISCSI)
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
	assert.Equal(t, map[string]bool{"192.0.2.30:3260": true, "192.0.2.31:3260": true}, rescanned)
	assert.Equal(t, []string{"mpatha"}, resized)
}
