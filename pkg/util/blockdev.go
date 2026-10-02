package util

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// blockDeviceNumber returns the device number of the block device node at path,
// following symlinks. It is a seam because test fixtures cannot mknod.
var blockDeviceNumber = func(path string) (uint64, error) {
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return 0, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFBLK {
		return 0, fmt.Errorf("%s is not a block device", path)
	}
	return uint64(stat.Rdev), nil //nolint:unconvert // Stat_t.Rdev width differs per platform (darwin: int32)
}

// isCurrentBlockDeviceNode reports whether devicePath is a block device node
// whose number equals the kernel's for that disk (sysDevFile holds "MAJ:MIN").
// Right after one session is torn down and a new one is connected to the same
// target (an iSCSI logout and login, an NVMe-oF disconnect and connect, or a
// handover between the Go and Rust node plugins), sysfs already names the new
// disk while /dev can still hold the previous disk's node of the same name
// (devtmpfs/udev removal lags), or a stale node with no fresh one yet. Opening
// such a node fails with ENXIO/ENODEV, so a device wait must keep polling until
// the node matches. One stat and one small sysfs read.
func isCurrentBlockDeviceNode(devicePath, sysDevFile string) bool {
	rdev, err := blockDeviceNumber(devicePath)
	if err != nil {
		return false
	}
	want, err := os.ReadFile(sysDevFile)
	if err != nil {
		return false
	}
	var major, minor uint32
	if _, err := fmt.Sscanf(strings.TrimSpace(string(want)), "%d:%d", &major, &minor); err != nil {
		return false
	}
	return unix.Major(rdev) == major && unix.Minor(rdev) == minor
}
