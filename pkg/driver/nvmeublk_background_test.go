package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

// The background tasks (NVMe-oF/iSCSI session GC and fast_io_fail_tmo
// convergence) act on kernel sessions and controllers only. A volume staged on
// a ublk device has neither, so these tests pin that a staged ublk device is
// never counted as an expected session, never vetoes a pass, never has a
// controller re-tuned on its behalf, and never makes a background task contact
// nvmeublkd. The identity lookups below are the REAL util functions for the
// ublk devices: only the kernel devices are stubbed.

const (
	ublkStagedMount = kubeletCSIStagingRoot + "/org.scale.csi.nvmeof/hash-ublk-fs/globalmount"
	ublkStagedLink  = kubeletCSIStagingRoot + "/org.scale.csi.nvmeof/hash-ublk-block/globalmount"
	kernelMount     = kubeletCSIStagingRoot + "/org.scale.csi.nvmeof/hash-kernel/globalmount"
	kernelLiveNQN   = "nqn.2011-06.com.example:kernel-live"
)

// stubUblkStagedDevices stages one kernel NVMe device and two ublk devices
// (one mounted, one raw-block) under this driver's kubelet staging directory.
func stubUblkStagedDevices(t *testing.T) {
	t.Helper()
	oldMounted, oldMounts, oldStaged := getMountedBlockDevices, getBlockDeviceMounts, getStagedBlockDevices
	oldNVMe, oldISCSI, oldParent := getNVMeInfoFromDevice, getISCSIInfoFromDevice, blockDeviceParent
	t.Cleanup(func() {
		getMountedBlockDevices, getBlockDeviceMounts, getStagedBlockDevices = oldMounted, oldMounts, oldStaged
		getNVMeInfoFromDevice, getISCSIInfoFromDevice, blockDeviceParent = oldNVMe, oldISCSI, oldParent
	})
	getMountedBlockDevices = func() (map[string]string, error) {
		return map[string]string{"/dev/nvme2n1": kernelMount, "/dev/ublkb0": ublkStagedMount}, nil
	}
	getBlockDeviceMounts = func() (map[string][]string, error) {
		return map[string][]string{"/dev/nvme2n1": {kernelMount}, "/dev/ublkb0": {ublkStagedMount}}, nil
	}
	getStagedBlockDevices = func() (map[string]string, error) {
		return map[string]string{"/dev/ublkb1": ublkStagedLink}, nil
	}
	blockDeviceParent = func(device string) string { return device }
	getNVMeInfoFromDevice = func(device string) (string, error) {
		if device == "/dev/nvme2n1" {
			return kernelLiveNQN, nil
		}
		return util.GetNVMeInfoFromDevice(device)
	}
	getISCSIInfoFromDevice = func(device string) (string, string, error) {
		// What the real lookup reports for a device without SCSI ancestry.
		return "", "", errors.New("failed to resolve sysfs path: no such file or directory")
	}
}

func TestNVMeoFSessionGCIgnoresUblkDevices(t *testing.T) {
	stubUblkStagedDevices(t)
	forbidNewNVMeUblkDaemon(t)
	oldList, oldDisconnect := gcListNVMeoFSessions, gcDisconnectNVMeoF
	t.Cleanup(func() { gcListNVMeoFSessions, gcDisconnectNVMeoF = oldList, oldDisconnect })

	d := newTestUblkNodeDriver(t)
	d.config.NVMeoF.TransportAddress = "192.0.2.20"

	expected := d.getExpectedNVMeoFNQNs()
	require.NotNil(t, expected, "staged ublk devices must not veto the NVMe-oF GC pass")
	assert.Equal(t, map[string]struct{}{kernelLiveNQN: {}}, expected,
		"a ublk device is not a kernel session and must not appear in the expected set")

	address := "traddr=192.0.2.20,trsvcid=4420"
	gcListNVMeoFSessions = func() ([]util.NVMeoFSessionInfo, error) {
		return []util.NVMeoFSessionInfo{
			{NQN: kernelLiveNQN, Address: address, Addresses: []string{address}},
			{NQN: "nqn.2011-06.com.example:kernel-orphan", Address: address, Addresses: []string{address}},
		}, nil
	}
	var disconnected []string
	gcDisconnectNVMeoF = func(nqn string) error {
		disconnected = append(disconnected, nqn)
		return nil
	}
	// Session GC only collects sessions this plugin recorded connecting.
	reg, err := newSessionRegistry(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, reg.record("nqn.2011-06.com.example:kernel-orphan"))
	d.nvmeSessions = reg
	d.orphanedNVMeSessionsSeen.Store("nqn.2011-06.com.example:kernel-orphan", time.Now().Add(-time.Hour))

	d.gcNVMeoFSessions(context.Background(), 0, false)
	assert.Equal(t, []string{"nqn.2011-06.com.example:kernel-orphan"}, disconnected,
		"GC still collects a real kernel orphan beside ublk volumes, and only that")
}

func TestISCSISessionGCIsNotVetoedByUblkDevices(t *testing.T) {
	stubUblkStagedDevices(t)
	forbidNewNVMeUblkDaemon(t)
	d := newTestUblkNodeDriver(t)
	expected := d.getExpectedISCSITargets()
	require.NotNil(t, expected, "a mounted ublk device must not veto every iSCSI GC pass on the node")
	assert.Empty(t, expected)
}

func TestNVMeoFTunablesNeverTouchUblkVolumes(t *testing.T) {
	stubUblkStagedDevices(t)
	forbidNewNVMeUblkDaemon(t)
	oldList, oldSet := listNVMeFabricsControllers, setNVMeControllerFastIOFailTmo
	t.Cleanup(func() { listNVMeFabricsControllers, setNVMeControllerFastIOFailTmo = oldList, oldSet })
	listNVMeFabricsControllers = func() ([]util.NVMeFabricsController, error) {
		return []util.NVMeFabricsController{
			{Name: "nvme2", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: kernelLiveNQN, FastIOFailTmo: -1},
			// A stray kernel controller for the SAME subsystem a ublk device
			// serves: the ublk device must not lend it ownership.
			{Name: "nvme9", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: testUblkNQN, FastIOFailTmo: -1},
		}, nil
	}
	var writes []tunableWrite
	setNVMeControllerFastIOFailTmo = func(controller string, seconds int) error {
		writes = append(writes, tunableWrite{controller, seconds})
		return nil
	}

	d := newMultipathTunablesDriver()
	d.config.NVMeoF.Ublk.Enabled = true
	d.reconcileNVMeoFControllerTunables(false)
	assert.Equal(t, []tunableWrite{{"nvme2", 15}}, writes)

	owned, err := d.stagedNVMeoFNQNs()
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{kernelLiveNQN: {}}, owned, "ublk devices resolve to no NQN in the ownership scan")
}
