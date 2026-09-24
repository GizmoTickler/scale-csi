package driver

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

type tunableWrite struct {
	controller string
	seconds    int
}

// stubNVMeTunables stubs the sysfs seams. Every controller whose SubsysNQN is in
// staged is backed by a device staged under this driver's kubelet directory.
func stubNVMeTunables(t *testing.T, controllers []util.NVMeFabricsController, setErr error, staged ...string) *[]tunableWrite {
	t.Helper()
	origList, origSet := listNVMeFabricsControllers, setNVMeControllerFastIOFailTmo
	origMounted, origStaged, origInfo := getMountedBlockDevices, getStagedBlockDevices, getNVMeInfoFromDevice
	t.Cleanup(func() {
		listNVMeFabricsControllers, setNVMeControllerFastIOFailTmo = origList, origSet
		getMountedBlockDevices, getStagedBlockDevices, getNVMeInfoFromDevice = origMounted, origStaged, origInfo
	})
	deviceNQN := map[string]string{}
	mounted := map[string]string{}
	for i, nqn := range staged {
		device := "/dev/nvme" + string(rune('a'+i)) + "n1"
		deviceNQN[device] = nqn
		mounted[device] = kubeletCSIStagingRoot + "/org.scale.csi.nvmeof/hash" + string(rune('a'+i)) + "/globalmount"
	}
	getMountedBlockDevices = func() (map[string]string, error) { return mounted, nil }
	getStagedBlockDevices = func() (map[string]string, error) { return map[string]string{}, nil }
	getNVMeInfoFromDevice = func(device string) (string, error) { return deviceNQN[device], nil }
	var writes []tunableWrite
	listNVMeFabricsControllers = func() ([]util.NVMeFabricsController, error) { return controllers, nil }
	setNVMeControllerFastIOFailTmo = func(controller string, seconds int) error {
		writes = append(writes, tunableWrite{controller, seconds})
		return setErr
	}
	return &writes
}

func newMultipathTunablesDriver() *Driver {
	d := newTestNodeDriver(ShareTypeNVMeoF)
	d.config.NVMeoF.TransportAddress = "192.0.2.10"
	d.config.NVMeoF.Multipath = true
	d.config.NVMeoF.Addresses = []string{"192.0.2.10", "192.0.2.11"}
	return d
}

// The live 2026-09-23 state: controllers staged before the flag existed kept
// fast_io_fail_tmo=off under multipath. The reconcile must converge them, and
// only them.
func TestReconcileNVMeoFControllerTunablesConvergesStaleControllers(t *testing.T) {
	writes := stubNVMeTunables(t, []util.NVMeFabricsController{
		{Name: "nvme1", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: "nqn.ours", FastIOFailTmo: -1},
		{Name: "nvme2", Transport: "tcp", Address: "traddr=192.0.2.11,trsvcid=4420,src_addr=192.0.2.20", SubsysNQN: "nqn.ours", FastIOFailTmo: -1},
		{Name: "nvme3", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: "nqn.ours2", FastIOFailTmo: 15},
		{Name: "nvme4", Transport: "tcp", Address: "traddr=198.51.100.9,trsvcid=4420", SubsysNQN: "nqn.foreign", FastIOFailTmo: -1},
	}, nil, "nqn.ours", "nqn.ours2", "nqn.foreign")
	before := testutil.ToFloat64(nvmeControllerTunableCorrections.WithLabelValues("fast_io_fail_tmo", "corrected"))

	newMultipathTunablesDriver().reconcileNVMeoFControllerTunables(false)

	assert.Equal(t, []tunableWrite{{"nvme1", 15}, {"nvme2", 15}}, *writes,
		"stale in-scope controllers converge to the 15s default; correct and foreign controllers are untouched")
	assert.Equal(t, before+2, testutil.ToFloat64(nvmeControllerTunableCorrections.WithLabelValues("fast_io_fail_tmo", "corrected")))
}

func TestReconcileNVMeoFControllerTunablesHonorsConfiguredValue(t *testing.T) {
	writes := stubNVMeTunables(t, []util.NVMeFabricsController{
		{Name: "nvme1", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: "nqn.ours", FastIOFailTmo: 15},
	}, nil, "nqn.ours")
	d := newMultipathTunablesDriver()
	d.config.NVMeoF.Connect.FastIOFailTmo = 5
	d.reconcileNVMeoFControllerTunables(false)
	assert.Equal(t, []tunableWrite{{"nvme1", 5}}, *writes)
}

// Single path with no explicit value keeps the historical "off": failing I/O
// fast has nowhere to fail over to.
func TestReconcileNVMeoFControllerTunablesSinglePathConvergesToOff(t *testing.T) {
	writes := stubNVMeTunables(t, []util.NVMeFabricsController{
		{Name: "nvme1", Transport: "tcp", Address: "traddr=192.0.2.100,trsvcid=4420", SubsysNQN: "nqn.ours", FastIOFailTmo: 15},
		{Name: "nvme2", Transport: "tcp", Address: "traddr=192.0.2.100,trsvcid=4420", SubsysNQN: "nqn.ours", FastIOFailTmo: -1},
	}, nil, "nqn.ours")
	newTestNodeDriver(ShareTypeNVMeoF).reconcileNVMeoFControllerTunables(false)
	assert.Equal(t, []tunableWrite{{"nvme1", -1}}, *writes)
}

func TestReconcileNVMeoFControllerTunablesDryRunWritesNothing(t *testing.T) {
	writes := stubNVMeTunables(t, []util.NVMeFabricsController{
		{Name: "nvme1", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: "nqn.ours", FastIOFailTmo: -1},
	}, nil, "nqn.ours")
	newMultipathTunablesDriver().reconcileNVMeoFControllerTunables(true)
	assert.Empty(t, *writes)
}

func TestReconcileNVMeoFControllerTunablesCountsWriteErrorsAndContinues(t *testing.T) {
	writes := stubNVMeTunables(t, []util.NVMeFabricsController{
		{Name: "nvme1", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: "nqn.ours", FastIOFailTmo: -1},
		{Name: "nvme2", Transport: "tcp", Address: "traddr=192.0.2.11,trsvcid=4420", SubsysNQN: "nqn.ours", FastIOFailTmo: -1},
	}, errors.New("read-only file system"), "nqn.ours")
	before := testutil.ToFloat64(nvmeControllerTunableCorrections.WithLabelValues("fast_io_fail_tmo", "error"))
	newMultipathTunablesDriver().reconcileNVMeoFControllerTunables(false)
	require.Len(t, *writes, 2, "one failed write must not stop the pass")
	assert.Equal(t, before+2, testutil.ToFloat64(nvmeControllerTunableCorrections.WithLabelValues("fast_io_fail_tmo", "error")))
}

// Verifier D1 (codex, 2026-09-24): address alone is not ownership. Another
// workload's controller at the SAME NAS address must never be touched: not a
// different NQN, not a different port, not a different transport, and not an
// NQN staged by a DIFFERENT CSI driver.
func TestReconcileNVMeoFControllerTunablesNeverTouchesForeignControllersAtTheSameAddress(t *testing.T) {
	writes := stubNVMeTunables(t, []util.NVMeFabricsController{
		{Name: "nvme90", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: "nqn.foreign.database", FastIOFailTmo: -1},
		{Name: "nvme91", Transport: "rdma", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: "nqn.ours", FastIOFailTmo: -1},
		{Name: "nvme92", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=9999", SubsysNQN: "nqn.ours", FastIOFailTmo: -1},
		{Name: "nvme93", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: "nqn.ours", FastIOFailTmo: -1},
	}, nil, "nqn.ours")
	// nqn.other-driver is mounted, but under ANOTHER driver's staging directory.
	getMountedBlockDevices = func() (map[string]string, error) {
		return map[string]string{
			"/dev/nvmean1": kubeletCSIStagingRoot + "/org.scale.csi.nvmeof/hasha/globalmount",
			"/dev/nvmezn1": kubeletCSIStagingRoot + "/other.csi.vendor/hashz/globalmount",
		}, nil
	}
	info := getNVMeInfoFromDevice
	getNVMeInfoFromDevice = func(device string) (string, error) {
		if device == "/dev/nvmezn1" {
			return "nqn.foreign.database", nil
		}
		return info(device)
	}

	newMultipathTunablesDriver().reconcileNVMeoFControllerTunables(false)

	assert.Equal(t, []tunableWrite{{"nvme93", 15}}, *writes,
		"only the controller whose NQN this driver staged, on the configured transport and port, is converged")
}

func TestReconcileNVMeoFControllerTunablesSkipsThePassWhenOwnershipCannotBeProven(t *testing.T) {
	writes := stubNVMeTunables(t, []util.NVMeFabricsController{
		{Name: "nvme1", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: "nqn.ours", FastIOFailTmo: -1},
	}, nil, "nqn.ours")
	getMountedBlockDevices = func() (map[string]string, error) { return nil, errors.New("mountinfo unreadable") }
	newMultipathTunablesDriver().reconcileNVMeoFControllerTunables(false)
	assert.Empty(t, *writes)
}
