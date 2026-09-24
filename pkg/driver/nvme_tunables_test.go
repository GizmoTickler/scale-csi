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

func stubNVMeTunables(t *testing.T, controllers []util.NVMeFabricsController, setErr error) *[]tunableWrite {
	t.Helper()
	origList, origSet := listNVMeFabricsControllers, setNVMeControllerFastIOFailTmo
	t.Cleanup(func() { listNVMeFabricsControllers, setNVMeControllerFastIOFailTmo = origList, origSet })
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
	}, nil)
	before := testutil.ToFloat64(nvmeControllerTunableCorrections.WithLabelValues("fast_io_fail_tmo", "corrected"))

	newMultipathTunablesDriver().reconcileNVMeoFControllerTunables(false)

	assert.Equal(t, []tunableWrite{{"nvme1", 15}, {"nvme2", 15}}, *writes,
		"stale in-scope controllers converge to the 15s default; correct and foreign controllers are untouched")
	assert.Equal(t, before+2, testutil.ToFloat64(nvmeControllerTunableCorrections.WithLabelValues("fast_io_fail_tmo", "corrected")))
}

func TestReconcileNVMeoFControllerTunablesHonorsConfiguredValue(t *testing.T) {
	writes := stubNVMeTunables(t, []util.NVMeFabricsController{
		{Name: "nvme1", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", FastIOFailTmo: 15},
	}, nil)
	d := newMultipathTunablesDriver()
	d.config.NVMeoF.Connect.FastIOFailTmo = 5
	d.reconcileNVMeoFControllerTunables(false)
	assert.Equal(t, []tunableWrite{{"nvme1", 5}}, *writes)
}

// Single path with no explicit value keeps the historical "off": failing I/O
// fast has nowhere to fail over to.
func TestReconcileNVMeoFControllerTunablesSinglePathConvergesToOff(t *testing.T) {
	writes := stubNVMeTunables(t, []util.NVMeFabricsController{
		{Name: "nvme1", Transport: "tcp", Address: "traddr=192.0.2.100,trsvcid=4420", FastIOFailTmo: 15},
		{Name: "nvme2", Transport: "tcp", Address: "traddr=192.0.2.100,trsvcid=4420", FastIOFailTmo: -1},
	}, nil)
	newTestNodeDriver(ShareTypeNVMeoF).reconcileNVMeoFControllerTunables(false)
	assert.Equal(t, []tunableWrite{{"nvme1", -1}}, *writes)
}

func TestReconcileNVMeoFControllerTunablesDryRunWritesNothing(t *testing.T) {
	writes := stubNVMeTunables(t, []util.NVMeFabricsController{
		{Name: "nvme1", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", FastIOFailTmo: -1},
	}, nil)
	newMultipathTunablesDriver().reconcileNVMeoFControllerTunables(true)
	assert.Empty(t, *writes)
}

func TestReconcileNVMeoFControllerTunablesCountsWriteErrorsAndContinues(t *testing.T) {
	writes := stubNVMeTunables(t, []util.NVMeFabricsController{
		{Name: "nvme1", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", FastIOFailTmo: -1},
		{Name: "nvme2", Transport: "tcp", Address: "traddr=192.0.2.11,trsvcid=4420", FastIOFailTmo: -1},
	}, errors.New("read-only file system"))
	before := testutil.ToFloat64(nvmeControllerTunableCorrections.WithLabelValues("fast_io_fail_tmo", "error"))
	newMultipathTunablesDriver().reconcileNVMeoFControllerTunables(false)
	require.Len(t, *writes, 2, "one failed write must not stop the pass")
	assert.Equal(t, before+2, testutil.ToFloat64(nvmeControllerTunableCorrections.WithLabelValues("fast_io_fail_tmo", "error")))
}
