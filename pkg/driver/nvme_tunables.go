package driver

import (
	"time"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

// Seams for tests.
var (
	listNVMeFabricsControllers     = util.ListNVMeFabricsControllers
	setNVMeControllerFastIOFailTmo = util.SetNVMeControllerFastIOFailTmo
)

// reconcileNVMeoFControllerTunables converges fast_io_fail_tmo on every live
// controller that belongs to this driver's configured NVMe-oF targets.
//
// `nvme connect` applies the flag only when a path is first created, and
// NodeStage skips the connect for a subsystem that is already live. So a
// controller connected before the flag existed, or before the configured value
// changed, would keep its old value for as long as the volume stays staged.
// Verified live on 2026-09-23: 17 of 30 controllers across three nodes still
// had fast_io_fail_tmo=off under multipath, meaning a dead path parked I/O
// instead of failing over. The kernel applies a sysfs write to the running
// controller, so no reconnect is needed.
//
// Scope matches session GC: a controller is ours if its traddr is one of the
// configured target addresses. dryRun logs without writing, like GC.
func (d *Driver) reconcileNVMeoFControllerTunables(dryRun bool) {
	if d.config == nil || d.config.NVMeoF.TransportAddress == "" {
		return
	}
	desired := d.nvmeConnectOptions(0).FastIOFailTmo
	desiredSeconds := -1
	if desired == 0 {
		desired = util.DefaultFastIOFailTmo
	}
	if desired > 0 {
		desiredSeconds = int(desired / time.Second)
	}

	targets := d.config.NVMeoF.multipathAddresses()
	if len(targets) == 0 {
		targets = []string{d.config.NVMeoF.TransportAddress}
	}

	controllers, err := listNVMeFabricsControllers()
	if err != nil {
		klog.Warningf("NVMe-oF tunables: failed to list controllers: %v", err)
		return
	}
	for _, ctrl := range controllers {
		if ctrl.FastIOFailTmo == desiredSeconds {
			continue
		}
		inScope := false
		for _, target := range targets {
			if nvmeSessionMatchesTransportAddress(ctrl.Address, target) {
				inScope = true
				break
			}
		}
		if !inScope {
			continue
		}
		if dryRun {
			klog.Infof("NVMe-oF tunables (dry run): would set %s (%s) fast_io_fail_tmo %d -> %d", ctrl.Name, ctrl.SubsysNQN, ctrl.FastIOFailTmo, desiredSeconds)
			continue
		}
		if err := setNVMeControllerFastIOFailTmo(ctrl.Name, desiredSeconds); err != nil {
			nvmeControllerTunableCorrections.WithLabelValues("fast_io_fail_tmo", "error").Inc()
			klog.Warningf("NVMe-oF tunables: failed to set %s (%s) fast_io_fail_tmo %d -> %d: %v", ctrl.Name, ctrl.SubsysNQN, ctrl.FastIOFailTmo, desiredSeconds, err)
			continue
		}
		nvmeControllerTunableCorrections.WithLabelValues("fast_io_fail_tmo", "corrected").Inc()
		klog.Infof("NVMe-oF tunables: set %s (%s) fast_io_fail_tmo %d -> %d", ctrl.Name, ctrl.SubsysNQN, ctrl.FastIOFailTmo, desiredSeconds)
	}
}
