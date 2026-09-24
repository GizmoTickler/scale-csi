package driver

import (
	"path/filepath"
	"strconv"
	"strings"
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
// Ownership is POSITIVE, never inferred from the NAS address alone: a
// controller is touched only when its subsystem NQN belongs to a device staged
// under THIS driver's kubelet staging directory, and its transport, traddr and
// trsvcid match this driver's configured targets. Another workload connected to
// the same NAS (a different NQN, port or transport) keeps whatever timeout it
// chose. If the staged-device scan fails the pass does nothing. dryRun logs
// without writing, like GC.
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
	var owned map[string]struct{}
	transport := strings.ToLower(strings.TrimSpace(d.config.NVMeoF.Transport))
	port := strconv.Itoa(d.config.NVMeoF.TransportServiceID)
	for _, ctrl := range controllers {
		if ctrl.FastIOFailTmo == desiredSeconds {
			continue
		}
		if transport != "" && !strings.EqualFold(ctrl.Transport, transport) {
			continue
		}
		if d.config.NVMeoF.TransportServiceID > 0 && nvmeAddressField(ctrl.Address, "trsvcid") != port {
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
		if owned == nil {
			if owned, err = d.stagedNVMeoFNQNs(); err != nil {
				klog.Warningf("NVMe-oF tunables: cannot prove controller ownership, skipping this pass: %v", err)
				return
			}
		}
		if _, ours := owned[ctrl.SubsysNQN]; !ours {
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

// stagedNVMeoFNQNs returns the subsystem NQNs of NVMe devices staged under this
// driver's own kubelet staging directory (filesystem mounts and raw-block
// symlinks). Kubelet keys that directory by CSI driver name, so it proves the
// device belongs to a volume THIS driver staged.
func (d *Driver) stagedNVMeoFNQNs() (map[string]struct{}, error) {
	root := filepath.Join(kubeletCSIStagingRoot, d.name) + string(filepath.Separator)
	// Every mount point per device: a published filesystem volume is mounted
	// at its staging path AND bind-mounted into the pod, and a single-target
	// inventory can keep only the pod path (codex re-verification N1).
	mounts, err := getBlockDeviceMounts()
	if err != nil {
		return nil, err
	}
	staged, err := getStagedBlockDevices()
	if err != nil {
		return nil, err
	}
	candidates := make(map[string]struct{})
	for device, targets := range mounts {
		for _, target := range targets {
			if strings.HasPrefix(target, root) {
				candidates[device] = struct{}{}
			}
		}
	}
	for device, link := range staged {
		if strings.HasPrefix(link, root) {
			candidates[device] = struct{}{}
		}
	}
	owned := make(map[string]struct{})
	for device := range candidates {
		nqn, infoErr := getNVMeInfoFromDevice(device)
		if infoErr != nil || nqn == "" {
			continue
		}
		owned[nqn] = struct{}{}
	}
	return owned, nil
}

// nvmeAddressField returns one key's value from a sysfs controller address
// ("traddr=...,trsvcid=...,src_addr=...").
func nvmeAddressField(address, key string) string {
	for _, field := range strings.Split(address, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(field), "=")
		if ok && k == key {
			return v
		}
	}
	return ""
}
