package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// backendHealthCallTimeout bounds each per-tick backend read so a hung call can
// never stall the poller for longer than a fixed window, independent of the
// configured interval. Mirrors the capacity gauge loop's guard.
const backendHealthCallTimeout = 30 * time.Second

// minBackendHealthInterval clamps the poll cadence. pool.query is cheap but not
// free, and health is not a sub-30s signal.
const minBackendHealthInterval = 30 * time.Second

// startBackendHealth launches the controller-only backend-health poll loop when
// backendHealth.enabled is set. Each tick is at most TWO bounded READ calls
// (pool.query + disk.temperature_alerts) — the loop never writes anything.
//
// DEFAULT OFF: an un-opted-in deployment issues zero additional API calls and
// its VolumeConditions keep the exact dataset-only semantics they had before.
func (d *Driver) startBackendHealth() {
	if d.config == nil || !d.config.BackendHealth.Enabled {
		return
	}
	interval, err := d.config.BackendHealth.IntervalDuration()
	if err != nil {
		klog.Errorf("Backend health polling disabled due to invalid interval %q: %v", d.config.BackendHealth.Interval, err)
		return
	}
	pool := d.parentPoolName()
	if pool == "" {
		klog.Error("Backend health polling disabled: no zfs.parentDataset configured")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.backendHealthCancel = cancel
	d.backendHealthWg.Add(1)
	go func() {
		defer d.backendHealthWg.Done()
		klog.Infof("Backend health polling started: interval=%v pool=%s", interval, pool)
		run := func() {
			callCtx, callCancel := context.WithTimeout(ctx, backendHealthCallTimeout)
			defer callCancel()
			d.sampleBackendHealth(callCtx, pool)
		}
		// Populate immediately so the first ControllerGetVolume after startup is
		// already pool-aware rather than falling back for a whole interval.
		run()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				run()
			case <-ctx.Done():
				klog.Info("Backend health polling stopped")
				return
			}
		}
	}()
}

func (d *Driver) stopBackendHealth() {
	if d.backendHealthCancel != nil {
		d.backendHealthCancel()
		d.backendHealthWg.Wait()
	}
}

// sampleBackendHealth takes one health sample and publishes it to the cache and
// the Prometheus gauges. A failed sample leaves the PREVIOUS snapshot in place:
// a transient backend blip must not flip every PVC's condition, and a stale
// snapshot is strictly better information than none.
func (d *Driver) sampleBackendHealth(ctx context.Context, pool string) {
	snapshot, err := d.truenasClient.PoolHealth(ctx, pool)
	if err != nil {
		if ctx.Err() == nil {
			klog.Warningf("Backend health sample failed for pool %s: %v", pool, err)
		}
		return
	}
	// Disk temperature alerts are a per-DISK signal; fan them out with the pool.
	alerts, alertErr := d.truenasClient.DiskTemperatureAlerts(ctx, snapshot.Disks)
	if alertErr != nil {
		if ctx.Err() == nil {
			klog.Warningf("Disk temperature alert sample failed for pool %s: %v", pool, alertErr)
		}
	} else {
		snapshot.TemperatureAlerts = len(alerts)
	}

	d.backendHealth.Store(snapshot)
	SetPoolHealthMetrics(snapshot)
	if snapshot.Degraded() {
		klog.Warningf("Pool %s is %s (healthy=%t detail=%q)", snapshot.Pool, snapshot.Status, snapshot.Healthy, snapshot.StatusDetail)
	}
}

// poolHealthSnapshot returns the most recent sample, or nil when the poller is
// disabled or has not yet produced one.
func (d *Driver) poolHealthSnapshot() *truenas.PoolHealthSnapshot {
	return d.backendHealth.Load()
}

// volumeCondition composes the dataset-level condition with the pool-level
// backend health snapshot. It is the single helper ControllerGetVolume and
// ListVolumes share, so both RPCs — and whichever the external-health-monitor
// picks — report an identical condition.
//
// Attribution rationale: ZFS exposes no per-dataset health. Every managed volume
// lives on ONE pool, so a pool condition applies to all of them; this is a
// deliberate fan-out, not an approximation.
//
// Severity is deliberately conservative:
//   - a definitive dataset-level negative marker still wins (unchanged);
//   - DEGRADED/FAULTED/UNAVAIL -> Abnormal, because the data path is at risk;
//   - an in-progress scrub/resilver or a disk temperature alert -> NOT abnormal,
//     just a descriptive message. A routine monthly scrub must never mark every
//     PVC in the cluster unhealthy.
func (d *Driver) volumeCondition(ds *truenas.Dataset) *csi.VolumeCondition {
	return composeVolumeCondition(volumeConditionFromDataset(ds), d.poolHealthSnapshot())
}

func composeVolumeCondition(base *csi.VolumeCondition, snapshot *truenas.PoolHealthSnapshot) *csi.VolumeCondition {
	if snapshot == nil {
		return base
	}
	if base != nil && base.GetAbnormal() {
		// A dataset-level failure is more specific than a pool-level one; keep it.
		return base
	}

	if snapshot.Degraded() {
		message := fmt.Sprintf("pool %s is %s", snapshot.Pool, snapshot.Status)
		if snapshot.StatusDetail != "" {
			message += ": " + snapshot.StatusDetail
		}
		return &csi.VolumeCondition{Abnormal: true, Message: message}
	}

	warnings := make([]string, 0, 2)
	if snapshot.Scanning() {
		function := snapshot.ScanFunction
		if function == "" {
			function = "scan"
		}
		warnings = append(warnings, fmt.Sprintf("pool %s %s in progress (%.1f%%)", snapshot.Pool, function, snapshot.ScanPercentage))
	}
	if snapshot.ScanErrors > 0 {
		warnings = append(warnings, fmt.Sprintf("pool %s last scan reported %d errors", snapshot.Pool, snapshot.ScanErrors))
	}
	if snapshot.TemperatureAlerts > 0 {
		warnings = append(warnings, fmt.Sprintf("pool %s has %d disk temperature alert(s)", snapshot.Pool, snapshot.TemperatureAlerts))
	}
	if len(warnings) == 0 {
		return base
	}

	message := joinNonEmpty(warnings, "; ")
	if base != nil && base.GetMessage() != "" {
		message = base.GetMessage() + "; " + message
	}
	return &csi.VolumeCondition{Abnormal: false, Message: message}
}

func joinNonEmpty(values []string, sep string) string {
	out := ""
	for _, value := range values {
		if value == "" {
			continue
		}
		if out != "" {
			out += sep
		}
		out += value
	}
	return out
}
