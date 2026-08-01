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

// backendHealthStaleIntervals is how many consecutive missed samples make the
// cached snapshot untrustworthy. Keeping the last snapshot across a blip is
// correct; keeping it across an outage is not — a stale DEGRADED keeps
// ScaleCSIPoolDegraded firing after the pool has recovered, and a stale ONLINE
// masks a real degradation. Past the TTL the driver falls back to the
// dataset-only condition (exactly the pre-GF5 semantics) and raises
// scale_csi_pool_health_stale.
const backendHealthStaleIntervals = 3

// backendHealthFlipSamples is the hysteresis depth for the condition fan-out: a
// pool-health transition must be confirmed by this many CONSECUTIVE samples
// before it flips every managed PVC's VolumeCondition.
//
// The fan-out is fleet-wide by construction — one pool backs every volume — so
// an unfiltered DEGRADED<->ONLINE flap would rewrite N conditions and churn N
// PVC events on every tick. Metrics are deliberately NOT damped: SetPoolHealthMetrics
// always publishes the raw sample, so a flap stays fully visible to Prometheus
// while the per-PVC condition stays stable.
//
// The FIRST observation is never damped: with no previous snapshot there is
// nothing to flap against, and delaying the initial signal would only blind the
// first interval after startup.
const backendHealthFlipSamples = 2

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

// backendHealthTTL is how long a cached snapshot may drive VolumeConditions
// before it is considered stale. It is derived from the configured poll cadence
// so it self-tunes, and never falls below the interval floor.
func (d *Driver) backendHealthTTL() time.Duration {
	interval := 60 * time.Second
	if d.config != nil {
		if resolved, err := d.config.BackendHealth.IntervalDuration(); err == nil && resolved > 0 {
			interval = resolved
		}
	}
	if interval < minBackendHealthInterval {
		interval = minBackendHealthInterval
	}
	return time.Duration(backendHealthStaleIntervals) * interval
}

// sampleBackendHealth takes one health sample and publishes it to the cache and
// the Prometheus gauges. A failed sample leaves the PREVIOUS snapshot in place:
// a transient backend blip must not flip every PVC's condition, and a recent
// snapshot is strictly better information than none. Past backendHealthTTL the
// snapshot stops being served at all (see poolHealthSnapshot).
func (d *Driver) sampleBackendHealth(ctx context.Context, pool string) {
	snapshot, err := d.truenasClient.PoolHealth(ctx, pool)
	if err != nil {
		if ctx.Err() == nil {
			klog.Warningf("Backend health sample failed for pool %s: %v", pool, err)
		}
		// Publish the staleness verdict from inside the poll loop so it keeps
		// updating even while the backend is unreachable.
		if previous := d.backendHealth.Load(); previous != nil {
			stale := time.Since(previous.SampledAt) > d.backendHealthTTL()
			SetPoolHealthStale(previous.Pool, stale)
			if stale {
				klog.Warningf("Backend health snapshot for pool %s is stale (last successful sample %v ago, TTL %v); "+
					"VolumeConditions fall back to dataset-only until the backend answers again",
					previous.Pool, time.Since(previous.SampledAt).Truncate(time.Second), d.backendHealthTTL())
			}
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

	// Metrics get the RAW sample, always: Prometheus must see a flap as a flap.
	SetPoolHealthMetrics(snapshot)
	SetPoolHealthStale(snapshot.Pool, false)
	if snapshot.Degraded() {
		klog.Warningf("Pool %s is %s (healthy=%t detail=%q)", snapshot.Pool, snapshot.Status, snapshot.Healthy, snapshot.StatusDetail)
	}
	d.publishBackendHealth(snapshot)
}

// publishBackendHealth applies the fan-out hysteresis and updates the cache that
// drives every managed volume's VolumeCondition.
func (d *Driver) publishBackendHealth(snapshot *truenas.PoolHealthSnapshot) {
	previous := d.backendHealth.Load()
	if previous == nil || previous.Degraded() == snapshot.Degraded() {
		// No transition to confirm (or nothing to compare against yet).
		d.backendHealthPendingFlips.Store(0)
		d.backendHealth.Store(snapshot)
		return
	}

	pending := d.backendHealthPendingFlips.Add(1)
	if pending < backendHealthFlipSamples {
		klog.V(2).Infof("Pool %s health transition (%s -> %s) held for confirmation (%d/%d consecutive samples); "+
			"per-PVC VolumeConditions are unchanged for now",
			snapshot.Pool, previous.Status, snapshot.Status, pending, backendHealthFlipSamples)
		// Keep serving the previous verdict, but carry the fresh sample time so the
		// staleness TTL measures backend liveness, not the age of the verdict.
		held := *previous
		held.SampledAt = snapshot.SampledAt
		d.backendHealth.Store(&held)
		return
	}
	klog.Infof("Pool %s health transition (%s -> %s) confirmed by %d consecutive samples; updating every managed volume's condition",
		snapshot.Pool, previous.Status, snapshot.Status, pending)
	d.backendHealthPendingFlips.Store(0)
	d.backendHealth.Store(snapshot)
}

// poolHealthSnapshot returns the most recent sample, or nil when the poller is
// disabled, has not yet produced one, or the cached one has aged past its TTL.
//
// Returning nil past the TTL is what makes composeVolumeCondition fall back to
// the pre-GF5 dataset-only condition instead of asserting hours-old pool state
// as current fact.
func (d *Driver) poolHealthSnapshot() *truenas.PoolHealthSnapshot {
	snapshot := d.backendHealth.Load()
	if snapshot == nil {
		return nil
	}
	if !snapshot.SampledAt.IsZero() && time.Since(snapshot.SampledAt) > d.backendHealthTTL() {
		return nil
	}
	return snapshot
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
