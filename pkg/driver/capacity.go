package driver

import (
	"context"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// capacityGaugeCallTimeout bounds each per-tick pool.dataset.query so a hung
// backend call can never hold the gauge goroutine (and delay every later sample)
// for longer than a fixed window, independent of the configured poll interval.
const capacityGaugeCallTimeout = 30 * time.Second

// startCapacityGauges launches the controller-only pool-capacity gauge poll loop
// when capacity.gaugeEnabled is set. Each tick issues exactly ONE bounded
// pool.dataset.query against the parent dataset and publishes
// scale_csi_pool_available_bytes / scale_csi_pool_capacity_bytes; default off
// means zero new API calls. The loop mirrors the orphan-reconcile ticker idiom
// (context cancel + WaitGroup) so Stop joins it cleanly.
func (d *Driver) startCapacityGauges() {
	if d.config == nil || !d.config.Capacity.GaugeEnabled {
		return
	}
	interval, err := d.config.Capacity.GaugeIntervalDuration()
	if err != nil || interval <= 0 {
		klog.Errorf("Pool-capacity gauges disabled due to invalid interval %q: %v", d.config.Capacity.GaugeInterval, err)
		return
	}
	parent := strings.TrimSuffix(d.config.ZFS.DatasetParentName, "/")
	if parent == "" {
		klog.Error("Pool-capacity gauges disabled: no zfs.parentDataset configured")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	// (C7) capacityStateMu + capacityStopped close the race where a Stop()
	// landing between this function's entry and the plain assignment below used
	// to observe a nil capacityCancel, skip cancellation, and let this loop
	// launch anyway, calling d.truenasClient.DatasetGet concurrently with
	// GracefulStop()/truenasClient.Close(). A Stop() that already won the race
	// is visible here under the same lock, so this call exits before ever
	// launching the loop.
	d.capacityStateMu.Lock()
	if d.capacityStopped || d.capacityCancel != nil {
		d.capacityStateMu.Unlock()
		cancel()
		return
	}
	d.capacityCancel = cancel
	d.capacityStateMu.Unlock()
	d.capacityWg.Add(1)
	go func() {
		defer d.capacityWg.Done()
		klog.Infof("Pool-capacity gauges started: interval=%v parent=%s", interval, parent)
		run := func() {
			callCtx, callCancel := context.WithTimeout(ctx, capacityGaugeCallTimeout)
			defer callCancel()
			ds, getErr := d.truenasClient.DatasetGet(callCtx, parent)
			if getErr != nil {
				if ctx.Err() == nil {
					klog.Warningf("Pool-capacity gauge sample failed: %v", getErr)
				}
				return
			}
			available := parsedPropertyBytes(ds.Available.Parsed)
			used := parsedPropertyBytes(ds.Used.Parsed)
			SetPoolCapacityMetrics(capacityPoolLabel(ds.Pool, parent), parent, float64(available), float64(used+available))
		}

		// Populate immediately rather than leaving the gauges unknown until the
		// first interval elapses.
		run()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				run()
			case <-ctx.Done():
				klog.Info("Pool-capacity gauges stopped")
				return
			}
		}
	}()
}

func (d *Driver) stopCapacityGauges() {
	// (C7) Terminal: capacityStopped=true is recorded under the same lock
	// startCapacityGauges checks before assigning capacityCancel, so a Stop()
	// that wins the race prevents the loop from EVER starting instead of the
	// two racing on a plain nil check.
	d.capacityStateMu.Lock()
	d.capacityStopped = true
	cancel := d.capacityCancel
	d.capacityCancel = nil
	d.capacityStateMu.Unlock()
	if cancel != nil {
		cancel()
	}
	d.capacityWg.Wait()
}

// parsedPropertyBytes coerces a ZFS property's parsed value (a float64 byte count
// from pool.dataset.query) to int64, treating any absent/non-numeric value as zero.
func parsedPropertyBytes(parsed interface{}) int64 {
	if v, ok := parsed.(float64); ok {
		return int64(v)
	}
	return 0
}

// capacityPoolLabel resolves the {pool} label: the dataset's reported pool name,
// falling back to the first path segment of the parent dataset.
func capacityPoolLabel(dsPool, parent string) string {
	if dsPool != "" {
		return dsPool
	}
	if idx := strings.Index(parent, "/"); idx > 0 {
		return parent[:idx]
	}
	return parent
}
