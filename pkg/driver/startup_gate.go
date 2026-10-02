package driver

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The per-volume publish gate. Strict fencing used to refuse every
// ControllerPublishVolume until startup had converged every attached volume in
// the cluster, so a drain that overlapped a controller restart waited for the
// slowest volume. The gate now holds a publish only for its own volume, and
// only while that volume is still pending: it had a VolumeAttachment in a
// startup pass's snapshot and has not converged since. A pending volume's
// publish converges it itself, under the volume lock it already holds, then
// goes on. A volume with no VolumeAttachment in the snapshot is not pending:
// its publish is the whole of its convergence, as the publish path revokes a
// stale record (takeOverStaleSingleNodePublication) and enforces the exact
// strict allowlist (applyBackendFence) before it grants. Until a pass has
// taken its snapshot, every publish is refused as before.
//
// Global readiness (d.ready) is unchanged: it latches when a pass converges,
// and gates CreateVolume, ControllerExpandVolume and the other provisioning
// calls as before.

// startupGateTrack records a pass's snapshot. A full pass replaces the
// pending set with its volumes; a targeted pass adds its own.
func (d *Driver) startupGateTrack(volumes map[string]*startupFencingVolume, volumeIDs []string, full bool) {
	d.startupGateMu.Lock()
	defer d.startupGateMu.Unlock()
	if full || d.startupGatePending == nil {
		d.startupGatePending = make(map[string]*startupFencingVolume, len(volumeIDs))
	}
	for _, volumeID := range volumeIDs {
		d.startupGatePending[volumeID] = volumes[volumeID]
	}
	d.startupGateSnapshot = true
}

// startupGateStillPending is whether volumeID still waits to converge.
func (d *Driver) startupGateStillPending(volumeID string) bool {
	d.startupGateMu.Lock()
	defer d.startupGateMu.Unlock()
	_, pending := d.startupGatePending[volumeID]
	return pending
}

// startupGateSettle records that volumeID converged from volume, the pending
// entry the caller read. The caller holds the volume lock. A later pass that
// replaced the entry with its own snapshot in the meantime keeps it: that
// snapshot has not been converged yet.
func (d *Driver) startupGateSettle(volumeID string, volume *startupFencingVolume) {
	d.startupGateMu.Lock()
	defer d.startupGateMu.Unlock()
	if d.startupGatePending[volumeID] == volume {
		delete(d.startupGatePending, volumeID)
	}
}

// startupPublishGate is ControllerPublishVolume's strict-startup check. The
// caller holds the volume lock. It returns nil once the publish may go on:
// readiness has latched, or the volume is not pending, or this call has just
// converged it.
func (d *Driver) startupPublishGate(ctx context.Context, volumeID string) error {
	if d.config == nil || d.config.Fencing.Mode != FencingModeStrict || !d.runController || d.ready.Load() {
		return nil
	}
	d.startupGateMu.Lock()
	snapshot := d.startupGateSnapshot
	volume := d.startupGatePending[volumeID]
	d.startupGateMu.Unlock()
	if !snapshot {
		return status.Error(codes.Unavailable, "strict fencing startup reconciliation has not converged; retry this controller operation")
	}
	if volume == nil {
		return nil
	}
	if err := d.reconcileStartupFencingVolumeLocked(ctx, volume); err != nil {
		return status.Errorf(codes.Unavailable,
			"strict fencing startup reconciliation has not converged for volume %s; retry this controller operation: %v", volumeID, err)
	}
	d.startupGateSettle(volumeID, volume)
	return nil
}
