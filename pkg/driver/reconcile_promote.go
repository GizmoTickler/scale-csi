package driver

import (
	"context"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// reconcilePromoteRestoredClones is the GF2/E3 background step that frees
// clone-restored volumes from their origin-snapshot pin. A `snapshotRestoreMode:
// clone` restore pins its CSI source snapshot as the ZFS origin forever, which
// keeps the source snapshot (and its source volume) from ever being reclaimed.
// Promoting the clone inverts the dependency so the tombstone reaper can finally
// reclaim the source snapshot and the source volume becomes destroyable (P3).
//
// Gated on zfs.promoteRestoredClones (default false). The step promotes a clone
// ONLY when it is the SOLE dependent of its origin snapshot (R3 ordering rule):
// promoting re-parents every sibling clone onto the promoted volume, coupling
// their lifecycles, so it is unsafe while other clones share the origin. A clone
// that is already independent (empty origin, e.g. previously promoted) is skipped
// — promote is idempotent and crash-safe (a single atomic ZFS operation).
func (d *Driver) reconcilePromoteRestoredClones(ctx context.Context, datasets []*truenas.Dataset, report *ReconcileReport) {
	if !d.config.ZFS.PromoteRestoredClones {
		return
	}

	// Count dependents per origin snapshot so the sole-dependent gate is exact.
	originDependents := make(map[string]int)
	for _, ds := range datasets {
		if origin := datasetOriginSnapshotID(ds); origin != "" {
			originDependents[origin]++
		}
	}

	for _, ds := range datasets {
		if ctx.Err() != nil {
			return
		}
		if !datasetHasLocalUserProperty(ds, PropDriverInstanceID, d.driverInstanceID()) {
			continue // not owned by this driver instance
		}
		if prop, ok := ds.UserProperties[PropVolumeContentSourceType]; !ok || prop.Value != "snapshot" {
			continue // not a snapshot-restored clone
		}
		origin := datasetOriginSnapshotID(ds)
		if origin == "" {
			continue // already independent (promoted earlier or never a clone)
		}
		if originDependents[origin] != 1 {
			// Sibling clones share this origin; promoting would re-parent them (P3).
			klog.V(4).Infof("GF2/E3: skipping promote of %s — origin %s has %d dependent clones", ds.Name, origin, originDependents[origin])
			continue
		}
		if err := d.truenasClient.DatasetPromote(ctx, ds.Name); err != nil {
			RecordClonePromoted(err)
			klog.Warningf("GF2/E3: failed to promote clone-restored volume %s (will retry next pass): %v", ds.Name, err)
			continue
		}
		RecordClonePromoted(nil)
		report.PromotedCloneCount++
		klog.Infof("GF2/E3: promoted clone-restored volume %s; origin snapshot %s pin released", ds.Name, origin)
	}
}
