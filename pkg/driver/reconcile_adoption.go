package driver

import (
	"context"
	"fmt"
	"path"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// adoptLegacyOwnershipStamps stamps driver_instance_id onto legacy managed
// datasets that provably belong to this cluster's Bound volumes but predate the
// v1.2.21 ownership stamp (the 2026-07-23 04:00Z incident: the reaper refused
// every age-eligible migration-era tombstone because its source dataset carried
// LOCAL managed_resource + csi_volume_name but NO instance stamp, so the ledger
// entries and -csi-deleted- snapshots accumulated forever).
//
// This is a WRITE that runs in detection mode (it is NOT gated by opts.Delete).
// That is safe and deliberate: it only adds provenance to datasets that are
// provably this cluster's Bound volumes (a live Bound PV of THIS driver
// references them, and they carry this driver's own LOCAL managed_resource +
// csi_volume_name); it deletes nothing; and it is required for the delete-mode
// reaper to ever act on legacy tombstones. Adoptions are capped at maxPerRun
// per pass as a blast-radius bound.
//
// Absolute rule: an existing driver_instance_id of ANY source (local,
// inherited, or foreign) is NEVER overwritten — a dataset stamped by another
// driver instance sharing the pool must not be hijacked.
//
// Residual: a legacy dataset that is NOT currently Bound is never adopted, so
// its tombstones stay refused (fail-safe); operators can bind it or clean it up
// manually.
func (d *Driver) adoptLegacyOwnershipStamps(ctx context.Context, datasets []*truenas.Dataset, report *ReconcileReport, maxPerRun int) {
	boundHandles, ok := d.liveBoundVolumeHandles(ctx)
	if !ok {
		// Fail-safe: a PV-list error or an empty view of this driver's PVs is an
		// API discontinuity, not evidence that adoption is safe — adopt nothing.
		return
	}
	parentName := d.parentDatasetName()
	for _, ds := range datasets {
		if maxPerRun > 0 && len(report.AdoptedStamps) >= maxPerRun {
			break
		}
		if ds == nil {
			continue
		}
		volumeID := path.Base(ds.Name)
		if !datasetStrictlyBelowParent(ds.Name, parentName) || !validVolumeIDLeaf(volumeID) || volumeID == bookkeepingDatasetLeaf {
			continue
		}
		// Presence (does the key exist at all, of ANY source) is decidable from the
		// sourceless listing's flat user_properties, so a dataset that already carries
		// an instance stamp is skipped WITHOUT the source-bearing re-read. In a
		// fully-migrated steady state every volume is stamped, so this drops the
		// adoption pass from N source-bearing GETs to zero. Only the source checks
		// below (LOCAL managed_resource / csi_volume_name) genuinely need source. The
		// overwrite-protection contract is unchanged: the source-bearing GET still
		// runs for actual candidates, the candidate-level presence check still runs
		// after it, and writeAndVerifyAdoptionStamp still re-reads under the
		// per-volume lock and re-proves absence immediately before any write.
		if _, present := datasetUserPropertyProjection(ds, PropDriverInstanceID); present {
			continue
		}
		// Source-bearing re-read (batch-12 DatasetGet pattern): the listing strips
		// property source, so it is never trusted for the ownership/source checks.
		candidate, err := d.truenasClient.DatasetGet(ctx, ds.Name)
		if err != nil {
			if !truenas.IsNotFoundError(err) {
				d.recordReconcileObjectFailure("stamp_adoption", ds.Name, err)
			}
			continue
		}
		// LOCAL managed_resource AND LOCAL csi_volume_name matching the dataset
		// leaf prove this driver created the dataset (a same-name CreateVolume
		// derives the leaf from the PVC UID, which is also the PV volumeHandle).
		if !datasetHasLocalUserProperty(candidate, PropManagedResource, "true") {
			continue
		}
		if !datasetHasLocalUserProperty(candidate, PropCSIVolumeName, volumeID) {
			continue
		}
		// Absolute rule: never overwrite an existing instance stamp of any source.
		if _, present := datasetUserPropertyProjection(candidate, PropDriverInstanceID); present {
			continue
		}
		// A live Bound PV of THIS driver must reference the volume.
		if _, bound := boundHandles[volumeID]; !bound {
			continue
		}
		adopted, adoptErr := d.writeAndVerifyAdoptionStamp(ctx, ds.Name, volumeID)
		if adoptErr != nil {
			d.recordReconcileObjectFailure("stamp_adoption", ds.Name, adoptErr)
			continue
		}
		if !adopted {
			continue
		}
		report.AdoptedStamps = append(report.AdoptedStamps, volumeID)
		klog.Infof("Orphan reconcile: adopted ownership stamp on legacy volume %s", volumeID)
	}
}

// writeAndVerifyAdoptionStamp persists driver_instance_id through the proven
// stampAndMirror user-property write path used at create time, then verifies the
// write with a source-bearing re-read before reporting it adopted. It serializes
// on the per-volume lock and re-proves the stamp is still absent immediately
// before writing, so a concurrent create or peer that stamped the dataset between
// the detection read and the write is never overwritten (the absolute rule). It
// returns adopted=false (no error) when the dataset is already stamped — a
// write-free no-op.
func (d *Driver) writeAndVerifyAdoptionStamp(ctx context.Context, datasetName, volumeID string) (bool, error) {
	lockKey := "volume:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return false, fmt.Errorf("volume operation in progress for %s", volumeID)
	}
	defer d.releaseOperationLock(lockKey)
	fresh, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return false, fmt.Errorf("adoption stamp revalidation failed: %w", err)
	}
	if _, present := datasetUserPropertyProjection(fresh, PropDriverInstanceID); present {
		return false, nil
	}
	if stampErr := stampAndMirror(ctx, d.truenasClient, fresh, datasetName, map[string]string{
		PropDriverInstanceID: d.driverInstanceID(),
	}); stampErr != nil {
		return false, stampErr
	}
	reread, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return false, fmt.Errorf("re-read dataset after adoption stamp: %w", err)
	}
	if !datasetHasLocalUserProperty(reread, PropDriverInstanceID, d.driverInstanceID()) {
		return false, fmt.Errorf("adoption stamp did not persist with a local source on %s", datasetName)
	}
	return true, nil
}
