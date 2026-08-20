package driver

import (
	"context"
	"path"
	"sort"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// migrateLegacyPropertyNamespace re-stamps volume datasets that still carry
// LOCAL truenas-csi:* user properties (written by releases before the
// scale-csi: namespace rename) under the canonical namespace, then removes the
// legacy spelling. Reads never depend on this sweep — every wire decoder folds
// legacy keys onto their canonical twins (pkg/truenas prop_ns.go) — so the
// sweep is pure hygiene: it converges the on-disk state so the legacy fold
// eventually has nothing left to do on datasets.
//
// Like adoptLegacyOwnershipStamps, this is a WRITE that runs in detection mode
// (NOT gated by opts.Delete): it copies values this driver already owns and
// deletes nothing but the redundant legacy spelling of those same values, and
// it is capped per pass by reconcile.repair.maxPerRun as a blast-radius bound.
//
// Scope rules:
//   - Volume datasets only. The parent and bookkeeping datasets hold ONLY
//     transient legacy entries (tombstone-ledger chunks, in-flight markers)
//     whose natural lifecycle removes both spellings via the widened removal
//     path, and copying a ledger entry here could resurrect one that a
//     concurrent reap just removed — those entries have no per-volume lock to
//     serialize on, so they are deliberately left to age out.
//   - Only LOCAL legacy properties migrate. An inherited value (a clone
//     reading its origin snapshot's stamps) is not this dataset's property:
//     writing it locally would freeze inheritance and forge ownership, and
//     "removing" it is meaningless.
//   - Snapshots never migrate: TrueNAS 26.0 has no working property mutation
//     for existing snapshots (pool.snapshot.update silently drops the request),
//     so the decode-time fold is permanent for them.
//
// Crash safety: the canonical write lands before the legacy removal, and reads
// accept both spellings with canonical-wins precedence, so every intermediate
// state (both present, only canonical, only legacy) reads identically and a
// re-run converges.
func (d *Driver) migrateLegacyPropertyNamespace(ctx context.Context, datasets []*truenas.Dataset, report *ReconcileReport, maxPerRun int) {
	parentName := d.parentDatasetName()
	for _, ds := range datasets {
		if maxPerRun > 0 && len(report.MigratedPropertyNamespaces) >= maxPerRun {
			break
		}
		if ds == nil || len(ds.LegacyCSIProperties) == 0 {
			continue
		}
		volumeID := path.Base(ds.Name)
		if !datasetStrictlyBelowParent(ds.Name, parentName) || !validVolumeIDLeaf(volumeID) || volumeID == bookkeepingDatasetLeaf {
			continue
		}
		migrated, err := d.migrateDatasetPropertyNamespace(ctx, ds.Name, volumeID)
		if err != nil {
			d.recordReconcileObjectFailure("property_namespace_migration", ds.Name, err)
			continue
		}
		if !migrated {
			continue
		}
		report.MigratedPropertyNamespaces = append(report.MigratedPropertyNamespaces, volumeID)
		klog.Infof("Orphan reconcile: migrated legacy truenas-csi:* stamps on %s to the scale-csi: namespace", ds.Name)
	}
}

// migrateDatasetPropertyNamespace performs one dataset's migration under the
// per-volume operation lock (the same lock every stamp-writing volume
// operation holds, so no publish/unpublish/create/delete can interleave), with
// a fresh source-bearing re-read as the only input to the write. It returns
// migrated=false (no error) when the locked re-read finds nothing left to do.
func (d *Driver) migrateDatasetPropertyNamespace(ctx context.Context, datasetName, volumeID string) (bool, error) {
	lockKey := volumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		// A live volume operation owns the dataset right now; next pass retries.
		return false, nil
	}
	defer d.releaseOperationLock(lockKey)

	// The nominating listing may be the sourceless resource-query view; the
	// locked pool.dataset.query re-read is the authoritative, source-bearing
	// input for the local-only rule.
	fresh, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	writes := make(map[string]string)
	removals := make([]string, 0, len(fresh.LegacyCSIProperties))
	for legacyKey, legacyProp := range fresh.LegacyCSIProperties {
		if !isLocalUserPropertySource(legacyProp.Source) {
			continue
		}
		canonicalKey, ok := truenas.CanonicalCSIPropertyKey(legacyKey)
		if !ok {
			continue
		}
		// The normalized map already resolved the winner between the two
		// spellings (local beats inherited, canonical beats legacy on ties), so
		// writing that winner canonically is idempotent whether or not the
		// canonical key was already on disk.
		winner, ok := fresh.UserProperties[canonicalKey]
		if !ok || winner.Value == "" {
			continue
		}
		writes[canonicalKey] = winner.Value
		removals = append(removals, legacyKey)
	}
	if len(removals) == 0 {
		return false, nil
	}
	if len(writes) > 0 {
		if err := d.truenasClient.DatasetSetUserProperties(ctx, datasetName, writes); err != nil {
			return false, err
		}
	}
	// Legacy-prefixed keys pass through the removal widening unchanged, so this
	// removes exactly the legacy spellings whose values were just re-stamped.
	sort.Strings(removals)
	if err := d.truenasClient.DatasetRemoveUserProperties(ctx, datasetName, removals); err != nil {
		return false, err
	}
	return true, nil
}
