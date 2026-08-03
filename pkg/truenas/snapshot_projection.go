package truenas

import (
	"maps"
	"slices"
	"strings"
)

// PROJECTION FIDELITY, SNAPSHOT EDITION.
//
// The dataset half of this problem is in dataset_projection.go: a projected read
// makes unrequested fields ABSENT, absent decodes to the Go zero value, and a
// fully-populated mock hides it until hardware says otherwise (GF1 re-drill
// D-3). The snapshot reads have exactly the same shape and had none of the same
// modeling:
//
//	zfs.resource.snapshot.query {paths, recursive, properties: [...],
//	                             get_user_properties: true}
//
// snapshotResourceQueryProperties is that projection, in ONE place, so it can be
// pinned on the wire and micro-reverted in a proof.
//
// WHAT THE PROJECTION CONTROLS, AND WHAT IT DOES NOT. `properties` selects ZFS
// PROPERTIES, which arrive in the response's `properties` map (Snapshot.Properties
// plus the ":"-bearing user properties). It does NOT select the response's
// top-level fields — `name`, `id`, `dataset`, `pool`, `type`, `snapshot_name` and
// `createtxg` decode from the top level (see rawSnapshot). So the model strips
// properties, never top-level fields.
//
// ★ THE ONE UNVERIFIED ASSUMPTION, STATED PLAINLY (N-10). ★ Snapshot.CreateTXG
// has real readers in safety-critical reasoning — tombstone identity matching
// (reconcile_tombstones.go) and promote refusal (reconcile_promote.go, which
// REFUSES when createtxg is missing). It is assumed present on every
// zfs.resource.snapshot.query response. The re-drill's key enumeration that
// includes `createtxg` was measured on the DATASET resource API
// (zfs.resource.query), NOT on the snapshot one, so for snapshots this is
// UNPROBED. It is not asserted as fact anywhere in this package: the drill
// (step 1d) measures it, and TestSnapshotGuardsFailClosedWithoutCreateTXG pins
// what the driver does if the assumption is wrong — both guards degrade CLOSED.
//
// projectSnapshotLikeResourceQuery is the test-side structural fix, mirroring
// projectDatasetLikePoolQuery: it strips a fully-populated snapshot down to what
// the CURRENT projection would actually deliver, so a reader of an unprojected
// property fails a unit test instead of shipping.

// snapshotResourceQueryProperties is the property projection EVERY
// zfs.resource.snapshot.query read carries.
//
//   - used     — Snapshot.GetSnapshotSize(); reported in CSI snapshot responses
//     and in the tombstone reaper's reclaimable-bytes accounting.
//   - creation — Snapshot.GetCreationTime(); the driver-scheduled-snapshot
//     ownership predicate (snapshot_schedule.go) and every age gate
//     (tombstones, spent-restore) read it, and each FAILS CLOSED
//     without it.
//
// Deliberately NOT projected: `clones`. TrueNAS 26.0 no longer projects it
// through either snapshot read API (see Snapshot.GetClones), so asking would be
// an unverified request shape; the authoritative dependency check is the
// dataset-origin scan (snapshotDependentClones), and GetClones is only a
// pre-25.04 fast path that degrades to it.
var snapshotResourceQueryProperties = []string{"used", "creation"}

// projectSnapshotLikeResourceQuery returns a copy of snap carrying only what a
// real zfs.resource.snapshot.query response would carry under the given
// projection: the top-level fields (always present) plus the projected
// properties, plus user properties (get_user_properties is always true).
// It never mutates its argument.
func projectSnapshotLikeResourceQuery(snap *Snapshot, projection []string) *Snapshot {
	if snap == nil {
		return nil
	}
	projected := *snap
	projected.Properties = make(map[string]interface{}, len(snap.Properties))
	for key, value := range snap.Properties {
		// A ":"-bearing key is a USER property; those come from
		// get_user_properties:true, not from the projection.
		if strings.Contains(key, ":") || slices.Contains(projection, key) {
			projected.Properties[key] = value
		}
	}
	projected.UserProperties = maps.Clone(snap.UserProperties)
	if projected.UserProperties == nil {
		projected.UserProperties = map[string]UserProperty{}
	}
	return &projected
}
