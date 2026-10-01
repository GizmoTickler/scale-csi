package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// The background import is a writer that needs a single controller: it starts
// only where fencing guarantees one (one replica, Recreate). With fencing off
// a second replica's pass could bring back a record another process removed.
func TestPublicationImportStartsOnlyWithASingleController(t *testing.T) {
	for mode, wantStarted := range map[FencingMode]bool{
		FencingModeOff: false, "": false, FencingModeAdditive: true, FencingModeStrict: true,
	} {
		d, _, store := importTestDriver(t)
		d.publicationStore = store
		d.config.Fencing.Mode = mode
		orig := publicationImportDelay
		publicationImportDelay = time.Hour
		d.startPublicationImport()
		d.publicationImportStateMu.Lock()
		started := d.publicationImportCancel != nil
		d.publicationImportStateMu.Unlock()
		d.stopPublicationImport()
		publicationImportDelay = orig
		assert.Equal(t, wantStarted, started, "fencing %q", mode)
	}
}

// When ZFS wins on age (a rollback, then this release again), a tombstone this
// release wrote for a node ZFS does not name is kept: it can only revoke, and
// it holds the identity a pending revoke needs.
func TestResolveKeepsAKubernetesTombstoneWhenZFSWins(t *testing.T) {
	keyA, keyK := publicationPropertyKey("node-a"), publicationPropertyKey("node-k")
	a := recordAt("node-a", publicationStatePublished, "2026-10-02T00:00:00Z")
	tombstone := recordAt("node-k", publicationStateRemoving, "2026-10-01T00:00:00Z")
	stale := recordAt("node-b", publicationStatePublished, "2026-10-01T00:00:00Z")
	got := resolvePublicationRecords(map[string]publicationRecord{keyA: a},
		map[string]publicationRecord{keyK: tombstone, publicationPropertyKey("node-b"): stale})
	assert.Equal(t, map[string]publicationRecord{keyA: a, keyK: tombstone}, got,
		"the tombstone stays; the published record the older release never saw does not")
}

// The sweep resolves each dataset's two stores as every read does, so the
// record a revoke re-reads is the one it classified.
func TestStaleSweepMergeResolvesByAge(t *testing.T) {
	key := publicationPropertyKey("node-a")
	newer := recordAt("node-a", publicationStateRemoving, "2026-10-02T00:00:00Z")
	older := recordAt("node-a", publicationStatePublished, "2026-10-01T00:00:00Z")
	merged := mergeStaleSweeps(
		staleSweep{candidates: []staleSweepCandidate{{datasetName: "pool/parent/v", records: map[string]publicationRecord{key: newer}}}, recordCount: 1},
		staleSweep{candidates: []staleSweepCandidate{{datasetName: "pool/parent/v", records: map[string]publicationRecord{key: older}}}, recordCount: 1})
	require.Len(t, merged.candidates, 1)
	assert.Equal(t, newer, merged.candidates[0].records[key])
	assert.Equal(t, 2, merged.recordCount)
}

// Records whose datasets are all missing at once, or missing from an empty
// listing, are the shape of a pool not yet imported, not of interrupted
// deletes: they are not forgotten.
func TestStaleSweepDoesNotForgetMassAbsentDatasets(t *testing.T) {
	ctx := context.Background()
	d, client, store := importTestDriver(t)
	for _, name := range []string{"gone-1", "gone-2", "gone-3"} {
		require.NoError(t, store.kube.store(ctx, "pool/parent/"+name, nil, publicationPropertyKey("node-a"), testRecord("node-a", publicationStatePublished)))
	}
	listed := []*truenas.Dataset{addReconcileDataset(client, "kept", time.Now().Add(-time.Hour), true, 1)}
	for _, datasets := range [][]*truenas.Dataset{nil, listed} {
		d.kubernetesStaleSweepCandidates(ctx, store.kube, datasets)
		listing, err := store.kube.all(ctx)
		require.NoError(t, err)
		assert.Len(t, listing.byDataset, 3, "listing of %d datasets: nothing forgotten", len(datasets))
	}
}
