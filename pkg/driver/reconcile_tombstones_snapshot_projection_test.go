package driver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// N-10, driver side. `Snapshot.CreateTXG` is read by two safety decisions —
// promote refusal and tombstone ledger identity — and it arrives as a TOP-LEVEL
// field of a PROJECTED read (zfs.resource.snapshot.query). That it is present on
// that API is an assumption carried over from a measurement taken on the DATASET
// resource API; the drill (step 1d) settles it.
//
// These tests do not assume either way. They read snapshots back THROUGH the
// client under the projection model, and pin both halves: with createtxg the
// guards work, and without it they degrade CLOSED — never open.

// snapshotProjectionTestClient returns a mock that models the real snapshot
// projection, optionally with the UNPROBED "createtxg absent" shape.
func snapshotProjectionTestClient(t *testing.T, createTXGAbsent bool) *truenas.MockClient {
	t.Helper()
	client := truenas.NewMockClient()
	client.ModelQueryProjection = true
	client.ModelSnapshotCreateTXGAbsent = createTXGAbsent
	return client
}

func addProjectedSnapshot(t *testing.T, client *truenas.MockClient, dataset, name string, creationUnix int64) *truenas.Snapshot {
	t.Helper()
	ctx := context.Background()
	if _, err := client.DatasetGet(ctx, dataset); err != nil {
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: dataset, Type: "FILESYSTEM"})
		require.NoError(t, err)
	}
	snap, err := client.SnapshotCreate(ctx, dataset, name, nil)
	require.NoError(t, err)
	client.SetSnapshotCreationTime(snap.ID, creationUnix)
	read, err := client.SnapshotGet(ctx, snap.ID)
	require.NoError(t, err)
	return read
}

// TestPromoteRefusesWhenCreateTXGIsAbsent pins the fail-closed half. If the
// snapshot resource API does not carry createtxg, every candidate decodes to 0
// and the promote migration set is unprovable — so promote must REFUSE, which is
// what the release note claims.
//
// PRE-FIX PROOF: this is the assumption's failure mode, which nothing modeled
// before — with ModelSnapshotCreateTXGAbsent removed (the pre-N-10 mock, which
// always populated CreateTXG) the "absent" case below cannot be constructed at
// all, so the claim was untested rather than true.
func TestPromoteRefusesWhenCreateTXGIsAbsent(t *testing.T) {
	const dataset = "pool/parent/promote-src"

	t.Run("createtxg present: a migration set is provable", func(t *testing.T) {
		client := snapshotProjectionTestClient(t, false)
		origin := addProjectedSnapshot(t, client, dataset, "origin-snap", 1754150400)
		older := addProjectedSnapshot(t, client, dataset, "older-snap", 1754150300)
		require.NotZero(t, origin.CreateTXG, "the projected read carries the top-level createtxg")

		migrating, refusal := migratingSnapshots([]*truenas.Snapshot{origin, older}, origin.ID)
		assert.Empty(t, refusal)
		assert.NotEmpty(t, migrating, "with createtxg the migration set is computable")
	})

	t.Run("createtxg absent: promote refuses rather than guessing", func(t *testing.T) {
		client := snapshotProjectionTestClient(t, true)
		origin := addProjectedSnapshot(t, client, dataset, "origin-snap", 1754150400)
		require.Zero(t, origin.CreateTXG, "the modeled shape: the field the guard reads is simply not there")

		migrating, refusal := migratingSnapshots([]*truenas.Snapshot{origin}, origin.ID)
		assert.Nil(t, migrating)
		assert.Contains(t, refusal, "createtxg",
			"the refusal must name what is missing; silence here would promote on an unprovable set")
	})
}

// TestTombstoneLedgerIdentityDegradesClosedWithoutCreateTXG pins the other
// reader. A v2 ledger entry that RECORDED a createtxg must not match a snapshot
// that exposes none: the entry's stronger identity cannot be satisfied, so the
// answer is "not a match", never "close enough".
func TestTombstoneLedgerIdentityDegradesClosedWithoutCreateTXG(t *testing.T) {
	const dataset = "pool/parent/tombstone-src"
	const creation = int64(1754150400)

	withTXG := snapshotProjectionTestClient(t, false)
	present := addProjectedSnapshot(t, withTXG, dataset, "snap-1", creation)
	require.NotZero(t, present.CreateTXG)

	entry := tombstoneLedgerEntry{
		Version: 2, Snapshot: present.ID, Dataset: dataset,
		CreatedAt: creation, CreateTXG: present.CreateTXG,
	}
	assert.True(t, tombstoneLedgerEntryMatchesSnapshot(entry, present),
		"the same snapshot under a read that carries createtxg matches")

	absentClient := snapshotProjectionTestClient(t, true)
	absent := addProjectedSnapshot(t, absentClient, dataset, "snap-1", creation)
	require.Zero(t, absent.CreateTXG)
	assert.False(t, tombstoneLedgerEntryMatchesSnapshot(entry, absent),
		"a v2 entry with a recorded createtxg must NOT match a snapshot that exposes none")

	// And the documented v1/v2-without-TXG degradation still matches on
	// full-ID-plus-creation-seconds, so this is a strengthening, not a wedge.
	degraded := tombstoneLedgerEntry{Version: 2, Snapshot: absent.ID, Dataset: dataset, CreatedAt: creation}
	assert.True(t, tombstoneLedgerEntryMatchesSnapshot(degraded, absent),
		"an entry written without a createtxg keeps the seconds-only predicate")
}

// TestProjectedSnapshotReadKeepsTheAgeGates proves the PROJECTION half at the
// driver level: creation is projected, so every age gate keeps working. If it
// were dropped from snapshotResourceQueryProperties this fails — which is the
// D-3 shape transplanted onto snapshots.
func TestProjectedSnapshotReadKeepsTheAgeGates(t *testing.T) {
	client := snapshotProjectionTestClient(t, false)
	snap := addProjectedSnapshot(t, client, "pool/parent/age-src", "snap-1", 1754150400)
	client.SetSnapshotUsedBytes(snap.ID, 4096)
	snap, err := client.SnapshotGet(context.Background(), snap.ID)
	require.NoError(t, err)

	assert.Equal(t, int64(1754150400), snap.GetCreationTime(),
		"the tombstone/spent-restore age gates and the scheduled-snapshot predicate read this")
	assert.NotZero(t, snap.GetSnapshotSize(), "and the reaper's reclaimable-bytes accounting reads this")
}
