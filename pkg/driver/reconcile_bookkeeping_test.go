package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// enableBookkeeping turns on the Fix 4b bookkeeping-dataset relocation for a test
// driver. CleanupParent stays off unless a test opts in.
func enableBookkeeping(d *Driver) {
	enabled := true
	d.config.Reconcile.Bookkeeping.Enabled = &enabled
}

func createParentDataset(t *testing.T, client *truenas.MockClient) {
	t.Helper()
	_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
}

// TestBookkeepingLedgerWriteRelocatesToChildDataset proves that with the
// relocation enabled, new tombstone ledger entries are written to the dedicated
// bookkeeping child dataset and NOT the inheritable parent.
func TestBookkeepingLedgerWriteRelocatesToChildDataset(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)
	createParentDataset(t, client)
	enableBookkeeping(d)

	entry := tombstoneLedgerEntry{
		Version:   tombstoneLedgerVersion,
		Snapshot:  "pool/parent/vol@snap-csi-deleted-1",
		Dataset:   "pool/parent/vol",
		RenamedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, entry))

	child, err := client.DatasetGet(ctx, d.bookkeepingDatasetName())
	require.NoError(t, err, "the bookkeeping child dataset must be created lazily on first write")
	require.Len(t, tombstoneLedgerFromDataset(child), 1, "the ledger entry must live on the child")

	parent, err := client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	assert.Empty(t, tombstoneLedgerFromDataset(parent), "the parent must not carry the relocated ledger entry")
}

// TestBookkeepingLedgerWriteStaysOnParentWhenDisabled proves the default (flag
// off) behavior is unchanged: the ledger entry is written to the parent.
func TestBookkeepingLedgerWriteStaysOnParentWhenDisabled(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)
	createParentDataset(t, client)

	entry := tombstoneLedgerEntry{
		Version:   tombstoneLedgerVersion,
		Snapshot:  "pool/parent/vol@snap-csi-deleted-1",
		Dataset:   "pool/parent/vol",
		RenamedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, entry))

	parent, err := client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	assert.Len(t, tombstoneLedgerFromDataset(parent), 1, "flag off: the ledger entry stays on the parent (v1.2.28 behavior)")
}

// TestBookkeepingInflightMarkerDualRead proves reads consult both locations: a
// marker written before the relocation (parent only) is still found after the
// relocation is enabled, and a marker written after (child only) is found too.
func TestBookkeepingInflightMarkerDualRead(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)
	createParentDataset(t, client)

	marker := inflightMarker{
		Version:   inflightMarkerVersion,
		Instance:  "test-instance",
		Dataset:   "pool/parent/vol-a",
		Mode:      inflightModeClone,
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}

	// Relocation disabled: the marker lands on the parent and reads back.
	require.NoError(t, d.writeInflightMarker(ctx, marker))
	got, err := d.readInflightMarker(ctx, "vol-a")
	require.NoError(t, err)
	require.NotNil(t, got, "flag off: marker on parent must be read")

	// Enable relocation: the parent-only marker is still found via dual-read.
	enableBookkeeping(d)
	got, err = d.readInflightMarker(ctx, "vol-a")
	require.NoError(t, err)
	assert.NotNil(t, got, "dual-read must find a parent-only marker after relocation is enabled")

	// A marker written after relocation lands on the child and reads back.
	markerB := marker
	markerB.Dataset = "pool/parent/vol-b"
	markerB.Nonce = "distinct-nonce"
	require.NoError(t, d.writeInflightMarker(ctx, markerB))
	child, err := client.DatasetGet(ctx, d.bookkeepingDatasetName())
	require.NoError(t, err)
	_, inChild := child.UserProperties[inflightMarkerKey("vol-b")]
	assert.True(t, inChild, "post-relocation marker must be written to the child dataset")
	gotB, err := d.readInflightMarker(ctx, "vol-b")
	require.NoError(t, err)
	assert.NotNil(t, gotB, "child-only marker must be read")
}

// TestBookkeepingMigration walks the three migration states: parent-only →
// both (additive copy, CleanupParent off) → child-only (CleanupParent on). The
// copy is strictly lossless: the parent entry is removed only after a confirmed
// copy.
func TestBookkeepingMigration(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)
	createParentDataset(t, client)

	// State 1: a ledger entry on the parent only (written before relocation).
	entry := tombstoneLedgerEntry{
		Version:   tombstoneLedgerVersion,
		Snapshot:  "pool/parent/vol@snap-csi-deleted-1",
		Dataset:   "pool/parent/vol",
		RenamedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, entry))
	key := tombstoneLedgerKey(entry.Snapshot)

	// State 2: enable relocation WITHOUT cleanup — migration copies to the child
	// and the parent entry remains (lossless; an older controller still reads it).
	enableBookkeeping(d)
	parent, err := client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	d.migrateParentBookkeeping(ctx, parent)

	child, err := client.DatasetGet(ctx, d.bookkeepingDatasetName())
	require.NoError(t, err)
	_, inChild := child.UserProperties[key]
	assert.True(t, inChild, "migration must copy the parent entry to the child")
	parent, err = client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	_, inParent := parent.UserProperties[key]
	assert.True(t, inParent, "without CleanupParent the parent entry must remain")

	// State 3: enable cleanup — the confirmed-copied parent entry is removed, the
	// child copy survives.
	d.config.Reconcile.Bookkeeping.CleanupParent = true
	parent, err = client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	d.migrateParentBookkeeping(ctx, parent)

	parent, err = client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	_, stillInParent := parent.UserProperties[key]
	assert.False(t, stillInParent, "with CleanupParent the confirmed-copied parent entry must be removed")
	child, err = client.DatasetGet(ctx, d.bookkeepingDatasetName())
	require.NoError(t, err)
	_, stillInChild := child.UserProperties[key]
	assert.True(t, stillInChild, "the child copy must survive parent cleanup")

	// The migrated entry is still readable via dual-read after the parent is clean.
	ledger := tombstoneLedgerFromDataset(child)
	require.Len(t, ledger, 1)
	assert.Equal(t, entry.Snapshot, ledger[key].Snapshot)
}
