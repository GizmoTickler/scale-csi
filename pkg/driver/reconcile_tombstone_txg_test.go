package driver

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func newTombstoneTXGTestDriver(client truenas.ClientInterface) *Driver {
	return &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
		},
		truenasClient: client,
	}
}

func seedTXGTombstone(
	t *testing.T,
	d *Driver,
	client *truenas.MockClient,
	sourceID, tombstoneName string,
	createdAt int64,
	createTXG uint64,
) *truenas.Snapshot {
	t.Helper()
	ctx := context.Background()
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/" + sourceID,
		Type: "FILESYSTEM",
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropDriverInstanceID, d.driverInstanceID()))
	snapshot, err := client.SnapshotCreate(ctx, source.Name, tombstoneName, nil)
	require.NoError(t, err)
	snapshot.Properties["creation"] = map[string]interface{}{"parsed": float64(createdAt)}
	snapshot.CreateTXG = createTXG
	return snapshot
}

func txgTombstoneObject(snapshot *truenas.Snapshot) ReconcileObject {
	return ReconcileObject{
		ID:             snapshot.ID,
		BackendID:      snapshot.ID,
		SourceVolumeID: path.Base(snapshot.Dataset),
		CreatedAt:      time.Unix(snapshot.GetCreationTime(), 0),
	}
}

func TestTombstoneLedgerV2RefusesSameSecondRecreatedSnapshot(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newTombstoneTXGTestDriver(client)
	mustCreateParentDataset(t, client)
	createdAt := time.Now().Add(-48 * time.Hour).Unix()
	recreated := seedTXGTombstone(t, d, client, "source", "snap-csi-deleted-1", createdAt, 137)
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, tombstoneLedgerEntry{
		Version:   2,
		Snapshot:  recreated.ID,
		Dataset:   recreated.Dataset,
		CreatedAt: createdAt,
		CreateTXG: 100,
		RenamedAt: time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano),
	}))

	retire := &tombstoneRetirementBatch{}
	reaped, reason := d.reapTombstoneSnapshot(ctx, txgTombstoneObject(recreated), time.Hour, retire)
	assert.False(t, reaped)
	assert.Contains(t, reason, "creation identity")
	assert.Empty(t, retire.snapshotIDs)
	_, err := client.SnapshotGet(ctx, recreated.ID)
	require.NoError(t, err, "same-second full-ID reuse with a different TXG must survive")
}

func TestTombstoneLedgerV1KeepsSecondsOnlyBehavior(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newTombstoneTXGTestDriver(client)
	mustCreateParentDataset(t, client)
	createdAt := time.Now().Add(-48 * time.Hour).Unix()
	snapshot := seedTXGTombstone(t, d, client, "source", "snap-csi-deleted-1", createdAt, 137)
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, tombstoneLedgerEntry{
		Version:   1,
		Snapshot:  snapshot.ID,
		Dataset:   snapshot.Dataset,
		CreatedAt: createdAt,
		RenamedAt: time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano),
	}))

	retire := &tombstoneRetirementBatch{}
	reaped, reason := d.reapTombstoneSnapshot(ctx, txgTombstoneObject(snapshot), time.Hour, retire)
	assert.True(t, reaped, reason)
	assert.Equal(t, []string{snapshot.ID}, retire.snapshotIDs)
	_, err := client.SnapshotGet(ctx, snapshot.ID)
	assert.True(t, truenas.IsNotFoundError(err), "v1 must retain its pre-v2 seconds-only semantics")
}

func TestTombstoneLedgerMixedV1V2EntriesReapIndependently(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newTombstoneTXGTestDriver(client)
	mustCreateParentDataset(t, client)
	createdAt := time.Now().Add(-48 * time.Hour).Unix()
	v1 := seedTXGTombstone(t, d, client, "source-v1", "one-csi-deleted-1", createdAt, 201)
	v2 := seedTXGTombstone(t, d, client, "source-v2", "two-csi-deleted-2", createdAt, 302)
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, tombstoneLedgerEntry{
		Version:   1,
		Snapshot:  v1.ID,
		Dataset:   v1.Dataset,
		CreatedAt: createdAt,
		RenamedAt: time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano),
	}))
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, tombstoneLedgerEntry{
		Version:   2,
		Snapshot:  v2.ID,
		Dataset:   v2.Dataset,
		CreatedAt: createdAt,
		CreateTXG: v2.CreateTXG,
		RenamedAt: time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano),
	}))

	parent, err := client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	ledger := tombstoneLedgerFromDataset(parent)
	require.Len(t, ledger, 2, "the dual-path parser must retain both v1 and v2 entries")
	assert.Equal(t, 1, ledger[tombstoneLedgerKey(v1.ID)].Version)
	assert.Equal(t, 2, ledger[tombstoneLedgerKey(v2.ID)].Version)

	retire := &tombstoneRetirementBatch{}
	for _, snapshot := range []*truenas.Snapshot{v1, v2} {
		reaped, reason := d.reapTombstoneSnapshot(ctx, txgTombstoneObject(snapshot), time.Hour, retire)
		assert.True(t, reaped, reason)
	}
	assert.ElementsMatch(t, []string{v1.ID, v2.ID}, retire.snapshotIDs)
}

func TestTombstoneLedgerV2ZeroTXGDegradesToCreationSeconds(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newTombstoneTXGTestDriver(client)
	mustCreateParentDataset(t, client)
	createdAt := time.Now().Add(-48 * time.Hour).Unix()
	snapshot := seedTXGTombstone(t, d, client, "source", "snap-csi-deleted-1", createdAt, 0)
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, tombstoneLedgerEntry{
		Version:   2,
		Snapshot:  snapshot.ID,
		Dataset:   snapshot.Dataset,
		CreatedAt: createdAt,
		RenamedAt: time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano),
	}))

	reaped, reason := d.reapTombstoneSnapshot(ctx, txgTombstoneObject(snapshot), time.Hour, &tombstoneRetirementBatch{})
	assert.True(t, reaped, reason)
}

func TestHandleSnapshotClonesWritesLedgerV2CreateTXG(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	client.NoDeferredSnapshotDestroy = true
	d := newTombstoneTXGTestDriver(client)
	mustCreateParentDataset(t, client)
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM"})
	require.NoError(t, err)
	snapshot, err := client.SnapshotCreate(ctx, source.Name, "snap-1", nil)
	require.NoError(t, err)
	snapshot.CreateTXG = 4242
	require.NoError(t, client.SnapshotClone(ctx, snapshot.ID, "pool/parent/restored"))

	require.NoError(t, d.handleSnapshotClones(ctx, snapshot))
	tombstoneID := findTombstoneID(t, client, source.Name)
	require.NotEmpty(t, tombstoneID)
	parent, err := client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	entry, ok := tombstoneLedgerFromDataset(parent)[tombstoneLedgerKey(tombstoneID)]
	require.True(t, ok)
	assert.Equal(t, tombstoneLedgerVersion, entry.Version)
	assert.Equal(t, uint64(4242), entry.CreateTXG)
}

func TestTombstoneScanFallbackRevalidatesCreateTXG(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newTombstoneTXGTestDriver(client)
	mustCreateParentDataset(t, client)
	createdAt := time.Now().Add(-48 * time.Hour).Unix()
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropDriverInstanceID, d.driverInstanceID()))
	snapshot, err := client.SnapshotCreate(ctx, source.Name, "snap-1", map[string]string{
		PropCSISnapshotName:  "snap-1",
		PropDriverInstanceID: d.driverInstanceID(),
	})
	require.NoError(t, err)
	snapshot.Properties["creation"] = map[string]interface{}{"parsed": float64(createdAt)}
	snapshot.CreateTXG = 500
	tombstoneName := snapshotTombstoneName(source.Name, "snap-1", 1)
	require.NoError(t, client.SnapshotRename(ctx, snapshot.ID, tombstoneName))
	tombstone, err := client.SnapshotGet(ctx, source.Name+"@"+tombstoneName)
	require.NoError(t, err)

	report := &ReconcileReport{}
	d.detectTombstonesByScanFallback(
		ctx,
		time.Now(),
		[]*truenas.Snapshot{tombstone},
		map[string]tombstoneLedgerEntry{},
		time.Hour,
		report,
	)
	require.Len(t, report.TombstoneSnapshots, 1)
	assert.Equal(t, uint64(500), report.TombstoneSnapshots[0].tombstoneCreateTXG)

	// Same full ID and creation second, but a fresh read now observes a different
	// non-reusable TXG: the scan-fallback authorization must be refused.
	tombstone.CreateTXG = 501
	reaped, reason := d.reapTombstoneSnapshot(
		ctx,
		report.TombstoneSnapshots[0],
		time.Hour,
		&tombstoneRetirementBatch{},
	)
	assert.False(t, reaped)
	assert.Contains(t, reason, "CreateTXG")
	_, err = client.SnapshotGet(ctx, tombstone.ID)
	require.NoError(t, err)
}
