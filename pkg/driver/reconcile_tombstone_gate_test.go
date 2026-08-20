package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// These tests pin the fix for the 2026-08-19 field incident (pvc-d1d92818): a
// ledger-proven tombstone gated on minOrphanAge (24h) with a daily reconcile
// CronJob missed its first eligible pass by minutes and stalled the volume's
// DeleteVolume for 24-48h, with the age-gated skip invisible in the logs.
// Ledger-proven tombstones now use the short tombstoneMinAge gate (default 1h);
// scan-fallback tombstones (weaker, retained-identity-only provenance) keep the
// full minOrphanAge gate.

// TestClassifyTombstonesUsesShortGate: a ledger-matched tombstone older than
// tombstoneMinAge but far younger than minOrphanAge is classified.
func TestClassifyTombstonesUsesShortGate(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newTombstoneTXGTestDriver(client)
	mustCreateParentDataset(t, client)
	// 2h old: past the 1h tombstone gate, 22h short of the old 24h gate.
	createdAt := time.Now().Add(-2 * time.Hour).Unix()
	snapshot := seedTXGTombstone(t, d, client, "field-source", "snap-csi-deleted-1", createdAt, 777)
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, tombstoneLedgerEntry{
		Version:   tombstoneLedgerVersion,
		Snapshot:  snapshot.ID,
		Dataset:   snapshot.Dataset,
		CreatedAt: createdAt,
		CreateTXG: snapshot.CreateTXG,
		RenamedAt: time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano),
	}))
	parent, err := client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	ledger := tombstoneLedgerFromDataset(parent)

	report := &ReconcileReport{}
	d.classifyTombstones(time.Now(), []*truenas.Snapshot{snapshot}, ledger, time.Hour, report)
	require.Len(t, report.TombstoneSnapshots, 1,
		"a 2h-old ledger-proven tombstone must be classified under the 1h tombstone gate")

	// And the reap revalidation must accept the SAME gate the classifier used —
	// classifying at 1h but revalidating at 24h would re-create the stall.
	retire := &tombstoneRetirementBatch{}
	reaped, reason := d.reapTombstoneSnapshot(ctx, report.TombstoneSnapshots[0], time.Hour, retire)
	assert.True(t, reaped, reason)

	// Regression arithmetic from the incident: created 05:11, daily cron 04:20.
	// Under the old shared 24h gate the first pass finds age 23h09m => skipped.
	created := time.Date(2026, 8, 18, 5, 11, 15, 0, time.UTC)
	firstCron := time.Date(2026, 8, 19, 4, 20, 0, 0, time.UTC)
	_, _, oldEligible := reconcileAge(firstCron, created.Unix(), 24*time.Hour)
	assert.False(t, oldEligible, "incident precondition: the old gate skips the first daily pass")
	_, _, newEligible := reconcileAge(firstCron, created.Unix(), time.Hour)
	assert.True(t, newEligible, "the tombstone gate makes the same tombstone eligible on the first pass")
}

// TestScanFallbackTombstoneKeepsFullGate: without a ledger entry, provenance
// rests on retained identity alone, so the conservative minOrphanAge gate
// stays in force — a 2h-old scan-fallback tombstone is NOT nominated.
func TestScanFallbackTombstoneKeepsFullGate(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newTombstoneTXGTestDriver(client)
	mustCreateParentDataset(t, client)
	createdAt := time.Now().Add(-2 * time.Hour).Unix()
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/scan-source", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropDriverInstanceID, d.driverInstanceID()))
	snapshot, err := client.SnapshotCreate(ctx, source.Name, "snap-1", map[string]string{
		PropCSISnapshotName:  "snap-1",
		PropDriverInstanceID: d.driverInstanceID(),
	})
	require.NoError(t, err)
	tombstoneName := snapshotTombstoneName(source.Name, "snap-1", 1)
	require.NoError(t, client.SnapshotRename(ctx, snapshot.ID, tombstoneName))
	tombstone, err := client.SnapshotGet(ctx, source.Name+"@"+tombstoneName)
	require.NoError(t, err)
	tombstone.Properties["creation"] = map[string]interface{}{"parsed": float64(createdAt)}

	report := &ReconcileReport{}
	d.detectTombstonesByScanFallback(ctx, time.Now(), []*truenas.Snapshot{tombstone},
		map[string]tombstoneLedgerEntry{}, 24*time.Hour, report)
	assert.Empty(t, report.TombstoneSnapshots,
		"a young scan-fallback tombstone must stay behind the full minOrphanAge gate")
	assert.Empty(t, report.ManualRecoveryTombstones)
	require.Len(t, report.TombstonePending, 1,
		"a proven but age-gated scan-fallback tombstone must still be in the oldest-age set")
}

// TestReconcileTombstoneMinAgeResolution pins the resolver contract: unset
// defaults to 1h (pre-1.10.3 ConfigMaps carry no key), an explicit value is
// honored, the gate never exceeds minOrphanAge, and garbage is a config error.
func TestReconcileTombstoneMinAgeResolution(t *testing.T) {
	d := &Driver{config: &Config{}}
	gate, err := d.reconcileTombstoneMinAge(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, time.Hour, gate, "unset must default, not error: old ConfigMaps carry no key")

	d.config.Reconcile.TombstoneMinAge = "30m"
	gate, err = d.reconcileTombstoneMinAge(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 30*time.Minute, gate)

	d.config.Reconcile.TombstoneMinAge = "48h"
	gate, err = d.reconcileTombstoneMinAge(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 24*time.Hour, gate, "the tombstone gate is capped at minOrphanAge")

	d.config.Reconcile.TombstoneMinAge = "not-a-duration"
	_, err = d.reconcileTombstoneMinAge(24 * time.Hour)
	assert.Error(t, err, "a present-but-invalid value is a configuration failure")

	// Unset with a minOrphanAge SHORTER than the 1h default must not invert the
	// gates: the tombstone gate follows the stricter operator choice down.
	d.config.Reconcile.TombstoneMinAge = ""
	gate, err = d.reconcileTombstoneMinAge(30 * time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 30*time.Minute, gate)
}

// TestScanFallbackDoesNotRouteAgeGatedLedgerTombstoneToManualRecovery is
// BLOCKER 3: dedup seeded only from eligible TombstoneSnapshots, so an
// age-gated ledger-proven item in TombstonePending was treated as an
// unproven lookalike. Fails on current code.
func TestScanFallbackDoesNotRouteAgeGatedLedgerTombstoneToManualRecovery(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newTombstoneTXGTestDriver(client)
	mustCreateParentDataset(t, client)
	createdAt := time.Now().Add(-30 * time.Minute).Unix()
	snapshot := seedTXGTombstone(t, d, client, "age-gated-source", "snap-csi-deleted-7", createdAt, 42)
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, tombstoneLedgerEntry{
		Version:   tombstoneLedgerVersion,
		Snapshot:  snapshot.ID,
		Dataset:   snapshot.Dataset,
		CreatedAt: createdAt,
		CreateTXG: snapshot.CreateTXG,
		RenamedAt: time.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339Nano),
	}))
	parent, err := client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	ledger := tombstoneLedgerFromDataset(parent)

	report := &ReconcileReport{}
	d.classifyTombstones(time.Now(), []*truenas.Snapshot{snapshot}, ledger, time.Hour, report)
	assert.Empty(t, report.TombstoneSnapshots)
	require.Len(t, report.TombstonePending, 1)

	d.detectTombstonesByScanFallback(ctx, time.Now(), []*truenas.Snapshot{snapshot}, ledger, 24*time.Hour, report)
	assert.Empty(t, report.ManualRecoveryTombstones,
		"an age-gated ledger-proven tombstone must not be routed to manual recovery")
	require.Len(t, report.TombstonePending, 1)
	assert.Equal(t, snapshot.ID, report.TombstonePending[0].ID)
	assert.Empty(t, report.TombstoneSnapshots)
}

// TestDeleteVolumeTombstoneRefusalStatesTheBound pins the honest error text:
// the refusal must state the actual clearing bound instead of implying the
// tombstone clears within minutes.
func TestDeleteVolumeTombstoneRefusalStatesTheBound(t *testing.T) {
	message := foreignSnapshotRefusalMessage("vol", 1, 0, 1, time.Hour)
	assert.Contains(t, message, "older than reconcile.tombstoneMinAge (1h0m0s)")
	assert.Contains(t, message, "next scheduled")
	assert.NotContains(t, message, "clear automatically on a later reconcile pass",
		"the old wording read as 'minutes' during a 22h stall")
}
