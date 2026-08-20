package driver

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func resetLastReapMetrics() {
	tombstoneReapLastSuccessTimestamp.Reset()
	tombstoneReapLastReaped.Reset()
	tombstoneReapLastSkippedOnCap.Reset()
	tombstoneReapLastSkippedRefused.Reset()
}

func seedLedgerProvenTombstone(
	t *testing.T,
	d *Driver,
	client *truenas.MockClient,
	sourceID, tombstoneName string,
	createdAt time.Time,
) *truenas.Snapshot {
	t.Helper()
	ctx := context.Background()
	source := addReconcileDataset(client, sourceID, createdAt.Add(-24*time.Hour), true, testGiB)
	require.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropDriverInstanceID, d.driverInstanceID()))
	snapshot, err := client.SnapshotCreate(ctx, source.Name, tombstoneName, nil)
	require.NoError(t, err)
	snapshot.Properties["creation"] = map[string]interface{}{"parsed": float64(createdAt.Unix())}
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, tombstoneLedgerEntry{
		Version:   tombstoneLedgerVersion,
		Snapshot:  snapshot.ID,
		Dataset:   snapshot.Dataset,
		CreatedAt: createdAt.Unix(),
		CreateTXG: snapshot.CreateTXG,
		RenamedAt: createdAt.UTC().Format(time.RFC3339Nano),
	}))
	return snapshot
}

func boundReapRecord(d *Driver, rec tombstoneReapRecord) tombstoneReapRecord {
	rec.Version = tombstoneReapRecordVersion
	rec.DriverInstanceID = d.driverInstanceID()
	return rec
}

func readReapRecordFromChild(t *testing.T, d *Driver, client *truenas.MockClient) *tombstoneReapRecord {
	t.Helper()
	child, err := client.DatasetGet(context.Background(), d.bookkeepingDatasetName())
	require.NoError(t, err, "delete-capable pass must create .csi-bookkeeping and write the reap record")
	rec := parseTombstoneReapRecord(child, d.driverInstanceID())
	require.NotNil(t, rec, "reap record must be present and parseable on the bookkeeping child")
	return rec
}

// TestClassifyTombstonesOldestAgeIncludesAgeGated is the 2026-08-18/20 incident
// class: a ledger-proven tombstone still behind the age gate must be invisible
// to the eligible count AND visible to the oldest-age gauge. Computing oldest
// age only over TombstoneSnapshots would report 0 (or omit the stuck object)
// exactly as the count gauge did during the incident.
func TestClassifyTombstonesOldestAgeIncludesAgeGated(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newTombstoneTXGTestDriver(client)
	mustCreateParentDataset(t, client)
	createdAt := time.Now().Add(-30 * time.Minute).Unix()
	snapshot := seedTXGTombstone(t, d, client, "age-gated-source", "snap-csi-deleted-1", createdAt, 42)
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
	assert.Empty(t, report.TombstoneSnapshots,
		"a 30m-old ledger-proven tombstone must stay behind the 1h gate")
	require.Len(t, report.TombstonePending, 1,
		"the age-gated tombstone must still be in the pending (oldest-age) set")
	assert.Equal(t, snapshot.ID, report.TombstonePending[0].ID)
	assert.InDelta(t, (30 * time.Minute).Seconds(), report.tombstoneOldestAge().Seconds(), 5)
}

func TestSetOrphanReconcileMetricsPublishesOldestAgeZeroWhenNone(t *testing.T) {
	SetOrphanReconcileMetrics(ReconcileReport{})
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneOldestAgeSeconds),
		"no pending tombstones must publish 0, not 'age unknown'")
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneUnknownAge),
		"none-remain must also publish unknown-age 0")
}

func TestLastTombstoneReapMetricsAbsentWithoutRecord(t *testing.T) {
	resetLastReapMetrics()
	assert.False(t, gatherHasMetric(t, "scale_csi_tombstone_reap_last_success_timestamp_seconds"),
		"a fresh install with no record must export no last-reap timestamp series")
	assert.False(t, gatherHasMetric(t, "scale_csi_tombstone_reap_last_reaped"))
	assert.False(t, gatherHasMetric(t, "scale_csi_tombstone_reap_last_skipped_on_cap"))
	assert.False(t, gatherHasMetric(t, "scale_csi_tombstone_reap_last_skipped_refused"))
}

// TestReconcileOrphansAgeGatedTombstonePublishesOldestAge drives the full
// detection path: classify → SetOrphanReconcileMetrics. A fixture that set the
// gauge itself would pass on e4e9d07; this fails there because the age-gated
// continue skipped the object before any age was recorded.
func TestReconcileOrphansAgeGatedTombstonePublishesOldestAge(t *testing.T) {
	resetLastReapMetrics()
	createdAt := time.Now().Add(-45 * time.Minute)
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("age-gated-source", "csi.scale.io")}, nil)
	mustCreateParentDataset(t, client)
	seedLedgerProvenTombstone(t, d, client, "age-gated-source", "snap-csi-deleted-7", createdAt)

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, 0, report.TombstoneSnapshotCount,
		"age-gated tombstone must stay off the eligible count")
	require.Len(t, report.TombstonePending, 1)
	assert.InDelta(t, (45 * time.Minute).Seconds(), testutil.ToFloat64(tombstoneOldestAgeSeconds), 5)
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneSnapshots))
}

// TestDeleteCapablePassPersistsReapRecord is the ITEM 1/2/4 round-trip: a
// delete-capable pass that hits the cap must write a durable record the
// controller can export, including skippedOnCap. On e4e9d07 nothing is written
// and the last-reap series do not exist.
func TestDeleteCapablePassPersistsReapRecord(t *testing.T) {
	resetLastReapMetrics()
	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour)
	pvs := []runtime.Object{
		reconcilePV("reap-a", "csi.scale.io"),
		reconcilePV("reap-b", "csi.scale.io"),
		reconcilePV("reap-c", "csi.scale.io"),
	}
	d, client := newReconcileTestDriver(t, false, pvs, nil)
	client.NoDeferredSnapshotDestroy = true
	d.config.Reconcile.Delete.MaxPerRun = 1
	mustCreateParentDataset(t, client)
	seedLedgerProvenTombstone(t, d, client, "reap-a", "a-csi-deleted-1", old)
	seedLedgerProvenTombstone(t, d, client, "reap-b", "b-csi-deleted-1", old)
	seedLedgerProvenTombstone(t, d, client, "reap-c", "c-csi-deleted-1", old)

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, 1, len(report.DeletedTombstones), "cap 1 must reap exactly one tombstone")
	assert.Equal(t, 2, report.CapSkippedDeletes(), "the other two tombstones must skip on the cap")

	rec := readReapRecordFromChild(t, d, client)
	assert.Equal(t, tombstoneReapRecordVersion, rec.Version)
	assert.Equal(t, d.driverInstanceID(), rec.DriverInstanceID)
	assert.Greater(t, rec.CompletedAt, int64(0))
	assert.Equal(t, 1, rec.Reaped)
	assert.Equal(t, 2, rec.SkippedOnCap)
	assert.Equal(t, 2, rec.RemainingEligible)
	assert.Equal(t, 0, rec.SkippedRefused, "cap skips must not be counted as guard refusals")

	assert.Equal(t, float64(rec.CompletedAt), testutil.ToFloat64(tombstoneReapLastSuccessTimestamp.WithLabelValues()))
	assert.Equal(t, float64(1), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()))
	assert.Equal(t, float64(2), testutil.ToFloat64(tombstoneReapLastSkippedOnCap.WithLabelValues()))
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneReapLastSkippedRefused.WithLabelValues()))
	// Overlay: the record is newer than this pass's observation, so the
	// eligible backlog gauge must show the POST-reap remaining, not the
	// pre-reap 3 that detection classified.
	assert.Equal(t, float64(2), testutil.ToFloat64(tombstoneSnapshots))
}

func TestDeleteCapablePassRanAndDidNothing(t *testing.T) {
	resetLastReapMetrics()
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("live-volume", "csi.scale.io")}, nil)
	mustCreateParentDataset(t, client)
	addReconcileDataset(client, "live-volume", time.Now().Add(-48*time.Hour), true, 100)

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.DeletedTombstones)
	assert.Equal(t, 0, report.CapSkippedDeletes())

	rec := readReapRecordFromChild(t, d, client)
	assert.Equal(t, 0, rec.Reaped)
	assert.Equal(t, 0, rec.SkippedOnCap)
	assert.Greater(t, rec.CompletedAt, int64(0))
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()))
	assert.True(t, gatherHasMetric(t, "scale_csi_tombstone_reap_last_success_timestamp_seconds"),
		"ran-and-did-nothing must present a timestamp series, unlike a fresh install")
}

func TestReapRecordWriteFailureDoesNotFailPass(t *testing.T) {
	resetLastReapMetrics()
	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour)
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("fail-source", "csi.scale.io")}, nil)
	client.NoDeferredSnapshotDestroy = true
	mustCreateParentDataset(t, client)
	tombstone := seedLedgerProvenTombstone(t, d, client, "fail-source", "fail-csi-deleted-1", old)
	client.FailUserPropertyKeys = map[string]struct{}{PropTombstoneReapLast: {}}

	failuresBefore := testutil.ToFloat64(reconcileFailuresTotal.WithLabelValues("reap_record"))
	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err, "a bookkeeping write failure must not fail the reap")
	assert.Contains(t, report.DeletedTombstones, tombstone.ID)
	_, getErr := client.SnapshotGet(ctx, tombstone.ID)
	assert.True(t, truenas.IsNotFoundError(getErr), "the tombstone must still have been reaped")
	assert.Equal(t, failuresBefore+1, testutil.ToFloat64(reconcileFailuresTotal.WithLabelValues("reap_record")),
		"the write failure must increment reconcile_failures_total{phase=\"reap_record\"}")
	child, childErr := client.DatasetGet(ctx, d.bookkeepingDatasetName())
	if childErr == nil {
		assert.Nil(t, parseTombstoneReapRecord(child, d.driverInstanceID()),
			"a failed write must not store a durable reap record")
	}
	assert.False(t, gatherHasMetric(t, "scale_csi_tombstone_reap_last_success_timestamp_seconds"),
		"an un-persisted reap must not publish last-reap gauges as if a durable record existed")
	assert.False(t, gatherHasMetric(t, "scale_csi_tombstone_reap_last_reaped"))
}

func TestReapRecordWriteFailureKeepsDurableRecord(t *testing.T) {
	resetLastReapMetrics()
	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour)
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("fail-keep-source", "csi.scale.io")}, nil)
	client.NoDeferredSnapshotDestroy = true
	mustCreateParentDataset(t, client)
	prior := boundReapRecord(d, tombstoneReapRecord{
		CompletedAt:     old.Unix(),
		CompletedAtNano: old.UnixNano(),
		Reaped:          9,
	})
	require.NoError(t, d.writeTombstoneReapRecord(ctx, prior))
	tombstone := seedLedgerProvenTombstone(t, d, client, "fail-keep-source", "fail-keep-csi-deleted-1", old)
	client.FailUserPropertyKeys = map[string]struct{}{PropTombstoneReapLast: {}}

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Contains(t, report.DeletedTombstones, tombstone.ID)
	assert.Equal(t, float64(prior.CompletedAt), testutil.ToFloat64(tombstoneReapLastSuccessTimestamp.WithLabelValues()),
		"write failure must keep last-reap gauges on the durable record, not the un-persisted pass")
	assert.Equal(t, float64(9), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()))
	stored := readReapRecordFromChild(t, d, client)
	assert.Equal(t, 9, stored.Reaped, "the durable record must remain the prior one")
}

// TestControllerDetectionExportsDurableReapRecord is the ITEM 1 scrape path:
// a second driver (the long-lived controller) running Delete:false must export
// the last-reap gauges from the record the CronJob wrote, without itself
// deleting anything.
// sourcelessResourceQueryClient models TrueNAS 26.0 DatasetGetUserProperties:
// zfs.resource.query returns user_properties as a flat map with Source == "".
type sourcelessResourceQueryClient struct {
	*truenas.MockClient
}

func (c *sourcelessResourceQueryClient) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	ds, err := c.MockClient.DatasetGet(ctx, name)
	if err != nil {
		return nil, err
	}
	return resourceQueryProjection(ds), nil
}

// TestControllerExportsReapRecordThroughSourcelessResourceQuery is the
// production scrape path on TrueNAS 26.0: the poller/controller reads the
// CronJob's record through DatasetGetUserProperties (zfs.resource.query),
// which cannot populate per-property Source. Treating Source != "local" as
// absent makes every last-reap series vanish and ScaleCSITombstoneReapNeverRan
// fire permanently. This test must fail on that inspect.
func TestControllerExportsReapRecordThroughSourcelessResourceQuery(t *testing.T) {
	resetLastReapMetrics()
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("src", "csi.scale.io")}, nil)
	mustCreateParentDataset(t, client)
	now := time.Now().Add(-time.Hour)
	rec := boundReapRecord(d, tombstoneReapRecord{
		CompletedAt:       now.Unix(),
		CompletedAtNano:   now.UnixNano(),
		Reaped:            4,
		SkippedOnCap:      2,
		RemainingEligible: 2,
	})
	require.NoError(t, d.writeTombstoneReapRecord(ctx, rec))

	d.truenasClient = &sourcelessResourceQueryClient{MockClient: client}
	d.refreshTombstoneReapMetrics(ctx)

	require.True(t, gatherHasMetric(t, "scale_csi_tombstone_reap_last_success_timestamp_seconds"),
		"a sourceless resource-query read of a local record must export last-reap gauges")
	assert.Equal(t, float64(rec.CompletedAt), testutil.ToFloat64(tombstoneReapLastSuccessTimestamp.WithLabelValues()))
	assert.Equal(t, float64(4), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()))
	assert.Equal(t, float64(2), testutil.ToFloat64(tombstoneReapLastSkippedOnCap.WithLabelValues()))
}

func TestControllerDetectionExportsDurableReapRecord(t *testing.T) {
	resetLastReapMetrics()
	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour)
	pvs := []runtime.Object{reconcilePV("ctrl-source", "csi.scale.io")}
	cron, client := newReconcileTestDriver(t, false, pvs, nil)
	client.NoDeferredSnapshotDestroy = true
	mustCreateParentDataset(t, client)
	seedLedgerProvenTombstone(t, cron, client, "ctrl-source", "ctrl-csi-deleted-1", old)

	_, err := cron.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	written := readReapRecordFromChild(t, cron, client)
	require.Equal(t, 1, written.Reaped)

	resetLastReapMetrics()
	controller, _ := newReconcileTestDriver(t, false, pvs, nil)
	controller.truenasClient = client
	report, err := controller.ReconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.DeletedTombstones, "controller detection must not reap")
	assert.Equal(t, float64(written.CompletedAt), testutil.ToFloat64(tombstoneReapLastSuccessTimestamp.WithLabelValues()))
	assert.Equal(t, float64(1), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()))
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneReapLastSkippedOnCap.WithLabelValues()))
}

// TestReapRecordPollOverlaysStaleEligibleBacklog is ITEM 4: after detection
// published a stale eligible count, a newer durable record (as the CronJob
// would write) must refresh the controller's backlog gauge without waiting
// for the next detection interval.
func TestReapRecordPollOverlaysStaleEligibleBacklog(t *testing.T) {
	resetLastReapMetrics()
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("stale-source", "csi.scale.io")}, nil)
	mustCreateParentDataset(t, client)
	SetOrphanReconcileMetrics(ReconcileReport{TombstoneSnapshotCount: 383})
	assert.Equal(t, float64(383), testutil.ToFloat64(tombstoneSnapshots))
	d.noteOrphanMetricsObservation(time.Now().Add(-time.Hour))

	now := time.Now()
	rec := boundReapRecord(d, tombstoneReapRecord{
		CompletedAt:       now.Unix(),
		CompletedAtNano:   now.UnixNano(),
		Reaped:            500,
		SkippedOnCap:      0,
		RemainingEligible: 0,
	})
	require.NoError(t, d.writeTombstoneReapRecord(context.Background(), rec))
	d.refreshTombstoneReapMetrics(context.Background())

	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneSnapshots),
		"a newer reap record must overlay the stale eligible count")
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneOldestAgeSeconds),
		"draining the pending set must publish oldest-age 0, not age-unknown")
	assert.Equal(t, float64(500), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()))
}

func TestTombstoneReapRecordRoundTripJSON(t *testing.T) {
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)
	completed := time.Now().Add(-time.Hour)
	want := boundReapRecord(d, tombstoneReapRecord{
		CompletedAt:       completed.Unix(),
		CompletedAtNano:   completed.UnixNano(),
		Reaped:            7,
		SkippedOnCap:      3,
		SkippedRefused:    1,
		RemainingEligible: 3,
		RemainingBytes:    4096,
		OldestCreatedAt:   completed.Add(-time.Hour).Unix(),
	})
	require.NoError(t, d.writeTombstoneReapRecord(context.Background(), want))
	got, err := d.readTombstoneReapRecord(context.Background())
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want, *got)

	child, err := client.DatasetGet(context.Background(), d.bookkeepingDatasetName())
	require.NoError(t, err)
	raw := child.UserProperties[PropTombstoneReapLast].Value
	var decoded tombstoneReapRecord
	require.NoError(t, json.Unmarshal([]byte(raw), &decoded))
	assert.Equal(t, want, decoded)
}

func TestSetReconcileDeleteEnabled(t *testing.T) {
	SetReconcileDeleteEnabled(false)
	assert.Equal(t, float64(0), testutil.ToFloat64(reconcileDeleteEnabled))
	SetReconcileDeleteEnabled(true)
	assert.Equal(t, float64(1), testutil.ToFloat64(reconcileDeleteEnabled))
}

func TestReapRecordPollOverlaysStaleEligibleBacklogLegacySeconds(t *testing.T) {
	resetLastReapMetrics()
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("stale-source", "csi.scale.io")}, nil)
	mustCreateParentDataset(t, client)
	SetOrphanReconcileMetrics(ReconcileReport{TombstoneSnapshotCount: 383})
	d.noteOrphanMetricsObservation(time.Now().Add(-time.Hour))

	rec := boundReapRecord(d, tombstoneReapRecord{
		CompletedAt:       time.Now().Unix(),
		Reaped:            500,
		RemainingEligible: 0,
	})
	require.NoError(t, d.writeTombstoneReapRecord(context.Background(), rec))
	d.refreshTombstoneReapMetrics(context.Background())
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneSnapshots),
		"a legacy seconds-only record newer than the observation must overlay")
}

func TestTombstoneReapRecordValidate(t *testing.T) {
	now := time.Now()
	valid := tombstoneReapRecord{
		Version:           tombstoneReapRecordVersion,
		DriverInstanceID:  "csi.scale.io@pool/parent",
		CompletedAt:       now.Unix(),
		CompletedAtNano:   now.UnixNano(),
		Reaped:            1,
		SkippedOnCap:      0,
		SkippedRefused:    0,
		RemainingEligible: 0,
		RemainingBytes:    0,
		OldestCreatedAt:   now.Add(-time.Hour).Unix(),
	}
	require.NoError(t, valid.validate(now))
	v1 := valid
	v1.Version = tombstoneReapRecordVersionV1
	v1.DriverInstanceID = ""
	require.NoError(t, v1.validate(now), "v1 records remain schema-valid until rewritten as v2")

	for _, tc := range []struct {
		name string
		mut  func(*tombstoneReapRecord)
	}{
		{name: "v2 missing instance id", mut: func(r *tombstoneReapRecord) { r.DriverInstanceID = "" }},
		{name: "unsupported version", mut: func(r *tombstoneReapRecord) { r.Version = 99 }},
		{name: "negative reaped", mut: func(r *tombstoneReapRecord) { r.Reaped = -1 }},
		{name: "negative skipped on cap", mut: func(r *tombstoneReapRecord) { r.SkippedOnCap = -1 }},
		{name: "negative skipped refused", mut: func(r *tombstoneReapRecord) { r.SkippedRefused = -1 }},
		{name: "negative remaining eligible", mut: func(r *tombstoneReapRecord) { r.RemainingEligible = -1 }},
		{name: "negative remaining bytes", mut: func(r *tombstoneReapRecord) { r.RemainingBytes = -1 }},
		{name: "nano inconsistent with seconds", mut: func(r *tombstoneReapRecord) { r.CompletedAtNano = (r.CompletedAt + 5) * int64(time.Second) }},
		{name: "future completed_at", mut: func(r *tombstoneReapRecord) {
			r.CompletedAt = now.Add(time.Hour).Unix()
			r.CompletedAtNano = 0
			r.OldestCreatedAt = 0
		}},
		{name: "future oldest_created_at", mut: func(r *tombstoneReapRecord) { r.OldestCreatedAt = now.Add(time.Hour).Unix() }},
		{name: "oldest after completed", mut: func(r *tombstoneReapRecord) { r.OldestCreatedAt = r.CompletedAt + 10 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := valid
			tc.mut(&rec)
			assert.Error(t, rec.validate(now))
		})
	}
}

func TestInvalidChildReapRecordDoesNotFallBackToParent(t *testing.T) {
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)
	require.NoError(t, d.ensureBookkeepingDataset(context.Background()))

	valid := boundReapRecord(d, tombstoneReapRecord{
		CompletedAt: time.Now().Add(-time.Hour).Unix(),
		Reaped:      3,
	})
	encoded, err := json.Marshal(valid)
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), d.parentDatasetName(), PropTombstoneReapLast, string(encoded)))
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), d.bookkeepingDatasetName(), PropTombstoneReapLast, `{"v":1,"completed_at":1,"reaped":-9}`))

	parent, err := client.DatasetGet(context.Background(), d.parentDatasetName())
	require.NoError(t, err)
	child, err := client.DatasetGet(context.Background(), d.bookkeepingDatasetName())
	require.NoError(t, err)
	assert.Nil(t, tombstoneReapRecordFromDatasets(parent, child, d.driverInstanceID()),
		"an invalid child record must not fall back to a valid parent")

	got, err := d.readTombstoneReapRecord(context.Background())
	require.NoError(t, err)
	assert.Nil(t, got, "read must not fall back after an invalid child")
}

func TestReapRecordFromDatasetsSelectsNewestValid(t *testing.T) {
	older := time.Now().Add(-2 * time.Hour)
	newer := time.Now().Add(-time.Minute)
	instanceID := "csi.scale.io@pool/parent"
	parentDS := &truenas.Dataset{Name: "pool/parent", UserProperties: map[string]truenas.UserProperty{
		PropTombstoneReapLast: {Value: mustEncodeReapRecord(t, tombstoneReapRecord{
			Version:          tombstoneReapRecordVersion,
			DriverInstanceID: instanceID,
			CompletedAt:      newer.Unix(),
			CompletedAtNano:  newer.UnixNano(),
			Reaped:           9,
		}), Source: "local"},
	}}
	childDS := &truenas.Dataset{Name: "pool/parent/.csi-bookkeeping", UserProperties: map[string]truenas.UserProperty{
		PropTombstoneReapLast: {Value: mustEncodeReapRecord(t, tombstoneReapRecord{
			Version:          tombstoneReapRecordVersion,
			DriverInstanceID: instanceID,
			CompletedAt:      older.Unix(),
			CompletedAtNano:  older.UnixNano(),
			Reaped:           1,
		}), Source: "local"},
	}}
	got := tombstoneReapRecordFromDatasets(parentDS, childDS, instanceID)
	require.NotNil(t, got)
	assert.Equal(t, 9, got.Reaped, "the newer valid record must win even when it lives on the parent")
}

func TestTombstoneReapRecordPollStartStopLifecycle(t *testing.T) {
	resetLastReapMetrics()
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("poll-source", "csi.scale.io")}, nil)
	mustCreateParentDataset(t, client)
	d.config.Reconcile.Delete.Enabled = true
	d.config.Reconcile.Delete.ReapRecordPollInterval = "15ms"

	now := time.Now()
	require.NoError(t, d.writeTombstoneReapRecord(context.Background(), boundReapRecord(d, tombstoneReapRecord{
		CompletedAt:     now.Unix(),
		CompletedAtNano: now.UnixNano(),
		Reaped:          4,
	})))

	d.startTombstoneReapRecordPoll()
	d.startTombstoneReapRecordPoll() // repeated start must not leak a goroutine
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()) == 4
	}, 2*time.Second, 10*time.Millisecond, "the poller must publish the durable record")
	d.stopTombstoneReapRecordPoll()
	d.stopTombstoneReapRecordPoll() // repeated stop must not deadlock
}

func TestTombstoneReapRecordPollStopBeforeStartIsTerminal(t *testing.T) {
	d, _ := newReconcileTestDriver(t, false, nil, nil)
	d.config.Reconcile.Delete.Enabled = true
	d.config.Reconcile.Delete.ReapRecordPollInterval = "10ms"
	d.stopTombstoneReapRecordPoll()
	d.startTombstoneReapRecordPoll()
	time.Sleep(50 * time.Millisecond)
	assert.Nil(t, d.reapRecordCancel, "stop is terminal; a later start must not launch the poller")
	d.stopTombstoneReapRecordPoll()
}

func TestInspectTombstoneReapRecordSource(t *testing.T) {
	now := time.Now().Add(-time.Hour)
	instanceID := "csi.scale.io@pool/parent"
	valid := mustEncodeReapRecord(t, tombstoneReapRecord{
		Version:          tombstoneReapRecordVersion,
		DriverInstanceID: instanceID,
		CompletedAt:      now.Unix(),
		Reaped:           3,
	})
	v1 := mustEncodeReapRecord(t, tombstoneReapRecord{
		Version:     tombstoneReapRecordVersionV1,
		CompletedAt: now.Unix(),
		Reaped:      3,
	})
	for _, tc := range []struct {
		name   string
		source string
		value  string
		want   reapRecordStatus
	}{
		{name: "local v2", source: "local", value: valid, want: reapRecordOK},
		{name: "empty resource-query v2", source: "", value: valid, want: reapRecordOK},
		{name: "clone origin", source: "tank/src@snap", value: valid, want: reapRecordAbsent},
		{name: "inherited", source: "inherited", value: valid, want: reapRecordAbsent},
		{name: "sourceless v1 needs source", source: "", value: v1, want: reapRecordLegacy},
		{name: "local v1", source: "local", value: v1, want: reapRecordOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := &truenas.Dataset{Name: "pool/parent/.csi-bookkeeping", UserProperties: map[string]truenas.UserProperty{
				PropTombstoneReapLast: {Value: tc.value, Source: tc.source},
			}}
			_, status := inspectTombstoneReapRecord(ds, instanceID)
			assert.Equal(t, tc.want, status)
		})
	}
}

func TestReadTombstoneReapRecordSelectsNewerParent(t *testing.T) {
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)
	require.NoError(t, d.ensureBookkeepingDataset(context.Background()))

	older := time.Now().Add(-2 * time.Hour)
	newer := time.Now().Add(-time.Minute)
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), d.bookkeepingDatasetName(), PropTombstoneReapLast, mustEncodeReapRecord(t, boundReapRecord(d, tombstoneReapRecord{
		CompletedAt:     older.Unix(),
		CompletedAtNano: older.UnixNano(),
		Reaped:          1,
	}))))
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), d.parentDatasetName(), PropTombstoneReapLast, mustEncodeReapRecord(t, boundReapRecord(d, tombstoneReapRecord{
		CompletedAt:     newer.Unix(),
		CompletedAtNano: newer.UnixNano(),
		Reaped:          9,
	}))))

	got, err := d.readTombstoneReapRecord(context.Background())
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 9, got.Reaped, "polling must consult the parent; a newer parent must beat an older valid child")
}

func TestReconcileLoadsNewerChildWhenBookkeepingRelocationOff(t *testing.T) {
	resetLastReapMetrics()
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("live-volume", "csi.scale.io")}, nil)
	mustCreateParentDataset(t, client)
	addReconcileDataset(client, "live-volume", time.Now().Add(-48*time.Hour), true, 100)
	require.False(t, d.bookkeepingEnabled(), "precondition: relocation off so readBookkeepingState does not fetch the child")
	require.NoError(t, d.ensureBookkeepingDataset(context.Background()))

	older := time.Now().Add(-2 * time.Hour)
	newer := time.Now().Add(-time.Minute)
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), d.parentDatasetName(), PropTombstoneReapLast, mustEncodeReapRecord(t, boundReapRecord(d, tombstoneReapRecord{
		CompletedAt:     older.Unix(),
		CompletedAtNano: older.UnixNano(),
		Reaped:          1,
	}))))
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), d.bookkeepingDatasetName(), PropTombstoneReapLast, mustEncodeReapRecord(t, boundReapRecord(d, tombstoneReapRecord{
		CompletedAt:     newer.Unix(),
		CompletedAtNano: newer.UnixNano(),
		Reaped:          7,
	}))))

	_, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{Delete: false, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, float64(7), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()),
		"a stale parent must not suppress a newer child when bookkeeping relocation is off")
}

func TestPublishLastTombstoneReapRecordMonotonicUnderRace(t *testing.T) {
	resetLastReapMetrics()
	d, _ := newReconcileTestDriver(t, false, nil, nil)
	older := time.Now().Add(-time.Hour)
	newer := time.Now()
	newerRec := &tombstoneReapRecord{
		Version:           tombstoneReapRecordVersion,
		DriverInstanceID:  d.driverInstanceID(),
		CompletedAt:       newer.Unix(),
		CompletedAtNano:   newer.UnixNano(),
		Reaped:            11,
		RemainingEligible: 0,
	}
	olderRec := &tombstoneReapRecord{
		Version:           tombstoneReapRecordVersion,
		DriverInstanceID:  d.driverInstanceID(),
		CompletedAt:       older.Unix(),
		CompletedAtNano:   older.UnixNano(),
		Reaped:            1,
		RemainingEligible: 383,
	}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			d.publishLastTombstoneReapRecord(olderRec, true)
		}()
		go func() {
			defer wg.Done()
			d.publishLastTombstoneReapRecord(newerRec, true)
		}()
	}
	wg.Wait()
	assert.Equal(t, float64(11), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()),
		"an older publisher must not overwrite newer last-reap gauges")
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneSnapshots),
		"the newer remaining-eligible overlay must stick")
}

func TestTombstoneReapRecordPollVsReconcileInterleaving(t *testing.T) {
	resetLastReapMetrics()
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("interleave-source", "csi.scale.io")}, nil)
	mustCreateParentDataset(t, client)
	d.config.Reconcile.Delete.Enabled = true
	d.config.Reconcile.Delete.ReapRecordPollInterval = "10ms"

	older := time.Now().Add(-time.Hour)
	newer := time.Now()
	d.publishLastTombstoneReapRecord(&tombstoneReapRecord{
		Version:           tombstoneReapRecordVersion,
		DriverInstanceID:  d.driverInstanceID(),
		CompletedAt:       newer.Unix(),
		CompletedAtNano:   newer.UnixNano(),
		Reaped:            7,
		RemainingEligible: 0,
	}, true)
	require.Equal(t, float64(7), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		d.startTombstoneReapRecordPoll()
	}()
	go func() {
		defer wg.Done()
		d.publishLastTombstoneReapRecord(&tombstoneReapRecord{
			Version:           tombstoneReapRecordVersion,
			DriverInstanceID:  d.driverInstanceID(),
			CompletedAt:       older.Unix(),
			CompletedAtNano:   older.UnixNano(),
			Reaped:            1,
			RemainingEligible: 383,
		}, true)
	}()
	wg.Wait()
	d.stopTombstoneReapRecordPoll()
	assert.Equal(t, float64(7), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()),
		"an older reconcile record must not overwrite a newer published poll result")
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneSnapshots),
		"the newer remaining-eligible overlay must stick")
}

func TestClassifyTombstonesUnknownAgeIsDistinctFromNoneRemain(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newTombstoneTXGTestDriver(client)
	mustCreateParentDataset(t, client)
	createdAt := time.Now().Add(-48 * time.Hour).Unix()
	snapshot := seedTXGTombstone(t, d, client, "unknown-age-source", "snap-csi-deleted-2", createdAt, 42)
	require.NoError(t, d.writeTombstoneLedgerEntry(ctx, tombstoneLedgerEntry{
		Version:   tombstoneLedgerVersion,
		Snapshot:  snapshot.ID,
		Dataset:   snapshot.Dataset,
		CreatedAt: createdAt,
		CreateTXG: snapshot.CreateTXG,
		RenamedAt: time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano),
	}))
	parent, err := client.DatasetGet(ctx, d.parentDatasetName())
	require.NoError(t, err)
	ledger := tombstoneLedgerFromDataset(parent)
	snapshot.Properties["creation"] = map[string]interface{}{"parsed": float64(0)}

	report := &ReconcileReport{}
	d.classifyTombstones(time.Now(), []*truenas.Snapshot{snapshot}, ledger, time.Hour, report)
	assert.Empty(t, report.TombstoneSnapshots)
	assert.Equal(t, 1, report.TombstoneUnknownAgeCount)
	require.Len(t, report.TombstonePending, 1)
	SetOrphanReconcileMetrics(*report)
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneOldestAgeSeconds),
		"unknown-age must not be published as a 1970 age")
	assert.Equal(t, float64(1), testutil.ToFloat64(tombstoneUnknownAge),
		"unknown-age must be distinct from none-remain")
}

func TestReconcileRepairCapIndependentOfDeleteMaxPerRun(t *testing.T) {
	ctx := context.Background()
	objects := make([]runtime.Object, 0, 6)
	for i := 0; i < 6; i++ {
		objects = append(objects, boundReconcilePV("cap-"+string(rune('0'+i)), "csi.scale.io"))
	}
	d, client := newReconcileTestDriver(t, false, objects, nil)
	d.config.Reconcile.Delete.MaxPerRun = 1000
	d.config.Reconcile.Repair.MaxPerRun = 5
	mustCreateParentDataset(t, client)
	for i := 0; i < 6; i++ {
		addReconcileDataset(client, "cap-"+string(rune('0'+i)), time.Now().Add(-72*time.Hour), true, testGiB)
	}

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Len(t, report.AdoptedStamps, 5,
		"a large delete.maxPerRun must not raise the always-on adoption cap")
}

// TestSourcelessReapRecordRejectsForeignInstanceID is BLOCKER 1: a record
// inherited onto both the configured parent and .csi-bookkeeping from a
// different instance appears sourceless and identical in both, so dual-read
// cannot tell it from a local record. v2 driver_instance_id must reject it
// on the sourceless path and accept a matching one. Fails on current code
// because inspect accepts sourceless records with no instance binding.
func TestSourcelessReapRecordRejectsForeignInstanceID(t *testing.T) {
	resetLastReapMetrics()
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)
	require.NoError(t, d.ensureBookkeepingDataset(ctx))

	now := time.Now().Add(-time.Hour)
	foreign := tombstoneReapRecord{
		Version:           tombstoneReapRecordVersion,
		DriverInstanceID:  "other.csi@tank/other",
		CompletedAt:       now.Unix(),
		CompletedAtNano:   now.UnixNano(),
		Reaped:            99,
		RemainingEligible: 7,
	}
	encoded := mustEncodeReapRecord(t, foreign)
	require.NoError(t, client.DatasetSetUserProperty(ctx, d.parentDatasetName(), PropTombstoneReapLast, encoded))
	require.NoError(t, client.DatasetSetUserProperty(ctx, d.bookkeepingDatasetName(), PropTombstoneReapLast, encoded))

	got, err := d.readTombstoneReapRecord(ctx)
	require.NoError(t, err)
	assert.Nil(t, got, "an ancestor-inherited record from a different instance must be rejected on the sourceless path")

	matching := foreign
	matching.DriverInstanceID = d.driverInstanceID()
	encoded = mustEncodeReapRecord(t, matching)
	require.NoError(t, client.DatasetSetUserProperty(ctx, d.parentDatasetName(), PropTombstoneReapLast, encoded))
	require.NoError(t, client.DatasetSetUserProperty(ctx, d.bookkeepingDatasetName(), PropTombstoneReapLast, encoded))

	got, err = d.readTombstoneReapRecord(ctx)
	require.NoError(t, err)
	require.NotNil(t, got, "a sourceless record bound to this instance must be accepted")
	assert.Equal(t, matching.Reaped, got.Reaped)
	assert.Equal(t, d.driverInstanceID(), got.DriverInstanceID)
}

func TestSourcelessReapRecordNeverAcceptsMissingInstanceID(t *testing.T) {
	now := time.Now().Add(-time.Hour)
	payload, err := json.Marshal(map[string]interface{}{
		"v":            tombstoneReapRecordVersion,
		"completed_at": now.Unix(),
		"reaped":       1,
	})
	require.NoError(t, err)
	ds := &truenas.Dataset{Name: "pool/parent/.csi-bookkeeping", UserProperties: map[string]truenas.UserProperty{
		PropTombstoneReapLast: {Value: string(payload), Source: ""},
	}}
	_, status := inspectTombstoneReapRecord(ds, "csi.scale.io@pool/parent")
	assert.Equal(t, reapRecordInvalid, status, "v2 without driver_instance_id must not be accepted from the sourceless path")
}

func TestLegacyV1ReapRecordAcceptedOnlyWhenLocal(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	mustCreateParentDataset(t, client)
	require.NoError(t, d.ensureBookkeepingDataset(ctx))

	now := time.Now().Add(-time.Hour)
	v1 := tombstoneReapRecord{
		Version:     tombstoneReapRecordVersionV1,
		CompletedAt: now.Unix(),
		Reaped:      5,
	}
	require.NoError(t, client.DatasetSetUserProperty(ctx, d.bookkeepingDatasetName(), PropTombstoneReapLast, mustEncodeReapRecord(t, v1)))

	got, err := d.readTombstoneReapRecord(ctx)
	require.NoError(t, err)
	require.NotNil(t, got, "a source-bearing local v1 record must be accepted until the next delete pass rewrites v2")
	assert.Equal(t, tombstoneReapRecordVersionV1, got.Version)
	assert.Equal(t, 5, got.Reaped)
}

// TestDetectionDoesNotRestoreStaleBacklogOverNewerReapRecord is BLOCKER 2:
// FIX B serialized record publishers, but SetOrphanReconcileMetrics wrote the
// same backlog gauges outside that mutex. Sequence: poll overlays remaining=0;
// finishing detection restores 383; detection's older record is then rejected
// by the monotonic guard, so the stale backlog sticks until the next poll.
// Fails on current code for that exact interleaving.
func TestDetectionDoesNotRestoreStaleBacklogOverNewerReapRecord(t *testing.T) {
	resetLastReapMetrics()
	d, _ := newReconcileTestDriver(t, false, nil, nil)
	older := time.Now().Add(-time.Hour)
	newer := time.Now()
	newerRec := &tombstoneReapRecord{
		Version:           tombstoneReapRecordVersion,
		DriverInstanceID:  d.driverInstanceID(),
		CompletedAt:       newer.Unix(),
		CompletedAtNano:   newer.UnixNano(),
		Reaped:            11,
		RemainingEligible: 0,
	}
	olderRec := &tombstoneReapRecord{
		Version:           tombstoneReapRecordVersion,
		DriverInstanceID:  d.driverInstanceID(),
		CompletedAt:       older.Unix(),
		CompletedAtNano:   older.UnixNano(),
		Reaped:            1,
		RemainingEligible: 383,
	}
	olderReport := ReconcileReport{TombstoneSnapshotCount: 383, TombstoneSnapshotBytes: 999}

	d.noteOrphanMetricsObservation(older)
	d.publishLastTombstoneReapRecord(newerRec, true)
	require.Equal(t, float64(0), testutil.ToFloat64(tombstoneSnapshots))
	require.Equal(t, float64(11), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()))

	d.publishOrphanAndReapMetrics(olderReport, olderRec, true)
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneSnapshots),
		"a finishing detection pass must not restore an older backlog over a newer poll overlay")
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneSnapshotsBytes))
	assert.Equal(t, float64(11), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()),
		"last-reap gauges must stay on the newer record")

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			d.publishLastTombstoneReapRecord(newerRec, true)
		}()
		go func() {
			defer wg.Done()
			d.publishOrphanAndReapMetrics(olderReport, olderRec, true)
		}()
	}
	wg.Wait()
	assert.Equal(t, float64(0), testutil.ToFloat64(tombstoneSnapshots),
		"detection/poller interleaving under -race must keep the newer backlog")
	assert.Equal(t, float64(11), testutil.ToFloat64(tombstoneReapLastReaped.WithLabelValues()))
}

func mustEncodeReapRecord(t *testing.T, rec tombstoneReapRecord) string {
	t.Helper()
	encoded, err := json.Marshal(rec)
	require.NoError(t, err)
	return string(encoded)
}
