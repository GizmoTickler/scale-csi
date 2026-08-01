package driver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func newScheduleTestDriver(client truenas.ClientInterface) *Driver {
	return &Driver{
		name: "csi.scale.io",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
}

func scheduleVolumeRequest(name string, params map[string]string) *csi.CreateVolumeRequest {
	if params == nil {
		params = map[string]string{}
	}
	params["protocol"] = "nfs"
	return &csi.CreateVolumeRequest{
		Name:               name,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         params,
	}
}

func TestParseSnapshotSchedule(t *testing.T) {
	got, err := parseSnapshotSchedule("0 0 * * *")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"minute": "0", "hour": "0", "dom": "*", "month": "*", "dow": "*"}, got)

	_, err = parseSnapshotSchedule("0 0 *")
	assert.Error(t, err, "fewer than five fields is rejected")
}

func TestParseSnapshotRetention(t *testing.T) {
	cases := []struct {
		in       string
		wantVal  int
		wantUnit string
		wantErr  bool
	}{
		{"", 30, "DAY", false}, // empty resolves to the 30d safety bound
		{"24h", 24, "HOUR", false},
		{"30d", 30, "DAY", false},
		{"2w", 2, "WEEK", false},
		{"6M", 6, "MONTH", false},
		{"6mo", 6, "MONTH", false},
		{"1y", 1, "YEAR", false},
		{"0d", 0, "", true}, // quantity must be positive
		{"30", 0, "", true}, // missing unit suffix
		{"xd", 0, "", true}, // non-numeric quantity
	}
	for _, tc := range cases {
		val, unit, err := parseSnapshotRetention(tc.in)
		if tc.wantErr {
			assert.Error(t, err, "input %q", tc.in)
			continue
		}
		require.NoError(t, err, "input %q", tc.in)
		assert.Equal(t, tc.wantVal, val, "input %q value", tc.in)
		assert.Equal(t, tc.wantUnit, unit, "input %q unit", tc.in)
	}
}

func TestResolveSnapshotTaskSpecPerStorageClassOverride(t *testing.T) {
	d := newScheduleTestDriver(truenas.NewMockClient())
	d.config.ZFS.SnapshotSchedule = "0 0 * * *" // global default

	spec, err := d.resolveSnapshotTaskSpec(map[string]string{"snapshotSchedule": "30 2 * * 1", "snapshotRetention": "2w"}, "vol1")
	require.NoError(t, err)
	require.NotNil(t, spec)
	assert.Equal(t, "30", spec.schedule["minute"])
	assert.Equal(t, 2, spec.lifetimeValue)
	assert.Equal(t, "WEEK", spec.lifetimeUnit)
	// The schema is DRIVER-MINTED with an unguessable per-volume nonce and it
	// must prove out against this volume's id.
	assert.True(t, schemaProvesVolumeOwnership(spec.namingSchema, "vol1"), "schema %q must re-derive for vol1", spec.namingSchema)
	assert.False(t, schemaProvesVolumeOwnership(spec.namingSchema, "vol2"), "another volume's id must not prove out")

	other, err := d.resolveSnapshotTaskSpec(map[string]string{"snapshotSchedule": "30 2 * * 1"}, "vol1")
	require.NoError(t, err)
	assert.NotEqual(t, spec.namingSchema, other.namingSchema, "each mint draws a fresh nonce")

	// An explicit empty parameter opts out even with a global default set.
	spec, err = d.resolveSnapshotTaskSpec(map[string]string{"snapshotSchedule": ""}, "vol1")
	require.NoError(t, err)
	assert.Nil(t, spec, "empty per-SC schedule opts out")

	// No parameter and no global default => not scheduled.
	d.config.ZFS.SnapshotSchedule = ""
	spec, err = d.resolveSnapshotTaskSpec(map[string]string{}, "vol1")
	require.NoError(t, err)
	assert.Nil(t, spec)

	// A malformed schedule is a validation error.
	_, err = d.resolveSnapshotTaskSpec(map[string]string{"snapshotSchedule": "bad"}, "vol1")
	assert.Error(t, err)
}

func TestCreateVolumeCreatesScopedSnapshotTask(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)

	_, err := d.CreateVolume(context.Background(), scheduleVolumeRequest("sched-vol", map[string]string{
		"snapshotSchedule":  "0 0 * * *",
		"snapshotRetention": "30d",
	}))
	require.NoError(t, err)

	require.Len(t, client.SnapshotTasks, 1, "exactly one driver-owned task is created")
	for _, task := range client.SnapshotTasks {
		assert.Equal(t, "pool/parent/sched-vol", task.Dataset, "the task is scoped to THIS volume's dataset")
		assert.False(t, task.Recursive, "the task must be non-recursive (P2)")
		assert.True(t, task.Enabled)
		assert.True(t, task.AllowEmpty)
		assert.Equal(t, 30, task.LifetimeValue)
		assert.Equal(t, "DAY", task.LifetimeUnit)
		assert.True(t, schemaProvesVolumeOwnership(task.NamingSchema, "sched-vol"))
	}

	ds, err := client.DatasetGet(context.Background(), "pool/parent/sched-vol")
	require.NoError(t, err)
	assert.NotEmpty(t, ds.UserProperties[PropSnapshotTaskID].Value, "the task id is bound to the dataset")
	assert.True(t, schemaProvesVolumeOwnership(ds.UserProperties[PropSnapshotNamingSchema].Value, "sched-vol"))
}

func TestCreateVolumeIdempotentDoesNotDuplicateTask(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	req := scheduleVolumeRequest("sched-idem", map[string]string{"snapshotSchedule": "0 0 * * *"})

	_, err := d.CreateVolume(context.Background(), req)
	require.NoError(t, err)
	ds, err := client.DatasetGet(context.Background(), "pool/parent/sched-idem")
	require.NoError(t, err)
	firstSchema := ds.UserProperties[PropSnapshotNamingSchema].Value

	_, err = d.CreateVolume(context.Background(), req)
	require.NoError(t, err)

	assert.Len(t, client.SnapshotTasks, 1, "the retry adopts the existing task rather than duplicating it")
	ds, err = client.DatasetGet(context.Background(), "pool/parent/sched-idem")
	require.NoError(t, err)
	assert.Equal(t, firstSchema, ds.UserProperties[PropSnapshotNamingSchema].Value,
		"the retry must NOT re-mint the nonce — that would orphan the ownership proof of existing snapshots")
}

// H2 — stamp-before-create: a binding-write failure must leave NO task behind.
// Before the fix the task was created first and the stamp was best-effort, so a
// stamp failure stranded a live task forever behind an unbindable volume.
func TestCreateVolumeBindingFailureCreatesNoStrandedTask(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	client.FailUserPropertyKeys = map[string]struct{}{PropSnapshotNamingSchema: {}}

	_, err := d.CreateVolume(context.Background(), scheduleVolumeRequest("sched-nostamp", map[string]string{
		"snapshotSchedule": "0 0 * * *",
	}))
	require.NoError(t, err, "a task failure never blocks provisioning")
	assert.Empty(t, client.SnapshotTasks, "no task may exist without its durable binding")
}

// H2 — a stranded task (its volume dataset destroyed) must be reclaimed by the
// reconcile sweep the old code's comment falsely claimed already existed.
func TestReconcileSweepsStrandedSnapshotTask(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	d.config.ZFS.SnapshotSchedule = "0 0 * * *"

	schema, err := newDriverScheduledNamingSchema("gone-vol")
	require.NoError(t, err)
	_, err = client.SnapshotTaskCreate(ctx, &truenas.SnapshotTaskCreateParams{
		Dataset:      "pool/parent/gone-vol",
		NamingSchema: schema,
	})
	require.NoError(t, err)
	// A FOREIGN task under the same parent must survive the sweep untouched.
	foreign, err := client.SnapshotTaskCreate(ctx, &truenas.SnapshotTaskCreateParams{
		Dataset:      "pool/parent/other-vol",
		NamingSchema: "auto-%Y-%m-%d_%H-%M",
	})
	require.NoError(t, err)

	report := &ReconcileReport{}
	d.sweepStrandedSnapshotTasks(ctx, nil, report, true)

	assert.Equal(t, []string{"pool/parent/gone-vol"}, report.DeletedSnapshotTasks, "the stranded driver task is reclaimed")
	assert.Len(t, client.SnapshotTasks, 1)
	assert.Contains(t, client.SnapshotTasks, foreign.ID, "a foreign task is never touched")
}

func TestSnapshotTaskSweepMakesNoCallsWhenSchedulingUnused(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	// No global schedule and no dataset carrying a schema binding.
	_, err := client.SnapshotTaskCreate(ctx, &truenas.SnapshotTaskCreateParams{Dataset: "pool/parent/x"})
	require.NoError(t, err)

	report := &ReconcileReport{}
	d.sweepStrandedSnapshotTasks(ctx, nil, report, true)
	assert.Empty(t, report.StrandedSnapshotTasks, "the sweep must not run in a deployment that never scheduled anything")
	assert.Len(t, client.SnapshotTasks, 1)
}

// seedScheduledVolume creates a scheduled volume and returns its minted schema.
func seedScheduledVolume(t *testing.T, client *truenas.MockClient, d *Driver, volumeID string) string {
	t.Helper()
	_, err := d.CreateVolume(context.Background(), scheduleVolumeRequest(volumeID, map[string]string{
		"snapshotSchedule": "0 0 * * *",
	}))
	require.NoError(t, err)
	ds, err := client.DatasetGet(context.Background(), "pool/parent/"+volumeID)
	require.NoError(t, err)
	schema := ds.UserProperties[PropSnapshotNamingSchema].Value
	require.True(t, schemaProvesVolumeOwnership(schema, volumeID))
	return schema
}

// renderScheduledSnapshotName renders what the TrueNAS task would name a
// snapshot for a driver-minted schema.
func renderScheduledSnapshotName(schema string) string {
	rendered := strings.ReplaceAll(schema, "%Y%m%d", "20260731")
	return strings.ReplaceAll(rendered, "%H%M%S", "120000")
}

func TestDeleteVolumeTreatsScheduledSnapshotAsOwnedNotForeign(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	schema := seedScheduledVolume(t, client, d, "sched-owned")
	_, err := client.SnapshotCreate(ctx, "pool/parent/sched-owned", renderScheduledSnapshotName(schema), nil)
	require.NoError(t, err)

	// destroyForeignSnapshotsOnDelete is false, yet deletion succeeds because the
	// snapshot's name proves out against the dataset's driver-minted schema (R4).
	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-owned"})
	require.NoError(t, err, "a PROVEN driver-scheduled snapshot must not trip the foreign guard")
}

// B1 — THE data-destruction regression. A foreign snapshot whose name merely
// begins with the schema's literal prefix ("csi-") must SURVIVE: the old
// prefix-only classifier declared it driver-owned, bypassed the foreign guard,
// and recursively destroyed it.
func TestDeleteVolumePreservesForeignSnapshotSharingScheduledPrefix(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	seedScheduledVolume(t, client, d, "sched-prefix")
	_, err := client.SnapshotCreate(ctx, "pool/parent/sched-prefix", "csi-manual-do-not-delete", nil)
	require.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-prefix"})
	require.Error(t, err, "a prefix-matching FOREIGN snapshot must still trip the foreign guard")
	assert.Contains(t, err.Error(), "non-CSI snapshots")

	survivor, err := client.SnapshotGet(ctx, "pool/parent/sched-prefix@csi-manual-do-not-delete")
	require.NoError(t, err, "the foreign snapshot must not be destroyed")
	assert.NotNil(t, survivor)
}

// B1 — a snapshot whose name is fully schema-SHAPED but carries a DIFFERENT
// nonce (an outsider imitating the convention, or another volume's schema) is
// not provable and must stay foreign.
func TestDeleteVolumePreservesSchemaShapedSnapshotWithForeignNonce(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	seedScheduledVolume(t, client, d, "sched-nonce")
	imitation := driverScheduledNamingSchema("sched-nonce", "0123456789abcdef")
	_, err := client.SnapshotCreate(ctx, "pool/parent/sched-nonce", renderScheduledSnapshotName(imitation), nil)
	require.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-nonce"})
	require.Error(t, err, "a schema-shaped name with a foreign nonce proves nothing")
	assert.Contains(t, err.Error(), "non-CSI snapshots")
}

func TestDeleteVolumeStillRefusesGenuinelyForeignSnapshot(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	_, err := d.CreateVolume(context.Background(), scheduleVolumeRequest("sched-foreign", nil))
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, "pool/parent/sched-foreign", "auto-2026-07-31_12-00", nil)
	require.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-foreign"})
	require.Error(t, err, "a genuinely foreign snapshot must still be refused")
	assert.Contains(t, err.Error(), "non-CSI snapshots")
}

// H2 — the task is removed BEFORE the foreign guard can refuse, so a refusal can
// never be self-sustaining (the task would otherwise keep minting snapshots).
func TestDeleteVolumeRemovesTaskEvenWhenForeignGuardRefuses(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	seedScheduledVolume(t, client, d, "sched-guarded")
	_, err := client.SnapshotCreate(ctx, "pool/parent/sched-guarded", "auto-2026-07-31_12-00", nil)
	require.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-guarded"})
	require.Error(t, err)
	assert.Empty(t, client.SnapshotTasks, "the task must be gone even though the delete was refused")
}

func TestDeleteVolumeDeletesSnapshotTask(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)

	seedScheduledVolume(t, client, d, "sched-del")
	require.Len(t, client.SnapshotTasks, 1)
	var taskID int
	for id := range client.SnapshotTasks {
		taskID = id
	}

	_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "sched-del"})
	require.NoError(t, err)
	assert.Empty(t, client.SnapshotTasks, "DeleteVolume removes the driver-owned task")
	assert.Contains(t, client.SnapshotTaskDeleteCalls, taskID)
}

// H2 — a pre-existing FOREIGN task on the volume's dataset must never be adopted
// (and therefore never deleted as if the driver owned it).
func TestScheduledTaskAdoptionIgnoresForeignTask(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)

	foreign, err := client.SnapshotTaskCreate(ctx, &truenas.SnapshotTaskCreateParams{
		Dataset:      "pool/parent/sched-adopt",
		Recursive:    true,
		NamingSchema: "auto-%Y-%m-%d_%H-%M",
	})
	require.NoError(t, err)

	seedScheduledVolume(t, client, d, "sched-adopt")
	assert.Len(t, client.SnapshotTasks, 2, "the driver creates its OWN task alongside the foreign one")

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-adopt"})
	require.NoError(t, err)
	assert.Contains(t, client.SnapshotTasks, foreign.ID, "the foreign task survives DeleteVolume")
	assert.Len(t, client.SnapshotTasks, 1)
}

// codex MED — the scheduled-snapshot gauge was unreachable in production because
// the pass's snapshot partition discarded non-CSI snapshots before classification.
func TestReconcileCountsScheduledSnapshotsFromProductionPartition(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)

	ds := addReconcileDataset(client, "source", time.Now().Add(-72*time.Hour), true, testGiB)
	require.NoError(t, client.DatasetSetUserProperty(ctx, ds.Name, PropDriverInstanceID, d.driverInstanceID()))
	schema, err := newDriverScheduledNamingSchema("source")
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, ds.Name, PropSnapshotNamingSchema, schema))
	_, err = client.SnapshotCreate(ctx, ds.Name, renderScheduledSnapshotName(schema), nil)
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, ds.Name, "auto-2026-07-31_12-00", nil)
	require.NoError(t, err)

	// Go through the PRODUCTION partition, not a hand-built snapshot slice.
	managed, tombstones, unowned, err := d.listAllManagedSnapshots(ctx)
	require.NoError(t, err)
	assert.Empty(t, managed)
	assert.Empty(t, tombstones)
	require.Len(t, unowned, 2)

	report := &ReconcileReport{}
	listed, err := client.DatasetQueryByParent(ctx, "pool/parent")
	require.NoError(t, err)
	d.countScheduledSnapshots(unowned, listed, report)
	assert.Equal(t, 1, report.ScheduledSnapshotCount, "only the schema-proven snapshot is counted")

	count := d.classifyOrphanSnapshots(time.Now(), managed, &kubernetesReconcileState{snapshotHandles: map[string]struct{}{}}, time.Hour, report)
	assert.Equal(t, 0, count)
	assert.Empty(t, report.OrphanSnapshots, "a scheduled snapshot is never an orphan/delete candidate (R4)")
}
