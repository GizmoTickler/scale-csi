package driver

import (
	"context"
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

	// Per-SC parameter overrides the global default.
	spec, err := d.resolveSnapshotTaskSpec(map[string]string{"snapshotSchedule": "30 2 * * 1", "snapshotRetention": "2w"}, "vol1")
	require.NoError(t, err)
	require.NotNil(t, spec)
	assert.Equal(t, "30", spec.schedule["minute"])
	assert.Equal(t, 2, spec.lifetimeValue)
	assert.Equal(t, "WEEK", spec.lifetimeUnit)
	assert.Equal(t, "csi-%Y%m%d-%H%M%S-vol1", spec.namingSchema)

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
		assert.Contains(t, task.NamingSchema, "csi-")
	}

	ds, err := client.DatasetGet(context.Background(), "pool/parent/sched-vol")
	require.NoError(t, err)
	assert.NotEmpty(t, ds.UserProperties[PropSnapshotTaskID].Value, "the task id is bound to the dataset")
	assert.Contains(t, ds.UserProperties[PropSnapshotNamingSchema].Value, "csi-")
}

func TestCreateVolumeIdempotentDoesNotDuplicateTask(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	req := scheduleVolumeRequest("sched-idem", map[string]string{"snapshotSchedule": "0 0 * * *"})

	_, err := d.CreateVolume(context.Background(), req)
	require.NoError(t, err)
	_, err = d.CreateVolume(context.Background(), req)
	require.NoError(t, err)

	assert.Len(t, client.SnapshotTasks, 1, "the retry adopts the existing task rather than duplicating it")
}

func TestDeleteVolumeDeletesSnapshotTask(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)

	_, err := d.CreateVolume(context.Background(), scheduleVolumeRequest("sched-del", map[string]string{"snapshotSchedule": "0 0 * * *"}))
	require.NoError(t, err)
	require.Len(t, client.SnapshotTasks, 1)
	var taskID int
	for id := range client.SnapshotTasks {
		taskID = id
	}

	_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "sched-del"})
	require.NoError(t, err)
	assert.Empty(t, client.SnapshotTasks, "DeleteVolume removes the driver-owned task")
	assert.Contains(t, client.SnapshotTaskDeleteCalls, taskID)
}

// seedScheduledSnapshot stamps a volume dataset as scheduled and creates a
// task-shaped snapshot (no CSI props, P2) under it, returning the snapshot.
func seedScheduledSnapshot(t *testing.T, client *truenas.MockClient, d *Driver, datasetName, snapName string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropDriverInstanceID, d.driverInstanceID()))
	require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropSnapshotNamingSchema, "csi-%Y%m%d-%H%M%S-vol"))
	_, err := client.SnapshotCreate(ctx, datasetName, snapName, nil) // no CSI props => foreign-looking
	require.NoError(t, err)
}

func TestDeleteVolumeTreatsScheduledSnapshotAsOwnedNotForeign(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	_, err := d.CreateVolume(context.Background(), scheduleVolumeRequest("sched-owned", nil))
	require.NoError(t, err)
	// A driver-scheduled snapshot (task-shaped name, no CSI props) under the volume.
	seedScheduledSnapshot(t, client, d, "pool/parent/sched-owned", "csi-20260731-120000-vol")

	// destroyForeignSnapshotsOnDelete is false, yet deletion succeeds because the
	// snapshot is recognized as driver-owned scheduled, not foreign (R4).
	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-owned"})
	require.NoError(t, err, "a driver-scheduled snapshot must not trip the foreign guard")
}

func TestDeleteVolumeStillRefusesGenuinelyForeignSnapshot(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	_, err := d.CreateVolume(context.Background(), scheduleVolumeRequest("sched-foreign", nil))
	require.NoError(t, err)
	// A genuinely foreign snapshot (box-wide task style name, no CSI props, no
	// schema match) must still trip the foreign guard.
	_, err = client.SnapshotCreate(ctx, "pool/parent/sched-foreign", "auto-2026-07-31_12-00", nil)
	require.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-foreign"})
	require.Error(t, err, "a genuinely foreign snapshot must still be refused")
	assert.Contains(t, err.Error(), "non-CSI snapshots")
}

func TestClassifyOrphanSnapshotsCountsScheduledNeverOrphans(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("source", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)

	ds := addReconcileDataset(client, "source", time.Now().Add(-72*time.Hour), true, testGiB)
	require.NoError(t, client.DatasetSetUserProperty(ctx, ds.Name, PropDriverInstanceID, d.driverInstanceID()))
	require.NoError(t, client.DatasetSetUserProperty(ctx, ds.Name, PropSnapshotNamingSchema, "csi-%Y%m%d-%H%M%S-vol"))
	_, err := client.SnapshotCreate(ctx, ds.Name, "csi-20260731-120000-vol", nil)
	require.NoError(t, err)

	snapshots, err := client.SnapshotList(ctx, ds.Name)
	require.NoError(t, err)
	datasets := []*truenas.Dataset{ds}

	report := &ReconcileReport{}
	count := d.classifyOrphanSnapshots(time.Now(), snapshots, datasets, &kubernetesReconcileState{snapshotHandles: map[string]struct{}{}}, time.Hour, report)
	assert.Equal(t, 0, count, "a scheduled snapshot is not a managed CSI snapshot")
	assert.Equal(t, 1, report.ScheduledSnapshotCount, "the scheduled snapshot is counted for visibility")
	assert.Empty(t, report.OrphanSnapshots, "a scheduled snapshot is never an orphan/delete candidate (R4)")
}
