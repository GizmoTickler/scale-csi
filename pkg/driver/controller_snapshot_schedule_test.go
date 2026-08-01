package driver

import (
	"context"
	"regexp"
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

// independentSchemaPattern is written from the DOCUMENTED schema contract
// (`csi-<volume>-<16 lowercase hex>-%Y%m%d-%H%M%S`), not from the driver's
// constants, so a test that uses it does not merely re-run the production
// algorithm and compare it with itself. assertMintedSchemaShape is the
// independent replacement for `schemaProvesVolumeOwnership(...)` self-checks.
var independentSchemaPattern = regexp.MustCompile(`^csi-(.+)-([0-9a-f]{16})-%Y%m%d-%H%M%S$`)

// assertMintedSchemaShape checks a minted schema against the published contract
// and returns the nonce it carries.
func assertMintedSchemaShape(t *testing.T, schema, volumeID string) string {
	t.Helper()
	match := independentSchemaPattern.FindStringSubmatch(schema)
	require.NotNil(t, match, "schema %q does not match the documented contract csi-<volume>-<16 hex>-%%Y%%m%%d-%%H%%M%%S", schema)
	assert.Equal(t, volumeID, match[1], "the schema must carry THIS volume's id")
	nonce := match[2]
	assert.NotEqual(t, strings.Repeat("0", 16), nonce, "an all-zero nonce would mean the CSPRNG was never consulted")
	return nonce
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
	// The schema is DRIVER-MINTED with an unguessable per-volume nonce. Checked
	// against the DOCUMENTED contract rather than by re-running the production
	// derivation and asserting it agrees with itself.
	nonce := assertMintedSchemaShape(t, spec.namingSchema, "vol1")
	assert.False(t, schemaProvesVolumeOwnership(spec.namingSchema, "vol2"), "another volume's id must not prove out")

	other, err := d.resolveSnapshotTaskSpec(map[string]string{"snapshotSchedule": "30 2 * * 1"}, "vol1")
	require.NoError(t, err)
	assert.NotEqual(t, spec.namingSchema, other.namingSchema, "each mint draws a fresh nonce")
	// Independent entropy evidence: 32 mints for the SAME volume id must yield 32
	// distinct nonces. A nonce derived from the volume id (or a constant) fails.
	nonces := map[string]struct{}{nonce: {}}
	for i := 0; i < 32; i++ {
		mint, mintErr := d.resolveSnapshotTaskSpec(map[string]string{"snapshotSchedule": "30 2 * * 1"}, "vol1")
		require.NoError(t, mintErr)
		nonces[assertMintedSchemaShape(t, mint.namingSchema, "vol1")] = struct{}{}
	}
	assert.Len(t, nonces, 33, "the nonce must be freshly drawn, not derived from the volume id")

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
		taskNonce := assertMintedSchemaShape(t, task.NamingSchema, "sched-vol")
		assert.NotEmpty(t, taskNonce)
	}

	ds, err := client.DatasetGet(context.Background(), "pool/parent/sched-vol")
	require.NoError(t, err)
	assert.NotEmpty(t, ds.UserProperties[PropSnapshotTaskID].Value, "the task id is bound to the dataset")
	stampedSchema := ds.UserProperties[PropSnapshotNamingSchema].Value
	assertMintedSchemaShape(t, stampedSchema, "sched-vol")
	for _, task := range client.SnapshotTasks {
		assert.Equal(t, task.NamingSchema, stampedSchema,
			"the dataset binding and the live task must name the SAME schema, or nothing later can pair them")
	}
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

// seedScheduledVolume creates a scheduled volume and returns the schema stamped
// on its dataset, checked against the DOCUMENTED schema contract rather than by
// re-running the production derivation and comparing it with itself.
func seedScheduledVolume(t *testing.T, client *truenas.MockClient, d *Driver, volumeID string) string {
	t.Helper()
	_, err := d.CreateVolume(context.Background(), scheduleVolumeRequest(volumeID, map[string]string{
		"snapshotSchedule": "0 0 * * *",
	}))
	require.NoError(t, err)
	ds, err := client.DatasetGet(context.Background(), "pool/parent/"+volumeID)
	require.NoError(t, err)
	schema := ds.UserProperties[PropSnapshotNamingSchema].Value
	assertMintedSchemaShape(t, schema, volumeID)
	return schema
}

// seededSchemaNonce returns the nonce carried by a seeded volume's schema,
// extracted with the independent contract pattern.
func seededSchemaNonce(t *testing.T, schema string) string {
	t.Helper()
	match := independentSchemaPattern.FindStringSubmatch(schema)
	require.NotNil(t, match, "schema %q does not match the documented contract", schema)
	return match[2]
}

// fireScheduledSnapshot has the MOCK's periodic-snapshot task machinery take the
// snapshot, exactly as the TrueNAS middleware would: the mock renders the task's
// naming schema through its OWN strftime expansion and stamps the matching
// creation property. Nothing in production names this snapshot, so a test built
// on it exercises the driver rather than itself.
func fireScheduledSnapshot(t *testing.T, client *truenas.MockClient, at time.Time, zone *time.Location) *truenas.Snapshot {
	t.Helper()
	created, err := client.FireSnapshotTasks(context.Background(), at, zone)
	require.NoError(t, err)
	require.Len(t, created, 1, "exactly one enabled task should have fired")
	return created[0]
}

// forgeSnapshot creates an unlabeled snapshot with a chosen name and a chosen
// creation time — the adversary's tool, used only in NEGATIVE tests.
func forgeSnapshot(t *testing.T, client *truenas.MockClient, dataset, name string, creation time.Time) {
	t.Helper()
	snap, err := client.SnapshotCreate(context.Background(), dataset, name, nil)
	require.NoError(t, err)
	client.SetSnapshotCreationTime(snap.ID, creation.Unix())
}

// renderScheduledSnapshotName renders a schema at a chosen instant the way a
// TrueNAS task would. Used to FORGE names in negative tests.
func renderScheduledSnapshotName(schema string, at time.Time) string {
	rendered := strings.ReplaceAll(schema, "%Y%m%d", at.Format("20060102"))
	return strings.ReplaceAll(rendered, "%H%M%S", at.Format("150405"))
}

// TestDeleteVolumeAcceptsTaskCreatedScheduledSnapshot is a FALSE-NEGATIVE guard,
// not an ownership proof, and it is labeled that way deliberately.
//
// REVERT-PROOF STATUS: this test PASSES on 03d37b8 by construction — a positive
// test cannot fail against a predicate that is too PERMISSIVE. Its job is the
// opposite direction: to prove the tightened predicate (canonical rendering,
// real calendar instant, creation-time agreement, live-task corroboration) does
// not reject a snapshot the driver's own task genuinely produced, including on a
// NAS whose timezone the driver cannot see. The revert-proof evidence for B1
// lives in the four negative tests below.
func TestDeleteVolumeAcceptsTaskCreatedScheduledSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name string
		zone *time.Location
	}{
		{"utc NAS", time.UTC},
		{"west-of-UTC NAS", time.FixedZone("PST", -8*3600)},
		{"quarter-hour-offset NAS", time.FixedZone("NPT", 5*3600+45*60)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := truenas.NewMockClient()
			d := newScheduleTestDriver(client)
			ctx := context.Background()

			seedScheduledVolume(t, client, d, "sched-owned")
			snap := fireScheduledSnapshot(t, client, time.Now().Add(-time.Hour), tc.zone)
			require.Equal(t, "pool/parent/sched-owned", snap.Dataset)

			// destroyForeignSnapshotsOnDelete is false, yet deletion succeeds: the
			// full ownership chain proves out for a task-created snapshot (R4).
			_, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-owned"})
			require.NoError(t, err, "a task-created scheduled snapshot must not trip the foreign guard")
		})
	}
}

// GF2-fix2/B1-b — a snapshot that satisfies every name/nonce/dataset/stamp check
// but has NO corroborating live periodic-snapshot task must stay FOREIGN. This
// is the "who created it" gap: an exact-schema name proves the creator could READ
// the schema, not that the driver's task wrote it.
//
// REVERT-PROOF: on 03d37b8 the five-link predicate accepts this snapshot, the
// foreign guard is bypassed, and the recursive destroy removes it — so the
// require.Error below fails on the pre-fix commit. Verified by running this test
// against 03d37b8 (see the fix summary).
func TestDeleteVolumePreservesExactSchemaSnapshotWithoutCorroboratingTask(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	schema := seedScheduledVolume(t, client, d, "sched-notask")
	// An actor who can read the dataset property (or pool.snapshottask.query)
	// mints a perfectly-shaped name AND creates it at the encoded second.
	at := time.Now().Add(-2 * time.Hour)
	forgeSnapshot(t, client, "pool/parent/sched-notask", renderScheduledSnapshotName(schema, at), at)
	// The volume's own task is removed first, so nothing corroborates authorship.
	for id := range client.SnapshotTasks {
		require.NoError(t, client.SnapshotTaskDelete(ctx, id))
	}

	_, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-notask"})
	require.Error(t, err, "with no corroborating task, an exact-schema snapshot proves nothing")
	assert.Contains(t, err.Error(), "non-CSI snapshots")

	survivor, err := client.SnapshotGet(ctx, "pool/parent/sched-notask@"+renderScheduledSnapshotName(schema, at))
	require.NoError(t, err, "the snapshot must survive")
	assert.NotNil(t, survivor)
}

// GF2-fix2/B1-a — an exact-schema, task-corroborated name whose encoded instant
// does NOT agree with the snapshot's real creation property must stay FOREIGN.
// A genuine task always agrees; a name authored at leisure almost never does.
//
// REVERT-PROOF: 03d37b8 never reads the creation property at all, so it accepts
// this snapshot and destroys it. Verified against 03d37b8.
func TestDeleteVolumePreservesSchemaShapedSnapshotWithDisagreeingCreationTime(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	schema := seedScheduledVolume(t, client, d, "sched-skew")
	// The name says 09:07:33; the snapshot was really created 7 minutes later,
	// which no civil UTC offset can explain (offsets are 15-minute multiples).
	named := time.Date(2026, 7, 31, 9, 7, 33, 0, time.UTC)
	forgeSnapshot(t, client, "pool/parent/sched-skew", renderScheduledSnapshotName(schema, named), named.Add(7*time.Minute))

	_, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-skew"})
	require.Error(t, err, "a name whose timestamp disagrees with the creation property proves nothing")
	assert.Contains(t, err.Error(), "non-CSI snapshots")
}

// GF2-fix2/B1-c — the matcher accepted any eight digits plus six digits, so a
// name encoding an impossible date passed the "complete rendering" contract.
//
// REVERT-PROOF: 03d37b8's `\d{8}-\d{6}` accepts 20260230-250000 and destroys the
// snapshot. Verified against 03d37b8.
func TestDeleteVolumePreservesSchemaShapedSnapshotWithImpossibleTimestamp(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	schema := seedScheduledVolume(t, client, d, "sched-baddate")
	name := strings.ReplaceAll(schema, "%Y%m%d", "20260230")
	name = strings.ReplaceAll(name, "%H%M%S", "250000")
	forgeSnapshot(t, client, "pool/parent/sched-baddate", name, time.Now())

	_, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-baddate"})
	require.Error(t, err, "30 February at 25:00 is not an instant any task ever rendered")
	assert.Contains(t, err.Error(), "non-CSI snapshots")

	survivor, err := client.SnapshotGet(ctx, "pool/parent/sched-baddate@"+name)
	require.NoError(t, err)
	assert.NotNil(t, survivor)
}

// GF2-fix2/B1-c — the matcher pushed the captured volume segment through
// sanitizeVolumeID before comparing, so a NON-CANONICAL spelling that merely
// sanitizes to the dataset leaf re-rendered to the stamped schema and passed.
// sanitizeVolumeID prefixes 'v' to a name that does not start with a lowercase
// alphanumeric, so segment "Abc" sanitizes to leaf "vAbc".
//
// REVERT-PROOF: 03d37b8 re-renders driverScheduledNamingSchema("Abc", nonce),
// which sanitizes to "csi-vAbc-<nonce>-…" — equal to the stamped schema — so it
// accepts and destroys. Verified against 03d37b8.
func TestDeleteVolumePreservesNonCanonicalVolumeSegment(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	schema := seedScheduledVolume(t, client, d, "vAbc")
	nonce := seededSchemaNonce(t, schema)
	at := time.Now().Add(-30 * time.Minute)
	name := "csi-Abc-" + nonce + "-" + at.UTC().Format("20060102-150405")
	forgeSnapshot(t, client, "pool/parent/vAbc", name, at)

	_, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "vAbc"})
	require.Error(t, err, "only the canonical volume segment is a rendering of the schema")
	assert.Contains(t, err.Error(), "non-CSI snapshots")

	survivor, err := client.SnapshotGet(ctx, "pool/parent/vAbc@"+name)
	require.NoError(t, err)
	assert.NotNil(t, survivor)
}

// GF2-fix2/B1-b — the corroboration must survive a RETRY of a DeleteVolume that
// failed after the task was already removed. Otherwise the H2 ordering (delete
// the task before the foreign guard) would wedge the volume's own scheduled
// snapshots as foreign forever, which is a worse failure than the one H2 fixed.
func TestDeleteVolumeRetryStillOwnsScheduledSnapshotAfterTaskRemoved(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	seedScheduledVolume(t, client, d, "sched-retry")
	fireScheduledSnapshot(t, client, time.Now().Add(-time.Hour), time.UTC)

	// Attempt 1 fails at the dataset destroy, AFTER the task has been deleted.
	client.FailDatasetDelete = map[string]struct{}{"pool/parent/sched-retry": {}}
	_, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-retry"})
	require.Error(t, err, "the injected destroy failure must surface")
	assert.Empty(t, client.SnapshotTasks, "H2: the task is gone even though the delete failed")

	// Attempt 2 must still recognize the driver's own scheduled snapshot.
	client.FailDatasetDelete = nil
	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-retry"})
	require.NoError(t, err, "the retry must not be wedged behind the foreign guard")
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
//
// The foreign nonce is DERIVED from the real one by flipping its first hex
// digit, so the test is deterministic. The previous version compared a fixed
// literal against a randomly minted 64-bit nonce: astronomically unlikely to
// collide, but not a proof.
func TestDeleteVolumePreservesSchemaShapedSnapshotWithForeignNonce(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	ctx := context.Background()

	schema := seedScheduledVolume(t, client, d, "sched-nonce")
	realNonce := seededSchemaNonce(t, schema)
	foreignNonce := flipFirstHexDigit(realNonce)
	require.NotEqual(t, realNonce, foreignNonce)

	at := time.Now().Add(-time.Hour)
	name := "csi-sched-nonce-" + foreignNonce + "-" + at.UTC().Format("20060102-150405")
	forgeSnapshot(t, client, "pool/parent/sched-nonce", name, at)

	_, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-nonce"})
	require.Error(t, err, "a schema-shaped name with a foreign nonce proves nothing")
	assert.Contains(t, err.Error(), "non-CSI snapshots")

	survivor, err := client.SnapshotGet(ctx, "pool/parent/sched-nonce@"+name)
	require.NoError(t, err)
	assert.NotNil(t, survivor)
}

// flipFirstHexDigit perturbs a nonce deterministically: '0'->'1', anything
// else -> '0'.
func flipFirstHexDigit(nonce string) string {
	if nonce == "" {
		return nonce
	}
	if nonce[0] == '0' {
		return "1" + nonce[1:]
	}
	return "0" + nonce[1:]
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
	_, err = client.SnapshotTaskCreate(ctx, &truenas.SnapshotTaskCreateParams{
		Dataset: ds.Name, NamingSchema: schema, Enabled: true,
	})
	require.NoError(t, err)
	// The task takes its own snapshot (mock-rendered, mock-timestamped).
	fireScheduledSnapshot(t, client, time.Now().Add(-time.Hour), time.UTC)
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
