package driver

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// snapshotHandleCountingClient wraps the mock so the resolver's lookup SHAPE is
// observable: a dataset-qualified handle must resolve through ONE non-recursive
// SnapshotList of the encoded dataset (recorded with its dataset argument),
// while a legacy short-name handle must keep the SnapshotFindByName scan it has
// always used — and neither path may leak into the other. SnapshotListAll is
// counted for the ListSnapshots pagination cache assertions (one full fetch per
// walk).
type snapshotHandleCountingClient struct {
	*truenas.MockClient
	snapshotListCalls       int
	snapshotListDatasets    []string
	snapshotFindByNameCalls int
	snapshotListAllCalls    int
}

func (c *snapshotHandleCountingClient) SnapshotList(ctx context.Context, dataset string) ([]*truenas.Snapshot, error) {
	c.snapshotListCalls++
	c.snapshotListDatasets = append(c.snapshotListDatasets, dataset)
	return c.MockClient.SnapshotList(ctx, dataset)
}

func (c *snapshotHandleCountingClient) SnapshotFindByName(ctx context.Context, parentDataset, name string) (*truenas.Snapshot, error) {
	c.snapshotFindByNameCalls++
	return c.MockClient.SnapshotFindByName(ctx, parentDataset, name)
}

func (c *snapshotHandleCountingClient) SnapshotListAll(ctx context.Context, parentDataset string, limit, offset int) ([]*truenas.Snapshot, error) {
	c.snapshotListAllCalls++
	return c.MockClient.SnapshotListAll(ctx, parentDataset, limit, offset)
}

func (c *snapshotHandleCountingClient) resetCounts() {
	c.snapshotListCalls = 0
	c.snapshotListDatasets = nil
	c.snapshotFindByNameCalls = 0
	c.snapshotListAllCalls = 0
}

func newSnapshotHandleFixture(t *testing.T) (*snapshotHandleCountingClient, *Driver) {
	t.Helper()
	client := &snapshotHandleCountingClient{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", DatasetEnableQuotas: true},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client)
	return client, d
}

func createHandleTestSourceVolume(t *testing.T, client *snapshotHandleCountingClient, name string) {
	t.Helper()
	_, err := client.MockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/" + name, Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
}

func TestParseQualifiedSnapshotHandle(t *testing.T) {
	for _, tc := range []struct {
		name        string
		handle      string
		wantDataset string
		wantShort   string
		wantOK      bool
	}{
		{name: "well-formed", handle: "pool/parent/vol@snap-1", wantDataset: "pool/parent/vol", wantShort: "snap-1", wantOK: true},
		{name: "legacy short name is not qualified", handle: "snap-1"},
		{name: "empty", handle: ""},
		{name: "empty dataset", handle: "@snap-1"},
		{name: "empty short name", handle: "pool/parent/vol@"},
		{name: "multiple @ is malformed, never legacy", handle: "pool/parent/vol@a@b"},
		{name: "slash in short name is malformed", handle: "pool/parent/vol@a/b"},
		{name: "bare @", handle: "@"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataset, short, ok := parseQualifiedSnapshotHandle(tc.handle)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantDataset, dataset)
			assert.Equal(t, tc.wantShort, short)
		})
	}
}

func TestSnapshotHandleShortName(t *testing.T) {
	assert.Equal(t, "snap-1", snapshotHandleShortName("snap-1"), "a legacy handle IS the short name")
	assert.Equal(t, "snap-1", snapshotHandleShortName("pool/parent/vol@snap-1"))
	assert.Equal(t, "", snapshotHandleShortName("pool/parent/vol@a@b"), "malformed handles address nothing")
	assert.Equal(t, "", snapshotHandleShortName("@"))
	assert.Equal(t, "", snapshotHandleShortName(""))
}

// Format detection is only unambiguous if a sanitized short name can never
// contain the qualified-handle delimiters. '/' and ' ' were always replaced;
// '@' is replaced too (ZFS forbids it in snapshot names, so no pre-existing
// snapshot can carry one — see sanitizeVolumeID).
func TestSanitizeVolumeIDNeverEmitsHandleDelimiters(t *testing.T) {
	for _, input := range []string{
		"snap@shot", "@leading", "trailing@", "a@b@c", "data/set@snap", "mixed @/chars", "🔥@🔥",
	} {
		got := sanitizeVolumeID(input)
		assert.NotContains(t, got, "@", "input %q", input)
		assert.NotContains(t, got, "/", "input %q", input)
	}
}

// The resolver's hostile-input contract: anything that cannot be a handle this
// driver issued resolves to (nil, nil) — the same "not found, not an error"
// every caller already maps to its idempotent/NotFound outcome — and a foreign
// or malformed handle must not touch the backend at all.
func TestResolveSnapshotHandleHostileInputs(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotHandleFixture(t)

	for _, tc := range []struct {
		name   string
		handle string
	}{
		{name: "empty handle", handle: ""},
		{name: "dataset outside the configured parent", handle: "other/tree/vol@snap-1"},
		{name: "the parent itself is not under the parent", handle: "pool/parent@snap-1"},
		{name: "multiple @", handle: "pool/parent/vol@a@b"},
		{name: "empty short name", handle: "pool/parent/vol@"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client.resetCounts()
			snap, err := d.resolveSnapshotHandle(ctx, tc.handle)
			require.NoError(t, err)
			assert.Nil(t, snap)
			assert.Zero(t, client.snapshotListCalls, "no targeted read for a handle the driver never issued")
			assert.Zero(t, client.snapshotFindByNameCalls, "no scan for a handle the driver never issued")
		})
	}
}

// A qualified handle whose encoded dataset no longer holds the snapshot must
// still resolve: DeleteVolume's clone promotion migrates snapshots to the
// promoted dataset while the CO keeps the original handle. The targeted miss
// falls back to the global short-name scan (safe: CreateSnapshot enforces
// global short-name uniqueness).
func TestResolveSnapshotHandleFindsPromotionMigratedSnapshot(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotHandleFixture(t)
	createHandleTestSourceVolume(t, client, "promoted")
	migrated, err := client.MockClient.SnapshotCreate(ctx, "pool/parent/promoted", "moved-snap", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "moved-snap",
		PropCSISnapshotSourceVolumeID: "old-source",
		PropCSISnapshotHandle:         "pool/parent/old-source@moved-snap",
	})
	require.NoError(t, err)

	client.resetCounts()
	snap, err := d.resolveSnapshotHandle(ctx, "pool/parent/old-source@moved-snap")
	require.NoError(t, err)
	require.NotNil(t, snap, "the stale-dataset handle must still find the migrated snapshot")
	assert.Equal(t, migrated.ID, snap.ID)
	assert.Equal(t, 1, client.snapshotListCalls, "the targeted read is tried first")
	assert.Equal(t, []string{"pool/parent/old-source"}, client.snapshotListDatasets)
	assert.Equal(t, 1, client.snapshotFindByNameCalls, "one fallback scan on the targeted miss")
}

func TestCreateSnapshotReturnsDatasetQualifiedHandle(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotHandleFixture(t)
	createHandleTestSourceVolume(t, client, "src")

	req := &csi.CreateSnapshotRequest{Name: "snap-a", SourceVolumeId: "src"}
	resp, err := d.CreateSnapshot(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "pool/parent/src@snap-a", resp.GetSnapshot().GetSnapshotId())

	// The handle is stamped IN the creation property map (26.0 cannot update
	// snapshot properties post-create), so reads report it back.
	stored := client.Snapshots["pool/parent/src@snap-a"]
	require.NotNil(t, stored)
	assert.Equal(t, "pool/parent/src@snap-a", stored.UserProperties[PropCSISnapshotHandle].Value)

	// Idempotent retry returns the IDENTICAL handle (read back from the stamp).
	retry, err := d.CreateSnapshot(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, resp.GetSnapshot().GetSnapshotId(), retry.GetSnapshot().GetSnapshotId())
}

// An idempotent retry that matches a PRE-UPGRADE snapshot (no stamp) must keep
// returning the legacy short handle its VolumeSnapshotContent already stores —
// a handle flap would make the CO see a different snapshot.
func TestCreateSnapshotRetryOfUnstampedLegacySnapshotReturnsShortHandle(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotHandleFixture(t)
	createHandleTestSourceVolume(t, client, "legacy-src")
	_, err := client.MockClient.SnapshotCreate(ctx, "pool/parent/legacy-src", "legacy-snap", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "legacy-snap",
		PropCSISnapshotSourceVolumeID: "legacy-src",
	})
	require.NoError(t, err)

	resp, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "legacy-snap", SourceVolumeId: "legacy-src"})
	require.NoError(t, err)
	assert.Equal(t, "legacy-snap", resp.GetSnapshot().GetSnapshotId(),
		"a pre-upgrade snapshot keeps its legacy handle")
}

func TestDeleteSnapshotQualifiedHandleUsesTargetedLookup(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotHandleFixture(t)
	createHandleTestSourceVolume(t, client, "del-src")
	created, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "del-snap", SourceVolumeId: "del-src"})
	require.NoError(t, err)
	handle := created.GetSnapshot().GetSnapshotId()
	require.Equal(t, "pool/parent/del-src@del-snap", handle)

	client.resetCounts()
	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: handle})
	require.NoError(t, err)
	assert.Equal(t, 1, client.snapshotListCalls, "a qualified handle resolves via ONE targeted SnapshotList")
	assert.Equal(t, []string{"pool/parent/del-src"}, client.snapshotListDatasets)
	assert.Zero(t, client.snapshotFindByNameCalls, "no full-parent scan on the targeted hit")
	_, exists := client.Snapshots["pool/parent/del-src@del-snap"]
	assert.False(t, exists, "the snapshot was actually deleted")

	// Idempotent second delete: the targeted miss legitimately pays one
	// fallback scan (the promotion-migration safety net) before concluding
	// "already deleted".
	client.resetCounts()
	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: handle})
	require.NoError(t, err)
}

func TestDeleteSnapshotLegacyHandleKeepsScanBehavior(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotHandleFixture(t)
	createHandleTestSourceVolume(t, client, "legacy-del")
	_, err := client.MockClient.SnapshotCreate(ctx, "pool/parent/legacy-del", "legacy-del-snap", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "legacy-del-snap",
		PropCSISnapshotSourceVolumeID: "legacy-del",
	})
	require.NoError(t, err)

	client.resetCounts()
	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "legacy-del-snap"})
	require.NoError(t, err)
	assert.Equal(t, 1, client.snapshotFindByNameCalls, "legacy handles keep the scan they always used")
	assert.Zero(t, client.snapshotListCalls, "no targeted read exists for a handle without a dataset")
	_, exists := client.Snapshots["pool/parent/legacy-del@legacy-del-snap"]
	assert.False(t, exists)

	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "legacy-del-snap"})
	require.NoError(t, err, "idempotent legacy delete")
}

func TestListSnapshotsByIDResolvesBothHandleFormats(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotHandleFixture(t)
	createHandleTestSourceVolume(t, client, "list-src")
	created, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "list-snap", SourceVolumeId: "list-src"})
	require.NoError(t, err)
	qualified := created.GetSnapshot().GetSnapshotId()

	// Qualified handle: targeted lookup, entry reports the SAME handle.
	client.resetCounts()
	resp, err := d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SnapshotId: qualified})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	assert.Equal(t, qualified, resp.Entries[0].Snapshot.GetSnapshotId(),
		"the by-id entry must echo the incoming handle when it matches")
	assert.Equal(t, 1, client.snapshotListCalls)
	assert.Zero(t, client.snapshotFindByNameCalls)

	// A pre-upgrade (unstamped) snapshot addressed by its legacy handle keeps
	// the scan and reports the legacy handle.
	_, err = client.MockClient.SnapshotCreate(ctx, "pool/parent/list-src", "legacy-list-snap", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "legacy-list-snap",
		PropCSISnapshotSourceVolumeID: "list-src",
	})
	require.NoError(t, err)
	client.resetCounts()
	resp, err = d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SnapshotId: "legacy-list-snap"})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	assert.Equal(t, "legacy-list-snap", resp.Entries[0].Snapshot.GetSnapshotId())
	assert.Equal(t, 1, client.snapshotFindByNameCalls)
	assert.Zero(t, client.snapshotListCalls)

	// A missing snapshot stays an empty (not error) response in both formats.
	resp, err = d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SnapshotId: "pool/parent/list-src@absent"})
	require.NoError(t, err)
	assert.Empty(t, resp.GetEntries())
}

// The paged ListSnapshots path reports each snapshot's OWN handle: stamped
// qualified handle when present, legacy short name otherwise — mixed eras in
// one listing.
func TestListSnapshotsEntriesReportPerSnapshotHandles(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotHandleFixture(t)
	createHandleTestSourceVolume(t, client, "mixed-src")
	created, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "stamped-snap", SourceVolumeId: "mixed-src"})
	require.NoError(t, err)
	_, err = client.MockClient.SnapshotCreate(ctx, "pool/parent/mixed-src", "unstamped-snap", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "unstamped-snap",
		PropCSISnapshotSourceVolumeID: "mixed-src",
	})
	require.NoError(t, err)

	resp, err := d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{})
	require.NoError(t, err)
	handles := make([]string, 0, len(resp.GetEntries()))
	for _, entry := range resp.GetEntries() {
		handles = append(handles, entry.Snapshot.GetSnapshotId())
	}
	assert.ElementsMatch(t, []string{created.GetSnapshot().GetSnapshotId(), "unstamped-snap"}, handles)
}

func TestCreateVolumeFromSnapshotBothHandleFormats(t *testing.T) {
	for _, tc := range []struct {
		name           string
		useQualified   bool
		wantList       int
		wantFindByName int
	}{
		{name: "qualified handle uses the targeted lookup", useQualified: true, wantList: 1, wantFindByName: 0},
		{name: "legacy handle keeps the scan", useQualified: false, wantList: 0, wantFindByName: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client, d := newSnapshotHandleFixture(t)
			createHandleTestSourceVolume(t, client, "restore-src")
			created, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "restore-snap", SourceVolumeId: "restore-src"})
			require.NoError(t, err)

			handle := "restore-snap"
			if tc.useQualified {
				handle = created.GetSnapshot().GetSnapshotId()
				require.Equal(t, "pool/parent/restore-src@restore-snap", handle)
			}
			client.resetCounts()
			resp, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
				Name:               "restored-" + tc.name,
				VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
				CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
				VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: handle},
				}},
			})
			require.NoError(t, err)
			assert.Equal(t, handle, resp.GetVolume().GetContentSource().GetSnapshot().GetSnapshotId(),
				"CreateVolume echoes the requested content source verbatim")
			assert.Equal(t, tc.wantList, client.snapshotListCalls)
			assert.Equal(t, tc.wantFindByName, client.snapshotFindByNameCalls)

			restored, err := client.DatasetGet(ctx, "pool/parent/"+sanitizeVolumeID("restored-"+tc.name))
			require.NoError(t, err)
			assert.Equal(t, "pool/parent/restore-src@restore-snap", restored.Origin.Parsed,
				"both formats clone the same backend snapshot")
		})
	}
}

// CreateVolume from a snapshot handle whose dataset lies outside the parent is
// NotFound (never a scan, never InvalidArgument for a well-shaped handle).
func TestCreateVolumeFromForeignQualifiedSnapshotHandleIsNotFound(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotHandleFixture(t)
	client.resetCounts()
	_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "foreign-restore",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "other/tree/vol@snap"},
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "snapshot not found")
	assert.Zero(t, client.snapshotListCalls)
	assert.Zero(t, client.snapshotFindByNameCalls)
}

// A 3-page walk costs exactly ONE full SnapshotListAll fetch; a fresh walk
// (empty starting token) refetches immediately; a continuation token arriving
// after the TTL refetches too. Page contents and next-token generation are
// identical to the uncached per-page behavior.
func TestListSnapshotsPaginationCacheOneFetchPerWalk(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotHandleFixture(t)
	createHandleTestSourceVolume(t, client, "page-src")
	for i := 0; i < 5; i++ {
		_, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
			Name: "page-snap-" + strconv.Itoa(i), SourceVolumeId: "page-src",
		})
		require.NoError(t, err)
	}

	// Reference listing: everything in one uncached page.
	full, err := d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{})
	require.NoError(t, err)
	require.Len(t, full.GetEntries(), 5)
	wantHandles := make([]string, 0, 5)
	for _, entry := range full.GetEntries() {
		wantHandles = append(wantHandles, entry.Snapshot.GetSnapshotId())
	}

	client.resetCounts()
	var gotHandles []string
	token := ""
	pages := 0
	for {
		resp, listErr := d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{MaxEntries: 2, StartingToken: token})
		require.NoError(t, listErr)
		pages++
		for _, entry := range resp.GetEntries() {
			gotHandles = append(gotHandles, entry.Snapshot.GetSnapshotId())
		}
		if resp.GetNextToken() == "" {
			break
		}
		token = resp.GetNextToken()
	}
	assert.Equal(t, 3, pages, "5 snapshots at 2 per page walk in 3 pages")
	assert.Equal(t, wantHandles, gotHandles, "cached paging returns exactly the uncached listing, in order")
	assert.Equal(t, 1, client.snapshotListAllCalls, "one full fetch serves the whole walk")

	// A fresh walk always refetches, even immediately.
	client.resetCounts()
	_, err = d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{MaxEntries: 2})
	require.NoError(t, err)
	assert.Equal(t, 1, client.snapshotListAllCalls, "an empty starting token bypasses the cache")

	// A continuation token after TTL expiry refetches rather than serving
	// stale state.
	client.resetCounts()
	d.snapshotPageCacheMu.Lock()
	d.snapshotPageCacheTime = time.Now().Add(-snapshotListPageCacheTTL - time.Second)
	d.snapshotPageCacheMu.Unlock()
	resp, err := d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{MaxEntries: 2, StartingToken: "2"})
	require.NoError(t, err)
	assert.Equal(t, 1, client.snapshotListAllCalls, "an expired cache is refreshed for a continuation token")
	assert.Len(t, resp.GetEntries(), 2)
	assert.Equal(t, "4", resp.GetNextToken())
}

// Deferred delete (snapshot with clones) addressed by a QUALIFIED handle: the
// tombstone and its ledger entry derive from the RESOLVED snapshot object, so
// the ledger key is byte-identical to what a legacy-handle delete of the same
// snapshot would have written, and the retry/reap lifecycle is unchanged.
func TestDeleteSnapshotQualifiedHandleDeferredDeleteKeepsLedgerConsistent(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotHandleFixture(t)
	// Model TrueNAS 26.0: no deferred destroy, so the tombstone AND its ledger
	// entry must survive for the orphan reaper (a backend that accepts the
	// deferred destroy retires the entry immediately, hiding what this test
	// proves).
	client.NoDeferredSnapshotDestroy = true
	createHandleTestSourceVolume(t, client, "clone-src")
	created, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "cloned-snap", SourceVolumeId: "clone-src"})
	require.NoError(t, err)
	handle := created.GetSnapshot().GetSnapshotId()
	require.NoError(t, client.MockClient.SnapshotClone(ctx, "pool/parent/clone-src@cloned-snap", "pool/parent/restored"))

	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: handle})
	require.NoError(t, err, "a snapshot with clones defers, it does not fail")

	// The tombstone exists and is named from the snapshot's own dataset+name.
	var tombstone *truenas.Snapshot
	for _, snap := range client.Snapshots {
		if strings.HasPrefix(snap.Name, "cloned-snap-csi-deleted-") {
			tombstone = snap
		}
	}
	require.NotNil(t, tombstone, "the deferred delete leaves a tombstone")
	assert.Equal(t, "pool/parent/clone-src", tombstone.Dataset)

	// The ledger entry is keyed by the tombstone's FULL ZFS ID — derived from
	// the resolved snapshot, never from the CSI handle string — so existing
	// (pre-qualified-handle) ledger entries and new ones share one derivation.
	parent, err := client.DatasetGet(ctx, "pool/parent")
	require.NoError(t, err)
	ledger := tombstoneLedgerFromDataset(parent)
	entry, recorded := ledger[tombstoneLedgerKey(tombstone.ID)]
	require.True(t, recorded, "the ledger key must derive from the tombstone ID regardless of handle format")
	assert.Equal(t, tombstone.ID, entry.Snapshot)

	// Retry after the tombstone rename is idempotent in both handle formats.
	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: handle})
	require.NoError(t, err)
	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "cloned-snap"})
	require.NoError(t, err)

	// The tombstone is never exposed as a CSI snapshot.
	listed, err := d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{})
	require.NoError(t, err)
	assert.Empty(t, listed.GetEntries())
}

// Orphan-reconcile liveness must match ACROSS handle formats: a live
// VolumeSnapshotContent protects its backend snapshot whichever era stamped
// which side, and a genuinely orphaned stamped snapshot is reported under its
// qualified handle (the string its content WOULD have stored).
func TestReconcileSnapshotLivenessAcrossHandleFormats(t *testing.T) {
	old := time.Now().Add(-48 * time.Hour)
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("live-volume", "csi.scale.io")},
		[]runtime.Object{
			// Post-upgrade content: qualified handle for a stamped snapshot.
			reconcileSnapshotContent("stamped-content", "storage", "stamped-object",
				"pool/parent/live-volume@stamped-snap", "csi.scale.io"),
			// Qualified content handle whose backend snapshot lost/never had a
			// stamp: the short-name liveness net must still protect it.
			reconcileSnapshotContent("unstamped-content", "storage", "unstamped-object",
				"pool/parent/live-volume@unstamped-snap", "csi.scale.io"),
			// Pre-upgrade content: legacy short handle for a stamped snapshot
			// (e.g. re-stamped by a same-name re-create after upgrade).
			reconcileSnapshotContent("legacy-content", "storage", "legacy-object",
				"legacy-live-snap", "csi.scale.io"),
		},
	)
	addReconcileDataset(client, "live-volume", old, true, 100)
	seedStampedSnapshot := func(name string, stamped bool) {
		properties := map[string]string{
			PropManagedResource:           "true",
			PropCSISnapshotName:           name,
			PropCSISnapshotSourceVolumeID: "live-volume",
		}
		if stamped {
			properties[PropCSISnapshotHandle] = "pool/parent/live-volume@" + name
		}
		snapshot, err := client.SnapshotCreate(context.Background(), "pool/parent/live-volume", name, properties)
		require.NoError(t, err)
		snapshot.Properties["creation"] = map[string]interface{}{"parsed": float64(old.Unix())}
	}
	seedStampedSnapshot("stamped-snap", true)
	seedStampedSnapshot("unstamped-snap", false)
	seedStampedSnapshot("legacy-live-snap", true)
	seedStampedSnapshot("orphaned-snap", true)

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	require.Len(t, report.OrphanSnapshots, 1,
		"every content-protected snapshot stays live regardless of handle-format pairing")
	assert.Equal(t, "pool/parent/live-volume@orphaned-snap", report.OrphanSnapshots[0].ID,
		"a stamped orphan is reported under the qualified handle its content would store")
}
