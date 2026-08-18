package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// The P-2/P-3 ListVolumes walk: one path-scoped listing per walk frozen into a
// short-TTL cache (stable pages under churn), hydrated per page through an
// id-filtered DatasetGetByNames read (encryption fields + user-property
// sources), with LIST_VOLUMES_PUBLISHED_NODES (F-1) fed from the hydrated
// publication records.

const listWalkParent = "pool/parent"

func newListWalkDriver(client truenas.ClientInterface) *Driver {
	return &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: listWalkParent,
			},
		},
		truenasClient: client,
	}
}

// seedListWalkVolume stores a managed, successfully provisioned volume dataset
// in the mock.
func seedListWalkVolume(mock *truenas.MockClient, name string) *truenas.Dataset {
	full := listWalkParent + "/" + name
	ds := &truenas.Dataset{
		ID:   full,
		Name: full,
		Type: "FILESYSTEM",
		UserProperties: map[string]truenas.UserProperty{
			PropManagedResource:  {Value: "true", Source: "local"},
			PropProvisionSuccess: {Value: "true", Source: "local"},
		},
	}
	mock.Datasets[full] = ds
	return ds
}

func listWalkPageIDs(t *testing.T, resp *csi.ListVolumesResponse) []string {
	t.Helper()
	ids := make([]string, 0, len(resp.Entries))
	for _, entry := range resp.Entries {
		ids = append(ids, entry.GetVolume().GetVolumeId())
	}
	return ids
}

// TestListVolumesWalkFrozenViewUnderChurn proves P-3: once a walk has started
// (empty starting token), create/delete churn cannot make its later pages skip
// or duplicate volumes — they are sliced from the frozen view captured at walk
// start. A NEW walk then observes the churned state.
func TestListVolumesWalkFrozenViewUnderChurn(t *testing.T) {
	mock := truenas.NewMockClient()
	for i := 0; i < 5; i++ {
		seedListWalkVolume(mock, fmt.Sprintf("vol-%d", i))
	}
	d := newListWalkDriver(mock)

	// Page 1 freezes the view.
	resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2})
	require.NoError(t, err)
	assert.Equal(t, []string{"vol-0", "vol-1"}, listWalkPageIDs(t, resp))
	require.Equal(t, "2", resp.NextToken)

	// Churn between pages: delete an already-listed volume (a live offset walk
	// would now SKIP one) and create one that sorts into the middle of the set
	// (a live offset walk would now DUPLICATE one).
	delete(mock.Datasets, listWalkParent+"/vol-0")
	seedListWalkVolume(mock, "vol-15") // sorts between vol-1 and vol-2

	resp, err = d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: "2"})
	require.NoError(t, err)
	assert.Equal(t, []string{"vol-2", "vol-3"}, listWalkPageIDs(t, resp),
		"continuation pages must come from the frozen view, unaffected by churn")
	require.Equal(t, "4", resp.NextToken)

	resp, err = d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: "4"})
	require.NoError(t, err)
	assert.Equal(t, []string{"vol-4"}, listWalkPageIDs(t, resp))
	assert.Empty(t, resp.NextToken)

	// A NEW walk (empty starting token) always refetches and sees the churn.
	resp, err = d.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	assert.Equal(t, []string{"vol-1", "vol-15", "vol-2", "vol-3", "vol-4"}, listWalkPageIDs(t, resp))
}

// TestListVolumesContinuationRefetchesAfterTTL: a continuation token arriving
// after the cache TTL (aborted/stalled walk, or a controller restart that lost
// the cache) is served against a REFRESHED listing rather than failing —
// offset tokens remain valid against the new set, exactly as they were against
// the old per-page reads.
func TestListVolumesContinuationRefetchesAfterTTL(t *testing.T) {
	mock := truenas.NewMockClient()
	client := &PaginatedMockClient{MockClient: mock}
	for i := 0; i < 5; i++ {
		seedListWalkVolume(mock, fmt.Sprintf("vol-%d", i))
	}
	d := newListWalkDriver(client)

	resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2})
	require.NoError(t, err)
	require.Equal(t, "2", resp.NextToken)
	require.Len(t, client.datasetQueryByParentParents, 1)

	// Within the TTL a continuation is served from the cache: no new listing.
	resp, err = d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: "2"})
	require.NoError(t, err)
	assert.Equal(t, []string{"vol-2", "vol-3"}, listWalkPageIDs(t, resp))
	assert.Len(t, client.datasetQueryByParentParents, 1,
		"a continuation inside the TTL must not refetch the listing")

	// Expire the cache and churn the backend. The next continuation refetches
	// and slices the refreshed, re-sorted view at the same offset.
	d.volumePageCacheMu.Lock()
	d.volumePageCacheTime = time.Now().Add(-volumeListPageCacheTTL - time.Second)
	d.volumePageCacheMu.Unlock()
	delete(mock.Datasets, listWalkParent+"/vol-0")

	resp, err = d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: "2"})
	require.NoError(t, err)
	assert.Len(t, client.datasetQueryByParentParents, 2,
		"a continuation past the TTL must refetch the listing")
	assert.Equal(t, []string{"vol-3", "vol-4"}, listWalkPageIDs(t, resp),
		"the expired-token page is sliced from the refreshed view")
	assert.Empty(t, resp.NextToken)

	// A cold cache (e.g. controller restart) behaves the same: the continuation
	// repopulates instead of erroring.
	restarted := newListWalkDriver(client)
	resp, err = restarted.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: "2"})
	require.NoError(t, err)
	assert.Equal(t, []string{"vol-3", "vol-4"}, listWalkPageIDs(t, resp))
}

// TestListVolumesHydrationMissSkipsDeletedEntry: a dataset deleted between the
// walk's frozen listing and the page's hydration read simply drops out of the
// page — no error, and the page math (token advance) is unaffected.
func TestListVolumesHydrationMissSkipsDeletedEntry(t *testing.T) {
	mock := truenas.NewMockClient()
	for i := 0; i < 5; i++ {
		seedListWalkVolume(mock, fmt.Sprintf("vol-%d", i))
	}
	d := newListWalkDriver(mock)

	resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2})
	require.NoError(t, err)
	require.Equal(t, "2", resp.NextToken)

	// vol-2 is in the frozen view but gone by the time page 2 hydrates it.
	delete(mock.Datasets, listWalkParent+"/vol-2")

	resp, err = d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: "2"})
	require.NoError(t, err, "a mid-walk deletion must not fail the page")
	assert.Equal(t, []string{"vol-3"}, listWalkPageIDs(t, resp),
		"the deleted volume is skipped, the rest of the page survives")
	assert.Equal(t, "4", resp.NextToken,
		"token advance counts frozen-view rows consumed, not surviving entries")

	resp, err = d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: "4"})
	require.NoError(t, err)
	assert.Equal(t, []string{"vol-4"}, listWalkPageIDs(t, resp))
	assert.Empty(t, resp.NextToken)
}

// TestListVolumesLockedEncryptedConditionSurvivesHydration proves the P-11
// constraint end to end: the path-scoped listing (zfs.resource.query) carries
// NO encryption fields — MockClient.DatasetQueryByParent deliberately zeroes
// them with live fidelity — so ONLY the per-page pool.dataset.query hydration
// can feed Encrypted/Locked into the entry's VolumeCondition. A locked
// encrypted volume must still list as Abnormal.
func TestListVolumesLockedEncryptedConditionSurvivesHydration(t *testing.T) {
	mock := truenas.NewMockClient()
	ds := seedListWalkVolume(mock, "locked-vol")
	ds.Encrypted = true
	ds.Locked = true
	ds.KeyFormat = "HEX"
	d := newListWalkDriver(mock)

	// Confirm the premise: the listing path itself delivers no lock signal.
	listed, err := mock.DatasetQueryByParent(context.Background(), listWalkParent)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.False(t, listed[0].Encrypted, "premise: zfs.resource.query carries no encryption fields (P-11)")
	require.False(t, listed[0].Locked, "premise: zfs.resource.query carries no lock state (P-11)")

	resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Entries, 1)
	condition := resp.Entries[0].GetStatus().GetVolumeCondition()
	require.NotNil(t, condition)
	assert.True(t, condition.GetAbnormal(),
		"a locked encrypted volume must be Abnormal — this only works if the hydrated read feeds the condition")
	assert.Contains(t, condition.GetMessage(), "locked")
}

// seedListWalkPublicationRecord stores a publication record user property the
// way storePublicationRecord persists it (local source, hashed node key).
func seedListWalkPublicationRecord(t *testing.T, ds *truenas.Dataset, record publicationRecord, source string) {
	t.Helper()
	encoded, err := json.Marshal(record)
	require.NoError(t, err)
	ds.UserProperties[publicationPropertyKey(record.Node)] = truenas.UserProperty{
		Value:  string(encoded),
		Source: source,
	}
}

// TestListVolumesPublishedNodeIds covers F-1: PublishedNodeIds comes from the
// hydrated page's publication records at zero extra API cost. Published AND
// unpublishing records are included (an unpublishing record means backend
// access removal is not confirmed complete, so health monitoring must still
// see the node); pre-provenance records without an EncodedID are conservatively
// omitted; clone-inherited records (non-local source) never count; and the ids
// are sorted for determinism.
func TestListVolumesPublishedNodeIds(t *testing.T) {
	mock := truenas.NewMockClient()
	ds := seedListWalkVolume(mock, "published-vol")

	seedListWalkPublicationRecord(t, ds, publicationRecord{
		Version:   publicationRecordVersion,
		Node:      "node-b",
		EncodedID: "encoded-node-b",
		State:     publicationStateRemoving,
	}, "local")
	seedListWalkPublicationRecord(t, ds, publicationRecord{
		Version:   publicationRecordVersion,
		Node:      "node-a",
		EncodedID: "encoded-node-a",
		State:     publicationStatePublished,
	}, "local")
	// Pre-provenance record: no encoded node id was ever stamped. Inventing one
	// would report a node id the CO never issued, so it is omitted.
	seedListWalkPublicationRecord(t, ds, publicationRecord{
		Version: publicationRecordVersion,
		Node:    "node-legacy",
		State:   publicationStatePublished,
	}, "local")
	// Clone-inherited record: the source volume's publication, not this one's.
	seedListWalkPublicationRecord(t, ds, publicationRecord{
		Version:   publicationRecordVersion,
		Node:      "node-clone-origin",
		EncodedID: "encoded-node-clone-origin",
		State:     publicationStatePublished,
	}, listWalkParent+"/source-vol@origin-snap")

	d := newListWalkDriver(mock)
	resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Entries, 1)
	assert.Equal(t, []string{"encoded-node-a", "encoded-node-b"},
		resp.Entries[0].GetStatus().GetPublishedNodeIds(),
		"published + unpublishing records included, sorted; empty-EncodedID and inherited records omitted")
}

// TestListVolumesPublishedNodeIdsUnreadableRecordDoesNotFailListing: a corrupt
// publication record makes publish/unpublish authorization fail loudly, but a
// read-only listing must not go dark over it — the entry is returned without
// published-node data.
func TestListVolumesPublishedNodeIdsUnreadableRecordDoesNotFailListing(t *testing.T) {
	mock := truenas.NewMockClient()
	ds := seedListWalkVolume(mock, "corrupt-record-vol")
	ds.UserProperties[publicationPropertyKey("node-x")] = truenas.UserProperty{
		Value:  "{not-json",
		Source: "local",
	}

	d := newListWalkDriver(mock)
	resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Entries, 1)
	assert.Empty(t, resp.Entries[0].GetStatus().GetPublishedNodeIds())
	condition := resp.Entries[0].GetStatus().GetVolumeCondition()
	require.NotNil(t, condition, "the rest of the entry is intact")
}

// TestControllerGetCapabilitiesAdvertisesListVolumesPublishedNodes: the F-1
// capability must be advertised next to LIST_VOLUMES, or the CO never reads
// the populated PublishedNodeIds.
func TestControllerGetCapabilitiesAdvertisesListVolumesPublishedNodes(t *testing.T) {
	d := newListWalkDriver(truenas.NewMockClient())
	resp, err := d.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	require.NoError(t, err)

	advertised := make(map[csi.ControllerServiceCapability_RPC_Type]bool)
	for _, capability := range resp.Capabilities {
		advertised[capability.GetRpc().GetType()] = true
	}
	assert.True(t, advertised[csi.ControllerServiceCapability_RPC_LIST_VOLUMES])
	assert.True(t, advertised[csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES])
}

// TestListVolumesWalkAPICallBudget pins the P-2 API shape: a full walk costs
// exactly ONE path-scoped listing call (fresh page only) plus ONE id-filtered
// DatasetGetByNames hydration per page — and ZERO pool.dataset.query
// DatasetList calls, whose cost scaled with total system dataset count.
func TestListVolumesWalkAPICallBudget(t *testing.T) {
	client := newAPICallCountingClient()
	for i := 0; i < 5; i++ {
		seedListWalkVolume(client.MockClient, fmt.Sprintf("vol-%d", i))
	}
	d := newListWalkDriver(client)

	token := ""
	pages := 0
	for {
		resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: token})
		require.NoError(t, err)
		pages++
		if resp.NextToken == "" {
			break
		}
		token = resp.NextToken
	}
	require.Equal(t, 3, pages)

	total, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["DatasetQueryByParent"], "one listing per walk")
	assert.Equal(t, pages, methods["DatasetGetByNames"], "one page hydration per page")
	assert.Zero(t, methods["DatasetList"], "the full-system-materializing filtered query must not run")
	assert.Equal(t, 1+pages, total, "no other backend call may hide in the walk")
}

// failingResourceQueryClient forces listAllManagedDatasets onto its paged
// pool.dataset.query fallback leg.
type failingResourceQueryClient struct {
	*truenas.MockClient
	datasetListCalls []int
}

func (c *failingResourceQueryClient) DatasetQueryByParent(ctx context.Context, parentDataset string) ([]*truenas.Dataset, error) {
	return nil, fmt.Errorf("zfs.resource.query unavailable")
}

func (c *failingResourceQueryClient) DatasetList(ctx context.Context, parentName string, limit, offset int) ([]*truenas.Dataset, error) {
	c.datasetListCalls = append(c.datasetListCalls, limit)
	return c.MockClient.DatasetList(ctx, parentName, limit, offset)
}

// TestListVolumesWalkFallbackPagedListing: when the path-scoped resource
// listing is unavailable, the walk transparently uses listAllManagedDatasets'
// paged pool.dataset.query fallback — still once per walk, with continuation
// pages served from the frozen view.
func TestListVolumesWalkFallbackPagedListing(t *testing.T) {
	client := &failingResourceQueryClient{MockClient: truenas.NewMockClient()}
	for i := 0; i < 5; i++ {
		seedListWalkVolume(client.MockClient, fmt.Sprintf("vol-%d", i))
	}
	d := newListWalkDriver(client)

	var walkedIDs []string
	token := ""
	for {
		resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: token})
		require.NoError(t, err)
		walkedIDs = append(walkedIDs, listWalkPageIDs(t, resp)...)
		if resp.NextToken == "" {
			break
		}
		token = resp.NextToken
	}
	assert.Equal(t, []string{"vol-0", "vol-1", "vol-2", "vol-3", "vol-4"}, walkedIDs)
	assert.Equal(t, []int{reconcileListPageSize}, client.datasetListCalls,
		"the fallback fetches the full managed set once per walk (5 < one fallback page)")
}
