package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func importTestDriver(t *testing.T) (*Driver, *truenas.MockClient, importingPublicationStore) {
	t.Helper()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	store := importingPublicationStore{
		kube:   kubernetesPublicationStore{client: newFakeVolumePublicationClient(), namespace: "scale-csi", instance: "one"},
		legacy: zfsPublicationStore{client: client},
	}
	d.publicationStore = store
	orig := publicationImportPace
	publicationImportPace = 0
	t.Cleanup(func() { publicationImportPace = orig })
	return d, client, store
}

func addDatasetWithZFSRecords(t *testing.T, client *truenas.MockClient, name string, nodes ...string) *truenas.Dataset {
	t.Helper()
	ds := addReconcileDataset(client, name, time.Now().Add(-time.Hour), true, 1)
	for _, node := range nodes {
		require.NoError(t, storePublicationRecord(context.Background(), client, ds, ds.Name,
			publicationPropertyKey(node), testRecord(node, publicationStatePublished)))
	}
	return ds
}

// A pass moves every volume's ZFS records to Kubernetes; a clone's inherited
// records are not its own and do not hold the import back.
func TestPublicationImportPassMovesEveryVolume(t *testing.T) {
	ctx := context.Background()
	d, client, store := importTestDriver(t)
	a := addDatasetWithZFSRecords(t, client, "vol-a", "node-1", "node-2")
	b := addDatasetWithZFSRecords(t, client, "vol-b", "node-3")
	clone := addReconcileDataset(client, "vol-clone", time.Now().Add(-time.Hour), true, 1)
	require.NoError(t, client.DatasetSetUserProperty(ctx, clone.Name, publicationPropertyKey("node-9"), `{"v":1}`))
	stored := client.Datasets[clone.Name].UserProperties[publicationPropertyKey("node-9")]
	stored.Source = "INHERITED from pool/parent/vol-a@snap"
	client.Datasets[clone.Name].UserProperties[publicationPropertyKey("node-9")] = stored

	remaining, err := d.importPublicationRecordsPass(ctx, store)
	require.NoError(t, err)
	assert.Zero(t, remaining)
	for ds, want := range map[*truenas.Dataset]int{a: 2, b: 1} {
		assert.Empty(t, zfsRecordKeysOf(t, client, ds.Name))
		got, recordsErr := store.kube.records(ctx, ds.Name, nil)
		require.NoError(t, recordsErr)
		assert.Len(t, got, want, ds.Name)
	}
}

// A volume busy with an operation is left for the next pass.
func TestPublicationImportPassLeavesABusyVolume(t *testing.T) {
	ctx := context.Background()
	d, client, store := importTestDriver(t)
	a := addDatasetWithZFSRecords(t, client, "vol-a", "node-1")
	require.True(t, d.acquireOperationLock(volumeLockKey("vol-a")))

	remaining, err := d.importPublicationRecordsPass(ctx, store)
	require.NoError(t, err)
	assert.Equal(t, 1, remaining)
	assert.Len(t, zfsRecordKeysOf(t, client, a.Name), 1, "untouched")

	d.releaseOperationLock(volumeLockKey("vol-a"))
	remaining, err = d.importPublicationRecordsPass(ctx, store)
	require.NoError(t, err)
	assert.Zero(t, remaining)
	assert.Empty(t, zfsRecordKeysOf(t, client, a.Name))
}

// staleListingClient lists datasets as they were before the test changed them.
type staleListingClient struct {
	*truenas.MockClient
	listing []*truenas.Dataset
}

func (c staleListingClient) DatasetQueryByParent(context.Context, string) ([]*truenas.Dataset, error) {
	return c.listing, nil
}

// A volume is read again under its lock: records an unpublish removed after
// the listing was taken do not come back from the listing's copy.
func TestPublicationImportPassReadsEachVolumeUnderItsLock(t *testing.T) {
	ctx := context.Background()
	d, client, store := importTestDriver(t)
	addDatasetWithZFSRecords(t, client, "vol-a", "node-1")
	// A source-bearing listing (the pool.dataset.query fallback), taken now.
	listed, err := client.DatasetGet(ctx, "pool/parent/vol-a")
	require.NoError(t, err)
	copied := *listed
	copied.UserProperties = make(map[string]truenas.UserProperty, len(listed.UserProperties))
	for key, value := range listed.UserProperties {
		copied.UserProperties[key] = value
	}
	stale := []*truenas.Dataset{&copied}
	fresh, err := client.DatasetGet(ctx, "pool/parent/vol-a")
	require.NoError(t, err)
	require.NoError(t, store.remove(ctx, fresh.Name, fresh, []string{publicationPropertyKey("node-1")}), "the unpublish")
	d.truenasClient = staleListingClient{MockClient: client, listing: stale}

	remaining, err := d.importPublicationRecordsPass(ctx, store)
	require.NoError(t, err)
	assert.Zero(t, remaining)
	got, err := store.kube.records(ctx, "pool/parent/vol-a", nil)
	require.NoError(t, err)
	assert.Empty(t, got, "the removed record stays removed")
}

func zfsRecordKeysOf(t *testing.T, client *truenas.MockClient, name string) []string {
	t.Helper()
	ds, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(ds)
	require.NoError(t, err)
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	return keys
}
