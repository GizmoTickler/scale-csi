package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// An importing store over a mock dataset that holds ZFS records an older
// release wrote.
func newImportingStore(t *testing.T, legacy ...publicationRecord) (importingPublicationStore, *apiCallCountingClient, *truenas.Dataset) {
	t.Helper()
	client := newAPICallCountingClient()
	ds := addReconcileDataset(client.MockClient, "vol-a", time.Now(), true, 0)
	zfs := zfsPublicationStore{client: client}
	for _, record := range legacy {
		require.NoError(t, zfs.store(context.Background(), ds.Name, ds, publicationPropertyKey(record.Node), record))
	}
	client.resetCalls()
	kube := kubernetesPublicationStore{client: newFakeVolumePublicationClient(), namespace: "scale-csi", instance: "one"}
	return importingPublicationStore{kube: kube, legacy: zfs}, client, ds
}

func zfsRecordKeys(t *testing.T, client *apiCallCountingClient, name string) []string {
	t.Helper()
	ds, err := client.MockClient.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(ds)
	require.NoError(t, err)
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	return keys
}

// Records an older release left on ZFS are read until a write imports them,
// and a write imports them all and empties the dataset of them in one update.
func TestImportingStoreReadsZFSRecordsAndImportsThemOnAWrite(t *testing.T) {
	ctx := context.Background()
	key1, key2, key3 := publicationPropertyKey("node-1"), publicationPropertyKey("node-2"), publicationPropertyKey("node-3")
	store, client, ds := newImportingStore(t, testRecord("node-1", publicationStatePublished), testRecord("node-2", publicationStatePublished))

	got, err := store.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Len(t, got, 2, "the ZFS records are read before any import")

	require.NoError(t, store.store(ctx, ds.Name, ds, key3, testRecord("node-3", publicationStatePublished)))
	total, methods := client.callSnapshot()
	assert.Equal(t, 1, total, "one dataset update removes them: %v", methods)
	assert.Equal(t, 1, methods["DatasetRemoveUserProperties"])
	assert.Empty(t, zfsRecordKeys(t, client, ds.Name))
	kube, err := store.kube.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Equal(t, map[string]publicationRecord{
		key1: testRecord("node-1", publicationStatePublished),
		key2: testRecord("node-2", publicationStatePublished),
		key3: testRecord("node-3", publicationStatePublished),
	}, kube)

	client.resetCalls()
	require.NoError(t, store.store(ctx, ds.Name, ds, key3, testRecord("node-3", publicationStateRemoving)))
	require.NoError(t, store.remove(ctx, ds.Name, ds, []string{key3}))
	total, methods = client.callSnapshot()
	assert.Zero(t, total, "once imported, a write never touches TrueNAS: %v", methods)
}

// Kubernetes wins per key: after a crash between writing a tombstone to
// Kubernetes and removing the dataset's older copy, the tombstone is what
// every reader sees.
func TestImportingStoreKubernetesWinsPerKey(t *testing.T) {
	ctx := context.Background()
	key := publicationPropertyKey("node-1")
	store, _, ds := newImportingStore(t, testRecord("node-1", publicationStatePublished))
	require.NoError(t, store.kube.store(ctx, ds.Name, ds, key, testRecord("node-1", publicationStateRemoving)))

	got, err := store.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Equal(t, publicationStateRemoving, got[key].State)
}

// A removal takes the dataset's copy away before the Kubernetes one: if
// TrueNAS refuses, the tombstone stays the record, and the older "published"
// copy on ZFS never becomes it.
func TestImportingStoreRemovalNeverExposesAnOlderZFSCopy(t *testing.T) {
	ctx := context.Background()
	key := publicationPropertyKey("node-1")
	store, client, ds := newImportingStore(t, testRecord("node-1", publicationStatePublished))
	require.NoError(t, store.kube.store(ctx, ds.Name, ds, key, testRecord("node-1", publicationStateRemoving)))

	client.InjectError = errors.New("middleware busy")
	require.Error(t, store.remove(ctx, ds.Name, ds, []string{key}))
	client.InjectError = nil
	got, err := store.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Equal(t, publicationStateRemoving, got[key].State, "the failed removal left the tombstone in charge")

	require.NoError(t, store.remove(ctx, ds.Name, ds, []string{key}))
	got, err = store.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Empty(t, got, "the retry removed both copies")
	assert.Empty(t, zfsRecordKeys(t, client, ds.Name))
}

// A record being removed is not imported first; the others are.
func TestImportingStoreRemovalImportsTheOtherRecords(t *testing.T) {
	ctx := context.Background()
	key1, key2 := publicationPropertyKey("node-1"), publicationPropertyKey("node-2")
	store, client, ds := newImportingStore(t, testRecord("node-1", publicationStatePublished), testRecord("node-2", publicationStatePublished))

	require.NoError(t, store.remove(ctx, ds.Name, ds, []string{key1}))
	assert.Empty(t, zfsRecordKeys(t, client, ds.Name))
	got, err := store.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Equal(t, map[string]publicationRecord{key2: testRecord("node-2", publicationStatePublished)}, got)
}

// A clone's inherited records are not its own: never imported, never removed.
func TestImportingStoreIgnoresInheritedRecords(t *testing.T) {
	ctx := context.Background()
	store, client, ds := newImportingStore(t)
	key := publicationPropertyKey("node-9")
	ds.UserProperties[key] = truenas.UserProperty{Value: `{"v":1,"node":"node-9","state":"published"}`, Source: "INHERITED from pool/parent/src@snap"}

	require.NoError(t, store.store(ctx, ds.Name, ds, publicationPropertyKey("node-1"), testRecord("node-1", publicationStatePublished)))
	total, methods := client.callSnapshot()
	assert.Zero(t, total, "nothing of its own to remove: %v", methods)
	got, err := store.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.NotContains(t, got, key)
}
