package driver

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

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
	kube := newKubernetesPublicationStore(newFakeVolumePublicationClient(), "scale-csi", "one")
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

// While records are being imported, the stale-record sweep sees both kinds: a
// record still on the dataset and one already in Kubernetes are each revoked
// once their VolumeAttachment stays absent for the grace period.
func TestStaleSweepRevokesRecordsInEitherStoreDuringImport(t *testing.T) {
	ctx := context.Background()
	// Another volume is attached: an empty VolumeAttachment list would engage the brake.
	otherPV := "pv-other"
	other := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "attachment-other"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.scale.io", NodeName: "worker-a",
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &otherPV},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	d, client := newReconcileTestDriver(t, false, []runtime.Object{other}, nil)
	d.config.Fencing = FencingConfig{Mode: FencingModeAdditive, StaleRecordGracePeriod: "10m"}
	d.config.NFS.ShareAllowedNetworks = []string{"192.0.2.0/24"}
	fake := newFakeVolumePublicationClient()
	kube := newKubernetesPublicationStore(fake, "scale-csi", "one")
	d.publicationStore = importingPublicationStore{kube: kube, legacy: zfsPublicationStore{client: client}}

	dataset := addReconcileDataset(client, "importing", time.Now().Add(-time.Hour), true, 1)
	quiet := []*truenas.Dataset{
		addReconcileDataset(client, "quiet-1", time.Now().Add(-time.Hour), true, 1),
		addReconcileDataset(client, "quiet-2", time.Now().Add(-time.Hour), true, 1),
	}
	dataset.Mountpoint = "/mnt/pool/parent/importing"
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Hosts: []string{"192.0.2.11", "192.0.2.12"}, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	record := func(name, ip string) publicationRecord {
		r, recordErr := newPublicationRecord(NodeIdentity{Name: name, IPs: []net.IP{net.ParseIP(ip)}},
			csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, false)
		require.NoError(t, recordErr)
		r.CSIAddedNFSHosts = []string{ip}
		return r
	}
	onZFS, inKube := record("gone-1", "192.0.2.11"), record("gone-2", "192.0.2.12")
	require.NoError(t, storePublicationRecord(ctx, client, dataset, dataset.Name, publicationPropertyKey(onZFS.Node), onZFS))
	require.NoError(t, kube.store(ctx, dataset.Name, dataset, publicationPropertyKey(inKube.Node), inKube))
	// A record whose dataset is gone (its delete's own removal never ran).
	orphan := record("gone-3", "192.0.2.13")
	require.NoError(t, kube.store(ctx, "pool/parent/deleted", nil, publicationPropertyKey(orphan.Node), orphan))
	state := &kubernetesReconcileState{liveVolumeAttachments: map[string]struct{}{"other": {}}, volumeAttachmentCount: 1}

	listed := append([]*truenas.Dataset{dataset}, quiet...)
	observedAt := time.Now()
	fake.ClearActions()
	d.reconcileStalePublicationRecords(ctx, listed, state, observedAt)
	perDataset := 0
	for _, action := range fake.Actions() {
		list, ok := action.(clienttesting.ListAction)
		if !ok {
			continue
		}
		for _, ds := range listed {
			if strings.Contains(list.GetListRestrictions().Labels.String(), shortHash(ds.Name)) {
				perDataset++
			}
		}
	}
	assert.Zero(t, perDataset, "the pass lists the instance's VolumePublications once, not each listed dataset's")
	dataset, err = client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	got, err := d.publications().records(ctx, dataset.Name, dataset)
	require.NoError(t, err)
	assert.Len(t, got, 2, "the first absence only starts the grace window")
	gone, err := kube.records(ctx, "pool/parent/deleted", nil)
	require.NoError(t, err)
	assert.Empty(t, gone, "the records of a deleted dataset are forgotten")

	d.reconcileStalePublicationRecords(ctx, listed, state, observedAt.Add(11*time.Minute))
	dataset, err = client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	got, err = d.publications().records(ctx, dataset.Name, dataset)
	require.NoError(t, err)
	assert.Empty(t, got, "both records revoked")
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Empty(t, share.Hosts, "both CSI-added grants removed")
}

// ListVolumes reads published node ids from the watch-fed cache: once it has
// synced, a resync of every volume costs no API call, and the cache follows
// writes.
func TestPublishedNodeIDsComeFromTheCacheWithoutAnAPICall(t *testing.T) {
	ctx := context.Background()
	store, client, ds := newImportingStore(t, testRecord("node-1", publicationStatePublished))
	fake := store.kube.client.(*dynamicfake.FakeDynamicClient)
	publicationCache, err := newPublicationCache(store.kube)
	require.NoError(t, err)
	t.Cleanup(publicationCache.close)
	publicationCache.start(ctx)
	store.cache = publicationCache
	d := &Driver{publicationStore: store, truenasClient: client}

	require.NoError(t, store.kube.store(ctx, ds.Name, ds, publicationPropertyKey("node-2"), testRecord("node-2", publicationStatePublished)))
	want := []string{testRecord("node-1", "").EncodedID, testRecord("node-2", "").EncodedID}
	require.Eventually(t, func() bool { return assert.ObjectsAreEqual(want, d.publishedNodeIDs(ctx, ds)) },
		5*time.Second, 10*time.Millisecond, "a record on ZFS and one in Kubernetes")

	fake.ClearActions()
	for range 50 {
		assert.Equal(t, want, d.publishedNodeIDs(ctx, ds))
	}
	assert.Empty(t, fake.Actions(), "no API call per read")

	require.NoError(t, store.kube.remove(ctx, ds.Name, ds, []string{publicationPropertyKey("node-2")}))
	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]string{testRecord("node-1", "").EncodedID}, d.publishedNodeIDs(ctx, ds))
	}, 5*time.Second, 10*time.Millisecond, "the cache follows a removal")
}

func recordAt(node, state, at string) publicationRecord {
	record := testRecord(node, state)
	record.UpdatedAt = at
	return record
}

// After a rollback and this release again, the older release's ZFS records
// are newer than every VolumePublication of the volume: ZFS alone decides,
// so a node that release unpublished (its record gone from ZFS) does not
// come back from a VolumePublication it never saw, not even after a write.
func TestImportingStoreZFSNewerThanEveryVolumePublicationDecidesAlone(t *testing.T) {
	ctx := context.Background()
	keyA, keyB, keyC := publicationPropertyKey("node-a"), publicationPropertyKey("node-b"), publicationPropertyKey("node-c")
	before, rolledBack, after := "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", "2026-10-03T00:00:00Z"
	store, client, ds := newImportingStore(t, recordAt("node-b", publicationStatePublished, rolledBack))
	require.NoError(t, store.kube.store(ctx, ds.Name, ds, keyA, recordAt("node-a", publicationStatePublished, before)))
	require.NoError(t, store.kube.store(ctx, ds.Name, ds, keyB, recordAt("node-b", publicationStatePublished, before)))

	got, err := store.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Equal(t, map[string]publicationRecord{keyB: recordAt("node-b", publicationStatePublished, rolledBack)}, got)

	require.NoError(t, store.store(ctx, ds.Name, ds, keyC, recordAt("node-c", publicationStatePublished, after)))
	got, err = store.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Equal(t, map[string]publicationRecord{
		keyB: recordAt("node-b", publicationStatePublished, rolledBack),
		keyC: recordAt("node-c", publicationStatePublished, after),
	}, got, "node-a stays unpublished after the import")
	assert.Empty(t, zfsRecordKeys(t, client, ds.Name))
}

// Otherwise the newer copy of each key wins, and records not yet imported
// stay: a crash between a VolumePublication write and the ZFS removal.
func TestImportingStoreMergesPerKeyByAge(t *testing.T) {
	ctx := context.Background()
	keyA, keyB := publicationPropertyKey("node-a"), publicationPropertyKey("node-b")
	store, _, ds := newImportingStore(t,
		recordAt("node-a", publicationStatePublished, "2026-10-01T00:00:00Z"),
		recordAt("node-b", publicationStatePublished, "2026-10-01T00:00:00Z"))
	require.NoError(t, store.kube.store(ctx, ds.Name, ds, keyA, recordAt("node-a", publicationStateRemoving, "2026-10-02T00:00:00Z")))

	got, err := store.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Equal(t, map[string]publicationRecord{
		keyA: recordAt("node-a", publicationStateRemoving, "2026-10-02T00:00:00Z"),
		keyB: recordAt("node-b", publicationStatePublished, "2026-10-01T00:00:00Z"),
	}, got)
}

// DeleteVolume removes the volume's VolumePublications, which did not go with
// its dataset; a retry after the dataset is gone removes them too.
func TestDeleteVolumeForgetsItsVolumePublications(t *testing.T) {
	ctx := context.Background()
	d, client, store := importTestDriver(t)
	ds := addReconcileDataset(client, "pvc-gone", time.Now().Add(-time.Hour), true, 0)
	require.NoError(t, store.kube.store(ctx, ds.Name, ds, publicationPropertyKey("node-1"), testRecord("node-1", publicationStatePublished)))
	other := addReconcileDataset(client, "pvc-kept", time.Now().Add(-time.Hour), true, 0)
	require.NoError(t, store.kube.store(ctx, other.Name, other, publicationPropertyKey("node-1"), testRecord("node-1", publicationStatePublished)))

	_, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "pvc-gone"})
	require.NoError(t, err)
	got, err := store.kube.records(ctx, ds.Name, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
	kept, err := store.kube.records(ctx, other.Name, nil)
	require.NoError(t, err)
	assert.Len(t, kept, 1, "another volume's records stay")

	// The dataset already gone: a leftover record is removed as well.
	require.NoError(t, store.kube.store(ctx, ds.Name, nil, publicationPropertyKey("node-2"), testRecord("node-2", publicationStatePublished)))
	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "pvc-gone"})
	require.NoError(t, err)
	got, err = store.kube.records(ctx, ds.Name, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}
