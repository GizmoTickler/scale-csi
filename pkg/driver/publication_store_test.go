package driver

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// newFakeVolumePublicationClient is a dynamic client whose VolumePublications
// behave as the API server's do under optimistic concurrency: every create
// and update stamps a new resourceVersion, a create of an existing name is
// AlreadyExists, and an update must carry the object's current
// resourceVersion or it is a Conflict (NotFound if the object is gone).
func newFakeVolumePublicationClient() *dynamicfake.FakeDynamicClient {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{volumePublicationGVR: volumePublicationListKind})
	var (
		mu      sync.Mutex
		version int
	)
	next := func() string {
		version++
		return strconv.Itoa(version)
	}
	client.PrependReactor("create", "volumepublications", func(action clienttesting.Action) (bool, runtime.Object, error) {
		object, ok := action.(clienttesting.CreateAction).GetObject().(*unstructured.Unstructured)
		if !ok {
			return false, nil, nil
		}
		mu.Lock()
		defer mu.Unlock()
		if object.GetResourceVersion() != "" {
			return true, nil, apierrors.NewBadRequest("resourceVersion should not be set on objects to be created")
		}
		object.SetResourceVersion(next())
		return false, nil, nil
	})
	client.PrependReactor("update", "volumepublications", func(action clienttesting.Action) (bool, runtime.Object, error) {
		update := action.(clienttesting.UpdateAction)
		object, ok := update.GetObject().(*unstructured.Unstructured)
		if !ok {
			return false, nil, nil
		}
		mu.Lock()
		defer mu.Unlock()
		current, err := client.Tracker().Get(volumePublicationGVR, update.GetNamespace(), object.GetName())
		if err != nil {
			return true, nil, err
		}
		currentMeta, err := meta.Accessor(current)
		if err != nil {
			return true, nil, err
		}
		if object.GetResourceVersion() == "" || object.GetResourceVersion() != currentMeta.GetResourceVersion() {
			return true, nil, apierrors.NewConflict(volumePublicationGVR.GroupResource(), object.GetName(),
				errors.New("the object has been modified; please apply your changes to the latest version and try again"))
		}
		object.SetResourceVersion(next())
		return false, nil, nil
	})
	client.PrependReactor("delete", "volumepublications", func(action clienttesting.Action) (bool, runtime.Object, error) {
		del := action.(clienttesting.DeleteAction)
		precondition := del.GetDeleteOptions().Preconditions
		if precondition == nil || precondition.ResourceVersion == nil {
			return false, nil, nil
		}
		mu.Lock()
		defer mu.Unlock()
		current, err := client.Tracker().Get(volumePublicationGVR, del.GetNamespace(), del.GetName())
		if err != nil {
			return true, nil, err
		}
		currentMeta, err := meta.Accessor(current)
		if err != nil {
			return true, nil, err
		}
		if currentMeta.GetResourceVersion() != *precondition.ResourceVersion {
			return true, nil, apierrors.NewConflict(volumePublicationGVR.GroupResource(), del.GetName(),
				errors.New("the ResourceVersion in the precondition does not match the ResourceVersion in record"))
		}
		return false, nil, nil
	})
	return client
}

func testRecord(node, state string) publicationRecord {
	return publicationRecord{
		Version:    publicationRecordVersion,
		Node:       node,
		EncodedID:  "sc1.id-" + node,
		NVMeNQN:    "nqn.2014-08.org.nvmexpress:uuid:" + node,
		IPs:        []string{"192.0.2.10"},
		State:      state,
		AccessMode: 1,
		UpdatedAt:  "2026-10-01T00:00:00Z",
	}
}

// Both stores honor the same contract: what is stored is read back, a
// re-store replaces, removal is idempotent, and datasets are separate.
func TestPublicationStoresShareOneContract(t *testing.T) {
	stores := map[string]func() (publicationStore, *truenas.Dataset, *truenas.Dataset){
		"zfs": func() (publicationStore, *truenas.Dataset, *truenas.Dataset) {
			client := truenas.NewMockClient()
			a := addReconcileDataset(client, "vol-a", time.Now(), true, 0)
			b := addReconcileDataset(client, "vol-b", time.Now(), true, 0)
			return zfsPublicationStore{client: client}, a, b
		},
		"kubernetes": func() (publicationStore, *truenas.Dataset, *truenas.Dataset) {
			return newKubernetesPublicationStore(newFakeVolumePublicationClient(), "scale-csi", "one"),
				&truenas.Dataset{Name: "pool/parent/vol-a", UserProperties: map[string]truenas.UserProperty{}},
				&truenas.Dataset{Name: "pool/parent/vol-b", UserProperties: map[string]truenas.UserProperty{}}
		},
	}
	for name, build := range stores {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, a, b := build()
			key1, key2 := publicationPropertyKey("node-1"), publicationPropertyKey("node-2")

			require.NoError(t, store.store(ctx, a.Name, a, key1, testRecord("node-1", publicationStatePublished)))
			require.NoError(t, store.store(ctx, a.Name, a, key2, testRecord("node-2", publicationStatePublished)))
			got, err := store.records(ctx, a.Name, a)
			require.NoError(t, err)
			assert.Equal(t, map[string]publicationRecord{
				key1: testRecord("node-1", publicationStatePublished),
				key2: testRecord("node-2", publicationStatePublished),
			}, got)

			tombstone := testRecord("node-1", publicationStateRemoving)
			tombstone.CSIAddedNVMeNQNs = []string{"nqn.x"}
			require.NoError(t, store.store(ctx, a.Name, a, key1, tombstone))
			got, err = store.records(ctx, a.Name, a)
			require.NoError(t, err)
			assert.Equal(t, tombstone, got[key1], "a re-store replaces")

			other, err := store.records(ctx, b.Name, b)
			require.NoError(t, err)
			assert.Empty(t, other, "another dataset sees none of them")

			require.NoError(t, store.remove(ctx, a.Name, a, []string{key1}))
			require.NoError(t, store.remove(ctx, a.Name, a, []string{key1}), "removing what is gone is fine")
			got, err = store.records(ctx, a.Name, a)
			require.NoError(t, err)
			assert.Equal(t, map[string]publicationRecord{key2: testRecord("node-2", publicationStatePublished)}, got)
		})
	}
}

func TestKubernetesPublicationStoreIsolationAndValidation(t *testing.T) {
	ctx := context.Background()
	client := newFakeVolumePublicationClient()
	one := newKubernetesPublicationStore(client, "scale-csi", "one")
	two := newKubernetesPublicationStore(client, "scale-csi", "two")
	key := publicationPropertyKey("node-1")
	require.NoError(t, one.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStatePublished)))
	got, err := two.records(ctx, "pool/v", nil)
	require.NoError(t, err)
	assert.Empty(t, got, "another driver instance in the namespace sees nothing")

	// The object is readable and labeled for kubectl.
	object, err := client.Resource(volumePublicationGVR).Namespace("scale-csi").Get(ctx, one.objectName("pool/v", key), metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "node-1", object.GetLabels()[labelVolumePublicationNode])
	assert.Equal(t, "VolumePublication", object.GetKind())

	// An object whose labels collide but names another dataset is not this one's.
	collider, err := one.object("pool/other", key, testRecord("node-1", publicationStatePublished))
	require.NoError(t, err)
	labels := collider.GetLabels()
	labels[labelVolumePublicationDS] = shortHash("pool/v")
	collider.SetLabels(labels)
	collider.SetName("vp-collider")
	_, err = client.Resource(volumePublicationGVR).Namespace("scale-csi").Create(ctx, collider, metav1.CreateOptions{})
	require.NoError(t, err)
	got, err = one.records(ctx, "pool/v", nil)
	require.NoError(t, err)
	assert.Len(t, got, 1)

	// A record with an unknown state fails the read, as a corrupt ZFS property does.
	bad := testRecord("node-2", "weird")
	require.NoError(t, one.store(ctx, "pool/w", nil, publicationPropertyKey("node-2"), bad))
	_, err = one.records(ctx, "pool/w", nil)
	assert.Error(t, err)
}

// A write is a compare-and-set against the locked read it was decided on:
// a record another process changed since is reported, and left as that
// process wrote it.
func TestKubernetesPublicationStoreReportsAConflict(t *testing.T) {
	ctx := context.Background()
	key := publicationPropertyKey("node-1")
	other := func(store kubernetesPublicationStore, change func(*unstructured.Unstructured) error) {
		t.Helper()
		object, err := store.object("pool/v", key, testRecord("node-1", publicationStatePublished))
		require.NoError(t, err)
		require.NoError(t, change(object))
	}
	for name, tc := range map[string]struct {
		before bool
		change func(store kubernetesPublicationStore)
	}{
		"changed": {before: true, change: func(store kubernetesPublicationStore) {
			other(store, func(object *unstructured.Unstructured) error {
				current, err := store.resource().Get(ctx, object.GetName(), metav1.GetOptions{})
				if err != nil {
					return err
				}
				_ = unstructured.SetNestedField(current.Object, "2026-10-02T00:00:00Z", "spec", "updatedAt")
				_, err = store.resource().Update(ctx, current, metav1.UpdateOptions{})
				return err
			})
		}},
		"created": {before: false, change: func(store kubernetesPublicationStore) {
			other(store, func(object *unstructured.Unstructured) error {
				_, err := store.resource().Create(ctx, object, metav1.CreateOptions{})
				return err
			})
		}},
		"removed": {before: true, change: func(store kubernetesPublicationStore) {
			other(store, func(object *unstructured.Unstructured) error {
				return store.resource().Delete(ctx, object.GetName(), metav1.DeleteOptions{})
			})
		}},
	} {
		t.Run(name, func(t *testing.T) {
			client := newFakeVolumePublicationClient()
			store := newKubernetesPublicationStore(client, "scale-csi", "one")
			// Another process (this store's twin, with its own versions) wrote
			// the record before the read when tc.before.
			twin := newKubernetesPublicationStore(client, "scale-csi", "one")
			if tc.before {
				require.NoError(t, twin.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStatePublished)))
			}
			_, err := store.lockedRecords(ctx, "pool/v", nil) // the locked read
			require.NoError(t, err)
			tc.change(twin)
			before, _ := twin.resource().Get(ctx, store.objectName("pool/v", key), metav1.GetOptions{})

			err = store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStateRemoving))
			require.ErrorIs(t, err, errPublicationRecordConflict)
			after, _ := twin.resource().Get(ctx, store.objectName("pool/v", key), metav1.GetOptions{})
			assert.Equal(t, before, after, "the other process's write was overwritten")

			// The next decision reads again, and its write goes through.
			_, err = store.lockedRecords(ctx, "pool/v", nil)
			require.NoError(t, err)
			require.NoError(t, store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStateRemoving)))
			got, err := store.records(ctx, "pool/v", nil)
			require.NoError(t, err)
			assert.Equal(t, publicationStateRemoving, got[key].State)
		})
	}
}

// A write after the locked read is one request: the read carried the
// object's resourceVersion (or its absence).
func TestKubernetesPublicationStoreWriteIsOneRequest(t *testing.T) {
	ctx := context.Background()
	client := newFakeVolumePublicationClient()
	store := newKubernetesPublicationStore(client, "scale-csi", "one")
	key := publicationPropertyKey("node-1")
	writes := func(record publicationRecord) []string {
		t.Helper()
		_, err := store.lockedRecords(ctx, "pool/v", nil)
		require.NoError(t, err)
		client.ClearActions()
		require.NoError(t, store.store(ctx, "pool/v", nil, key, record))
		verbs := make([]string, 0, len(client.Actions()))
		for _, action := range client.Actions() {
			verbs = append(verbs, action.GetVerb())
		}
		return verbs
	}
	assert.Equal(t, []string{"create"}, writes(testRecord("node-1", publicationStatePublished)), "first write")
	assert.Equal(t, []string{"update"}, writes(testRecord("node-1", publicationStateRemoving)), "a rewrite")
	// Back to back without a read between: the version the last write
	// returned is carried.
	client.ClearActions()
	require.NoError(t, store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStatePublished)))
	assert.Len(t, client.Actions(), 1)
	// After a removal the record is known absent: the next write creates.
	require.NoError(t, store.remove(ctx, "pool/v", nil, []string{key}))
	client.ClearActions()
	require.NoError(t, store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStatePublished)))
	require.Len(t, client.Actions(), 1)
	assert.Equal(t, "create", client.Actions()[0].GetVerb())
}

// foreignUpdate rewrites the record as another process would, bumping its
// resourceVersion.
func foreignUpdate(t *testing.T, store kubernetesPublicationStore, datasetName, key, updatedAt string) {
	t.Helper()
	ctx := context.Background()
	current, err := store.resource().Get(ctx, store.objectName(datasetName, key), metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(current.Object, updatedAt, "spec", "updatedAt"))
	_, err = store.resource().Update(ctx, current, metav1.UpdateOptions{})
	require.NoError(t, err)
}

// A read made without the volume lock (a ListVolumes page, the startup diff)
// between the locked read and the write does not make a foreign write the
// write's starting point: the write still conflicts instead of overwriting it.
func TestKubernetesPublicationStoreUnlockedReadDoesNotDefeatTheCompareAndSet(t *testing.T) {
	ctx := context.Background()
	client := newFakeVolumePublicationClient()
	store := newKubernetesPublicationStore(client, "scale-csi", "one")
	key := publicationPropertyKey("node-1")
	require.NoError(t, store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStatePublished)))

	_, err := store.lockedRecords(ctx, "pool/v", nil) // the volume lock is held from here
	require.NoError(t, err)
	foreignUpdate(t, newKubernetesPublicationStore(client, "scale-csi", "one"), "pool/v", key, "2026-10-02T09:00:00Z")
	_, err = store.records(ctx, "pool/v", nil) // a reporting read, without the lock
	require.NoError(t, err)

	err = store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStateRemoving))
	require.ErrorIs(t, err, errPublicationRecordConflict, "the foreign write was overwritten")
	got, err := store.records(ctx, "pool/v", nil)
	require.NoError(t, err)
	assert.Equal(t, "2026-10-02T09:00:00Z", got[key].UpdatedAt)
}

// A reporting read whose answer predates this process's own last write does
// not wind the write's starting point back: the next locked write succeeds.
func TestKubernetesPublicationStoreStaleUnlockedReadCausesNoConflict(t *testing.T) {
	ctx := context.Background()
	client := newFakeVolumePublicationClient()
	store := newKubernetesPublicationStore(client, "scale-csi", "one")
	key := publicationPropertyKey("node-1")
	require.NoError(t, store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStatePublished)))
	stale, err := store.resource().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)

	_, err = store.lockedRecords(ctx, "pool/v", nil)
	require.NoError(t, err)
	require.NoError(t, store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStateRemoving)))

	// A reporting read that was answered before that write lands after it.
	var once sync.Once
	client.PrependReactor("list", "volumepublications", func(clienttesting.Action) (bool, runtime.Object, error) {
		served := false
		once.Do(func() { served = true })
		if !served {
			return false, nil, nil
		}
		return true, stale.DeepCopy(), nil
	})
	_, err = store.records(ctx, "pool/v", nil)
	require.NoError(t, err)

	require.NoError(t, store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStatePublished)),
		"a stale reporting read caused a conflict")
}

// A removal is a compare-and-set too: a record changed since the locked read
// is kept and the removal reported.
func TestKubernetesPublicationStoreRemoveIsACompareAndSet(t *testing.T) {
	ctx := context.Background()
	client := newFakeVolumePublicationClient()
	store := newKubernetesPublicationStore(client, "scale-csi", "one")
	key := publicationPropertyKey("node-1")
	require.NoError(t, store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStatePublished)))
	_, err := store.lockedRecords(ctx, "pool/v", nil)
	require.NoError(t, err)
	foreignUpdate(t, newKubernetesPublicationStore(client, "scale-csi", "one"), "pool/v", key, "2026-10-02T09:00:00Z")

	err = store.remove(ctx, "pool/v", nil, []string{key})
	require.ErrorIs(t, err, errPublicationRecordConflict)
	got, err := store.records(ctx, "pool/v", nil)
	require.NoError(t, err)
	assert.Contains(t, got, key, "a record changed since the read was removed")

	_, err = store.lockedRecords(ctx, "pool/v", nil)
	require.NoError(t, err)
	require.NoError(t, store.remove(ctx, "pool/v", nil, []string{key}))
	got, err = store.records(ctx, "pool/v", nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}
