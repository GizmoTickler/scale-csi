package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func newFakeVolumePublicationClient() *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{volumePublicationGVR: volumePublicationListKind})
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
			return kubernetesPublicationStore{client: newFakeVolumePublicationClient(), namespace: "scale-csi", instance: "one"},
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
	one := kubernetesPublicationStore{client: client, namespace: "scale-csi", instance: "one"}
	two := kubernetesPublicationStore{client: client, namespace: "scale-csi", instance: "two"}
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

// A write that loses a race with another process retries and wins: the record
// was decided under the volume lock.
func TestKubernetesPublicationStoreRetriesAConflict(t *testing.T) {
	ctx := context.Background()
	client := newFakeVolumePublicationClient()
	store := kubernetesPublicationStore{client: client, namespace: "scale-csi", instance: "one"}
	key := publicationPropertyKey("node-1")
	require.NoError(t, store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStatePublished)))
	conflicts := 1
	client.PrependReactor("update", "volumepublications", func(clienttesting.Action) (bool, runtime.Object, error) {
		if conflicts > 0 {
			conflicts--
			return true, nil, apierrors.NewConflict(volumePublicationGVR.GroupResource(), "x", nil)
		}
		return false, nil, nil
	})
	require.NoError(t, store.store(ctx, "pool/v", nil, key, testRecord("node-1", publicationStateRemoving)))
	got, err := store.records(ctx, "pool/v", nil)
	require.NoError(t, err)
	assert.Equal(t, publicationStateRemoving, got[key].State)
}
