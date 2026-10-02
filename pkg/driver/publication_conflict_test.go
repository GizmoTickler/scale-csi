package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// A publish whose record write finds the record written by another process
// since its locked read fails Aborted, which the attacher retries at once,
// not Internal.
func TestPublishRecordConflictIsAborted(t *testing.T) {
	d, gated := newLockTestVolume(t, "record-conflict")
	fake := newFakeVolumePublicationClient()
	d.publicationStore = importingPublicationStore{
		kube:   newKubernetesPublicationStore(fake, "scale-csi", "conflict"),
		legacy: zfsPublicationStore{client: gated},
	}
	fake.PrependReactor("create", "volumepublications", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewAlreadyExists(volumePublicationGVR.GroupResource(), "vp")
	})
	_, err := d.ControllerPublishVolume(context.Background(), lockTestPublishRequest(t, "record-conflict", "worker-a",
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER))
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err), "%v", err)
}

// A store on a volume that still has ZFS records imports them first. That
// import must not take a foreign write made since the caller's locked read
// as its starting point: the caller's write still conflicts.
func TestImportingStoreWriteKeepsTheCallersReadAcrossTheImport(t *testing.T) {
	ctx := context.Background()
	store, _, ds := newImportingStore(t, testRecord("node-1", publicationStatePublished))
	key := publicationPropertyKey("node-2")
	require.NoError(t, store.kube.store(ctx, ds.Name, ds, key, testRecord("node-2", publicationStatePublished)))

	_, err := store.lockedRecords(ctx, ds.Name, ds) // the caller's decision read
	require.NoError(t, err)
	foreignUpdate(t, newKubernetesPublicationStore(store.kube.client, "scale-csi", "one"), ds.Name, key, "2026-10-02T09:00:00Z")

	err = store.store(ctx, ds.Name, ds, key, testRecord("node-2", publicationStateRemoving))
	require.ErrorIs(t, err, errPublicationRecordConflict, "the import adopted the foreign write and the store overwrote it")
	got, err := store.kube.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Equal(t, "2026-10-02T09:00:00Z", got[key].UpdatedAt)
}

// The same through remove.
func TestImportingStoreRemoveKeepsTheCallersReadAcrossTheImport(t *testing.T) {
	ctx := context.Background()
	store, _, ds := newImportingStore(t, testRecord("node-1", publicationStatePublished))
	key := publicationPropertyKey("node-2")
	require.NoError(t, store.kube.store(ctx, ds.Name, ds, key, testRecord("node-2", publicationStatePublished)))
	_, err := store.lockedRecords(ctx, ds.Name, ds)
	require.NoError(t, err)
	foreignUpdate(t, newKubernetesPublicationStore(store.kube.client, "scale-csi", "one"), ds.Name, key, "2026-10-02T09:00:00Z")

	err = store.remove(ctx, ds.Name, ds, []string{key})
	require.ErrorIs(t, err, errPublicationRecordConflict)
	got, err := store.kube.records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Contains(t, got, key)
}

// A removal of a record the locked read saw absent does not delete one
// another process created since: it is reported as a conflict.
func TestKubernetesStoreRemoveOfARecordCreatedSinceTheRead(t *testing.T) {
	ctx := context.Background()
	client := newFakeVolumePublicationClient()
	store := newKubernetesPublicationStore(client, "scale-csi", "one")
	key := publicationPropertyKey("node-1")
	_, err := store.lockedRecords(ctx, "pool/v", nil) // absent
	require.NoError(t, err)
	require.NoError(t, newKubernetesPublicationStore(client, "scale-csi", "one").store(ctx, "pool/v", nil, key,
		testRecord("node-1", publicationStatePublished)))

	err = store.remove(ctx, "pool/v", nil, []string{key})
	require.ErrorIs(t, err, errPublicationRecordConflict)
	got, err := store.records(ctx, "pool/v", nil)
	require.NoError(t, err)
	assert.Contains(t, got, key, "a record created since the read was removed")

	// Still absent: the removal is a no-op.
	_, err = store.lockedRecords(ctx, "pool/w", nil)
	require.NoError(t, err)
	require.NoError(t, store.remove(ctx, "pool/w", nil, []string{key}))
}

// A stale-record takeover whose revoke meets a record conflict returns
// Aborted, which the attacher retries, not Internal.
func TestStaleTakeoverRevokeConflictIsAborted(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	takeoverNFSVolume(t, ctx, client, "takeover-conflict")
	d := newTakeoverTestDriver(client,
		takeoverPV("takeover-conflict"),
		takeoverVA("va-takeover-conflict-b", "takeover-conflict", "worker-b"),
	)
	fake := newFakeVolumePublicationClient()
	d.publicationStore = importingPublicationStore{
		kube:   newKubernetesPublicationStore(fake, "scale-csi", "takeover"),
		legacy: zfsPublicationStore{client: client},
	}
	_, err := d.ControllerPublishVolume(ctx, takeoverPublishRequest("takeover-conflict", "worker-a", "192.0.2.11"))
	require.NoError(t, err)
	// The revoke's removing tombstone loses to another writer.
	fake.PrependReactor("update", "volumepublications", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(volumePublicationGVR.GroupResource(), "vp", errors.New("modified"))
	})
	_, err = d.ControllerPublishVolume(ctx, takeoverPublishRequest("takeover-conflict", "worker-b", "192.0.2.12"))
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err), "%v", err)
}
