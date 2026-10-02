package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clienttesting "k8s.io/client-go/testing"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func selectingDriver(t *testing.T, setting string, listErr ...error) (*Driver, *int) {
	t.Helper()
	origSetting, origNamespace, origInterval, origTestStore := publicationStoreSetting, podNamespace, publicationStoreProbeInterval, testPublicationStore
	t.Cleanup(func() {
		publicationStoreSetting, podNamespace, publicationStoreProbeInterval, testPublicationStore = origSetting, origNamespace, origInterval, origTestStore
	})
	testPublicationStore = nil // this test is about the store a driver picks
	publicationStoreSetting = func() string { return setting }
	podNamespace = func() (string, error) { return "scale-csi", nil }
	publicationStoreProbeInterval = time.Millisecond

	fake := newFakeVolumePublicationClient()
	lists := 0
	fake.PrependReactor("list", "volumepublications", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if list, ok := action.(clienttesting.ListAction); ok && !list.GetListRestrictions().Labels.Empty() {
			return false, nil, nil // the cache's watch, not the probe
		}
		lists++
		if lists <= len(listErr) && listErr[lists-1] != nil {
			return true, nil, listErr[lists-1]
		}
		return false, nil, nil
	})
	d := &Driver{
		runController: true,
		config:        &Config{DriverInstanceID: "one"},
		truenasClient: truenas.NewMockClient(),
		eventRecorder: &EventRecorder{dynamicClient: fake},
	}
	t.Cleanup(func() {
		if publicationCache := d.publicationCacheRef.Load(); publicationCache != nil {
			publicationCache.close()
		}
	})
	return d, &lists
}

func TestPublicationStoreSelection(t *testing.T) {
	ctx := context.Background()
	resource := schema.GroupResource{Group: volumePublicationGVR.Group, Resource: volumePublicationGVR.Resource}

	for _, setting := range []string{"", "zfs"} {
		d, lists := selectingDriver(t, setting)
		require.NoError(t, d.selectPublicationStore(ctx))
		assert.IsType(t, zfsPublicationStore{}, d.publications(), "setting %q", setting)
		assert.Zero(t, *lists, "the ZFS store needs no Kubernetes")
	}

	d, _ := selectingDriver(t, "kubernetes")
	require.NoError(t, d.selectPublicationStore(ctx))
	store, ok := d.publications().(importingPublicationStore)
	require.True(t, ok, "%T", d.publications())
	assert.Equal(t, "scale-csi", store.kube.namespace)
	assert.Equal(t, "one", store.kube.instance)
	require.NotNil(t, store.cache)
	assert.True(t, store.cache.informer.HasSynced(), "the cache synced before serving")
	assert.Same(t, store.cache, d.publicationCacheRef.Load(), "Stop() can end the watch")

	d, _ = selectingDriver(t, "kubernetes", apierrors.NewNotFound(resource, ""))
	err := d.selectPublicationStore(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CRD")
	assert.IsType(t, zfsPublicationStore{}, d.publications(), "never a silent fallback: the caller stops")

	d, _ = selectingDriver(t, "kubernetes", apierrors.NewForbidden(resource, "", errors.New("rbac")))
	err = d.selectPublicationStore(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Role")

	// A passing API error is retried; a lasting one stops the controller.
	d, lists := selectingDriver(t, "kubernetes", errors.New("connection refused"))
	require.NoError(t, d.selectPublicationStore(ctx))
	assert.Equal(t, 2, *lists)
	failing := make([]error, publicationStoreProbeAttempts)
	for i := range failing {
		failing[i] = errors.New("connection refused")
	}
	d, lists = selectingDriver(t, "kubernetes", failing...)
	require.Error(t, d.selectPublicationStore(ctx))
	assert.Equal(t, publicationStoreProbeAttempts, *lists)

	d, _ = selectingDriver(t, "etcd")
	require.Error(t, d.selectPublicationStore(ctx), "an unknown store is refused")
	d, _ = selectingDriver(t, "kubernetes")
	d.eventRecorder = nil
	require.Error(t, d.selectPublicationStore(ctx), "no client: refused, not ZFS")
}
