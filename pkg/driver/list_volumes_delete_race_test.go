package driver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// blockingListClient holds its first listing, after reading it, until release.
type blockingListClient struct {
	*truenas.MockClient
	read    chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingListClient(mock *truenas.MockClient) *blockingListClient {
	return &blockingListClient{MockClient: mock, read: make(chan struct{}), release: make(chan struct{})}
}

func (c *blockingListClient) DatasetQueryByParent(ctx context.Context, parent string) ([]*truenas.Dataset, error) {
	out, err := c.MockClient.DatasetQueryByParent(ctx, parent)
	c.once.Do(func() { close(c.read); <-c.release })
	return out, err
}

// newListWalkDriverWithRecordsInKubernetes is newListWalkDriver with
// publication records kept as VolumePublications: the store under which a
// page is served from the listing alone, with no re-read by name to drop a
// volume that is gone.
func newListWalkDriverWithRecordsInKubernetes(client truenas.ClientInterface) *Driver {
	d := newListWalkDriver(client)
	d.publicationStore = importingPublicationStore{
		kube:   kubernetesPublicationStore{client: newFakeVolumePublicationClient(), namespace: "scale-csi", instance: "list-walk"},
		legacy: zfsPublicationStore{client: client},
	}
	return d
}

// A volume DeleteVolume removed while a fresh walk's listing was in flight is
// left out of that walk, as the per-page re-read left it out before v1.22.
func TestListVolumesLeavesOutAVolumeDeletedWhileItsListingRan(t *testing.T) {
	mock := truenas.NewMockClient()
	for _, n := range []string{"vol-a", "vol-b"} {
		seedListWalkVolume(mock, n)
	}
	client := newBlockingListClient(mock)
	d := newListWalkDriverWithRecordsInKubernetes(client)
	var resp *csi.ListVolumesResponse
	var err error
	done := make(chan struct{})
	go func() {
		resp, err = d.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
		close(done)
	}()
	<-client.read
	// DeleteVolume of vol-a completes now: the dataset goes, and DeleteVolume's
	// deferred hook runs.
	delete(mock.Datasets, listWalkParent+"/vol-a")
	d.forgetListedVolume("vol-a")
	close(client.release)
	<-done
	require.NoError(t, err)
	require.Equal(t, []string{"vol-b"}, listWalkPageIDs(t, resp), "a volume DeleteVolume already removed is reported")
}

// A walk that joins a shared listing another caller began earlier leaves out
// a volume deleted after that listing began, though before the walk arrived.
func TestListVolumesJoiningASharedListingLeavesOutAnEarlierDelete(t *testing.T) {
	mock := truenas.NewMockClient()
	for _, n := range []string{"vol-a", "vol-b"} {
		seedListWalkVolume(mock, n)
	}
	client := newBlockingListClient(mock)
	d := newListWalkDriverWithRecordsInKubernetes(client)
	reconcilerDone := make(chan struct{})
	go func() {
		_, _ = d.listAllManagedDatasets(context.Background())
		close(reconcilerDone)
	}()
	<-client.read
	delete(mock.Datasets, listWalkParent+"/vol-a")
	d.forgetListedVolume("vol-a")
	var resp *csi.ListVolumesResponse
	var err error
	done := make(chan struct{})
	go func() {
		resp, err = d.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
		close(done)
	}()
	require.Eventually(t, func() bool {
		d.managedListingMu.Lock()
		defer d.managedListingMu.Unlock()
		return d.managedListing != nil && d.managedListing.joined == 1
	}, 5*time.Second, time.Millisecond, "the walk joins the reconciler's listing")
	close(client.release)
	<-done
	<-reconcilerDone
	require.NoError(t, err)
	require.Equal(t, []string{"vol-b"}, listWalkPageIDs(t, resp))
	d.volumePageCacheMu.Lock()
	defer d.volumePageCacheMu.Unlock()
	require.Empty(t, d.volumePageListings)
}

// Deletes are not kept once no listing can need them, so the record stays
// bounded when nothing lists volumes.
func TestListVolumesDeleteRecordStaysBoundedWithoutAWalk(t *testing.T) {
	d := newListWalkDriver(truenas.NewMockClient())
	for _, n := range []string{"vol-a", "vol-b", "vol-c"} {
		d.forgetListedVolume(n)
	}
	d.volumePageCacheMu.Lock()
	defer d.volumePageCacheMu.Unlock()
	require.Empty(t, d.volumePageDeleted)
}

// A walk that arrived while a shared listing was in flight may be served that
// listing's rows after it ends; a delete made after the shared listing began
// is kept for the walk even when the listing has ended and another delete
// prunes the record meanwhile.
func TestListVolumesKeepsADeleteAJoinedListingMayHold(t *testing.T) {
	d := newListWalkDriverWithRecordsInKubernetes(truenas.NewMockClient())
	shared := &managedListingCall{done: make(chan struct{}), start: time.Now()}
	d.managedListingMu.Lock()
	d.managedListing = shared
	d.managedListingMu.Unlock()
	d.forgetListedVolume("vol-a")
	listing := d.beginVolumeListing()
	d.managedListingMu.Lock()
	d.managedListing = nil // the shared listing ends before the walk filters its rows
	d.managedListingMu.Unlock()
	d.forgetListedVolume("vol-b")
	require.True(t, d.listedVolumeDeletedSince(listWalkParent+"/vol-a", shared.start),
		"a delete the joined listing may still hold was pruned")
	d.endVolumeListing(listing)
	d.volumePageCacheMu.Lock()
	defer d.volumePageCacheMu.Unlock()
	require.Empty(t, d.volumePageDeleted, "once no listing can need them the deletes go")
}
