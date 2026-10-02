package driver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// A walk frozen from a listing that began at S1 meets a cached view another
// walk began later, at S2. The older walk does not replace the newer view,
// and a volume DeleteVolume removed in [S1, S2), recorded before the older
// walk froze, is left out of that walk's first page (by the freeze-time
// filter: once the listing has ended, the newer view's start would let the
// delete be pruned).
func TestListVolumesOlderWalkKeepsTheNewerViewAndFiltersItsDeletes(t *testing.T) {
	mock := truenas.NewMockClient()
	for _, n := range []string{"vol-a", "vol-b"} {
		seedListWalkVolume(mock, n)
	}
	client := newBlockingListClient(mock)
	d := newListWalkDriverWithRecordsInKubernetes(client)

	type result struct {
		page  []listedVolume
		start time.Time
		err   error
	}
	done := make(chan result, 1)
	go func() {
		page, start, _, listing, err := d.managedVolumesForListPage(context.Background(), true, 10, 0)
		d.endVolumeListing(listing)
		done <- result{page, start, err}
	}()
	<-client.read // the listing began at S1 and has read its rows

	delete(mock.Datasets, listWalkParent+"/vol-a")
	d.forgetListedVolume("vol-a") // deleted in [S1, S2)
	time.Sleep(time.Millisecond)
	newer := time.Now() // S2: a later walk's view is cached meanwhile
	newerView := []listedVolume{{name: listWalkParent + "/vol-b"}}
	d.volumePageCacheMu.Lock()
	d.volumePageCache, d.volumePageCacheStart, d.volumePageCacheTime = newerView, newer, time.Now()
	d.volumePageCacheMu.Unlock()

	close(client.release)
	got := <-done
	require.NoError(t, got.err)
	require.True(t, got.start.Before(newer))
	names := make([]string, 0, len(got.page))
	for _, volume := range got.page {
		names = append(names, volume.name)
	}
	assert.Equal(t, []string{listWalkParent + "/vol-b"}, names, "a delete in [S1, S2) is in the older walk's page")

	d.volumePageCacheMu.Lock()
	defer d.volumePageCacheMu.Unlock()
	assert.Equal(t, newer, d.volumePageCacheStart, "the older walk replaced the newer view")
	assert.Equal(t, newerView, d.volumePageCache)
}

// listPageHookClient runs hook once, inside the first DatasetGetByNames: the
// re-read of a page's datasets that still carry ZFS record keys.
type listPageHookClient struct {
	*truenas.MockClient
	once sync.Once
	hook func()
}

func (c *listPageHookClient) DatasetGetByNames(ctx context.Context, names []string) (map[string]*truenas.Dataset, error) {
	c.once.Do(c.hook)
	return c.MockClient.DatasetGetByNames(ctx, names)
}

// A continuation page is served from the cached view: the page's TTL can
// lapse while the page re-reads, and a delete pruned then would bring back,
// on that page, a volume DeleteVolume removed after the view's listing
// began. Serving the page registers it as a listing from the view's start
// until its entries are built, so no prune can drop that delete.
func TestListVolumesContinuationPageKeepsItsDeletesAcrossATTLLapse(t *testing.T) {
	mock := truenas.NewMockClient()
	seedListWalkVolume(mock, "vol-a")
	keyed := seedListWalkVolume(mock, "vol-b")
	// Not yet imported: a ZFS record key, so the page re-reads vol-b.
	seedListWalkPublicationRecord(t, keyed, publicationRecord{Version: publicationRecordVersion, Node: "node-a",
		EncodedID: "encoded-node-a", State: publicationStatePublished}, "local")
	seedListWalkVolume(mock, "vol-c")
	client := &listPageHookClient{MockClient: mock}
	d := newListWalkDriverWithRecordsInKubernetes(client)

	resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 1})
	require.NoError(t, err)
	require.Equal(t, "1", resp.NextToken)

	// DeleteVolume of vol-c completes between the pages.
	delete(mock.Datasets, listWalkParent+"/vol-c")
	d.forgetListedVolume("vol-c")
	client.hook = func() {
		// The view's TTL lapses while the page re-reads, and another
		// volume's delete prunes the record meanwhile.
		d.volumePageCacheMu.Lock()
		d.volumePageCacheTime = time.Now().Add(-2 * volumeListPageCacheTTL)
		d.volumePageCacheMu.Unlock()
		d.forgetListedVolume("vol-unrelated")
	}
	resp, err = d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: "1"})
	require.NoError(t, err)
	assert.Equal(t, []string{"vol-b"}, listWalkPageIDs(t, resp), "a volume DeleteVolume removed is reported")

	d.volumePageCacheMu.Lock()
	defer d.volumePageCacheMu.Unlock()
	assert.Empty(t, d.volumePageListings, "the page's listing ended")
}
