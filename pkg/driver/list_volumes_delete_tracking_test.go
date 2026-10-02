package driver

import (
	"context"
	"testing"
	"time"

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
		page, start, _, err := d.managedVolumesForListPage(context.Background(), true, 10, 0)
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
