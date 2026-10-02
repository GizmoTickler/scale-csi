package driver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// gatedListingClient holds every DatasetQueryByParent until released and
// counts them.
type gatedListingClient struct {
	*truenas.MockClient
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (c *gatedListingClient) DatasetQueryByParent(ctx context.Context, parent string) ([]*truenas.Dataset, error) {
	c.calls.Add(1)
	c.entered <- struct{}{}
	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return c.MockClient.DatasetQueryByParent(ctx, parent)
}

// DatasetList honours a cancelled context, as the real client does.
func (c *gatedListingClient) DatasetList(ctx context.Context, parent string, limit, offset int) ([]*truenas.Dataset, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.MockClient.DatasetList(ctx, parent, limit, offset)
}

func newGatedListingDriver(t *testing.T) (*Driver, *gatedListingClient) {
	t.Helper()
	mock := truenas.NewMockClient()
	ctx := context.Background()
	for _, name := range []string{"pool/parent/pvc-b", "pool/parent/pvc-a"} {
		_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: name, Type: "VOLUME", Volsize: testGiB})
		require.NoError(t, err)
		require.NoError(t, mock.DatasetSetUserProperties(ctx, name, map[string]string{PropManagedResource: "true"}))
	}
	client := &gatedListingClient{MockClient: mock, entered: make(chan struct{}, 8), release: make(chan struct{})}
	d := &Driver{config: &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}}, truenasClient: client}
	return d, client
}

// Callers that arrive while a listing runs share it, and each gets its own
// copy: sorting one result or writing into one of its datasets leaves the
// others alone.
func TestConcurrentManagedListingsShareOneRead(t *testing.T) {
	d, client := newGatedListingDriver(t)
	ctx := context.Background()
	const callers = 4
	results := make([][]*truenas.Dataset, callers)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		results[0], err = d.listAllManagedDatasets(ctx)
		assert.NoError(t, err)
	}()
	<-client.entered
	for i := 1; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			results[i], err = d.listAllManagedDatasets(ctx)
			assert.NoError(t, err)
		}(i)
	}
	require.Eventually(t, func() bool {
		d.managedListingMu.Lock()
		defer d.managedListingMu.Unlock()
		return d.managedListing != nil
	}, time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond) // let the joiners reach the wait
	close(client.release)
	wg.Wait()

	assert.Equal(t, int32(1), client.calls.Load(), "one listing for every concurrent caller")
	for i := range results {
		require.Len(t, results[i], 2)
	}
	results[0][0], results[0][1] = results[0][1], results[0][0]
	results[0][0].UserProperties["scale-csi:probe"] = truenas.UserProperty{Value: "x"}
	for i := 1; i < callers; i++ {
		assert.NotSame(t, results[0][0], results[i][0])
		for _, dataset := range results[i] {
			assert.NotContains(t, dataset.UserProperties, "scale-csi:probe")
		}
	}
}

// A caller whose listing was cancelled under it runs its own.
func TestManagedListingJoinerOutlivesACancelledLeader(t *testing.T) {
	d, client := newGatedListingDriver(t)
	leaderCtx, cancel := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, err := d.listAllManagedDatasets(leaderCtx)
		assert.ErrorIs(t, err, context.Canceled)
	}()
	<-client.entered
	joined := make(chan []*truenas.Dataset, 1)
	go func() {
		datasets, err := d.listAllManagedDatasets(context.Background())
		assert.NoError(t, err)
		joined <- datasets
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-leaderDone
	<-client.entered
	close(client.release)
	assert.Len(t, <-joined, 2)
}
