package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// countingSnapshotListClient wraps the mock so the batched ListSnapshots
// restore-size path can be observed: it counts DatasetGetByNames round trips
// (recording the exact names of each batch) and any residual per-snapshot
// DatasetGet reads — the N+1 shape the batching removed — and can fail the
// batched read to prove a batch failure still fails the RPC with Internal
// instead of silently degrading restore sizes.
type countingSnapshotListClient struct {
	*truenas.MockClient
	datasetGetCalls        int
	datasetGetByNamesCalls int
	batches                [][]string
	batchErr               error
}

func (c *countingSnapshotListClient) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	c.datasetGetCalls++
	return c.MockClient.DatasetGet(ctx, name)
}

func (c *countingSnapshotListClient) DatasetGetByNames(ctx context.Context, names []string) (map[string]*truenas.Dataset, error) {
	c.datasetGetByNamesCalls++
	c.batches = append(c.batches, append([]string(nil), names...))
	if c.batchErr != nil {
		return nil, c.batchErr
	}
	return c.MockClient.DatasetGetByNames(ctx, names)
}

func newSnapshotListFixture() (*countingSnapshotListClient, *Driver) {
	client := &countingSnapshotListClient{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}},
		truenasClient: client,
	}
	return client, d
}

// createCSISnapshot stamps the snapshot with the local CSI markers the driver
// writes at CreateSnapshot, so isCSISnapshot classifies it as CSI-created and
// ListSnapshots produces an entry for it.
func createCSISnapshot(t *testing.T, client *countingSnapshotListClient, dataset, name, sourceVolumeID string) {
	t.Helper()
	_, err := client.SnapshotCreate(context.Background(), dataset, name, map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           name,
		PropCSISnapshotSourceVolumeID: sourceVolumeID,
	})
	require.NoError(t, err)
}

// TestListSnapshotsPageBatchesUniqueSourceDatasetReads proves the paged path
// no longer issues one DatasetGet per snapshot to compute restore sizes: a
// page of 6 snapshots over 2 unique source datasets (the scheduled-snapshot
// shape, where many snapshots share a source) costs exactly one
// DatasetGetByNames round trip — both names fit one chunk of the 32 KiB
// budget — and zero DatasetGet reads. The batch must also contain ONLY the
// datasets of snapshots that produce entries: the non-CSI snapshot's dataset
// is excluded because such snapshots never needed a dataset read.
func TestListSnapshotsPageBatchesUniqueSourceDatasetReads(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotListFixture()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/src-a", Type: "VOLUME", Volsize: 12 * testGiB})
	require.NoError(t, err)
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/src-b", Type: "VOLUME", Volsize: 7 * testGiB})
	require.NoError(t, err)
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/plain", Type: "FILESYSTEM"})
	require.NoError(t, err)
	for i := 1; i <= 3; i++ {
		createCSISnapshot(t, client, "pool/parent/src-a", fmt.Sprintf("snap-a-%d", i), "src-a")
		createCSISnapshot(t, client, "pool/parent/src-b", fmt.Sprintf("snap-b-%d", i), "src-b")
	}
	// A manual snapshot without the CSI markers produces no entry, so its
	// dataset must never appear in the batched read.
	_, err = client.SnapshotCreate(ctx, "pool/parent/plain", "manual", nil)
	require.NoError(t, err)

	resp, err := d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 6)
	// The restore-size override must survive the batching: every entry reports
	// its source dataset's capacity, not the snapshot's own used size.
	expected := map[string]int64{"src-a": 12 * testGiB, "src-b": 7 * testGiB}
	for _, entry := range resp.GetEntries() {
		assert.Equal(t, expected[entry.GetSnapshot().GetSourceVolumeId()], entry.GetSnapshot().GetSizeBytes())
	}

	assert.Equal(t, 1, client.datasetGetByNamesCalls, "2 unique dataset names must fit a single chunk")
	assert.Equal(t, 0, client.datasetGetCalls, "the per-snapshot N+1 DatasetGet reads must be gone")
	require.Len(t, client.batches, 1)
	assert.ElementsMatch(t, []string{"pool/parent/src-a", "pool/parent/src-b"}, client.batches[0],
		"only unique datasets of entry-producing snapshots belong in the batch")
}

// TestListSnapshotsKeepsEntryWhenSourceDatasetMissing pins the NotFound
// equivalence of the batched path: DatasetGetByNames omits absent names
// rather than erroring, and a name missing from the result map must behave
// exactly like the per-snapshot path's DatasetGet NotFound — the entry is
// kept, just without the restore-size override.
func TestListSnapshotsKeepsEntryWhenSourceDatasetMissing(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotListFixture()
	// The source dataset was destroyed after the snapshot was taken (the mock,
	// like ZFS listings raced against a delete, does not require the dataset to
	// still exist for the snapshot to be listed).
	createCSISnapshot(t, client, "pool/parent/ghost", "orphaned", "ghost")
	client.SetSnapshotUsedBytes("pool/parent/ghost@orphaned", 2*testGiB)

	resp, err := d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	assert.Equal(t, 2*testGiB, resp.GetEntries()[0].GetSnapshot().GetSizeBytes(),
		"a missing source dataset keeps the snapshot's own size — no override")
	assert.Equal(t, 1, client.datasetGetByNamesCalls)
	assert.Equal(t, 0, client.datasetGetCalls)
}

// TestListSnapshotsBatchReadFailureFailsInternal pins the failure semantics
// of the batched path: a DatasetGetByNames error is a real read failure (the
// batch API expresses NotFound by omission, never by error), so the whole RPC
// must fail with Internal — mentioning a dataset and snapshot like the
// per-snapshot path did — rather than silently returning wrong restore sizes.
func TestListSnapshotsBatchReadFailureFailsInternal(t *testing.T) {
	ctx := context.Background()
	client, d := newSnapshotListFixture()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/src-a", Type: "VOLUME", Volsize: testGiB})
	require.NoError(t, err)
	createCSISnapshot(t, client, "pool/parent/src-a", "snap-a-1", "src-a")
	client.batchErr = errors.New("injected batch failure")

	_, err = d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "pool/parent/src-a@snap-a-1",
		"the error must identify a snapshot in the failed batch")
	assert.Contains(t, err.Error(), "injected batch failure")
}
