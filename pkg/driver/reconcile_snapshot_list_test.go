package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// snapshotListCountingMock counts SnapshotListAll invocations. On TrueNAS 26.0
// each invocation re-transfers the entire parent snapshot payload, so the number
// of invocations per reconcile pass is the wire-volume lever Fix 4a collapses to
// one.
type snapshotListCountingMock struct {
	*truenas.MockClient
	listAllCalls int
}

func (m *snapshotListCountingMock) SnapshotListAll(ctx context.Context, parentDataset string, limit, offset int) ([]*truenas.Snapshot, error) {
	m.listAllCalls++
	return m.MockClient.SnapshotListAll(ctx, parentDataset, limit, offset)
}

// TestListAllManagedSnapshotsSingleFetch proves the reconcile pass issues the
// recursive snapshot listing exactly once regardless of snapshot count. Pre-fix
// the pass re-called SnapshotListAll once per 100-item page (ceil(N/100) full
// payload transfers — O(N²) wire volume on TrueNAS 26.0).
func TestListAllManagedSnapshotsSingleFetch(t *testing.T) {
	ctx := context.Background()
	base := truenas.NewMockClient()

	// Well over one page (reconcileListPageSize == 100) of CSI snapshots.
	const snapshotCount = 250
	created := time.Now().Add(-48 * time.Hour)
	for i := 0; i < snapshotCount; i++ {
		addReconcileSnapshot(t, base, fmt.Sprintf("vol-%03d", i), fmt.Sprintf("snap-%03d", i), created, true, 1)
	}

	mock := &snapshotListCountingMock{MockClient: base}
	d := newOrphanShareSweepDriver(base)
	d.truenasClient = mock

	managed, tombstones, err := d.listAllManagedSnapshots(ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, mock.listAllCalls, "the recursive snapshot listing must be issued exactly once per pass")
	assert.Len(t, managed, snapshotCount, "every CSI snapshot must still be returned by the single fetch")
	assert.Empty(t, tombstones)
}
