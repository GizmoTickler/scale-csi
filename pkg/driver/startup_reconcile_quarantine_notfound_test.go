package driver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// A transient "dataset not found" (a pool not yet imported: pool.dataset.query
// answers []) must not end a live quarantine: a later pass converging another
// volume would then let the strict loop exit, and the volume whose quarantine
// was dropped would never converge.
func TestStartupQuarantineSurvivesATransientNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := truenas.NewMockClient()
	var objects []runtime.Object
	objects, _ = staleRecordVolume(t, objects, "q1")
	objects, _ = staleRecordVolume(t, objects, "q2")
	kube := kubernetesfake.NewSimpleClientset(objects...)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(256))
	staleRecordNFSVolume(t, client, "q1", "worker-gone-1")
	staleRecordNFSVolume(t, client, "q2", "worker-gone-2")
	d.ready.Store(false)
	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)
	require.Eventually(t, d.ready.Load, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, 2, d.startupQuarantineCount())

	// The real DatasetGet's empty-result error, once per dataset: heal reads
	// each quarantined dataset first, so it is heal that sees it, never the
	// re-runs heal's signals start meanwhile.
	d.truenasClient = &notFoundOnceClient{MockClient: client, pending: map[string]bool{
		"pool/parent/q1": true, "pool/parent/q2": true,
	}}
	d.healStartupQuarantines(ctx)
	// The re-runs heal asked for re-quarantine both volumes (their stale
	// records are still there); a dropped quarantine would stay dropped.
	require.Eventually(t, func() bool { return d.startupQuarantineCount() == 2 }, 3*time.Second, 10*time.Millisecond,
		"a transient not-found ended a live quarantine")

	// q1's stale record goes; heal signals it; the targeted pass converges q1.
	ds1, err := client.DatasetGet(ctx, "pool/parent/q1")
	require.NoError(t, err)
	require.NoError(t, d.publications().remove(ctx, ds1.Name, ds1, []string{publicationPropertyKey("worker-gone-1")}))
	d.healStartupQuarantines(ctx)
	d.requestStartupAttachmentReconcile("pool/parent/q1") // what the revoke would send
	require.Eventually(t, func() bool {
		ds, _ := client.DatasetGet(ctx, "pool/parent/q1")
		_, ok := mustStoredRecords(t, d, ds)[publicationPropertyKey("worker-q1")]
		return ok
	}, 3*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	// q2's stale record is revoked later; its signal reaches no loop.
	ds2, err := client.DatasetGet(ctx, "pool/parent/q2")
	require.NoError(t, err)
	require.NoError(t, d.publications().remove(ctx, ds2.Name, ds2, []string{publicationPropertyKey("worker-gone-2")}))
	d.requestStartupAttachmentReconcile("pool/parent/q2")
	ok := false
	for i := 0; i < 100 && !ok; i++ {
		ds, _ := client.DatasetGet(ctx, "pool/parent/q2")
		_, ok = mustStoredRecords(t, d, ds)[publicationPropertyKey("worker-q2")]
		time.Sleep(20 * time.Millisecond)
	}
	require.True(t, ok, "q2 never converged: its quarantine was cleared by a transient not-found and the loop exited")
}

// notFoundOnceClient answers DatasetGet for each pending dataset once with the
// error the real client returns for an empty pool.dataset.query result.
type notFoundOnceClient struct {
	*truenas.MockClient
	mu      sync.Mutex
	pending map[string]bool
}

func (c *notFoundOnceClient) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	c.mu.Lock()
	fail := c.pending[name]
	delete(c.pending, name)
	c.mu.Unlock()
	if fail {
		return nil, errors.New("failed to get dataset: dataset not found: " + name)
	}
	return c.MockClient.DatasetGet(ctx, name)
}
