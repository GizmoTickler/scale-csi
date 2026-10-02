package driver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// quarantinedStartupDriver starts a strict reconcile loop over one volume, q1,
// left quarantined by a stale record for worker-gone-1.
func quarantinedStartupDriver(t *testing.T) (*Driver, *truenas.MockClient, *kubernetesfake.Clientset) {
	t.Helper()
	client := truenas.NewMockClient()
	objects, _ := staleRecordVolume(t, nil, "q1")
	kube := kubernetesfake.NewSimpleClientset(objects...)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(256))
	staleRecordNFSVolume(t, client, "q1", "worker-gone-1")
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), "pool/parent/q1", PropManagedResource, "true"))
	d.ready.Store(false)
	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)
	require.Eventually(t, d.ready.Load, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, d.startupQuarantineCount())
	return d, client, kube
}

// countReconcileRequests counts the re-run requests made from here on.
func countReconcileRequests(t *testing.T) func() int {
	t.Helper()
	var mu sync.Mutex
	signals := 0
	startupReconcileRequestedHook = func(string) { mu.Lock(); signals++; mu.Unlock() }
	t.Cleanup(func() { startupReconcileRequestedHook = nil })
	return func() int { mu.Lock(); defer mu.Unlock(); return signals }
}

// sweepFromListing runs the stale-record sweep on the input it really gets:
// the managed-dataset listing, whose user properties carry no source.
func sweepFromListing(ctx context.Context, t *testing.T, d *Driver) {
	t.Helper()
	datasets, err := d.listAllManagedDatasets(ctx)
	require.NoError(t, err)
	d.reconcileStalePublicationRecords(ctx, datasets, nil, time.Now())
}

// The sweep's listing has no property sources, so records read from it would
// always look gone and every sweep would re-run (and re-quarantine) the
// volume. Heal reads each quarantined dataset itself: a stale record still in
// place is seen, and nothing is signaled.
func TestStartupQuarantineHealReadsRecordsWithTheirSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d, client, _ := quarantinedStartupDriver(t)
	signals := countReconcileRequests(t)

	for i := 0; i < 3; i++ {
		sweepFromListing(ctx, t, d)
	}
	ds, err := client.DatasetGet(ctx, "pool/parent/q1")
	require.NoError(t, err)
	require.Contains(t, mustStoredRecords(t, d, ds), publicationPropertyKey("worker-gone-1"))
	require.Zero(t, signals(), "heal signaled a volume whose stale record is still present")
	require.Equal(t, 1, d.startupQuarantineCount())
}

// A quarantined volume detached before its stale record goes has no
// attachment, so no worker of the re-run touches it: the targeted pass itself
// ends the quarantine.
func TestStartupQuarantineOfADetachedVolumeClears(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d, client, kube := quarantinedStartupDriver(t)

	require.NoError(t, kube.StorageV1().VolumeAttachments().Delete(ctx, "va-q1", metav1.DeleteOptions{}))
	ds, err := client.DatasetGet(ctx, "pool/parent/q1")
	require.NoError(t, err)
	require.NoError(t, d.publications().remove(ctx, ds.Name, ds, []string{publicationPropertyKey("worker-gone-1")}))

	sweepFromListing(ctx, t, d)
	require.Eventually(t, func() bool { return d.startupQuarantineCount() == 0 }, 3*time.Second, 10*time.Millisecond,
		"a detached volume's quarantine is never cleared")
}

// A quarantined volume whose dataset was deleted has nothing left to fence:
// the sweep forgets its quarantine.
func TestStartupQuarantineOfADeletedVolumeClears(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d, client, _ := quarantinedStartupDriver(t)

	require.NoError(t, client.DatasetDelete(ctx, "pool/parent/q1", false, false))
	d.reconcileStalePublicationRecords(ctx, nil, nil, time.Now())
	require.Zero(t, d.startupQuarantineCount(), "a deleted volume's quarantine is never cleared")
}

// In additive mode the stored record also carries the grant's provenance,
// which the snapshot's record lacks until later: only the identity decides
// whether to re-read the node, so a steady restart reads none.
func TestStartupAdditiveSteadyRestartReadsNoNodeIdentity(t *testing.T) {
	const volumes = 3
	ctx := context.Background()
	client := truenas.NewMockClient()
	kube := kubernetesfake.NewSimpleClientset(startupNFSVolumes(t, client, volumes)...)
	requests := countKubeRequests(kube)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(64))
	d.config.Fencing.Mode = FencingModeAdditive
	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	requests.take()

	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	got := requests.take()
	require.Zero(t, got["get csinodes"], "a steady additive restart re-read CSINodes: %v", got)
	require.Zero(t, got["get nodes"], "a steady additive restart re-read Nodes: %v", got)
}
