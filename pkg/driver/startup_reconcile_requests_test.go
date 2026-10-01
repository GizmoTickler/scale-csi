package driver

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// countingPublicationStore counts record writes on top of whichever store the
// driver would use.
type countingPublicationStore struct {
	publicationStore
	mu     sync.Mutex
	stores int
}

func (s *countingPublicationStore) store(ctx context.Context, datasetName string, ds *truenas.Dataset, key string, record publicationRecord) error {
	s.mu.Lock()
	s.stores++
	s.mu.Unlock()
	return s.publicationStore.store(ctx, datasetName, ds, key, record)
}

func (s *countingPublicationStore) storeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.stores
	s.stores = 0
	return n
}

// kubeRequestCounts tallies list and get calls per resource on a fake clientset.
type kubeRequestCounts struct {
	mu    sync.Mutex
	calls map[string]int
}

func countKubeRequests(kube *kubernetesfake.Clientset) *kubeRequestCounts {
	counts := &kubeRequestCounts{calls: map[string]int{}}
	kube.PrependReactor("*", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		counts.mu.Lock()
		counts.calls[action.GetVerb()+" "+action.GetResource().Resource]++
		counts.mu.Unlock()
		return false, nil, nil
	})
	return counts
}

func (c *kubeRequestCounts) take() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.calls
	c.calls = map[string]int{}
	return out
}

// startupNFSVolumes provisions n NFS volumes, each attached to its own node.
func startupNFSVolumes(t *testing.T, client *truenas.MockClient, n int) []runtime.Object {
	t.Helper()
	var objects []runtime.Object
	for i := 0; i < n; i++ {
		volumeID := fmt.Sprintf("restart-%d", i)
		objects, _ = staleRecordVolume(t, objects, volumeID)
		dataset, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
			Name: "pool/parent/" + volumeID, Type: "FILESYSTEM",
		})
		require.NoError(t, err)
		share, err := client.NFSShareCreate(context.Background(), &truenas.NFSShareCreateParams{
			Path: dataset.Mountpoint, Networks: []string{"192.0.2.0/24"}, Enabled: true,
		})
		require.NoError(t, err)
		require.NoError(t, client.DatasetSetUserProperty(context.Background(), dataset.Name, PropNFSShareID, fmt.Sprint(share.ID)))
	}
	return objects
}

// A startup pass lists the cluster once, then re-reads only each volume's own
// VolumeAttachments by name; a restart with every record already in place
// writes no record.
func TestStartupReconcileRequestsPerVolume(t *testing.T) {
	const volumes = 3
	ctx := context.Background()
	client := truenas.NewMockClient()
	kube := kubernetesfake.NewSimpleClientset(startupNFSVolumes(t, client, volumes)...)
	requests := countKubeRequests(kube)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(64))
	store := &countingPublicationStore{publicationStore: d.publications()}
	d.publicationStore = store

	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	want := map[string]int{
		"list persistentvolumes": 1, "list volumeattachments": 1, "list csinodes": 1, "list nodes": 1,
		"get volumeattachments": volumes,
	}
	assert.Equal(t, want, requests.take(), "4 cluster-wide lists per pass, then one GET per attachment (was 4 more lists per volume)")
	assert.Equal(t, volumes, store.storeCount(), "the first pass writes each missing record")

	// A restart: every record is already in place and unchanged.
	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	assert.Equal(t, want, requests.take())
	assert.Zero(t, store.storeCount(), "an unchanged record is not rewritten on restart")
	for i := 0; i < volumes; i++ {
		dataset, err := client.DatasetGet(ctx, fmt.Sprintf("pool/parent/restart-%d", i))
		require.NoError(t, err)
		records := mustStoredRecords(t, d, dataset)
		assert.Contains(t, records, publicationPropertyKey(fmt.Sprintf("worker-restart-%d", i)))
	}
}

// A record that differs from what the attachment says (an address the node no
// longer reports, here) is still rewritten on restart.
func TestStartupReconcileRewritesAChangedRecord(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	kube := kubernetesfake.NewSimpleClientset(startupNFSVolumes(t, client, 1)...)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(16))
	store := &countingPublicationStore{publicationStore: d.publications()}
	d.publicationStore = store
	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	require.Equal(t, 1, store.storeCount())

	dataset, err := client.DatasetGet(ctx, "pool/parent/restart-0")
	require.NoError(t, err)
	key := publicationPropertyKey("worker-restart-0")
	stale := mustStoredRecords(t, d, dataset)[key]
	stale.IPs = []string{"192.0.2.77"}
	require.NoError(t, d.publications().store(ctx, dataset.Name, dataset, key, stale))
	store.storeCount()

	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	assert.Equal(t, 1, store.storeCount())
	dataset, err = client.DatasetGet(ctx, "pool/parent/restart-0")
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.11"}, mustStoredRecords(t, d, dataset)[key].IPs)
}

// The per-volume refresh re-reads only the snapshot's own attachments, so it
// cannot see a VolumeAttachment created after the snapshot. Before a volume is
// quarantined on a "stale" record, the full listing must confirm that the
// record's node really has no attachment: here it does, so the volume reports
// a genuine conflict instead of being deferred behind a record the stale-record
// sweep would never revoke.
func TestStartupQuarantineIsConfirmedAgainstAFullListing(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	objects := startupNFSVolumes(t, client, 1)
	// A second attachment, to node "worker-late", that the snapshot list misses.
	pvName := "pv-restart-0"
	late := &storagev1.VolumeAttachment{}
	late.Name = "va-late"
	late.Spec = storagev1.VolumeAttachmentSpec{
		Attacher: "csi.scale.io", NodeName: "worker-late",
		Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
	}
	late.Status.Attached = true
	objects = append(objects, late)
	kube := kubernetesfake.NewSimpleClientset(objects...)
	var mu sync.Mutex
	lists := 0
	kube.PrependReactor("list", "volumeattachments", func(clienttesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		lists++
		if lists > 1 {
			return false, nil, nil
		}
		list, err := kube.Tracker().List(storagev1.SchemeGroupVersion.WithResource("volumeattachments"),
			storagev1.SchemeGroupVersion.WithKind("VolumeAttachment"), "")
		if err != nil {
			return true, nil, err
		}
		snapshot := &storagev1.VolumeAttachmentList{}
		for _, item := range list.(*storagev1.VolumeAttachmentList).Items {
			if item.Name != late.Name {
				snapshot.Items = append(snapshot.Items, item)
			}
		}
		return true, snapshot, nil
	})
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(16))
	// worker-late already holds a published record (its publish ran after the
	// snapshot), which conflicts with the snapshot's single-node attachment.
	dataset, err := client.DatasetGet(ctx, "pool/parent/restart-0")
	require.NoError(t, err)
	lateRecord, err := newPublicationRecord(NodeIdentity{Name: "worker-late", IPs: []net.IP{net.ParseIP("192.0.2.12")}},
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false)
	require.NoError(t, err)
	require.NoError(t, d.publications().store(ctx, dataset.Name, dataset, publicationPropertyKey("worker-late"), lateRecord))

	err = d.reconcilePublishedAttachments(ctx)
	require.Error(t, err, "a conflict with a live attachment is not a stale record")
	assert.Contains(t, err.Error(), "has not converged")
	assert.Zero(t, d.startupReconcileQuarantineCount.Load())
}
