package driver

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// publishThroughInterceptor runs ControllerPublishVolume the way the CO
// reaches it: through the gRPC interceptor that applies the strict gate.
func publishThroughInterceptor(d *Driver, volumeID, nodeID string) error {
	req := &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID, NodeId: nodeID,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
		VolumeContext: map[string]string{"node_attach_driver": "nfs"},
	}
	_, err := d.logInterceptor(context.Background(), req,
		&grpc.UnaryServerInfo{FullMethod: csi.Controller_ControllerPublishVolume_FullMethodName},
		func(ctx context.Context, r interface{}) (interface{}, error) {
			return d.ControllerPublishVolume(ctx, r.(*csi.ControllerPublishVolumeRequest))
		})
	return err
}

// gateNode returns the CSI node ID of a node on the NFS test network.
func gateNode(t *testing.T, name, ip string) string {
	t.Helper()
	nodeID, err := encodeNodeIdentity(NodeIdentity{Name: name, IPs: []net.IP{net.ParseIP(ip)}})
	require.NoError(t, err)
	return nodeID
}

// newGateDriver is a strict NFS controller over n attached volumes
// (restart-0..n-1) plus extra objects.
func newGateDriver(t *testing.T, n int, extra ...runtime.Object) (*Driver, *truenas.MockClient, *kubernetesfake.Clientset) {
	t.Helper()
	client := truenas.NewMockClient()
	objects := append(startupNFSVolumes(t, client, n), extra...)
	kube := kubernetesfake.NewSimpleClientset(objects...)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(256))
	d.runController = true
	d.ready.Store(false)
	return d, client, kube
}

// A volume with no VolumeAttachment in the startup snapshot is published at
// once, while another volume's convergence still holds strict readiness
// down.
func TestStrictPublishOfAVolumeWithoutAttachmentIsNotHeldByOthers(t *testing.T) {
	setStartupTimings(t, 20*time.Millisecond, time.Hour)
	d, client, _ := newGateDriver(t, 2)
	startupNFSVolumeShare(t, client, "fresh")

	// restart-1 stays unconverged: a live operation holds its lock.
	require.True(t, d.acquireOperationLock(volumeLockKey("restart-1")))
	defer d.releaseOperationLock(volumeLockKey("restart-1"))
	_, err := d.reconcilePublishedAttachmentsFor(context.Background(), nil)
	require.Error(t, err)
	require.False(t, d.ready.Load())

	require.NoError(t, publishThroughInterceptor(d, "fresh", gateNode(t, "worker-fresh", "192.0.2.21")),
		"a volume startup has nothing to converge for is not held by another volume")
	require.NoError(t, publishThroughInterceptor(d, "restart-0", gateNode(t, "worker-restart-0", "192.0.2.11")),
		"a converged volume is not held by another volume")
	assert.False(t, d.ready.Load(), "global readiness still waits for restart-1")
}

// A publish of a volume startup has not converged yet converges it first,
// under the volume lock, and then publishes.
func TestStrictPublishConvergesItsPendingVolumeFirst(t *testing.T) {
	setStartupTimings(t, 20*time.Millisecond, time.Hour)
	ctx := context.Background()
	d, client, _ := newGateDriver(t, 1)

	require.True(t, d.acquireOperationLock(volumeLockKey("restart-0")))
	_, err := d.reconcilePublishedAttachmentsFor(ctx, nil)
	require.Error(t, err)
	d.releaseOperationLock(volumeLockKey("restart-0"))
	dataset, err := client.DatasetGet(ctx, "pool/parent/restart-0")
	require.NoError(t, err)
	require.Empty(t, mustStoredRecords(t, d, dataset), "startup has not converged the volume")
	require.True(t, d.startupGateStillPending("restart-0"))

	require.NoError(t, publishThroughInterceptor(d, "restart-0", gateNode(t, "worker-restart-0", "192.0.2.11")))
	assert.False(t, d.startupGateStillPending("restart-0"))
	dataset, err = client.DatasetGet(ctx, "pool/parent/restart-0")
	require.NoError(t, err)
	assert.Contains(t, mustStoredRecords(t, d, dataset), publicationPropertyKey("worker-restart-0"))

	// The startup retry finds it converged and leaves it alone.
	failed, err := d.reconcilePublishedAttachmentsFor(ctx, map[string]struct{}{"pool/parent/restart-0": {}})
	require.NoError(t, err)
	assert.Nil(t, failed)
}

// A drain overlapping a restart: the volume moves from the node the startup
// snapshot saw to another. The publish converges the volume from the
// snapshot (whose attachment is gone by now) and grants the new node.
func TestStrictPublishAfterADrainPastTheSnapshot(t *testing.T) {
	setStartupTimings(t, 20*time.Millisecond, time.Hour)
	ctx := context.Background()
	newNode := &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-new"},
		Spec:       storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: gateNode(t, "worker-new", "192.0.2.31")}}},
	}
	d, client, kube := newGateDriver(t, 1, newNode)
	require.True(t, d.acquireOperationLock(volumeLockKey("restart-0")))
	_, err := d.reconcilePublishedAttachmentsFor(ctx, nil)
	require.Error(t, err)
	d.releaseOperationLock(volumeLockKey("restart-0"))

	require.NoError(t, kube.StorageV1().VolumeAttachments().Delete(ctx, "va-restart-0", metav1.DeleteOptions{}))
	require.NoError(t, publishThroughInterceptor(d, "restart-0", gateNode(t, "worker-new", "192.0.2.31")))
	dataset, err := client.DatasetGet(ctx, "pool/parent/restart-0")
	require.NoError(t, err)
	records := mustStoredRecords(t, d, dataset)
	assert.Contains(t, records, publicationPropertyKey("worker-new"))
	assert.NotContains(t, records, publicationPropertyKey("worker-restart-0"))
}

// A pending volume whose convergence fails is never published: the publish
// returns Unavailable and grants nothing, so no node is granted before its
// volume's fence is right.
func TestStrictPublishOfAPendingVolumeThatCannotConvergeGrantsNothing(t *testing.T) {
	setStartupTimings(t, 20*time.Millisecond, time.Hour)
	ctx := context.Background()
	d, client, kube := newGateDriver(t, 1)
	require.True(t, d.acquireOperationLock(volumeLockKey("restart-0")))
	_, err := d.reconcilePublishedAttachmentsFor(ctx, nil)
	require.Error(t, err)
	d.releaseOperationLock(volumeLockKey("restart-0"))

	// The attached node's identity no longer carries an address: strict
	// fencing cannot build its NFS grant, so the volume cannot converge.
	csiNode, err := kube.StorageV1().CSINodes().Get(ctx, "worker-restart-0", metav1.GetOptions{})
	require.NoError(t, err)
	noAddress, err := encodeNodeIdentity(NodeIdentity{Name: "worker-restart-0"})
	require.NoError(t, err)
	csiNode.Spec.Drivers[0].NodeID = noAddress
	_, err = kube.StorageV1().CSINodes().Update(ctx, csiNode, metav1.UpdateOptions{})
	require.NoError(t, err)

	err = publishThroughInterceptor(d, "restart-0", gateNode(t, "worker-other", "192.0.2.41"))
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.True(t, d.startupGateStillPending("restart-0"))
	dataset, err := client.DatasetGet(ctx, "pool/parent/restart-0")
	require.NoError(t, err)
	assert.Empty(t, mustStoredRecords(t, d, dataset), "nothing was granted")
	share, err := client.NFSShareFindByPath(ctx, dataset.Mountpoint)
	require.NoError(t, err)
	assert.NotContains(t, share.Hosts, "192.0.2.41")
}

// Until a startup pass has taken its snapshot, no publish goes through.
func TestStrictPublishBeforeTheSnapshotIsRefused(t *testing.T) {
	d, client, _ := newGateDriver(t, 0)
	startupNFSVolumeShare(t, client, "fresh")
	err := publishThroughInterceptor(d, "fresh", gateNode(t, "worker-fresh", "192.0.2.21"))
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

// A publish that converged a volume from one pass's snapshot does not settle
// the entry a later full pass put in its place: that snapshot has not been
// converged yet.
func TestStartupGateSettleKeepsALaterPassEntry(t *testing.T) {
	d := &Driver{}
	first := &startupFencingVolume{volumeID: "pvc-a"}
	d.startupGateTrack(map[string]*startupFencingVolume{"pvc-a": first}, []string{"pvc-a"}, true)
	second := &startupFencingVolume{volumeID: "pvc-a"}
	d.startupGateTrack(map[string]*startupFencingVolume{"pvc-a": second}, []string{"pvc-a"}, true)

	d.startupGateSettle("pvc-a", first)
	assert.True(t, d.startupGateStillPending("pvc-a"), "the later pass's entry is still pending")
	d.startupGateSettle("pvc-a", second)
	assert.False(t, d.startupGateStillPending("pvc-a"))
}
