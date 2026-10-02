package driver

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// setAttachLockWait shortens attachLockWait for one test.
func setAttachLockWait(t *testing.T, wait time.Duration) {
	t.Helper()
	old := attachLockWait
	attachLockWait = wait
	t.Cleanup(func() { attachLockWait = old })
}

func TestLockModeCompatibility(t *testing.T) {
	modes := []lockMode{lockExclusive, lockAttach, lockData}
	compatible := map[[2]lockMode]bool{
		{lockAttach, lockData}: true,
		{lockData, lockAttach}: true,
	}
	for _, held := range modes {
		for _, wanted := range modes {
			var table operationLockTable
			ok, _ := table.tryAcquire("volume:v", held)
			require.True(t, ok)
			got, _ := table.tryAcquire("volume:v", wanted)
			assert.Equal(t, compatible[[2]lockMode{held, wanted}], got, "held %s, wanted %s", held, wanted)
		}
	}
}

// A release of one shared holder leaves the other holding: an exclusive
// taker still waits for it.
func TestLockSharedReleaseKeepsTheOtherHolder(t *testing.T) {
	d := &Driver{}
	key := volumeLockKey("v")
	require.True(t, d.acquireOperationLockMode(key, lockAttach))
	require.True(t, d.acquireOperationLockMode(key, lockData))
	d.releaseOperationLockMode(key, lockAttach)
	assert.False(t, d.acquireOperationLock(key), "the data holder still holds the volume")
	assert.Equal(t, []string{key}, d.heldOperationLocks())
	// A release in a mode not held is a no-op.
	d.releaseOperationLock(key)
	assert.False(t, d.acquireOperationLock(key))
	d.releaseOperationLockMode(key, lockData)
	assert.True(t, d.acquireOperationLock(key))
	d.releaseOperationLock(key)
	assert.Empty(t, d.heldOperationLocks())
}

// The startup diff's lock watch sees a volume taken in a shared mode, both
// when it was already held as the watch began and when it is taken during
// the watch. Diff-first startup relies on it seeing every writer.
func TestLockWatchSeesSharedModes(t *testing.T) {
	d := &Driver{}
	require.True(t, d.acquireOperationLockMode(volumeLockKey("held-data"), lockData))
	watch := d.beginStartupLockWatch()
	defer d.endStartupLockWatch(watch)
	assert.True(t, watch.wasTouched(volumeLockKey("held-data")))
	require.True(t, d.acquireOperationLockMode(volumeLockKey("taken-attach"), lockAttach))
	assert.True(t, watch.wasTouched(volumeLockKey("taken-attach")))
	require.True(t, d.acquireOperationLockModeWait(context.Background(), volumeLockKey("taken-data"), lockData, time.Second))
	assert.True(t, watch.wasTouched(volumeLockKey("taken-data")))
	assert.False(t, watch.wasTouched(volumeLockKey("untouched")))
}

// gatedDatasetGetClient holds every DatasetGet of one dataset, after the
// first, until release: the first is the test's own setup read.
type gatedDatasetGetClient struct {
	*truenas.MockClient
	name    string
	entered chan struct{}
	release chan struct{}
	gets    atomic.Int32
	once    sync.Once
}

func (c *gatedDatasetGetClient) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	if name == c.name && c.release != nil {
		c.gets.Add(1)
		c.once.Do(func() { close(c.entered) })
		<-c.release
	}
	return c.MockClient.DatasetGet(ctx, name)
}

func lockTestPublishRequest(t *testing.T, volumeID, node string, mode csi.VolumeCapability_AccessMode_Mode) *csi.ControllerPublishVolumeRequest {
	t.Helper()
	nodeID, err := encodeNodeIdentity(NodeIdentity{Name: node, NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:" + node})
	require.NoError(t, err)
	return &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   nodeID,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
		},
		VolumeContext: map[string]string{"node_attach_driver": "nvmeof"},
	}
}

// newLockTestVolume is a strict NVMe-oF volume with its share, behind a
// client that can hold its DatasetGet.
func newLockTestVolume(t *testing.T, volumeID string) (*Driver, *gatedDatasetGetClient) {
	t.Helper()
	ctx := context.Background()
	h := newFencingTestHarness(t, FencingModeStrict, ShareTypeNVMeoF)
	datasetName := "pool/parent/" + volumeID
	ds, err := h.client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME", Volsize: testGiB})
	require.NoError(t, err)
	require.NoError(t, h.client.DatasetSetUserProperties(ctx, datasetName, map[string]string{PropManagedResource: "true"}))
	require.NoError(t, h.d.createNVMeoFShareForDataset(ctx, ds, datasetName, volumeID, true, true, nil))
	gated := &gatedDatasetGetClient{MockClient: h.client, name: datasetName}
	h.d.truenasClient = gated
	return h.d, gated
}

// A snapshot of a volume no longer waits out, or fails on, a publish of it:
// the publish holds the volume's lock attach-class, the snapshot data-class.
// M1 saw VolSync's snapshot fail Aborted against a publish this way.
func TestLockCreateSnapshotRunsWhileAPublishHoldsTheVolume(t *testing.T) {
	ctx := context.Background()
	d, gated := newLockTestVolume(t, "snap-vs-publish")
	gated.entered, gated.release = make(chan struct{}), make(chan struct{})

	publishErr := make(chan error, 1)
	go func() {
		_, err := d.ControllerPublishVolume(ctx, lockTestPublishRequest(t, "snap-vs-publish", "worker-a",
			csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER))
		publishErr <- err
	}()
	<-gated.entered // the publish holds the volume lock, mid-flight

	// The snapshot's own read of the volume must not be held.
	snapshotDone := make(chan error, 1)
	go func() {
		_, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "snap-1", SourceVolumeId: "snap-vs-publish"})
		snapshotDone <- err
	}()
	// The snapshot's DatasetGet is gated too: let exactly the snapshot pass
	// by releasing after it has entered, while the publish is still held.
	require.Eventually(t, func() bool { return gated.gets.Load() == 2 }, 5*time.Second, time.Millisecond,
		"the snapshot reached its read while the publish held the volume")
	close(gated.release)
	require.NoError(t, <-snapshotDone)
	require.NoError(t, <-publishErr)
}

// A publish no longer fails on a snapshot holding its volume.
func TestLockPublishRunsWhileASnapshotHoldsTheVolume(t *testing.T) {
	setAttachLockWait(t, 0)
	d, _ := newLockTestVolume(t, "publish-vs-snap")
	require.True(t, d.acquireOperationLockMode(volumeLockKey("publish-vs-snap"), lockData))
	defer d.releaseOperationLockMode(volumeLockKey("publish-vs-snap"), lockData)
	_, err := d.ControllerPublishVolume(context.Background(), lockTestPublishRequest(t, "publish-vs-snap", "worker-a",
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER))
	require.NoError(t, err)
	_, err = d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "publish-vs-snap", NodeId: lockTestPublishRequest(t, "publish-vs-snap", "worker-a",
			csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER).NodeId,
	})
	require.NoError(t, err)
}

// A publish waits for a conflicting holder that lets go within
// attachLockWait instead of returning Aborted at once: the attacher's
// backoff after an Aborted overshoots a short conflict by seconds.
func TestLockPublishWaitsForAConflictingHolder(t *testing.T) {
	setAttachLockWait(t, 5*time.Second)
	d, _ := newLockTestVolume(t, "publish-waits")
	key := volumeLockKey("publish-waits")
	require.True(t, d.acquireOperationLock(key)) // an expand, say
	go func() {
		time.Sleep(100 * time.Millisecond)
		d.releaseOperationLock(key)
	}()
	_, err := d.ControllerPublishVolume(context.Background(), lockTestPublishRequest(t, "publish-waits", "worker-a",
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER))
	require.NoError(t, err)
}

// The wait is bounded: a holder past attachLockWait still gets Aborted, for
// an unpublish as for a publish.
func TestLockAttachWaitIsBounded(t *testing.T) {
	setAttachLockWait(t, 50*time.Millisecond)
	d, _ := newLockTestVolume(t, "attach-bounded")
	key := volumeLockKey("attach-bounded")
	require.True(t, d.acquireOperationLockMode(key, lockAttach)) // another publish
	defer d.releaseOperationLockMode(key, lockAttach)
	request := lockTestPublishRequest(t, "attach-bounded", "worker-a", csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	start := time.Now()
	_, err := d.ControllerPublishVolume(context.Background(), request)
	assert.Equal(t, codes.Aborted, status.Code(err))
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	assert.Less(t, time.Since(start), 3*time.Second)
	_, err = d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{VolumeId: "attach-bounded", NodeId: request.NodeId})
	assert.Equal(t, codes.Aborted, status.Code(err))
}

// Two attach-class operations on one volume are still serialised: strict
// fencing decides each grant from what the previous one left. The second
// publish does not reach its read of the volume until the first is done.
func TestLockTwoPublishesOfAVolumeStaySerialised(t *testing.T) {
	setAttachLockWait(t, 5*time.Second)
	ctx := context.Background()
	d, gated := newLockTestVolume(t, "two-publishes")
	gated.entered, gated.release = make(chan struct{}), make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		_, err := d.ControllerPublishVolume(ctx, lockTestPublishRequest(t, "two-publishes", "worker-a",
			csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER))
		errs <- err
	}()
	<-gated.entered
	go func() {
		_, err := d.ControllerPublishVolume(ctx, lockTestPublishRequest(t, "two-publishes", "worker-b",
			csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER))
		errs <- err
	}()
	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, int32(1), gated.gets.Load(), "the second publish ran alongside the first")
	close(gated.release)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
}

// Exclusive operations still turn away at once while a shared holder holds
// the volume, and a snapshot still turns away while one does.
func TestLockExclusiveOperationsStillConflictWithSharedHolders(t *testing.T) {
	d, _ := newLockTestVolume(t, "exclusive")
	key := volumeLockKey("exclusive")
	require.True(t, d.acquireOperationLockMode(key, lockAttach))
	_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "exclusive"})
	assert.Equal(t, codes.Aborted, status.Code(err), "delete alongside a publish")
	_, err = d.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: "exclusive", CapacityRange: &csi.CapacityRange{RequiredBytes: 2 * testGiB}})
	assert.Equal(t, codes.Aborted, status.Code(err), "expand alongside a publish")
	d.releaseOperationLockMode(key, lockAttach)

	require.True(t, d.acquireOperationLockMode(key, lockData))
	_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "exclusive"})
	assert.Equal(t, codes.Aborted, status.Code(err), "delete alongside a snapshot")
	_, err = d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "snap-2", SourceVolumeId: "exclusive"})
	assert.Equal(t, codes.Aborted, status.Code(err), "two snapshots of one volume stay serialised")
	d.releaseOperationLockMode(key, lockData)

	require.True(t, d.acquireOperationLock(key))
	_, err = d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "snap-3", SourceVolumeId: "exclusive"})
	assert.Equal(t, codes.Aborted, status.Code(err), "a snapshot alongside an exclusive holder")
	d.releaseOperationLock(key)
}

// A publish that waited for its volume's lock resolves the node's identity
// after the wait: a node whose CSINode changed NQN while the publish waited
// is granted its current NQN, never the one read before the lock.
func TestLockPublishResolvesTheNodeIdentityAfterTheWait(t *testing.T) {
	setAttachLockWait(t, 5*time.Second)
	ctx := context.Background()
	d, gated := newLockTestVolume(t, "identity-after-wait")
	d.name = "csi.scale.io"
	csiNodeFor := func(nqn string) *storagev1.CSINode {
		nodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: nqn})
		require.NoError(t, err)
		return &storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}, Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: nodeID}},
		}}
	}
	kube := kubernetesfake.NewSimpleClientset(csiNodeFor("nqn.2014-08.org.nvmexpress:uuid:old"))
	d.eventRecorder = &EventRecorder{clientset: kube}

	key := volumeLockKey("identity-after-wait")
	require.True(t, d.acquireOperationLock(key))
	errs := make(chan error, 1)
	go func() {
		// A legacy plain node ID: the NQN comes from the CSINode.
		_, err := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: "identity-after-wait", NodeId: "worker-a",
			VolumeCapability: &csi.VolumeCapability{
				AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			},
			VolumeContext: map[string]string{"node_attach_driver": "nvmeof"},
		})
		errs <- err
	}()
	time.Sleep(100 * time.Millisecond) // the publish is waiting for the lock
	_, err := kube.StorageV1().CSINodes().Update(ctx, csiNodeFor("nqn.2014-08.org.nvmexpress:uuid:new"), metav1.UpdateOptions{})
	require.NoError(t, err)
	d.releaseOperationLock(key)
	require.NoError(t, <-errs)

	subsystemIDText, err := gated.MockClient.DatasetGetUserProperty(ctx, "pool/parent/identity-after-wait", PropNVMeoFSubsystemID)
	require.NoError(t, err)
	subsystemID, err := strconv.Atoi(subsystemIDText)
	require.NoError(t, err)
	associations, err := gated.MockClient.NVMeoFHostSubsysListBySubsystem(ctx, subsystemID)
	require.NoError(t, err)
	require.Len(t, associations, 1)
	assert.Equal(t, "nqn.2014-08.org.nvmexpress:uuid:new", associations[0].HostNQN)
}
