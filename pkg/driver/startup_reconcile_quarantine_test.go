package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// A quarantined volume's stale record can go without the revoke's signal (an
// operator removes it, or the revoke finds it already gone). The periodic
// stale-record sweep notices the record is gone and re-runs that volume.
func TestStartupQuarantineIsHealedByTheSweepWhenTheStaleRecordGoesWithoutASignal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := truenas.NewMockClient()
	objects, _ := staleRecordVolume(t, nil, "q1")
	kube := kubernetesfake.NewSimpleClientset(objects...)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(64))
	staleRecordNFSVolume(t, client, "q1", "worker-gone-1")

	d.ready.Store(false)
	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)
	require.Eventually(t, d.ready.Load, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, d.startupQuarantineCount())

	sweep := func() {
		dataset, err := client.DatasetGet(ctx, "pool/parent/q1")
		require.NoError(t, err)
		// The sweep's own work needs Kubernetes state; with none it returns
		// early, but still checks the quarantines against the listing.
		d.reconcileStalePublicationRecords(ctx, []*truenas.Dataset{dataset}, nil, time.Now())
	}
	sweep()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, d.startupQuarantineCount(), "a stale record still in place keeps the quarantine")

	dataset, err := client.DatasetGet(ctx, "pool/parent/q1")
	require.NoError(t, err)
	require.NoError(t, d.publications().remove(ctx, dataset.Name, dataset, []string{publicationPropertyKey("worker-gone-1")}))
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, d.startupQuarantineCount(), "nothing re-runs the volume before the sweep")

	sweep()
	require.Eventually(t, func() bool { return d.startupQuarantineCount() == 0 }, 3*time.Second, 10*time.Millisecond,
		"the sweep did not re-run the quarantined volume")
	dataset, err = client.DatasetGet(ctx, "pool/parent/q1")
	require.NoError(t, err)
	records, err := storedPublicationRecords(d, dataset)
	require.NoError(t, err)
	assert.Contains(t, records, publicationPropertyKey("worker-q1"))
}

// Once the loop has returned, a re-run request is dropped rather than kept in
// a pending set nothing will ever take.
func TestStartupReconcileRequestAfterTheLoopExitedIsDropped(t *testing.T) {
	client := truenas.NewMockClient()
	kube := kubernetesfake.NewSimpleClientset([]runtime.Object{}...)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(64))
	d.ready.Store(false)
	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)
	require.Eventually(t, func() bool {
		d.startupReconcileTargetsMu.Lock()
		defer d.startupReconcileTargetsMu.Unlock()
		return d.startupReconcileExited
	}, 3*time.Second, 10*time.Millisecond, "a converged strict loop exits")

	d.requestStartupAttachmentReconcile("pool/parent/late")
	assert.Empty(t, d.takeStartupReconcileTargets())
}

func TestStartupErrOnlyBusy(t *testing.T) {
	busy := fmt.Errorf("startup reconcile volume a: %w", errStartupVolumeBusy)
	other := errors.New("startup fencing for volume b has not converged")
	assert.True(t, startupErrOnlyBusy(busy))
	assert.True(t, startupErrOnlyBusy(errors.Join(busy, busy)))
	assert.True(t, startupErrOnlyBusy(fmt.Errorf("pass: %w", errors.Join(busy))))
	assert.False(t, startupErrOnlyBusy(errors.Join(busy, other)))
	assert.False(t, startupErrOnlyBusy(other))
	assert.False(t, startupErrOnlyBusy(errors.Join()))
	assert.False(t, startupErrOnlyBusy(nil))
}

// The stale-record revoke signals the loop only after releasing the volume
// lock, so the targeted re-run it wakes never finds the lock still held by the
// revoke itself.
func TestStaleRecordRevokeSignalsAfterReleasingTheVolumeLock(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	objects, _ := staleRecordVolume(t, nil, "q1")
	kube := kubernetesfake.NewSimpleClientset(objects...)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(64))
	d.eventRecorder.dynamicClient = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			volumeSnapshotContentGVR: "VolumeSnapshotContentList",
			volumeSnapshotGVR:        "VolumeSnapshotList",
		})
	staleRecordNFSVolume(t, client, "q1", "worker-gone-1")

	var lockFreeAtSignal []bool
	startupReconcileRequestedHook = func(string) {
		free := d.acquireOperationLock(volumeLockKey("q1"))
		if free {
			d.releaseOperationLock(volumeLockKey("q1"))
		}
		lockFreeAtSignal = append(lockFreeAtSignal, free)
	}
	t.Cleanup(func() { startupReconcileRequestedHook = nil })

	dataset, err := client.DatasetGet(ctx, "pool/parent/q1")
	require.NoError(t, err)
	key := publicationPropertyKey("worker-gone-1")
	revoked, err := d.revokeStalePublicationRecord(ctx, dataset.Name, "q1", key, mustStoredRecords(t, d, dataset)[key], 1)
	require.NoError(t, err)
	require.True(t, revoked)
	assert.Equal(t, []bool{true}, lockFreeAtSignal)
}

// A targeted re-run that finds its volume busy keeps strict readiness, the
// volume's quarantine and its gauge; the retry converges it once the lock is
// free.
func TestStartupBusyTargetedPassKeepsReadinessAndTheQuarantine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := truenas.NewMockClient()
	objects, _ := staleRecordVolume(t, nil, "q1")
	kube := kubernetesfake.NewSimpleClientset(objects...)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(64))
	staleRecordNFSVolume(t, client, "q1", "worker-gone-1")

	d.ready.Store(false)
	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)
	require.Eventually(t, d.ready.Load, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, d.startupQuarantineCount())

	dataset, err := client.DatasetGet(ctx, "pool/parent/q1")
	require.NoError(t, err)
	require.NoError(t, d.publications().remove(ctx, dataset.Name, dataset, []string{publicationPropertyKey("worker-gone-1")}))
	require.True(t, d.acquireOperationLock(volumeLockKey("q1")))
	d.requestStartupAttachmentReconcile(dataset.Name)
	time.Sleep(300 * time.Millisecond)
	assert.True(t, d.ready.Load(), "a busy targeted pass dropped cluster-wide readiness")
	assert.Equal(t, 1, d.startupQuarantineCount(), "a busy targeted pass cleared the quarantine")
	assert.Equal(t, float64(1), testutil.ToFloat64(startupFencingUnconvergedVolumes.WithLabelValues("q1")))

	d.releaseOperationLock(volumeLockKey("q1"))
	require.Eventually(t, func() bool { return d.startupQuarantineCount() == 0 }, 10*time.Second, 20*time.Millisecond,
		"the retry did not converge the volume")
	assert.True(t, d.ready.Load())
}
