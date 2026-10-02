package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// setStartupTimings shortens the startup loop's lock wait and backoff for one
// test.
func setStartupTimings(t *testing.T, lockWait, backoff time.Duration) {
	t.Helper()
	oldWait, oldInitial, oldMax := startupVolumeLockWait, startupReconcileInitialBackoff, startupReconcileMaxBackoff
	startupVolumeLockWait, startupReconcileInitialBackoff, startupReconcileMaxBackoff = lockWait, backoff, backoff
	t.Cleanup(func() {
		startupVolumeLockWait, startupReconcileInitialBackoff, startupReconcileMaxBackoff = oldWait, oldInitial, oldMax
	})
}

// A startup worker that finds a volume locked by a live CSI operation waits
// for the lock, so one brief publish no longer fails the whole pass.
func TestStartupWorkerWaitsForABusyVolumeLock(t *testing.T) {
	setStartupTimings(t, 5*time.Second, time.Second)
	ctx := context.Background()
	client := truenas.NewMockClient()
	kube := kubernetesfake.NewSimpleClientset(startupNFSVolumes(t, client, 2)...)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(64))

	require.True(t, d.acquireOperationLock(volumeLockKey("restart-1")))
	go func() {
		time.Sleep(200 * time.Millisecond)
		d.releaseOperationLock(volumeLockKey("restart-1"))
	}()
	failed, err := d.reconcilePublishedAttachmentsFor(ctx, nil)
	require.NoError(t, err, "a lock released within the wait must not fail the pass")
	assert.Nil(t, failed)
	dataset, err := client.DatasetGet(ctx, "pool/parent/restart-1")
	require.NoError(t, err)
	assert.Contains(t, mustStoredRecords(t, d, dataset), publicationPropertyKey("worker-restart-1"))
}

// The wait is bounded: a lock held past it fails that volume, and only that
// volume, as busy.
func TestStartupWorkerLockWaitIsBounded(t *testing.T) {
	setStartupTimings(t, 50*time.Millisecond, time.Second)
	ctx := context.Background()
	client := truenas.NewMockClient()
	kube := kubernetesfake.NewSimpleClientset(startupNFSVolumes(t, client, 3)...)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(64))

	require.True(t, d.acquireOperationLock(volumeLockKey("restart-1")))
	defer d.releaseOperationLock(volumeLockKey("restart-1"))
	start := time.Now()
	failed, err := d.reconcilePublishedAttachmentsFor(ctx, nil)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 3*time.Second)
	assert.True(t, errors.Is(err, errStartupVolumeBusy))
	assert.True(t, startupErrOnlyBusy(err))
	assert.Equal(t, map[string]struct{}{"pool/parent/restart-1": {}}, failed)
}

// After a pass fails on one volume, the retry re-runs that volume only; the
// volumes that converged are not reconciled again, and readiness latches once
// the failed one converges.
func TestStartupRetryReRunsOnlyTheFailedVolumes(t *testing.T) {
	setStartupTimings(t, 20*time.Millisecond, 100*time.Millisecond)
	client := truenas.NewMockClient()
	kube := kubernetesfake.NewSimpleClientset(startupNFSVolumes(t, client, 3)...)
	gets := countVAGets(kube)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(64))
	d.runController = true

	require.True(t, d.acquireOperationLock(volumeLockKey("restart-1")))
	d.ready.Store(false)
	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)
	require.Eventually(t, func() bool { return gets.get("va-restart-0") == 1 && gets.get("va-restart-2") == 1 },
		3*time.Second, 10*time.Millisecond)
	time.Sleep(300 * time.Millisecond) // several retries against the held lock
	assert.False(t, d.ready.Load(), "strict readiness waits for every volume")
	d.releaseOperationLock(volumeLockKey("restart-1"))

	require.Eventually(t, d.ready.Load, 3*time.Second, 10*time.Millisecond)
	assert.Equal(t, 1, gets.get("va-restart-0"), "a converged volume is not re-run by the retry")
	assert.Equal(t, 1, gets.get("va-restart-2"), "a converged volume is not re-run by the retry")
	assert.Equal(t, 1, gets.get("va-restart-1"), "the busy volume is reconciled once, after its lock is free")
}
