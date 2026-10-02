package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// A quarantined volume's stale record can go without the revoke's signal (an
// operator removes it, or the revoke finds it already gone). The loop re-runs
// quarantined volumes on its own, so the volume still converges.
func TestStartupQuarantineConvergesWhenTheStaleRecordGoesWithoutASignal(t *testing.T) {
	original := startupQuarantineRecheckInterval
	startupQuarantineRecheckInterval = 20 * time.Millisecond
	t.Cleanup(func() { startupQuarantineRecheckInterval = original })

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

	dataset, err := client.DatasetGet(ctx, "pool/parent/q1")
	require.NoError(t, err)
	require.NoError(t, d.publications().remove(ctx, dataset.Name, dataset, []string{publicationPropertyKey("worker-gone-1")}))

	require.Eventually(t, func() bool { return d.startupQuarantineCount() == 0 }, 3*time.Second, 10*time.Millisecond,
		"the quarantined volume was never re-run")
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
