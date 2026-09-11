package driver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// TestStopStartupAttachmentReconcileBeforeStartPreventsLoopFromEverRunning is
// the regression test for the R1 shutdown race: startStartupAttachmentReconcile
// has the identical shape as startOrphanReconcile's C7 defect (a plain nil
// check on the CancelFunc field), but was not covered by that fix. Run() calls
// ensureNFSProtocols — a real TrueNAS network call — before
// startStartupAttachmentReconcile, so a Stop() landing while that call is in
// flight must be observed by startStartupAttachmentReconcile and prevent the
// loop from EVER launching, not merely fail to cancel a loop that started
// anyway. Before the fix, stopStartupAttachmentReconcile only canceled
// whatever startupReconcileCancel happened to already be assigned, with no
// memory that a stop was ever requested, so a Stop() that raced ahead of the
// assignment was silently lost and the subsequent Start() launched a loop that
// calls reconcileEncryptedUnlocks and reconcilePublishedAttachments — both of
// which WRITE backend fencing state — concurrently with the rest of shutdown,
// with no goroutine ever joined by Stop().
func TestStopStartupAttachmentReconcileBeforeStartPreventsLoopFromEverRunning(t *testing.T) {
	client := truenas.NewMockClient()
	kube := fake.NewSimpleClientset()
	d := newStartupGeometryDriver(client, kube, record.NewFakeRecorder(16))
	// No PersistentVolumes/VolumeAttachments are registered, so a launched loop
	// converges trivially on its very first pass and, in FencingModeStrict
	// (which newStartupGeometryDriver configures), immediately flips d.ready to
	// true. That makes d.ready an observable proxy for "did the loop launch".
	d.ready.Store(false)

	// Stop BEFORE Start ever runs — the observable analogue of a Stop() that
	// wins the race against startupReconcileCancel's assignment (e.g. landing
	// while ensureNFSProtocols is still in flight in Run()).
	d.stopStartupAttachmentReconcile()
	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)

	require.Never(t, d.ready.Load, 300*time.Millisecond, 10*time.Millisecond,
		"a startup fencing reconcile loop must never launch once Stop() has already been observed")
}
