package driver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// TestReconcileSpentRestoreDefersIncompleteRestorePVCs proves the classification
// gate: a source PVC that EXISTS in Pending or Lost (an incomplete/stalled
// restore) exempts the snapshot — it is not classified, and an operator-visible
// skip reason is recorded. An absent PVC still classifies as spent and a Bound
// PVC stays exempt (both existing behaviors preserved).
func TestReconcileSpentRestoreDefersIncompleteRestorePVCs(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	dynamicObjects := []runtime.Object{
		reconcileVolumeSnapshot("storage", "volsync-pending-dst-dest", "pending-source", "pending-content", old),
		reconcileSnapshotContent("pending-content", "storage", "volsync-pending-dst-dest", "pending-handle", "csi.scale.io"),
		reconcileVolumeSnapshot("storage", "volsync-lost-dst-dest", "lost-source", "lost-content", old),
		reconcileSnapshotContent("lost-content", "storage", "volsync-lost-dst-dest", "lost-handle", "csi.scale.io"),
		reconcileVolumeSnapshot("storage", "volsync-absent-dst-dest", "absent-source", "absent-content", old),
		reconcileSnapshotContent("absent-content", "storage", "volsync-absent-dst-dest", "absent-handle", "csi.scale.io"),
		reconcileVolumeSnapshot("storage", "volsync-bound-dst-dest", "bound-source", "bound-content", old),
		reconcileSnapshotContent("bound-content", "storage", "volsync-bound-dst-dest", "bound-handle", "csi.scale.io"),
	}
	coreObjects := []runtime.Object{
		reconcilePVC("storage", "pending-source", corev1.ClaimPending),
		reconcilePVC("storage", "lost-source", corev1.ClaimLost),
		// absent-source intentionally has no PVC.
		reconcilePVC("storage", "bound-source", corev1.ClaimBound),
	}

	d, backend := newReconcileTestDriver(t, true, coreObjects, dynamicObjects)
	for _, handle := range []string{"pending-handle", "lost-handle", "absent-handle", "bound-handle"} {
		addReconcileSnapshot(t, backend, "restore-source", handle, old.Time, true, 1)
	}

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)

	// Only the absent-PVC snapshot classifies as spent.
	require.Len(t, report.SpentRestoreSnapshots, 1)
	assert.Equal(t, "volsync-absent-dst-dest", report.SpentRestoreSnapshots[0].Name)
	assert.True(t, report.SpentRestoreSnapshots[0].SourcePVCWasMissing)

	// The deferred restores surface operator-visible skip reasons.
	skipReasons := map[string]string{}
	for _, skip := range report.SkippedDeletes {
		skipReasons[skip.ID] = skip.Reason
	}
	assert.Contains(t, skipReasons["storage/volsync-pending-dst-dest"], string(corev1.ClaimPending))
	assert.Contains(t, skipReasons["storage/volsync-lost-dst-dest"], string(corev1.ClaimLost))
	assert.NotContains(t, skipReasons, "storage/volsync-absent-dst-dest")
	assert.NotContains(t, skipReasons, "storage/volsync-bound-dst-dest")
}

// TestReconcileSpentRestoreRevalidationDefersPendingPVC proves the same discipline
// in the delete path: a snapshot classified as spent (source PVC absent at
// detection) must NOT be reaped if a Pending PVC appears before the live
// revalidation — the restore restarted and its backend snapshot is still needed.
func TestReconcileSpentRestoreRevalidationDefersPendingPVC(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	dynamicObjects := []runtime.Object{
		reconcileVolumeSnapshot("storage", "volsync-app-dst-dest", "flapping-source", "spent-content", old),
		reconcileSnapshotContent("spent-content", "storage", "volsync-app-dst-dest", "spent-handle", "csi.scale.io"),
	}
	// No PVC at detection: classification sees the source PVC as absent → spent.
	d, backend := newReconcileTestDriver(t, true, nil, dynamicObjects)
	addReconcileSnapshot(t, backend, "restore-source", "spent-handle", old.Time, true, 1)

	// Between classification and revalidation a Pending PVC appears (restore
	// restarted). The live revalidation Get must veto the reap.
	clientset, _, err := d.kubernetesReconcileClients()
	require.NoError(t, err)
	fakeClient := clientset.(*kubernetesfake.Clientset)
	fakeClient.PrependReactor("get", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, reconcilePVC("storage", "flapping-source", corev1.ClaimPending), nil
	})

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)

	require.Len(t, report.SpentRestoreSnapshots, 1, "absent at detection: still classified")
	assert.Empty(t, report.DeletedSpentRestoreObjects, "a Pending PVC at revalidation must veto the reap")

	found := false
	for _, skip := range report.SkippedDeletes {
		if skip.ID == "storage/volsync-app-dst-dest" && strings.Contains(skip.Reason, string(corev1.ClaimPending)) {
			found = true
		}
	}
	assert.True(t, found, "revalidation must record a skip reason naming the Pending PVC")
}
