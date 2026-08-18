package driver

import (
	"context"
	"fmt"
	"path"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// SpentRestoreSnapshot describes a VolSync restore-destination snapshot whose
// intermediate source PVC is no longer Bound.
type SpentRestoreSnapshot struct {
	Namespace           string
	Name                string
	ContentName         string
	SnapshotHandle      string
	SourcePVC           string
	SourcePVCPhase      corev1.PersistentVolumeClaimPhase
	CreationTime        time.Time
	Age                 time.Duration
	BackendSnapshotID   string
	ClassifiedAt        time.Time
	SourcePVCWasMissing bool
}

// spentRestoreDeferredPVCPhase reports whether a source PVC that EXISTS in the
// given phase indicates an incomplete or stalled restore whose backend snapshot
// must NOT be reaped. Pending and Lost (and any unknown/empty phase) defer
// conservatively; Bound does not (the restore completed) and neither does
// "Released" (the restore's PV was already let go — torn down), so in both those
// cases the snapshot is genuinely spent. A missing PVC is handled by the caller.
func spentRestoreDeferredPVCPhase(phase corev1.PersistentVolumeClaimPhase) bool {
	switch phase {
	case corev1.ClaimBound:
		return false
	case corev1.ClaimPending, corev1.ClaimLost:
		return true
	}
	// "Released" is a PV phase sometimes observed on a lingering PVC after its PV
	// was released; it means the restore is torn down, so the snapshot is spent.
	if phase == corev1.PersistentVolumeClaimPhase("Released") {
		return false
	}
	return true
}

func (d *Driver) classifySpentRestoreSnapshots(
	ctx context.Context,
	now time.Time,
	state *kubernetesReconcileState,
	snapshots []*truenas.Snapshot,
	report *ReconcileReport,
) []SpentRestoreSnapshot {
	if state == nil {
		return nil
	}
	// Resolve backend snapshots against the pass's already-fetched managed-snapshot
	// slice instead of issuing one SnapshotFindByName/SnapshotGet per candidate —
	// the short-name path previously did a FULL recursive snapshot-set transfer per
	// candidate. Classification is read-only detection; the pre-delete guard
	// (revalidateSpentRestoreSnapshot) still re-fetches the backend snapshot live,
	// so a snapshot absent from this pass's listing is simply reconsidered next pass
	// rather than mis-reaped. Index by full ID (dataset@snap) and by short name.
	byID := make(map[string]*truenas.Snapshot, len(snapshots))
	byShortName := make(map[string]*truenas.Snapshot, len(snapshots))
	for _, snap := range snapshots {
		if snap == nil {
			continue
		}
		byID[snap.ID] = snap
		if shortName := snapshotShortName(snap); shortName != "" {
			if _, exists := byShortName[shortName]; !exists {
				byShortName[shortName] = snap
			}
		}
	}
	spent := make([]SpentRestoreSnapshot, 0)
	for i := range state.volumeSnapshots {
		snapshot := &state.volumeSnapshots[i]
		matched, _ := path.Match("volsync-*-dst-dest*", snapshot.GetName())
		if !matched {
			continue
		}
		content, ok := state.snapshotContentsByRef[namespacedName(snapshot.GetNamespace(), snapshot.GetName())]
		if !ok {
			boundContent, found, nestedErr := unstructured.NestedString(snapshot.Object, "status", "boundVolumeSnapshotContentName")
			if nestedErr != nil {
				d.recordReconcileObjectFailure("spent_restore_classification", snapshot.GetNamespace()+"/"+snapshot.GetName(), nestedErr)
				continue
			}
			if !found || boundContent == "" {
				continue
			}
			content, ok = state.snapshotContentsByName[boundContent]
		}
		if !ok {
			continue
		}
		sourcePVC, found, nestedErr := unstructured.NestedString(snapshot.Object, "spec", "source", "persistentVolumeClaimName")
		if nestedErr != nil {
			d.recordReconcileObjectFailure("spent_restore_classification", snapshot.GetNamespace()+"/"+snapshot.GetName(), nestedErr)
			continue
		}
		if !found || sourcePVC == "" {
			continue
		}
		pvc, exists := state.pvcs[namespacedName(snapshot.GetNamespace(), sourcePVC)]
		if exists {
			if pvc.Status.Phase == corev1.ClaimBound {
				// Restore completed: the backend snapshot is spent and reclaimable.
				continue
			}
			if spentRestoreDeferredPVCPhase(pvc.Status.Phase) {
				// A PVC that exists in Pending, Lost, or an unknown phase means the
				// restore is incomplete or stalled. The backend snapshot still backs
				// it, so classifying it as spent could destroy an in-flight restore.
				// Defer with an operator-visible reason. (A Released PVC means the
				// restore's PV was already released — torn down — so it stays spent.)
				klog.V(2).Infof("Orphan reconcile: deferring spent-restore snapshot %s/%s: source PVC %s is %s",
					snapshot.GetNamespace(), snapshot.GetName(), sourcePVC, pvc.Status.Phase)
				d.recordReconcileSkip(report, "spent-restore-snapshot", namespacedName(snapshot.GetNamespace(), snapshot.GetName()),
					fmt.Sprintf("source PVC %s is %s (restore incomplete)", sourcePVC, pvc.Status.Phase))
				continue
			}
		}
		// Resolve against the pass's in-memory snapshot index — no per-candidate
		// backend round trip (see the maps built above). A "@" handle matches a full
		// snapshot ID; a bare short name matches byShortName. A dataset-qualified
		// handle additionally falls back to its SHORT-NAME component, because a
		// clone promotion migrates snapshots to the promoted dataset while the
		// content keeps the original handle — the same cross-format net the
		// orphan classifier uses (short names are globally unique among CSI
		// snapshots, so this cannot resolve to a different snapshot).
		backendSnapshot := byID[content.snapshotHandle]
		if backendSnapshot == nil {
			backendSnapshot = byShortName[content.snapshotHandle]
		}
		if backendSnapshot == nil {
			if shortName := snapshotHandleShortName(content.snapshotHandle); shortName != "" && shortName != content.snapshotHandle {
				backendSnapshot = byShortName[shortName]
			}
		}
		if backendSnapshot == nil || !isCSISnapshot(backendSnapshot) {
			continue
		}
		createdAt := snapshot.GetCreationTimestamp().Time
		backendCreatedAt := time.Unix(backendSnapshot.GetCreationTime(), 0)
		if createdAt.IsZero() || backendSnapshot.GetCreationTime() <= 0 {
			d.recordReconcileObjectFailure("spent_restore_classification", backendSnapshot.ID,
				fmt.Errorf("snapshot creation time is unavailable"))
			continue
		}
		// TrueNAS 26.0 cannot mutate user properties on an existing snapshot:
		// zfs.resource.snapshot.update is absent and pool.snapshot.update silently
		// drops them. Use the later of the Kubernetes and backend creation times as
		// a durable, monotonic, write-free age origin. Clock skew can only delay GC.
		// Classification remains immediate for observability; guarded deletion
		// revalidates this origin and applies minOrphanAge immediately before GC.
		ageOrigin := laterTime(createdAt, backendCreatedAt)
		age := now.Sub(ageOrigin)
		phase := corev1.PersistentVolumeClaimPhase("")
		if exists {
			phase = pvc.Status.Phase
		}
		spent = append(spent, SpentRestoreSnapshot{
			Namespace:           snapshot.GetNamespace(),
			Name:                snapshot.GetName(),
			ContentName:         content.name,
			SnapshotHandle:      content.snapshotHandle,
			SourcePVC:           sourcePVC,
			SourcePVCPhase:      phase,
			CreationTime:        createdAt,
			Age:                 age,
			BackendSnapshotID:   backendSnapshot.ID,
			ClassifiedAt:        ageOrigin,
			SourcePVCWasMissing: !exists,
		})
	}
	return spent
}

func (d *Driver) revalidateSpentRestoreSnapshot(
	ctx context.Context,
	clientset kubernetes.Interface,
	dynamicClient dynamic.Interface,
	detected SpentRestoreSnapshot,
	minOrphanAge time.Duration,
) (spent SpentRestoreSnapshot, safe bool, reason string) {
	snapshot, err := dynamicClient.Resource(volumeSnapshotGVR).Namespace(detected.Namespace).Get(
		ctx, detected.Name, metav1.GetOptions{},
	)
	if err != nil {
		return SpentRestoreSnapshot{}, false, fmt.Sprintf("VolumeSnapshot revalidation failed: %v", err)
	}
	sourcePVC, found, nestedErr := unstructured.NestedString(
		snapshot.Object, "spec", "source", "persistentVolumeClaimName",
	)
	if nestedErr != nil || !found || sourcePVC == "" || sourcePVC != detected.SourcePVC {
		return SpentRestoreSnapshot{}, false, "VolumeSnapshot source PVC identity changed"
	}
	contentName, found, nestedErr := unstructured.NestedString(
		snapshot.Object, "status", "boundVolumeSnapshotContentName",
	)
	if nestedErr != nil || !found || contentName == "" || contentName != detected.ContentName {
		return SpentRestoreSnapshot{}, false, "VolumeSnapshotContent binding changed"
	}
	content, err := dynamicClient.Resource(volumeSnapshotContentGVR).Get(ctx, contentName, metav1.GetOptions{})
	if err != nil {
		return SpentRestoreSnapshot{}, false, fmt.Sprintf("VolumeSnapshotContent revalidation failed: %v", err)
	}
	driverName, _, driverErr := unstructured.NestedString(content.Object, "spec", "driver")
	referenceNamespace, _, namespaceErr := unstructured.NestedString(
		content.Object, "spec", "volumeSnapshotRef", "namespace",
	)
	referenceName, _, nameErr := unstructured.NestedString(
		content.Object, "spec", "volumeSnapshotRef", "name",
	)
	if driverErr != nil || namespaceErr != nil || nameErr != nil || driverName != d.name ||
		referenceNamespace != detected.Namespace || referenceName != detected.Name {
		return SpentRestoreSnapshot{}, false, "VolumeSnapshotContent is no longer bound to this driver's restore snapshot"
	}
	handle, found, handleErr := unstructured.NestedString(content.Object, "status", "snapshotHandle")
	if handleErr != nil || !found || handle == "" || handle != detected.SnapshotHandle {
		return SpentRestoreSnapshot{}, false, "VolumeSnapshotContent handle changed"
	}

	pvc, err := clientset.CoreV1().PersistentVolumeClaims(detected.Namespace).Get(
		ctx, sourcePVC, metav1.GetOptions{},
	)
	missing := apierrors.IsNotFound(err)
	if err != nil && !missing {
		return SpentRestoreSnapshot{}, false, fmt.Sprintf("source PVC revalidation failed: %v", err)
	}
	if err == nil {
		if pvc.Status.Phase == corev1.ClaimBound {
			return SpentRestoreSnapshot{}, false, "source PVC became Bound during revalidation"
		}
		if spentRestoreDeferredPVCPhase(pvc.Status.Phase) {
			// A PVC that exists in Pending, Lost, or an unknown phase means the
			// restore is incomplete or stalled; the backend snapshot still backs it
			// and must not be reaped. (Released stays spent: the PV was let go.)
			return SpentRestoreSnapshot{}, false, fmt.Sprintf("source PVC %s is %s (restore incomplete)", sourcePVC, pvc.Status.Phase)
		}
	}
	createdAt := snapshot.GetCreationTimestamp().Time
	if createdAt.IsZero() || !createdAt.Equal(detected.CreationTime) {
		return SpentRestoreSnapshot{}, false, "snapshot creation identity changed or has not exceeded the minimum orphan age"
	}
	backendSnapshot, backendErr := d.findBackendSnapshotForHandle(ctx, handle)
	if backendErr != nil || backendSnapshot == nil || backendSnapshot.ID != detected.BackendSnapshotID {
		return SpentRestoreSnapshot{}, false, fmt.Sprintf("backend spent-restore snapshot revalidation failed: %v", backendErr)
	}
	if backendSnapshot.GetCreationTime() <= 0 {
		return SpentRestoreSnapshot{}, false, "backend spent-restore snapshot creation time is unavailable"
	}
	// This is intentionally write-free on TrueNAS 26.0; existing snapshots have
	// no API that can persist a user property. Recompute the same conservative
	// age origin and require both object identities to remain unchanged.
	ageOrigin := laterTime(createdAt, time.Unix(backendSnapshot.GetCreationTime(), 0))
	if !ageOrigin.Equal(detected.ClassifiedAt) {
		return SpentRestoreSnapshot{}, false, "spent-restore snapshot creation identity changed"
	}
	age := time.Since(ageOrigin)
	if age <= minOrphanAge {
		return SpentRestoreSnapshot{}, false, "spent-restore snapshot creation age has not exceeded the minimum orphan age"
	}
	phase := corev1.PersistentVolumeClaimPhase("")
	if pvc != nil {
		phase = pvc.Status.Phase
	}
	return SpentRestoreSnapshot{
		Namespace:           detected.Namespace,
		Name:                detected.Name,
		ContentName:         contentName,
		SnapshotHandle:      handle,
		SourcePVC:           sourcePVC,
		SourcePVCPhase:      phase,
		CreationTime:        createdAt,
		Age:                 age,
		BackendSnapshotID:   backendSnapshot.ID,
		ClassifiedAt:        ageOrigin,
		SourcePVCWasMissing: missing,
	}, true, ""
}
