package driver

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type snapshotContentState struct {
	name           string
	snapshotHandle string
}

type kubernetesReconcileState struct {
	volumeHandles                  map[string]struct{}
	volumeHandlesByPV              map[string]string
	liveVolumeAttachments          map[string]struct{}
	volumeAttachmentCount          int
	snapshotHandles                map[string]struct{}
	handlelessSnapshotContentNames []string
	snapshotContentsByRef          map[string]snapshotContentState
	snapshotContentsByName         map[string]snapshotContentState
	volumeSnapshots                []unstructured.Unstructured
	pvcs                           map[string]*corev1.PersistentVolumeClaim
}

func (d *Driver) kubernetesReconcileClients() (
	clientset kubernetes.Interface,
	dynamicClient dynamic.Interface,
	err error,
) {
	if d.eventRecorder == nil || d.eventRecorder.clientset == nil || d.eventRecorder.dynamicClient == nil {
		return nil, nil, fmt.Errorf("kubernetes clients are unavailable; orphan reconcile requires in-cluster client access")
	}
	return d.eventRecorder.clientset, d.eventRecorder.dynamicClient, nil
}

func (d *Driver) loadKubernetesReconcileState(ctx context.Context, minOrphanAge time.Duration) (*kubernetesReconcileState, error) {
	clientset, dynamicClient, err := d.kubernetesReconcileClients()
	if err != nil {
		return nil, err
	}
	state := &kubernetesReconcileState{
		volumeHandles:          make(map[string]struct{}),
		volumeHandlesByPV:      make(map[string]string),
		liveVolumeAttachments:  make(map[string]struct{}),
		snapshotHandles:        make(map[string]struct{}),
		snapshotContentsByRef:  make(map[string]snapshotContentState),
		snapshotContentsByName: make(map[string]snapshotContentState),
		pvcs:                   make(map[string]*corev1.PersistentVolumeClaim),
	}

	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list PersistentVolumes: %w", err)
	}
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.name && pv.Spec.CSI.VolumeHandle != "" {
			state.volumeHandles[pv.Spec.CSI.VolumeHandle] = struct{}{}
			state.volumeHandlesByPV[pv.Name] = pv.Spec.CSI.VolumeHandle
		}
	}
	if d.config.Fencing.Enabled() {
		attachments, listErr := clientset.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
		if listErr != nil {
			return nil, fmt.Errorf("list VolumeAttachments: %w", listErr)
		}
		for i := range attachments.Items {
			attachment := &attachments.Items[i]
			if attachment.Spec.Attacher != d.name || attachment.Spec.Source.PersistentVolumeName == nil {
				continue
			}
			state.volumeAttachmentCount++
			pvName := *attachment.Spec.Source.PersistentVolumeName
			volumeHandle := state.volumeHandlesByPV[pvName]
			if volumeHandle == "" {
				// CSI PV names normally equal CreateVolumeRequest.name. This fallback
				// makes a temporarily missing PV object a conservative live grant.
				volumeHandle = pvName
			}
			state.liveVolumeAttachments[volumeAttachmentKey(volumeHandle, attachment.Spec.NodeName)] = struct{}{}
		}
	}

	contents, err := dynamicClient.Resource(volumeSnapshotContentGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list VolumeSnapshotContents: %w", err)
	}
	for i := range contents.Items {
		content := &contents.Items[i]
		driverName, found, nestedErr := unstructured.NestedString(content.Object, "spec", "driver")
		if nestedErr != nil {
			d.recordReconcileObjectFailure("snapshot_content_classification", content.GetName(), nestedErr)
			continue
		}
		if !found {
			continue
		}
		if driverName != d.name {
			continue
		}
		handle, found, nestedErr := unstructured.NestedString(content.Object, "status", "snapshotHandle")
		if nestedErr != nil || !found || handle == "" {
			// Pre-provisioned contents declare their backend handle in
			// spec.source before the snapshotter ever populates status; that
			// handle is authoritative and keeps the content a live grant.
			sourceHandle, sourceFound, sourceErr := unstructured.NestedString(
				content.Object, "spec", "source", "snapshotHandle",
			)
			switch {
			case sourceErr == nil && sourceFound && sourceHandle != "":
				handle = sourceHandle
			case !content.GetCreationTimestamp().Time.IsZero() &&
				time.Since(content.GetCreationTimestamp().Time) < minOrphanAge:
				// Dynamic content mid-creation (e.g. the nightly GC run racing
				// VolSync's hourly snapshot schedule). Its backend snapshot
				// cannot be older than the content object, and every guarded
				// snapshot delete re-proves age >= minOrphanAge immediately
				// before destroy, so content younger than the gate cannot own
				// a delete-eligible backend snapshot. Skip it without failing
				// the whole snapshot deletion pass closed. The safety margin
				// of this carve-out is minOrphanAge minus TrueNAS<->kube-API
				// clock skew: it is enormous at the 24h default but shrinks if
				// an operator tunes minOrphanAge down to minutes.
				klog.V(2).Infof(
					"Orphan reconcile: ignoring in-flight driver VolumeSnapshotContent %s (age %v < min orphan age %v, status.snapshotHandle not yet populated)",
					content.GetName(), time.Since(content.GetCreationTimestamp().Time).Round(time.Second), minOrphanAge,
				)
				continue
			default:
				klog.Warningf(
					"Orphan reconcile: skipping driver VolumeSnapshotContent %s because status.snapshotHandle is unavailable",
					content.GetName(),
				)
				state.handlelessSnapshotContentNames = append(state.handlelessSnapshotContentNames, content.GetName())
				continue
			}
		}
		state.snapshotHandles[handle] = struct{}{}
		namespace, _, namespaceErr := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "namespace")
		name, _, nameErr := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "name")
		if namespaceErr != nil || nameErr != nil {
			nestedErr := namespaceErr
			if nestedErr == nil {
				nestedErr = nameErr
			}
			d.recordReconcileObjectFailure("snapshot_content_classification", content.GetName(), nestedErr)
			continue
		}
		contentState := snapshotContentState{
			name: content.GetName(), snapshotHandle: handle,
		}
		state.snapshotContentsByName[content.GetName()] = contentState
		if namespace != "" && name != "" {
			state.snapshotContentsByRef[namespacedName(namespace, name)] = contentState
		}
	}

	// VolumeSnapshots and PVCs are always loaded so spent-restore classification
	// can run regardless of the global detached flag (see reconcileOrphans).
	volumeSnapshots, err := dynamicClient.Resource(volumeSnapshotGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list VolumeSnapshots: %w", err)
	}
	state.volumeSnapshots = volumeSnapshots.Items
	pvcs, err := clientset.CoreV1().PersistentVolumeClaims(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list PersistentVolumeClaims: %w", err)
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		state.pvcs[namespacedName(pvc.Namespace, pvc.Name)] = pvc
	}
	return state, nil
}

// The broad list used for classification is necessarily a sample. These live
// GETs occur after backend identity revalidation and immediately before the CSI
// delete call. Any object returned under the expected name is a veto, without
// trusting its current fields; a concurrent binder must always win the race.
func (d *Driver) hardRecheckPersistentVolumeAbsent(ctx context.Context, orphan ReconcileObject) (safe bool, reason string) {
	clientset, _, err := d.kubernetesReconcileClients()
	if err != nil {
		return false, err.Error()
	}
	name := orphan.PVName
	if name == "" {
		name = orphan.ID
	}
	// Also close the legacy-name gap for volumes whose durable csi_volume_name
	// does not equal the actual PV metadata name.
	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Sprintf("final PersistentVolume handle recheck failed: %v", err)
	}
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.name && pv.Spec.CSI.VolumeHandle == orphan.ID {
			return false, fmt.Sprintf("PersistentVolume %s appeared for handle %s during final live recheck", pv.Name, orphan.ID)
		}
	}
	// Keep the required object-specific GET as the final API operation before
	// the caller invokes DeleteVolume.
	if _, err := clientset.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{}); err == nil {
		return false, fmt.Sprintf("PersistentVolume %s appeared during final live recheck", name)
	} else if !apierrors.IsNotFound(err) {
		return false, fmt.Sprintf("final PersistentVolume %s recheck failed: %v", name, err)
	}
	return true, ""
}

func (d *Driver) hardRecheckSnapshotContentAbsent(ctx context.Context, orphan ReconcileObject) (safe bool, reason string) {
	_, dynamicClient, err := d.kubernetesReconcileClients()
	if err != nil {
		return false, err.Error()
	}
	name := orphan.KubernetesName
	if name == "" {
		name = orphan.ID
	}
	contents, err := dynamicClient.Resource(volumeSnapshotContentGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Sprintf("final VolumeSnapshotContent handle recheck failed: %v", err)
	}
	for i := range contents.Items {
		content := &contents.Items[i]
		driverName, _, _ := unstructured.NestedString(content.Object, "spec", "driver")
		handle, _, _ := unstructured.NestedString(content.Object, "status", "snapshotHandle")
		if driverName == d.name && handle == orphan.ID {
			return false, fmt.Sprintf("VolumeSnapshotContent %s appeared for handle %s during final live recheck", content.GetName(), orphan.ID)
		}
	}
	// Keep the required object-specific GET as the final API operation before
	// the caller invokes DeleteSnapshot.
	if _, err := dynamicClient.Resource(volumeSnapshotContentGVR).Get(ctx, name, metav1.GetOptions{}); err == nil {
		return false, fmt.Sprintf("VolumeSnapshotContent %s appeared during final live recheck", name)
	} else if !apierrors.IsNotFound(err) {
		return false, fmt.Sprintf("final VolumeSnapshotContent %s recheck failed: %v", name, err)
	}
	return true, ""
}

// remnantHasNoKubernetesReference live-lists PersistentVolumes and
// VolumeAttachments (NOT informer caches) and reports whether any object owned
// by this driver references volumeID. It is the classification and pre-destroy
// hard-recheck for remnant orphans, mirroring liveVolumeAttachmentExists.
func (d *Driver) remnantHasNoKubernetesReference(ctx context.Context, volumeID string) (safe bool, reason string) {
	clientset, _, err := d.kubernetesReconcileClients()
	if err != nil {
		return false, err.Error()
	}
	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Sprintf("live PersistentVolume list for remnant recheck: %v", err)
	}
	handlesByPV := make(map[string]string)
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != d.name {
			continue
		}
		handlesByPV[pv.Name] = pv.Spec.CSI.VolumeHandle
		if pv.Spec.CSI.VolumeHandle == volumeID {
			return false, fmt.Sprintf("PersistentVolume %s references remnant volume %s", pv.Name, volumeID)
		}
	}
	// Volume names derive from the PVC UID (the CreateVolume name), and a PV's
	// spec.csi.volumeHandle equals that name. A remnant has no PV because
	// provisioning never completed, and a Pending PVC that would retry this exact
	// volume re-enters CreateVolume and recovers the remnant through its marker
	// (recoverInFlightContentSourceRemnant) rather than binding a competing PV —
	// so a missing PV is sufficient proof no claim references the remnant, and no
	// separate PVC scan is needed. A VolumeAttachment, by contrast, can outlive a
	// deleted PV (operator force-finalizer), so it is rechecked live as well.
	attachments, err := clientset.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Sprintf("live VolumeAttachment list for remnant recheck: %v", err)
	}
	for i := range attachments.Items {
		attachment := &attachments.Items[i]
		if attachment.Spec.Attacher != d.name || attachment.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		pvName := *attachment.Spec.Source.PersistentVolumeName
		if handlesByPV[pvName] == volumeID || pvName == volumeID {
			return false, fmt.Sprintf("VolumeAttachment %s references remnant volume %s", attachment.Name, volumeID)
		}
	}
	return true, ""
}

// liveBoundVolumeHandles live-lists PersistentVolumes (clientset List, NOT
// informer caches) and returns the volume handles referenced by a Bound PV of
// THIS driver. The boolean is false when the list fails or when NO PV references
// this driver at all — the standard fail-safe shared with the remnant classifier:
// an API discontinuity (or an empty view of the driver's PVs) is not evidence
// that adoption is safe, so the caller adopts nothing this pass.
func (d *Driver) liveBoundVolumeHandles(ctx context.Context) (map[string]struct{}, bool) {
	clientset, _, err := d.kubernetesReconcileClients()
	if err != nil {
		return nil, false
	}
	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		d.recordReconcileObjectFailure("stamp_adoption_pv_list", d.name, err)
		return nil, false
	}
	handles := make(map[string]struct{})
	driverPVCount := 0
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != d.name {
			continue
		}
		driverPVCount++
		if pv.Status.Phase == corev1.VolumeBound {
			handles[pv.Spec.CSI.VolumeHandle] = struct{}{}
		}
	}
	if driverPVCount == 0 {
		return nil, false
	}
	return handles, true
}

func volumeAttachmentKey(volumeID, nodeName string) string {
	return volumeID + "\x00" + nodeName
}

func (d *Driver) liveVolumeAttachmentExists(ctx context.Context, volumeID, nodeName string) (exists bool, attachmentCount int, retErr error) {
	clientset, _, err := d.kubernetesReconcileClients()
	if err != nil {
		return false, 0, err
	}
	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, 0, fmt.Errorf("final PersistentVolume list for attachment recheck: %w", err)
	}
	handlesByPV := make(map[string]string)
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.name {
			handlesByPV[pv.Name] = pv.Spec.CSI.VolumeHandle
		}
	}
	attachments, err := clientset.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, 0, fmt.Errorf("final VolumeAttachment list: %w", err)
	}
	count := 0
	for i := range attachments.Items {
		attachment := &attachments.Items[i]
		if attachment.Spec.Attacher != d.name || attachment.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		count++
		pvName := *attachment.Spec.Source.PersistentVolumeName
		if (handlesByPV[pvName] == volumeID || pvName == volumeID) && attachment.Spec.NodeName == nodeName {
			return true, count, nil
		}
	}
	return false, count, nil
}
