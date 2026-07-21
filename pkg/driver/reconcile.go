package driver

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

const (
	reconcileListPageSize           = 100
	staleRecordMassAbsenceThreshold = 2
)

var (
	volumeSnapshotContentGVR = schema.GroupVersionResource{
		Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents",
	}
	volumeSnapshotGVR = schema.GroupVersionResource{
		Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots",
	}
)

// ReconcileOptions controls one orphan reconcile pass.
type ReconcileOptions struct {
	// Delete enables guarded cleanup. It must only be set by the separately
	// gated run-once entrypoint; the periodic controller pass always uses false.
	Delete bool

	// MinOrphanAge prevents in-flight or rebuild-transient resources from being
	// classified for cleanup. A non-positive value falls back to the config.
	MinOrphanAge time.Duration
}

// ReconcileObject describes a managed backend resource detected as orphaned.
type ReconcileObject struct {
	ID             string
	BackendID      string
	KubernetesName string
	PVName         string
	SourceVolumeID string
	CreatedAt      time.Time
	Age            time.Duration
	Bytes          int64
}

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

// ReconcileActionFailure records a guarded cleanup that was skipped or refused.
type ReconcileActionFailure struct {
	Kind   string
	ID     string
	Reason string
}

// ReconcileReport is the complete detection and optional cleanup result.
type ReconcileReport struct {
	OrphanVolumeCount          int
	OrphanSnapshotCount        int
	SpentRestoreSnapshotCount  int
	OrphanVolumeBytes          int64
	OrphanSnapshotBytes        int64
	OrphanVolumes              []ReconcileObject
	OrphanSnapshots            []ReconcileObject
	SpentRestoreSnapshots      []SpentRestoreSnapshot
	DeletedVolumes             []string
	DeletedSnapshots           []string
	DeletedSpentRestoreObjects []string
	SkippedDeletes             []ReconcileActionFailure
	DeleteEnabled              bool
}

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

type stalePublicationObservation struct {
	FirstMissing time.Time
	UpdatedAt    string
	State        string
	EncodedID    string
}

func newStalePublicationObservation(now time.Time, record publicationRecord) stalePublicationObservation {
	return stalePublicationObservation{
		FirstMissing: now,
		UpdatedAt:    record.UpdatedAt,
		State:        record.State,
		EncodedID:    record.EncodedID,
	}
}

func (observation stalePublicationObservation) matches(record publicationRecord) bool {
	return observation.UpdatedAt == record.UpdatedAt &&
		observation.State == record.State &&
		observation.EncodedID == record.EncodedID
}

// ReconcileOrphans detects managed TrueNAS resources that no longer have a
// matching Kubernetes object. Backend deletion, when explicitly enabled, is
// routed exclusively through the existing guarded CSI delete methods.
func (d *Driver) ReconcileOrphans(ctx context.Context, opts ReconcileOptions) (ReconcileReport, error) {
	// The exported run-once path is also used by the chart's orphan-GC CronJob.
	// It must never become a second fencing writer beside the live controller;
	// stale publication revocation is exclusive to the controller loop below.
	return d.reconcileOrphans(ctx, opts, false)
}

func (d *Driver) reconcileOrphans(ctx context.Context, opts ReconcileOptions, reconcileStalePublications bool) (report ReconcileReport, retErr error) {
	report = ReconcileReport{DeleteEnabled: opts.Delete}
	defer func() {
		sort.Slice(report.OrphanVolumes, func(i, j int) bool { return report.OrphanVolumes[i].ID < report.OrphanVolumes[j].ID })
		sort.Slice(report.OrphanSnapshots, func(i, j int) bool { return report.OrphanSnapshots[i].ID < report.OrphanSnapshots[j].ID })
		sort.Slice(report.SpentRestoreSnapshots, func(i, j int) bool {
			left := report.SpentRestoreSnapshots[i].Namespace + "/" + report.SpentRestoreSnapshots[i].Name
			right := report.SpentRestoreSnapshots[j].Namespace + "/" + report.SpentRestoreSnapshots[j].Name
			return left < right
		})
		report.OrphanVolumeCount = len(report.OrphanVolumes)
		report.OrphanSnapshotCount = len(report.OrphanSnapshots)
		report.SpentRestoreSnapshotCount = len(report.SpentRestoreSnapshots)
		// Publish even a partial pass so a single malformed object cannot freeze
		// the last visible inventory indefinitely.
		SetOrphanReconcileMetrics(report)
		if retErr == nil {
			RecordReconcileSuccess(time.Now())
		}
	}()
	if d.config == nil || d.truenasClient == nil {
		RecordReconcileFailure("configuration")
		return report, fmt.Errorf("driver configuration and TrueNAS client are required")
	}
	minOrphanAge, err := d.reconcileMinOrphanAge(opts.MinOrphanAge)
	if err != nil {
		RecordReconcileFailure("configuration")
		return report, err
	}

	datasets, err := d.listAllManagedDatasets(ctx)
	if err != nil {
		RecordReconcileFailure("list_backend_volumes")
		return report, fmt.Errorf("list managed backend volumes: %w", err)
	}
	snapshots, err := d.listAllManagedSnapshots(ctx)
	if err != nil {
		RecordReconcileFailure("list_backend_snapshots")
		return report, fmt.Errorf("list managed backend snapshots: %w", err)
	}
	kubeState, err := d.loadKubernetesReconcileState(ctx)
	if err != nil {
		RecordReconcileFailure("load_kubernetes_state")
		return report, err
	}
	if reconcileStalePublications && d.config.Fencing.Enabled() {
		d.reconcileStalePublicationRecords(ctx, datasets, kubeState, time.Now())
	}

	now := time.Now()
	managedBackendVolumeCount := 0
	for _, ds := range datasets {
		if ds == nil || ds.UserProperties[PropManagedResource].Value != "true" {
			continue
		}
		managedBackendVolumeCount++
		volumeID := path.Base(ds.Name)
		if _, live := kubeState.volumeHandles[volumeID]; live {
			continue
		}
		createdAt, age, eligible := reconcileAge(now, ds.GetCreationTime(), minOrphanAge)
		if !eligible {
			if ds.GetCreationTime() <= 0 {
				klog.Warningf("Orphan reconcile: skipping managed volume %s because its creation time is unavailable", ds.Name)
			}
			continue
		}
		pvName := ds.UserProperties[PropCSIVolumeName].Value
		if pvName == "" || pvName == "-" {
			pvName = volumeID
		}
		item := ReconcileObject{
			ID:        volumeID,
			BackendID: ds.Name,
			PVName:    pvName,
			CreatedAt: createdAt,
			Age:       age,
			Bytes:     ds.GetUsedBytes(),
		}
		report.OrphanVolumes = append(report.OrphanVolumes, item)
		report.OrphanVolumeBytes += item.Bytes
	}

	managedBackendSnapshotCount := 0
	for _, snap := range snapshots {
		if !isCSISnapshot(snap) {
			continue
		}
		managedBackendSnapshotCount++
		snapshotHandle, ok := reconcileSnapshotHandle(snap)
		if !ok {
			klog.Warningf("Orphan reconcile: skipping managed snapshot %s because its CSI handle cannot be derived", snap.ID)
			continue
		}
		if _, live := kubeState.snapshotHandles[snapshotHandle]; live {
			continue
		}
		createdAt, age, eligible := reconcileAge(now, snap.GetCreationTime(), minOrphanAge)
		if !eligible {
			if snap.GetCreationTime() <= 0 {
				klog.Warningf("Orphan reconcile: skipping managed snapshot %s because its creation time is unavailable", snap.ID)
			}
			continue
		}
		sourceVolumeID := snap.UserProperties[PropCSISnapshotSourceVolumeID].Value
		if sourceVolumeID == "" || sourceVolumeID == "-" {
			sourceVolumeID = path.Base(snap.Dataset)
		}
		item := ReconcileObject{
			ID:             snapshotHandle,
			BackendID:      snap.ID,
			KubernetesName: snap.UserProperties[PropCSISnapshotName].Value,
			SourceVolumeID: sourceVolumeID,
			CreatedAt:      createdAt,
			Age:            age,
			Bytes:          snap.GetSnapshotSize(),
		}
		report.OrphanSnapshots = append(report.OrphanSnapshots, item)
		report.OrphanSnapshotBytes += item.Bytes
	}

	if d.config.ZFS.DetachedVolumesFromSnapshots {
		report.SpentRestoreSnapshots = d.classifySpentRestoreSnapshots(ctx, now, kubeState)
		if ctxErr := ctx.Err(); ctxErr != nil {
			RecordReconcileFailure("spent_restore_classification")
			return report, ctxErr
		}
	}

	logAction := "[DRY RUN] would delete"
	if opts.Delete {
		logAction = "will attempt guarded delete of"
	}
	for _, orphan := range report.OrphanSnapshots {
		klog.Infof("Orphan reconcile: %s managed snapshot %s (backend=%s age=%v bytes=%d)",
			logAction, orphan.ID, orphan.BackendID, orphan.Age, orphan.Bytes)
	}
	for _, orphan := range report.OrphanVolumes {
		klog.Infof("Orphan reconcile: %s managed volume %s (backend=%s pv=%s age=%v bytes=%d)",
			logAction, orphan.ID, orphan.BackendID, orphan.PVName, orphan.Age, orphan.Bytes)
	}
	for i := range report.SpentRestoreSnapshots {
		spent := &report.SpentRestoreSnapshots[i]
		spentAction := logAction
		if spent.Age <= minOrphanAge {
			spentAction = "classified (creation-age gate not yet met):"
		}
		klog.Infof("Orphan reconcile: %s spent restore VolumeSnapshot %s/%s (content=%s sourcePVC=%s phase=%s)",
			spentAction, spent.Namespace, spent.Name, spent.ContentName, spent.SourcePVC, spent.SourcePVCPhase)
	}

	if !opts.Delete {
		return report, nil
	}
	if len(kubeState.volumeHandles) == 0 && managedBackendVolumeCount > 0 {
		RecordReconcileFailure("safety_brake")
		return report, fmt.Errorf(
			"refusing to GC: zero live PVs for driver but %d managed backend volumes exist — cluster rebuild in progress?",
			managedBackendVolumeCount,
		)
	}
	snapshotDeleteBlockReason := snapshotDeletePassBlockReason(kubeState, managedBackendSnapshotCount)

	// Re-list Kubernetes state immediately before mutation so a newly created PV
	// or snapshot binding cannot be deleted using a stale detection snapshot.
	currentState, err := d.loadKubernetesReconcileState(ctx)
	if err != nil {
		RecordReconcileFailure("revalidate_kubernetes_state")
		return report, fmt.Errorf("revalidate Kubernetes state before delete: %w", err)
	}
	if len(currentState.volumeHandles) == 0 && managedBackendVolumeCount > 0 {
		RecordReconcileFailure("safety_brake")
		return report, fmt.Errorf(
			"refusing to GC: zero live PVs for driver but %d managed backend volumes exist — cluster rebuild in progress?",
			managedBackendVolumeCount,
		)
	}
	if reason := snapshotDeletePassBlockReason(currentState, managedBackendSnapshotCount); reason != "" {
		snapshotDeleteBlockReason = reason
	}
	if err := d.deleteDetectedOrphans(
		ctx,
		&report,
		currentState,
		minOrphanAge,
		d.config.Reconcile.Delete.MaxPerRun,
		snapshotDeleteBlockReason,
	); err != nil {
		RecordReconcileFailure("delete")
		return report, err
	}
	return report, nil
}

func reconcileSnapshotHandle(snapshot *truenas.Snapshot) (string, bool) {
	if snapshot == nil {
		return "", false
	}
	if snapshotHandle, ok := extractSnapshotName(snapshot.ID); ok {
		return snapshotHandle, true
	}
	if snapshot.Name != "" {
		return snapshot.Name, true
	}
	return "", false
}

func snapshotDeletePassBlockReason(state *kubernetesReconcileState, managedBackendSnapshotCount int) string {
	if state == nil {
		return "Kubernetes snapshot state is unavailable"
	}
	if len(state.handlelessSnapshotContentNames) > 0 {
		return fmt.Sprintf(
			"%d driver VolumeSnapshotContents have no readable status.snapshotHandle",
			len(state.handlelessSnapshotContentNames),
		)
	}
	if len(state.snapshotHandles) == 0 && managedBackendSnapshotCount > 0 {
		return fmt.Sprintf(
			"zero live VolumeSnapshotContents for driver but %d managed backend snapshots exist — cluster rebuild in progress?",
			managedBackendSnapshotCount,
		)
	}
	return ""
}

func (d *Driver) reconcileMinOrphanAge(requested time.Duration) (time.Duration, error) {
	if requested > 0 {
		return requested, nil
	}
	if d.config == nil {
		return 0, fmt.Errorf("driver config is unavailable")
	}
	minAge, err := d.config.Reconcile.MinOrphanAgeDuration()
	if err != nil || minAge <= 0 {
		if err != nil {
			return 0, fmt.Errorf("parse reconcile minimum orphan age: %w", err)
		}
		return 0, fmt.Errorf("reconcile minimum orphan age must be positive")
	}
	return minAge, nil
}

func reconcileAge(now time.Time, creationUnix int64, minAge time.Duration) (time.Time, time.Duration, bool) {
	if creationUnix <= 0 {
		return time.Time{}, 0, false
	}
	createdAt := time.Unix(creationUnix, 0)
	age := now.Sub(createdAt)
	return createdAt, age, age > minAge
}

func (d *Driver) listAllManagedDatasets(ctx context.Context) ([]*truenas.Dataset, error) {
	var datasets []*truenas.Dataset
	for offset := 0; ; offset += reconcileListPageSize {
		page, err := d.truenasClient.DatasetList(ctx, d.config.ZFS.DatasetParentName, reconcileListPageSize, offset)
		if err != nil {
			return nil, err
		}
		datasets = append(datasets, page...)
		if len(page) < reconcileListPageSize {
			return datasets, nil
		}
	}
}

func (d *Driver) listAllManagedSnapshots(ctx context.Context) ([]*truenas.Snapshot, error) {
	var snapshots []*truenas.Snapshot
	for offset := 0; ; offset += reconcileListPageSize {
		page, err := d.truenasClient.SnapshotListAll(ctx, d.config.ZFS.DatasetParentName, reconcileListPageSize, offset)
		if err != nil {
			return nil, err
		}
		for _, snap := range page {
			if isCSISnapshot(snap) {
				snapshots = append(snapshots, snap)
			}
		}
		if len(page) < reconcileListPageSize {
			return snapshots, nil
		}
	}
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

func (d *Driver) loadKubernetesReconcileState(ctx context.Context) (*kubernetesReconcileState, error) {
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
			klog.Warningf(
				"Orphan reconcile: skipping driver VolumeSnapshotContent %s because status.snapshotHandle is unavailable",
				content.GetName(),
			)
			state.handlelessSnapshotContentNames = append(state.handlelessSnapshotContentNames, content.GetName())
			continue
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

	if !d.config.ZFS.DetachedVolumesFromSnapshots {
		return state, nil
	}
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

func (d *Driver) classifySpentRestoreSnapshots(
	ctx context.Context,
	now time.Time,
	state *kubernetesReconcileState,
) []SpentRestoreSnapshot {
	if state == nil {
		return nil
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
		if exists && pvc.Status.Phase == corev1.ClaimBound {
			continue
		}
		backendSnapshot, backendErr := d.findBackendSnapshotForHandle(ctx, content.snapshotHandle)
		if backendErr != nil {
			d.recordReconcileObjectFailure("spent_restore_classification", content.snapshotHandle, backendErr)
			continue
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

func (d *Driver) findBackendSnapshotForHandle(ctx context.Context, handle string) (*truenas.Snapshot, error) {
	if strings.Contains(handle, "@") {
		snapshot, err := d.truenasClient.SnapshotGet(ctx, handle)
		if err != nil {
			if truenas.IsNotFoundError(err) {
				return nil, nil
			}
			return nil, err
		}
		return snapshot, nil
	}
	return d.truenasClient.SnapshotFindByName(ctx, d.config.ZFS.DatasetParentName, handle)
}

func laterTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func (d *Driver) deleteDetectedOrphans(
	ctx context.Context,
	report *ReconcileReport,
	currentState *kubernetesReconcileState,
	minOrphanAge time.Duration,
	maxPerRun int,
	snapshotDeleteBlockReason string,
) error {
	deletedCount := 0
	deletionCapReached := func(kind, id string) bool {
		if deletedCount < maxPerRun {
			return false
		}
		d.recordReconcileSkip(
			report,
			kind,
			id,
			fmt.Sprintf("deletion cap reached (maxPerRun=%d)", maxPerRun),
		)
		return true
	}

	if snapshotDeleteBlockReason != "" {
		klog.Errorf("Orphan reconcile: snapshot deletion pass skipped: %s", snapshotDeleteBlockReason)
		for _, orphan := range report.OrphanSnapshots {
			d.recordReconcileSkip(report, "snapshot", orphan.ID, snapshotDeleteBlockReason)
		}
	} else {
		for _, orphan := range report.OrphanSnapshots {
			if err := ctx.Err(); err != nil {
				return err
			}
			if deletionCapReached("snapshot", orphan.ID) {
				continue
			}
			if _, live := currentState.snapshotHandles[orphan.ID]; live {
				d.recordReconcileSkip(report, "snapshot", orphan.ID, "snapshot handle became live during revalidation")
				continue
			}
			if safe, reason := d.revalidateOrphanSnapshot(ctx, orphan, minOrphanAge); !safe {
				d.recordReconcileSkip(report, "snapshot", orphan.ID, reason)
				continue
			}
			if safe, reason := d.hardRecheckSnapshotContentAbsent(ctx, orphan); !safe {
				d.recordReconcileSkip(report, "snapshot", orphan.ID, reason)
				continue
			}
			klog.Infof("Orphan reconcile: deleting managed snapshot %s through guarded DeleteSnapshot", orphan.ID)
			if _, err := d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: orphan.ID}); err != nil {
				d.recordReconcileSkip(report, "snapshot", orphan.ID, err.Error())
				continue
			}
			report.DeletedSnapshots = append(report.DeletedSnapshots, orphan.ID)
			deletedCount++
		}
	}

	for _, orphan := range report.OrphanVolumes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if deletionCapReached("volume", orphan.ID) {
			continue
		}
		if _, live := currentState.volumeHandles[orphan.ID]; live {
			d.recordReconcileSkip(report, "volume", orphan.ID, "volume handle became live during revalidation")
			continue
		}
		if safe, reason := d.revalidateOrphanVolume(ctx, orphan, minOrphanAge); !safe {
			d.recordReconcileSkip(report, "volume", orphan.ID, reason)
			continue
		}
		if safe, reason := d.hardRecheckPersistentVolumeAbsent(ctx, orphan); !safe {
			d.recordReconcileSkip(report, "volume", orphan.ID, reason)
			continue
		}
		klog.Infof("Orphan reconcile: deleting managed volume %s through guarded DeleteVolume", orphan.ID)
		if _, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: orphan.ID}); err != nil {
			d.recordReconcileSkip(report, "volume", orphan.ID, err.Error())
			continue
		}
		report.DeletedVolumes = append(report.DeletedVolumes, orphan.ID)
		deletedCount++
	}

	if !d.config.ZFS.DetachedVolumesFromSnapshots {
		return nil
	}
	clientset, dynamicClient, err := d.kubernetesReconcileClients()
	if err != nil {
		return err
	}
	for i := range report.SpentRestoreSnapshots {
		detected := &report.SpentRestoreSnapshots[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		key := namespacedName(detected.Namespace, detected.Name)
		if deletionCapReached("spent-restore-snapshot", key) {
			continue
		}
		spent, safe, reason := d.revalidateSpentRestoreSnapshot(
			ctx, clientset, dynamicClient, *detected, minOrphanAge,
		)
		if !safe {
			d.recordReconcileSkip(report, "spent-restore-snapshot", key, reason)
			continue
		}
		klog.Infof("Orphan reconcile: deleting spent restore VolumeSnapshot %s", key)
		err := dynamicClient.Resource(volumeSnapshotGVR).Namespace(spent.Namespace).Delete(ctx, spent.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			d.recordReconcileSkip(report, "spent-restore-snapshot", key, err.Error())
			continue
		}
		report.DeletedSpentRestoreObjects = append(report.DeletedSpentRestoreObjects, key)
		deletedCount++
	}
	return nil
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

func (d *Driver) revalidateOrphanVolume(
	ctx context.Context,
	orphan ReconcileObject,
	minOrphanAge time.Duration,
) (safe bool, reason string) {
	dataset, err := d.truenasClient.DatasetGet(ctx, orphan.BackendID)
	if err != nil {
		return false, fmt.Sprintf("backend volume revalidation failed: %v", err)
	}
	if dataset == nil || dataset.UserProperties[PropManagedResource].Value != "true" {
		return false, "backend volume is no longer CSI-managed"
	}
	if path.Base(dataset.Name) != orphan.ID {
		return false, "backend volume identity changed"
	}
	createdAt, _, eligible := reconcileAge(time.Now(), dataset.GetCreationTime(), minOrphanAge)
	if !eligible || !createdAt.Equal(orphan.CreatedAt) {
		return false, "backend volume creation identity or age changed"
	}
	return true, ""
}

func (d *Driver) revalidateOrphanSnapshot(
	ctx context.Context,
	orphan ReconcileObject,
	minOrphanAge time.Duration,
) (safe bool, reason string) {
	snapshot, err := d.truenasClient.SnapshotGet(ctx, orphan.BackendID)
	if err != nil {
		return false, fmt.Sprintf("backend snapshot revalidation failed: %v", err)
	}
	snapshotHandle, ok := reconcileSnapshotHandle(snapshot)
	if !isCSISnapshot(snapshot) || !ok || snapshotHandle != orphan.ID {
		return false, "backend snapshot is no longer the detected CSI-managed object"
	}
	createdAt, _, eligible := reconcileAge(time.Now(), snapshot.GetCreationTime(), minOrphanAge)
	if !eligible || !createdAt.Equal(orphan.CreatedAt) {
		return false, "backend snapshot creation identity or age changed"
	}
	return true, ""
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
	if err == nil && pvc.Status.Phase == corev1.ClaimBound {
		return SpentRestoreSnapshot{}, false, "source PVC became Bound during revalidation"
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

func volumeAttachmentKey(volumeID, nodeName string) string {
	return volumeID + "\x00" + nodeName
}

func stalePublicationObservationKey(datasetName, propertyKey string) string {
	return datasetName + "\x00" + propertyKey
}

func publicationPropertyCount(datasets []*truenas.Dataset) int {
	count := 0
	for _, dataset := range datasets {
		if dataset == nil {
			continue
		}
		for key, property := range dataset.UserProperties {
			if strings.HasPrefix(key, publicationPropertyPrefix) &&
				!strings.Contains(strings.ToLower(property.Source), "inherit") {
				count++
			}
		}
	}
	return count
}

// reconcileStalePublicationRecords repairs the operator force-finalizer escape
// hatch. A finalizer-removed VolumeAttachment never reaches external-attacher's
// normal ControllerUnpublishVolume call, so its durable record otherwise blocks
// SINGLE_NODE volumes forever. Absence must be continuous for the configured
// grace period and is proved again under the same per-volume lock used by CSI.
func (d *Driver) reconcileStalePublicationRecords(
	ctx context.Context,
	datasets []*truenas.Dataset,
	state *kubernetesReconcileState,
	now time.Time,
) {
	if state == nil {
		return
	}
	recordCount := publicationPropertyCount(datasets)
	if state.volumeAttachmentCount == 0 && recordCount >= staleRecordMassAbsenceThreshold {
		// A zero-result VA list while several backend records exist is the shape of
		// an etcd restore or informer/API discontinuity, not evidence for mass
		// revocation. Restart every observation's grace window after recovery.
		d.stalePublicationRecordsSeen.Range(func(key, _ interface{}) bool {
			d.stalePublicationRecordsSeen.Delete(key)
			return true
		})
		RecordFencingStaleDeferred()
		klog.Warningf("Stale fencing record reconcile deferred: VolumeAttachment list is empty while %d records exist (brake threshold=%d)",
			recordCount, staleRecordMassAbsenceThreshold)
		return
	}
	grace, err := d.config.Fencing.StaleRecordGracePeriodDuration()
	if err != nil || grace <= 0 {
		d.recordReconcileObjectFailure("stale_publication_configuration", "fencing.staleRecordGracePeriod", err)
		return
	}
	for _, dataset := range datasets {
		if dataset == nil {
			continue
		}
		records, parseErr := publicationRecordsFromDataset(dataset)
		if parseErr != nil {
			d.recordReconcileObjectFailure("stale_publication_classification", dataset.Name, parseErr)
			continue
		}
		volumeID := path.Base(dataset.Name)
		for propertyKey := range records {
			record := records[propertyKey]
			observationKey := stalePublicationObservationKey(dataset.Name, propertyKey)
			if _, live := state.liveVolumeAttachments[volumeAttachmentKey(volumeID, record.Node)]; live {
				d.stalePublicationRecordsSeen.Delete(observationKey)
				continue
			}
			firstMissing := now
			if record.State != publicationStateRemoving {
				candidate := newStalePublicationObservation(now, record)
				actual, loaded := d.stalePublicationRecordsSeen.LoadOrStore(observationKey, candidate)
				observation, valid := actual.(stalePublicationObservation)
				if !loaded || !valid || !observation.matches(record) {
					d.stalePublicationRecordsSeen.Store(observationKey, candidate)
					observation = candidate
				}
				firstMissing = observation.FirstMissing
				if now.Sub(firstMissing) < grace {
					continue
				}
			}
			revoked, err := d.revokeStalePublicationRecord(ctx, dataset.Name, volumeID, propertyKey, record, recordCount)
			if err != nil {
				d.recordReconcileObjectFailure("stale_publication_cleanup", volumeID+"/"+record.Node, err)
				continue
			}
			d.stalePublicationRecordsSeen.Delete(observationKey)
			if !revoked {
				continue
			}
			klog.Infof("Stale fencing record reconcile revoked volume=%s node=%s after continuous absence since %s",
				volumeID, record.Node, firstMissing.UTC().Format(time.RFC3339))
		}
	}
}

func (d *Driver) revokeStalePublicationRecord(
	ctx context.Context,
	datasetName, volumeID, propertyKey string,
	detected publicationRecord,
	recordCount int,
) (bool, error) {
	lockKey := "volume:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return false, fmt.Errorf("volume operation is in progress")
	}
	defer d.releaseOperationLock(lockKey)

	dataset, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return false, fmt.Errorf("fresh dataset read: %w", err)
	}
	records, err := publicationRecordsFromDataset(dataset)
	if err != nil {
		return false, fmt.Errorf("fresh publication record read: %w", err)
	}
	current, exists := records[propertyKey]
	if !exists || !samePublicationRecordGeneration(current, detected) {
		// The grant was removed or republished while the outer pass waited for
		// the volume lock. Never apply an old grace decision to a new generation.
		return false, nil
	}
	live, attachmentCount, err := d.liveVolumeAttachmentExists(ctx, volumeID, current.Node)
	if err != nil {
		return false, err
	}
	if live {
		return false, nil
	}
	if attachmentCount == 0 && recordCount >= staleRecordMassAbsenceThreshold {
		RecordFencingStaleDeferred()
		return false, fmt.Errorf("mass-absence brake engaged during final VolumeAttachment recheck")
	}
	nodeID := current.EncodedID
	if nodeID == "" {
		nodeID = current.Node
	}
	shareType := shareTypeForPublishedVolume(dataset, nil)
	if err := d.unpublishFencedVolume(ctx, dataset, datasetName, shareType, nodeID); err != nil {
		return false, fmt.Errorf("revoke backend grant and publication record: %w", err)
	}
	return true, nil
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

func (d *Driver) recordReconcileObjectFailure(phase, id string, err error) {
	RecordReconcileFailure(phase)
	if err == nil {
		err = fmt.Errorf("unknown error")
	}
	klog.Errorf("Reconcile object skipped phase=%s id=%s: %v", phase, id, err)
}

func (d *Driver) recordReconcileSkip(report *ReconcileReport, kind, id, reason string) {
	klog.Warningf("Orphan reconcile: guarded delete skipped kind=%s id=%s: %s", kind, id, reason)
	report.SkippedDeletes = append(report.SkippedDeletes, ReconcileActionFailure{Kind: kind, ID: id, Reason: reason})
}

func namespacedName(namespace, name string) string {
	return strings.TrimSuffix(namespace, "/") + "/" + strings.TrimPrefix(name, "/")
}

func (d *Driver) startOrphanReconcile() {
	if d.config == nil {
		klog.Info("Orphan reconcile detection disabled because configuration is unavailable")
		return
	}
	if !d.config.Reconcile.Enabled && !d.config.Fencing.Enabled() {
		klog.Info("Orphan and stale fencing record reconciliation disabled")
		return
	}
	interval, err := d.config.Reconcile.IntervalDuration()
	if err != nil || interval <= 0 {
		klog.Errorf("Orphan reconcile detection disabled due to invalid interval %q: %v", d.config.Reconcile.Interval, err)
		return
	}
	minAge, err := d.config.Reconcile.MinOrphanAgeDuration()
	if err != nil || minAge <= 0 {
		klog.Errorf("Orphan reconcile detection disabled due to invalid minimum orphan age %q: %v", d.config.Reconcile.MinOrphanAge, err)
		return
	}
	cadence := interval
	if d.config.Fencing.Enabled() {
		grace, graceErr := d.config.Fencing.StaleRecordGracePeriodDuration()
		if graceErr != nil || grace <= 0 {
			klog.Errorf("Controller reconciliation disabled due to invalid fencing stale-record grace %q: %v",
				d.config.Fencing.StaleRecordGracePeriod, graceErr)
			return
		}
		cadence = controllerReconcileCadence(interval, grace)
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.reconcileCancel = cancel
	d.reconcileWg.Add(1)
	go func() {
		defer d.reconcileWg.Done()
		klog.Infof("Controller reconciliation started: interval=%v cadence=%v minOrphanAge=%v orphanDetection=%t staleFencingRecords=%t delete=false",
			interval, cadence, minAge, d.config.Reconcile.Enabled, d.config.Fencing.Enabled())
		run := func() {
			report, reconcileErr := d.reconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: minAge}, true)
			if reconcileErr != nil && ctx.Err() == nil {
				klog.Errorf("Orphan reconcile detection failed: %v", reconcileErr)
				return
			}
			if reconcileErr == nil {
				klog.Infof("Orphan reconcile detection complete: volumes=%d snapshots=%d spentRestoreSnapshots=%d",
					report.OrphanVolumeCount, report.OrphanSnapshotCount, report.SpentRestoreSnapshotCount)
			}
		}

		// Populate metrics immediately rather than leaving them unknown until the
		// first interval elapses.
		run()
		ticker := time.NewTicker(cadence)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				run()
			case <-ctx.Done():
				klog.Info("Orphan reconcile detection stopped")
				return
			}
		}
	}()
}

func controllerReconcileCadence(orphanInterval, staleGrace time.Duration) time.Duration {
	if staleGrace > 0 && staleGrace < orphanInterval {
		return staleGrace
	}
	return orphanInterval
}

func (d *Driver) stopOrphanReconcile() {
	if d.reconcileCancel != nil {
		d.reconcileCancel()
		d.reconcileWg.Wait()
	}
}
