package driver

import (
	"context"
	"encoding/json"
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

const reconcileListPageSize = 100

const (
	PropSpentRestoreMarker     = "truenas-csi:spent_restore"
	spentRestoreMarkerVersion  = 1
	spentRestoreClassification = "volsync-destination"
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
	snapshotHandles                map[string]struct{}
	handlelessSnapshotContentNames []string
	snapshotContentsByRef          map[string]snapshotContentState
	snapshotContentsByName         map[string]snapshotContentState
	volumeSnapshots                []unstructured.Unstructured
	pvcs                           map[string]*corev1.PersistentVolumeClaim
}

type spentRestoreMarker struct {
	Version        int    `json:"v"`
	Classification string `json:"classification"`
	SnapshotID     string `json:"snapshot_id"`
	ClassifiedAt   string `json:"classified_at"`
}

// ReconcileOrphans detects managed TrueNAS resources that no longer have a
// matching Kubernetes object. Backend deletion, when explicitly enabled, is
// routed exclusively through the existing guarded CSI delete methods.
func (d *Driver) ReconcileOrphans(ctx context.Context, opts ReconcileOptions) (ReconcileReport, error) {
	report := ReconcileReport{DeleteEnabled: opts.Delete}
	if d.config == nil || d.truenasClient == nil {
		return report, fmt.Errorf("driver configuration and TrueNAS client are required")
	}
	minOrphanAge, err := d.reconcileMinOrphanAge(opts.MinOrphanAge)
	if err != nil {
		return report, err
	}

	datasets, err := d.listAllManagedDatasets(ctx)
	if err != nil {
		return report, fmt.Errorf("list managed backend volumes: %w", err)
	}
	snapshots, err := d.listAllManagedSnapshots(ctx)
	if err != nil {
		return report, fmt.Errorf("list managed backend snapshots: %w", err)
	}
	kubeState, err := d.loadKubernetesReconcileState(ctx)
	if err != nil {
		return report, err
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
		report.SpentRestoreSnapshots, err = d.classifySpentRestoreSnapshots(ctx, now, kubeState)
		if err != nil {
			return report, fmt.Errorf("classify spent restore snapshots: %w", err)
		}
	}
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
	SetOrphanReconcileMetrics(report)

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
		klog.Infof("Orphan reconcile: %s spent restore VolumeSnapshot %s/%s (content=%s sourcePVC=%s phase=%s)",
			logAction, spent.Namespace, spent.Name, spent.ContentName, spent.SourcePVC, spent.SourcePVCPhase)
	}

	if !opts.Delete {
		return report, nil
	}
	if len(kubeState.volumeHandles) == 0 && managedBackendVolumeCount > 0 {
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
		return report, fmt.Errorf("revalidate Kubernetes state before delete: %w", err)
	}
	if len(currentState.volumeHandles) == 0 && managedBackendVolumeCount > 0 {
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
			return nil, fmt.Errorf("read VolumeSnapshotContent %s driver: %w", content.GetName(), nestedErr)
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
		namespace, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "namespace")
		name, _, _ := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "name")
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

func (d *Driver) classifySpentRestoreSnapshots(ctx context.Context, now time.Time, state *kubernetesReconcileState) ([]SpentRestoreSnapshot, error) {
	if state == nil {
		return nil, nil
	}
	spent := make([]SpentRestoreSnapshot, 0)
	for i := range state.volumeSnapshots {
		snapshot := &state.volumeSnapshots[i]
		matched, matchErr := path.Match("volsync-*-dst-dest*", snapshot.GetName())
		if matchErr != nil {
			continue
		}
		content, ok := state.snapshotContentsByRef[namespacedName(snapshot.GetNamespace(), snapshot.GetName())]
		if !ok {
			boundContent, _, _ := unstructured.NestedString(snapshot.Object, "status", "boundVolumeSnapshotContentName")
			content, ok = state.snapshotContentsByName[boundContent]
		}
		if !ok {
			continue
		}
		sourcePVC, _, _ := unstructured.NestedString(snapshot.Object, "spec", "source", "persistentVolumeClaimName")
		if sourcePVC == "" {
			continue
		}
		pvc, exists := state.pvcs[namespacedName(snapshot.GetNamespace(), sourcePVC)]
		if exists && pvc.Status.Phase == corev1.ClaimBound {
			continue
		}
		backendSnapshot, backendErr := d.findBackendSnapshotForHandle(ctx, content.snapshotHandle)
		if backendErr != nil {
			return nil, backendErr
		}
		if backendSnapshot == nil || !isCSISnapshot(backendSnapshot) {
			continue
		}
		marker, marked, markerErr := spentRestoreMarkerFromSnapshot(backendSnapshot)
		if markerErr != nil {
			return nil, fmt.Errorf("snapshot %s: %w", backendSnapshot.ID, markerErr)
		}
		if !marked {
			if !matched {
				continue
			}
			marker = spentRestoreMarker{
				Version:        spentRestoreMarkerVersion,
				Classification: spentRestoreClassification,
				SnapshotID:     backendSnapshot.ID,
				ClassifiedAt:   now.UTC().Format(time.RFC3339Nano),
			}
			encoded, marshalErr := json.Marshal(marker)
			if marshalErr != nil {
				return nil, marshalErr
			}
			if setErr := d.truenasClient.SnapshotSetUserProperty(ctx, backendSnapshot.ID, PropSpentRestoreMarker, string(encoded)); setErr != nil {
				return nil, fmt.Errorf("stamp backend snapshot %s: %w", backendSnapshot.ID, setErr)
			}
			// TrueNAS 26.0 has shipped middleware builds that acknowledge
			// pool.snapshot.update while silently dropping user-property writes.
			// Re-read and prove the marker before reporting classification; without
			// that proof this run must fail closed and can never start the age gate.
			verified, verifyErr := d.truenasClient.SnapshotGet(ctx, backendSnapshot.ID)
			if verifyErr != nil {
				return nil, fmt.Errorf("verify backend snapshot marker %s: %w", backendSnapshot.ID, verifyErr)
			}
			verifiedMarker, verifiedMarked, verifyMarkerErr := spentRestoreMarkerFromSnapshot(verified)
			if verifyMarkerErr != nil || !verifiedMarked || verifiedMarker != marker {
				return nil, fmt.Errorf("backend snapshot %s did not persist spent-restore marker", backendSnapshot.ID)
			}
			backendSnapshot = verified
			klog.Infof("Orphan reconcile: classified spent restore snapshot %s/%s and stamped backend snapshot %s",
				snapshot.GetNamespace(), snapshot.GetName(), backendSnapshot.ID)
		}
		classifiedAt, parseErr := time.Parse(time.RFC3339Nano, marker.ClassifiedAt)
		if parseErr != nil {
			return nil, fmt.Errorf("snapshot %s has invalid spent-restore timestamp: %w", backendSnapshot.ID, parseErr)
		}
		phase := corev1.PersistentVolumeClaimPhase("")
		if exists {
			phase = pvc.Status.Phase
		}
		createdAt := snapshot.GetCreationTimestamp().Time
		age := now.Sub(classifiedAt)
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
			ClassifiedAt:        classifiedAt,
			SourcePVCWasMissing: !exists,
		})
	}
	return spent, nil
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

func spentRestoreMarkerFromSnapshot(snapshot *truenas.Snapshot) (spentRestoreMarker, bool, error) {
	if snapshot == nil {
		return spentRestoreMarker{}, false, nil
	}
	property, exists := snapshot.UserProperties[PropSpentRestoreMarker]
	if !exists || property.Value == "" || property.Value == "-" ||
		strings.Contains(strings.ToLower(property.Source), "inherit") {
		return spentRestoreMarker{}, false, nil
	}
	var marker spentRestoreMarker
	if err := json.Unmarshal([]byte(property.Value), &marker); err != nil {
		return spentRestoreMarker{}, false, fmt.Errorf("invalid spent-restore marker: %w", err)
	}
	if marker.Version != spentRestoreMarkerVersion || marker.Classification != spentRestoreClassification ||
		marker.SnapshotID == "" || marker.SnapshotID != snapshot.ID || marker.ClassifiedAt == "" {
		return spentRestoreMarker{}, false, fmt.Errorf("invalid spent-restore marker contents")
	}
	return marker, true, nil
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
	marker, marked, markerErr := spentRestoreMarkerFromSnapshot(backendSnapshot)
	if markerErr != nil || !marked {
		return SpentRestoreSnapshot{}, false, fmt.Sprintf("backend spent-restore marker is unavailable or invalid: %v", markerErr)
	}
	classifiedAt, parseErr := time.Parse(time.RFC3339Nano, marker.ClassifiedAt)
	if parseErr != nil || !classifiedAt.Equal(detected.ClassifiedAt) {
		return SpentRestoreSnapshot{}, false, "backend spent-restore classification identity changed"
	}
	age := time.Since(classifiedAt)
	if age <= minOrphanAge {
		return SpentRestoreSnapshot{}, false, "spent-restore marker has not exceeded the minimum orphan age"
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
		ClassifiedAt:        classifiedAt,
		SourcePVCWasMissing: missing,
	}, true, ""
}

func (d *Driver) recordReconcileSkip(report *ReconcileReport, kind, id, reason string) {
	klog.Warningf("Orphan reconcile: guarded delete skipped kind=%s id=%s: %s", kind, id, reason)
	report.SkippedDeletes = append(report.SkippedDeletes, ReconcileActionFailure{Kind: kind, ID: id, Reason: reason})
}

func namespacedName(namespace, name string) string {
	return strings.TrimSuffix(namespace, "/") + "/" + strings.TrimPrefix(name, "/")
}

func (d *Driver) startOrphanReconcile() {
	if d.config == nil || !d.config.Reconcile.Enabled {
		klog.Info("Orphan reconcile detection disabled")
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

	ctx, cancel := context.WithCancel(context.Background())
	d.reconcileCancel = cancel
	d.reconcileWg.Add(1)
	go func() {
		defer d.reconcileWg.Done()
		klog.Infof("Orphan reconcile detection started: interval=%v minOrphanAge=%v delete=false", interval, minAge)
		run := func() {
			report, reconcileErr := d.ReconcileOrphans(ctx, ReconcileOptions{Delete: false, MinOrphanAge: minAge})
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
		ticker := time.NewTicker(interval)
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

func (d *Driver) stopOrphanReconcile() {
	if d.reconcileCancel != nil {
		d.reconcileCancel()
		d.reconcileWg.Wait()
	}
}
