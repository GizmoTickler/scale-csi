package driver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func TestReconcileOrphansDetectsOnlyOldNonLiveManagedResources(t *testing.T) {
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("live-volume", "csi.scale.io")},
		[]runtime.Object{reconcileSnapshotContent("live-content", "storage", "live-snapshot-object", "live-snapshot", "csi.scale.io")},
	)
	old := time.Now().Add(-48 * time.Hour)
	young := time.Now().Add(-30 * time.Minute)
	addReconcileDataset(client, "live-volume", old, true, 100)
	addReconcileDataset(client, "orphan-volume", old, true, 200)
	addReconcileDataset(client, "young-volume", young, true, 300)
	addReconcileDataset(client, "foreign-volume", old, false, 400)
	addReconcileSnapshot(t, client, "live-volume", "live-snapshot", old, true, 10)
	addReconcileSnapshot(t, client, "orphan-volume", "orphan-snapshot", old, true, 20)
	addReconcileSnapshot(t, client, "young-volume", "young-snapshot", young, true, 30)
	addReconcileSnapshot(t, client, "foreign-volume", "foreign-snapshot", old, false, 40)

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	require.Len(t, report.OrphanVolumes, 1)
	assert.Equal(t, "orphan-volume", report.OrphanVolumes[0].ID)
	assert.Equal(t, int64(200), report.OrphanVolumeBytes)
	require.Len(t, report.OrphanSnapshots, 1)
	assert.Equal(t, "orphan-snapshot", report.OrphanSnapshots[0].ID)
	assert.Equal(t, int64(20), report.OrphanSnapshotBytes)
	assert.Empty(t, report.DeletedVolumes)
	assert.Empty(t, report.DeletedSnapshots)
	assert.Empty(t, client.DatasetDeleteCalls)
}

func TestReconcileOrphansGuardedDeleteRefusesDependentVolume(t *testing.T) {
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("live-volume", "csi.scale.io")}, nil,
	)
	old := time.Now().Add(-48 * time.Hour)
	addReconcileDataset(client, "a-source", old, true, 100)
	clone := addReconcileDataset(client, "z-clone", old, true, 100)
	clone.Origin = truenas.DatasetProperty{
		Parsed: "pool/parent/a-source@dependency", Rawvalue: "pool/parent/a-source@dependency",
	}

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{
		Delete: true, MinOrphanAge: time.Hour,
	})
	require.NoError(t, err)
	assert.Contains(t, report.DeletedVolumes, "z-clone")
	require.NotEmpty(t, report.SkippedDeletes)
	assert.Equal(t, "volume", report.SkippedDeletes[0].Kind)
	assert.Equal(t, "a-source", report.SkippedDeletes[0].ID)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "dependent clones")
	_, getErr := client.DatasetGet(context.Background(), "pool/parent/a-source")
	require.NoError(t, getErr, "guarded DeleteVolume must leave the dependent-bearing source intact")
	for _, call := range client.DatasetDeleteCalls {
		assert.NotEqual(t, "pool/parent/a-source", call.Name, "reconcile must never bypass the pre-delete dependency guard")
	}
}

func TestReconcileVolumeDeleteFinalLiveGetVetoesNewPV(t *testing.T) {
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("live-volume", "csi.scale.io")}, nil,
	)
	addReconcileDataset(client, "late-volume", time.Now().Add(-48*time.Hour), true, 100)
	clientset, _, err := d.kubernetesReconcileClients()
	require.NoError(t, err)
	fakeClient := clientset.(*kubernetesfake.Clientset)
	fakeClient.PrependReactor("get", "persistentvolumes", func(action clienttesting.Action) (bool, runtime.Object, error) {
		get := action.(clienttesting.GetAction)
		if get.GetName() != "late-volume" {
			return false, nil, nil
		}
		return true, reconcilePV("late-volume", "csi.scale.io"), nil
	})

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.DeletedVolumes)
	require.Len(t, report.SkippedDeletes, 1)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "final live recheck")
	_, err = client.DatasetGet(context.Background(), "pool/parent/late-volume")
	require.NoError(t, err)
}

func TestReconcileSnapshotDeleteFinalLiveGetVetoesNewContent(t *testing.T) {
	d, client := newReconcileTestDriver(t, false, nil, []runtime.Object{
		reconcileSnapshotContent("live-content", "storage", "live-snapshot", "live-handle", "csi.scale.io"),
	})
	addReconcileSnapshot(t, client, "orphan-volume", "late-handle", time.Now().Add(-48*time.Hour), true, 20)
	backend := client.Snapshots["pool/parent/orphan-volume@late-handle"]
	backend.UserProperties[PropCSISnapshotName] = truenas.UserProperty{Value: "late-content", Source: "local"}
	_, dynamicClient, err := d.kubernetesReconcileClients()
	require.NoError(t, err)
	fakeDynamic := dynamicClient.(*dynamicfake.FakeDynamicClient)
	fakeDynamic.PrependReactor("get", "volumesnapshotcontents", func(action clienttesting.Action) (bool, runtime.Object, error) {
		get := action.(clienttesting.GetAction)
		if get.GetName() != "late-content" {
			return false, nil, nil
		}
		return true, reconcileSnapshotContent("late-content", "storage", "late-snapshot", "late-handle", "csi.scale.io"), nil
	})

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.DeletedSnapshots)
	require.Len(t, report.SkippedDeletes, 1)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "final live recheck")
	_, err = client.SnapshotGet(context.Background(), backend.ID)
	require.NoError(t, err)
}

func TestReconcileOrphansDeleteRefusesEmptyLivePVSetAfterDetection(t *testing.T) {
	d, client := newReconcileTestDriver(t, false, nil, nil)
	addReconcileDataset(client, "orphan-volume", time.Now().Add(-48*time.Hour), true, 200)

	detected, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, 1, detected.OrphanVolumeCount, "read-only detection must continue with an empty live set")

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{
		Delete: true, MinOrphanAge: time.Hour,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to GC: zero live PVs for driver but 1 managed backend volumes exist")
	assert.Equal(t, 1, report.OrphanVolumeCount, "detection must still report before delete is refused")
	assert.Equal(t, "orphan-volume", report.OrphanVolumes[0].ID)
	assert.Empty(t, report.DeletedVolumes)
	assert.Empty(t, client.DatasetDeleteCalls)
}

func TestReconcileOrphansDeleteCapIsEnforcedAcrossObjectTypes(t *testing.T) {
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("live-volume", "csi.scale.io")},
		[]runtime.Object{
			reconcileSnapshotContent("live-content", "storage", "live-snapshot", "live-snapshot", "csi.scale.io"),
		},
	)
	d.config.Reconcile.Delete.MaxPerRun = 2
	old := time.Now().Add(-48 * time.Hour)
	addReconcileSnapshot(t, client, "orphan-volume", "orphan-snapshot", old, true, 20)
	for _, volumeID := range []string{"orphan-d", "orphan-b", "orphan-a", "orphan-c"} {
		addReconcileDataset(client, volumeID, old, true, 100)
	}

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{
		Delete: true, MinOrphanAge: time.Hour,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"orphan-snapshot"}, report.DeletedSnapshots)
	assert.Equal(t, []string{"orphan-a"}, report.DeletedVolumes)
	require.Len(t, report.SkippedDeletes, 3)
	for _, skipped := range report.SkippedDeletes {
		assert.Equal(t, "volume", skipped.Kind)
		assert.Contains(t, skipped.Reason, "deletion cap reached")
	}
	assert.Len(t, client.DatasetDeleteCalls, 1)
}

func TestReconcileOrphansDeleteSkipsSnapshotsForEmptyLiveVSCSet(t *testing.T) {
	d, client := newReconcileTestDriver(t, false, nil, nil)
	addReconcileSnapshot(t, client, "orphan-volume", "orphan-snapshot", time.Now().Add(-48*time.Hour), true, 20)

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{
		Delete: true, MinOrphanAge: time.Hour,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, report.OrphanSnapshotCount)
	assert.Empty(t, report.DeletedSnapshots)
	require.Len(t, report.SkippedDeletes, 1)
	assert.Equal(t, "snapshot", report.SkippedDeletes[0].Kind)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "zero live VolumeSnapshotContents for driver")
	_, getErr := client.SnapshotGet(context.Background(), "pool/parent/orphan-volume@orphan-snapshot")
	require.NoError(t, getErr)
}

func TestReconcileOrphansDetectionSkipsHandlelessDriverVSC(t *testing.T) {
	d, client := newReconcileTestDriver(t, false, nil, []runtime.Object{
		reconcileSnapshotContent("binding-content", "storage", "binding-snapshot", "", "csi.scale.io"),
	})
	addReconcileSnapshot(t, client, "orphan-volume", "orphan-snapshot", time.Now().Add(-48*time.Hour), true, 20)

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, 1, report.OrphanSnapshotCount)
	assert.Equal(t, "orphan-snapshot", report.OrphanSnapshots[0].ID)
}

func TestReconcileOrphansDeleteSkipsSnapshotsForHandlelessDriverVSC(t *testing.T) {
	d, client := newReconcileTestDriver(t, false, nil, []runtime.Object{
		reconcileSnapshotContent("live-content", "storage", "live-snapshot", "live-snapshot", "csi.scale.io"),
		reconcileSnapshotContent("binding-content", "storage", "binding-snapshot", "", "csi.scale.io"),
	})
	addReconcileSnapshot(t, client, "orphan-volume", "orphan-snapshot", time.Now().Add(-48*time.Hour), true, 20)

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{
		Delete: true, MinOrphanAge: time.Hour,
	})
	require.NoError(t, err)
	assert.Empty(t, report.DeletedSnapshots)
	require.Len(t, report.SkippedDeletes, 1)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "no readable status.snapshotHandle")
	_, getErr := client.SnapshotGet(context.Background(), "pool/parent/orphan-volume@orphan-snapshot")
	require.NoError(t, getErr)
}

func TestReconcileOrphansUsesSnapshotIDBeforeSnapshotName(t *testing.T) {
	d, client := newReconcileTestDriver(t, false, nil, []runtime.Object{
		reconcileSnapshotContent("live-content", "storage", "live-snapshot", "live-snapshot", "csi.scale.io"),
	})
	addReconcileSnapshot(t, client, "live-volume", "live-snapshot", time.Now().Add(-48*time.Hour), true, 20)
	snapshot := client.Snapshots["pool/parent/live-volume@live-snapshot"]
	snapshot.Name = snapshot.ID

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.OrphanSnapshots, "the short CSI handle must be derived from the full backend snapshot ID")
}

func TestReconcileSnapshotHandleFallsBackToNameOnlyForUnparsableID(t *testing.T) {
	handle, ok := reconcileSnapshotHandle(&truenas.Snapshot{ID: "not-a-zfs-snapshot", Name: "fallback-name"})
	assert.True(t, ok)
	assert.Equal(t, "fallback-name", handle)

	handle, ok = reconcileSnapshotHandle(&truenas.Snapshot{
		ID: "pool/parent/volume@authoritative-name", Name: "stale-name",
	})
	assert.True(t, ok)
	assert.Equal(t, "authoritative-name", handle)
}

func TestReconcileSpentRestoreRecognitionRequiresDetachedAndNonBoundPVC(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	dynamicObjects := []runtime.Object{
		reconcileVolumeSnapshot("storage", "volsync-app-dst-dest", "released-source", "released-content", old),
		reconcileSnapshotContent("released-content", "storage", "volsync-app-dst-dest", "released-handle", "csi.scale.io"),
		reconcileVolumeSnapshot("storage", "volsync-db-dst-dest-final", "missing-source", "missing-content", old),
		reconcileSnapshotContent("missing-content", "storage", "volsync-db-dst-dest-final", "missing-handle", "csi.scale.io"),
		reconcileVolumeSnapshot("storage", "volsync-bound-dst-dest", "bound-source", "bound-content", old),
		reconcileSnapshotContent("bound-content", "storage", "volsync-bound-dst-dest", "bound-handle", "csi.scale.io"),
		reconcileVolumeSnapshot("storage", "manual-snapshot", "released-source", "manual-content", old),
		reconcileSnapshotContent("manual-content", "storage", "manual-snapshot", "manual-handle", "csi.scale.io"),
	}
	coreObjects := []runtime.Object{
		reconcilePVC("storage", "released-source", corev1.PersistentVolumeClaimPhase("Released")),
		reconcilePVC("storage", "bound-source", corev1.ClaimBound),
	}

	detached, backend := newReconcileTestDriver(t, true, coreObjects, dynamicObjects)
	for _, handle := range []string{"released-handle", "missing-handle", "bound-handle", "manual-handle"} {
		addReconcileSnapshot(t, backend, "restore-source", handle, old.Time, true, 1)
	}
	report, err := detached.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	require.Len(t, report.SpentRestoreSnapshots, 2)
	assert.Equal(t, "volsync-app-dst-dest", report.SpentRestoreSnapshots[0].Name)
	assert.Equal(t, corev1.PersistentVolumeClaimPhase("Released"), report.SpentRestoreSnapshots[0].SourcePVCPhase)
	assert.Equal(t, "volsync-db-dst-dest-final", report.SpentRestoreSnapshots[1].Name)
	assert.True(t, report.SpentRestoreSnapshots[1].SourcePVCWasMissing)

	attached, _ := newReconcileTestDriver(t, false, coreObjects, dynamicObjects)
	report, err = attached.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Empty(t, report.SpentRestoreSnapshots)
}

func TestReconcileDeletesSpentRestoreSnapshotOnlyThroughKubernetes(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	volumeSnapshot := reconcileVolumeSnapshot(
		"storage", "volsync-app-dst-dest", "gone-source", "spent-content", old,
	)
	content := reconcileSnapshotContent(
		"spent-content", "storage", "volsync-app-dst-dest", "still-live-handle", "csi.scale.io",
	)
	d, client := newReconcileTestDriver(t, true, nil, []runtime.Object{volumeSnapshot, content})
	addReconcileSnapshot(t, client, "restore-source", "still-live-handle", old.Time, true, 1)

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{
		Delete: true, MinOrphanAge: time.Hour,
	})
	require.NoError(t, err)
	assert.Empty(t, report.DeletedSpentRestoreObjects, "first classification must only stamp the durable marker")
	backendSnapshot, err := client.SnapshotFindByName(context.Background(), "pool/parent", "still-live-handle")
	require.NoError(t, err)
	require.NotNil(t, backendSnapshot)
	marker := spentRestoreMarker{
		Version:        spentRestoreMarkerVersion,
		Classification: spentRestoreClassification,
		SnapshotID:     backendSnapshot.ID,
		ClassifiedAt:   time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano),
	}
	encodedMarker, err := json.Marshal(marker)
	require.NoError(t, err)
	require.NoError(t, client.SnapshotSetUserProperty(context.Background(), backendSnapshot.ID, PropSpentRestoreMarker, string(encodedMarker)))

	report, err = d.ReconcileOrphans(context.Background(), ReconcileOptions{
		Delete: true, MinOrphanAge: time.Hour,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"storage/volsync-app-dst-dest"}, report.DeletedSpentRestoreObjects)
	_, dynamicClient, err := d.kubernetesReconcileClients()
	require.NoError(t, err)
	_, err = dynamicClient.Resource(volumeSnapshotGVR).Namespace("storage").Get(
		context.Background(), "volsync-app-dst-dest", metav1.GetOptions{},
	)
	assert.True(t, apierrors.IsNotFound(err))
	assert.Empty(t, client.DatasetDeleteCalls, "spent restore cleanup must not directly destroy a backend dataset")
}

func TestReconcileSpentRestoreClassificationFailsClosedWhenMarkerWriteIsNotDurable(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	volumeSnapshot := reconcileVolumeSnapshot(
		"storage", "volsync-app-dst-dest", "gone-source", "spent-content", old,
	)
	content := reconcileSnapshotContent(
		"spent-content", "storage", "volsync-app-dst-dest", "spent-handle", "csi.scale.io",
	)
	d, client := newReconcileTestDriver(t, true, nil, []runtime.Object{volumeSnapshot, content})
	addReconcileSnapshot(t, client, "restore-source", "spent-handle", old.Time, true, 1)
	client.SimulateUpdateNoOp = true

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not persist spent-restore marker")
	assert.Empty(t, report.SpentRestoreSnapshots)
	backend, getErr := client.SnapshotGet(context.Background(), "pool/parent/restore-source@spent-handle")
	require.NoError(t, getErr)
	assert.NotContains(t, backend.UserProperties, PropSpentRestoreMarker)
}

func newReconcileTestDriver(
	t *testing.T,
	detached bool,
	coreObjects []runtime.Object,
	dynamicObjects []runtime.Object,
) (*Driver, *truenas.MockClient) {
	t.Helper()
	scheme := runtime.NewScheme()
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{
			volumeSnapshotContentGVR: "VolumeSnapshotContentList",
			volumeSnapshotGVR:        "VolumeSnapshotList",
		},
		dynamicObjects...,
	)
	client := truenas.NewMockClient()
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS: ZFSConfig{
				DatasetParentName:            "pool/parent",
				DetachedVolumesFromSnapshots: detached,
			},
			NFS: NFSConfig{ShareHost: "192.0.2.10"},
			Reconcile: ReconcileConfig{
				MinOrphanAge: "24h",
				Delete: ReconcileDeleteConfig{
					MaxPerRun: 5,
				},
			},
		},
		truenasClient: client,
		eventRecorder: &EventRecorder{
			clientset:     kubernetesfake.NewSimpleClientset(coreObjects...),
			dynamicClient: dynamicClient,
		},
	}
	return d, client
}

func addReconcileDataset(
	client *truenas.MockClient,
	volumeID string,
	createdAt time.Time,
	managed bool,
	usedBytes int64,
) *truenas.Dataset {
	properties := map[string]truenas.UserProperty{}
	if managed {
		properties[PropManagedResource] = truenas.UserProperty{Value: "true", Source: "local"}
		properties[PropCSIVolumeName] = truenas.UserProperty{Value: volumeID, Source: "local"}
	}
	dataset := &truenas.Dataset{
		ID:             "pool/parent/" + volumeID,
		Name:           "pool/parent/" + volumeID,
		Type:           "FILESYSTEM",
		Creation:       truenas.DatasetProperty{Parsed: float64(createdAt.Unix())},
		Used:           truenas.DatasetProperty{Parsed: float64(usedBytes)},
		UserProperties: properties,
	}
	client.Datasets[dataset.Name] = dataset
	return dataset
}

func addReconcileSnapshot(
	t *testing.T,
	client *truenas.MockClient,
	volumeID string,
	snapshotID string,
	createdAt time.Time,
	managed bool,
	usedBytes int64,
) {
	t.Helper()
	properties := map[string]string{}
	if managed {
		properties[PropManagedResource] = "true"
		properties[PropCSISnapshotName] = snapshotID
		properties[PropCSISnapshotSourceVolumeID] = volumeID
	}
	snapshot, err := client.SnapshotCreate(
		context.Background(), "pool/parent/"+volumeID, snapshotID, properties,
	)
	require.NoError(t, err)
	snapshot.Properties["creation"] = map[string]interface{}{"parsed": float64(createdAt.Unix())}
	snapshot.Properties["used"] = map[string]interface{}{"parsed": float64(usedBytes)}
}

func reconcilePV(volumeHandle, driverName string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volumeHandle},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: driverName, VolumeHandle: volumeHandle},
			},
		},
	}
}

func reconcilePVC(namespace, name string, phase corev1.PersistentVolumeClaimPhase) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: phase},
	}
}

func reconcileSnapshotContent(
	name, namespace, snapshotName, handle, driverName string,
) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind":       "VolumeSnapshotContent",
		"metadata":   map[string]interface{}{"name": name},
		"spec": map[string]interface{}{
			"driver": driverName,
			"volumeSnapshotRef": map[string]interface{}{
				"namespace": namespace,
				"name":      snapshotName,
			},
		},
		"status": map[string]interface{}{"snapshotHandle": handle},
	}}
}

func reconcileVolumeSnapshot(
	namespace, name, sourcePVC, contentName string,
	creationTime metav1.Time,
) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "snapshot.storage.k8s.io/v1",
		"kind":       "VolumeSnapshot",
		"metadata": map[string]interface{}{
			"namespace":         namespace,
			"name":              name,
			"creationTimestamp": creationTime.Format(time.RFC3339),
		},
		"spec": map[string]interface{}{
			"source": map[string]interface{}{"persistentVolumeClaimName": sourcePVC},
		},
		"status": map[string]interface{}{"boundVolumeSnapshotContentName": contentName},
	}}
}
