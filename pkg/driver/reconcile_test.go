package driver

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

type republishOnDatasetGetClient struct {
	*truenas.MockClient
	datasetName string
	propertyKey string
	replacement publicationRecord
	armed       bool
}

func (c *republishOnDatasetGetClient) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	if c.armed && name == c.datasetName {
		c.armed = false
		if err := storePublicationRecord(ctx, c.MockClient, c.Datasets[name], name, c.propertyKey, c.replacement); err != nil {
			return nil, err
		}
	}
	return c.MockClient.DatasetGet(ctx, name)
}

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
	assert.Equal(t, []string{"storage/volsync-app-dst-dest"}, report.DeletedSpentRestoreObjects)
	_, dynamicClient, err := d.kubernetesReconcileClients()
	require.NoError(t, err)
	_, err = dynamicClient.Resource(volumeSnapshotGVR).Namespace("storage").Get(
		context.Background(), "volsync-app-dst-dest", metav1.GetOptions{},
	)
	assert.True(t, apierrors.IsNotFound(err))
	assert.Empty(t, client.DatasetDeleteCalls, "spent restore cleanup must not directly destroy a backend dataset")
}

func TestReconcileSpentRestoreUsesLaterCreationTimeWithoutSnapshotWrites(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	volumeSnapshot := reconcileVolumeSnapshot(
		"storage", "volsync-app-dst-dest", "gone-source", "spent-content", old,
	)
	content := reconcileSnapshotContent(
		"spent-content", "storage", "volsync-app-dst-dest", "spent-handle", "csi.scale.io",
	)
	d, client := newReconcileTestDriver(t, true, nil, []runtime.Object{volumeSnapshot, content})
	backendCreated := time.Now().Add(-30 * time.Minute)
	addReconcileSnapshot(t, client, "restore-source", "spent-handle", backendCreated, true, 1)
	// This models the TrueNAS 26.0 wire behavior: update calls can acknowledge
	// existing-snapshot properties without persisting them. Reconciliation must
	// make no such call and must conservatively use the newer backend creation.
	client.SimulateUpdateNoOp = true

	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	require.Len(t, report.SpentRestoreSnapshots, 1,
		"classification is immediate; only destructive GC is creation-age gated")
	assert.Less(t, report.SpentRestoreSnapshots[0].Age, time.Hour)
	assert.Zero(t, client.SnapshotSetCalls)
	assert.Zero(t, client.SnapshotRemoveCalls)

	backend := client.Snapshots["pool/parent/restore-source@spent-handle"]
	olderBackendCreated := time.Now().Add(-2 * time.Hour)
	backend.Properties["creation"] = map[string]interface{}{"parsed": float64(olderBackendCreated.Unix())}
	report, err = d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	require.Len(t, report.SpentRestoreSnapshots, 1)
	assert.Greater(t, report.SpentRestoreSnapshots[0].Age, time.Hour)
	assert.WithinDuration(t, olderBackendCreated, report.SpentRestoreSnapshots[0].ClassifiedAt, time.Second,
		"the later of the Kubernetes and backend creation times is the age origin")
	assert.Zero(t, client.SnapshotSetCalls)
	assert.Zero(t, client.SnapshotRemoveCalls)
}

func TestReconcileSpentRestoreMalformedObjectIsIsolatedAndMetricsStillPublish(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	malformed := reconcileSnapshotContent("malformed", "storage", "broken", "broken-handle", "csi.scale.io")
	malformed.Object["spec"].(map[string]interface{})["driver"] = map[string]interface{}{"not": "a string"}
	validSnapshot := reconcileVolumeSnapshot("storage", "volsync-good-dst-dest", "gone", "good-content", old)
	validContent := reconcileSnapshotContent("good-content", "storage", "volsync-good-dst-dest", "good-handle", "csi.scale.io")
	d, client := newReconcileTestDriver(t, true, nil, []runtime.Object{malformed, validSnapshot, validContent})
	addReconcileSnapshot(t, client, "restore-source", "good-handle", old.Time, true, 1)

	failureMetric := reconcileFailuresTotal.WithLabelValues("snapshot_content_classification")
	failuresBefore := testutil.ToFloat64(failureMetric)
	successBefore := testutil.ToFloat64(reconcileLastSuccessTimestamp)
	report, err := d.ReconcileOrphans(context.Background(), ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	require.Len(t, report.SpentRestoreSnapshots, 1)
	assert.Equal(t, "volsync-good-dst-dest", report.SpentRestoreSnapshots[0].Name)
	assert.Equal(t, failuresBefore+1, testutil.ToFloat64(failureMetric))
	assert.GreaterOrEqual(t, testutil.ToFloat64(reconcileLastSuccessTimestamp), successBefore)
	assert.Equal(t, float64(report.SpentRestoreSnapshotCount), testutil.ToFloat64(spentRestoreSnapshots))
}

func TestStalePublicationRecordRequiresContinuousAbsenceThenRevokes(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	d.config.Fencing = FencingConfig{Mode: FencingModeAdditive, StaleRecordGracePeriod: "10m"}
	d.config.NFS.ShareAllowedNetworks = []string{"192.0.2.0/24"}
	dataset := addReconcileDataset(client, "stale-publication", time.Now().Add(-time.Hour), true, 1)
	dataset.Mountpoint = "/mnt/pool/parent/stale-publication"
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Hosts: []string{"192.0.2.11"}, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	record, err := newPublicationRecord(NodeIdentity{Name: "gone-worker", IPs: []net.IP{net.ParseIP("192.0.2.11")}},
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false)
	require.NoError(t, err)
	record.CSIAddedNFSHosts = []string{"192.0.2.11"}
	key := publicationPropertyKey(record.Node)
	require.NoError(t, storePublicationRecord(ctx, client, dataset, dataset.Name, key, record))
	state := &kubernetesReconcileState{liveVolumeAttachments: map[string]struct{}{}, volumeAttachmentCount: 0}

	observedAt := time.Now()
	d.reconcileStalePublicationRecords(ctx, []*truenas.Dataset{dataset}, state, observedAt)
	dataset, err = client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	_, present := dataset.UserProperties[key]
	assert.True(t, present, "the first absence only starts the grace window")

	d.reconcileStalePublicationRecords(ctx, []*truenas.Dataset{dataset}, state, observedAt.Add(11*time.Minute))
	dataset, err = client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	_, present = dataset.UserProperties[key]
	assert.False(t, present)
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Empty(t, share.Hosts)
	assert.Equal(t, []string{"192.0.2.0/24"}, share.Networks,
		"additive cleanup removes only the CSI-added host grant")
}

func TestStaleLegacyPublicationWithoutProvenancePreservesMatchingNFSHost(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	d.config.Fencing = FencingConfig{Mode: FencingModeAdditive, StaleRecordGracePeriod: "1ns"}
	d.config.NFS.ShareAllowedNetworks = []string{"192.0.2.0/24"}
	dataset := addReconcileDataset(client, "legacy-static-publication", time.Now().Add(-time.Hour), true, 1)
	dataset.Mountpoint = "/mnt/pool/parent/legacy-static-publication"
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Hosts: []string{"192.0.2.11"}, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	record, err := newPublicationRecord(NodeIdentity{Name: "gone-worker", IPs: []net.IP{net.ParseIP("192.0.2.11")}},
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false)
	require.NoError(t, err)
	key := publicationPropertyKey(record.Node)
	require.NoError(t, storePublicationRecord(ctx, client, dataset, dataset.Name, key, record))
	state := &kubernetesReconcileState{liveVolumeAttachments: map[string]struct{}{}, volumeAttachmentCount: 0}

	observedAt := time.Now()
	d.reconcileStalePublicationRecords(ctx, []*truenas.Dataset{dataset}, state, observedAt)
	d.reconcileStalePublicationRecords(ctx, []*truenas.Dataset{dataset}, state, observedAt.Add(time.Second))
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	assert.NotContains(t, fresh.UserProperties, key, "the stale legacy record itself is still retired")
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.11"}, share.Hosts,
		"an old record without CSI-added provenance cannot authorize static policy removal")
}

func TestStalePublicationGraceResetsWhenRecordIsRepublished(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	d.config.Fencing = FencingConfig{Mode: FencingModeAdditive, StaleRecordGracePeriod: "10m"}
	d.config.NFS.ShareAllowedNetworks = []string{"192.0.2.0/24"}
	dataset := addReconcileDataset(client, "republished-record", time.Now().Add(-time.Hour), true, 1)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	record, err := newPublicationRecord(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.11")}},
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false)
	require.NoError(t, err)
	key := publicationPropertyKey(record.Node)
	require.NoError(t, storePublicationRecord(ctx, client, dataset, dataset.Name, key, record))
	state := &kubernetesReconcileState{liveVolumeAttachments: map[string]struct{}{}, volumeAttachmentCount: 0}
	t0 := time.Now()
	d.reconcileStalePublicationRecords(ctx, []*truenas.Dataset{dataset}, state, t0)

	// A successful republish of the same (volume,node) is a new generation even
	// when its transport identity is unchanged. The old absence timer cannot be
	// applied to it.
	record.UpdatedAt = t0.Add(time.Minute).UTC().Format(time.RFC3339Nano)
	require.NoError(t, storePublicationRecord(ctx, client, dataset, dataset.Name, key, record))
	d.reconcileStalePublicationRecords(ctx, []*truenas.Dataset{dataset}, state, t0.Add(11*time.Minute))
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	_, present := fresh.UserProperties[key]
	assert.True(t, present, "republish must restart the continuous-absence grace period")

	d.reconcileStalePublicationRecords(ctx, []*truenas.Dataset{fresh}, state, t0.Add(22*time.Minute))
	fresh, err = client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	_, present = fresh.UserProperties[key]
	assert.False(t, present)
}

func TestStalePublicationRechecksGenerationAtDestructiveBoundary(t *testing.T) {
	ctx := context.Background()
	d, base := newReconcileTestDriver(t, false, nil, nil)
	d.config.Fencing = FencingConfig{Mode: FencingModeAdditive, StaleRecordGracePeriod: "1ns"}
	dataset := addReconcileDataset(base, "boundary-republish", time.Now().Add(-time.Hour), true, 1)
	record, err := newPublicationRecord(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.11")}},
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false)
	require.NoError(t, err)
	key := publicationPropertyKey(record.Node)
	require.NoError(t, storePublicationRecord(ctx, base, dataset, dataset.Name, key, record))
	state := &kubernetesReconcileState{liveVolumeAttachments: map[string]struct{}{}, volumeAttachmentCount: 0}
	t0 := time.Now()
	d.reconcileStalePublicationRecords(ctx, []*truenas.Dataset{dataset}, state, t0)

	replacement := record
	replacement.UpdatedAt = t0.Add(time.Second).UTC().Format(time.RFC3339Nano)
	wrapper := &republishOnDatasetGetClient{
		MockClient: base, datasetName: dataset.Name, propertyKey: key, replacement: replacement, armed: true,
	}
	d.truenasClient = wrapper
	d.reconcileStalePublicationRecords(ctx, []*truenas.Dataset{dataset}, state, t0.Add(time.Second))
	fresh, err := base.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(fresh)
	require.NoError(t, err)
	require.Contains(t, records, key)
	assert.Equal(t, replacement.UpdatedAt, records[key].UpdatedAt,
		"a republish racing the final read must veto revocation of the old generation")
}

func TestRunOnceOrphanReconcileNeverMutatesFencingRecords(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	d.config.Fencing = FencingConfig{Mode: FencingModeStrict, StaleRecordGracePeriod: "1ns"}
	dataset := addReconcileDataset(client, "cronjob-must-not-fence", time.Now().Add(-time.Hour), true, 1)
	record, err := newPublicationRecord(NodeIdentity{Name: "gone-worker", IPs: []net.IP{net.ParseIP("192.0.2.11")}},
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false)
	require.NoError(t, err)
	key := publicationPropertyKey(record.Node)
	require.NoError(t, storePublicationRecord(ctx, client, dataset, dataset.Name, key, record))

	_, err = d.ReconcileOrphans(ctx, ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	assert.Contains(t, fresh.UserProperties, key)
	_, observed := d.stalePublicationRecordsSeen.Load(stalePublicationObservationKey(dataset.Name, key))
	assert.False(t, observed, "the separate --mode=reconcile process must never enter the fencing writer path")
}

func TestControllerReconcileCadenceDoesNotExceedStaleGrace(t *testing.T) {
	assert.Equal(t, 10*time.Minute, controllerReconcileCadence(time.Hour, 10*time.Minute))
	assert.Equal(t, 5*time.Minute, controllerReconcileCadence(5*time.Minute, 10*time.Minute))
}

func TestStalePublicationMassAbsenceBrakeDefersAllRecords(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false, nil, nil)
	d.config.Fencing = FencingConfig{Mode: FencingModeStrict, StaleRecordGracePeriod: "1ns"}
	datasets := make([]*truenas.Dataset, 0, 2)
	for index := 0; index < 2; index++ {
		dataset := addReconcileDataset(client, "brake-"+strconv.Itoa(index), time.Now().Add(-time.Hour), true, 1)
		record, err := newPublicationRecord(NodeIdentity{Name: "worker-" + strconv.Itoa(index), IPs: []net.IP{net.ParseIP("192.0.2.11")}},
			csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false)
		require.NoError(t, err)
		require.NoError(t, storePublicationRecord(ctx, client, dataset, dataset.Name, publicationPropertyKey(record.Node), record))
		datasets = append(datasets, dataset)
	}
	metricBefore := testutil.ToFloat64(fencingStaleDeferredTotal)
	d.reconcileStalePublicationRecords(ctx, datasets,
		&kubernetesReconcileState{liveVolumeAttachments: map[string]struct{}{}, volumeAttachmentCount: 0}, time.Now())
	assert.Equal(t, metricBefore+1, testutil.ToFloat64(fencingStaleDeferredTotal))
	for _, dataset := range datasets {
		fresh, err := client.DatasetGet(ctx, dataset.Name)
		require.NoError(t, err)
		records, err := publicationRecordsFromDataset(fresh)
		require.NoError(t, err)
		assert.Len(t, records, 1)
	}
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
