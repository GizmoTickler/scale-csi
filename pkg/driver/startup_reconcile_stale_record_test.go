package driver

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// ---------------------------------------------------------------------------
// C11 — a stale publication record blocks strict-mode startup convergence for
// the WHOLE CLUSTER, not just the one volume it actually concerns.
//
// reconcileStartupFencingVolume validates each live publication against the
// dataset's stored publication records via validatePublicationCompatibility.
// A record left in state "published" for a node whose VolumeAttachment was
// force-removed (finalizer stripped) — exactly the condition
// reconcileStalePublicationRecords exists to revoke, given continuous absence
// for fencing.staleRecordGracePeriod — makes that check fail FailedPrecondition
// against the CURRENTLY live node's own publish. That error used to join
// reconcilePublishedAttachments' per-pass errors.Join, so in FencingModeStrict
// d.ready never becomes true and CreateVolume / ControllerPublishVolume /
// ControllerExpandVolume are refused cluster-wide until the periodic sweep
// happens to revoke that one record.
//
// This mirrors the shape of startup_reconcile_geometry_test.go's permanent
// geometry carve-out exactly (same fixture helpers pattern, same two-test
// split: "converges around it" + "strict readiness is not held down by it").
// ---------------------------------------------------------------------------

// staleRecordVolume appends the PV / VolumeAttachment / CSINode trio for one
// LIVE NFS volume attached to node "worker-<volumeID>", matching the objects
// reconcilePublishedAttachments reads. Returns the identity so the caller can
// derive the SAME live node name the fixture used.
func staleRecordVolume(t *testing.T, objects []runtime.Object, volumeID string) ([]runtime.Object, NodeIdentity) { //nolint:unparam // the returned identity is part of the documented contract above ("so the caller can derive the SAME live node name"); today's one caller happens to discard it via _
	t.Helper()
	pvName := "pv-" + volumeID
	identity := NodeIdentity{Name: "worker-" + volumeID, IPs: []net.IP{net.ParseIP("192.0.2.11")}}
	nodeID, err := encodeNodeIdentity(identity)
	require.NoError(t, err)
	objects = append(objects,
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvName},
			Spec: corev1.PersistentVolumeSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "csi.scale.io", VolumeHandle: volumeID,
					VolumeAttributes: map[string]string{"node_attach_driver": "nfs"},
				}},
			},
		},
		&storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: "va-" + volumeID},
			Spec: storagev1.VolumeAttachmentSpec{
				Attacher: "csi.scale.io", NodeName: identity.Name,
				Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
			},
			Status: storagev1.VolumeAttachmentStatus{Attached: true},
		},
		&storagev1.CSINode{
			ObjectMeta: metav1.ObjectMeta{Name: identity.Name},
			Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{
				{Name: "csi.scale.io", NodeID: nodeID},
			}},
		},
	)
	return objects, identity
}

// newStaleRecordDriver builds a strict-fencing, NFS-only controller.
func newStaleRecordDriver(client truenas.ClientInterface, kube *kubernetesfake.Clientset, recorder *record.FakeRecorder) *Driver {
	return &Driver{
		name: "csi.scale.io",
		config: &Config{
			DriverName: "csi.scale.io",
			Fencing:    FencingConfig{Mode: FencingModeStrict, StartupReconcileTimeout: "5s"},
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			NFS:        NFSConfig{Enabled: true, ShareHost: "192.0.2.10", ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
		eventRecorder: &EventRecorder{recorder: recorder, clientset: kube, enabled: true},
	}
}

// staleRecordNFSVolume provisions an NFS-shared dataset and stamps a STALE
// "published" record for staleNode — a node that carries NO live
// VolumeAttachment for this volume in the fixture (see staleRecordVolume,
// which only ever creates the trio for the LIVE node). This is the exact
// backend shape a force-removed VA finalizer leaves behind: the record
// survives on the dataset with nothing in Kubernetes to prove it live.
func staleRecordNFSVolume(t *testing.T, client *truenas.MockClient, volumeID, staleNode string) {
	t.Helper()
	ctx := context.Background()
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/" + volumeID, Type: "FILESYSTEM",
	})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, fmt.Sprint(share.ID)))

	staleIdentity := NodeIdentity{Name: staleNode, IPs: []net.IP{net.ParseIP("192.0.2.99")}}
	record, err := newPublicationRecord(staleIdentity, csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false)
	require.NoError(t, err)
	key := publicationPropertyKey(staleNode)
	require.NoError(t, storePublicationRecord(ctx, client, dataset, dataset.Name, key, record))
}

// TestStartupReconcileConvergesAroundAStaleFencingRecord is the C11
// regression.
func TestStartupReconcileConvergesAroundAStaleFencingRecord(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := truenas.NewMockClient()
	var objects []runtime.Object
	objects, _ = staleRecordVolume(t, objects, "quarantine")
	recorder := record.NewFakeRecorder(16)
	d := newStaleRecordDriver(client, kubernetesfake.NewSimpleClientset(objects...), recorder)
	staleRecordNFSVolume(t, client, "quarantine", "worker-evicted")

	require.NoError(t, d.reconcilePublishedAttachments(ctx),
		"a conflict whose only blocking record has no live VolumeAttachment must not be reported as non-convergence")

	// The refused volume is not silently dropped: it is surfaced on its own PV.
	events := drainEvents(recorder)
	var warning string
	for _, event := range events {
		if strings.Contains(event, "StartupFencingStaleRecordConflict") {
			warning = event
		}
	}
	require.NotEmpty(t, warning, "the quarantine must reach the operator as a PV Event, not only a klog line (%v)", events)
	assert.Contains(t, warning, corev1.EventTypeWarning)
	assert.Contains(t, warning, "worker-evicted", "the Event must name the stale node")

	// The visibility gauge (C11) names the quarantined volume.
	assert.Equal(t, float64(1), testutil.ToFloat64(startupFencingUnconvergedVolumes.WithLabelValues("quarantine")),
		"scale_csi_startup_fencing_unconverged_volumes must mark the quarantined volume")
}

// TestStrictReadinessIsNotHeldDownByAStaleFencingRecord is the half that
// matters operationally: with the conflict classified as a per-volume
// condition, the strict-mode controller reaches ready.
func TestStrictReadinessIsNotHeldDownByAStaleFencingRecord(t *testing.T) {
	client := truenas.NewMockClient()
	var objects []runtime.Object
	objects, _ = staleRecordVolume(t, objects, "quarantine-ready")
	d := newStaleRecordDriver(client, kubernetesfake.NewSimpleClientset(objects...), record.NewFakeRecorder(16))
	staleRecordNFSVolume(t, client, "quarantine-ready", "worker-evicted-ready")
	d.ready.Store(false)

	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)
	require.Eventually(t, d.ready.Load, 3*time.Second, 10*time.Millisecond,
		"a stale record with no live VolumeAttachment must not block controller readiness cluster-wide")
}

// TestStartupReconcileStillBlocksOnGenuineDualVAConflict pins the OTHER half
// of the classification: when the conflicting record's node DOES have a live
// VolumeAttachment (a real, if transient, dual-VA state — normal during
// migration per the existing comment in reconcileStartupFencingVolume), the
// stale-record carve-out must NOT apply and the pass must keep blocking
// exactly as before.
func TestStartupReconcileStillBlocksOnGenuineDualVAConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := truenas.NewMockClient()
	var objects []runtime.Object //nolint:prealloc // staleRecordVolume replaces this slice with its own return value before the local append below, so a capacity hint here would not carry through
	objects, _ = staleRecordVolume(t, objects, "dual-va")
	// A SECOND live VolumeAttachment for a different node on the SAME volume —
	// both nodes are genuinely live, so the conflicting record is NOT stale.
	secondIdentity := NodeIdentity{Name: "worker-dual-va-2", IPs: []net.IP{net.ParseIP("192.0.2.12")}}
	secondNodeID, err := encodeNodeIdentity(secondIdentity)
	require.NoError(t, err)
	pvName := "pv-dual-va"
	objects = append(objects,
		&storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: "va-dual-va-2"},
			Spec: storagev1.VolumeAttachmentSpec{
				Attacher: "csi.scale.io", NodeName: secondIdentity.Name,
				Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
			},
			Status: storagev1.VolumeAttachmentStatus{Attached: true},
		},
		&storagev1.CSINode{
			ObjectMeta: metav1.ObjectMeta{Name: secondIdentity.Name},
			Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{
				{Name: "csi.scale.io", NodeID: secondNodeID},
			}},
		},
	)
	recorder := record.NewFakeRecorder(16)
	d := newStaleRecordDriver(client, kubernetesfake.NewSimpleClientset(objects...), recorder)
	// Stamp a published record for the FIRST live node so the second live
	// node's publish hits the same-volume-different-node conflict path.
	staleRecordNFSVolume(t, client, "dual-va", "worker-dual-va")

	err = d.reconcilePublishedAttachments(ctx)
	require.Error(t, err, "a conflict against a node that IS live must keep blocking convergence")
	for _, event := range drainEvents(recorder) {
		assert.NotContains(t, event, "StartupFencingStaleRecordConflict",
			"a genuine dual-VA conflict is not the stale-record condition")
	}
}

// TestQuarantinedVolumeEventuallyConverges is the fix/strict-quarantine
// regression. quarantineStaleStartupFencingVolume (C11) returns nil so ONE
// volume blocked only by a stale publication record does not hold strict-mode
// readiness down for the whole cluster — that part is correct and preserved
// here (ready still latches true). The defect introduced alongside it: since
// the pass-level error was nil, the pre-fix strict branch in
// startStartupAttachmentReconcile stored ready=true and RETURNED, permanently
// exiting the only goroutine that ever calls reconcilePublishedAttachments.
// The quarantined volume's own publication record and backend fence — both
// only written inside the SAME per-volume pass, after the compatibility
// check that triggers the quarantine — were therefore never applied, and
// nothing was left running to retry once the stale record blocking it was
// revoked.
//
// This test proves the fix, not just the carve-out: after
// revokeStalePublicationRecord (the periodic stale-record sweep's per-record
// action) revokes the stale record, the previously-quarantined volume's own
// record must eventually converge and the per-volume gauge must clear —
// without a second controller restart to re-launch the goroutine.
func TestQuarantinedVolumeEventuallyConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := truenas.NewMockClient()
	var objects []runtime.Object
	objects, liveIdentity := staleRecordVolume(t, objects, "converge")
	kube := kubernetesfake.NewSimpleClientset(objects...)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			volumeSnapshotContentGVR: "VolumeSnapshotContentList",
			volumeSnapshotGVR:        "VolumeSnapshotList",
		},
	)
	d := newStaleRecordDriver(client, kube, record.NewFakeRecorder(16))
	// revokeStalePublicationRecord -> liveVolumeAttachmentExists needs a
	// dynamic client too (kubernetesReconcileClients requires both); the
	// shared newStaleRecordDriver helper only wires the plain clientset used
	// by reconcilePublishedAttachments itself.
	d.eventRecorder.dynamicClient = dynamicClient
	staleRecordNFSVolume(t, client, "converge", "worker-departed")

	const datasetName = "pool/parent/converge"
	liveKey := publicationPropertyKey(liveIdentity.Name)
	staleKey := publicationPropertyKey("worker-departed")

	dataset, err := client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(dataset)
	require.NoError(t, err)
	require.Contains(t, records, staleKey, "fixture precondition: the stale record must be present")
	require.NotContains(t, records, liveKey, "fixture precondition: the live node has no record yet")

	d.ready.Store(false)
	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)

	require.Eventually(t, d.ready.Load, 3*time.Second, 10*time.Millisecond,
		"a stale record with no live VolumeAttachment must not block controller readiness cluster-wide")

	// This is the line that distinguishes a genuine quarantine (deferred) from
	// an already-converged pass: readiness is true, but the live node's own
	// publication record must NOT exist yet, because quarantine returns from
	// inside the publication loop before persisting it.
	dataset, err = client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	records, err = publicationRecordsFromDataset(dataset)
	require.NoError(t, err)
	require.NotContains(t, records, liveKey,
		"the quarantined volume's own publication record must not be written until the stale record is revoked")
	require.Equal(t, float64(1), testutil.ToFloat64(startupFencingUnconvergedVolumes.WithLabelValues("converge")),
		"the volume must still show as unconverged while quarantined")

	// Simulate what the periodic stale-record sweep does once continuous
	// absence has been observed for fencing.staleRecordGracePeriod: revoke the
	// stale record under the per-volume lock.
	staleRecord := records[staleKey]
	revoked, err := d.revokeStalePublicationRecord(ctx, datasetName, "converge", staleKey, staleRecord, 1)
	require.NoError(t, err)
	require.True(t, revoked, "the stale record has no live VolumeAttachment and must be revoked")

	// The revoke must re-drive startup convergence for the volume it was
	// blocking: the live node's own record eventually appears. On pre-fix
	// code this never happens because the goroutine that would write it
	// already returned permanently when readiness first latched true.
	require.Eventually(t, func() bool {
		dataset, err := client.DatasetGet(ctx, datasetName)
		if err != nil {
			return false
		}
		records, err := publicationRecordsFromDataset(dataset)
		if err != nil {
			return false
		}
		_, converged := records[liveKey]
		return converged
	}, 3*time.Second, 10*time.Millisecond,
		"a quarantined volume must eventually converge once the stale record blocking it is revoked, not be abandoned permanently")

	// (C11 gauge latch) Once genuinely converged, the volume must drop out of
	// the unconverged gauge instead of latching at 1 for the life of the pod.
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(startupFencingUnconvergedVolumes.WithLabelValues("converge")) == 0
	}, 3*time.Second, 10*time.Millisecond,
		"the per-volume gauge must clear once the volume converges, not latch at 1 forever")
}
