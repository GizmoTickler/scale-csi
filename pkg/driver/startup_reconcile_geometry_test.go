package driver

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// ---------------------------------------------------------------------------
// GF-4 round 8, HIGH-2 — the PERMANENT geometry refusal inside the startup
// attachment reconcile.
//
// resolveExtentGeometry refuses (FailedPrecondition) for an iSCSI volume whose
// extent is ABSENT and which can neither state its geometry nor prove it holds
// no block data. That refusal is correct. What is not is where it landed: the
// startup reconcile joined it into the per-pass error set, and in
// FencingModeStrict any non-nil pass holds d.ready false — which blocks
// CreateVolume / ControllerPublishVolume / ControllerExpandVolume for the WHOLE
// cluster. Every other failure on that path is transient and self-heals; this
// one clears only when an operator runs zfs set, so the retry loop never
// converges.
//
// The shape below is the reachable one: TrueNAS is restored from a
// configuration backup (or an upgrade loses the iSCSI extent config) while the
// zvols and the Kubernetes VolumeAttachments survive.
// ---------------------------------------------------------------------------

// startupGeometryVolume appends the PV / VolumeAttachment / CSINode trio for one
// attached volume, matching the objects reconcilePublishedAttachments reads.
func startupGeometryVolume(t *testing.T, objects []runtime.Object, volumeID, protocol string) []runtime.Object {
	t.Helper()
	pvName := "pv-" + volumeID
	identity := NodeIdentity{Name: "worker-" + volumeID, IPs: []net.IP{net.ParseIP("192.0.2.11")}}
	if protocol == "iscsi" {
		identity.ISCSIIQN = "iqn.1993-08.org.debian:" + volumeID
	}
	nodeID, err := encodeNodeIdentity(identity)
	require.NoError(t, err)
	return append(objects,
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvName},
			Spec: corev1.PersistentVolumeSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "csi.scale.io", VolumeHandle: volumeID,
					VolumeAttributes: map[string]string{"node_attach_driver": protocol},
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
}

// newStartupGeometryDriver builds a strict-fencing controller with both an NFS
// and an iSCSI backend configured.
func newStartupGeometryDriver(client truenas.ClientInterface, kube *kubernetesfake.Clientset, recorder *record.FakeRecorder) *Driver {
	return &Driver{
		name: "csi.scale.io",
		config: &Config{
			DriverName: "csi.scale.io",
			Fencing:    FencingConfig{Mode: FencingModeStrict, StartupReconcileTimeout: "5s"},
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			NFS:        NFSConfig{Enabled: true, ShareHost: "192.0.2.10", ShareAllowedNetworks: []string{"192.0.2.0/24"}},
			ISCSI: ISCSIConfig{
				Enabled: true, TargetPortal: "192.0.2.10:3260", ExtentBlocksize: 512, ExtentRpm: "SSD",
			},
		},
		truenasClient: client,
		eventRecorder: &EventRecorder{recorder: recorder, clientset: kube, enabled: true},
	}
}

// unestablishableGeometryZvol is the post-restore shape: an attached iSCSI
// volume this driver really provisioned, whose EXTENT no longer exists (the
// TrueNAS config was restored from a backup, or an admin/upgrade lost it) and
// which records no geometry of its own because it predates GF-4. Its own
// extent-ID bookkeeping is the witness that it HAS been block-addressed, so it
// cannot be treated as data-free either — ensureShareExists refuses it, and no
// retry ever clears that.
func unestablishableGeometryZvol(t *testing.T, d *Driver, client *truenas.MockClient, volumeID string) {
	t.Helper()
	ctx := context.Background()
	datasetName := "pool/parent/" + volumeID
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropDriverInstanceID: d.driverInstanceID(),
	}))
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, volumeID, nil))

	// The extent is gone; the target and the volume's own extent-ID bookkeeping
	// survive, and the geometry was never recorded (pre-GF4 volume).
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NotNil(t, extent)
	require.NoError(t, client.ISCSIExtentDelete(ctx, extent.ID, false, true))
	require.NoError(t, client.DatasetRemoveUserProperties(ctx, datasetName,
		[]string{PropBlockISCSIBlocksize, PropBlockISCSIPblocksize}))
}

// convergeableNFSVolume is an ordinary attached NFS volume whose share exists.
func convergeableNFSVolume(t *testing.T, client *truenas.MockClient, volumeID string) {
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
}

// TestStartupReconcileConvergesAroundThePermanentGeometryRefusal is the HIGH-2
// regression.
//
// FAILS PRE-FIX: yes. VERIFIED EMPIRICALLY in this worktree by deleting the
// `errors.As(err, &permanent)` arm from reconcileStartupFencingVolume (its
// pre-cc258eb shape, where the refusal was returned like any other error) and
// re-running: reconcilePublishedAttachments returns non-nil, no Event is
// emitted, and — in the strict-readiness subtest — d.ready never becomes true,
// so every assertion below fails.
func TestStartupReconcileConvergesAroundThePermanentGeometryRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := truenas.NewMockClient()
	var objects []runtime.Object
	objects = startupGeometryVolume(t, objects, "wedged", "iscsi")
	objects = startupGeometryVolume(t, objects, "healthy", "nfs")
	recorder := record.NewFakeRecorder(16)
	d := newStartupGeometryDriver(client, kubernetesfake.NewSimpleClientset(objects...), recorder)
	unestablishableGeometryZvol(t, d, client, "wedged")
	convergeableNFSVolume(t, client, "healthy")

	require.NoError(t, d.reconcilePublishedAttachments(ctx),
		"a refusal only an operator can clear must not be reported as non-convergence")

	// The unrelated volume still converged in the same pass.
	healthy, err := client.DatasetGet(ctx, "pool/parent/healthy")
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(healthy)
	require.NoError(t, err)
	assert.Len(t, records, 1, "one un-resolvable volume must not stop every other volume from converging")

	// The refused volume is not silently dropped: it is surfaced on its own PV.
	events := drainEvents(recorder)
	var warning string
	for _, event := range events {
		if strings.Contains(event, "StartupShareGeometryUnestablishable") {
			warning = event
		}
	}
	require.NotEmpty(t, warning, "the refusal must reach the operator as a PV Event, not only a klog line (%v)", events)
	assert.Contains(t, warning, corev1.EventTypeWarning)
	assert.Contains(t, warning, "zfs set", "the Event must carry the recovery command")
	assert.Contains(t, warning, PropBlockISCSIBlocksize)

	// And no backend object was created for the refused volume.
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/wedged")
	require.NoError(t, err)
	assert.Nil(t, extent, "the refusal must not have re-created the extent at the controller default")
}

// TestStrictReadinessIsNotHeldDownByThePermanentGeometryRefusal is the half that
// matters operationally: with the refusal classified as a per-volume condition,
// the strict-mode controller reaches ready and serves CreateVolume /
// ControllerPublishVolume / ControllerExpandVolume for every other volume.
func TestStrictReadinessIsNotHeldDownByThePermanentGeometryRefusal(t *testing.T) {
	client := truenas.NewMockClient()
	var objects []runtime.Object
	objects = startupGeometryVolume(t, objects, "wedged-ready", "iscsi")
	objects = startupGeometryVolume(t, objects, "healthy-ready", "nfs")
	d := newStartupGeometryDriver(client, kubernetesfake.NewSimpleClientset(objects...), record.NewFakeRecorder(16))
	unestablishableGeometryZvol(t, d, client, "wedged-ready")
	convergeableNFSVolume(t, client, "healthy-ready")
	d.ready.Store(false)

	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)
	require.Eventually(t, d.ready.Load, 3*time.Second, 10*time.Millisecond,
		"a refusal no retry can clear must not block controller readiness cluster-wide")
}

// extentCreateFailingClient fails iSCSI extent creation, which is a TRANSIENT
// ensureShareExists failure (an API error) — the class that must keep blocking
// convergence.
type extentCreateFailingClient struct {
	*truenas.MockClient
}

func (c *extentCreateFailingClient) ISCSIExtentCreate(ctx context.Context, name, diskPath, serial string, blocksize int, pblocksize bool, rpm string, opts ...truenas.ISCSIExtentCreateOptions) (*truenas.ISCSIExtent, error) {
	return nil, fmt.Errorf("injected: iscsi.extent.create returned 503")
}

// TestStartupReconcileStillBlocksOnTransientShareFailures pins the OTHER half of
// the classification. Only the permanent geometry refusal is exempt; an ordinary
// ensureShareExists error is transient, self-heals on retry, and must keep
// holding convergence (and with it strict readiness) exactly as before — if it
// did not, the startup reconcile would report converged while a fence had never
// been applied.
func TestStartupReconcileStillBlocksOnTransientShareFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mock := truenas.NewMockClient()
	var objects []runtime.Object
	objects = startupGeometryVolume(t, objects, "transient", "iscsi")
	recorder := record.NewFakeRecorder(16)
	d := newStartupGeometryDriver(mock, kubernetesfake.NewSimpleClientset(objects...), recorder)

	// Same post-restore shape, except this volume DOES record its geometry: the
	// rebuild gets past the geometry choke point and fails on the API instead.
	datasetName := "pool/parent/transient"
	_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, mock.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropDriverInstanceID: d.driverInstanceID(),
	}))
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "transient", nil))
	extent, err := mock.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NotNil(t, extent)
	require.NoError(t, mock.ISCSIExtentDelete(ctx, extent.ID, false, true))
	stored, err := mock.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	require.Equal(t, "512", stored.UserProperties[PropBlockISCSIBlocksize].Value,
		"the fixture is only meaningful if this volume's geometry IS establishable")

	d.truenasClient = &extentCreateFailingClient{MockClient: mock}
	err = d.reconcilePublishedAttachments(ctx)
	require.Error(t, err, "a transient share failure must still hold the pass open")
	assert.Contains(t, err.Error(), "ensure share for startup attachment transient")
	for _, event := range drainEvents(recorder) {
		assert.NotContains(t, event, "StartupShareGeometryUnestablishable",
			"a transient failure is not the permanent geometry condition")
	}
}
