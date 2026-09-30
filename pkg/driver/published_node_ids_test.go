package driver

import (
	"context"
	"strconv"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
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

// ListVolumes must report a publication under the node id the CO uses for the
// node, the one its CSINode advertises. The external-attacher's reconciler
// compares the two every minute and force-republishes every attached volume
// it cannot find. The controller used to store its own re-encoding of the
// resolved identity, which adds the Node's addresses, so no attached volume
// ever matched: a live cluster with 29 attachments made about 440 TrueNAS
// calls a minute, one publication-record write per volume among them, with
// nothing changing.
func TestListVolumesReportsTheCONodeIDForAPublication(t *testing.T) {
	const volumeID = "published-volume"
	nqn := "nqn.2014-08.org.nvmexpress:uuid:worker-a"
	// What the node plugin registers: no addresses, the controller adds them.
	coNodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: nqn})
	require.NoError(t, err)

	setup := func(t *testing.T, objects ...runtime.Object) (*Driver, *truenas.MockClient) {
		t.Helper()
		ctx := context.Background()
		h := newFencingTestHarness(t, FencingModeStrict, ShareTypeNVMeoF)
		objects = append(objects,
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "worker-a"},
				Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{
					{Type: corev1.NodeInternalIP, Address: "192.0.2.11"},
				}},
			},
			&storagev1.CSINode{
				ObjectMeta: metav1.ObjectMeta{Name: "worker-a"},
				Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{
					{Name: h.d.name, NodeID: coNodeID},
				}},
			},
		)
		h.d.eventRecorder = &EventRecorder{recorder: record.NewFakeRecorder(16), clientset: kubernetesfake.NewSimpleClientset(objects...), enabled: true}
		datasetName := "pool/parent/" + volumeID
		ds, err := h.client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME", Volsize: testGiB})
		require.NoError(t, err)
		require.NoError(t, h.d.createNVMeoFShareForDataset(ctx, ds, datasetName, volumeID, true, true, nil))
		require.NoError(t, h.client.DatasetSetUserProperties(ctx, datasetName, map[string]string{
			PropManagedResource: "true", PropProvisionSuccess: "true",
		}))
		// The fixture must make the two ids differ, or it cannot tell them apart.
		resolved, err := h.d.resolveControllerNodeIdentity(ctx, coNodeID)
		require.NoError(t, err)
		reencoded, err := encodeNodeIdentity(resolved)
		require.NoError(t, err)
		require.NotEqual(t, coNodeID, reencoded, "the resolved identity carries the Node's address")
		return h.d, h.client
	}
	publishedNodeIDs := func(t *testing.T, d *Driver) []string {
		t.Helper()
		resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
		require.NoError(t, err)
		require.Len(t, resp.Entries, 1)
		return resp.Entries[0].GetStatus().GetPublishedNodeIds()
	}
	fencedHosts := func(t *testing.T, client *truenas.MockClient) []string {
		t.Helper()
		ctx := context.Background()
		text, err := client.DatasetGetUserProperty(ctx, "pool/parent/"+volumeID, PropNVMeoFSubsystemID)
		require.NoError(t, err)
		subsystemID, err := strconv.Atoi(text)
		require.NoError(t, err)
		associations, err := client.NVMeoFHostSubsysListBySubsystem(ctx, subsystemID)
		require.NoError(t, err)
		hosts := make([]string, 0, len(associations))
		for _, association := range associations {
			hosts = append(hosts, association.HostNQN)
		}
		return hosts
	}

	t.Run("ControllerPublishVolume", func(t *testing.T) {
		d, client := setup(t)
		_, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
			VolumeId: volumeID, NodeId: coNodeID,
			VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			}},
			VolumeContext: map[string]string{"node_attach_driver": "nvmeof"},
		})
		require.NoError(t, err)
		assert.Equal(t, []string{coNodeID}, publishedNodeIDs(t, d))
		assert.Equal(t, []string{nqn}, fencedHosts(t, client))
	})

	t.Run("startup reconciliation", func(t *testing.T) {
		pvName := "pv-" + volumeID
		d, client := setup(t,
			&corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: pvName},
				Spec: corev1.PersistentVolumeSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
						Driver: fencingDriverName(ShareTypeNVMeoF), VolumeHandle: volumeID,
						VolumeAttributes: map[string]string{"node_attach_driver": "nvmeof"},
					}},
				},
			},
			&storagev1.VolumeAttachment{
				ObjectMeta: metav1.ObjectMeta{Name: "va-" + volumeID},
				Spec: storagev1.VolumeAttachmentSpec{
					Attacher: fencingDriverName(ShareTypeNVMeoF), NodeName: "worker-a",
					Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
				},
				Status: storagev1.VolumeAttachmentStatus{Attached: true},
			},
		)
		require.NoError(t, d.reconcilePublishedAttachments(context.Background()))
		assert.Equal(t, []string{coNodeID}, publishedNodeIDs(t, d))
		assert.Equal(t, []string{nqn}, fencedHosts(t, client))
	})
}
