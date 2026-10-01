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
	datasetName := "pool/parent/" + volumeID
	nqn := "nqn.2014-08.org.nvmexpress:uuid:worker-a"
	// What the node plugin registers: no addresses, the controller adds them.
	coNodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: nqn})
	require.NoError(t, err)

	// setup registers worker-a's CSINode with csiNodeID and a Node with an address.
	setup := func(t *testing.T, csiNodeID string, objects ...runtime.Object) (*Driver, *truenas.MockClient) {
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
					{Name: h.d.name, NodeID: csiNodeID},
				}},
			},
		)
		h.d.eventRecorder = &EventRecorder{recorder: record.NewFakeRecorder(16), clientset: kubernetesfake.NewSimpleClientset(objects...), enabled: true}
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
	attachedAtStartup := func() []runtime.Object {
		pvName := "pv-" + volumeID
		return []runtime.Object{
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
		}
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
		text, err := client.DatasetGetUserProperty(ctx, datasetName, PropNVMeoFSubsystemID)
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
	// The stored record still carries the resolved identity for fencing, and
	// handing its id back to unpublishFencedVolume (what the stale-single-node
	// takeover and the stale-record revoke do) removes the record and the fence.
	checkRecordAndRevoke := func(t *testing.T, d *Driver, client *truenas.MockClient) {
		t.Helper()
		ctx := context.Background()
		ds, err := client.DatasetGet(ctx, datasetName)
		require.NoError(t, err)
		records, err := storedPublicationRecords(d, ds)
		require.NoError(t, err)
		stored, ok := records[publicationPropertyKey("worker-a")]
		require.True(t, ok)
		assert.Equal(t, []string{"192.0.2.11"}, stored.IPs)
		assert.Equal(t, nqn, stored.NVMeNQN)
		require.NoError(t, d.unpublishFencedVolume(ctx, ds, datasetName, ShareTypeNVMeoF, stored.EncodedID, nil))
		ds, err = client.DatasetGet(ctx, datasetName)
		require.NoError(t, err)
		_, retained := mustStoredRecords(t, d, ds)[publicationPropertyKey("worker-a")]
		assert.False(t, retained, "revoking by the stored id removes the record")
		assert.Empty(t, fencedHosts(t, client))
	}

	t.Run("ControllerPublishVolume", func(t *testing.T) {
		d, client := setup(t, coNodeID)
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
		checkRecordAndRevoke(t, d, client)
	})

	t.Run("startup reconciliation", func(t *testing.T) {
		d, client := setup(t, coNodeID, attachedAtStartup()...)
		require.NoError(t, d.reconcilePublishedAttachments(context.Background()))
		assert.Equal(t, []string{coNodeID}, publishedNodeIDs(t, d))
		assert.Equal(t, []string{nqn}, fencedHosts(t, client))
		checkRecordAndRevoke(t, d, client)
	})

	// A CSINode whose id names another node: startup uses the VolumeAttachment's
	// node name for the record, so the id must not be stored, or revoking the
	// record by it would find nothing and leave it behind for good.
	t.Run("startup reconciliation with a CSINode id naming another node", func(t *testing.T) {
		otherID, err := encodeNodeIdentity(NodeIdentity{Name: "renamed", NVMeNQN: nqn})
		require.NoError(t, err)
		d, client := setup(t, otherID, attachedAtStartup()...)
		require.NoError(t, d.reconcilePublishedAttachments(context.Background()))
		ids := publishedNodeIDs(t, d)
		require.Len(t, ids, 1)
		parsed, err := parseNodeIdentity(ids[0])
		require.NoError(t, err)
		assert.Equal(t, "worker-a", parsed.Name)
		checkRecordAndRevoke(t, d, client)
	})
}

// keepCONodeID adopts an id only when it parses and names the record's node:
// anything else keeps the re-encoding, which always does.
func TestKeepCONodeIDOnlyAdoptsAnIDNamingTheRecordsNode(t *testing.T) {
	const reencoded = "sc1.reencoded"
	nameA, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: "nqn.x"})
	require.NoError(t, err)
	nameB, err := encodeNodeIdentity(NodeIdentity{Name: "worker-b", NVMeNQN: "nqn.x"})
	require.NoError(t, err)
	for _, c := range []struct{ name, nodeID, want string }{
		{"the CO's id for this node", nameA, nameA},
		{"a legacy plain name for this node", "worker-a", "worker-a"},
		{"empty", "", reencoded},
		{"not base64", nodeIdentityPrefix + "!!!", reencoded},
		{"a newer envelope version", nodeIdentityPrefix + "AgEBbg", reencoded},
		{"another node's id", nameB, reencoded},
		{"another node's plain name", "worker-b", reencoded},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := publicationRecord{Node: "worker-a", EncodedID: reencoded}
			r.keepCONodeID(c.nodeID)
			assert.Equal(t, c.want, r.EncodedID)
		})
	}
}
