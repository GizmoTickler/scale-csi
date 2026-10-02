package driver

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// A node re-registers (new IP or NQN) between the startup snapshot and the
// per-volume step, after a live publish granted the new identity: the pass
// re-reads the identity under the volume lock and must not revoke it.
func TestStartupReconcileGrantsTheNodesCurrentIdentity(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	datasetName := "pool/parent/idvol"
	ds, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: ds.Mountpoint, Networks: []string{"192.0.2.0/24"}, Enabled: true})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropNFSShareID, strconv.Itoa(share.ID)))

	oldID, _ := encodeNodeIdentity(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.11")}})
	newID, _ := encodeNodeIdentity(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.22")}})
	pvName := "pv-idvol"
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName}, Spec: corev1.PersistentVolumeSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
			Driver: "csi.scale.io", VolumeHandle: "idvol", VolumeAttributes: map[string]string{"node_attach_driver": "nfs"}}}}}
	va := &storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "va-idvol"},
		Spec:   storagev1.VolumeAttachmentSpec{Attacher: "csi.scale.io", NodeName: "worker-a", Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName}},
		Status: storagev1.VolumeAttachmentStatus{Attached: true}}
	mkCSINode := func(id string) *storagev1.CSINode {
		return &storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}, Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: id}}}}
	}
	mkNode := func(ip string) *corev1.Node {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ip}}}}
	}
	kube := kubernetesfake.NewSimpleClientset(pv, va, mkCSINode(newID), mkNode("192.0.2.22"))
	d := &Driver{name: "csi.scale.io", config: &Config{
		Fencing: FencingConfig{Mode: FencingModeStrict}, ZFS: ZFSConfig{DatasetParentName: "pool/parent"},
		NFS: NFSConfig{ShareHost: "192.0.2.10", ShareAllowedNetworks: []string{"192.0.2.0/24"}}},
		truenasClient: client, eventRecorder: &EventRecorder{clientset: kube}}

	// Live publish with the node's CURRENT identity.
	_, err = d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{VolumeId: "idvol", NodeId: newID,
		VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}},
		VolumeContext:    map[string]string{"node_attach_driver": "nfs"}})
	require.NoError(t, err)
	share, _ = client.NFSShareGet(ctx, share.ID)

	// The startup snapshot lists see the identity from BEFORE the change;
	// by the time the worker runs, the API holds the new one.
	snap := 0
	kube.PrependReactor("list", "csinodes", func(clienttesting.Action) (bool, runtime.Object, error) {
		snap++
		if snap == 1 {
			return true, &storagev1.CSINodeList{Items: []storagev1.CSINode{*mkCSINode(oldID)}}, nil
		}
		return false, nil, nil
	})
	nsnap := 0
	kube.PrependReactor("list", "nodes", func(clienttesting.Action) (bool, runtime.Object, error) {
		nsnap++
		if nsnap == 1 {
			return true, &corev1.NodeList{Items: []corev1.Node{*mkNode("192.0.2.11")}}, nil
		}
		return false, nil, nil
	})
	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	share, _ = client.NFSShareGet(ctx, share.ID)
	fresh, _ := client.DatasetGet(ctx, datasetName)
	records, _ := storedPublicationRecords(d, fresh)
	t.Logf("after startup pass: hosts=%v record=%+v", share.Hosts, records[publicationPropertyKey("worker-a")])
	require.Equal(t, []string{"192.0.2.22"}, share.Hosts, "startup pass must not revoke the node's current identity")
}
