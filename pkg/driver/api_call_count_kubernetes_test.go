package driver

import (
	"context"
	"net"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// newKubernetesRecordsAPICallCountDriver is newFencedAPICallCountDriver with
// publication records kept as VolumePublications, as the chart runs it.
func newKubernetesRecordsAPICallCountDriver(t *testing.T, client *apiCallCountingClient, protocol string, mode FencingMode) *Driver {
	t.Helper()
	d := newFencedAPICallCountDriver(t, client, protocol, mode)
	d.publicationStore = importingPublicationStore{
		kube:   newKubernetesPublicationStore(newFakeVolumePublicationClient(), "scale-csi", "golden"),
		legacy: zfsPublicationStore{client: client},
	}
	return d
}

// With records in Kubernetes, publish and unpublish write no dataset property:
// what is left is the share checks and, with fencing on, the allowlist change.
// Compare TestControllerPublishUnpublishGoldenAPICallCounts (records on ZFS).
func TestControllerPublishUnpublishGoldenAPICallCountsWithRecordsInKubernetes(t *testing.T) {
	ctx := context.Background()
	nfsNode := func(t *testing.T) string {
		t.Helper()
		id, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.31")}})
		require.NoError(t, err)
		return id
	}
	nvmeNode := func(t *testing.T) string {
		t.Helper()
		id, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-a"})
		require.NoError(t, err)
		return id
	}

	t.Run("off NFS publish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newKubernetesRecordsAPICallCountDriver(t, client, "nfs", FencingModeOff)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("k-off-nfs", "nfs"))
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.ControllerPublishVolume(ctx, nfsPublishRequest("k-off-nfs", nfsNode(t)))
		require.NoError(t, err)
		// 3 on ZFS, minus the record write.
		assertAPICallMethodMap(t, "off NFS publish", client, map[string]int{"DatasetGet": 1, "NFSShareGet": 1})
	})
	t.Run("off NFS unpublish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newKubernetesRecordsAPICallCountDriver(t, client, "nfs", FencingModeOff)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("k-off-nfs-unpub", "nfs"))
		require.NoError(t, err)
		node := nfsNode(t)
		_, err = d.ControllerPublishVolume(ctx, nfsPublishRequest("k-off-nfs-unpub", node))
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "k-off-nfs-unpub", NodeId: node})
		require.NoError(t, err)
		// 2 on ZFS, minus the record removal: the volume read is all.
		assertAPICallMethodMap(t, "off NFS unpublish", client, map[string]int{"DatasetGet": 1})
	})
	t.Run("additive NFS publish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newKubernetesRecordsAPICallCountDriver(t, client, "nfs", FencingModeAdditive)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("k-additive-nfs", "nfs"))
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.ControllerPublishVolume(ctx, nfsPublishRequest("k-additive-nfs", nfsNode(t)))
		require.NoError(t, err)
		assertAPICallMethodMap(t, "additive NFS publish", client, map[string]int{
			"DatasetGet": 1, "NFSShareGet": 2, "NFSShareUpdate": 1,
		})
	})
	t.Run("additive NFS unpublish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newKubernetesRecordsAPICallCountDriver(t, client, "nfs", FencingModeAdditive)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("k-additive-nfs-unpub", "nfs"))
		require.NoError(t, err)
		node := nfsNode(t)
		_, err = d.ControllerPublishVolume(ctx, nfsPublishRequest("k-additive-nfs-unpub", node))
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "k-additive-nfs-unpub", NodeId: node})
		require.NoError(t, err)
		// 5 on ZFS, minus the tombstone write and the record removal.
		assertAPICallMethodMap(t, "additive NFS unpublish", client, map[string]int{
			"DatasetGet": 1, "NFSShareGet": 1, "NFSShareUpdate": 1,
		})
	})
	t.Run("strict NVMe-oF publish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newKubernetesRecordsAPICallCountDriver(t, client, "nvmeof", FencingModeStrict)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("k-strict-nvme", "nvmeof"))
		require.NoError(t, err)
		node := nvmeNode(t)
		client.resetCalls()
		_, err = d.ControllerPublishVolume(ctx, nvmeoFPublishRequest("k-strict-nvme", node))
		require.NoError(t, err)
		// A first publish (not the cached republish the ZFS golden pins): the
		// host is created, resolved and associated; no record write. 8 calls
		// (10 before the dead boundary list and the eager host lookup for the
		// classification were dropped): classification list, one host lookup
		// and create for the missing association, the create, the post-write list.
		assertAPICallMethodMap(t, "strict NVMe-oF first publish", client, map[string]int{
			"DatasetGet":                      1,
			"NVMeoFNamespaceGet":              1,
			"NVMeoFSubsystemGet":              1,
			"NVMeoFHostFindByNQN":             1,
			"NVMeoFHostCreate":                1,
			"NVMeoFHostSubsysListBySubsystem": 2,
			"NVMeoFHostSubsysCreate":          1,
		})
	})
	t.Run("strict NVMe-oF unpublish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newKubernetesRecordsAPICallCountDriver(t, client, "nvmeof", FencingModeStrict)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("k-strict-nvme-unpub", "nvmeof"))
		require.NoError(t, err)
		node := nvmeNode(t)
		_, err = d.ControllerPublishVolume(ctx, nvmeoFPublishRequest("k-strict-nvme-unpub", node))
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "k-strict-nvme-unpub", NodeId: node})
		require.NoError(t, err)
		// 7 on ZFS, minus the tombstone write and the record removal: 5 (7
		// before). A move (this plus a first publish elsewhere) is 13 calls.
		assertAPICallMethodMap(t, "strict NVMe-oF unpublish", client, map[string]int{
			"DatasetGet":                      1,
			"NVMeoFNamespaceGet":              1,
			"NVMeoFSubsystemGet":              1,
			"NVMeoFHostSubsysListBySubsystem": 1,
			"NVMeoFHostSubsysDelete":          1,
		})
	})
}

// kubernetesRecordVerbs runs op and returns the VolumePublication requests it
// made, by verb.
func kubernetesRecordVerbs(t *testing.T, d *Driver, op func()) map[string]int {
	t.Helper()
	fake, ok := d.publicationStore.(importingPublicationStore).kube.client.(*dynamicfake.FakeDynamicClient)
	require.True(t, ok)
	fake.ClearActions()
	op()
	verbs := make(map[string]int)
	for _, action := range fake.Actions() {
		verbs[action.GetVerb()]++
	}
	return verbs
}

// The Kubernetes requests of a strict NVMe-oF move's two halves. Each record
// write is one request: it is a compare-and-set at the resourceVersion the
// locked read saw (v1.23; a GET before every write until then).
func TestControllerPublishUnpublishGoldenKubernetesRequestCounts(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newKubernetesRecordsAPICallCountDriver(t, client, "nvmeof", FencingModeStrict)
	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("k-requests", "nvmeof"))
	require.NoError(t, err)
	node, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-a"})
	require.NoError(t, err)

	publish := kubernetesRecordVerbs(t, d, func() {
		_, err := d.ControllerPublishVolume(ctx, nvmeoFPublishRequest("k-requests", node))
		require.NoError(t, err)
	})
	// The locked read and the record create: 2 (3 before).
	assert.Equal(t, map[string]int{"list": 1, "create": 1}, publish, "first publish")

	republish := kubernetesRecordVerbs(t, d, func() {
		_, err := d.ControllerPublishVolume(ctx, nvmeoFPublishRequest("k-requests", node))
		require.NoError(t, err)
	})
	// An unchanged record is not rewritten: the read is all.
	assert.Equal(t, map[string]int{"list": 1}, republish, "republish")

	unpublish := kubernetesRecordVerbs(t, d, func() {
		_, err := d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "k-requests", NodeId: node})
		require.NoError(t, err)
	})
	// The read, the removing tombstone, the removal: 3 (4 before).
	assert.Equal(t, map[string]int{"list": 1, "update": 1, "delete": 1}, unpublish, "unpublish")
}
