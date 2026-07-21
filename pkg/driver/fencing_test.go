package driver

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func TestControllerPublishSingleWriterRejectsSecondNodeAndNodeGoneUnpublishIsIdempotent(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nvmeof",
		config: &Config{
			DriverName: "org.scale.csi.nvmeof",
			Fencing:    FencingConfig{Mode: FencingModeStrict},
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			NVMeoF: NVMeoFConfig{
				Transport:          "TCP",
				TransportAddress:   "192.0.2.20",
				TransportServiceID: 4420,
			},
		},
		truenasClient:     client,
		nvmeResolvedHosts: make(map[string]int),
	}

	datasetName := "pool/parent/fenced-volume"
	ds, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, d.createNVMeoFShareForDataset(ctx, ds, datasetName, "fenced-volume", true, true))

	nodeA, err := encodeNodeIdentity(NodeIdentity{
		Name: "worker-a", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-a",
	})
	require.NoError(t, err)
	nodeB, err := encodeNodeIdentity(NodeIdentity{
		Name: "worker-b", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-b",
	})
	require.NoError(t, err)
	request := func(nodeID string) *csi.ControllerPublishVolumeRequest {
		return &csi.ControllerPublishVolumeRequest{
			VolumeId: "fenced-volume",
			NodeId:   nodeID,
			VolumeCapability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
				},
			},
			VolumeContext: map[string]string{"node_attach_driver": "nvmeof"},
		}
	}

	_, err = d.ControllerPublishVolume(ctx, request(nodeA))
	require.NoError(t, err)
	_, err = d.ControllerPublishVolume(ctx, request(nodeB))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "worker-a")

	subsystemIDText, err := client.DatasetGetUserProperty(ctx, datasetName, PropNVMeoFSubsystemID)
	require.NoError(t, err)
	subsystemID, err := strconv.Atoi(subsystemIDText)
	require.NoError(t, err)
	associations, err := client.NVMeoFHostSubsysListBySubsystem(ctx, subsystemID)
	require.NoError(t, err)
	require.Len(t, associations, 1)
	assert.Equal(t, "nqn.2014-08.org.nvmexpress:uuid:worker-a", associations[0].HostNQN)

	// Simulate a node-gone retry from an older external-attacher. Only the plain
	// node name remains, so the NQN must come from the durable publish property.
	_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "fenced-volume", NodeId: "worker-a",
	})
	require.NoError(t, err)
	associations, err = client.NVMeoFHostSubsysListBySubsystem(ctx, subsystemID)
	require.NoError(t, err)
	assert.Empty(t, associations)
	ds, err = client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	_, retained := ds.UserProperties[publicationPropertyKey("worker-a")]
	assert.False(t, retained)

	_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "fenced-volume", NodeId: "worker-a",
	})
	require.NoError(t, err, "a repeated unpublish must remain idempotent")

	// The allowlist itself remains part of the publication proof. Even if the
	// recovery property is missing, a different backend host blocks a new
	// single-node publish instead of being silently replaced.
	hostA, err := client.NVMeoFHostFindByNQN(ctx, "nqn.2014-08.org.nvmexpress:uuid:worker-a")
	require.NoError(t, err)
	require.NotNil(t, hostA)
	_, err = client.NVMeoFHostSubsysCreate(ctx, hostA.ID, subsystemID)
	require.NoError(t, err)
	_, err = d.ControllerPublishVolume(ctx, request(nodeB))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), hostA.HostNQN)
	ds, err = client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	_, retained = ds.UserProperties[publicationPropertyKey("worker-b")]
	assert.False(t, retained, "backend conflict must be detected before persisting a new publication")
}

func TestStartupReconcileBackfillsAttachedNFSNodeAndEnforcesFence(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	datasetName := "pool/parent/upgrade-volume"
	ds, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: ds.Mountpoint, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropNFSShareID, strconv.Itoa(share.ID)))

	encodedNodeID, err := encodeNodeIdentity(NodeIdentity{
		Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.11")},
	})
	require.NoError(t, err)
	pvName := "pv-upgrade-volume"
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "csi.scale.io", VolumeHandle: "upgrade-volume",
					VolumeAttributes: map[string]string{"node_attach_driver": "nfs"},
				},
			},
		},
	}
	attachment := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "attachment-upgrade-volume"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.scale.io",
			NodeName: "worker-a",
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	csiNode := &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-a"},
		Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{
			Name: "csi.scale.io", NodeID: encodedNodeID,
		}}},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-a"},
		Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{
			Type: corev1.NodeInternalIP, Address: "192.0.2.11",
		}}},
	}
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS: NFSConfig{
				ShareHost: "192.0.2.10", ShareAllowedNetworks: []string{"192.0.2.0/24"},
			},
		},
		truenasClient: client,
		eventRecorder: &EventRecorder{
			clientset: kubernetesfake.NewSimpleClientset(pv, attachment, csiNode, node),
		},
	}

	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	ds, err = client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(ds)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "worker-a", records[publicationPropertyKey("worker-a")].Node)

	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.11"}, share.Hosts)
	assert.Empty(t, share.Networks, "the node host list is the publication allowlist; the configured CIDR is an intersection bound")
	assert.True(t, share.Enabled)
}

func TestStartupReconcileStrictRejectsConflictingSingleNodeAttachmentsBeforeMutation(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	datasetName := "pool/parent/conflicting-upgrade"
	ds, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: ds.Mountpoint, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropNFSShareID, strconv.Itoa(share.ID)))

	pvName := "pv-conflicting-upgrade"
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
				Driver: "csi.scale.io", VolumeHandle: "conflicting-upgrade",
			}},
		},
	}
	attachment := func(name, node string) *storagev1.VolumeAttachment {
		return &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: storagev1.VolumeAttachmentSpec{
				Attacher: "csi.scale.io", NodeName: node,
				Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
			},
			Status: storagev1.VolumeAttachmentStatus{Attached: true},
		}
	}
	csiNode := func(name, address string) *storagev1.CSINode {
		encoded, encodeErr := encodeNodeIdentity(NodeIdentity{Name: name, IPs: []net.IP{net.ParseIP(address)}})
		require.NoError(t, encodeErr)
		return &storagev1.CSINode{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: encoded}}},
		}
	}
	node := func(name, address string) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{
				Type: corev1.NodeInternalIP, Address: address,
			}}},
		}
	}
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:     NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
		eventRecorder: &EventRecorder{clientset: kubernetesfake.NewSimpleClientset(
			pv,
			attachment("attachment-a", "worker-a"), attachment("attachment-b", "worker-b"),
			csiNode("worker-a", "192.0.2.11"), csiNode("worker-b", "192.0.2.12"),
			node("worker-a", "192.0.2.11"), node("worker-b", "192.0.2.12"),
		)},
	}

	err = d.reconcilePublishedAttachments(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already published")
	ds, err = client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(ds)
	require.NoError(t, err)
	assert.Empty(t, records, "strict conflict preflight must run before the first backend write")
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.0/24"}, share.Networks)
}

func TestISCSIMultiNodePublishMaintainsExactPerTargetInitiatorGroup(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.iscsi",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			ISCSI: ISCSIConfig{
				TargetPortal: "192.0.2.10:3260", ExtentBlocksize: 512, ExtentRpm: "SSD",
			},
		},
		truenasClient: client,
		serviceReloadDebouncer: NewServiceReloadDebouncer(0, func(context.Context, string) error {
			return nil
		}),
	}
	t.Cleanup(d.serviceReloadDebouncer.Stop)
	datasetName := "pool/parent/shared-iscsi"
	ds, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME", Volsize: testGiB})
	require.NoError(t, err)
	require.NoError(t, d.createISCSIShareForDataset(ctx, ds, datasetName, "shared-iscsi", true, true))

	encode := func(name, iqn string) string {
		t.Helper()
		encoded, encodeErr := encodeNodeIdentity(NodeIdentity{Name: name, ISCSIIQN: iqn})
		require.NoError(t, encodeErr)
		return encoded
	}
	capability := &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	}}
	for _, nodeID := range []string{
		encode("worker-a", "iqn.1993-08.org.debian:worker-a"),
		encode("worker-b", "iqn.1993-08.org.debian:worker-b"),
	} {
		_, err = d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: "shared-iscsi", NodeId: nodeID, VolumeCapability: capability,
			VolumeContext: map[string]string{"node_attach_driver": "iscsi"},
		})
		require.NoError(t, err)
	}

	ds, err = client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	initiatorID := mustAtoi(t, datasetUserProperty(ds, PropISCSIInitiatorID))
	initiator, err := client.ISCSIInitiatorGet(ctx, initiatorID)
	require.NoError(t, err)
	require.NotNil(t, initiator)
	assert.Equal(t, []string{
		"iqn.1993-08.org.debian:worker-a",
		"iqn.1993-08.org.debian:worker-b",
	}, initiator.Initiators)
	target, err := d.resolveISCSITarget(ctx, ds, datasetName)
	require.NoError(t, err)
	require.NotNil(t, target)
	require.Len(t, target.Groups, 1)
	assert.Equal(t, initiatorID, target.Groups[0].Initiator)
	assert.Equal(t, "NONE", target.Groups[0].AuthMethod)
	assert.NotZero(t, target.Groups[0].Initiator, "NONE must never be paired with an allow-all initiator group")

	_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "shared-iscsi", NodeId: "worker-a",
	})
	require.NoError(t, err)
	initiator, err = client.ISCSIInitiatorGet(ctx, initiatorID)
	require.NoError(t, err)
	require.NotNil(t, initiator)
	assert.Equal(t, []string{"iqn.1993-08.org.debian:worker-b"}, initiator.Initiators)
}

func TestAdditiveISCSIFencingDropsAllowAllButPreservesRestrictedStaticGroup(t *testing.T) {
	client := truenas.NewMockClient()
	client.ISCSIInitiators[2] = &truenas.ISCSIInitiator{
		ID: 2, Initiators: []string{"iqn.1993-08.org.debian:static-worker"},
	}
	d := &Driver{
		config:        &Config{Fencing: FencingConfig{Mode: FencingModeAdditive}},
		truenasClient: client,
	}
	target := &truenas.ISCSITarget{Groups: []truenas.ISCSITargetGroup{
		{Portal: 1, Initiator: 0, AuthMethod: "NONE"},
		{Portal: 1, Initiator: 1, AuthMethod: "NONE"},
		{Portal: 1, Initiator: 2, AuthMethod: "CHAP", AuthNetworks: []string{"192.0.2.0/24"}},
	}}

	groups, err := d.safeAdditiveISCSIGroups(context.Background(), target, 0)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, 2, groups[0].Initiator)
	assert.Equal(t, "CHAP", groups[0].AuthMethod)
	assert.Equal(t, []string{"192.0.2.0/24"}, groups[0].AuthNetworks)
}

func TestStartupReconcileAdditiveDefersLegacyNodeWithoutStrippingStaticNVMeHost(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	staticNQN := "nqn.2014-08.org.nvmexpress:legacy-static-host"
	host, err := client.NVMeoFHostCreate(ctx, staticNQN)
	require.NoError(t, err)
	subsystem, err := client.NVMeoFSubsystemCreate(ctx, "upgrade-nvme", false, []int{host.ID})
	require.NoError(t, err)
	namespace, err := client.NVMeoFNamespaceCreate(ctx, subsystem.ID, "zvol/pool/parent/upgrade-nvme", "ZVOL")
	require.NoError(t, err)
	ds, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/upgrade-nvme", Type: "VOLUME"})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, ds.Name, map[string]string{
		PropNVMeoFSubsystemID: strconv.Itoa(subsystem.ID),
		PropNVMeoFNamespaceID: strconv.Itoa(namespace.ID),
	}))

	pvName := "pv-upgrade-nvme"
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
				Driver: "csi.scale.io", VolumeHandle: "upgrade-nvme",
				VolumeAttributes: map[string]string{"node_attach_driver": "nvmeof"},
			}},
		},
	}
	attachment := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "attachment-upgrade-nvme"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.scale.io", NodeName: "legacy-worker",
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	csiNode := &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-worker"},
		Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{
			Name: "csi.scale.io", NodeID: "legacy-worker",
		}}},
	}
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NVMeoF:  NVMeoFConfig{SubsystemHosts: []string{staticNQN}},
		},
		truenasClient: client,
		eventRecorder: &EventRecorder{
			clientset: kubernetesfake.NewSimpleClientset(pv, attachment, csiNode),
		},
	}

	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	associations, err := client.NVMeoFHostSubsysListBySubsystem(ctx, subsystem.ID)
	require.NoError(t, err)
	require.Len(t, associations, 1)
	assert.Equal(t, host.ID, associations[0].HostID)
	ds, err = client.DatasetGet(ctx, ds.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(ds)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Empty(t, records[publicationPropertyKey("legacy-worker")].NVMeNQN,
		"legacy identity is retained as a placeholder so later publishes cannot silently tighten around it")
}

func TestCreateVolumeRejectsUnownedDatasetCollision(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/collision", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	d := newComplianceTestDriver(client)

	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "collision",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Contains(t, err.Error(), PropDriverInstanceID)
	assert.Empty(t, client.NFSShares, "ownership rejection must happen before any backend object is adopted or created")

	dataset := client.Datasets["pool/parent/collision"]
	dataset.UserProperties[PropDriverInstanceID] = truenas.UserProperty{
		Value: d.driverInstanceID(), Source: "inherited from pool/parent",
	}
	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "collision",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err), "an inherited owner is not dataset-specific proof")
	assert.Empty(t, client.NFSShares)

	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropDriverInstanceID, d.driverInstanceID()))
	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "collision",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs"},
	})
	require.NoError(t, err, "an explicit local ownership property permits operator-controlled adoption")
}
