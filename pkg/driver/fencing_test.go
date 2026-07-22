package driver

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

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

func TestNVMeBackendCompatibilityFallsBackToHostIDWhenHostNQNIsEmpty(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			NVMeoF:  NVMeoFConfig{Transport: "TCP", TransportAddress: "192.0.2.20", TransportServiceID: 4420},
		},
		truenasClient: client, nvmeResolvedHosts: make(map[string]int),
	}
	datasetName := "pool/parent/host-id-fallback"
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME", Volsize: testGiB})
	require.NoError(t, err)
	require.NoError(t, d.createNVMeoFShareForDataset(ctx, dataset, datasetName, "host-id-fallback", true, true))
	subsystemID := mustAtoi(t, datasetUserProperty(dataset, PropNVMeoFSubsystemID))
	nqn := "nqn.2014-08.org.nvmexpress:uuid:worker-a"
	host, err := client.NVMeoFHostCreate(ctx, nqn)
	require.NoError(t, err)
	_, err = client.NVMeoFHostSubsysCreate(ctx, host.ID, subsystemID)
	require.NoError(t, err)
	client.EmptyNVMeHostNQN = true

	err = d.validateBackendSingleNodeCompatibility(ctx, dataset, datasetName, ShareTypeNVMeoF,
		NodeIdentity{Name: "worker-a", NVMeNQN: nqn}, map[string]publicationRecord{},
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	require.NoError(t, err, "the nested association HostID remains authoritative when host.hostnqn is omitted")

	err = d.validateBackendSingleNodeCompatibility(ctx, dataset, datasetName, ShareTypeNVMeoF,
		NodeIdentity{Name: "worker-b", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-b"},
		map[string]publicationRecord{}, csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "host ID")
}

func TestControllerUnpublishVolumeEmptyNodeIDRevokesAllPublications(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS: NFSConfig{
				ShareHost: "192.0.2.10", ShareAllowedNetworks: []string{"192.0.2.0/24"},
			},
		},
		truenasClient: client,
	}
	datasetName := "pool/parent/unpublish-all"
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: dataset.Mountpoint, Enabled: true})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropNFSShareID, strconv.Itoa(share.ID)))
	capability := &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	}}
	for index, address := range []string{"192.0.2.11", "192.0.2.12"} {
		nodeID, encodeErr := encodeNodeIdentity(NodeIdentity{
			Name: "worker-" + strconv.Itoa(index), IPs: []net.IP{net.ParseIP(address)},
		})
		require.NoError(t, encodeErr)
		_, err = d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: "unpublish-all", NodeId: nodeID, VolumeCapability: capability,
			VolumeContext: map[string]string{"node_attach_driver": "nfs"},
		})
		require.NoError(t, err)
	}

	_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "unpublish-all"})
	require.NoError(t, err, "CSI v1.12 requires an empty node_id to unpublish from every node")
	dataset, err = client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(dataset)
	require.NoError(t, err)
	assert.Empty(t, records)
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Empty(t, share.Hosts)
	assert.False(t, share.Enabled)
}

func TestAdditivePublishDefersMissingAndOutOfCIDRIdentityWhilePreservingNFSNetworks(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS: NFSConfig{
				ShareHost: "192.0.2.10", ShareAllowedNetworks: []string{"192.0.2.0/24"},
			},
		},
		truenasClient: client,
	}
	datasetName := "pool/parent/additive-nfs"
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Networks: []string{"198.51.100.0/24"}, Hosts: []string{"192.0.2.99"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropNFSShareID, strconv.Itoa(share.ID)))
	request := func(nodeID string) *csi.ControllerPublishVolumeRequest {
		return &csi.ControllerPublishVolumeRequest{
			VolumeId: "additive-nfs", NodeId: nodeID,
			VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			}},
			VolumeContext: map[string]string{"node_attach_driver": "nfs"},
		}
	}

	missingMetric := fencingDeferredTotal.WithLabelValues("missing_identity", "nfs")
	missingBefore := testutil.ToFloat64(missingMetric)
	_, err = d.ControllerPublishVolume(ctx, request("legacy-worker"))
	require.NoError(t, err)
	assert.Equal(t, missingBefore+1, testutil.ToFloat64(missingMetric))

	outsideID, err := encodeNodeIdentity(NodeIdentity{Name: "outside-worker", IPs: []net.IP{net.ParseIP("203.0.113.20")}})
	require.NoError(t, err)
	outsideMetric := fencingDeferredTotal.WithLabelValues("outside_allowed_network", "nfs")
	outsideBefore := testutil.ToFloat64(outsideMetric)
	_, err = d.ControllerPublishVolume(ctx, request(outsideID))
	require.NoError(t, err)
	assert.Equal(t, outsideBefore+1, testutil.ToFloat64(outsideMetric))

	insideID, err := encodeNodeIdentity(NodeIdentity{Name: "inside-worker", IPs: []net.IP{net.ParseIP("192.0.2.11")}})
	require.NoError(t, err)
	_, err = d.ControllerPublishVolume(ctx, request(insideID))
	require.NoError(t, err)
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"198.51.100.0/24"}, share.Networks)
	assert.ElementsMatch(t, []string{"192.0.2.11", "192.0.2.99"}, share.Hosts)

	dataset, err = client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(dataset)
	require.NoError(t, err)
	require.Len(t, records, 3, "deferred publishes retain durable ownership while enforceable peers converge")

	d.config.Fencing.Mode = FencingModeStrict
	_, err = d.ControllerPublishVolume(ctx, request("strict-legacy-worker"))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestAdditiveSingleNodeDeferredOwnershipRejectsSecondLegacyNode(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:     NFSConfig{ShareHost: "192.0.2.10", ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/deferred-single", Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	request := func(node string) *csi.ControllerPublishVolumeRequest {
		return &csi.ControllerPublishVolumeRequest{
			VolumeId: "deferred-single", NodeId: node,
			VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			}},
			VolumeContext: map[string]string{"node_attach_driver": "nfs"},
		}
	}

	_, err = d.ControllerPublishVolume(ctx, request("legacy-a"))
	require.NoError(t, err)
	_, err = d.ControllerPublishVolume(ctx, request("legacy-b"))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	dataset, err = client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(dataset)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Contains(t, records, publicationPropertyKey("legacy-a"))
	assert.NotContains(t, records, publicationPropertyKey("legacy-b"))
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.0/24"}, share.Networks)
	assert.Empty(t, share.Hosts)
}

func TestAdditiveDeferredAndValidNFSPublishesPreserveBroadAllowAll(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:     NFSConfig{ShareHost: "192.0.2.10", ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/broad-nfs", Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: dataset.Mountpoint, Enabled: true})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	capability := &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	}}
	publish := func(nodeID string) error {
		_, publishErr := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: "broad-nfs", NodeId: nodeID, VolumeCapability: capability,
			VolumeContext: map[string]string{"node_attach_driver": "nfs"},
		})
		return publishErr
	}
	require.NoError(t, publish("legacy-worker"))
	validID, err := encodeNodeIdentity(NodeIdentity{Name: "current-worker", IPs: []net.IP{net.ParseIP("192.0.2.11")}})
	require.NoError(t, err)
	require.NoError(t, publish(validID))

	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.True(t, share.Enabled)
	assert.Empty(t, share.Hosts)
	assert.Empty(t, share.Networks, "additive must not narrow a legacy allow-all share")
	dataset, err = client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(dataset)
	require.NoError(t, err)
	assert.Len(t, records, 2)
}

func TestAdditiveNFSUnpublishUsesDurableCSIAddedProvenance(t *testing.T) {
	for _, test := range []struct {
		name          string
		initialHosts  []string
		wantOwned     []string
		wantRemaining []string
	}{
		{
			name: "new dynamic host is removed", wantOwned: []string{"192.0.2.11"},
		},
		{
			name: "matching backend-only static host is preserved", initialHosts: []string{"192.0.2.11"},
			wantRemaining: []string{"192.0.2.11"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			client := truenas.NewMockClient()
			d := &Driver{
				name: "org.scale.csi.nfs",
				config: &Config{
					Fencing: FencingConfig{Mode: FencingModeAdditive},
					ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
					NFS:     NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
				},
				truenasClient: client,
			}
			volumeID := "nfs-provenance-" + strconv.Itoa(len(test.initialHosts))
			dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
				Name: "pool/parent/" + volumeID, Type: "FILESYSTEM",
			})
			require.NoError(t, err)
			share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
				Path: dataset.Mountpoint, Hosts: test.initialHosts,
				Networks: []string{"198.51.100.0/24"}, Enabled: true,
			})
			require.NoError(t, err)
			require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
			nodeID, err := encodeNodeIdentity(NodeIdentity{
				Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.11")},
			})
			require.NoError(t, err)
			request := &csi.ControllerPublishVolumeRequest{
				VolumeId: volumeID, NodeId: nodeID,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				}},
				VolumeContext: map[string]string{"node_attach_driver": "nfs"},
			}
			_, err = d.ControllerPublishVolume(ctx, request)
			require.NoError(t, err)
			_, err = d.ControllerPublishVolume(ctx, request)
			require.NoError(t, err)
			fresh, err := client.DatasetGet(ctx, dataset.Name)
			require.NoError(t, err)
			records, err := publicationRecordsFromDataset(fresh)
			require.NoError(t, err)
			require.Len(t, records, 1)
			assert.Equal(t, test.wantOwned, records[publicationPropertyKey("worker-a")].CSIAddedNFSHosts)

			_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
				VolumeId: volumeID, NodeId: nodeID,
			})
			require.NoError(t, err)
			share, err = client.NFSShareGet(ctx, share.ID)
			require.NoError(t, err)
			assert.Equal(t, test.wantRemaining, share.Hosts)
			assert.Equal(t, []string{"198.51.100.0/24"}, share.Networks)
		})
	}
}

func TestCompactAdditiveGrantsNeverDropsBackendLiveEntries(t *testing.T) {
	// Stale entries (neither current nor still on the backend) are dropped; the
	// remaining entries keep first-seen order; duplicates dedup uniformly.
	got, overCap := compactAdditiveGrants(
		[]string{"stale", "still-on-backend", "current", "still-on-backend"},
		[]string{"current", "current"},
		[]string{"still-on-backend"},
	)
	assert.False(t, overCap)
	assert.Equal(t, []string{"still-on-backend", "current"}, got)

	// A pathological pile of entries that are ALL still present on the backend is
	// never evicted — every one is a grant that may still need revoking. The
	// function reports over-cap and the caller must fail the publish instead.
	previous := make([]string, 0, additiveGrantHardCap+5)
	backend := make([]string, 0, additiveGrantHardCap+5)
	for i := 0; i < additiveGrantHardCap+5; i++ {
		host := "10.0.0." + strconv.Itoa(i)
		previous = append(previous, host)
		backend = append(backend, host)
	}
	kept, capExceeded := compactAdditiveGrants(previous, nil, backend)
	assert.True(t, capExceeded)
	assert.Equal(t, previous, kept, "backend-live entries must never be evicted")

	// Over-cap input with mostly-dead entries resolves through compaction alone.
	resolved, stillOver := compactAdditiveGrants(previous, []string{"current"}, []string{"10.0.0.1"})
	assert.False(t, stillOver)
	assert.Equal(t, []string{"10.0.0.1", "current"}, resolved)
}

// Over-cap backend-live provenance must FAIL the publish (fail-closed, bounded)
// instead of silently evicting entries that additive teardown still needs; the
// same shape with dead entries self-heals through compaction and publishes.
func TestAdditiveNFSPublishFailsWhenBackendLiveProvenanceExceedsCap(t *testing.T) {
	ctx := context.Background()
	buildVolume := func(t *testing.T, client *truenas.MockClient, volumeID string, liveHostCount int) []string {
		t.Helper()
		dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
			Name: "pool/parent/" + volumeID, Type: "FILESYSTEM",
		})
		require.NoError(t, err)
		hosts := make([]string, 0, liveHostCount)
		for i := 0; i < liveHostCount; i++ {
			hosts = append(hosts, "192.0.2."+strconv.Itoa(i+1))
		}
		share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
			Path: dataset.Mountpoint, Hosts: hosts, Enabled: true,
		})
		require.NoError(t, err)
		require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
		record, err := newPublicationRecord(NodeIdentity{
			Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.1")},
		}, csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, false)
		require.NoError(t, err)
		record.CSIAddedNFSHosts = append([]string(nil), hosts...)
		require.NoError(t, storePublicationRecord(
			ctx, client, dataset, dataset.Name, publicationPropertyKey("worker-a"), record,
		))
		return hosts
	}
	newNFSDriver := func(client *truenas.MockClient) *Driver {
		return &Driver{
			name: "org.scale.csi.nfs",
			config: &Config{
				Fencing: FencingConfig{Mode: FencingModeAdditive},
				ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
				NFS:     NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
			},
			truenasClient: client,
		}
	}
	publish := func(d *Driver, volumeID string) error {
		nodeID, err := encodeNodeIdentity(NodeIdentity{
			Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.1")},
		})
		require.NoError(t, err)
		_, publishErr := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: volumeID, NodeId: nodeID,
			VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			}},
			VolumeContext: map[string]string{"node_attach_driver": "nfs"},
		})
		return publishErr
	}

	t.Run("all backend-live over cap fails closed", func(t *testing.T) {
		client := truenas.NewMockClient()
		d := newNFSDriver(client)
		liveHosts := buildVolume(t, client, "nfs-provenance-overflow", additiveGrantHardCap+3)

		err := publish(d, "nfs-provenance-overflow")
		require.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		// The refused publish must not have rewritten the record: every
		// backend-live provenance entry survives for future revocation.
		fresh, err := client.DatasetGet(ctx, "pool/parent/nfs-provenance-overflow")
		require.NoError(t, err)
		records, err := publicationRecordsFromDataset(fresh)
		require.NoError(t, err)
		assert.Equal(t, liveHosts, records[publicationPropertyKey("worker-a")].CSIAddedNFSHosts,
			"backend-live provenance must be fully preserved by a refused publish")
	})

	t.Run("over cap with dead entries resolves through compaction", func(t *testing.T) {
		client := truenas.NewMockClient()
		d := newNFSDriver(client)
		buildVolume(t, client, "nfs-provenance-compacts", additiveGrantHardCap+3)
		// A prior convergence already removed all but one host from the backend:
		// those provenance entries are dead and compaction may retire them.
		share, err := client.NFSShareFindByPath(ctx, "/mnt/pool/parent/nfs-provenance-compacts")
		require.NoError(t, err)
		require.NotNil(t, share)
		_, err = client.NFSShareUpdate(ctx, share.ID, map[string]interface{}{"hosts": []string{"192.0.2.1"}})
		require.NoError(t, err)

		require.NoError(t, publish(d, "nfs-provenance-compacts"))
		fresh, err := client.DatasetGet(ctx, "pool/parent/nfs-provenance-compacts")
		require.NoError(t, err)
		records, err := publicationRecordsFromDataset(fresh)
		require.NoError(t, err)
		assert.Equal(t, []string{"192.0.2.1"},
			records[publicationPropertyKey("worker-a")].CSIAddedNFSHosts)
	})
}

func TestAdditiveNFSIdentityRotationRemovesOldCSIAddedGrant(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:     NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/nfs-identity-rotation", Type: "FILESYSTEM",
	})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Networks: []string{"198.51.100.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	publish := func(address string) string {
		t.Helper()
		nodeID, encodeErr := encodeNodeIdentity(NodeIdentity{
			Name: "worker-a", IPs: []net.IP{net.ParseIP(address)},
		})
		require.NoError(t, encodeErr)
		_, publishErr := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: "nfs-identity-rotation", NodeId: nodeID,
			VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			}},
			VolumeContext: map[string]string{"node_attach_driver": "nfs"},
		})
		require.NoError(t, publishErr)
		return nodeID
	}

	publish("192.0.2.11")
	currentNodeID := publish("192.0.2.12")
	// The second publish already revoked 192.0.2.11 from the backend share, so the
	// idempotent retry compacts that now-stale provenance entry away (it can no
	// longer be re-revoked, and there is nothing left to revoke) while retaining
	// the live grant. Bounding the list this way is the fix for unbounded churn.
	publish("192.0.2.12")
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.12"}, share.Hosts)
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(fresh)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.12"},
		records[publicationPropertyKey("worker-a")].CSIAddedNFSHosts)

	_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "nfs-identity-rotation", NodeId: currentNodeID,
	})
	require.NoError(t, err)
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Empty(t, share.Hosts)
	assert.Equal(t, []string{"198.51.100.0/24"}, share.Networks)
}

func TestAdditiveNFSDeferredIdentityProtectsEarlierCSIAddedGrant(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:     NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/nfs-deferred-provenance", Type: "FILESYSTEM",
	})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Networks: []string{"198.51.100.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	publish := func(nodeID string) {
		t.Helper()
		_, publishErr := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: "nfs-deferred-provenance", NodeId: nodeID,
			VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			}},
			VolumeContext: map[string]string{"node_attach_driver": "nfs"},
		})
		require.NoError(t, publishErr)
	}
	workerA, err := encodeNodeIdentity(NodeIdentity{
		Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.11")},
	})
	require.NoError(t, err)
	workerB, err := encodeNodeIdentity(NodeIdentity{
		Name: "worker-b", IPs: []net.IP{net.ParseIP("192.0.2.12")},
	})
	require.NoError(t, err)
	publish(workerA)
	publish("worker-a") // simulate a temporarily legacy CSINode registration
	publish(workerB)    // force convergence for an enforceable peer

	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"192.0.2.11", "192.0.2.12"}, share.Hosts,
		"deferred identity must not cause its last known dynamic grant to be revoked")
}

func TestAdditiveDeferredAndValidISCSIPublishesPreserveLegacyAllowAll(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.iscsi",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			ISCSI:   ISCSIConfig{TargetPortal: "192.0.2.10:3260"},
		},
		truenasClient: client,
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/broad-iscsi", Type: "VOLUME", Volsize: testGiB})
	require.NoError(t, err)
	target, err := client.ISCSITargetCreate(ctx, "broad-iscsi", "", "ISCSI", []truenas.ISCSITargetGroup{{
		Portal: 1, Initiator: 1, AuthMethod: "NONE",
	}})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropISCSITargetID, strconv.Itoa(target.ID)))
	capability := &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	}}
	publish := func(nodeID string) error {
		_, publishErr := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: "broad-iscsi", NodeId: nodeID, VolumeCapability: capability,
			VolumeContext: map[string]string{"node_attach_driver": "iscsi"},
		})
		return publishErr
	}
	require.NoError(t, publish("legacy-worker"))
	validID, err := encodeNodeIdentity(NodeIdentity{Name: "current-worker", ISCSIIQN: "iqn.1993-08.org.debian:current-worker"})
	require.NoError(t, err)
	require.NoError(t, publish(validID))

	target, err = client.ISCSITargetGet(ctx, target.ID)
	require.NoError(t, err)
	staticPreserved := false
	dynamicPresent := false
	for _, group := range target.Groups {
		if group.Initiator == 1 {
			staticPreserved = true
		}
		if group.Initiator > 1 {
			dynamicPresent = true
		}
	}
	assert.True(t, staticPreserved, "additive must retain the legacy allow-all target group")
	assert.True(t, dynamicPresent, "the enforceable peer still receives its CSI-owned group")
}

func TestAdditiveNVMePublishAndUnpublishPreserveAllowAnyHost(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nvmeof",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			NVMeoF: NVMeoFConfig{Transport: "TCP", TransportAddress: "192.0.2.20", TransportServiceID: 4420,
				SubsystemAllowAnyHost: true},
		},
		truenasClient: client, nvmeResolvedHosts: make(map[string]int),
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/broad-nvme", Type: "VOLUME", Volsize: testGiB})
	require.NoError(t, err)
	require.NoError(t, d.createNVMeoFShareForDataset(ctx, dataset, dataset.Name, "broad-nvme", true, true))
	nodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-a"})
	require.NoError(t, err)
	_, err = d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: "broad-nvme", NodeId: nodeID,
		VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		}},
		VolumeContext: map[string]string{"node_attach_driver": "nvmeof"},
	})
	require.NoError(t, err)
	subsystemID := mustAtoi(t, datasetUserProperty(dataset, PropNVMeoFSubsystemID))
	subsystem, err := client.NVMeoFSubsystemGet(ctx, subsystemID)
	require.NoError(t, err)
	assert.True(t, subsystem.AllowAnyHost)

	_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "broad-nvme", NodeId: nodeID})
	require.NoError(t, err)
	subsystem, err = client.NVMeoFSubsystemGet(ctx, subsystemID)
	require.NoError(t, err)
	assert.True(t, subsystem.AllowAnyHost, "additive teardown must not tighten a legacy allow-any subsystem")
}

// A ZFS clone inherits the source volume's publication_* user property with the
// origin snapshot name as its source (not "local"). The exact-"local" filter must
// exclude it so a freshly cloned volume owns zero publication records and can be
// published without being seen as "published elsewhere".
func TestClonedVolumeInheritsNoPublicationRecordsAndPublishesCleanly(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:     NFSConfig{ShareHost: "192.0.2.10", ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
	}
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM"})
	require.NoError(t, err)
	sourceShare, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: source.Mountpoint, Hosts: []string{"192.0.2.11"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropNFSShareID, strconv.Itoa(sourceShare.ID)))
	sourceRecord, err := newPublicationRecord(NodeIdentity{
		Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.11")},
	}, csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false)
	require.NoError(t, err)
	require.NoError(t, storePublicationRecord(ctx, client, source, source.Name, publicationPropertyKey("worker-a"), sourceRecord))
	snapshot, err := client.SnapshotCreate(ctx, source.Name, "snap-1", map[string]string{PropManagedResource: "true"})
	require.NoError(t, err)

	require.NoError(t, client.SnapshotClone(ctx, snapshot.ID, "pool/parent/clone"))
	clone, err := client.DatasetGet(ctx, "pool/parent/clone")
	require.NoError(t, err)
	inherited, ok := clone.UserProperties[publicationPropertyKey("worker-a")]
	require.True(t, ok, "precondition: the clone inherited the source's publication property")
	require.NotEqual(t, "local", inherited.Source, "precondition: inheritance is reported with an origin-name source")
	records, err := publicationRecordsFromDataset(clone)
	require.NoError(t, err)
	assert.Empty(t, records, "a freshly cloned volume must own zero publication records")

	// Give the clone its own CSI identity + NFS share, then publish it to a
	// different node. The inherited source record must not make this look like the
	// clone is already published elsewhere.
	require.NoError(t, client.DatasetSetUserProperty(ctx, clone.Name, PropCSIVolumeName, "clone"))
	cloneShare, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: clone.Mountpoint, Enabled: false})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, clone.Name, PropNFSShareID, strconv.Itoa(cloneShare.ID)))
	nodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-b", IPs: []net.IP{net.ParseIP("192.0.2.12")}})
	require.NoError(t, err)
	_, err = d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: "clone", NodeId: nodeID,
		VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		}},
		VolumeContext: map[string]string{"node_attach_driver": "nfs"},
	})
	require.NoError(t, err)
	fresh, err := client.DatasetGet(ctx, clone.Name)
	require.NoError(t, err)
	cloneRecords, err := publicationRecordsFromDataset(fresh)
	require.NoError(t, err)
	require.Len(t, cloneRecords, 1)
	_, published := cloneRecords[publicationPropertyKey("worker-b")]
	assert.True(t, published, "the clone publishes cleanly under its own node record")
}

func TestAdditiveNVMeUnpublishUsesDurableCSIAddedProvenance(t *testing.T) {
	for _, test := range []struct {
		name                 string
		preassociate         bool
		wantOwned            []string
		wantAssociationCount int
	}{
		{name: "new dynamic association is removed", wantOwned: []string{"nqn.2014-08.org.nvmexpress:uuid:worker-a"}},
		{name: "matching backend-only static association is preserved", preassociate: true, wantAssociationCount: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			client := truenas.NewMockClient()
			d := &Driver{
				name: "org.scale.csi.nvmeof",
				config: &Config{
					Fencing: FencingConfig{Mode: FencingModeAdditive},
					ZFS:     ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
					NVMeoF: NVMeoFConfig{
						Transport: "TCP", TransportAddress: "192.0.2.20", TransportServiceID: 4420,
						SubsystemAllowAnyHost: true,
					},
				},
				truenasClient: client, nvmeResolvedHosts: make(map[string]int),
			}
			volumeID := "nvme-provenance-" + strconv.FormatBool(test.preassociate)
			dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
				Name: "pool/parent/" + volumeID, Type: "VOLUME", Volsize: testGiB,
			})
			require.NoError(t, err)
			require.NoError(t, d.createNVMeoFShareForDataset(ctx, dataset, dataset.Name, volumeID, true, true))
			subsystemID := mustAtoi(t, datasetUserProperty(dataset, PropNVMeoFSubsystemID))
			nqn := "nqn.2014-08.org.nvmexpress:uuid:worker-a"
			if test.preassociate {
				host, hostErr := client.NVMeoFHostCreate(ctx, nqn)
				require.NoError(t, hostErr)
				_, associationErr := client.NVMeoFHostSubsysCreate(ctx, host.ID, subsystemID)
				require.NoError(t, associationErr)
			}
			nodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: nqn})
			require.NoError(t, err)
			request := &csi.ControllerPublishVolumeRequest{
				VolumeId: volumeID, NodeId: nodeID,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				}},
				VolumeContext: map[string]string{"node_attach_driver": "nvmeof"},
			}
			_, err = d.ControllerPublishVolume(ctx, request)
			require.NoError(t, err)
			_, err = d.ControllerPublishVolume(ctx, request)
			require.NoError(t, err)
			fresh, err := client.DatasetGet(ctx, dataset.Name)
			require.NoError(t, err)
			records, err := publicationRecordsFromDataset(fresh)
			require.NoError(t, err)
			require.Len(t, records, 1)
			assert.Equal(t, test.wantOwned, records[publicationPropertyKey("worker-a")].CSIAddedNVMeNQNs)

			_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
				VolumeId: volumeID, NodeId: nodeID,
			})
			require.NoError(t, err)
			associations, err := client.NVMeoFHostSubsysListBySubsystem(ctx, subsystemID)
			require.NoError(t, err)
			assert.Len(t, associations, test.wantAssociationCount)
		})
	}
}

func TestAdditiveNVMeIdentityRotationRemovesOldCSIAddedAssociation(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nvmeof",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			NVMeoF: NVMeoFConfig{
				Transport: "TCP", TransportAddress: "192.0.2.20", TransportServiceID: 4420,
				SubsystemAllowAnyHost: true,
			},
		},
		truenasClient: client, nvmeResolvedHosts: make(map[string]int),
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/nvme-identity-rotation", Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, d.createNVMeoFShareForDataset(
		ctx, dataset, dataset.Name, "nvme-identity-rotation", true, true,
	))
	subsystemID := mustAtoi(t, datasetUserProperty(dataset, PropNVMeoFSubsystemID))
	publish := func(nqn string) string {
		t.Helper()
		nodeID, encodeErr := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: nqn})
		require.NoError(t, encodeErr)
		_, publishErr := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: "nvme-identity-rotation", NodeId: nodeID,
			VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			}},
			VolumeContext: map[string]string{"node_attach_driver": "nvmeof"},
		})
		require.NoError(t, publishErr)
		return nodeID
	}
	oldNQN := "nqn.2014-08.org.nvmexpress:uuid:worker-a-old"
	newNQN := "nqn.2014-08.org.nvmexpress:uuid:worker-a-new"
	publish(oldNQN)
	currentNodeID := publish(newNQN)
	// The second publish already detached oldNQN from the subsystem, so the
	// idempotent retry compacts that now-stale provenance entry away while
	// retaining the live association. This bounds the list against NQN churn.
	publish(newNQN)
	associations, err := client.NVMeoFHostSubsysListBySubsystem(ctx, subsystemID)
	require.NoError(t, err)
	require.Len(t, associations, 1)
	assert.Equal(t, newNQN, associations[0].HostNQN)
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(fresh)
	require.NoError(t, err)
	assert.Equal(t, []string{newNQN},
		records[publicationPropertyKey("worker-a")].CSIAddedNVMeNQNs)

	_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "nvme-identity-rotation", NodeId: currentNodeID,
	})
	require.NoError(t, err)
	associations, err = client.NVMeoFHostSubsysListBySubsystem(ctx, subsystemID)
	require.NoError(t, err)
	assert.Empty(t, associations)
}

func TestAdditiveNVMeDeferredIdentityProtectsEarlierCSIAddedAssociation(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nvmeof",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			NVMeoF: NVMeoFConfig{
				Transport: "TCP", TransportAddress: "192.0.2.20", TransportServiceID: 4420,
				SubsystemAllowAnyHost: true,
			},
		},
		truenasClient: client, nvmeResolvedHosts: make(map[string]int),
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/nvme-deferred-provenance", Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, d.createNVMeoFShareForDataset(
		ctx, dataset, dataset.Name, "nvme-deferred-provenance", true, true,
	))
	subsystemID := mustAtoi(t, datasetUserProperty(dataset, PropNVMeoFSubsystemID))
	publish := func(nodeID string) {
		t.Helper()
		_, publishErr := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: "nvme-deferred-provenance", NodeId: nodeID,
			VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			}},
			VolumeContext: map[string]string{"node_attach_driver": "nvmeof"},
		})
		require.NoError(t, publishErr)
	}
	workerA, err := encodeNodeIdentity(NodeIdentity{
		Name: "worker-a", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-a",
	})
	require.NoError(t, err)
	workerB, err := encodeNodeIdentity(NodeIdentity{
		Name: "worker-b", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-b",
	})
	require.NoError(t, err)
	publish(workerA)
	publish("worker-a") // simulate a temporarily legacy CSINode registration
	publish(workerB)    // force convergence for an enforceable peer

	associations, err := client.NVMeoFHostSubsysListBySubsystem(ctx, subsystemID)
	require.NoError(t, err)
	require.Len(t, associations, 2)
	assert.ElementsMatch(t,
		[]string{"nqn.2014-08.org.nvmexpress:uuid:worker-a", "nqn.2014-08.org.nvmexpress:uuid:worker-b"},
		[]string{associations[0].HostNQN, associations[1].HostNQN},
		"deferred identity must not cause its last known dynamic association to be revoked",
	)
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

func TestStartupReconcileIgnoresAttachmentDeletedAfterInitialSnapshot(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/stale-startup", Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	nodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.11")}})
	require.NoError(t, err)
	pvName := "pv-stale-startup"
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName}, Spec: corev1.PersistentVolumeSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
			Driver: "csi.scale.io", VolumeHandle: "stale-startup", VolumeAttributes: map[string]string{"node_attach_driver": "nfs"},
		}},
	}}
	attachment := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-stale-startup"},
		Spec: storagev1.VolumeAttachmentSpec{Attacher: "csi.scale.io", NodeName: "worker-a",
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName}},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	csiNode := &storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}, Spec: storagev1.CSINodeSpec{
		Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: nodeID}},
	}}
	kube := kubernetesfake.NewSimpleClientset(pv, attachment, csiNode)
	var attachmentLists atomic.Int32
	kube.PrependReactor("list", "volumeattachments", func(clienttesting.Action) (bool, runtime.Object, error) {
		if attachmentLists.Add(1) == 1 {
			return false, nil, nil
		}
		return true, &storagev1.VolumeAttachmentList{}, nil
	})
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict}, ZFS: ZFSConfig{DatasetParentName: "pool/parent"},
			NFS: NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client, eventRecorder: &EventRecorder{clientset: kube},
	}

	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(fresh)
	require.NoError(t, err)
	assert.Empty(t, records, "the under-lock VA refresh must veto a stale startup grant")
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.0/24"}, share.Networks)
}

func TestStartupReconcileMixedDeferredAndKnownSingleNodeFailsBeforeMutation(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/mixed-single", Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	validID, err := encodeNodeIdentity(NodeIdentity{Name: "known-worker", IPs: []net.IP{net.ParseIP("192.0.2.11")}})
	require.NoError(t, err)
	pvName := "pv-mixed-single"
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName}, Spec: corev1.PersistentVolumeSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
			Driver: "csi.scale.io", VolumeHandle: "mixed-single", VolumeAttributes: map[string]string{"node_attach_driver": "nfs"},
		}},
	}}
	objects := []runtime.Object{pv,
		&storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "va-legacy"}, Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.scale.io", NodeName: "legacy-worker", Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		}, Status: storagev1.VolumeAttachmentStatus{Attached: true}},
		&storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "va-known"}, Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.scale.io", NodeName: "known-worker", Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		}, Status: storagev1.VolumeAttachmentStatus{Attached: true}},
		&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "legacy-worker"}, Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: "legacy-worker"}},
		}},
		&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "known-worker"}, Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: validID}},
		}},
	}
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive}, ZFS: ZFSConfig{DatasetParentName: "pool/parent"},
			NFS: NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client, eventRecorder: &EventRecorder{clientset: kubernetesfake.NewSimpleClientset(objects...)},
	}

	require.Error(t, d.reconcilePublishedAttachments(ctx))
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(fresh)
	require.NoError(t, err)
	assert.Empty(t, records, "all compatibility checks must pass before startup persists either owner")
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.0/24"}, share.Networks)
	assert.Empty(t, share.Hosts)
}

func TestStartupReconcileAdditiveConvergesKnownPeerWhileLegacyPeerDefers(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/mixed-multi", Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Hosts: []string{"192.0.2.99"}, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	knownID, err := encodeNodeIdentity(NodeIdentity{Name: "known-worker", IPs: []net.IP{net.ParseIP("192.0.2.11")}})
	require.NoError(t, err)
	pvName := "pv-mixed-multi"
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName}, Spec: corev1.PersistentVolumeSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
			Driver: "csi.scale.io", VolumeHandle: "mixed-multi", VolumeAttributes: map[string]string{"node_attach_driver": "nfs"},
		}},
	}}
	objects := []runtime.Object{pv,
		&storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "va-multi-legacy"}, Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.scale.io", NodeName: "legacy-worker", Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		}, Status: storagev1.VolumeAttachmentStatus{Attached: true}},
		&storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "va-multi-known"}, Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.scale.io", NodeName: "known-worker", Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		}, Status: storagev1.VolumeAttachmentStatus{Attached: true}},
		&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "legacy-worker"}, Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: "legacy-worker"}},
		}},
		&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "known-worker"}, Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: knownID}},
		}},
	}
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive}, ZFS: ZFSConfig{DatasetParentName: "pool/parent"},
			NFS: NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}, ShareAllowedHosts: []string{"192.0.2.99"}},
		},
		truenasClient: client, eventRecorder: &EventRecorder{clientset: kubernetesfake.NewSimpleClientset(objects...)},
	}

	reconcileErr := d.reconcilePublishedAttachments(ctx)
	require.Error(t, reconcileErr)
	assert.ErrorIs(t, reconcileErr, errFenceDeferred)
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(fresh)
	require.NoError(t, err)
	require.Len(t, records, 2)
	assert.Equal(t, []string{"192.0.2.11"},
		records[publicationPropertyKey("known-worker")].CSIAddedNFSHosts,
		"startup must persist additive ownership before applying the host grant")
	assert.Empty(t, records[publicationPropertyKey("legacy-worker")].CSIAddedNFSHosts)
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"192.0.2.11", "192.0.2.99"}, share.Hosts)
	assert.Equal(t, []string{"192.0.2.0/24"}, share.Networks)
}

func TestStartupReconcileAdditivePreservesNFSGrantDuringIdentityGapThenRotates(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/startup-nfs-identity-gap", Type: "FILESYSTEM",
	})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Hosts: []string{"192.0.2.11"},
		Networks: []string{"198.51.100.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	record, err := newPublicationRecord(NodeIdentity{
		Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.11")},
	}, csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, false)
	require.NoError(t, err)
	record.CSIAddedNFSHosts = []string{"192.0.2.11"}
	require.NoError(t, storePublicationRecord(
		ctx, client, dataset, dataset.Name, publicationPropertyKey("worker-a"), record,
	))
	pvName := "pv-startup-nfs-identity-gap"
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName}, Spec: corev1.PersistentVolumeSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
			Driver: "csi.scale.io", VolumeHandle: "startup-nfs-identity-gap",
			VolumeAttributes: map[string]string{"node_attach_driver": "nfs"},
		}},
	}}
	attachment := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-startup-nfs-identity-gap"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.scale.io", NodeName: "worker-a",
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	csiNode := &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-a"},
		Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{
			Name: "csi.scale.io", NodeID: "worker-a",
		}}},
	}
	kube := kubernetesfake.NewSimpleClientset(pv, attachment, csiNode)
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:     NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client, eventRecorder: &EventRecorder{clientset: kube},
	}

	reconcileErr := d.reconcilePublishedAttachments(ctx)
	require.Error(t, reconcileErr)
	assert.ErrorIs(t, reconcileErr, errFenceDeferred)
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.11"}, share.Hosts,
		"startup defer must preserve the live node's prior CSI-added grant")

	rotatedID, err := encodeNodeIdentity(NodeIdentity{
		Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.12")},
	})
	require.NoError(t, err)
	csiNode.Spec.Drivers[0].NodeID = rotatedID
	_, err = kube.StorageV1().CSINodes().Update(ctx, csiNode, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.12"}, share.Hosts)
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(fresh)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.11", "192.0.2.12"},
		records[publicationPropertyKey("worker-a")].CSIAddedNFSHosts)
}

func TestStartupReconcileAdditivePreservesNVMeGrantDuringIdentityGapThenRotates(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			NVMeoF: NVMeoFConfig{
				Transport: "TCP", TransportAddress: "192.0.2.20", TransportServiceID: 4420,
				SubsystemAllowAnyHost: true,
			},
		},
		truenasClient: client, nvmeResolvedHosts: make(map[string]int),
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/startup-nvme-identity-gap", Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, d.createNVMeoFShareForDataset(
		ctx, dataset, dataset.Name, "startup-nvme-identity-gap", true, true,
	))
	subsystemID := mustAtoi(t, datasetUserProperty(dataset, PropNVMeoFSubsystemID))
	oldNQN := "nqn.2014-08.org.nvmexpress:uuid:worker-a-old"
	oldHost, err := client.NVMeoFHostCreate(ctx, oldNQN)
	require.NoError(t, err)
	_, err = client.NVMeoFHostSubsysCreate(ctx, oldHost.ID, subsystemID)
	require.NoError(t, err)
	record, err := newPublicationRecord(NodeIdentity{
		Name: "worker-a", NVMeNQN: oldNQN,
	}, csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, false)
	require.NoError(t, err)
	record.CSIAddedNVMeNQNs = []string{oldNQN}
	require.NoError(t, storePublicationRecord(
		ctx, client, dataset, dataset.Name, publicationPropertyKey("worker-a"), record,
	))
	pvName := "pv-startup-nvme-identity-gap"
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName}, Spec: corev1.PersistentVolumeSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
			Driver: "csi.scale.io", VolumeHandle: "startup-nvme-identity-gap",
			VolumeAttributes: map[string]string{"node_attach_driver": "nvmeof"},
		}},
	}}
	attachment := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-startup-nvme-identity-gap"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.scale.io", NodeName: "worker-a",
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	csiNode := &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-a"},
		Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{
			Name: "csi.scale.io", NodeID: "worker-a",
		}}},
	}
	kube := kubernetesfake.NewSimpleClientset(pv, attachment, csiNode)
	d.eventRecorder = &EventRecorder{clientset: kube}

	reconcileErr := d.reconcilePublishedAttachments(ctx)
	require.Error(t, reconcileErr)
	assert.ErrorIs(t, reconcileErr, errFenceDeferred)
	associations, err := client.NVMeoFHostSubsysListBySubsystem(ctx, subsystemID)
	require.NoError(t, err)
	require.Len(t, associations, 1)
	assert.Equal(t, oldNQN, associations[0].HostNQN,
		"startup defer must preserve the live node's prior CSI-added association")

	newNQN := "nqn.2014-08.org.nvmexpress:uuid:worker-a-new"
	rotatedID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: newNQN})
	require.NoError(t, err)
	csiNode.Spec.Drivers[0].NodeID = rotatedID
	_, err = kube.StorageV1().CSINodes().Update(ctx, csiNode, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	associations, err = client.NVMeoFHostSubsysListBySubsystem(ctx, subsystemID)
	require.NoError(t, err)
	require.Len(t, associations, 1)
	assert.Equal(t, newNQN, associations[0].HostNQN)
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(fresh)
	require.NoError(t, err)
	// oldNQN is still associated on the subsystem when the rotation publish reads
	// the backend, so both are retained; provenance now preserves first-seen order
	// (oldest first) so the hard cap can evict oldest-first.
	assert.Equal(t, []string{oldNQN, newNQN},
		records[publicationPropertyKey("worker-a")].CSIAddedNVMeNQNs)
}

func TestStartupReconcileRejectsUnknownBackendSingleNodeGrant(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/backend-conflict", Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Hosts: []string{"192.0.2.99"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	nodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.11")}})
	require.NoError(t, err)
	pvName := "pv-backend-conflict"
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName}, Spec: corev1.PersistentVolumeSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
			Driver: "csi.scale.io", VolumeHandle: "backend-conflict", VolumeAttributes: map[string]string{"node_attach_driver": "nfs"},
		}},
	}}
	attachment := &storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "va-backend-conflict"}, Spec: storagev1.VolumeAttachmentSpec{
		Attacher: "csi.scale.io", NodeName: "worker-a", Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
	}, Status: storagev1.VolumeAttachmentStatus{Attached: true}}
	csiNode := &storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}, Spec: storagev1.CSINodeSpec{
		Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: nodeID}},
	}}
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive}, ZFS: ZFSConfig{DatasetParentName: "pool/parent"},
			NFS: NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
		eventRecorder: &EventRecorder{clientset: kubernetesfake.NewSimpleClientset(pv, attachment, csiNode)},
	}

	err = d.reconcilePublishedAttachments(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "published elsewhere")
	fresh, getErr := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, getErr)
	records, recordErr := publicationRecordsFromDataset(fresh)
	require.NoError(t, recordErr)
	assert.Empty(t, records)
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

	// A dual-VA during pod migration is transient, not a process-fatal preflight
	// condition. Once Kubernetes drains the old attachment, the next background
	// attempt converges without a controller restart.
	require.NoError(t, d.eventRecorder.clientset.StorageV1().VolumeAttachments().Delete(
		ctx, "attachment-b", metav1.DeleteOptions{},
	))
	require.NoError(t, d.reconcilePublishedAttachments(ctx))
	ds, err = client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	records, err = publicationRecordsFromDataset(ds)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Contains(t, records, publicationPropertyKey("worker-a"))
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

	client.RejectEmptyISCSITargetGroups = true
	_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "shared-iscsi", NodeId: "worker-b",
	})
	require.NoError(t, err)
	initiator, err = client.ISCSIInitiatorGet(ctx, initiatorID)
	require.NoError(t, err)
	require.NotNil(t, initiator)
	assert.NotNil(t, initiator.Initiators, "a non-nil empty initiator list is deny-all, not allow-all")
	assert.Empty(t, initiator.Initiators)
	target, err = d.resolveISCSITarget(ctx, ds, datasetName)
	require.NoError(t, err)
	require.NotEmpty(t, target.Groups, "last unpublish must retain portal relationships")
}

func TestISCSILastUnpublishReattachesDenyGroupToExistingTargetPortals(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	client.RejectEmptyISCSITargetGroups = true
	datasetName := "pool/parent/interrupted-iscsi"
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME", Volsize: testGiB})
	require.NoError(t, err)
	static, err := client.ISCSIInitiatorCreateWithInitiators(ctx, []string{"iqn.1993-08.org.debian:static"}, "static")
	require.NoError(t, err)
	target, err := client.ISCSITargetCreate(ctx, "interrupted-iscsi", "", "ISCSI", []truenas.ISCSITargetGroup{{
		Portal: 7, Initiator: static.ID, AuthMethod: "NONE",
	}})
	require.NoError(t, err)
	dynamic, err := client.ISCSIInitiatorCreateWithInitiators(ctx, []string{}, "scale-csi fencing: "+datasetName)
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropISCSITargetID:    strconv.Itoa(target.ID),
		PropISCSIInitiatorID: strconv.Itoa(dynamic.ID),
	}))
	d := &Driver{
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict},
			ISCSI:   ISCSIConfig{TargetPortal: "203.0.113.250:3260"}, // deliberately unresolvable
		},
		truenasClient: client,
	}

	require.NoError(t, d.applyISCSIFence(ctx, dataset, datasetName, nil))
	target, err = client.ISCSITargetGet(ctx, target.ID)
	require.NoError(t, err)
	require.Equal(t, []truenas.ISCSITargetGroup{{Portal: 7, Initiator: dynamic.ID, AuthMethod: "NONE"}}, target.Groups,
		"recovery must use the target's actual portal rather than config lookup")
}

func TestStrictISCSIShareCreationStartsWithPortalBoundDenyAllGroup(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	client.RejectEmptyISCSITargetGroups = true
	d := &Driver{
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			ISCSI:   ISCSIConfig{TargetPortal: "192.0.2.10:3260"},
		},
		truenasClient: client,
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/strict-iscsi", Type: "VOLUME", Volsize: testGiB})
	require.NoError(t, err)
	require.NoError(t, d.createISCSIShareForDataset(ctx, dataset, dataset.Name, "strict-iscsi", true, true))
	targetID := mustAtoi(t, datasetUserProperty(dataset, PropISCSITargetID))
	target, err := client.ISCSITargetGet(ctx, targetID)
	require.NoError(t, err)
	require.NotEmpty(t, target.Groups)
	for _, group := range target.Groups {
		assert.Positive(t, group.Portal)
		initiator, getErr := client.ISCSIInitiatorGet(ctx, group.Initiator)
		require.NoError(t, getErr)
		require.NotNil(t, initiator)
		assert.NotNil(t, initiator.Initiators)
		assert.Empty(t, initiator.Initiators, "strict target creation must begin deny-all")
	}
}

func TestAdditiveISCSIFencingPreservesBroadAndRestrictedStaticGroups(t *testing.T) {
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
	require.Len(t, groups, 3)
	assert.Equal(t, 0, groups[0].Initiator)
	assert.Equal(t, 1, groups[1].Initiator)
	assert.Equal(t, 2, groups[2].Initiator)
	assert.Equal(t, "CHAP", groups[2].AuthMethod)
	assert.Equal(t, []string{"192.0.2.0/24"}, groups[2].AuthNetworks)
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

	reconcileErr := d.reconcilePublishedAttachments(ctx)
	require.Error(t, reconcileErr)
	assert.ErrorIs(t, reconcileErr, errFenceDeferred)
	associations, err := client.NVMeoFHostSubsysListBySubsystem(ctx, subsystem.ID)
	require.NoError(t, err)
	require.Len(t, associations, 1)
	assert.Equal(t, host.ID, associations[0].HostID)
	ds, err = client.DatasetGet(ctx, ds.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(ds)
	require.NoError(t, err)
	require.Len(t, records, 1, "additive mode must retain ownership while waiting for node identity")
	assert.Equal(t, "legacy-worker", records[publicationPropertyKey("legacy-worker")].Node)
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

func TestCreateVolumePostCreateOwnershipAndLegacyBackfill(t *testing.T) {
	ctx := context.Background()
	request := func(name string) *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name: name, CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			Parameters:         map[string]string{"protocol": "nfs"},
		}
	}

	client := truenas.NewMockClient()
	client.DropDatasetCreateUserProperties = true
	d := newComplianceTestDriver(client)
	_, err := d.CreateVolume(ctx, request("post-create-owner"))
	require.NoError(t, err)
	created, err := client.DatasetGet(ctx, "pool/parent/post-create-owner")
	require.NoError(t, err)
	assert.True(t, datasetHasLocalUserProperty(created, PropDriverInstanceID, d.driverInstanceID()))

	legacy, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/legacy-owned", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, legacy.Name, map[string]string{
		PropManagedResource: "true", PropCSIVolumeName: "legacy-owned",
	}))
	_, err = d.CreateVolume(ctx, request("legacy-owned"))
	require.NoError(t, err)
	legacy, err = client.DatasetGet(ctx, legacy.Name)
	require.NoError(t, err)
	assert.True(t, datasetHasLocalUserProperty(legacy, PropDriverInstanceID, d.driverInstanceID()))

	foreign, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/foreign-owner", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, foreign.Name, map[string]string{
		PropManagedResource: "true", PropCSIVolumeName: "foreign-owner", PropDriverInstanceID: "another-controller",
	}))
	_, err = d.CreateVolume(ctx, request("foreign-owner"))
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Contains(t, err.Error(), "another-controller")
}

func TestCreateVolumeCreateRaceNeverAdoptsOrDeletesWinner(t *testing.T) {
	ctx := context.Background()
	base := truenas.NewMockClient()
	d := newComplianceTestDriver(base)
	client := &racedDatasetCreateMock{MockClient: base, owner: d.driverInstanceID()}
	d.truenasClient = client

	_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "raced-volume",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
	assert.Empty(t, client.DatasetDeleteCalls, "the losing creator must never clean up the winning controller's dataset")
	assert.Empty(t, client.NFSShares, "the retry path must perform the full compatibility check before creating a share")
	dataset, getErr := client.DatasetGet(ctx, "pool/parent/raced-volume")
	require.NoError(t, getErr)
	assert.True(t, datasetHasLocalUserProperty(dataset, PropDriverInstanceID, d.driverInstanceID()))
}

func TestCreateVolumeCloneRacesNeverAdoptOrDeleteWinner(t *testing.T) {
	ctx := context.Background()

	t.Run("snapshot clone", func(t *testing.T) {
		client := &racedSnapshotCloneDestinationMock{MockClient: truenas.NewMockClient()}
		mustCreateParentDataset(t, client.MockClient)
		d := newComplianceTestDriver(client)
		source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
			Name: "pool/parent/snapshot-race-source", Type: "FILESYSTEM", Refquota: testGiB,
		})
		require.NoError(t, err)
		snapshot, err := client.SnapshotCreate(ctx, source.Name, "snapshot-race-point", nil)
		require.NoError(t, err)

		_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
			Name: "snapshot-clone-race", CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			Parameters:         map[string]string{"protocol": "nfs"},
			VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapshot.Name},
			}},
		})
		require.Error(t, err)
		assert.Equal(t, codes.Aborted, status.Code(err))
		assert.Empty(t, client.DatasetDeleteCalls)
		assert.Empty(t, client.NFSShares)
		winner, getErr := client.DatasetGet(ctx, "pool/parent/snapshot-clone-race")
		require.NoError(t, getErr)
		assert.Empty(t, datasetUserProperty(winner, PropDriverInstanceID), "loser must not stamp the raced winner")
	})

	t.Run("volume clone", func(t *testing.T) {
		client := &racedSnapshotCloneDestinationMock{MockClient: truenas.NewMockClient()}
		mustCreateParentDataset(t, client.MockClient)
		d := newComplianceTestDriver(client)
		source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
			Name: "pool/parent/volume-race-source", Type: "FILESYSTEM", Refquota: testGiB,
		})
		require.NoError(t, err)

		_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
			Name: "volume-clone-race", CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			Parameters:         map[string]string{"protocol": "nfs"},
			VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "volume-race-source"},
			}},
		})
		require.Error(t, err)
		assert.Equal(t, codes.Aborted, status.Code(err))
		assert.Empty(t, client.DatasetDeleteCalls)
		assert.Empty(t, client.NFSShares)
		winner, getErr := client.DatasetGet(ctx, "pool/parent/volume-clone-race")
		require.NoError(t, getErr)
		assert.Empty(t, datasetUserProperty(winner, PropDriverInstanceID))
		_, snapshotErr := client.SnapshotGet(ctx, source.Name+"@clone-source-volume-clone-race")
		require.NoError(t, snapshotErr, "loser must not delete the winner's deterministic origin snapshot")
	})
}

func TestCreateVolumeDetachedCopyRaceNeverMutatesWinner(t *testing.T) {
	ctx := context.Background()
	client := &racedDetachedCopyDestinationMock{MockClient: truenas.NewMockClient()}
	mustCreateParentDataset(t, client.MockClient)
	d := newComplianceTestDriver(client)
	d.config.ZFS.DetachedVolumesFromSnapshots = true
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/copy-race-source", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	snapshot, err := client.SnapshotCreate(ctx, source.Name, "copy-race-point", nil)
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: "detached-copy-race", CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs"},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapshot.Name},
		}},
	})
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
	assert.Empty(t, client.DatasetDeleteCalls)
	assert.Empty(t, client.NFSShares)
	winner, getErr := client.DatasetGet(ctx, "pool/parent/detached-copy-race")
	require.NoError(t, getErr)
	assert.Empty(t, datasetUserProperty(winner, PropDriverInstanceID))
}

func TestCreateVolumeCloneOwnershipUpdateMustPersistBeforeShareCreation(t *testing.T) {
	ctx := context.Background()
	client := &silentCloneOwnerUpdateMock{MockClient: truenas.NewMockClient()}
	mustCreateParentDataset(t, client.MockClient)
	d := newComplianceTestDriver(client)
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/clone-source", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, source.Name, "clone-point", nil)
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "clone-owner-verify",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs"},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "clone-point"},
		}},
	})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "did not persist locally")
	assert.Empty(t, client.NFSShares, "ownership must be verified before any share is created")
	_, getErr := client.DatasetGet(ctx, "pool/parent/clone-owner-verify")
	require.Error(t, getErr, "a clone whose ownership could not be proven is cleaned up")
}

func TestCreateVolumePostCreateOwnershipUpdateMustPersist(t *testing.T) {
	ctx := context.Background()
	client := &silentDatasetPropertyUpdateMock{MockClient: truenas.NewMockClient()}
	d := newComplianceTestDriver(client)

	_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "silent-owner-update",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "did not persist locally")
	assert.Empty(t, client.NFSShares)
	_, getErr := client.DatasetGet(ctx, "pool/parent/silent-owner-update")
	require.Error(t, getErr, "the caller may clean up only the dataset it definitely created")
}

func TestStartupReconcileIsolatesPerVolumeFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := truenas.NewMockClient()
	goodDataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/good", Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: goodDataset.Mountpoint, Enabled: true})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, goodDataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))

	objects := []runtime.Object{}
	for _, volumeID := range []string{"good", "missing"} {
		pvName := "pv-" + volumeID
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvName},
			Spec: corev1.PersistentVolumeSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "csi.scale.io", VolumeHandle: volumeID,
					VolumeAttributes: map[string]string{"node_attach_driver": "nfs"},
				}},
			},
		}
		nodeID, encodeErr := encodeNodeIdentity(NodeIdentity{Name: "worker-" + volumeID, IPs: []net.IP{net.ParseIP("192.0.2.11")}})
		require.NoError(t, encodeErr)
		objects = append(objects, pv,
			&storagev1.VolumeAttachment{
				ObjectMeta: metav1.ObjectMeta{Name: "va-" + volumeID},
				Spec: storagev1.VolumeAttachmentSpec{Attacher: "csi.scale.io", NodeName: "worker-" + volumeID,
					Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName}},
				Status: storagev1.VolumeAttachmentStatus{Attached: true},
			},
			&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "worker-" + volumeID}, Spec: storagev1.CSINodeSpec{
				Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: nodeID}},
			}},
		)
	}
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict}, ZFS: ZFSConfig{DatasetParentName: "pool/parent"},
			NFS: NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
		eventRecorder: &EventRecorder{clientset: kubernetesfake.NewSimpleClientset(objects...)},
	}
	err = d.reconcilePublishedAttachments(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing")
	goodDataset, err = client.DatasetGet(ctx, goodDataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(goodDataset)
	require.NoError(t, err)
	require.Len(t, records, 1, "one broken volume must not prevent another worker from converging")
}

func TestBackgroundStartupStrictReadinessGatesUntilConverged(t *testing.T) {
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict, StartupReconcileTimeout: "1s"},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
		},
		truenasClient: truenas.NewMockClient(),
		eventRecorder: &EventRecorder{clientset: kubernetesfake.NewSimpleClientset()},
	}
	d.ready.Store(false)
	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)
	require.Eventually(t, d.ready.Load, time.Second, 10*time.Millisecond,
		"strict readiness becomes true only after the background attachment snapshot converges")
}

func TestBackgroundStartupAdditiveWaitsForDeferredTrigger(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/later-attachment", Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path: dataset.Mountpoint, Networks: []string{"192.0.2.0/24"}, Enabled: true,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	kube := kubernetesfake.NewSimpleClientset()
	var attachmentLists atomic.Int32
	kube.PrependReactor("list", "volumeattachments", func(clienttesting.Action) (bool, runtime.Object, error) {
		attachmentLists.Add(1)
		return false, nil, nil
	})
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive, StartupReconcileTimeout: "1s"},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:     NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client, eventRecorder: &EventRecorder{clientset: kube},
	}
	d.startStartupAttachmentReconcile()
	t.Cleanup(d.stopStartupAttachmentReconcile)
	require.Eventually(t, func() bool { return attachmentLists.Load() > 0 }, time.Second, time.Millisecond,
		"the first empty startup pass must complete before the attachment appears")
	initialLists := attachmentLists.Load()
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, initialLists, attachmentLists.Load(),
		"a converged additive controller must not poll the whole cluster indefinitely")

	nodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-later", IPs: []net.IP{net.ParseIP("192.0.2.11")}})
	require.NoError(t, err)
	pvName := "pv-later-attachment"
	_, err = kube.CoreV1().PersistentVolumes().Create(ctx, &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName}, Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
				Driver: "csi.scale.io", VolumeHandle: "later-attachment", VolumeAttributes: map[string]string{"node_attach_driver": "nfs"},
			}},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = kube.StorageV1().CSINodes().Create(ctx, &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-later"}, Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: nodeID}},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = kube.StorageV1().VolumeAttachments().Create(ctx, &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-later-attachment"},
		Spec: storagev1.VolumeAttachmentSpec{Attacher: "csi.scale.io", NodeName: "worker-later",
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName}},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(30 * time.Millisecond)
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(fresh)
	require.NoError(t, err)
	assert.Empty(t, records, "new attachments are reconciled only after a real deferral signals work")

	d.recordFencingDeferred(NodeIdentity{Name: "worker-later"}, ShareTypeNFS, "missing_identity", "test trigger")

	require.Eventually(t, func() bool {
		fresh, getErr := client.DatasetGet(ctx, dataset.Name)
		if getErr != nil {
			return false
		}
		records, recordErr := publicationRecordsFromDataset(fresh)
		return recordErr == nil && len(records) == 1
	}, time.Second, 10*time.Millisecond,
		"a deferred publish must trigger additive startup convergence without polling")
	convergedLists := attachmentLists.Load()
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, convergedLists, attachmentLists.Load(),
		"the triggered retry must return to an idle wait after convergence")
}

func TestStrictStartupGateBlocksGrantRPCButKeepsProbeAndTeardownUsable(t *testing.T) {
	d := &Driver{
		runController: true,
		config:        &Config{Fencing: FencingConfig{Mode: FencingModeStrict}},
	}
	d.ready.Store(false)
	handlerCalled := false
	handler := func(context.Context, interface{}) (interface{}, error) {
		handlerCalled = true
		return &csi.ControllerPublishVolumeResponse{}, nil
	}

	_, err := d.logInterceptor(context.Background(), &csi.ControllerPublishVolumeRequest{},
		&grpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/ControllerPublishVolume"}, handler)
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.False(t, handlerCalled, "strict startup must gate grants at the Unix-socket RPC boundary")

	assert.False(t, d.strictStartupControllerRPCBlocked("/csi.v1.Identity/Probe"))
	assert.False(t, d.strictStartupControllerRPCBlocked("/csi.v1.Controller/ControllerUnpublishVolume"),
		"teardown must remain available to drain a transient duplicate VolumeAttachment")
	d.ready.Store(true)
	assert.False(t, d.strictStartupControllerRPCBlocked("/csi.v1.Controller/ControllerPublishVolume"))
}

// FIX 4 regression: on a backend that omits the expanded hostnqn field, a
// republish resolves the CURRENT NQN's association by HostID. That association
// is backend-live and must keep the NQN in provenance — otherwise compaction
// drops it and the final unpublish can never revoke the association.
func TestAdditiveNVMeHostnqnlessRepublishRetainsProvenanceForUnpublish(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	client.EmptyNVMeHostNQN = true
	d := &Driver{
		name: "org.scale.csi.nvmeof",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeAdditive},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			NVMeoF: NVMeoFConfig{
				Transport: "TCP", TransportAddress: "192.0.2.20", TransportServiceID: 4420,
				SubsystemAllowAnyHost: true,
			},
		},
		truenasClient: client, nvmeResolvedHosts: make(map[string]int),
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/nvme-hostnqnless", Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, d.createNVMeoFShareForDataset(ctx, dataset, dataset.Name, "nvme-hostnqnless", true, true))
	subsystemID := mustAtoi(t, datasetUserProperty(dataset, PropNVMeoFSubsystemID))
	nqn := "nqn.2014-08.org.nvmexpress:uuid:worker-a"
	nodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: nqn})
	require.NoError(t, err)
	request := &csi.ControllerPublishVolumeRequest{
		VolumeId: "nvme-hostnqnless", NodeId: nodeID,
		VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		}},
		VolumeContext: map[string]string{"node_attach_driver": "nvmeof"},
	}

	_, err = d.ControllerPublishVolume(ctx, request)
	require.NoError(t, err)
	// The republish resolves the live association only by HostID (hostnqn is
	// hidden); the current NQN must survive compaction as backend-live.
	_, err = d.ControllerPublishVolume(ctx, request)
	require.NoError(t, err)
	fresh, err := client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(fresh)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, []string{nqn}, records[publicationPropertyKey("worker-a")].CSIAddedNVMeNQNs,
		"the live NQN must survive a hostnqn-less republish")

	_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "nvme-hostnqnless", NodeId: nodeID,
	})
	require.NoError(t, err)
	associations, err := client.NVMeoFHostSubsysListBySubsystem(ctx, subsystemID)
	require.NoError(t, err)
	assert.Empty(t, associations, "unpublish must revoke the association added by publish")
}

// FIX 1 regression: ControllerPublishVolume MUST be idempotent for a repeated
// (volume, node) publish. A same-node republish re-affirms the grant instead of
// failing AlreadyExists; only a genuine single-node<->multi-node capability
// change is rejected, and as InvalidArgument rather than a silent success.
func TestControllerPublishSameNodeRepublishIsIdempotent(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:     NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
	}
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/same-node", Type: "FILESYSTEM"})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: dataset.Mountpoint, Enabled: true})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))

	nodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.11")}})
	require.NoError(t, err)
	publish := func(mode csi.VolumeCapability_AccessMode_Mode, readonly bool) error {
		_, publishErr := d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: "same-node", NodeId: nodeID, Readonly: readonly,
			VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode}},
			VolumeContext:    map[string]string{"node_attach_driver": "nfs"},
		})
		return publishErr
	}

	require.NoError(t, publish(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false))

	// Same (volume, node), identical parameters: idempotent success.
	require.NoError(t, publish(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false))

	// Same node, readonly flip: tolerated (the backend fence is unchanged).
	require.NoError(t, publish(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, true))

	// Same node, same-family access-mode change: tolerated.
	require.NoError(t, publish(csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER, false))

	// The grant is still exactly worker-a's.
	share, err = client.NFSShareGet(ctx, share.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.11"}, share.Hosts)
	dataset, err = client.DatasetGet(ctx, dataset.Name)
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(dataset)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Contains(t, records, publicationPropertyKey("worker-a"))

	// A genuine single-node -> multi-node capability change on the same node is
	// incompatible and must be rejected explicitly, not silently accepted.
	err = publish(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, false)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// newTakeoverTestDriver builds a strict NFS fencing driver whose event recorder
// carries the supplied Kubernetes objects plus a non-nil dynamic client, so the
// synchronous stale-publication takeover can consult the VolumeAttachment list.
func newTakeoverTestDriver(client *truenas.MockClient, objects ...runtime.Object) *Driver {
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			volumeSnapshotContentGVR: "VolumeSnapshotContentList",
			volumeSnapshotGVR:        "VolumeSnapshotList",
		},
	)
	return &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:     NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
		eventRecorder: &EventRecorder{
			clientset:     kubernetesfake.NewSimpleClientset(objects...),
			dynamicClient: dynamicClient,
		},
	}
}

func takeoverNFSVolume(t *testing.T, ctx context.Context, client *truenas.MockClient, volumeID string) int {
	t.Helper()
	dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/" + volumeID, Type: "FILESYSTEM",
	})
	require.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: dataset.Mountpoint, Enabled: true})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, dataset.Name, PropNFSShareID, strconv.Itoa(share.ID)))
	return share.ID
}

func takeoverPublishRequest(volumeID, node, ip string) *csi.ControllerPublishVolumeRequest {
	nodeID, _ := encodeNodeIdentity(NodeIdentity{Name: node, IPs: []net.IP{net.ParseIP(ip)}})
	return &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID, NodeId: nodeID,
		VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		}},
		VolumeContext: map[string]string{"node_attach_driver": "nfs"},
	}
}

func takeoverPV(volumeID string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-" + volumeID},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
				Driver: "csi.scale.io", VolumeHandle: volumeID,
				VolumeAttributes: map[string]string{"node_attach_driver": "nfs"},
			}},
		},
	}
}

func takeoverVA(name, volumeID, node string) *storagev1.VolumeAttachment {
	pvName := "pv-" + volumeID
	return &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.scale.io", NodeName: node,
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
}

// FIX 2 regression: a SINGLE_NODE publish for node B whose durable record still
// points at node A takes over synchronously when A has no live VolumeAttachment
// (stale record): A's record and backend allowlist entry are revoked and B is
// granted, instead of stalling on FailedPrecondition until the grace-period
// reconcile runs.
func TestControllerPublishTakesOverStaleSingleNodeRecord(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	shareID := takeoverNFSVolume(t, ctx, client, "takeover-stale")
	// The mover was rescheduled to worker-b: its VolumeAttachment exists, but
	// worker-a's attachment is gone, leaving a stale publication record.
	d := newTakeoverTestDriver(client,
		takeoverPV("takeover-stale"),
		takeoverVA("va-takeover-b", "takeover-stale", "worker-b"),
	)

	_, err := d.ControllerPublishVolume(ctx, takeoverPublishRequest("takeover-stale", "worker-a", "192.0.2.11"))
	require.NoError(t, err)
	share, err := client.NFSShareGet(ctx, shareID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.11"}, share.Hosts)

	_, err = d.ControllerPublishVolume(ctx, takeoverPublishRequest("takeover-stale", "worker-b", "192.0.2.12"))
	require.NoError(t, err)

	share, err = client.NFSShareGet(ctx, shareID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.12"}, share.Hosts, "worker-a's allowlist entry must be revoked and worker-b granted")
	dataset, err := client.DatasetGet(ctx, "pool/parent/takeover-stale")
	require.NoError(t, err)
	records, err := publicationRecordsFromDataset(dataset)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Contains(t, records, publicationPropertyKey("worker-b"))
	assert.NotContains(t, records, publicationPropertyKey("worker-a"))
}

// FIX 2 safety guard: a LIVE VolumeAttachment on the blocking node is a genuine
// double-mount and must keep returning FailedPrecondition (no takeover).
func TestControllerPublishKeepsConflictWhenBlockingNodeStillAttached(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	shareID := takeoverNFSVolume(t, ctx, client, "takeover-live")
	d := newTakeoverTestDriver(client,
		takeoverPV("takeover-live"),
		takeoverVA("va-takeover-live-a", "takeover-live", "worker-a"),
		takeoverVA("va-takeover-live-b", "takeover-live", "worker-b"),
	)

	_, err := d.ControllerPublishVolume(ctx, takeoverPublishRequest("takeover-live", "worker-a", "192.0.2.11"))
	require.NoError(t, err)

	_, err = d.ControllerPublishVolume(ctx, takeoverPublishRequest("takeover-live", "worker-b", "192.0.2.12"))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "worker-a")

	share, err := client.NFSShareGet(ctx, shareID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.11"}, share.Hosts, "a live blocking attachment must not be revoked")
	dataset, getErr := client.DatasetGet(ctx, "pool/parent/takeover-live")
	require.NoError(t, getErr)
	records, recErr := publicationRecordsFromDataset(dataset)
	require.NoError(t, recErr)
	require.Len(t, records, 1)
	assert.Contains(t, records, publicationPropertyKey("worker-a"))
}

// FIX 2 safety guard: when the VolumeAttachment lister errors (unsynced /
// unavailable), takeover is refused and the conservative FailedPrecondition is
// returned (fail safe), leaving the blocking record untouched.
func TestControllerPublishFailsSafeWhenAttachmentListUnavailable(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	shareID := takeoverNFSVolume(t, ctx, client, "takeover-unsafe")
	kube := kubernetesfake.NewSimpleClientset(takeoverPV("takeover-unsafe"))
	kube.PrependReactor("list", "volumeattachments", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("informer cache not synced")
	})
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			volumeSnapshotContentGVR: "VolumeSnapshotContentList",
			volumeSnapshotGVR:        "VolumeSnapshotList",
		},
	)
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			Fencing: FencingConfig{Mode: FencingModeStrict},
			ZFS:     ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:     NFSConfig{ShareAllowedNetworks: []string{"192.0.2.0/24"}},
		},
		truenasClient: client,
		eventRecorder: &EventRecorder{clientset: kube, dynamicClient: dynamicClient},
	}

	_, err := d.ControllerPublishVolume(ctx, takeoverPublishRequest("takeover-unsafe", "worker-a", "192.0.2.11"))
	require.NoError(t, err)

	_, err = d.ControllerPublishVolume(ctx, takeoverPublishRequest("takeover-unsafe", "worker-b", "192.0.2.12"))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "an unavailable lister must fail safe, not take over")

	share, err := client.NFSShareGet(ctx, shareID)
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.11"}, share.Hosts, "fail-safe must leave the blocking grant intact")
}
