package driver

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// A node that mounts its NAS over a storage fabric presents the fabric
// address to the NFS server. These tests pin nfs.nodeIdentityNetworks: the
// default leaves every node_id byte-identical, the opt-in adds the fabric
// address, and the controller then stamps it onto the export's hosts.

const (
	identityTestNQN     = "nqn.2014-08.org.nvmexpress:uuid:0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718"
	identityTestMgmtIP  = "198.51.100.11"
	identityTestFabric  = "192.168.201.21"
	identityTestFabric6 = "fd00:201::21"
)

// fakeNodeIdentityHost points discovery at fixed interfaces and no nvme/iscsi
// identity, and counts interface listings.
func fakeNodeIdentityHost(t *testing.T, nodeIP string, interfaces ...string) *int {
	t.Helper()
	origRead, origCmd, origAddrs := nodeReadIdentityFile, nodeIdentityCommand, nodeInterfaceAddrs
	t.Cleanup(func() {
		nodeReadIdentityFile, nodeIdentityCommand, nodeInterfaceAddrs = origRead, origCmd, origAddrs
	})
	nodeIdentityCommand = func(context.Context, string, ...string) ([]byte, error) {
		return []byte(identityTestNQN + "\n"), nil
	}
	nodeReadIdentityFile = func(string) ([]byte, error) { return nil, fmt.Errorf("absent") }
	listed := 0
	nodeInterfaceAddrs = func() ([]net.Addr, error) {
		listed++
		addrs := make([]net.Addr, 0, len(interfaces))
		for _, value := range interfaces {
			ip, network, err := net.ParseCIDR(value)
			require.NoError(t, err)
			addrs = append(addrs, &net.IPNet{IP: ip, Mask: network.Mask})
		}
		return addrs, nil
	}
	t.Setenv("NODE_IP", nodeIP)
	t.Setenv("NODE_IPS", "")
	return &listed
}

func discoveredNodeID(t *testing.T, networks []string) string {
	t.Helper()
	parsed, err := parseNodeIdentityNetworks(networks)
	require.NoError(t, err)
	identity := discoverNodeIdentity(context.Background(), "k8s-1", parsed)
	nodeID, err := encodeNodeIdentity(identity)
	require.NoError(t, err)
	return nodeID
}

func TestNodeIdentityNetworksDefaultLeavesNodeIDUnchanged(t *testing.T) {
	interfaces := []string{identityTestMgmtIP + "/24", identityTestFabric + "/24", identityTestFabric6 + "/64", "10.244.1.7/32"}
	listed := fakeNodeIdentityHost(t, identityTestMgmtIP, interfaces...)

	// The node_id every release before this one produced for this node.
	want, err := encodeNodeIdentity(NodeIdentity{Name: "k8s-1", NVMeNQN: identityTestNQN, IPs: []net.IP{net.ParseIP(identityTestMgmtIP)}})
	require.NoError(t, err)

	assert.Equal(t, want, discoveredNodeID(t, nil), "no nfs.nodeIdentityNetworks: the node_id is unchanged")
	assert.Equal(t, want, discoveredNodeID(t, []string{}), "an empty list is the default")
	assert.Zero(t, *listed, "with NODE_IP set and no networks the interfaces are never read")

	// The interface fallback (no NODE_IP) is also unchanged.
	t.Setenv("NODE_IP", "")
	fallback, err := encodeNodeIdentity(NodeIdentity{Name: "k8s-1", NVMeNQN: identityTestNQN, IPs: []net.IP{
		net.ParseIP(identityTestMgmtIP), net.ParseIP(identityTestFabric), net.ParseIP(identityTestFabric6), net.ParseIP("10.244.1.7"),
	}})
	require.NoError(t, err)
	assert.Equal(t, fallback, discoveredNodeID(t, nil))
}

func TestNodeIdentityNetworksAddTheFabricAddresses(t *testing.T) {
	fakeNodeIdentityHost(t, identityTestMgmtIP,
		identityTestMgmtIP+"/24", identityTestFabric+"/24", "192.168.202.21/24", identityTestFabric6+"/64", "fe80::21/64", "10.244.1.7/32")

	nodeID := discoveredNodeID(t, []string{"192.168.201.0/24", "fd00:201::/64", "fe80::/10"})
	identity, err := parseNodeIdentity(nodeID)
	require.NoError(t, err)
	got := make([]string, 0, len(identity.IPs))
	for _, ip := range identity.IPs {
		got = append(got, ip.String())
	}
	assert.Equal(t, []string{identityTestFabric, identityTestMgmtIP, identityTestFabric6}, got,
		"the MGMT address stays; only fabric addresses inside a listed network are added; link-local never")
}

func TestNodeIdentityDroppedIPsNamesOnlyIdentityNetworkAddresses(t *testing.T) {
	networks, err := parseNodeIdentityNetworks([]string{"192.168.201.0/24"})
	require.NoError(t, err)
	identity := NodeIdentity{Name: strings.Repeat("n", 60), NVMeNQN: identityTestNQN, ISCSIIQN: "iqn.2004-10.com.ubuntu:01:5f1d3a9c2b7e"}
	for i := 1; i <= 8; i++ {
		identity.IPs = append(identity.IPs, net.IPv4(192, 168, 201, byte(i)))
	}
	identity.IPs = append(identity.IPs, net.ParseIP("203.0.113.9"))
	nodeID, err := encodeNodeIdentity(identity)
	require.NoError(t, err)
	parsed, err := parseNodeIdentity(nodeID)
	require.NoError(t, err)
	require.Less(t, len(parsed.IPs), 8, "the fixture must overflow the 256-byte limit")

	dropped := nodeIdentityDroppedIPs(identity, networks, nodeID)
	require.NotEmpty(t, dropped)
	for _, ip := range dropped {
		assert.True(t, nodeIdentityNetworksContain(networks, ip), "%s is not an identity-network address", ip)
	}
	assert.Len(t, dropped, 8-len(parsed.IPs))
	assert.Empty(t, nodeIdentityDroppedIPs(identity, nil, nodeID), "nothing to report without networks")
}

func TestLoadConfigNodeIdentityNetworks(t *testing.T) {
	base := requiredTestConfig + "nfs:\n  enabled: true\n  shareHost: 192.0.2.10\n"
	cfg, err := loadTestConfig(t, base)
	require.NoError(t, err)
	assert.Empty(t, cfg.NFS.NodeIdentityNetworks, "unset by default")

	cfg, err = loadTestConfig(t, base+"  nodeIdentityNetworks: [192.168.201.0/24, \"fd00:201::/64\", 192.168.202.21]\n")
	require.NoError(t, err)
	assert.Equal(t, []string{"192.168.201.0/24", "fd00:201::/64", "192.168.202.21"}, cfg.NFS.NodeIdentityNetworks)

	for _, bad := range []string{"192.168.201.0/33", "nas01", "\"::ffff:192.168.201.0/120\"", "\"fe80::1%eth0\""} {
		_, err = loadTestConfig(t, base+"  nodeIdentityNetworks: ["+bad+"]\n")
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "nfs.nodeIdentityNetworks", bad)
	}
}

// TestStrictNFSFenceGrantsTheFabricAddress is the blocker end to end: a node
// whose identity carries its fabric address gets that address into the
// export's hosts, so a mount from the fabric is no longer refused.
func TestStrictNFSFenceGrantsTheFabricAddress(t *testing.T) {
	ctx := context.Background()
	fakeNodeIdentityHost(t, identityTestMgmtIP, identityTestMgmtIP+"/24", identityTestFabric+"/24")

	for _, tc := range []struct {
		name      string
		networks  []string
		wantHosts []string
	}{
		{"default: the MGMT address only, the fabric mount is refused", nil, []string{identityTestMgmtIP}},
		{"identity networks: the fabric address is granted", []string{"192.168.201.0/24"}, []string{identityTestFabric, identityTestMgmtIP}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newFencingTestHarness(t, FencingModeStrict, ShareTypeNFS, withNFSShareConfig("192.168.201.10"))
			d, client := h.d, h.client
			datasetName := "pool/parent/fabric-nfs"
			dataset, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
			require.NoError(t, err)
			share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: dataset.Mountpoint, Enabled: true})
			require.NoError(t, err)
			require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropNFSShareID, strconv.Itoa(share.ID)))

			_, err = d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
				VolumeId: "fabric-nfs", NodeId: discoveredNodeID(t, tc.networks),
				VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				}},
				VolumeContext: map[string]string{"node_attach_driver": "nfs"},
			})
			require.NoError(t, err)
			share, err = client.NFSShareGet(ctx, share.ID)
			require.NoError(t, err)
			assert.Equal(t, tc.wantHosts, share.Hosts)
		})
	}
}
