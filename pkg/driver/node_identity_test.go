package driver

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeIdentityRoundTripAndForwardCompatibility(t *testing.T) {
	encoded, err := encodeNodeIdentity(NodeIdentity{
		Name:     "worker-a",
		NVMeNQN:  "nqn.2014-08.org.nvmexpress:uuid:worker-a",
		ISCSIIQN: "iqn.1993-08.org.debian:worker-a",
		IPs: []net.IP{
			net.ParseIP("2001:db8::10"),
			net.ParseIP("192.0.2.10"),
			net.ParseIP("192.0.2.10"),
		},
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(encoded), maxCSINodeIDBytes)

	identity, err := parseNodeIdentity(encoded)
	require.NoError(t, err)
	assert.Equal(t, "worker-a", identity.Name)
	assert.Equal(t, "nqn.2014-08.org.nvmexpress:uuid:worker-a", identity.NVMeNQN)
	assert.Equal(t, "iqn.1993-08.org.debian:worker-a", identity.ISCSIIQN)
	assert.Equal(t, []string{"192.0.2.10", "2001:db8::10"}, nodeIPStrings(identity.IPs))
	assert.False(t, identity.Legacy)

	// Future encoders may append TLVs that this controller does not know. The
	// current fields must remain readable instead of making rolling upgrades
	// depend on lockstep controller/node replacement.
	raw, err := base64.RawURLEncoding.DecodeString(encoded[len(nodeIdentityPrefix):])
	require.NoError(t, err)
	raw = append(raw, 127, 3, 'n', 'e', 'w')
	identity, err = parseNodeIdentity(nodeIdentityPrefix + base64.RawURLEncoding.EncodeToString(raw))
	require.NoError(t, err)
	assert.Equal(t, "worker-a", identity.Name)
	assert.Equal(t, "nqn.2014-08.org.nvmexpress:uuid:worker-a", identity.NVMeNQN)
}

func TestParseNodeIdentityTreatsPlainNodeIDAsLegacyName(t *testing.T) {
	identity, err := parseNodeIdentity("worker-from-old-node-plugin")
	require.NoError(t, err)
	assert.Equal(t, "worker-from-old-node-plugin", identity.Name)
	assert.True(t, identity.Legacy)
	assert.Empty(t, identity.NVMeNQN)
	assert.Empty(t, identity.ISCSIIQN)
	assert.Empty(t, identity.IPs)
}

func TestParseNodeIdentityRejectsVersionZero(t *testing.T) {
	raw := []byte{0, nodeIdentityFieldName, byte(len("worker-a"))}
	raw = append(raw, "worker-a"...)
	_, err := parseNodeIdentity(nodeIdentityPrefix + base64.RawURLEncoding.EncodeToString(raw))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported node identity version 0")
}

func TestEncodeNodeIdentityPrioritizesExactIdentityOverSecondaryIPsAndFormatting(t *testing.T) {
	ips := make([]net.IP, 0, 32)
	for index := 1; index <= 32; index++ {
		ips = append(ips, net.ParseIP("2001:db8::"+fmt.Sprintf("%x", index)))
	}
	encoded, err := encodeNodeIdentity(NodeIdentity{
		Name:     "  worker-a  ",
		NVMeNQN:  "  nqn.2014-08.org.nvmexpress:uuid:worker-a  ",
		ISCSIIQN: "  iqn.1993-08.org.debian:worker-a  ",
		IPs:      ips,
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(encoded), maxCSINodeIDBytes)
	decoded, err := parseNodeIdentity(encoded)
	require.NoError(t, err)
	assert.Equal(t, "worker-a", decoded.Name)
	assert.Equal(t, "nqn.2014-08.org.nvmexpress:uuid:worker-a", decoded.NVMeNQN)
	assert.Equal(t, "iqn.1993-08.org.debian:worker-a", decoded.ISCSIIQN)
	assert.NotEmpty(t, decoded.IPs)
	assert.Less(t, len(decoded.IPs), len(ips), "secondary IPs are the first semantic class dropped at 256 bytes")
}

func nodeIPStrings(ips []net.IP) []string {
	values := make([]string, 0, len(ips))
	for _, ip := range ips {
		values = append(values, ip.String())
	}
	return values
}

// TestEncodeNodeIdentityRejectsDenyAllSentinelIQN pins the round-2 F2 fix at the
// node_id / publication-record choke point: encodeNodeIdentity (which both
// NodeGetInfo's node_id and newPublicationRecord flow through) must reject the
// reserved deny-all sentinel so it can never be packed into a node identity.
func TestEncodeNodeIdentityRejectsDenyAllSentinelIQN(t *testing.T) {
	_, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", ISCSIIQN: iscsiDenyAllSentinelIQN})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved iSCSI fencing identity")

	// A real IQN still encodes.
	_, err = encodeNodeIdentity(NodeIdentity{Name: "worker-a", ISCSIIQN: "iqn.1993-08.org.debian:worker-a"})
	require.NoError(t, err)
}

// TestDiscoverNodeIdentityRejectsDenyAllSentinelIQN proves a misconfigured
// initiatorname.iscsi carrying the sentinel is refused as the node's identity
// (treated as unreported) rather than propagated into node_id. The node plugin
// still starts for other protocols; fenced iSCSI publish then fails closed.
func TestDiscoverNodeIdentityRejectsDenyAllSentinelIQN(t *testing.T) {
	origRead := nodeReadIdentityFile
	origCmd := nodeIdentityCommand
	origAddrs := nodeInterfaceAddrs
	t.Cleanup(func() {
		nodeReadIdentityFile = origRead
		nodeIdentityCommand = origCmd
		nodeInterfaceAddrs = origAddrs
	})
	nodeIdentityCommand = func(context.Context, string, ...string) ([]byte, error) {
		return nil, fmt.Errorf("nvme not installed")
	}
	nodeInterfaceAddrs = func() ([]net.Addr, error) { return nil, nil }
	nodeReadIdentityFile = func(path string) ([]byte, error) {
		if path == "/etc/iscsi/initiatorname.iscsi" {
			return []byte("InitiatorName=" + iscsiDenyAllSentinelIQN + "\n"), nil
		}
		return nil, fmt.Errorf("no such file")
	}

	identity := discoverNodeIdentity(context.Background(), "worker-a")
	assert.Equal(t, "worker-a", identity.Name)
	assert.Empty(t, identity.ISCSIIQN, "the reserved deny-all sentinel must never be accepted as a node IQN")
}

// TestParsedDenyAllSentinelNodeIDIsRejectedAtValidation covers an
// already-persisted collision: parseNodeIdentity stays intentionally tolerant so
// a legacy node_id carrying the sentinel is surfaced (not silently dropped), but
// the parsed identity must then be rejected at protocol validation so it is never
// enforced as an allowlist grant.
func TestParsedDenyAllSentinelNodeIDIsRejectedAtValidation(t *testing.T) {
	raw := []byte{nodeIdentityVersion}
	raw = append(raw, nodeIdentityFieldName, byte(len("worker-a")))
	raw = append(raw, "worker-a"...)
	raw = append(raw, nodeIdentityFieldIQN, byte(len(iscsiDenyAllSentinelIQN)))
	raw = append(raw, iscsiDenyAllSentinelIQN...)
	identity, err := parseNodeIdentity(nodeIdentityPrefix + base64.RawURLEncoding.EncodeToString(raw))
	require.NoError(t, err, "parse stays tolerant so a persisted collision is surfaced, not dropped")
	require.Equal(t, iscsiDenyAllSentinelIQN, identity.ISCSIIQN)

	require.Error(t, validateIdentityForProtocol(identity, ShareTypeISCSI),
		"a persisted sentinel identity must be rejected before it can be enforced as a grant")
}
