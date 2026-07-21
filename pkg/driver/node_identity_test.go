package driver

import (
	"encoding/base64"
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

func nodeIPStrings(ips []net.IP) []string {
	values := make([]string, 0, len(ips))
	for _, ip := range ips {
		values = append(values, ip.String())
	}
	return values
}
