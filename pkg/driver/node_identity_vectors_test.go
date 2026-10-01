package driver

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The node id is a contract with the Rust node agent (rust/scale-csi-node): a
// node that moves between the two must keep a byte-identical id, or kubelet
// re-registers it and the controller sees a different fencing identity.
// testdata/node_identity_vectors.json holds this encoder's and parser's output
// for a fixed set of inputs; the Rust tests read the same file. This test fails
// when the Go code changes without regenerating it:
//
//	UPDATE_NODE_ID_VECTORS=1 go test ./pkg/driver -run TestNodeIdentityVectors

const nodeIdentityVectorsFile = "testdata/node_identity_vectors.json"

type nodeIdentityVectorInput struct {
	Name                  string   `json:"name"`
	NVMeNQN               string   `json:"nvme_nqn,omitempty"`
	ISCSIIQN              string   `json:"iscsi_iqn,omitempty"`
	IPs                   []string `json:"ips,omitempty"`
	ISCSIReportedSentinel bool     `json:"iscsi_reported_sentinel,omitempty"`
	// Protocols, when set, applies nodeIdentityForEnabledProtocols first.
	Protocols *nodeIdentityVectorProtocols `json:"protocols,omitempty"`
}

type nodeIdentityVectorProtocols struct {
	NFS    bool `json:"nfs"`
	ISCSI  bool `json:"iscsi"`
	NVMeoF bool `json:"nvmeof"`
}

type nodeIdentityVectorIdentity struct {
	Name                  string   `json:"name"`
	NVMeNQN               string   `json:"nvme_nqn"`
	ISCSIIQN              string   `json:"iscsi_iqn"`
	IPs                   []string `json:"ips"`
	Legacy                bool     `json:"legacy"`
	ISCSIReportedSentinel bool     `json:"iscsi_reported_sentinel"`
}

type nodeIdentityEncodeVector struct {
	Case   string                  `json:"case"`
	Input  nodeIdentityVectorInput `json:"input"`
	NodeID string                  `json:"node_id,omitempty"`
	Error  bool                    `json:"error,omitempty"`
}

type nodeIdentityParseVector struct {
	Case     string                      `json:"case"`
	NodeID   string                      `json:"node_id"`
	Identity *nodeIdentityVectorIdentity `json:"identity,omitempty"`
	Error    bool                        `json:"error,omitempty"`
}

type nodeIdentityVectors struct {
	Encode []nodeIdentityEncodeVector `json:"encode"`
	Parse  []nodeIdentityParseVector  `json:"parse"`
}

func nodeIdentityVectorEncodeCases() []struct {
	name  string
	input nodeIdentityVectorInput
} {
	nqn := "nqn.2014-08.org.nvmexpress:uuid:0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718"
	iqn := "iqn.2004-10.com.ubuntu:01:5f1d3a9c2b7e"
	all := &nodeIdentityVectorProtocols{NFS: true, ISCSI: true, NVMeoF: true}
	manyIPs := make([]string, 0, 40)
	for i := 1; i <= 40; i++ {
		manyIPs = append(manyIPs, net.IPv4(10, 1, byte(i/10), byte(i)).String())
	}
	return []struct {
		name  string
		input nodeIdentityVectorInput
	}{
		{"name only", nodeIdentityVectorInput{Name: "k8s-0"}},
		{"name and nqn", nodeIdentityVectorInput{Name: "k8s-0", NVMeNQN: nqn}},
		{"name, nqn and iqn", nodeIdentityVectorInput{Name: "k8s-1", NVMeNQN: nqn, ISCSIIQN: iqn}},
		{"typical shape: nqn and host ip", nodeIdentityVectorInput{Name: "k8s-1", NVMeNQN: nqn, IPs: []string{"198.51.100.11"}}},
		{"surrounding whitespace is trimmed", nodeIdentityVectorInput{Name: " k8s-0\n", NVMeNQN: "\t" + nqn + "\n", ISCSIIQN: " " + iqn + " "}},
		{"unicode whitespace is trimmed", nodeIdentityVectorInput{Name: " k8s-0 ", NVMeNQN: nqn + "\u0085"}},
		{"ipv4 canonical order, duplicates and non-unicast dropped", nodeIdentityVectorInput{Name: "n", IPs: []string{
			"198.51.100.11", "10.0.0.2", "10.0.0.2", "127.0.0.1", "0.0.0.0", "169.254.1.1", "224.0.0.1", "9.9.9.9", "100.64.0.1",
		}}},
		{"ipv6 and mapped ipv4", nodeIdentityVectorInput{Name: "n", IPs: []string{
			"2001:db8::1", "fe80::1", "::1", "::", "ff02::1", "::ffff:192.0.2.7", "2001:db8:0:0:1:0:0:1", "fd00::a", "2001:DB8::2",
		}}},
		{"mixed families sort by their text", nodeIdentityVectorInput{Name: "n", IPs: []string{"2001:db8::9", "192.0.2.1", "10.9.9.9", "fd12:3456::1", "1.2.3.4"}}},
		{"ipv4-compatible ipv6", nodeIdentityVectorInput{Name: "n", IPs: []string{"::102:304", "::1.2.3.5"}}},
		{"sentinel flag", nodeIdentityVectorInput{Name: "k8s-2", NVMeNQN: nqn, ISCSIReportedSentinel: true, IPs: []string{"192.0.2.5"}}},
		{"ips beyond the 256-byte limit are dropped", nodeIdentityVectorInput{Name: "k8s-0", NVMeNQN: nqn, ISCSIIQN: iqn, IPs: manyIPs}},
		{"mandatory fields over the limit", nodeIdentityVectorInput{Name: strings.Repeat("n", 100), NVMeNQN: strings.Repeat("q", 100)}},
		{"a field over 255 bytes", nodeIdentityVectorInput{Name: strings.Repeat("n", 256)}},
		{"empty name", nodeIdentityVectorInput{Name: " ", NVMeNQN: nqn}},
		{"the deny-all sentinel as the iqn", nodeIdentityVectorInput{Name: "k8s-0", ISCSIIQN: " " + iscsiDenyAllSentinelIQN + " "}},
		{"all protocols enabled", nodeIdentityVectorInput{Name: "k8s-0", NVMeNQN: nqn, ISCSIIQN: iqn, IPs: []string{"192.0.2.1"}, Protocols: all}},
		{"nvmeof only prunes ips, iqn and sentinel", nodeIdentityVectorInput{Name: "k8s-0", NVMeNQN: nqn, ISCSIIQN: iqn, ISCSIReportedSentinel: true, IPs: []string{"192.0.2.1"}, Protocols: &nodeIdentityVectorProtocols{NVMeoF: true}}},
		{"nfs only keeps ips", nodeIdentityVectorInput{Name: "k8s-0", NVMeNQN: nqn, ISCSIIQN: iqn, IPs: []string{"192.0.2.1"}, Protocols: &nodeIdentityVectorProtocols{NFS: true}}},
		{"iscsi only keeps iqn and sentinel", nodeIdentityVectorInput{Name: "k8s-0", NVMeNQN: nqn, ISCSIReportedSentinel: true, IPs: []string{"192.0.2.1"}, Protocols: &nodeIdentityVectorProtocols{ISCSI: true}}},
	}
}

func nodeIdentityVectorParseCases(t *testing.T) []struct{ name, nodeID string } {
	t.Helper()
	enc := func(raw ...byte) string { return nodeIdentityPrefix + base64.RawURLEncoding.EncodeToString(raw) }
	tlv := func(fieldType byte, value string) []byte {
		return append([]byte{fieldType, byte(len(value))}, value...)
	}
	cat := func(parts ...[]byte) []byte {
		out := []byte{nodeIdentityVersion}
		for _, part := range parts {
			out = append(out, part...)
		}
		return out
	}
	typical, err := encodeNodeIdentity(NodeIdentity{Name: "k8s-1", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:9a8b7c6d-5e4f-4a3b-9c2d-1e0f2a3b4c5d", IPs: []net.IP{net.ParseIP("198.51.100.11")}})
	require.NoError(t, err)
	return []struct{ name, nodeID string }{
		{"legacy plain name", "k8s-0"},
		{"typical id", typical},
		{"unknown field types are skipped", enc(cat(tlv(1, "n"), tlv(9, "future"), tlv(2, "nqn.x"))...)},
		{"ips are canonicalised on parse", enc(cat(tlv(1, "n"), tlv(4, "\xc0\x00\x02\x09"), tlv(4, "\x0a\x00\x00\x01"), tlv(4, "\x7f\x00\x00\x01"), tlv(4, "\x0a\x00\x00\x01"))...)},
		{"mapped ipv4 in an ipv6 field", enc(cat(tlv(1, "n"), tlv(5, "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xc0\x00\x02\x07"))...)},
		{"sentinel flag", enc(cat(tlv(1, "n"), tlv(6, "\x01"))...)},
		{"a later name field wins", enc(cat(tlv(1, "a"), tlv(1, "b"))...)},
		{"non-zero trailing bits", nodeIdentityPrefix + "AQEBbh"},
		{"embedded line breaks", nodeIdentityPrefix + "AQEB\r\nbg"},
		{"prefix only", nodeIdentityPrefix},
		{"not base64url", nodeIdentityPrefix + "!!!"},
		{"padded base64", nodeIdentityPrefix + base64.URLEncoding.EncodeToString(cat(tlv(1, "ab")))},
		{"wrong version", enc(2, 1, 1, 'n')},
		{"truncated header", enc(1, 1)},
		{"zero-length field", enc(1, 1, 0)},
		{"length past the end", enc(1, 1, 5, 'n')},
		{"no name", enc(cat(tlv(2, "nqn.x"))...)},
		{"short ipv4", enc(cat(tlv(1, "n"), tlv(4, "\x01\x02\x03"))...)},
		{"short ipv6", enc(cat(tlv(1, "n"), tlv(5, "\x01\x02\x03\x04"))...)},
		{"empty", ""},
	}
}

func vectorIdentity(identity NodeIdentity) *nodeIdentityVectorIdentity {
	out := &nodeIdentityVectorIdentity{
		Name: identity.Name, NVMeNQN: identity.NVMeNQN, ISCSIIQN: identity.ISCSIIQN,
		IPs: []string{}, Legacy: identity.Legacy, ISCSIReportedSentinel: identity.ISCSIReportedSentinel,
	}
	for _, ip := range identity.IPs {
		out.IPs = append(out.IPs, ip.String())
	}
	return out
}

func TestNodeIdentityVectors(t *testing.T) {
	var got nodeIdentityVectors
	for _, c := range nodeIdentityVectorEncodeCases() {
		identity := NodeIdentity{Name: c.input.Name, NVMeNQN: c.input.NVMeNQN, ISCSIIQN: c.input.ISCSIIQN, ISCSIReportedSentinel: c.input.ISCSIReportedSentinel}
		for _, value := range c.input.IPs {
			ip := net.ParseIP(value)
			require.NotNil(t, ip, "case %q: bad ip %q", c.name, value)
			identity.IPs = append(identity.IPs, ip)
		}
		if p := c.input.Protocols; p != nil {
			identity = nodeIdentityForEnabledProtocols(identity, &Config{NFS: NFSConfig{Enabled: p.NFS}, ISCSI: ISCSIConfig{Enabled: p.ISCSI}, NVMeoF: NVMeoFConfig{Enabled: p.NVMeoF}})
		}
		nodeID, err := encodeNodeIdentity(identity)
		got.Encode = append(got.Encode, nodeIdentityEncodeVector{Case: c.name, Input: c.input, NodeID: nodeID, Error: err != nil})
	}
	for _, c := range nodeIdentityVectorParseCases(t) {
		identity, err := parseNodeIdentity(c.nodeID)
		var parsed *nodeIdentityVectorIdentity
		if err == nil {
			parsed = vectorIdentity(identity)
		}
		got.Parse = append(got.Parse, nodeIdentityParseVector{Case: c.name, NodeID: c.nodeID, Identity: parsed, Error: err != nil})
	}
	encoded, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)
	encoded = append(encoded, '\n')

	if os.Getenv("UPDATE_NODE_ID_VECTORS") != "" {
		require.NoError(t, os.WriteFile(nodeIdentityVectorsFile, encoded, 0o600))
	}
	want, err := os.ReadFile(nodeIdentityVectorsFile)
	require.NoError(t, err, "generate it with UPDATE_NODE_ID_VECTORS=1")
	require.Equal(t, string(want), string(encoded),
		"the Go node id encoding changed; regenerate %s with UPDATE_NODE_ID_VECTORS=1 and make the Rust agent match", nodeIdentityVectorsFile)
}
