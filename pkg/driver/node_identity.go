package driver

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sort"
	"strings"
)

// CSI limits node_id to 256 bytes. A small versioned TLV envelope keeps the
// identifier comfortably below that limit for normal NQNs/IQNs while allowing
// future controllers to skip fields they do not understand. The textual prefix
// lets a new controller distinguish an old, plain node name without guessing.
const (
	nodeIdentityPrefix    = "sc1."
	nodeIdentityVersion   = byte(1)
	maxCSINodeIDBytes     = 256
	nodeIdentityFieldName = byte(1)
	nodeIdentityFieldNQN  = byte(2)
	nodeIdentityFieldIQN  = byte(3)
	nodeIdentityFieldIPv4 = byte(4)
	nodeIdentityFieldIPv6 = byte(5)
)

// NodeIdentity is the durable identity carried by NodeGetInfo and copied into
// per-volume dataset properties at publish time. Name is the Kubernetes node
// name; transport fields are deliberately protocol-specific so allowlists never
// accidentally use a display name as an initiator identity.
type NodeIdentity struct {
	Name     string
	NVMeNQN  string
	ISCSIIQN string
	IPs      []net.IP
	Legacy   bool
}

var (
	nodeReadIdentityFile = os.ReadFile
	nodeIdentityCommand  = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, name, args...).Output()
	}
	nodeInterfaceAddrs = net.InterfaceAddrs
)

func appendNodeIdentityField(dst []byte, fieldType byte, value []byte) ([]byte, error) {
	if len(value) == 0 {
		return dst, nil
	}
	if len(value) > 255 {
		return nil, fmt.Errorf("node identity field %d is too long", fieldType)
	}
	dst = append(dst, fieldType, byte(len(value)))
	dst = append(dst, value...)
	return dst, nil
}

// encodeNodeIdentity returns a stable base64url encoding. IPs are canonicalized
// and appended last, so excess secondary addresses can be dropped without ever
// dropping the Kubernetes name or a block-transport identity.
func encodeNodeIdentity(identity NodeIdentity) (string, error) {
	identity = trimNodeIdentityDescriptionFields(identity)
	if strings.TrimSpace(identity.Name) == "" {
		return "", fmt.Errorf("node name is required")
	}
	raw := []byte{nodeIdentityVersion}
	var err error
	if raw, err = appendNodeIdentityField(raw, nodeIdentityFieldName, []byte(identity.Name)); err != nil {
		return "", err
	}
	if raw, err = appendNodeIdentityField(raw, nodeIdentityFieldNQN, []byte(identity.NVMeNQN)); err != nil {
		return "", err
	}
	if raw, err = appendNodeIdentityField(raw, nodeIdentityFieldIQN, []byte(identity.ISCSIIQN)); err != nil {
		return "", err
	}
	if len(nodeIdentityPrefix)+base64.RawURLEncoding.EncodedLen(len(raw)) > maxCSINodeIDBytes {
		return "", fmt.Errorf("node name and transport identities exceed CSI's %d-byte node_id limit", maxCSINodeIDBytes)
	}

	for _, ip := range canonicalNodeIPs(identity.IPs) {
		fieldType := nodeIdentityFieldIPv6
		encoded := ip.To16()
		if ip4 := ip.To4(); ip4 != nil {
			fieldType = nodeIdentityFieldIPv4
			encoded = ip4
		}
		candidate, appendErr := appendNodeIdentityField(raw, fieldType, encoded)
		if appendErr != nil {
			return "", appendErr
		}
		if len(nodeIdentityPrefix)+base64.RawURLEncoding.EncodedLen(len(candidate)) > maxCSINodeIDBytes {
			break
		}
		raw = candidate
	}
	return nodeIdentityPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

// trimNodeIdentityDescriptionFields is the final lossless packing stage after
// secondary IPs and disabled protocol classes have been deprioritized. The
// current envelope intentionally carries no human-facing description fields;
// the only description-class bytes discovery can introduce are surrounding
// file/command formatting whitespace, which is safe to remove. Kubernetes node
// names, NQNs, and IQNs are exact authorization identities and are never
// truncated; if those mandatory values still exceed 256 bytes startup fails.
func trimNodeIdentityDescriptionFields(identity NodeIdentity) NodeIdentity {
	identity.Name = strings.TrimSpace(identity.Name)
	identity.NVMeNQN = strings.TrimSpace(identity.NVMeNQN)
	identity.ISCSIIQN = strings.TrimSpace(identity.ISCSIIQN)
	return identity
}

// parseNodeIdentity is intentionally tolerant in both upgrade directions. A
// value without our prefix is an older node_id and remains usable as its node
// name. Unknown TLV fields are skipped; malformed envelopes are rejected rather
// than partially interpreted as transport authorization.
func parseNodeIdentity(nodeID string) (NodeIdentity, error) {
	if nodeID == "" {
		return NodeIdentity{}, fmt.Errorf("node ID is empty")
	}
	if !strings.HasPrefix(nodeID, nodeIdentityPrefix) {
		return NodeIdentity{Name: nodeID, Legacy: true}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(nodeID, nodeIdentityPrefix))
	if err != nil || len(raw) == 0 {
		return NodeIdentity{}, fmt.Errorf("invalid encoded node ID")
	}
	if raw[0] != nodeIdentityVersion {
		return NodeIdentity{}, fmt.Errorf("unsupported node identity version %d", raw[0])
	}
	identity := NodeIdentity{}
	for offset := 1; offset < len(raw); {
		if len(raw)-offset < 2 {
			return NodeIdentity{}, fmt.Errorf("truncated node identity field header")
		}
		fieldType, length := raw[offset], int(raw[offset+1])
		offset += 2
		if length == 0 || len(raw)-offset < length {
			return NodeIdentity{}, fmt.Errorf("invalid node identity field length")
		}
		value := raw[offset : offset+length]
		offset += length
		switch fieldType {
		case nodeIdentityFieldName:
			identity.Name = string(value)
		case nodeIdentityFieldNQN:
			identity.NVMeNQN = string(value)
		case nodeIdentityFieldIQN:
			identity.ISCSIIQN = string(value)
		case nodeIdentityFieldIPv4:
			if len(value) != net.IPv4len {
				return NodeIdentity{}, fmt.Errorf("invalid IPv4 identity length")
			}
			identity.IPs = append(identity.IPs, net.IP(append([]byte(nil), value...)))
		case nodeIdentityFieldIPv6:
			if len(value) != net.IPv6len {
				return NodeIdentity{}, fmt.Errorf("invalid IPv6 identity length")
			}
			identity.IPs = append(identity.IPs, net.IP(append([]byte(nil), value...)))
		default:
			// Reserved for future identity types.
		}
	}
	if identity.Name == "" {
		return NodeIdentity{}, fmt.Errorf("encoded node ID has no node name")
	}
	identity.IPs = canonicalNodeIPs(identity.IPs)
	return identity, nil
}

// nodeIdentityForEnabledProtocols drops identity classes this deployment
// cannot consume before enforcing the CSI size limit. Within enabled classes,
// encodeNodeIdentity packs mandatory name/transport identifiers first, then as
// many canonical IPs as fit, and finally strips only lossless description
// formatting before giving up.
func nodeIdentityForEnabledProtocols(identity NodeIdentity, config *Config) NodeIdentity {
	if config == nil {
		return identity
	}
	if !config.NFS.Enabled {
		identity.IPs = nil
	}
	if !config.ISCSI.Enabled {
		identity.ISCSIIQN = ""
	}
	if !config.NVMeoF.Enabled {
		identity.NVMeNQN = ""
	}
	return identity
}

func canonicalNodeIPs(ips []net.IP) []net.IP {
	byString := make(map[string]net.IP, len(ips))
	for _, candidate := range ips {
		ip := candidate
		if ip == nil || ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast() || ip.IsLinkLocalUnicast() {
			continue
		}
		if ip4 := ip.To4(); ip4 != nil {
			ip = ip4
		} else if ip16 := ip.To16(); ip16 != nil {
			ip = ip16
		} else {
			continue
		}
		byString[ip.String()] = append(net.IP(nil), ip...)
	}
	keys := make([]string, 0, len(byString))
	for key := range byString {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]net.IP, 0, len(keys))
	for _, key := range keys {
		result = append(result, byString[key])
	}
	return result
}

func discoverNodeIdentity(ctx context.Context, nodeName string) NodeIdentity {
	identity := NodeIdentity{Name: nodeName}
	if output, err := nodeIdentityCommand(ctx, "nvme", "show-hostnqn"); err == nil {
		identity.NVMeNQN = strings.TrimSpace(string(output))
	}
	if identity.NVMeNQN == "" {
		if contents, err := nodeReadIdentityFile("/etc/nvme/hostnqn"); err == nil {
			identity.NVMeNQN = strings.TrimSpace(string(contents))
		}
	}
	if contents, err := nodeReadIdentityFile("/etc/iscsi/initiatorname.iscsi"); err == nil {
		for _, line := range strings.Split(string(contents), "\n") {
			key, value, found := strings.Cut(strings.TrimSpace(line), "=")
			if found && strings.EqualFold(strings.TrimSpace(key), "InitiatorName") {
				identity.ISCSIIQN = strings.TrimSpace(value)
				break
			}
		}
	}
	for _, envName := range []string{"NODE_IP", "NODE_IPS"} {
		for _, value := range strings.FieldsFunc(os.Getenv(envName), func(r rune) bool { return r == ',' || r == ' ' }) {
			if ip := net.ParseIP(value); ip != nil {
				identity.IPs = append(identity.IPs, ip)
			}
		}
	}
	// The chart injects status.hostIP. Prefer that stable control-plane identity
	// over enumerating host-network/CNI interfaces, whose addresses and lexical
	// order can change across restarts and would make CSI's node_id unstable.
	if len(identity.IPs) == 0 {
		if addresses, err := nodeInterfaceAddrs(); err == nil {
			for _, address := range addresses {
				value := address.String()
				if host, _, splitErr := net.SplitHostPort(value); splitErr == nil {
					value = host
				} else if slash := strings.IndexByte(value, '/'); slash >= 0 {
					value = value[:slash]
				}
				if ip := net.ParseIP(value); ip != nil {
					identity.IPs = append(identity.IPs, ip)
				}
			}
		}
	}
	identity.IPs = canonicalNodeIPs(identity.IPs)
	return identity
}
