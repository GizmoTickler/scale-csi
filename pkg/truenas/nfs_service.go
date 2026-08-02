package truenas

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// NFS major-version protocol tokens as `nfs.config` spells them.
const (
	NFSProtocolV3 = "NFSV3"
	NFSProtocolV4 = "NFSV4"
)

// NFSServiceConfig is the subset of the GLOBAL `nfs.config` service object the
// driver needs. NFS version selection is a server-wide setting: there is no
// per-share `vers`/`protocol` field on `sharing.nfs.*`. A client picks v3 vs v4
// with the node-side `vers=` mount option, and the server must list that MAJOR
// version here for the mount to succeed. v4.1 is part of the server's NFSV4
// support and needs no extra server flag.
type NFSServiceConfig struct {
	// Protocols is the enabled major-version set, e.g. ["NFSV3","NFSV4"].
	// UNKNOWN string tokens are preserved (trimmed/uppercased) rather than
	// filtered out: nfs.update REPLACES this list, so dropping a token the driver
	// does not recognize — a future NFSV5, say — would silently disable it
	// appliance-wide on the next managed write.
	Protocols []string
	// ProtocolsComplete reports that the whole protocols list was read without
	// anomaly, i.e. that Protocols is a faithful and COMPLETE picture of what the
	// appliance currently enables. Any managed write that REPLACES the list must
	// refuse unless this is true.
	ProtocolsComplete bool
	// ProtocolsAnomaly describes why the list could not be read completely: the
	// field is absent, is not a list, or contains ANY unusable item. Empty when
	// the field parsed cleanly, including for a cleanly-empty list.
	ProtocolsAnomaly string
	// V4Krb / V4KrbEnabled report whether Kerberos is configured. KRB5* share
	// security is unusable without them, so the driver fails closed.
	V4Krb        bool
	V4KrbEnabled bool
	// RDMA reports the global NFS-over-RDMA switch (clients then mount with
	// proto=rdma). Read-only here; the driver never flips it.
	RDMA bool
	// Servers is the configured nfsd thread count (diagnostics only).
	Servers int
}

// SupportsMajorVersion reports whether the server has the given major-version
// token (NFSV3/NFSV4) enabled. An empty/unknown Protocols list is treated as
// "cannot prove unsupported" and returns true so a preflight never fails closed
// on a backend that did not report the field.
func (c *NFSServiceConfig) SupportsMajorVersion(protocol string) bool {
	if c == nil || len(c.Protocols) == 0 {
		return true
	}
	want := strings.ToUpper(strings.TrimSpace(protocol))
	for _, p := range c.Protocols {
		if strings.ToUpper(strings.TrimSpace(p)) == want {
			return true
		}
	}
	return false
}

// NFSServiceConfig reads the global NFS service configuration.
func (c *Client) NFSServiceConfig(ctx context.Context) (*NFSServiceConfig, error) {
	result, err := c.Call(ctx, "nfs.config")
	if err != nil {
		return nil, fmt.Errorf("failed to read NFS service config: %w", err)
	}
	return parseNFSServiceConfig(result)
}

// NFSServiceUpdate mutates the GLOBAL NFS service configuration. It is only
// reachable from the hard-gated, default-off `nfs.ensureProtocols` path: a
// service-wide write has blast radius across every export on the box, driver
// managed or not.
func (c *Client) NFSServiceUpdate(ctx context.Context, params map[string]interface{}) (*NFSServiceConfig, error) {
	result, err := c.Call(ctx, "nfs.update", params)
	if err != nil {
		return nil, fmt.Errorf("failed to update NFS service config: %w", err)
	}
	return parseNFSServiceConfig(result)
}

// parseNFSProtocolList reads nfs.config's `protocols` field ALL-OR-NOTHING
// (M3). It returns the normalized tokens plus a non-empty anomaly string when
// the list could not be read in full.
//
// Partial parsing is the dangerous case and the one this exists for: nfs.update
// {protocols: X} SETS the list, it does not union with it. If the response ever
// carries a reshaped item (say ["NFSV4", {"name":"NFSV5"}]) and the parser
// silently kept only the readable half, a managed write would compute its merge
// against an INCOMPLETE base and remove the item it could not read — disabling a
// live protocol for every export on the appliance, driver-managed or not. A
// half-read list is therefore reported as no basis for a write at all, exactly
// like a missing or wrong-typed one.
func parseNFSProtocolList(m map[string]interface{}) (protocols []string, anomaly string) {
	raw, present := m["protocols"]
	if !present {
		return nil, `nfs.config returned no "protocols" field`
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Sprintf(`nfs.config "protocols" is %T, not a list`, raw)
	}
	for index, item := range items {
		token, ok := item.(string)
		if !ok {
			return nil, fmt.Sprintf(`nfs.config "protocols" entry %d is %T, not a string`, index, item)
		}
		if strings.TrimSpace(token) == "" {
			return nil, fmt.Sprintf(`nfs.config "protocols" entry %d is an empty string`, index)
		}
		protocols = append(protocols, strings.ToUpper(strings.TrimSpace(token)))
	}
	sort.Strings(protocols)
	return protocols, ""
}

func parseNFSServiceConfig(data interface{}) (*NFSServiceConfig, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected nfs.config response format %T", data)
	}
	cfg := &NFSServiceConfig{}
	cfg.Protocols, cfg.ProtocolsAnomaly = parseNFSProtocolList(m)
	cfg.ProtocolsComplete = cfg.ProtocolsAnomaly == ""
	if v, ok := m["v4_krb"].(bool); ok {
		cfg.V4Krb = v
	}
	if v, ok := m["v4_krb_enabled"].(bool); ok {
		cfg.V4KrbEnabled = v
	}
	if v, ok := m["rdma"].(bool); ok {
		cfg.RDMA = v
	}
	if v, ok := m["servers"].(float64); ok {
		cfg.Servers = int(v)
	}
	return cfg, nil
}
