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
	Protocols []string
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

func parseNFSServiceConfig(data interface{}) (*NFSServiceConfig, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected nfs.config response format %T", data)
	}
	cfg := &NFSServiceConfig{}
	if v, ok := m["protocols"].([]interface{}); ok {
		for _, item := range v {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				cfg.Protocols = append(cfg.Protocols, strings.ToUpper(strings.TrimSpace(s)))
			}
		}
		sort.Strings(cfg.Protocols)
	}
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
