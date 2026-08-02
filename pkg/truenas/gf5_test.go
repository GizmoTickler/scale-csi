package truenas

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gf5TestClient spins the shared websocket mock with a per-method result map and
// returns a connected client. Methods not in the map produce a JSON-RPC error.
func gf5TestClient(t *testing.T, results map[string]func(req rpcTestRequest) (interface{}, *rpcError)) *Client {
	t.Helper()
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			var resp rpcTestResponse
			resp.JSONRPC = "2.0"
			resp.ID = req.ID
			if req.Method == "auth.login_with_api_key" {
				resp.Result = true
			} else if handler, ok := results[req.Method]; ok {
				result, rpcErr := handler(req)
				resp.Result, resp.Error = result, rpcErr
			} else {
				resp.Error = &rpcError{Code: -32601, Message: "Method not found: " + req.Method}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	t.Cleanup(mock.close)

	wsURL := strings.Replace(server.URL, "http://", "", 1)
	parts := strings.Split(wsURL, ":")
	host := parts[0]
	port := 80
	if len(parts) > 1 {
		_, _ = fmt.Sscanf(parts[1], "%d", &port)
	}
	client, err := NewClient(&ClientConfig{
		Host:           host,
		Port:           port,
		Protocol:       "http",
		APIKey:         "test-api-key",
		Timeout:        5 * time.Second,
		ConnectTimeout: 5 * time.Second,
		MaxConnections: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func static(result interface{}) func(rpcTestRequest) (interface{}, *rpcError) {
	return func(rpcTestRequest) (interface{}, *rpcError) { return result, nil }
}

// TestNFSShareCreateParamsOmitsGF5FieldsWhenUnset is the wire-level byte-identity
// guard: the two GF5 fields must not appear at all in a default payload.
func TestNFSShareCreateParamsOmitsGF5FieldsWhenUnset(t *testing.T) {
	encoded, err := json.Marshal(&NFSShareCreateParams{Path: "/mnt/tank/vol", Enabled: true})
	require.NoError(t, err)
	assert.JSONEq(t, `{"path":"/mnt/tank/vol","enabled":true}`, string(encoded))
}

func TestNFSServiceConfig(t *testing.T) {
	client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
		"nfs.config": static(map[string]interface{}{
			"protocols":      []interface{}{"NFSV4", "NFSV3"},
			"v4_krb":         false,
			"v4_krb_enabled": false,
			"rdma":           false,
			"servers":        float64(64),
		}),
	})

	cfg, err := client.NFSServiceConfig(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"NFSV3", "NFSV4"}, cfg.Protocols, "protocols are normalized and sorted")
	assert.Equal(t, 64, cfg.Servers)
	assert.False(t, cfg.RDMA)
	assert.True(t, cfg.SupportsMajorVersion("NFSV4"))
	assert.True(t, cfg.SupportsMajorVersion("nfsv3"))
	assert.False(t, cfg.SupportsMajorVersion("NFSV2"))
}

func TestNFSServiceConfigSupportsMajorVersionFailsOpen(t *testing.T) {
	// A nil or empty protocol list cannot PROVE a version unsupported, so the
	// preflight must not fail closed on it.
	var cfg *NFSServiceConfig
	assert.True(t, cfg.SupportsMajorVersion("NFSV4"))
	assert.True(t, (&NFSServiceConfig{}).SupportsMajorVersion("NFSV4"))
}

func TestNFSServiceUpdate(t *testing.T) {
	var seen []interface{}
	client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
		"nfs.update": func(req rpcTestRequest) (interface{}, *rpcError) {
			seen = req.Params
			return map[string]interface{}{"protocols": []interface{}{"NFSV3", "NFSV4"}}, nil
		},
	})

	cfg, err := client.NFSServiceUpdate(context.Background(), map[string]interface{}{"protocols": []string{"NFSV3", "NFSV4"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"NFSV3", "NFSV4"}, cfg.Protocols)
	require.Len(t, seen, 1)
}

func TestACLTemplateDACL(t *testing.T) {
	t.Run("resolves an NFS4 template", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			"filesystem.acltemplate.query": static([]interface{}{map[string]interface{}{
				"name":    "NFS4_RESTRICTED",
				"acltype": "NFS4",
				"acl": []interface{}{
					map[string]interface{}{"tag": "owner@", "type": "ALLOW", "perms": map[string]interface{}{"BASIC": "FULL_CONTROL"}},
					map[string]interface{}{"tag": "group@", "type": "ALLOW", "perms": map[string]interface{}{"BASIC": "MODIFY"}},
				},
			}}),
		})
		dacl, err := client.ACLTemplateDACL(context.Background(), "NFS4_RESTRICTED")
		require.NoError(t, err)
		require.Len(t, dacl, 2)
		assert.Equal(t, "owner@", dacl[0].Tag)
		assert.Equal(t, "FULL_CONTROL", dacl[0].Perms["BASIC"])
	})

	t.Run("rejects a POSIX1E template", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			"filesystem.acltemplate.query": static([]interface{}{map[string]interface{}{
				"name": "POSIX_OPEN", "acltype": "POSIX1E", "acl": []interface{}{},
			}}),
		})
		_, err := client.ACLTemplateDACL(context.Background(), "POSIX_OPEN")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "POSIX1E")
	})

	t.Run("missing template is an error", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			"filesystem.acltemplate.query": static([]interface{}{}),
		})
		_, err := client.ACLTemplateDACL(context.Background(), "NOPE")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

// TestFilesystemSetACLJobSemantics proves setacl is dispatched as a @job: the
// call returns a job id and the client awaits its terminal state.
func TestFilesystemSetACLJobSemantics(t *testing.T) {
	t.Run("successful job", func(t *testing.T) {
		var setaclArgs []interface{}
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			SetACLMethod: func(req rpcTestRequest) (interface{}, *rpcError) {
				setaclArgs = req.Params
				return float64(7001), nil
			},
			"core.get_jobs": static([]interface{}{map[string]interface{}{
				"id": float64(7001), "state": "SUCCESS",
			}}),
			"core.subscribe": static("sub-1"),
		})

		err := client.FilesystemSetACL(context.Background(), &SetACLOptions{
			Path:       "/mnt/tank/k8s/vol",
			DACL:       []ACLEntry{{Tag: "owner@", Type: "ALLOW", Perms: map[string]interface{}{"BASIC": "FULL_CONTROL"}}},
			NFS41Flags: map[string]bool{"protected": true},
		})
		require.NoError(t, err)
		require.Len(t, setaclArgs, 1)
		args, ok := setaclArgs[0].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "/mnt/tank/k8s/vol", args["path"])
		assert.NotNil(t, args["dacl"])
		flags, ok := args["nfs41_flags"].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, true, flags["protected"])
	})

	t.Run("failed job surfaces as an error", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			SetACLMethod: static(float64(7002)),
			"core.get_jobs": static([]interface{}{map[string]interface{}{
				"id": float64(7002), "state": "FAILED", "error": "acltype is not NFSV4",
			}}),
			"core.subscribe": static("sub-1"),
		})

		err := client.FilesystemSetACL(context.Background(), &SetACLOptions{
			Path: "/mnt/tank/k8s/vol",
			DACL: []ACLEntry{{Tag: "owner@"}},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "job 7002")
	})

	t.Run("argument validation happens before dispatch", func(t *testing.T) {
		client := gf5TestClient(t, nil)
		require.Error(t, client.FilesystemSetACL(context.Background(), nil))
		require.Error(t, client.FilesystemSetACL(context.Background(), &SetACLOptions{Path: ""}))
		require.Error(t, client.FilesystemSetACL(context.Background(), &SetACLOptions{Path: "/mnt/x"}))
	})
}

func TestFilesystemGetACL(t *testing.T) {
	client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
		"filesystem.getacl": static(map[string]interface{}{
			"path":    "/mnt/tank/k8s/vol",
			"acltype": "NFS4",
			"trivial": false,
			"acl": []interface{}{
				map[string]interface{}{"tag": "owner@", "type": "ALLOW", "id": float64(-1)},
				map[string]interface{}{"tag": "USER", "type": "ALLOW", "id": float64(3000)},
				map[string]interface{}{"no-tag": true},
			},
			"nfs41_flags": map[string]interface{}{"protected": true, "autoinherit": false},
		}),
	})

	acl, err := client.FilesystemGetACL(context.Background(), "/mnt/tank/k8s/vol")
	require.NoError(t, err)
	assert.Equal(t, "NFS4", acl.ACLType)
	assert.False(t, acl.Trivial)
	require.Len(t, acl.ACL, 2, "entries without a tag are dropped")
	assert.Nil(t, acl.ACL[0].ID, "the -1 sentinel is not a real uid")
	require.NotNil(t, acl.ACL[1].ID)
	assert.Equal(t, 3000, *acl.ACL[1].ID)
	assert.True(t, acl.NFS41Flags["protected"])
}

func TestZFSPropertyChoices(t *testing.T) {
	client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
		// Array shape.
		"pool.dataset.recordsize_choices": static([]interface{}{"4K", "128K", "1M"}),
		// Object shape (keys are the accepted values) — TrueNAS uses both.
		"pool.dataset.compression_choices": static(map[string]interface{}{"LZ4": "lz4", "ZSTD": "zstd"}),
		"pool.dataset.checksum_choices":    static([]interface{}{"BLAKE3", "SHA256", ""}),
	})

	choices, err := client.ZFSPropertyChoices(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"4K", "128K", "1M"}, choices.Recordsize)
	assert.Len(t, choices.Compression, 2)
	assert.Equal(t, []string{"BLAKE3", "SHA256"}, choices.Checksum, "empty entries are dropped")

	allowed, known := choices.AllowsRecordsize("1m")
	assert.True(t, allowed)
	assert.True(t, known)
	allowed, known = choices.AllowsRecordsize("64K")
	assert.False(t, allowed)
	assert.True(t, known)

	// An unreported list must never be read as "unsupported".
	empty := &ZFSPropertyChoices{}
	allowed, known = empty.AllowsChecksum("ANYTHING")
	assert.True(t, allowed)
	assert.False(t, known)
}

func TestRecommendedZvolBlocksize(t *testing.T) {
	client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
		"pool.dataset.recommended_zvol_blocksize": static("16K"),
	})
	size, err := client.RecommendedZvolBlocksize(context.Background(), "flashstor")
	require.NoError(t, err)
	assert.Equal(t, "16K", size)
}

func TestPoolHasSpecialVdev(t *testing.T) {
	t.Run("special vdev present", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			"pool.query": static([]interface{}{map[string]interface{}{
				"name": "flashstor",
				"topology": map[string]interface{}{
					"data":    []interface{}{map[string]interface{}{"type": "RAIDZ1"}},
					"special": []interface{}{map[string]interface{}{"type": "MIRROR"}},
				},
			}}),
		})
		present, err := client.PoolHasSpecialVdev(context.Background(), "flashstor")
		require.NoError(t, err)
		assert.True(t, present)
	})

	t.Run("no special vdev", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			"pool.query": static([]interface{}{map[string]interface{}{
				"name":     "tank",
				"topology": map[string]interface{}{"data": []interface{}{}, "special": []interface{}{}},
			}}),
		})
		present, err := client.PoolHasSpecialVdev(context.Background(), "tank")
		require.NoError(t, err)
		assert.False(t, present)
	})

	t.Run("missing pool is an error", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			"pool.query": static([]interface{}{}),
		})
		_, err := client.PoolHasSpecialVdev(context.Background(), "nope")
		require.Error(t, err)
	})
}

func TestPoolHealth(t *testing.T) {
	client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
		"pool.query": static([]interface{}{map[string]interface{}{
			"name":          "flashstor",
			"status":        "degraded",
			"healthy":       false,
			"warning":       true,
			"status_detail": "One or more devices are faulted.",
			"scan": map[string]interface{}{
				"function":   "resilver",
				"state":      "scanning",
				"percentage": 42.5,
				"errors":     float64(3),
			},
			"topology": map[string]interface{}{
				"data": []interface{}{map[string]interface{}{
					"type": "RAIDZ1",
					"children": []interface{}{
						map[string]interface{}{"disk": "nvme1n1"},
						map[string]interface{}{"disk": "nvme0n1"},
						map[string]interface{}{"disk": "nvme0n1"},
					},
				}},
				"special": []interface{}{map[string]interface{}{"disk": "nvme9n1"}},
				"cache":   []interface{}{},
			},
		}}),
	})

	snapshot, err := client.PoolHealth(context.Background(), "flashstor")
	require.NoError(t, err)
	assert.Equal(t, PoolStatusDegraded, snapshot.Status, "status is normalized to upper case")
	assert.False(t, snapshot.Healthy)
	assert.True(t, snapshot.Warning)
	assert.Equal(t, PoolScanFunctionResilver, snapshot.ScanFunction)
	assert.Equal(t, PoolScanStateScanning, snapshot.ScanState)
	assert.InDelta(t, 42.5, snapshot.ScanPercentage, 0.001)
	assert.Equal(t, int64(3), snapshot.ScanErrors)
	assert.Equal(t, []string{"nvme0n1", "nvme1n1", "nvme9n1"}, snapshot.Disks,
		"member disks are deduplicated, sorted, and collected across every topology class")
	assert.True(t, snapshot.Degraded())
	assert.True(t, snapshot.Scanning())
	assert.False(t, snapshot.SampledAt.IsZero())
}

// TestPoolHealthRejectsUnexpectedMultipleFilteredResults proves the decoder
// fails closed when the filtered middleware response has a valid first item
// followed by an extra item. The response shape must be exactly one pool.
func TestPoolHealthRejectsUnexpectedMultipleFilteredResults(t *testing.T) {
	_, err := poolHealthFromQueryResult("flashstor", []interface{}{
		map[string]interface{}{"name": "flashstor", "status": "ONLINE", "healthy": true},
		map[string]interface{}{"name": "wrong-pool", "status": "ONLINE", "healthy": true},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected exactly one")
}

func TestPoolHealthSeverityHelpers(t *testing.T) {
	var nilSnapshot *PoolHealthSnapshot
	assert.False(t, nilSnapshot.Degraded())
	assert.False(t, nilSnapshot.Scanning())

	for _, status := range []string{PoolStatusDegraded, PoolStatusFaulted, PoolStatusUnavail} {
		assert.True(t, (&PoolHealthSnapshot{Status: status}).Degraded(), status)
	}
	// OFFLINE/REMOVED are not data-path risks.
	for _, status := range []string{PoolStatusOnline, PoolStatusOffline, PoolStatusRemoved, ""} {
		assert.False(t, (&PoolHealthSnapshot{Status: status}).Degraded(), status)
	}
}

func TestPoolHealthMissingPool(t *testing.T) {
	client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
		"pool.query": static([]interface{}{}),
	})
	_, err := client.PoolHealth(context.Background(), "nope")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDiskTemperatureAlerts(t *testing.T) {
	t.Run("empty names short circuits without an API call", func(t *testing.T) {
		client := gf5TestClient(t, nil)
		alerts, err := client.DiskTemperatureAlerts(context.Background(), nil)
		require.NoError(t, err)
		assert.Empty(t, alerts)
	})

	t.Run("healthy backend returns no alerts", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			"disk.temperature_alerts": static([]interface{}{}),
		})
		alerts, err := client.DiskTemperatureAlerts(context.Background(), []string{"nvme0n1"})
		require.NoError(t, err)
		assert.Empty(t, alerts)
	})

	t.Run("both string and object alert shapes are captured", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			"disk.temperature_alerts": static([]interface{}{
				"nvme0n1 is too hot",
				map[string]interface{}{"device": "nvme1n1"},
				map[string]interface{}{"formatted": "nvme2n1 at 80C"},
				map[string]interface{}{"unexpected": "shape"},
			}),
		})
		alerts, err := client.DiskTemperatureAlerts(context.Background(), []string{"nvme0n1", "nvme1n1", "nvme2n1"})
		require.NoError(t, err)
		assert.Equal(t, []string{"nvme0n1 is too hot", "nvme1n1", "nvme2n1 at 80C"}, alerts)
	})

	// L3: the LENGTH of this slice is published as scale_csi_pool_disk_temp_alerts
	// and read as a disk count. The middleware returns one entry per ALERT, and a
	// single drive can raise several at once, so two alerts on one disk used to
	// report two disks. Entries whose device is identifiable are deduplicated;
	// prose-only fallback entries are not, because there is nothing to
	// deduplicate them on and merging them would hide a second real alert.
	t.Run("a disk raising several alerts is counted once", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			"disk.temperature_alerts": static([]interface{}{
				map[string]interface{}{"device": "nvme0n1"},
				map[string]interface{}{"device": "nvme0n1"},
				map[string]interface{}{"name": "nvme0n1"},
				"nvme1n1",
				"nvme1n1",
				map[string]interface{}{"device": "nvme2n1"},
			}),
		})
		alerts, err := client.DiskTemperatureAlerts(context.Background(), []string{"nvme0n1", "nvme1n1", "nvme2n1"})
		require.NoError(t, err)
		assert.Equal(t, []string{"nvme0n1", "nvme1n1", "nvme2n1"}, alerts,
			"the count is disks-with-an-alert, not alert entries")
	})

	t.Run("unidentifiable alerts are still counted individually", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			"disk.temperature_alerts": static([]interface{}{
				map[string]interface{}{"formatted": "a disk is too hot"},
				map[string]interface{}{"formatted": "a disk is too hot"},
			}),
		})
		alerts, err := client.DiskTemperatureAlerts(context.Background(), []string{"nvme0n1"})
		require.NoError(t, err)
		assert.Len(t, alerts, 2,
			"prose fallbacks name no device; collapsing them would hide a second real alert")
	})
}

// ---------------------------------------------------------------------------
// M3 round 2 — protocols parsing must be ALL-OR-NOTHING
// ---------------------------------------------------------------------------

// TestParseNFSServiceConfigProtocolsCompleteness pins M3's round-2 gap. Round 1
// only caught a WHOLLY unreadable list; a PARTIALLY parseable one still reported
// success, and `nfs.update {protocols: X}` REPLACES the list rather than unioning
// with it — so merging into a half-read base silently removes whatever the
// reader could not parse, for every export on the appliance.
func TestParseNFSServiceConfigProtocolsCompleteness(t *testing.T) {
	t.Run("a clean list is complete", func(t *testing.T) {
		cfg, err := parseNFSServiceConfig(map[string]interface{}{
			"protocols": []interface{}{"NFSV4", "nfsv3"},
		})
		require.NoError(t, err)
		assert.True(t, cfg.ProtocolsComplete)
		assert.Empty(t, cfg.ProtocolsAnomaly)
		assert.Equal(t, []string{"NFSV3", "NFSV4"}, cfg.Protocols)
	})

	t.Run("an UNKNOWN token is preserved, not filtered", func(t *testing.T) {
		cfg, err := parseNFSServiceConfig(map[string]interface{}{
			"protocols": []interface{}{"NFSV4", "NFSV5"},
		})
		require.NoError(t, err)
		assert.True(t, cfg.ProtocolsComplete)
		assert.Equal(t, []string{"NFSV4", "NFSV5"}, cfg.Protocols,
			"a future protocol must survive a managed write verbatim")
	})

	t.Run("a partially parseable list is INCOMPLETE", func(t *testing.T) {
		cfg, err := parseNFSServiceConfig(map[string]interface{}{
			"protocols": []interface{}{"NFSV4", map[string]interface{}{"name": "NFSV5"}},
		})
		require.NoError(t, err)
		assert.False(t, cfg.ProtocolsComplete,
			"one unreadable entry makes the whole list an unsafe basis for a replacement write")
		assert.Contains(t, cfg.ProtocolsAnomaly, "entry 1")
	})

	t.Run("an empty-string entry is INCOMPLETE", func(t *testing.T) {
		cfg, err := parseNFSServiceConfig(map[string]interface{}{
			"protocols": []interface{}{"NFSV4", "   "},
		})
		require.NoError(t, err)
		assert.False(t, cfg.ProtocolsComplete)
	})

	t.Run("a missing or wrong-typed field is INCOMPLETE", func(t *testing.T) {
		cfg, err := parseNFSServiceConfig(map[string]interface{}{"servers": float64(64)})
		require.NoError(t, err)
		assert.False(t, cfg.ProtocolsComplete)
		assert.Contains(t, cfg.ProtocolsAnomaly, "no \"protocols\" field")

		cfg, err = parseNFSServiceConfig(map[string]interface{}{"protocols": "NFSV4"})
		require.NoError(t, err)
		assert.False(t, cfg.ProtocolsComplete)
		assert.Contains(t, cfg.ProtocolsAnomaly, "not a list")
	})
}
