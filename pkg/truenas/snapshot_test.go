package truenas

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resetSnapshotAPIPrefix is retained for existing tests; prefix state is now
// scoped to each newly-created client.
func resetSnapshotAPIPrefix() {
}

func newSnapshotTestClient(t *testing.T, serverURL string) *Client {
	t.Helper()
	wsURL := strings.Replace(serverURL, "http://", "", 1)
	parts := strings.Split(wsURL, ":")
	port := 80
	if len(parts) > 1 {
		_, _ = fmt.Sscanf(parts[1], "%d", &port)
	}
	client, err := NewClient(&ClientConfig{
		Host:           parts[0],
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

func TestParseSnapshotTrueNAS26WireShape(t *testing.T) {
	snapshot, err := parseSnapshot(map[string]interface{}{
		"name":          "tank/k8s/volumes/pvc-123@snap-1",
		"snapshot_name": "snap-1",
		"dataset":       "tank/k8s/volumes/pvc-123",
		"pool":          "tank",
		"type":          "SNAPSHOT",
		"createtxg":     "331921",
		"properties":    map[string]interface{}{},
		"user_properties": map[string]interface{}{
			"truenas-csi:managed_resource": "true",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "tank/k8s/volumes/pvc-123@snap-1", snapshot.ID)
	assert.Equal(t, "snap-1", snapshot.Name)
	assert.Equal(t, uint64(331921), snapshot.CreateTXG)
	assert.Equal(t, "true", snapshot.UserProperties["truenas-csi:managed_resource"].Value)
}

func TestSnapshotResourceQueryTrueNAS26FlatUserPropertiesAndDetectionCache(t *testing.T) {
	mock := newMockWSServer()
	var probeCalls atomic.Int32
	var queryCalls atomic.Int32
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case snapshotResourceQueryMethod:
				options := req.Params[0].(map[string]interface{})
				paths := options["paths"].([]interface{})
				if len(paths) == 0 {
					probeCalls.Add(1)
					resp.Result = []interface{}{}
					break
				}
				queryCalls.Add(1)
				resp.Result = []interface{}{
					map[string]interface{}{
						"name":          "tank/k8s/volumes/pvc-123@snap-1",
						"snapshot_name": "snap-1",
						"dataset":       "tank/k8s/volumes/pvc-123",
						"pool":          "tank",
						"type":          "SNAPSHOT",
						"createtxg":     float64(331921),
						"properties": map[string]interface{}{
							"used":     map[string]interface{}{"parsed": float64(4096)},
							"creation": map[string]interface{}{"rawvalue": "1700000000"},
						},
						"user_properties": map[string]interface{}{
							"truenas-csi:managed_resource": "true",
						},
					},
					map[string]interface{}{
						"name":            "tank/k8s/volumes/other@wrong-dataset",
						"snapshot_name":   "wrong-dataset",
						"dataset":         "tank/k8s/volumes/other",
						"properties":      map[string]interface{}{},
						"user_properties": map[string]interface{}{},
					},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()
	client := newSnapshotTestClient(t, server.URL)

	for range 2 {
		snapshots, err := client.SnapshotList(context.Background(), "tank/k8s/volumes/pvc-123")
		require.NoError(t, err)
		require.Len(t, snapshots, 1)
		assert.Equal(t, "tank/k8s/volumes/pvc-123@snap-1", snapshots[0].ID)
		assert.True(t, snapshots[0].ResourceQuery)
		assert.Equal(t, "snap-1", snapshots[0].Name)
		assert.Equal(t, uint64(331921), snapshots[0].CreateTXG)
		assert.Equal(t, "true", snapshots[0].UserProperties["truenas-csi:managed_resource"].Value)
	}
	snapshot, err := client.SnapshotGet(context.Background(), "tank/k8s/volumes/pvc-123@snap-1")
	require.NoError(t, err)
	assert.Equal(t, int64(4096), snapshot.GetSnapshotSize())
	assert.Equal(t, int64(1700000000), snapshot.GetCreationTime())
	assert.Equal(t, int32(1), probeCalls.Load())
	assert.Equal(t, int32(3), queryCalls.Load())
}

func TestSnapshotResourceQueryTrueNAS26RecursiveFilteringAndPagination(t *testing.T) {
	mock := newMockWSServer()
	optionsSeen := make(chan map[string]interface{}, 2)
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case snapshotResourceQueryMethod:
				options := req.Params[0].(map[string]interface{})
				if len(options["paths"].([]interface{})) == 0 {
					resp.Result = []interface{}{}
					break
				}
				optionsSeen <- options
				resp.Result = []interface{}{
					map[string]interface{}{"name": "tank/k8s/volumes/pvc-b@snap-b", "snapshot_name": "snap-b", "dataset": "tank/k8s/volumes/pvc-b", "properties": map[string]interface{}{}, "user_properties": map[string]interface{}{}},
					map[string]interface{}{"name": "tank/k8s/volumes@parent", "snapshot_name": "parent", "dataset": "tank/k8s/volumes", "properties": map[string]interface{}{}, "user_properties": map[string]interface{}{}},
					map[string]interface{}{"name": "tank/k8s/volumes-other@outside", "snapshot_name": "outside", "dataset": "tank/k8s/volumes-other", "properties": map[string]interface{}{}, "user_properties": map[string]interface{}{}},
					map[string]interface{}{"name": "tank/k8s/volumes/pvc-a@target-snap", "snapshot_name": "target-snap", "dataset": "tank/k8s/volumes/pvc-a", "properties": map[string]interface{}{}, "user_properties": map[string]interface{}{"truenas-csi:managed_resource": "true"}},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()
	client := newSnapshotTestClient(t, server.URL)

	snapshots, err := client.SnapshotListAll(context.Background(), "tank/k8s/volumes", 1, 1)
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	assert.Equal(t, "tank/k8s/volumes/pvc-b@snap-b", snapshots[0].ID)
	snapshot, err := client.SnapshotFindByName(context.Background(), "tank/k8s/volumes", "target-snap")
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.Equal(t, "true", snapshot.UserProperties["truenas-csi:managed_resource"].Value)

	for range 2 {
		options := <-optionsSeen
		assert.Equal(t, true, options["recursive"])
		assert.Equal(t, []interface{}{"used", "creation"}, options["properties"])
	}
}

// TestSnapshotCreate_Success tests creating a snapshot
func TestSnapshotCreate_Success(t *testing.T) {
	resetSnapshotAPIPrefix()
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

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				// API version detection - return success for pool.snapshot
				resp.Result = []interface{}{}
			case "pool.snapshot.create":
				resp.Result = map[string]interface{}{
					"id":      "tank/k8s/volumes/pvc-123@snap-test",
					"name":    "snap-test",
					"dataset": "tank/k8s/volumes/pvc-123",
					"pool":    "tank",
					"type":    "SNAPSHOT",
					"properties": map[string]interface{}{
						"used": map[string]interface{}{
							"parsed": float64(1024),
						},
						"creation": map[string]interface{}{
							"parsed": float64(1700000000),
						},
					},
					"user_properties": map[string]interface{}{},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	snap, err := client.SnapshotCreate(ctx, "tank/k8s/volumes/pvc-123", "snap-test", nil)
	require.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Equal(t, "tank/k8s/volumes/pvc-123@snap-test", snap.ID)
	assert.Equal(t, "snap-test", snap.Name)
	assert.Equal(t, "tank/k8s/volumes/pvc-123", snap.Dataset)
}

// TestSnapshotCreate_AlreadyExists tests idempotent snapshot creation
func TestSnapshotCreate_AlreadyExists(t *testing.T) {
	resetSnapshotAPIPrefix()
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

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.create":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "snapshot already exists",
				}
			case "pool.snapshot.get_instance":
				resp.Result = map[string]interface{}{
					"id":              "tank/k8s/volumes/pvc-123@existing",
					"name":            "existing",
					"dataset":         "tank/k8s/volumes/pvc-123",
					"pool":            "tank",
					"type":            "SNAPSHOT",
					"properties":      map[string]interface{}{},
					"user_properties": map[string]interface{}{},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	snap, err := client.SnapshotCreate(ctx, "tank/k8s/volumes/pvc-123", "existing", nil)
	require.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Equal(t, "existing", snap.Name)
}

// TestSnapshotCreate_ZFSAPIPrefix tests that zfs.snapshot.* API is used for TrueNAS 24.x
func TestSnapshotCreate_ZFSAPIPrefix(t *testing.T) {
	resetSnapshotAPIPrefix()
	mock := newMockWSServer()
	usedMethod := ""
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			var resp rpcTestResponse
			resp.JSONRPC = "2.0"
			resp.ID = req.ID

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				// Fail to trigger fallback to zfs.snapshot
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			case "zfs.snapshot.query":
				resp.Result = []interface{}{}
			case "zfs.snapshot.create":
				usedMethod = req.Method
				resp.Result = map[string]interface{}{
					"id":              "tank/k8s/volumes/pvc-123@zfs-snap",
					"name":            "zfs-snap",
					"dataset":         "tank/k8s/volumes/pvc-123",
					"pool":            "tank",
					"type":            "SNAPSHOT",
					"properties":      map[string]interface{}{},
					"user_properties": map[string]interface{}{},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	snap, err := client.SnapshotCreate(ctx, "tank/k8s/volumes/pvc-123", "zfs-snap", nil)
	require.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Equal(t, "zfs.snapshot.create", usedMethod)
}

func TestSnapshotCreateSendsPropertiesOnBothAPIGenerations(t *testing.T) {
	tests := []struct {
		name         string
		poolAPI      bool
		expectedCall string
	}{
		{name: "pool snapshot", poolAPI: true, expectedCall: "pool.snapshot.create"},
		{name: "legacy zfs snapshot", expectedCall: "zfs.snapshot.create"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := newMockWSServer()
			payloads := make(chan map[string]interface{}, 1)
			server := mock.start(func(conn *websocket.Conn) {
				for {
					var req rpcTestRequest
					if err := conn.ReadJSON(&req); err != nil {
						return
					}
					resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
					switch req.Method {
					case "auth.login_with_api_key":
						resp.Result = true
					case "pool.snapshot.query":
						if tc.poolAPI {
							resp.Result = []interface{}{}
						} else {
							resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
						}
					case "zfs.snapshot.query":
						resp.Result = []interface{}{}
					case tc.expectedCall:
						payloads <- req.Params[0].(map[string]interface{})
						resp.Result = map[string]interface{}{
							"id": "tank/csi/source@inline", "name": "inline", "dataset": "tank/csi/source",
						}
					default:
						resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
					}
					if err := conn.WriteJSON(resp); err != nil {
						return
					}
				}
			})
			defer mock.close()

			client := newSnapshotTestClient(t, server.URL)
			properties := map[string]string{
				"truenas-csi:managed_resource":  "true",
				"truenas-csi:csi_snapshot_name": "inline",
			}
			_, err := client.SnapshotCreate(context.Background(), "tank/csi/source", "inline", properties)
			require.NoError(t, err)

			payload := <-payloads
			assert.Equal(t, "tank/csi/source", payload["dataset"])
			assert.Equal(t, "inline", payload["name"])
			assert.Equal(t, map[string]interface{}{
				"truenas-csi:managed_resource":  "true",
				"truenas-csi:csi_snapshot_name": "inline",
			}, payload["properties"])
		})
	}
}

func TestSnapshotCreatePropertiesValidationFallbackIsCached(t *testing.T) {
	mock := newMockWSServer()
	var createCalls atomic.Int32
	var createCallsWithProperties atomic.Int32
	var updateCalls atomic.Int32
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.create":
				createCalls.Add(1)
				params := req.Params[0].(map[string]interface{})
				if _, ok := params["properties"]; ok {
					createCallsWithProperties.Add(1)
					resp.Error = &rpcError{Code: -32602, Message: "Invalid params"}
					break
				}
				name := params["name"].(string)
				resp.Result = map[string]interface{}{
					"id": "tank/csi/source@" + name, "name": name, "dataset": "tank/csi/source",
				}
			case "pool.snapshot.update":
				updateCalls.Add(1)
				resp.Result = nil
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()
	client := newSnapshotTestClient(t, server.URL)
	properties := map[string]string{"truenas-csi:a": "a", "truenas-csi:b": "b"}

	_, err := client.SnapshotCreate(context.Background(), "tank/csi/source", "first", properties)
	require.NoError(t, err)
	_, err = client.SnapshotCreate(context.Background(), "tank/csi/source", "second", properties)
	require.NoError(t, err)
	assert.Equal(t, int32(3), createCalls.Load(), "only the first call probes the properties payload")
	assert.Equal(t, int32(1), createCallsWithProperties.Load())
	assert.Equal(t, int32(4), updateCalls.Load())
}

// TestSnapshotDelete_Success tests deleting a snapshot
func TestSnapshotDelete_Success(t *testing.T) {
	resetSnapshotAPIPrefix()
	mock := newMockWSServer()
	deleteCalled := false
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			var resp rpcTestResponse
			resp.JSONRPC = "2.0"
			resp.ID = req.ID

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.delete":
				deleteCalled = true
				resp.Result = true
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	err = client.SnapshotDelete(ctx, "tank/k8s/volumes/pvc-123@snap-delete", false, false)
	assert.NoError(t, err)
	assert.True(t, deleteCalled)
}

// TestSnapshotDelete_NotFound tests deleting a non-existent snapshot (idempotent)
func TestSnapshotDelete_NotFound(t *testing.T) {
	resetSnapshotAPIPrefix()
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

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.delete":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "snapshot does not exist",
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	// Should succeed - idempotent delete
	err = client.SnapshotDelete(ctx, "tank/k8s/volumes/pvc-123@nonexistent", false, false)
	assert.NoError(t, err)
}

// TestSnapshotDelete_InvalidParams_NotFound tests delete with -32602 error when snapshot doesn't exist
func TestSnapshotDelete_InvalidParams_NotFound(t *testing.T) {
	resetSnapshotAPIPrefix()
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

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.delete":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "Invalid params",
				}
			case "pool.snapshot.get_instance":
				// Snapshot doesn't exist
				resp.Error = &rpcError{
					Code:    -32602,
					Message: "Invalid params",
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	// Should succeed - snapshot doesn't exist
	err = client.SnapshotDelete(ctx, "tank/k8s/volumes/pvc-123@nonexistent", false, false)
	assert.NoError(t, err)
}

// TestSnapshotDelete_HasClones tests delete failure when snapshot has clones
func TestSnapshotDelete_HasClones(t *testing.T) {
	resetSnapshotAPIPrefix()
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

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.delete":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "Invalid params",
				}
			case "pool.snapshot.get_instance":
				// Snapshot exists and has clones
				resp.Result = map[string]interface{}{
					"id":      "tank/k8s/volumes/pvc-123@with-clones",
					"name":    "with-clones",
					"dataset": "tank/k8s/volumes/pvc-123",
					"pool":    "tank",
					"type":    "SNAPSHOT",
					"properties": map[string]interface{}{
						"clones": map[string]interface{}{
							"value": "tank/k8s/volumes/clone1,tank/k8s/volumes/clone2",
						},
					},
					"user_properties": map[string]interface{}{},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	err = client.SnapshotDelete(ctx, "tank/k8s/volumes/pvc-123@with-clones", false, false)
	assert.Error(t, err)

	// Verify it's the correct error type
	var clonesErr *ErrSnapshotHasClones
	assert.ErrorAs(t, err, &clonesErr)
	assert.Len(t, clonesErr.Clones, 2)
	assert.Contains(t, clonesErr.Clones, "tank/k8s/volumes/clone1")
	assert.Contains(t, clonesErr.Clones, "tank/k8s/volumes/clone2")
}

// TestSnapshotDelete_HasClones_TrueNAS26 verifies the has-clones classification
// on the 26.0 API generation, where snapshot queries never expose the ZFS
// clones property: dependent clones must be detected via the dataset origin
// projection or the tombstone/defer flow in DeleteSnapshot is unreachable.
func TestSnapshotDelete_HasClones_TrueNAS26(t *testing.T) {
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case snapshotResourceQueryMethod:
				options := req.Params[0].(map[string]interface{})
				if len(options["paths"].([]interface{})) == 0 {
					resp.Result = []interface{}{} // detection probe
					break
				}
				// 26.0 shape: no clones property, ever.
				resp.Result = []interface{}{
					map[string]interface{}{
						"name":            "tank/k8s/volumes/pvc-123@with-clones",
						"snapshot_name":   "with-clones",
						"dataset":         "tank/k8s/volumes/pvc-123",
						"pool":            "tank",
						"type":            "SNAPSHOT",
						"properties":      map[string]interface{}{},
						"user_properties": map[string]interface{}{},
					},
				}
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.delete":
				resp.Error = &rpcError{Code: -1, Message: "Invalid params"}
			case "pool.dataset.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":   "tank/k8s/volumes/restored-pvc",
						"name": "tank/k8s/volumes/restored-pvc",
						"pool": "tank",
						"type": "VOLUME",
						"origin": map[string]interface{}{
							"parsed":   "tank/k8s/volumes/pvc-123@with-clones",
							"rawvalue": "tank/k8s/volumes/pvc-123@with-clones",
							"value":    "tank/k8s/volumes/pvc-123@with-clones",
							"source":   "NONE",
						},
					},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()
	client := newSnapshotTestClient(t, server.URL)

	err := client.SnapshotDelete(context.Background(), "tank/k8s/volumes/pvc-123@with-clones", false, false)
	var clonesErr *ErrSnapshotHasClones
	require.ErrorAs(t, err, &clonesErr)
	assert.Equal(t, []string{"tank/k8s/volumes/restored-pvc"}, clonesErr.Clones)
}

// TestSnapshotGet_Success tests getting a snapshot
func TestSnapshotGet_Success(t *testing.T) {
	resetSnapshotAPIPrefix()
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

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.get_instance":
				resp.Result = map[string]interface{}{
					"id":      "tank/k8s/volumes/pvc-123@snap-get",
					"name":    "snap-get",
					"dataset": "tank/k8s/volumes/pvc-123",
					"pool":    "tank",
					"type":    "SNAPSHOT",
					"properties": map[string]interface{}{
						"used": map[string]interface{}{
							"parsed": float64(2048),
						},
						"creation": map[string]interface{}{
							"parsed": map[string]interface{}{
								"$date": float64(1700000000000),
							},
						},
					},
					"user_properties": map[string]interface{}{
						"truenas-csi:source_volume": map[string]interface{}{
							"value":  "pvc-original",
							"source": "LOCAL",
						},
					},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	snap, err := client.SnapshotGet(ctx, "tank/k8s/volumes/pvc-123@snap-get")
	require.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Equal(t, "tank/k8s/volumes/pvc-123@snap-get", snap.ID)
	assert.Equal(t, "snap-get", snap.Name)
	assert.Equal(t, int64(2048), snap.GetSnapshotSize())
	assert.Equal(t, int64(1700000000), snap.GetCreationTime())
	assert.Equal(t, "pvc-original", snap.UserProperties["truenas-csi:source_volume"].Value)
}

// TestSnapshotGet_NotFound tests getting a non-existent snapshot
func TestSnapshotGet_NotFound(t *testing.T) {
	resetSnapshotAPIPrefix()
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

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.get_instance":
				resp.Error = &rpcError{
					Code:    -32602,
					Message: "Invalid params",
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	snap, err := client.SnapshotGet(ctx, "tank/k8s/volumes/pvc-123@nonexistent")
	assert.Error(t, err)
	assert.Nil(t, snap)
	assert.Contains(t, err.Error(), "not found")
}

// TestSnapshotList_Success tests listing snapshots for a dataset
func TestSnapshotList_Success(t *testing.T) {
	resetSnapshotAPIPrefix()
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

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":              "tank/k8s/volumes/pvc-123@snap1",
						"name":            "snap1",
						"dataset":         "tank/k8s/volumes/pvc-123",
						"pool":            "tank",
						"type":            "SNAPSHOT",
						"properties":      map[string]interface{}{},
						"user_properties": map[string]interface{}{},
					},
					map[string]interface{}{
						"id":              "tank/k8s/volumes/pvc-123@snap2",
						"name":            "snap2",
						"dataset":         "tank/k8s/volumes/pvc-123",
						"pool":            "tank",
						"type":            "SNAPSHOT",
						"properties":      map[string]interface{}{},
						"user_properties": map[string]interface{}{},
					},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	snapshots, err := client.SnapshotList(ctx, "tank/k8s/volumes/pvc-123")
	require.NoError(t, err)
	assert.Len(t, snapshots, 2)
	assert.Equal(t, "snap1", snapshots[0].Name)
	assert.Equal(t, "snap2", snapshots[1].Name)
}

// TestSnapshotListAll_Success tests listing all snapshots under a parent dataset
func TestSnapshotListAll_Success(t *testing.T) {
	resetSnapshotAPIPrefix()
	mock := newMockWSServer()
	queryParams := make(chan []interface{}, 1)
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			var resp rpcTestResponse
			resp.JSONRPC = "2.0"
			resp.ID = req.ID

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				if len(req.Params) > 0 {
					if filters, ok := req.Params[0].([]interface{}); ok && len(filters) > 0 {
						queryParams <- req.Params
					}
				}
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":              "tank/k8s/volumes/pvc-1@snap",
						"name":            "snap",
						"dataset":         "tank/k8s/volumes/pvc-1",
						"pool":            "tank",
						"type":            "SNAPSHOT",
						"properties":      map[string]interface{}{},
						"user_properties": map[string]interface{}{},
					},
					map[string]interface{}{
						"id":              "tank/k8s/volumes/pvc-2@snap",
						"name":            "snap",
						"dataset":         "tank/k8s/volumes/pvc-2",
						"pool":            "tank",
						"type":            "SNAPSHOT",
						"properties":      map[string]interface{}{},
						"user_properties": map[string]interface{}{},
					},
					map[string]interface{}{
						"id":              "tank/k8s/volumes/pvc-3@backup",
						"name":            "backup",
						"dataset":         "tank/k8s/volumes/pvc-3",
						"pool":            "tank",
						"type":            "SNAPSHOT",
						"properties":      map[string]interface{}{},
						"user_properties": map[string]interface{}{},
					},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	snapshots, err := client.SnapshotListAll(ctx, "tank/k8s/volumes", 0, 0)
	require.NoError(t, err)
	assert.Len(t, snapshots, 3)
	params := <-queryParams
	filters := params[0].([]interface{})
	datasetFilter := filters[0].([]interface{})
	assert.Equal(t, []interface{}{"dataset", "^", "tank/k8s/volumes/"}, datasetFilter)
}

// TestSnapshotFindByName_Success tests finding a snapshot by name
func TestSnapshotFindByName_Success(t *testing.T) {
	resetSnapshotAPIPrefix()
	mock := newMockWSServer()
	queryParams := make(chan []interface{}, 1)
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			var resp rpcTestResponse
			resp.JSONRPC = "2.0"
			resp.ID = req.ID

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				if len(req.Params) > 0 {
					if filters, ok := req.Params[0].([]interface{}); ok && len(filters) > 0 {
						queryParams <- req.Params
					}
				}
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":              "tank/k8s/volumes/pvc-find@target-snap",
						"name":            "target-snap",
						"dataset":         "tank/k8s/volumes/pvc-find",
						"pool":            "tank",
						"type":            "SNAPSHOT",
						"properties":      map[string]interface{}{},
						"user_properties": map[string]interface{}{},
					},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	snap, err := client.SnapshotFindByName(ctx, "tank/k8s/volumes", "target-snap")
	require.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Equal(t, "target-snap", snap.Name)
	params := <-queryParams
	filters := params[0].([]interface{})
	datasetFilter := filters[0].([]interface{})
	assert.Equal(t, []interface{}{"dataset", "^", "tank/k8s/volumes/"}, datasetFilter)
}

// TestSnapshotFindByName_NotFound tests finding a snapshot that doesn't exist
func TestSnapshotFindByName_NotFound(t *testing.T) {
	resetSnapshotAPIPrefix()
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

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	snap, err := client.SnapshotFindByName(ctx, "tank/k8s/volumes", "nonexistent")
	require.NoError(t, err) // Not an error, just returns nil
	assert.Nil(t, snap)
}

// TestSnapshotClone_Success tests cloning a snapshot
func TestSnapshotClone_Success(t *testing.T) {
	resetSnapshotAPIPrefix()
	mock := newMockWSServer()
	cloneCalled := false
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			var resp rpcTestResponse
			resp.JSONRPC = "2.0"
			resp.ID = req.ID

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.clone":
				cloneCalled = true
				resp.Result = true
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	err = client.SnapshotClone(ctx, "tank/k8s/volumes/pvc-source@snap", "tank/k8s/volumes/pvc-clone")
	assert.NoError(t, err)
	assert.True(t, cloneCalled)
}

// TestSnapshotClone_AlreadyExists tests idempotent clone creation
func TestSnapshotClone_AlreadyExists(t *testing.T) {
	resetSnapshotAPIPrefix()
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

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.clone":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "dataset already exists",
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	// Should succeed - idempotent
	err = client.SnapshotClone(ctx, "tank/k8s/volumes/pvc-source@snap", "tank/k8s/volumes/pvc-existing")
	assert.NoError(t, err)
}

// TestSnapshotRollback_Success tests rolling back to a snapshot
func TestSnapshotRollback_Success(t *testing.T) {
	resetSnapshotAPIPrefix()
	mock := newMockWSServer()
	rollbackCalled := false
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			var resp rpcTestResponse
			resp.JSONRPC = "2.0"
			resp.ID = req.ID

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.rollback":
				rollbackCalled = true
				resp.Result = true
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	err = client.SnapshotRollback(ctx, "tank/k8s/volumes/pvc-123@snap-rollback", true, false, false)
	assert.NoError(t, err)
	assert.True(t, rollbackCalled)
}

// TestSnapshotSetUserProperty_Success tests setting a user property on a snapshot
func TestSnapshotSetUserProperty_Success(t *testing.T) {
	resetSnapshotAPIPrefix()
	mock := newMockWSServer()
	updateCalled := false
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			var resp rpcTestResponse
			resp.JSONRPC = "2.0"
			resp.ID = req.ID

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.snapshot.query":
				resp.Result = []interface{}{}
			case "pool.snapshot.update":
				updateCalled = true
				resp.Result = map[string]interface{}{
					"id":              "tank/k8s/volumes/pvc-123@snap-prop",
					"name":            "snap-prop",
					"dataset":         "tank/k8s/volumes/pvc-123",
					"properties":      map[string]interface{}{},
					"user_properties": map[string]interface{}{},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}

			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

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
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	err = client.SnapshotSetUserProperty(ctx, "tank/k8s/volumes/pvc-123@snap-prop", "truenas-csi:test", "test-value")
	assert.NoError(t, err)
	assert.True(t, updateCalled)
}

// TestParseSnapshot tests the parseSnapshot function
func TestParseSnapshot_ValidData(t *testing.T) {
	data := map[string]interface{}{
		"id":      "tank/test@snap",
		"name":    "snap",
		"dataset": "tank/test",
		"pool":    "tank",
		"type":    "SNAPSHOT",
		"properties": map[string]interface{}{
			"used": map[string]interface{}{
				"parsed": float64(4096),
			},
			"creation": map[string]interface{}{
				"rawvalue": "1700000000",
			},
			"clones": map[string]interface{}{
				"value": "tank/clone1,tank/clone2",
			},
			"custom:prop": map[string]interface{}{
				"value":  "custom-value",
				"source": "LOCAL",
			},
		},
		"user_properties": map[string]interface{}{
			"user:key": map[string]interface{}{
				"value":  "user-value",
				"source": "LOCAL",
			},
		},
	}

	snap, err := parseSnapshot(data)
	require.NoError(t, err)
	assert.Equal(t, "tank/test@snap", snap.ID)
	assert.Equal(t, "snap", snap.Name)
	assert.Equal(t, "tank/test", snap.Dataset)
	assert.Equal(t, "tank", snap.Pool)
	assert.Equal(t, "SNAPSHOT", snap.Type)
	assert.Equal(t, int64(4096), snap.GetSnapshotSize())
	assert.Equal(t, int64(1700000000), snap.GetCreationTime())
	assert.Len(t, snap.GetClones(), 2)
	assert.Equal(t, "user-value", snap.UserProperties["user:key"].Value)
	// User properties from properties map (keys with :)
	assert.Equal(t, "custom-value", snap.UserProperties["custom:prop"].Value)
}

// TestParseSnapshot_InvalidData tests parseSnapshot with invalid input
func TestParseSnapshot_InvalidData(t *testing.T) {
	snap, err := parseSnapshot("invalid")
	assert.Error(t, err)
	assert.Nil(t, snap)
	assert.Contains(t, err.Error(), "unexpected snapshot format")
}

// TestSnapshot_GetSnapshotSize tests the GetSnapshotSize method
func TestSnapshot_GetSnapshotSize(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]interface{}
		expected   int64
	}{
		{
			name: "with parsed value",
			properties: map[string]interface{}{
				"used": map[string]interface{}{
					"parsed": float64(8192),
				},
			},
			expected: 8192,
		},
		{
			name:       "without used property",
			properties: map[string]interface{}{},
			expected:   0,
		},
		{
			name: "with nil parsed",
			properties: map[string]interface{}{
				"used": map[string]interface{}{
					"value": "8K",
				},
			},
			expected: 0,
		},
		{
			// TrueNAS 26.0 zfs.resource.snapshot.query shape (live-probed)
			name: "with 26.0 numeric value and raw",
			properties: map[string]interface{}{
				"used": map[string]interface{}{
					"value":  float64(2723840),
					"raw":    "2723840",
					"source": nil,
				},
			},
			expected: 2723840,
		},
		{
			name: "with 26.0 raw string only",
			properties: map[string]interface{}{
				"used": map[string]interface{}{
					"raw": "4096",
				},
			},
			expected: 4096,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := &Snapshot{Properties: tt.properties}
			assert.Equal(t, tt.expected, snap.GetSnapshotSize())
		})
	}
}

// TestSnapshot_GetCreationTime tests the GetCreationTime method
func TestSnapshot_GetCreationTime(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]interface{}
		expected   int64
	}{
		{
			name: "with $date format",
			properties: map[string]interface{}{
				"creation": map[string]interface{}{
					"parsed": map[string]interface{}{
						"$date": float64(1700000000000),
					},
				},
			},
			expected: 1700000000,
		},
		{
			name: "with direct float64",
			properties: map[string]interface{}{
				"creation": map[string]interface{}{
					"parsed": float64(1700000000),
				},
			},
			expected: 1700000000,
		},
		{
			name: "with rawvalue string",
			properties: map[string]interface{}{
				"creation": map[string]interface{}{
					"rawvalue": "1700000000",
				},
			},
			expected: 1700000000,
		},
		{
			name:       "without creation property",
			properties: map[string]interface{}{},
			expected:   0,
		},
		{
			// TrueNAS 26.0 zfs.resource.snapshot.query shape (live-probed)
			name: "with 26.0 numeric value and raw",
			properties: map[string]interface{}{
				"creation": map[string]interface{}{
					"value":  float64(1754693322),
					"raw":    "1754693322",
					"source": nil,
				},
			},
			expected: 1754693322,
		},
		{
			name: "with 26.0 raw string only",
			properties: map[string]interface{}{
				"creation": map[string]interface{}{
					"raw": "1700000001",
				},
			},
			expected: 1700000001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := &Snapshot{Properties: tt.properties}
			assert.Equal(t, tt.expected, snap.GetCreationTime())
		})
	}
}

// TestSnapshot_GetClones tests the GetClones method
func TestSnapshot_GetClones(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]interface{}
		expected   []string
	}{
		{
			name: "with multiple clones",
			properties: map[string]interface{}{
				"clones": map[string]interface{}{
					"value": "tank/clone1,tank/clone2,tank/clone3",
				},
			},
			expected: []string{"tank/clone1", "tank/clone2", "tank/clone3"},
		},
		{
			name: "with single clone",
			properties: map[string]interface{}{
				"clones": map[string]interface{}{
					"value": "tank/single-clone",
				},
			},
			expected: []string{"tank/single-clone"},
		},
		{
			name: "with empty value",
			properties: map[string]interface{}{
				"clones": map[string]interface{}{
					"value": "",
				},
			},
			expected: nil,
		},
		{
			name:       "without clones property",
			properties: map[string]interface{}{},
			expected:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := &Snapshot{Properties: tt.properties}
			assert.Equal(t, tt.expected, snap.GetClones())
		})
	}
}

// TestErrSnapshotHasClones_Error tests the ErrSnapshotHasClones error type
func TestErrSnapshotHasClones_Error(t *testing.T) {
	err := &ErrSnapshotHasClones{
		SnapshotID: "tank/test@snap",
		Clones:     []string{"tank/clone1", "tank/clone2"},
	}

	assert.Contains(t, err.Error(), "tank/test@snap")
	assert.Contains(t, err.Error(), "dependent clones")
	assert.Contains(t, err.Error(), "tank/clone1")
	assert.Contains(t, err.Error(), "tank/clone2")
}

// TestMockClient_SnapshotOperations tests the MockClient snapshot operations
func TestMockClient_SnapshotOperations(t *testing.T) {
	mock := NewMockClient()
	ctx := context.Background()

	// Create a dataset first
	_, err := mock.DatasetCreate(ctx, &DatasetCreateParams{
		Name: "tank/test/snap-parent",
		Type: "FILESYSTEM",
	})
	require.NoError(t, err)

	// Test create snapshot
	snap, err := mock.SnapshotCreate(ctx, "tank/test/snap-parent", "test-snap", nil)
	require.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Equal(t, "tank/test/snap-parent@test-snap", snap.ID)
	assert.Equal(t, "test-snap", snap.Name)

	// Test get snapshot
	snap, err = mock.SnapshotGet(ctx, "tank/test/snap-parent@test-snap")
	require.NoError(t, err)
	assert.Equal(t, "test-snap", snap.Name)

	// Test list snapshots
	snapshots, err := mock.SnapshotList(ctx, "tank/test/snap-parent")
	require.NoError(t, err)
	assert.Len(t, snapshots, 1)

	// Test find by name
	snap, err = mock.SnapshotFindByName(ctx, "tank/test/snap-parent", "test-snap")
	require.NoError(t, err)
	assert.NotNil(t, snap)

	// Test set user property
	err = mock.SnapshotSetUserProperty(ctx, "tank/test/snap-parent@test-snap", "test:key", "test-value")
	require.NoError(t, err)

	// Test clone
	err = mock.SnapshotClone(ctx, "tank/test/snap-parent@test-snap", "tank/test/snap-clone")
	require.NoError(t, err)

	// Verify clone created as dataset
	exists, err := mock.DatasetExists(ctx, "tank/test/snap-clone")
	require.NoError(t, err)
	assert.True(t, exists)

	// Non-deferred delete of a snapshot with dependent clones must fail,
	// mirroring real ZFS semantics.
	err = mock.SnapshotDelete(ctx, "tank/test/snap-parent@test-snap", false, false)
	var cloneErr *ErrSnapshotHasClones
	require.ErrorAs(t, err, &cloneErr)

	// Deferred delete succeeds and the snapshot is reclaimed once the last
	// dependent clone is removed.
	err = mock.SnapshotDelete(ctx, "tank/test/snap-parent@test-snap", true, false)
	require.NoError(t, err)
	err = mock.DatasetDelete(ctx, "tank/test/snap-clone", false, false)
	require.NoError(t, err)

	// Verify deletion
	_, err = mock.SnapshotGet(ctx, "tank/test/snap-parent@test-snap")
	assert.Error(t, err)
}

// TestMockClient_SnapshotErrorInjection tests error injection in MockClient for snapshots
func TestMockClient_SnapshotErrorInjection(t *testing.T) {
	mock := NewMockClient()
	ctx := context.Background()

	// Inject error
	mock.InjectError = fmt.Errorf("injected snapshot error")

	_, err := mock.SnapshotCreate(ctx, "tank/test", "snap", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "injected snapshot error")

	// Clear error
	mock.InjectError = nil

	// Should succeed now
	snap, err := mock.SnapshotCreate(ctx, "tank/test", "snap", nil)
	require.NoError(t, err)
	assert.NotNil(t, snap)
}

// TestSnapshotCreate_TableDriven uses table-driven tests for various create scenarios
func TestSnapshotCreate_TableDriven(t *testing.T) {
	tests := []struct {
		name         string
		dataset      string
		snapName     string
		mockResponse interface{}
		mockError    *rpcError
		expectError  bool
	}{
		{
			name:     "basic snapshot",
			dataset:  "tank/vol/basic",
			snapName: "snap1",
			mockResponse: map[string]interface{}{
				"id":              "tank/vol/basic@snap1",
				"name":            "snap1",
				"dataset":         "tank/vol/basic",
				"pool":            "tank",
				"type":            "SNAPSHOT",
				"properties":      map[string]interface{}{},
				"user_properties": map[string]interface{}{},
			},
			expectError: false,
		},
		{
			name:     "snapshot with special chars",
			dataset:  "tank/vol/special",
			snapName: "snap-2024-01-01_12-00-00",
			mockResponse: map[string]interface{}{
				"id":              "tank/vol/special@snap-2024-01-01_12-00-00",
				"name":            "snap-2024-01-01_12-00-00",
				"dataset":         "tank/vol/special",
				"pool":            "tank",
				"type":            "SNAPSHOT",
				"properties":      map[string]interface{}{},
				"user_properties": map[string]interface{}{},
			},
			expectError: false,
		},
		{
			name:     "snapshot creation failure",
			dataset:  "tank/vol/fail",
			snapName: "fail-snap",
			mockError: &rpcError{
				Code:    -1,
				Message: "failed to create snapshot",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetSnapshotAPIPrefix()
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

					switch req.Method {
					case "auth.login_with_api_key":
						resp.Result = true
					case "pool.snapshot.query":
						resp.Result = []interface{}{}
					case "pool.snapshot.create":
						if tt.mockError != nil {
							resp.Error = tt.mockError
						} else {
							resp.Result = tt.mockResponse
						}
					default:
						resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
					}

					if err := conn.WriteJSON(resp); err != nil {
						return
					}
				}
			})
			defer mock.close()

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
			defer func() { _ = client.Close() }()

			ctx := context.Background()
			snap, err := client.SnapshotCreate(ctx, tt.dataset, tt.snapName, nil)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, snap)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, snap)
				assert.Equal(t, tt.snapName, snap.Name)
			}
		})
	}
}

// Benchmark tests
func BenchmarkParseSnapshot(b *testing.B) {
	data := map[string]interface{}{
		"id":      "tank/test@snap",
		"name":    "snap",
		"dataset": "tank/test",
		"pool":    "tank",
		"type":    "SNAPSHOT",
		"properties": map[string]interface{}{
			"used": map[string]interface{}{
				"parsed": float64(4096),
			},
			"creation": map[string]interface{}{
				"parsed": map[string]interface{}{
					"$date": float64(1700000000000),
				},
			},
			"clones": map[string]interface{}{
				"value": "tank/clone1,tank/clone2",
			},
		},
		"user_properties": map[string]interface{}{
			"user:key1": map[string]interface{}{
				"value":  "value1",
				"source": "LOCAL",
			},
			"user:key2": map[string]interface{}{
				"value":  "value2",
				"source": "LOCAL",
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parseSnapshot(data)
	}
}

func BenchmarkSnapshot_GetClones(b *testing.B) {
	snap := &Snapshot{
		Properties: map[string]interface{}{
			"clones": map[string]interface{}{
				"value": "tank/clone1,tank/clone2,tank/clone3,tank/clone4,tank/clone5",
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = snap.GetClones()
	}
}
