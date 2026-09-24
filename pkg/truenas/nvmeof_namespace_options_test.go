package truenas

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every nvmet.namespace.query must ask middleware to skip its per-row locked-path
// lookup, which measured ~90ms of each ~100ms query on nas01. The driver never
// reads NVMeoFNamespace.Locked.
func TestNVMeoFNamespaceQueriesSkipLockedInfo(t *testing.T) {
	var mu sync.Mutex
	var queryOptions []interface{}
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
			case "system.info":
				resp.Result = map[string]interface{}{"version": "TrueNAS-SCALE-25.10.0", "hostname": "truenas-test"}
			case "nvmet.namespace.query":
				mu.Lock()
				if len(req.Params) >= 2 {
					queryOptions = append(queryOptions, req.Params[1])
				} else {
					queryOptions = append(queryOptions, nil)
				}
				mu.Unlock()
				resp.Result = []interface{}{map[string]interface{}{
					"id": float64(10), "device_path": "zvol/tank/vol1", "device_type": "ZVOL", "enabled": true,
					"subsys": map[string]interface{}{"id": float64(3)},
				}}
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
	port := 80
	if len(parts) > 1 {
		_, _ = fmt.Sscanf(parts[1], "%d", &port)
	}
	client, err := NewClient(&ClientConfig{
		Host: parts[0], Port: port, Protocol: "http", APIKey: "test-api-key",
		Timeout: 5 * time.Second, ConnectTimeout: 5 * time.Second, MaxConnections: 1,
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	_, err = client.NVMeoFNamespaceGet(ctx, 10)
	require.NoError(t, err)
	_, err = client.NVMeoFNamespaceFindByDevice(ctx, 3, "/dev/zvol/tank/vol1")
	require.NoError(t, err)
	_, err = client.NVMeoFNamespaceFindByDevicePath(ctx, "zvol/tank/vol1")
	require.NoError(t, err)
	_, err = client.NVMeoFNamespaceListBySubsystem(ctx, 3)
	require.NoError(t, err)
	_, err = client.NVMeoFNamespaceList(ctx)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, queryOptions, 5)
	want := map[string]interface{}{"extra": map[string]interface{}{"retrieve_locked_info": false}}
	for i, options := range queryOptions {
		assert.Equal(t, want, options, "query %d", i)
	}
}
