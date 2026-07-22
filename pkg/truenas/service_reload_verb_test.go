package truenas

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServiceReloadCachesResolvedVerb verifies that once the reload verb is
// resolved (TrueNAS 26.0 removed service.reload, so the first call falls back
// to service.control), subsequent reloads skip the service.reload probe and go
// straight to service.control — turning a 2-RTT reload into a 1-RTT reload.
func TestServiceReloadCachesResolvedVerb(t *testing.T) {
	var reloadCalls atomic.Int32
	var controlCalls atomic.Int32
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
			case "service.reload":
				reloadCalls.Add(1)
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			case "service.control":
				controlCalls.Add(1)
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

	host, port := testServerAddress(t, server.URL)
	client, err := NewClient(&ClientConfig{
		Host:                host,
		Port:                port,
		Protocol:            "http",
		APIKey:              "test-api-key",
		Timeout:             time.Second,
		ConnectTimeout:      time.Second,
		MaxConnections:      1,
		APIRetryMaxAttempts: 1,
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, client.ServiceReload(ctx, "iscsitarget"))
	require.NoError(t, client.ServiceReload(ctx, "iscsitarget"))

	assert.Equal(t, int32(1), reloadCalls.Load(), "service.reload must be probed only on the first reload")
	assert.Equal(t, int32(2), controlCalls.Load(), "service.control must serve both reloads once the verb is cached")
}
