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

func TestServiceReloadUsesAndCachesServiceControl(t *testing.T) {
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
				resp.Result = nil
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

	assert.Zero(t, reloadCalls.Load(), "TrueNAS 26.0 must not receive a service.reload probe")
	assert.Equal(t, int32(2), controlCalls.Load(), "service.control must serve and remain cached for both reloads")
}

func TestServiceReloadFallsBackToAndCachesLegacyVerb(t *testing.T) {
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
			case "service.control":
				controlCalls.Add(1)
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			case "service.reload":
				reloadCalls.Add(1)
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

	assert.Equal(t, int32(1), controlCalls.Load(), "service.control must be probed only on the first legacy reload")
	assert.Equal(t, int32(2), reloadCalls.Load(), "service.reload must serve both reloads once the legacy verb is cached")
}
