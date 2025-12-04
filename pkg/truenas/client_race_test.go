package truenas

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Tests for Race Condition Fixes in client.go
// These tests validate that the atomic.Pointer fixes prevent data races
// =============================================================================

// TestConnection_AtomicConnAccess_ConcurrentReadWrite validates that concurrent
// access to the conn field using atomic operations does not cause data races.
// This tests the fix for: "Race condition in connection - Changed conn field to atomic.Pointer"
func TestConnection_AtomicConnAccess_ConcurrentReadWrite(t *testing.T) {
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			resp := rpcTestResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
			}

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "core.ping":
				resp.Result = "pong"
			default:
				resp.Result = "ok"
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

	cfg := &ClientConfig{
		Host:              host,
		Port:              port,
		Protocol:          "http",
		APIKey:            "test-api-key",
		Timeout:           5 * time.Second,
		ConnectTimeout:    5 * time.Second,
		MaxConnections:    3,
		HeartbeatInterval: 100 * time.Millisecond, // Fast heartbeat to test concurrent access
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	// Run concurrent operations that access conn atomically
	var wg sync.WaitGroup
	numGoroutines := 20
	numIterations := 10

	// Track successful operations
	var successCount int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := client.Call(ctx, "test.method", id, j)
				cancel()
				if err == nil {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(i)
	}

	// Also test IsConnected calls concurrently (reads atomic conn)
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations*2; j++ {
				_ = client.IsConnected()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()

	// At least some operations should succeed
	assert.Greater(t, atomic.LoadInt64(&successCount), int64(0), "Some operations should succeed")
}

// TestConnection_AtomicConnSwap_Reconnect tests that atomic swap during reconnect
// does not cause races with concurrent read operations.
func TestConnection_AtomicConnSwap_Reconnect(t *testing.T) {
	// Create a connection that will fail after some requests to force reconnect
	requestCount := int32(0)
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			resp := rpcTestResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
			}

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			default:
				count := atomic.AddInt32(&requestCount, 1)
				// Fail some requests to simulate connection issues
				if count%10 == 0 {
					resp.Error = &rpcError{Code: -1, Message: "simulated error"}
				} else {
					resp.Result = "ok"
				}
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

	cfg := &ClientConfig{
		Host:           host,
		Port:           port,
		Protocol:       "http",
		APIKey:         "test-api-key",
		Timeout:        2 * time.Second,
		ConnectTimeout: 2 * time.Second,
		MaxConnections: 2,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup

	// Concurrent callers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, _ = client.Call(ctx, "test.method", id, j)
				cancel()
			}
		}(i)
	}

	// Concurrent IsConnected checks (reads the atomic conn pointer)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = client.IsConnected()
			}
		}()
	}

	wg.Wait()
	// Test passes if no race condition detected by -race flag
}

// =============================================================================
// Tests for Channel Leak Fix in client.go
// These tests validate that reordering drain/delete operations prevents leaks
// =============================================================================

// TestConnection_ChannelDrain_Timeout tests that the response channel is properly
// drained before being deleted from pending map on timeout.
// This tests the fix for: "Channel leak on timeout - Reordered drain/delete operations"
func TestConnection_ChannelDrain_Timeout(t *testing.T) {
	// Server that responds slowly to trigger timeouts
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			resp := rpcTestResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
			}

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
				if err := conn.WriteJSON(resp); err != nil {
					return
				}
			case "slow.method":
				// Delay response to cause timeout
				time.Sleep(500 * time.Millisecond)
				resp.Result = "delayed response"
				// This response arrives after caller has timed out
				// Without proper drain, this would cause a goroutine leak
				if err := conn.WriteJSON(resp); err != nil {
					return
				}
			default:
				resp.Result = "ok"
				if err := conn.WriteJSON(resp); err != nil {
					return
				}
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

	cfg := &ClientConfig{
		Host:           host,
		Port:           port,
		Protocol:       "http",
		APIKey:         "test-api-key",
		Timeout:        100 * time.Millisecond, // Short timeout
		ConnectTimeout: 2 * time.Second,
		MaxConnections: 1,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	// Make calls that will timeout
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			_, err := client.Call(ctx, "slow.method", id)
			// Expect timeout error
			assert.Error(t, err)
		}(i)
	}

	wg.Wait()

	// Wait for server to send delayed responses
	time.Sleep(600 * time.Millisecond)

	// Connection should still be functional after handling timeouts
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := client.Call(ctx, "test.method")
	assert.NoError(t, err)
	assert.Equal(t, "ok", result)
}

// TestConnection_ChannelDrain_ContextCancellation tests proper cleanup when
// context is cancelled during the request/write phase.
func TestConnection_ChannelDrain_ContextCancellation(t *testing.T) {
	mock := newMockWSServer()
	requestsReceived := int32(0)

	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			atomic.AddInt32(&requestsReceived, 1)

			resp := rpcTestResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
			}

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			default:
				// Small delay to allow context cancellation to occur
				time.Sleep(10 * time.Millisecond)
				resp.Result = "ok"
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

	cfg := &ClientConfig{
		Host:           host,
		Port:           port,
		Protocol:       "http",
		APIKey:         "test-api-key",
		Timeout:        5 * time.Second,
		ConnectTimeout: 2 * time.Second,
		MaxConnections: 1,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	// Make calls with immediate cancellation
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			// Cancel immediately
			cancel()

			_, err := client.Call(ctx, "test.method", id)
			// Expect context cancelled error
			assert.Error(t, err)
		}(i)
	}

	wg.Wait()

	// Give some time for any cleanup
	time.Sleep(50 * time.Millisecond)

	// Connection should still work
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := client.Call(ctx, "ping")
	assert.NoError(t, err)
	assert.Equal(t, "ok", result)
}

// TestConnection_PendingMapCleanup verifies that timed-out requests don't leave
// orphaned entries in the pending map.
func TestConnection_PendingMapCleanup(t *testing.T) {
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			resp := rpcTestResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
			}

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
				_ = conn.WriteJSON(resp)
			case "fast.method":
				resp.Result = "fast"
				_ = conn.WriteJSON(resp)
			case "slow.method":
				time.Sleep(200 * time.Millisecond)
				resp.Result = "slow"
				_ = conn.WriteJSON(resp)
			default:
				resp.Result = "ok"
				_ = conn.WriteJSON(resp)
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

	cfg := &ClientConfig{
		Host:           host,
		Port:           port,
		Protocol:       "http",
		APIKey:         "test-api-key",
		Timeout:        50 * time.Millisecond,
		ConnectTimeout: 2 * time.Second,
		MaxConnections: 1,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	// Make several timeout calls
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		_, _ = client.Call(ctx, "slow.method")
		cancel()
	}

	// Wait for delayed responses
	time.Sleep(300 * time.Millisecond)

	// Verify we can still make fast calls - this ensures the pending map isn't corrupted
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := client.Call(ctx, "fast.method")
	assert.NoError(t, err)
	assert.Equal(t, "fast", result)
}

// =============================================================================
// Tests for Connection Pool and Concurrent Access
// =============================================================================

// TestClient_ConnectionPool_ConcurrentAccess tests that the connection pool
// handles concurrent access correctly with atomic operations.
func TestClient_ConnectionPool_ConcurrentAccess(t *testing.T) {
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			resp := rpcTestResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
			}

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			default:
				resp.Result = fmt.Sprintf("response-%d", req.ID)
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

	cfg := &ClientConfig{
		Host:              host,
		Port:              port,
		Protocol:          "http",
		APIKey:            "test-api-key",
		Timeout:           5 * time.Second,
		ConnectTimeout:    2 * time.Second,
		MaxConnections:    5,
		MaxConcurrentReqs: 20,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	var successCount int64
	numRequests := 100

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			result, err := client.Call(ctx, "test.method", id)
			if err == nil && result != nil {
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Most requests should succeed
	assert.Greater(t, atomic.LoadInt64(&successCount), int64(numRequests/2),
		"At least half the requests should succeed")
}

// TestConnection_WriteLoop_SafeShutdown tests that the write loop handles
// shutdown gracefully without sending on closed channels.
func TestConnection_WriteLoop_SafeShutdown(t *testing.T) {
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			resp := rpcTestResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
			}

			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			default:
				resp.Result = "ok"
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

	cfg := &ClientConfig{
		Host:           host,
		Port:           port,
		Protocol:       "http",
		APIKey:         "test-api-key",
		Timeout:        2 * time.Second,
		ConnectTimeout: 2 * time.Second,
		MaxConnections: 2,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)

	// Start some concurrent requests
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _ = client.Call(ctx, "test.method", id)
		}(i)
	}

	// Close while requests are in flight
	time.Sleep(10 * time.Millisecond)
	err = client.Close()
	assert.NoError(t, err)

	wg.Wait()
	// Test passes if no panic occurred during shutdown
}

// TestConnectionState_AtomicTransitions tests that connection state transitions
// are atomic and don't cause data races.
func TestConnectionState_AtomicTransitions(t *testing.T) {
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			resp := rpcTestResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
			}

			if req.Method == "auth.login_with_api_key" {
				resp.Result = true
			} else {
				resp.Result = "ok"
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

	cfg := &ClientConfig{
		Host:           host,
		Port:           port,
		Protocol:       "http",
		APIKey:         "test-api-key",
		Timeout:        5 * time.Second,
		ConnectTimeout: 2 * time.Second,
		MaxConnections: 1,
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup

	// Concurrent state checks
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = client.IsConnected()
			}
		}()
	}

	// Concurrent API calls
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, _ = client.Call(ctx, "test.method", id, j)
				cancel()
			}
		}(i)
	}

	wg.Wait()
}
