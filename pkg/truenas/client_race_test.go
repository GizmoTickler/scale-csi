package truenas

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
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

			_, callErr := client.Call(ctx, "slow.method", id)
			// Expect timeout error
			assert.Error(t, callErr)
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
// context is canceled during the request/write phase.
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

			_, callErr := client.Call(ctx, "test.method", id)
			// Expect context canceled error
			assert.Error(t, callErr)
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

func TestConnection_ConnectWaitHonorsContext(t *testing.T) {
	conn := NewConnection(0, &ClientConfig{})
	conn.connMu.Lock()
	atomic.StoreInt32(&conn.connState, int32(stateConnecting))
	conn.connectDone = make(chan struct{})
	conn.connMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := conn.connect(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), 250*time.Millisecond)
}

func TestConnection_ReconnectUnderLoad_SingleWriterGeneration(t *testing.T) {
	var connectionCount int32
	var successCount int64
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		connectionNumber := atomic.AddInt32(&connectionCount, 1)
		requestsOnConnection := 0
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}

			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			if req.Method == "auth.login_with_api_key" {
				resp.Result = true
			} else {
				requestsOnConnection++
				// Abruptly drop the first few generations while many callers are
				// sharing the writer. The httptest server continues accepting new
				// WebSocket connections for reconnects.
				if connectionNumber <= 4 && requestsOnConnection == 3 {
					_ = conn.UnderlyingConn().Close()
					return
				}
				resp.Result = "ok"
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

	host, port := testServerAddress(t, server.URL)
	client, err := NewClient(&ClientConfig{
		Host:                  host,
		Port:                  port,
		Protocol:              "http",
		APIKey:                "test-api-key",
		Timeout:               3 * time.Second,
		ConnectTimeout:        time.Second,
		WriteTimeout:          time.Second,
		MaxConnections:        1,
		MaxConcurrentReqs:     30,
		MaxRetries:            1,
		RetryInterval:         time.Millisecond,
		APIRetryMaxAttempts:   8,
		APIRetryInitialDelay:  time.Millisecond,
		APIRetryMaxDelay:      10 * time.Millisecond,
		APIRetryBackoffFactor: 2,
		HeartbeatInterval:     time.Hour,
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	for caller := 0; caller < 24; caller++ {
		wg.Add(1)
		go func(caller int) {
			defer wg.Done()
			for request := 0; request < 8; request++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				result, callErr := client.Call(ctx, "pool.dataset.query", caller, request)
				cancel()
				if callErr == nil && result == "ok" {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(caller)
	}
	wg.Wait()

	assert.GreaterOrEqual(t, atomic.LoadInt32(&connectionCount), int32(5))
	assert.Greater(t, atomic.LoadInt64(&successCount), int64(96), "most calls should survive repeated reconnects")
}

func TestClient_CircuitBreakerCountsLogicalCalls(t *testing.T) {
	var queryCount int32
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req.Method == "auth.login_with_api_key" {
				if err := conn.WriteJSON(rpcTestResponse{JSONRPC: "2.0", ID: req.ID, Result: true}); err != nil {
					return
				}
				continue
			}
			atomic.AddInt32(&queryCount, 1)
			_ = conn.UnderlyingConn().Close()
			return
		}
	})
	defer mock.close()

	host, port := testServerAddress(t, server.URL)
	client, err := NewClient(&ClientConfig{
		Host:                 host,
		Port:                 port,
		Protocol:             "http",
		APIKey:               "test-api-key",
		Timeout:              time.Second,
		ConnectTimeout:       time.Second,
		WriteTimeout:         time.Second,
		MaxConnections:       1,
		MaxRetries:           1,
		RetryInterval:        time.Millisecond,
		APIRetryMaxAttempts:  3,
		APIRetryInitialDelay: time.Millisecond,
		APIRetryMaxDelay:     time.Millisecond,
		CircuitBreaker: &CircuitBreakerConfig{
			Enabled:             true,
			FailureThreshold:    2,
			SuccessThreshold:    1,
			Timeout:             time.Hour,
			HalfOpenMaxRequests: 1,
		},
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.Call(ctx, "pool.dataset.query")
	cancel()
	require.Error(t, err)
	stats := client.CircuitBreakerStats()
	require.NotNil(t, stats)
	assert.Equal(t, int64(1), stats.TotalFailures)
	assert.Equal(t, CircuitClosed, stats.State)
	assert.Equal(t, int32(3), atomic.LoadInt32(&queryCount), "one logical call should use all configured attempts")

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.Call(ctx, "pool.dataset.query")
	cancel()
	require.Error(t, err)
	stats = client.CircuitBreakerStats()
	assert.Equal(t, int64(2), stats.TotalFailures)
	assert.Equal(t, CircuitOpen, stats.State)
	assert.Equal(t, int32(6), atomic.LoadInt32(&queryCount), "the breaker must not count individual attempts as failures")
}

func TestClient_CircuitBreakerHalfOpenRetryDoesNotWedge(t *testing.T) {
	var queryCount int32
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req.Method == "auth.login_with_api_key" {
				if err := conn.WriteJSON(rpcTestResponse{JSONRPC: "2.0", ID: req.ID, Result: true}); err != nil {
					return
				}
				continue
			}

			requestNumber := atomic.AddInt32(&queryCount, 1)
			// The first logical call opens the breaker. The first half-open
			// logical call then needs all three attempts before TrueNAS recovers.
			if requestNumber <= 5 {
				_ = conn.UnderlyingConn().Close()
				return
			}
			if err := conn.WriteJSON(rpcTestResponse{JSONRPC: "2.0", ID: req.ID, Result: "ok"}); err != nil {
				return
			}
		}
	})
	defer mock.close()

	host, port := testServerAddress(t, server.URL)
	client, err := NewClient(&ClientConfig{
		Host:                 host,
		Port:                 port,
		Protocol:             "http",
		APIKey:               "test-api-key",
		Timeout:              time.Second,
		ConnectTimeout:       time.Second,
		WriteTimeout:         time.Second,
		MaxConnections:       1,
		MaxRetries:           1,
		RetryInterval:        time.Millisecond,
		APIRetryMaxAttempts:  3,
		APIRetryInitialDelay: time.Millisecond,
		APIRetryMaxDelay:     time.Millisecond,
		CircuitBreaker: &CircuitBreakerConfig{
			Enabled:             true,
			FailureThreshold:    1,
			SuccessThreshold:    2,
			Timeout:             10 * time.Millisecond,
			HalfOpenMaxRequests: 3,
		},
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.Call(ctx, "pool.dataset.query")
	cancel()
	require.Error(t, err)
	require.Equal(t, CircuitOpen, client.circuitBreaker.State())
	require.Eventually(t, func() bool {
		return time.Since(client.circuitBreaker.Stats().LastFailure) >= 10*time.Millisecond
	}, time.Second, time.Millisecond)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	result, err := client.Call(ctx, "pool.dataset.query")
	cancel()
	require.NoError(t, err)
	assert.Equal(t, "ok", result)
	assert.Equal(t, CircuitHalfOpen, client.circuitBreaker.State())

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	result, err = client.Call(ctx, "pool.dataset.query")
	cancel()
	require.NoError(t, err)
	assert.Equal(t, "ok", result)
	assert.Equal(t, CircuitClosed, client.circuitBreaker.State(), "a healthy follow-up probe must close the breaker")
	assert.Equal(t, int32(7), atomic.LoadInt32(&queryCount))
}

func TestClient_SnapshotPrefixReprobesAfterTransientFailure(t *testing.T) {
	var poolProbes int32
	var zfsProbes int32
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
			case "pool.snapshot.query":
				if atomic.AddInt32(&poolProbes, 1) == 1 {
					resp.Error = &rpcError{Code: -1, Message: "temporary outage"}
				} else {
					resp.Result = []interface{}{}
				}
			case "zfs.snapshot.query":
				atomic.AddInt32(&zfsProbes, 1)
				resp.Error = &rpcError{Code: -1, Message: "temporary outage"}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "method not found"}
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assert.Equal(t, "zfs.snapshot", client.detectSnapshotAPIPrefix(ctx))
	assert.Equal(t, "pool.snapshot", client.detectSnapshotAPIPrefix(ctx))
	assert.Equal(t, int32(2), atomic.LoadInt32(&poolProbes))
	assert.Equal(t, int32(1), atomic.LoadInt32(&zfsProbes))
}

func TestClient_DoesNotRetryAmbiguousMutation(t *testing.T) {
	var mutationCount int32
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req.Method == "auth.login_with_api_key" {
				_ = conn.WriteJSON(rpcTestResponse{JSONRPC: "2.0", ID: req.ID, Result: true})
				continue
			}
			atomic.AddInt32(&mutationCount, 1)
			_ = conn.UnderlyingConn().Close()
			return
		}
	})
	defer mock.close()

	host, port := testServerAddress(t, server.URL)
	client, err := NewClient(&ClientConfig{
		Host:                 host,
		Port:                 port,
		Protocol:             "http",
		APIKey:               "test-api-key",
		Timeout:              time.Second,
		ConnectTimeout:       time.Second,
		MaxConnections:       1,
		APIRetryMaxAttempts:  3,
		APIRetryInitialDelay: time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = client.Call(ctx, "pool.dataset.create", map[string]interface{}{"name": "tank/test"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrAmbiguousResult))
	assert.Equal(t, int32(1), atomic.LoadInt32(&mutationCount))
}

func TestClient_AmbiguousMutationWinsOverCanceledContext(t *testing.T) {
	var mutationCount int32
	mutationReceived := make(chan struct{})
	disconnect := make(chan struct{})
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req.Method == "auth.login_with_api_key" {
				if err := conn.WriteJSON(rpcTestResponse{JSONRPC: "2.0", ID: req.ID, Result: true}); err != nil {
					return
				}
				continue
			}
			atomic.AddInt32(&mutationCount, 1)
			close(mutationReceived)
			<-disconnect
			_ = conn.UnderlyingConn().Close()
			return
		}
	})
	defer mock.close()

	host, port := testServerAddress(t, server.URL)
	client, err := NewClient(&ClientConfig{
		Host:                 host,
		Port:                 port,
		Protocol:             "http",
		APIKey:               "test-api-key",
		Timeout:              time.Second,
		ConnectTimeout:       time.Second,
		MaxConnections:       1,
		APIRetryMaxAttempts:  3,
		APIRetryInitialDelay: time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, callErr := client.Call(ctx, "pool.dataset.create", map[string]interface{}{"name": "tank/test"})
		resultCh <- callErr
	}()

	select {
	case <-mutationReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("mutation was not sent")
	}
	cancel()
	close(disconnect)

	select {
	case err = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("mutation call did not return")
	}
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrAmbiguousResult)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(1), atomic.LoadInt32(&mutationCount), "ambiguous mutation must not be retried")
}

func testServerAddress(t *testing.T, serverURL string) (string, int) {
	t.Helper()
	parsed, err := url.Parse(serverURL)
	require.NoError(t, err)
	host, portText, err := net.SplitHostPort(parsed.Host)
	require.NoError(t, err)
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return host, port
}
