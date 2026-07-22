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

// TestConnectionLostBeforeSendIsTransportFailure covers the callWithGeneration
// guard (client.go:1052). A stale generation means the request never reached the
// wire; the error must wrap ErrTransportFailure so IsConnectionError classifies
// it as retryable transport failure rather than a hard API error.
func TestConnectionLostBeforeSendIsTransportFailure(t *testing.T) {
	conn := NewConnection(0, &ClientConfig{})

	// Passing a generation that does not match the connection's current
	// generation trips the pre-send guard.
	_, err := conn.callWithGeneration(context.Background(), 999, false, "pool.dataset.query")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTransportFailure)
	assert.True(t, IsConnectionError(err), "a pre-send connection loss must be a connection error")
}

// forceAuthLossDisconnect simulates the connection dying the instant
// authentication completes: it marks the generation stopped and releases the
// write loop so the post-auth check in connectWithRetry observes a dead
// connection. The caller must invoke this after the auth request has been sent
// (guard already passed) but before the post-auth generation check runs.
func forceAuthLossDisconnect(conn *Connection) {
	conn.mu.Lock()
	conn.stopped = true
	writeDone := conn.writeLoopDone
	conn.mu.Unlock()
	if writeDone != nil {
		close(writeDone)
	}
}

// startAuthLossServer starts a mock WebSocket server that reads the auth
// request, waits for the test to force a disconnect, then writes a successful
// auth response and closes. This deterministically drives connectWithRetry into
// its "connection lost during authentication" branch (client.go:747).
func startAuthLossServer(t *testing.T, authRead, release chan struct{}) *mockWSServer {
	t.Helper()
	mock := newMockWSServer()
	mock.start(func(conn *websocket.Conn) {
		var req rpcTestRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}
		if req.Method != "auth.login_with_api_key" {
			return
		}
		authRead <- struct{}{}
		<-release
		_ = conn.WriteJSON(rpcTestResponse{JSONRPC: "2.0", ID: req.ID, Result: true})
	})
	return mock
}

// TestConnectWithRetryAuthLossIsTransportFailure covers the connect/auth path
// (client.go:747). Losing the connection during authentication is a pre-request
// failure and must wrap ErrTransportFailure.
func TestConnectWithRetryAuthLossIsTransportFailure(t *testing.T) {
	authRead := make(chan struct{})
	release := make(chan struct{})
	mock := startAuthLossServer(t, authRead, release)
	defer mock.close()

	host, port := testServerAddress(t, mock.server.URL)
	conn := NewConnection(0, &ClientConfig{
		Host:           host,
		Port:           port,
		Protocol:       "http",
		APIKey:         "test-api-key",
		Timeout:        2 * time.Second,
		ConnectTimeout: time.Second,
		RetryInterval:  time.Millisecond,
		MaxRetries:     0,
	})

	errCh := make(chan error, 1)
	go func() { errCh <- conn.connectWithRetry(context.Background()) }()

	// The auth request has been written (guard passed); now drop the connection
	// before the post-auth generation check runs.
	<-authRead
	forceAuthLossDisconnect(conn)
	close(release)

	err := <-errCh
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTransportFailure)
	assert.True(t, IsConnectionError(err), "an auth-phase connection loss must be a connection error")
}

// TestClient_AuthLossRetriesAndRecordsBreakerFailure proves the Call-loop
// behavior change: a connection loss that previously escaped as a bare error
// (no retry + circuitBreaker.RecordSuccess) is now retried and records a breaker
// failure. Before the fix this made exactly one attempt and recorded a success;
// after the fix it exhausts APIRetryMaxAttempts and records no successes.
func TestClient_AuthLossRetriesAndRecordsBreakerFailure(t *testing.T) {
	const apiAttempts = 3

	authRead := make(chan struct{}, apiAttempts)
	release := make(chan struct{}, apiAttempts)
	var authCount atomic.Int32
	mock := newMockWSServer()
	mock.start(func(conn *websocket.Conn) {
		var req rpcTestRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}
		if req.Method != "auth.login_with_api_key" {
			return
		}
		authCount.Add(1)
		authRead <- struct{}{}
		<-release
		_ = conn.WriteJSON(rpcTestResponse{JSONRPC: "2.0", ID: req.ID, Result: true})
	})
	defer mock.close()

	host, port := testServerAddress(t, mock.server.URL)
	cfg := &ClientConfig{
		Host:                  host,
		Port:                  port,
		Protocol:              "http",
		APIKey:                "test-api-key",
		Timeout:               2 * time.Second,
		ConnectTimeout:        time.Second,
		RetryInterval:         time.Millisecond,
		MaxRetries:            0,
		APIRetryMaxAttempts:   apiAttempts,
		APIRetryInitialDelay:  time.Millisecond,
		APIRetryMaxDelay:      time.Millisecond,
		APIRetryBackoffFactor: 1,
	}
	conn := NewConnection(0, cfg)

	breaker := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    10,
		SuccessThreshold:    1,
		Timeout:             time.Hour,
		HalfOpenMaxRequests: 1,
	})
	client := &Client{
		config:         cfg,
		pool:           []*Connection{conn},
		semaphore:      make(chan struct{}, 1),
		circuitBreaker: breaker,
	}

	// For every connect attempt, drop the connection during authentication.
	go func() {
		for range apiAttempts {
			<-authRead
			forceAuthLossDisconnect(conn)
			release <- struct{}{}
		}
	}()

	_, err := client.Call(context.Background(), "pool.dataset.query")
	require.Error(t, err)
	assert.True(t, IsConnectionError(err), "the exhausted call must surface a connection error")
	assert.Equal(t, int32(apiAttempts), authCount.Load(), "the connection error must be retried across all attempts")

	stats := client.CircuitBreakerStats()
	require.NotNil(t, stats)
	assert.Equal(t, int64(0), stats.TotalSuccesses, "a flapping backend must not be recorded as a success")
	assert.GreaterOrEqual(t, stats.TotalFailures, int64(1), "the exhausted call must record a breaker failure")
}
