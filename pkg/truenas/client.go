// Package truenas provides a client for the TrueNAS Scale API.
package truenas

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"k8s.io/klog/v2"
)

// APIError represents an error from the TrueNAS API.
type APIError struct {
	Code    int
	Message string
	Data    interface{}
}

func (e *APIError) Error() string {
	return fmt.Sprintf("TrueNAS API error [%d]: %s", e.Code, e.Message)
}

// IsNotFoundError returns true if the error indicates a resource was not found.
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if apiErr, ok := err.(*APIError); ok {
		// Common "not found" error codes from TrueNAS
		return apiErr.Code == -1 || strings.Contains(strings.ToLower(apiErr.Message), "not found") ||
			strings.Contains(strings.ToLower(apiErr.Message), "does not exist")
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "not found") || strings.Contains(errStr, "does not exist")
}

// IsAlreadyExistsError returns true if the error indicates a resource already exists.
func IsAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	if apiErr, ok := err.(*APIError); ok {
		return strings.Contains(strings.ToLower(apiErr.Message), "already exists")
	}
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
}

// IsConnectionError returns true if the error indicates a connection problem.
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "refused") ||
		strings.Contains(errStr, "connection lost")
}

// ClientConfig holds the configuration for the TrueNAS client.
type ClientConfig struct {
	Host               string
	Port               int
	Protocol           string
	APIKey             string
	AllowInsecure      bool
	Timeout            time.Duration
	ConnectTimeout     time.Duration
	MaxRetries         int           // Maximum number of connection retries (default: 3)
	RetryInterval      time.Duration // Initial retry interval (default: 1s, exponential backoff applied)
	HeartbeatInterval  time.Duration // Interval for WebSocket heartbeat (default: 30s)
	MaxConnections     int           // Maximum number of concurrent connections (default: 5)
	MaxConcurrentReqs  int           // Maximum number of concurrent API requests (default: 10)
}

// writeRequest represents a request to be written to the WebSocket.
type writeRequest struct {
	data     interface{}
	resultCh chan error
}

// connectionState represents the current state of the connection.
type connectionState int32

const (
	stateDisconnected connectionState = iota
	stateConnecting
	stateConnected
)

// Connection represents a single WebSocket connection to TrueNAS.
type Connection struct {
	id            int
	config        *ClientConfig
	conn          *websocket.Conn
	mu            sync.RWMutex
	messageID     int64
	pending       map[int64]chan *rpcResponse
	pendingMu     sync.RWMutex
	authenticated bool
	closed        bool

	// Connection state management
	connState int32 // atomic connectionState
	connCond  *sync.Cond
	connMu    sync.Mutex

	// Write loop channel
	writeCh       chan writeRequest
	writeLoopDone chan struct{}

	// Heartbeat management
	heartbeatDone chan struct{}
	lastPong      int64 // atomic unix timestamp

	// Safe channel closure
	closeMu             sync.Mutex
	writeLoopDoneClosed bool
	heartbeatDoneClosed bool
}

// NewConnection creates a new connection instance.
func NewConnection(id int, cfg *ClientConfig) *Connection {
	c := &Connection{
		id:            id,
		config:        cfg,
		pending:       make(map[int64]chan *rpcResponse),
		writeCh:       make(chan writeRequest, 100),
		writeLoopDone: make(chan struct{}),
		heartbeatDone: make(chan struct{}),
	}
	c.connCond = sync.NewCond(&c.connMu)
	return c
}

// Client is a TrueNAS API client using WebSocket JSON-RPC 2.0 with connection pooling.
type Client struct {
	config    *ClientConfig
	pool      []*Connection
	next      uint64         // For round-robin selection
	semaphore chan struct{}  // Limits concurrent requests to prevent TrueNAS overload
}

// rpcRequest is a JSON-RPC 2.0 request.
type rpcRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int64         `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params,omitempty"`
}

// rpcResponse is a JSON-RPC 2.0 response.
type rpcResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int64       `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   *rpcError   `json:"error,omitempty"`
}

// rpcError is a JSON-RPC 2.0 error.
type rpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// NewClient creates a new TrueNAS API client.
func NewClient(cfg *ClientConfig) (*Client, error) {
	if cfg.Host == "" {
		return nil, fmt.Errorf("host is required")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("api key is required")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 60 * time.Second
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 1 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.Protocol == "" {
		cfg.Protocol = "https"
	}
	if cfg.Port == 0 {
		if cfg.Protocol == "https" {
			cfg.Port = 443
		} else {
			cfg.Port = 80
		}
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 5
	}
	if cfg.MaxConcurrentReqs == 0 {
		cfg.MaxConcurrentReqs = 10 // Limit concurrent requests to prevent overwhelming TrueNAS
	}

	client := &Client{
		config:    cfg,
		pool:      make([]*Connection, cfg.MaxConnections),
		semaphore: make(chan struct{}, cfg.MaxConcurrentReqs),
	}

	// Initialize connection pool
	for i := 0; i < cfg.MaxConnections; i++ {
		client.pool[i] = NewConnection(i, cfg)
	}

	// Connect initially (at least one connection)
	// We'll try to connect all, but only fail if ALL fail
	connected := 0
	var lastErr error
	var wg sync.WaitGroup

	var errMu sync.Mutex
	for _, conn := range client.pool {
		wg.Add(1)
		go func(c *Connection) {
			defer wg.Done()
			if err := c.connect(); err != nil {
				errMu.Lock()
				lastErr = err
				errMu.Unlock()
			}
		}(conn)
	}
	wg.Wait()

	// Check how many connected
	for _, conn := range client.pool {
		if conn.IsConnected() {
			connected++
		}
	}

	if connected == 0 {
		// Try one last time synchronously to get the error
		if err := client.pool[0].connect(); err != nil {
			return nil, fmt.Errorf("failed to establish any connections (last error: %v): %w", lastErr, err)
		}
	}

	return client, nil
}

// connect establishes the WebSocket connection and authenticates.
func (c *Connection) connect() error {
	// Fast path: already connected
	if atomic.LoadInt32(&c.connState) == int32(stateConnected) && c.conn != nil {
		return nil
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()

	// Double-check after acquiring lock
	currentState := connectionState(atomic.LoadInt32(&c.connState))

	switch currentState {
	case stateConnected:
		if c.conn != nil {
			return nil
		}
	case stateConnecting:
		for atomic.LoadInt32(&c.connState) == int32(stateConnecting) {
			c.connCond.Wait()
		}
		if atomic.LoadInt32(&c.connState) == int32(stateConnected) && c.conn != nil {
			return nil
		}
	}

	atomic.StoreInt32(&c.connState, int32(stateConnecting))

	err := c.connectWithRetry()

	if err != nil {
		atomic.StoreInt32(&c.connState, int32(stateDisconnected))
	} else {
		atomic.StoreInt32(&c.connState, int32(stateConnected))
	}

	c.connCond.Broadcast()
	return err
}

// connectWithRetry attempts to connect with exponential backoff retry.
func (c *Connection) connectWithRetry() error {
	wsScheme := "ws"
	if c.config.Protocol == "https" {
		wsScheme = "wss"
	}
	wsURL := fmt.Sprintf("%s://%s:%d/api/current", wsScheme, c.config.Host, c.config.Port)

	dialer := websocket.Dialer{
		HandshakeTimeout: c.config.ConnectTimeout,
	}
	if c.config.AllowInsecure {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	headers := http.Header{}
	headers.Set("User-Agent", "truenas-scale-csi")

	var lastErr error
	retryInterval := c.config.RetryInterval

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			klog.V(2).Infof("Conn %d: Retrying connection (attempt %d/%d) after %v", c.id, attempt, c.config.MaxRetries, retryInterval)
			c.connMu.Unlock()
			time.Sleep(retryInterval)
			c.connMu.Lock()
			if c.closed {
				return fmt.Errorf("connection closed during reconnection")
			}
			retryInterval *= 2
			if retryInterval > 30*time.Second {
				retryInterval = 30 * time.Second
			}
		}

		klog.V(2).Infof("Conn %d: Connecting to %s (attempt %d)", c.id, wsURL, attempt+1)

		conn, _, err := dialer.Dial(wsURL, headers)
		if err != nil {
			lastErr = fmt.Errorf("failed to connect: %w", err)
			continue
		}

		c.mu.Lock()
		c.conn = conn
		c.closed = false
		c.writeCh = make(chan writeRequest, 100)
		c.writeLoopDone = make(chan struct{})
		c.heartbeatDone = make(chan struct{})
		c.mu.Unlock()

		c.closeMu.Lock()
		c.writeLoopDoneClosed = false
		c.heartbeatDoneClosed = false
		c.closeMu.Unlock()

		go c.readMessages()
		go c.writeLoop()

		if err := c.authenticateDirect(); err != nil {
			c.cleanupConnection()
			lastErr = fmt.Errorf("authentication failed: %w", err)
			continue
		}

		c.mu.Lock()
		c.authenticated = true
		atomic.StoreInt64(&c.lastPong, time.Now().Unix())
		c.mu.Unlock()

		go c.heartbeatLoop()

		klog.Infof("Conn %d: Connected and authenticated", c.id)
		return nil
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", c.config.MaxRetries+1, lastErr)
}

// cleanupConnection closes the connection and stops goroutines.
func (c *Connection) cleanupConnection() {
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.authenticated = false
	c.mu.Unlock()

	c.closeMu.Lock()
	if !c.writeLoopDoneClosed {
		close(c.writeLoopDone)
		c.writeLoopDoneClosed = true
	}
	if !c.heartbeatDoneClosed {
		close(c.heartbeatDone)
		c.heartbeatDoneClosed = true
	}
	c.closeMu.Unlock()
}

// authenticateDirect performs API key authentication using direct write.
func (c *Connection) authenticateDirect() error {
	c.mu.Lock()
	c.messageID++
	id := c.messageID
	conn := c.conn
	c.mu.Unlock()

	if conn == nil {
		return fmt.Errorf("no connection")
	}

	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  "auth.login_with_api_key",
		Params:  []interface{}{c.config.APIKey},
	}

	respChan := make(chan *rpcResponse, 1)
	c.pendingMu.Lock()
	c.pending[id] = respChan
	c.pendingMu.Unlock()

	if err := conn.WriteJSON(req); err != nil {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return fmt.Errorf("failed to send auth request: %w", err)
	}

	select {
	case resp := <-respChan:
		if resp.Error != nil {
			return &APIError{
				Code:    resp.Error.Code,
				Message: resp.Error.Message,
				Data:    resp.Error.Data,
			}
		}
		success, ok := resp.Result.(bool)
		if !ok || !success {
			return fmt.Errorf("authentication returned unexpected result: %v", resp.Result)
		}
		return nil
	case <-time.After(c.config.Timeout):
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return fmt.Errorf("authentication timeout")
	}
}

// writeLoop handles all WebSocket writes.
func (c *Connection) writeLoop() {
	for {
		select {
		case <-c.writeLoopDone:
			return
		case req, ok := <-c.writeCh:
			if !ok {
				return
			}

			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			var err error
			if conn == nil {
				err = fmt.Errorf("no connection")
			} else {
				err = conn.WriteJSON(req.data)
			}

			select {
			case req.resultCh <- err:
			default:
			}
		}
	}
}

// heartbeatLoop sends periodic pings.
func (c *Connection) heartbeatLoop() {
	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.heartbeatDone:
			return
		case <-ticker.C:
			c.mu.RLock()
			conn := c.conn
			closed := c.closed
			c.mu.RUnlock()

			if closed || conn == nil {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := c.CallWithContext(ctx, "core.ping")
			cancel()

			if err != nil {
				klog.Warningf("Conn %d: Heartbeat ping failed: %v", c.id, err)
				lastPong := atomic.LoadInt64(&c.lastPong)
				if time.Since(time.Unix(lastPong, 0)) > c.config.HeartbeatInterval*3 {
					klog.Errorf("Conn %d: Connection appears dead, reconnecting", c.id)
					c.handleDisconnect()
					return
				}
			} else {
				atomic.StoreInt64(&c.lastPong, time.Now().Unix())
			}
		}
	}
}

// readMessages reads incoming WebSocket messages.
func (c *Connection) readMessages() {
	for {
		c.mu.RLock()
		conn := c.conn
		closed := c.closed
		c.mu.RUnlock()

		if closed || conn == nil {
			return
		}

		var resp rpcResponse
		if err := conn.ReadJSON(&resp); err != nil {
			c.mu.RLock()
			wasClosed := c.closed
			c.mu.RUnlock()
			if !wasClosed {
				klog.Errorf("Conn %d: WebSocket read error: %v", c.id, err)
			}
			c.handleDisconnect()
			return
		}

		c.pendingMu.Lock()
		if ch, ok := c.pending[resp.ID]; ok {
			ch <- &resp
			delete(c.pending, resp.ID)
		}
		c.pendingMu.Unlock()
	}
}

// handleDisconnect handles WebSocket disconnection.
func (c *Connection) handleDisconnect() {
	atomic.StoreInt32(&c.connState, int32(stateDisconnected))

	c.mu.Lock()
	c.authenticated = false
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}

	c.closeMu.Lock()
	if !c.writeLoopDoneClosed {
		close(c.writeLoopDone)
		c.writeLoopDoneClosed = true
	}
	if !c.heartbeatDoneClosed {
		close(c.heartbeatDone)
		c.heartbeatDoneClosed = true
	}
	c.closeMu.Unlock()

	c.pendingMu.Lock()
	for id, ch := range c.pending {
		select {
		case ch <- &rpcResponse{
			ID: id,
			Error: &rpcError{
				Code:    -1,
				Message: "connection lost",
			},
		}:
		default:
		}
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()

	c.connCond.Broadcast()
}

// CallWithContext makes a JSON-RPC call using this connection.
func (c *Connection) CallWithContext(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		if err := c.connect(); err != nil {
			return nil, err
		}
	}

	c.mu.Lock()
	c.messageID++
	id := c.messageID
	writeCh := c.writeCh
	c.mu.Unlock()

	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}

	respChan := make(chan *rpcResponse, 1)
	c.pendingMu.Lock()
	c.pending[id] = respChan
	c.pendingMu.Unlock()

	writeReq := writeRequest{
		data:     req,
		resultCh: make(chan error, 1),
	}

	select {
	case writeCh <- writeReq:
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, ctx.Err()
	}

	select {
	case err := <-writeReq.resultCh:
		if err != nil {
			c.pendingMu.Lock()
			delete(c.pending, id)
			c.pendingMu.Unlock()
			return nil, fmt.Errorf("failed to send request: %w", err)
		}
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, ctx.Err()
	}

	klog.V(4).Infof("Conn %d: Call: %s", c.id, method)

	select {
	case resp := <-respChan:
		if resp.Error != nil {
			return nil, &APIError{
				Code:    resp.Error.Code,
				Message: resp.Error.Message,
				Data:    resp.Error.Data,
			}
		}
		return resp.Result, nil
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return nil, fmt.Errorf("request timeout: %s", method)
	}
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	atomic.StoreInt32(&c.connState, int32(stateDisconnected))

	c.closeMu.Lock()
	if !c.writeLoopDoneClosed {
		close(c.writeLoopDone)
		c.writeLoopDoneClosed = true
	}
	if !c.heartbeatDoneClosed {
		close(c.heartbeatDone)
		c.heartbeatDoneClosed = true
	}
	c.closeMu.Unlock()

	c.connCond.Broadcast()

	if conn != nil {
		return conn.Close()
	}
	return nil
}

// IsConnected returns true if connected.
func (c *Connection) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil && c.authenticated && atomic.LoadInt32(&c.connState) == int32(stateConnected)
}

// Call makes a JSON-RPC call to the TrueNAS API using the connection pool.
func (c *Client) Call(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	return c.CallWithContext(ctx, method, params...)
}

// CallWithContext makes a JSON-RPC call with a context using the connection pool.
// Uses a semaphore to limit concurrent requests and prevent overwhelming TrueNAS.
// Implements automatic retry on connection errors with exponential backoff.
func (c *Client) CallWithContext(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	// Acquire semaphore slot (limit concurrent requests)
	select {
	case c.semaphore <- struct{}{}:
		// Got a slot, continue
	case <-ctx.Done():
		return nil, fmt.Errorf("context cancelled while waiting for request slot: %w", ctx.Err())
	}
	defer func() { <-c.semaphore }() // Release slot when done

	const maxRetries = 3
	var lastErr error
	retryDelay := 500 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Select best available connection
		conn := c.selectConnection()

		result, err := conn.CallWithContext(ctx, method, params...)
		if err == nil {
			return result, nil
		}

		// Check if error is retryable (connection-related)
		if !IsConnectionError(err) {
			// Not a connection error - return immediately (API errors, not found, etc.)
			return nil, err
		}

		lastErr = err
		klog.V(2).Infof("API call %s failed on conn %d (attempt %d/%d): %v", method, conn.id, attempt+1, maxRetries, err)

		// Don't retry on last attempt or if context is done
		if attempt < maxRetries-1 {
			select {
			case <-time.After(retryDelay):
				retryDelay *= 2 // Exponential backoff
				if retryDelay > 5*time.Second {
					retryDelay = 5 * time.Second
				}
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			}
		}
	}

	return nil, fmt.Errorf("API call %s failed after %d retries: %w", method, maxRetries, lastErr)
}

// selectConnection selects the best available connection from the pool.
// Prefers connected connections and uses round-robin for load balancing.
func (c *Client) selectConnection() *Connection {
	poolSize := uint64(len(c.pool))

	// Try to find a connected connection using round-robin
	startIdx := atomic.AddUint64(&c.next, 1) % poolSize
	for i := uint64(0); i < poolSize; i++ {
		idx := (startIdx + i) % poolSize
		conn := c.pool[idx]
		if conn.IsConnected() {
			return conn
		}
	}

	// No connected connections - return the round-robin selection
	// The connection will attempt to reconnect when used
	return c.pool[startIdx]
}

// Close closes all connections in the pool.
func (c *Client) Close() error {
	var lastErr error
	for _, conn := range c.pool {
		if err := conn.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// IsConnected returns true if at least one connection is active.
func (c *Client) IsConnected() bool {
	for _, conn := range c.pool {
		if conn.IsConnected() {
			return true
		}
	}
	return false
}

// ServiceReload reloads a TrueNAS service configuration.
// This is useful for forcing services like iSCSI to pick up new configuration
// after creating targets/extents via the API.
// Common service names: "iscsitarget", "nfs", "cifs", "nvmeof"
//
// NOTE: This only attempts reload (not restart) to avoid disrupting existing
// sessions. If reload fails, we log and continue - the caller should have
// retry logic to handle propagation delays.
func (c *Client) ServiceReload(ctx context.Context, service string) error {
	klog.V(4).Infof("Reloading service: %s", service)
	_, err := c.Call(ctx, "service.reload", service)
	if err != nil {
		// Don't try restart - it could disrupt existing sessions/connections.
		// Just log and continue; the node-side retry logic will handle propagation delays.
		klog.V(4).Infof("service.reload failed for %s (this is often normal, target may still propagate): %v", service, err)
		return nil
	}
	klog.Infof("Service %s reloaded successfully", service)
	return nil
}
