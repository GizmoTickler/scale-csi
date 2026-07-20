// Package truenas provides a client for the TrueNAS Scale API.
package truenas

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
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

// FullError returns a detailed error string including Data field for debugging.
// Use this when logging errors before fallback logic to capture full context.
func (e *APIError) FullError() string {
	if e.Data == nil {
		return fmt.Sprintf("TrueNAS API error [%d]: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("TrueNAS API error [%d]: %s (data: %+v)", e.Code, e.Message, e.Data)
}

// LogAPIError logs full error details for debugging. This is useful before
// fallback logic that may mask the original error.
func LogAPIError(err error, ctx string) {
	if err == nil {
		return
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		klog.V(4).Infof("%s: %s", ctx, apiErr.FullError())
	} else {
		klog.V(4).Infof("%s: %v", ctx, err)
	}
}

// IsNotFoundError returns true if the error indicates a resource was not found.
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return strings.Contains(strings.ToLower(apiErr.Message), "not found") ||
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
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return strings.Contains(strings.ToLower(apiErr.Message), "already exists")
	}
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
}

// IsConnectionError returns true if the error indicates a connection problem.
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrTransportFailure) || errors.Is(err, ErrAmbiguousResult) {
		return true
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "refused") ||
		strings.Contains(errStr, "connection lost")
}

// ErrAmbiguousResult indicates that a request was written successfully, but the
// connection was lost before a response arrived. The server may have applied
// the request, so callers must decide whether it is safe to retry.
var ErrAmbiguousResult = errors.New("request result is ambiguous")

// ErrTransportFailure indicates that a request failed in the WebSocket
// transport before a response was received. Unlike ErrAmbiguousResult, it does
// not imply that the server may have applied the request.
var ErrTransportFailure = errors.New("request transport failed")

const defaultWriteTimeout = 30 * time.Second

// MetricsRecorder is a callback for recording API request metrics.
// method is the API method name, duration is in seconds, err is nil on success.
type MetricsRecorder func(method string, duration float64, err error)

// SystemInfo represents TrueNAS system information including version.
type SystemInfo struct {
	Version       string `json:"version"` // Full version string (e.g., "TrueNAS-SCALE-25.10.0")
	VersionMajor  int    // Major version (e.g., 25)
	VersionMinor  int    // Minor version (e.g., 10)
	VersionPatch  int    // Patch version (e.g., 0)
	Hostname      string `json:"hostname"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	SystemProduct string `json:"system_product"`
	SystemSerial  string `json:"system_serial"`
}

// ClientConfig holds the configuration for the TrueNAS client.
type ClientConfig struct {
	Host              string
	Port              int
	Protocol          string
	APIKey            string
	AllowInsecure     bool
	CACert            string
	CACertFile        string
	Timeout           time.Duration
	ConnectTimeout    time.Duration
	WriteTimeout      time.Duration   // Timeout for each WebSocket write (default: 30s)
	MaxRetries        int             // Maximum number of connection retries (default: 3)
	RetryInterval     time.Duration   // Initial retry interval (default: 1s, exponential backoff applied)
	HeartbeatInterval time.Duration   // Interval for WebSocket heartbeat (default: 30s)
	MaxConnections    int             // Maximum number of concurrent connections (default: 5)
	MaxConcurrentReqs int             // Maximum number of concurrent API requests (default: 10)
	LazyConnect       bool            // Skip eager connection; connect on first API use (node-only mode)
	MetricsRecorder   MetricsRecorder // Optional callback for recording request metrics

	// Circuit breaker configuration
	CircuitBreaker *CircuitBreakerConfig

	// Retry configuration for API calls
	APIRetryMaxAttempts   int           // Maximum retry attempts for API calls (default: 3)
	APIRetryInitialDelay  time.Duration // Initial delay between retries (default: 500ms)
	APIRetryMaxDelay      time.Duration // Maximum delay between retries (default: 5s)
	APIRetryBackoffFactor float64       // Backoff multiplier (default: 2.0)
}

// writeRequest represents a request to be written to the WebSocket.
type writeRequest struct {
	id       int64
	data     interface{}
	resultCh chan error
}

type pendingCall struct {
	responseCh chan *rpcResponse
	generation uint64
	method     string
	sent       bool
}

type generationHandles struct {
	generation    uint64
	writeCh       chan writeRequest
	writeDone     chan struct{}
	heartbeatDone chan struct{}
	waitGroup     *sync.WaitGroup
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
	id                  int
	config              *ClientConfig
	conn                atomic.Pointer[websocket.Conn] // atomic for lock-free reads
	mu                  sync.RWMutex
	messageID           int64
	pending             map[int64]*pendingCall
	pendingMu           sync.RWMutex
	authenticated       bool
	generation          uint64
	stopped             bool
	generationWG        *sync.WaitGroup
	sendMu              sync.Mutex
	shutdown            atomic.Bool
	insecureWarningOnce *sync.Once

	// Connection state management
	connState   int32 // atomic connectionState
	connMu      sync.Mutex
	connectDone chan struct{}

	// Write loop channel
	writeCh       chan writeRequest
	writeLoopDone chan struct{}

	// Heartbeat management
	heartbeatDone chan struct{}
	lastPong      int64 // atomic unix timestamp

}

// NewConnection creates a new connection instance.
func NewConnection(id int, cfg *ClientConfig) *Connection {
	c := &Connection{
		id:                  id,
		config:              cfg,
		pending:             make(map[int64]*pendingCall),
		stopped:             true,
		insecureWarningOnce: &sync.Once{},
	}
	return c
}

// Client is a TrueNAS API client using WebSocket JSON-RPC 2.0 with connection pooling.
type Client struct {
	config          *ClientConfig
	pool            []*Connection
	next            uint64          // For round-robin selection
	semaphore       chan struct{}   // Limits concurrent requests to prevent TrueNAS overload
	metricsRecorder MetricsRecorder // Optional callback for recording request metrics
	circuitBreaker  *CircuitBreaker // Optional circuit breaker for API calls

	// NVMe-oF ports are shared resources. Serialize find-or-create and cache
	// successful resolutions so concurrent volume creates cannot race.
	nvmePortMu       sync.Mutex
	nvmeResolvedPort map[string]*NVMeoFPort

	// Version detection cache
	versionMu    sync.RWMutex
	versionCache *SystemInfo

	// Snapshot API prefix detection cache. This is per-client because different
	// clients may connect to different TrueNAS versions.
	snapshotPrefixMu        sync.Mutex
	snapshotAPIPrefix       string
	snapshotPrefixProbeDone chan struct{}

	// TrueNAS 26.0 introduced zfs.resource.snapshot.query, which is the only
	// snapshot read API that still exposes user properties. Keep this detection
	// separate from the mutation API prefix because create/update/delete remain
	// under pool.snapshot.*, while rename also moved to zfs.resource.snapshot.*.
	snapshotResourceMu        sync.Mutex
	snapshotResourceAvailable bool
	snapshotResourceDetected  bool
	snapshotResourceProbeDone chan struct{}

	// Snapshot create-property support is probed independently for each mutation
	// API generation. The mutex also serializes the first probe so concurrent
	// creates do not all retry a rejected payload shape.
	snapshotCreatePropertiesMu      sync.Mutex
	snapshotCreatePropertiesSupport map[string]bool
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
	JSONRPC      string      `json:"jsonrpc"`
	ID           int64       `json:"id"`
	Result       interface{} `json:"result,omitempty"`
	Error        *rpcError   `json:"error,omitempty"`
	transportErr error
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
	if cfg.Port < 0 {
		return nil, fmt.Errorf("port must not be negative")
	}
	if cfg.Timeout < 0 || cfg.ConnectTimeout < 0 || cfg.WriteTimeout < 0 || cfg.RetryInterval < 0 || cfg.HeartbeatInterval < 0 ||
		cfg.APIRetryInitialDelay < 0 || cfg.APIRetryMaxDelay < 0 {
		return nil, fmt.Errorf("client timeouts and retry delays must not be negative")
	}
	if cfg.MaxConnections < 0 || cfg.MaxConcurrentReqs < 0 || cfg.MaxRetries < 0 || cfg.APIRetryMaxAttempts < 0 {
		return nil, fmt.Errorf("client connection, concurrency, and retry counts must not be negative")
	}
	if cfg.APIRetryBackoffFactor < 0 {
		return nil, fmt.Errorf("API retry backoff factor must not be negative")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 60 * time.Second
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = defaultWriteTimeout
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

	// API retry defaults
	if cfg.APIRetryMaxAttempts == 0 {
		cfg.APIRetryMaxAttempts = 3
	}
	if cfg.APIRetryInitialDelay == 0 {
		cfg.APIRetryInitialDelay = 500 * time.Millisecond
	}
	if cfg.APIRetryMaxDelay == 0 {
		cfg.APIRetryMaxDelay = 5 * time.Second
	}
	if cfg.APIRetryBackoffFactor == 0 {
		cfg.APIRetryBackoffFactor = 2.0
	}

	// Initialize circuit breaker
	var cb *CircuitBreaker
	if cfg.CircuitBreaker != nil && cfg.CircuitBreaker.Enabled {
		cb = NewCircuitBreaker(cfg.CircuitBreaker)
		klog.Infof("Circuit breaker enabled: failureThreshold=%d, successThreshold=%d, timeout=%v",
			cb.config.FailureThreshold, cb.config.SuccessThreshold, cb.config.Timeout)
	}

	client := &Client{
		config:           cfg,
		pool:             make([]*Connection, cfg.MaxConnections),
		semaphore:        make(chan struct{}, cfg.MaxConcurrentReqs),
		metricsRecorder:  cfg.MetricsRecorder,
		circuitBreaker:   cb,
		nvmeResolvedPort: make(map[string]*NVMeoFPort),
	}

	// Initialize connection pool
	insecureWarningOnce := &sync.Once{}
	for i := 0; i < cfg.MaxConnections; i++ {
		client.pool[i] = NewConnection(i, cfg)
		client.pool[i].insecureWarningOnce = insecureWarningOnce
	}
	if cfg.LazyConnect {
		klog.Infof("TrueNAS client initialized in lazy-connect mode")
		return client, nil
	}

	// Connect initially (at least one connection)
	// We'll try to connect all, but only fail if ALL fail
	// Stagger connection attempts to avoid thundering herd on TrueNAS
	connected := 0
	var lastErr error
	var wg sync.WaitGroup

	var errMu sync.Mutex
	for i, conn := range client.pool {
		wg.Add(1)
		go func(c *Connection, idx int) {
			defer wg.Done()
			// Stagger connections: add 50-150ms jitter per connection to avoid thundering herd
			if idx > 0 {
				jitter := time.Duration(50+rand.Intn(100)) * time.Millisecond
				time.Sleep(time.Duration(idx) * jitter)
			}
			if err := c.connect(context.Background()); err != nil {
				errMu.Lock()
				lastErr = err
				errMu.Unlock()
			}
		}(conn, i)
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
		if err := client.pool[0].connect(context.Background()); err != nil {
			if lastErr == nil {
				return nil, fmt.Errorf("failed to establish any connections: %w", err)
			}
			return nil, fmt.Errorf("failed to establish any connections (previous: %s): %w", lastErr.Error(), err)
		}
	}

	return client, nil
}

// connect establishes the WebSocket connection and authenticates. Concurrent
// callers wait on the in-flight connection attempt without losing cancellation.
func (c *Connection) connect(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.shutdown.Load() {
			return fmt.Errorf("connection closed")
		}
		if atomic.LoadInt32(&c.connState) == int32(stateConnected) && c.hasAuthenticatedConnection() {
			return nil
		}

		c.connMu.Lock()
		switch connectionState(atomic.LoadInt32(&c.connState)) {
		case stateConnected:
			if c.hasAuthenticatedConnection() {
				c.connMu.Unlock()
				return nil
			}
			atomic.StoreInt32(&c.connState, int32(stateDisconnected))
		case stateConnecting:
			done := c.connectDone
			c.connMu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		atomic.StoreInt32(&c.connState, int32(stateConnecting))
		done := make(chan struct{})
		c.connectDone = done
		c.connMu.Unlock()

		err := c.connectWithRetry(ctx)
		c.connMu.Lock()
		c.mu.RLock()
		connected := !c.stopped && c.conn.Load() != nil && c.authenticated
		if err == nil && !connected {
			err = fmt.Errorf("connection lost during authentication")
		}
		if err != nil {
			atomic.StoreInt32(&c.connState, int32(stateDisconnected))
		} else {
			atomic.StoreInt32(&c.connState, int32(stateConnected))
		}
		c.mu.RUnlock()
		close(done)
		c.connectDone = nil
		c.connMu.Unlock()
		return err
	}
}

func (c *Connection) hasAuthenticatedConnection() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.stopped && c.conn.Load() != nil && c.authenticated
}

// connectWithRetry attempts to connect with exponential backoff retry.
func (c *Connection) connectWithRetry(ctx context.Context) error {
	wsScheme := "ws"
	if c.config.Protocol == "https" {
		wsScheme = "wss"
	}
	wsURL := fmt.Sprintf("%s://%s:%d/api/current", wsScheme, c.config.Host, c.config.Port)

	dialer := websocket.Dialer{
		HandshakeTimeout: c.config.ConnectTimeout,
	}
	if c.config.Protocol == "https" {
		tlsConfig, err := buildTLSConfig(c.config)
		if err != nil {
			return fmt.Errorf("failed to configure TrueNAS TLS: %w", err)
		}
		dialer.TLSClientConfig = tlsConfig
	}
	if c.config.AllowInsecure {
		c.insecureWarningOnce.Do(func() {
			klog.Warningf("TLS verification DISABLED for TrueNAS %s — man-in-the-middle possible; set truenas.caCert for secure verification", c.config.Host)
		})
	}

	headers := http.Header{}
	headers.Set("User-Agent", "truenas-scale-csi")

	var lastErr error
	retryInterval := c.config.RetryInterval

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if c.shutdown.Load() {
			return fmt.Errorf("connection closed")
		}
		if attempt > 0 {
			klog.V(2).Infof("Conn %d: Retrying connection (attempt %d/%d) after %v", c.id, attempt, c.config.MaxRetries, retryInterval)
			timer := time.NewTimer(retryInterval)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return ctx.Err()
			}
			retryInterval *= 2
			if retryInterval > 30*time.Second {
				retryInterval = 30 * time.Second
			}
		}

		klog.V(2).Infof("Conn %d: Connecting to %s (attempt %d)", c.id, wsURL, attempt+1)

		wsConn, _, err := dialer.DialContext(ctx, wsURL, headers)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = fmt.Errorf("failed to connect: %w", err)
			continue
		}

		handles, err := c.startGeneration(wsConn)
		if err != nil {
			_ = wsConn.Close()
			return err
		}
		go c.readMessages(handles.generation, wsConn, handles.waitGroup)
		go c.writeLoop(handles.generation, wsConn, handles.writeCh, handles.writeDone, handles.waitGroup)

		authCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
		err = c.authenticate(authCtx, handles.generation)
		cancel()
		if err != nil {
			c.cleanupConnection(handles.generation, err)
			handles.waitGroup.Wait()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = fmt.Errorf("authentication failed: %w", err)
			continue
		}

		c.mu.Lock()
		if handles.generation != c.generation || c.stopped {
			c.mu.Unlock()
			c.cleanupConnection(handles.generation, fmt.Errorf("connection lost during authentication"))
			handles.waitGroup.Wait()
			lastErr = fmt.Errorf("connection lost during authentication")
			continue
		}
		c.authenticated = true
		handles.waitGroup.Add(1)
		c.mu.Unlock()
		atomic.StoreInt64(&c.lastPong, time.Now().Unix())

		go c.heartbeatLoop(handles.generation, handles.heartbeatDone, handles.waitGroup)

		klog.Infof("Conn %d: Connected and authenticated", c.id)
		return nil
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", c.config.MaxRetries+1, lastErr)
}

func buildTLSConfig(cfg *ClientConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.AllowInsecure {
		tlsConfig.InsecureSkipVerify = true //nolint:gosec // Explicit operator opt-out, warned once at connect.
		return tlsConfig, nil
	}

	var caPEM []byte
	if cfg.CACert != "" {
		caPEM = append(caPEM, cfg.CACert...)
		caPEM = append(caPEM, '\n')
	}
	if cfg.CACertFile != "" {
		filePEM, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("read CA certificate file %q: %w", cfg.CACertFile, err)
		}
		caPEM = append(caPEM, filePEM...)
	}
	if len(caPEM) == 0 {
		return tlsConfig, nil
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA certificate does not contain a valid PEM certificate")
	}
	tlsConfig.RootCAs = pool
	return tlsConfig, nil
}

func (c *Connection) startGeneration(wsConn *websocket.Conn) (generationHandles, error) {
	c.mu.RLock()
	previousWG := c.generationWG
	c.mu.RUnlock()
	if previousWG != nil {
		previousWG.Wait()
	}

	c.mu.Lock()
	if c.shutdown.Load() {
		c.mu.Unlock()
		return generationHandles{}, fmt.Errorf("connection closed")
	}
	c.generation++
	generation := c.generation
	writeCh := make(chan writeRequest, 100)
	writeDone := make(chan struct{})
	heartbeatDone := make(chan struct{})
	generationWG := &sync.WaitGroup{}
	generationWG.Add(2)
	c.conn.Store(wsConn)
	c.writeCh = writeCh
	c.writeLoopDone = writeDone
	c.heartbeatDone = heartbeatDone
	c.generationWG = generationWG
	c.authenticated = false
	c.stopped = false
	c.mu.Unlock()
	return generationHandles{
		generation:    generation,
		writeCh:       writeCh,
		writeDone:     writeDone,
		heartbeatDone: heartbeatDone,
		waitGroup:     generationWG,
	}, nil
}

// cleanupConnection closes one connection generation and stops its goroutines.
func (c *Connection) cleanupConnection(generation uint64, cause error) {
	if !c.stopGeneration(generation) {
		return
	}
	c.failPending(generation, cause)
}

func (c *Connection) stopGeneration(generation uint64) bool {
	c.mu.Lock()
	if generation != c.generation || c.stopped {
		c.mu.Unlock()
		return false
	}
	c.stopped = true
	c.authenticated = false
	conn := c.conn.Swap(nil)
	close(c.writeLoopDone)
	close(c.heartbeatDone)
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	return true
}

func (c *Connection) authenticate(ctx context.Context, generation uint64) error {
	result, err := c.callWithGeneration(ctx, generation, true, "auth.login_with_api_key", c.config.APIKey)
	if err != nil {
		return fmt.Errorf("failed to send auth request: %w", err)
	}
	success, ok := result.(bool)
	if !ok || !success {
		return fmt.Errorf("authentication returned unexpected result: %v", result)
	}
	return nil
}

// writeLoop handles all WebSocket writes. Invariant: this is the only function
// that calls WriteJSON, including authentication and heartbeat requests.
func (c *Connection) writeLoop(generation uint64, conn *websocket.Conn, writeCh <-chan writeRequest, done <-chan struct{}, generationWG *sync.WaitGroup) {
	defer generationWG.Done()
	for {
		select {
		case <-done:
			return
		case req, ok := <-writeCh:
			if !ok {
				return
			}

			c.sendMu.Lock()
			writeTimeout := c.config.WriteTimeout
			if writeTimeout <= 0 {
				writeTimeout = defaultWriteTimeout
			}
			var err = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err == nil {
				err = conn.WriteJSON(req.data)
			}
			if err == nil {
				c.pendingMu.Lock()
				if pending, ok := c.pending[req.id]; ok && pending.generation == generation {
					pending.sent = true
				}
				c.pendingMu.Unlock()
			}
			c.sendMu.Unlock()

			select {
			case req.resultCh <- err:
			default:
			}
			if err != nil {
				c.handleDisconnect(generation, fmt.Errorf("websocket write failed: %w", err))
				return
			}
		}
	}
}

// heartbeatLoop sends periodic pings.
func (c *Connection) heartbeatLoop(generation uint64, done <-chan struct{}, generationWG *sync.WaitGroup) {
	defer generationWG.Done()
	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if !c.isGenerationActive(generation) {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := c.callWithGeneration(ctx, generation, false, "core.ping")
			cancel()

			if err != nil {
				klog.Warningf("Conn %d: Heartbeat ping failed: %v", c.id, err)
				lastPong := atomic.LoadInt64(&c.lastPong)
				if time.Since(time.Unix(lastPong, 0)) > c.config.HeartbeatInterval*3 {
					klog.Errorf("Conn %d: Connection appears dead, reconnecting", c.id)
					c.handleDisconnect(generation, err)
					return
				}
			} else {
				atomic.StoreInt64(&c.lastPong, time.Now().Unix())
			}
		}
	}
}

func (c *Connection) isGenerationActive(generation uint64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return generation == c.generation && !c.stopped && c.conn.Load() != nil
}

// readMessages reads incoming WebSocket messages.
// Uses a read deadline to prevent goroutine leaks when the connection dies without error.
func (c *Connection) readMessages(generation uint64, conn *websocket.Conn, generationWG *sync.WaitGroup) {
	defer generationWG.Done()
	// Recover from gorilla/websocket panics (e.g., "repeated read on failed websocket connection")
	// This can happen if the connection fails between timeout checks
	defer func() {
		if r := recover(); r != nil {
			klog.Warningf("Conn %d: Recovered from WebSocket panic: %v", c.id, r)
			c.handleDisconnect(generation, fmt.Errorf("websocket read panic: %v", r))
		}
	}()

	// Read deadline allows us to periodically check if the connection should be closed.
	// Must be longer than HeartbeatInterval (default 30s) to give heartbeat time to fire
	// and get a response before the read deadline expires.
	const readDeadlineInterval = 45 * time.Second

	for {
		if !c.isGenerationActive(generation) {
			return
		}

		// Set read deadline to prevent blocking forever on a dead connection
		if err := conn.SetReadDeadline(time.Now().Add(readDeadlineInterval)); err != nil {
			klog.V(4).Infof("Conn %d: Failed to set read deadline: %v", c.id, err)
		}

		var resp rpcResponse
		if err := conn.ReadJSON(&resp); err != nil {
			// Check if this is a timeout - if so, just loop again to check connection state
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}

			if c.isGenerationActive(generation) {
				klog.Errorf("Conn %d: WebSocket read error: %v", c.id, err)
			}
			c.handleDisconnect(generation, err)
			return
		}

		c.pendingMu.Lock()
		if pending, ok := c.pending[resp.ID]; ok && pending.generation == generation {
			pending.responseCh <- &resp
			delete(c.pending, resp.ID)
		}
		c.pendingMu.Unlock()
	}
}

// handleDisconnect handles WebSocket disconnection.
func (c *Connection) handleDisconnect(generation uint64, cause error) {
	if !c.stopGeneration(generation) {
		return
	}
	atomic.CompareAndSwapInt32(&c.connState, int32(stateConnected), int32(stateDisconnected))
	c.failPending(generation, cause)
}

func (c *Connection) failPending(generation uint64, cause error) {
	// Wait for an in-progress write to finish so successful writes are marked as
	// sent before pending requests are classified.
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for id, pending := range c.pending {
		if pending.generation != generation {
			continue
		}
		transportErr := requestTransportError(pending.method, pending.sent, cause)
		select {
		case pending.responseCh <- &rpcResponse{
			ID:           id,
			transportErr: transportErr,
		}:
		default:
		}
		delete(c.pending, id)
	}
}

// CallWithContext makes a JSON-RPC call using this connection.
func (c *Connection) CallWithContext(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	if err := c.connect(ctx); err != nil {
		return nil, err
	}
	c.mu.RLock()
	generation := c.generation
	c.mu.RUnlock()
	return c.callWithGeneration(ctx, generation, false, method, params...)
}

func (c *Connection) callWithGeneration(ctx context.Context, generation uint64, allowUnauthenticated bool, method string, params ...interface{}) (interface{}, error) {
	c.mu.Lock()
	if generation != c.generation || c.stopped || c.conn.Load() == nil || (!allowUnauthenticated && !c.authenticated) {
		c.mu.Unlock()
		return nil, fmt.Errorf("connection lost before request was sent")
	}
	c.messageID++
	id := c.messageID
	writeCh := c.writeCh
	respChan := make(chan *rpcResponse, 1)
	c.pendingMu.Lock()
	c.pending[id] = &pendingCall{
		responseCh: respChan,
		generation: generation,
		method:     method,
	}
	c.pendingMu.Unlock()
	c.mu.Unlock()

	writeReq := writeRequest{
		id: id,
		data: rpcRequest{
			JSONRPC: "2.0",
			ID:      id,
			Method:  method,
			Params:  params,
		},
		resultCh: make(chan error, 1),
	}

	select {
	case writeCh <- writeReq:
	case resp := <-respChan:
		return responseResult(resp)
	case <-ctx.Done():
		return c.canceledCallResult(id, method, respChan, ctx.Err())
	}

	select {
	case err := <-writeReq.resultCh:
		if err != nil {
			return c.failedWriteResult(id, method, respChan, err)
		}
	case resp := <-respChan:
		return responseResult(resp)
	case <-ctx.Done():
		return c.canceledCallResult(id, method, respChan, ctx.Err())
	}

	klog.V(4).Infof("Conn %d: Call: %s", c.id, method)

	select {
	case resp := <-respChan:
		return responseResult(resp)
	case <-ctx.Done():
		return c.canceledCallResult(id, method, respChan, ctx.Err())
	}
}

func requestTransportError(method string, sent bool, cause error) error {
	if sent {
		return fmt.Errorf("%w: connection lost after %s was sent: %w", ErrAmbiguousResult, method, cause)
	}
	return fmt.Errorf("%w: failed to send %s request: %w", ErrTransportFailure, method, cause)
}

func (c *Connection) failedWriteResult(id int64, method string, responseCh <-chan *rpcResponse, cause error) (interface{}, error) {
	c.pendingMu.Lock()
	select {
	case resp := <-responseCh:
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return responseResult(resp)
	default:
	}
	pending, ok := c.pending[id]
	sent := ok && pending.sent
	delete(c.pending, id)
	c.pendingMu.Unlock()
	return nil, requestTransportError(method, sent, cause)
}

func (c *Connection) canceledCallResult(id int64, method string, responseCh <-chan *rpcResponse, ctxErr error) (interface{}, error) {
	c.pendingMu.Lock()
	select {
	case resp := <-responseCh:
		delete(c.pending, id)
		c.pendingMu.Unlock()
		return responseResult(resp)
	default:
	}
	pending, ok := c.pending[id]
	sent := ok && pending.sent
	delete(c.pending, id)
	c.pendingMu.Unlock()

	if sent {
		return nil, fmt.Errorf("%w: %s was sent before the caller context ended: %w", ErrAmbiguousResult, method, ctxErr)
	}
	return nil, ctxErr
}

func responseResult(resp *rpcResponse) (interface{}, error) {
	if resp.transportErr != nil {
		return nil, resp.transportErr
	}
	if resp.Error != nil {
		return nil, &APIError{
			Code:    resp.Error.Code,
			Message: resp.Error.Message,
			Data:    resp.Error.Data,
		}
	}
	return resp.Result, nil
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.shutdown.Store(true)
	c.mu.RLock()
	generation := c.generation
	generationWG := c.generationWG
	c.mu.RUnlock()
	c.cleanupConnection(generation, fmt.Errorf("connection closed"))
	atomic.StoreInt32(&c.connState, int32(stateDisconnected))
	if generationWG != nil {
		generationWG.Wait()
	}
	return nil
}

// IsConnected returns true if connected.
func (c *Connection) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn.Load() != nil && c.authenticated && atomic.LoadInt32(&c.connState) == int32(stateConnected)
}

// Call makes a JSON-RPC call to the TrueNAS API using the connection pool.
func (c *Client) Call(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	return c.CallWithContext(ctx, method, params...)
}

// CallWithContext makes a JSON-RPC call with a context using the connection pool.
// Uses a semaphore to limit concurrent requests and prevent overwhelming TrueNAS.
// Implements circuit breaker pattern and automatic retry on connection errors with exponential backoff.
func (c *Client) CallWithContext(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	start := time.Now()
	var halfOpenProbe bool
	var breakerOutcomeRecorded bool

	// Admit the logical call exactly once. Retries below only inspect state and
	// never consume additional half-open probe slots.
	if c.circuitBreaker != nil {
		admission := c.circuitBreaker.admit()
		if !admission.allowed {
			err := fmt.Errorf("API call %s blocked: %w", method, ErrCircuitOpen)
			if c.metricsRecorder != nil {
				c.metricsRecorder(method, time.Since(start).Seconds(), err)
			}
			return nil, err
		}
		halfOpenProbe = admission.halfOpenProbe
		defer func() {
			if halfOpenProbe && !breakerOutcomeRecorded {
				c.circuitBreaker.RecordFailure()
			}
		}()
	}

	// Acquire semaphore slot (limit concurrent requests)
	select {
	case c.semaphore <- struct{}{}:
		// Got a slot, continue
	case <-ctx.Done():
		return nil, fmt.Errorf("context canceled while waiting for request slot: %w", ctx.Err())
	}
	defer func() { <-c.semaphore }() // Release slot when done

	maxRetries := c.config.APIRetryMaxAttempts
	var lastErr error
	retryDelay := c.config.APIRetryInitialDelay

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Abort if another logical call opened the circuit while this call was
		// backing off. State is non-consuming, unlike admission above.
		if c.circuitBreaker != nil && c.circuitBreaker.State() == CircuitOpen {
			err := fmt.Errorf("API call %s blocked: %w", method, ErrCircuitOpen)
			if c.metricsRecorder != nil {
				c.metricsRecorder(method, time.Since(start).Seconds(), err)
			}
			return nil, err
		}

		// Select best available connection
		conn := c.selectConnection()

		// Bound each attempt so a wedged-but-live request cannot pin a semaphore
		// slot forever. Only apply the cfg.Timeout cap when the caller supplied NO
		// deadline (e.g. background session GC using context.Background()). Callers
		// that already carry a deadline — CSI sidecar RPCs — are bounded by it, and
		// capping them shorter would wrongly fail a legitimately-long single call
		// (large clone / recursive snapshot) that the sidecar allowed time for.
		callCtx := ctx
		cancel := func() {}
		if c.config.Timeout > 0 {
			if _, hasDeadline := ctx.Deadline(); !hasDeadline {
				callCtx, cancel = context.WithTimeout(ctx, c.config.Timeout)
			}
		}
		result, err := conn.CallWithContext(callCtx, method, params...)
		attemptTimedOut := err != nil && errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil
		cancel()
		if err == nil {
			// Record success with circuit breaker
			if c.circuitBreaker != nil {
				c.circuitBreaker.RecordSuccess()
				breakerOutcomeRecorded = true
			}
			// Record successful request metrics
			if c.metricsRecorder != nil {
				c.metricsRecorder(method, time.Since(start).Seconds(), nil)
			}
			return result, nil
		}
		if attemptTimedOut {
			// A request that consumed its complete per-call budget must release the
			// semaphore now. Failures that arrive earlier may still be retried, and
			// each such retry receives a fresh per-attempt timeout above.
			if c.circuitBreaker != nil {
				c.circuitBreaker.RecordFailure()
				breakerOutcomeRecorded = true
			}
			if c.metricsRecorder != nil {
				c.metricsRecorder(method, time.Since(start).Seconds(), err)
			}
			return nil, err
		}
		if errors.Is(err, ErrAmbiguousResult) && !isIdempotentAPIMethod(method) {
			// Mutations are not retried after a successful write because the server
			// may already have applied them.
			if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
				err = errors.Join(err, ctxErr)
			}
			if c.circuitBreaker != nil {
				c.circuitBreaker.RecordFailure()
				breakerOutcomeRecorded = true
			}
			if c.metricsRecorder != nil {
				c.metricsRecorder(method, time.Since(start).Seconds(), err)
			}
			return nil, err
		}
		if ctx.Err() != nil {
			if c.metricsRecorder != nil {
				c.metricsRecorder(method, time.Since(start).Seconds(), ctx.Err())
			}
			return nil, ctx.Err()
		}

		// Check if error is retryable (connection-related)
		if !IsConnectionError(err) {
			// Not a connection error - return immediately (API errors, not found, etc.)
			// An API-level error still proves the transport round-trip worked, so a
			// half-open probe must count it as breaker success — otherwise a benign
			// "not found" reply would reopen the circuit on a healthy connection.
			if c.circuitBreaker != nil {
				c.circuitBreaker.RecordSuccess()
				breakerOutcomeRecorded = true
			}
			// Record failed request metrics
			if c.metricsRecorder != nil {
				c.metricsRecorder(method, time.Since(start).Seconds(), err)
			}
			return nil, err
		}

		lastErr = err
		klog.V(2).Infof("API call %s failed on conn %d (attempt %d/%d): %v", method, conn.id, attempt+1, maxRetries, err)

		// Don't retry on last attempt or if context is done
		if attempt < maxRetries-1 {
			timer := time.NewTimer(retryDelay)
			select {
			case <-timer.C:
				retryDelay = time.Duration(float64(retryDelay) * c.config.APIRetryBackoffFactor)
				if retryDelay > c.config.APIRetryMaxDelay {
					retryDelay = c.config.APIRetryMaxDelay
				}
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				finalErr := fmt.Errorf("context canceled during retry: %w", ctx.Err())
				if c.metricsRecorder != nil {
					c.metricsRecorder(method, time.Since(start).Seconds(), finalErr)
				}
				return nil, finalErr
			}
		}
	}

	finalErr := fmt.Errorf("API call %s failed after %d retries: %w", method, maxRetries, lastErr)
	if c.circuitBreaker != nil {
		c.circuitBreaker.RecordFailure()
		breakerOutcomeRecorded = true
	}
	// Record failed request metrics after all retries exhausted
	if c.metricsRecorder != nil {
		c.metricsRecorder(method, time.Since(start).Seconds(), finalErr)
	}
	return nil, finalErr
}

// isIdempotentAPIMethod is deliberately conservative: only known read-only
// operation names may be retried after an ambiguous post-write disconnect.
func isIdempotentAPIMethod(method string) bool {
	operation := method
	if index := strings.LastIndexByte(method, '.'); index >= 0 {
		operation = method[index+1:]
	}
	switch operation {
	case "query", "get", "get_instance", "get_jobs", "lookup", "config", "info", "ping":
		return true
	default:
		return strings.HasSuffix(operation, "_choices")
	}
}

// CircuitBreakerStats returns statistics about the circuit breaker.
// Returns nil if circuit breaker is not enabled.
func (c *Client) CircuitBreakerStats() *CircuitBreakerStats {
	if c.circuitBreaker == nil {
		return nil
	}
	stats := c.circuitBreaker.Stats()
	return &stats
}

// ResetCircuitBreaker manually resets the circuit breaker to closed state.
// This is useful for manual intervention after fixing underlying issues.
func (c *Client) ResetCircuitBreaker() {
	if c.circuitBreaker != nil {
		c.circuitBreaker.Reset()
	}
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

// ActiveConnectionCount returns the number of authenticated connections in the pool.
func (c *Client) ActiveConnectionCount() int {
	connected := 0
	for _, conn := range c.pool {
		if conn.IsConnected() {
			connected++
		}
	}
	return connected
}

// ServiceReload reloads a TrueNAS service configuration.
// This is useful for forcing services like iSCSI to pick up new configuration
// after creating targets/extents via the API.
// Common service names: "iscsitarget", "nfs", "cifs", "nvmeof"
//
// Returns an error if the reload fails. Callers should treat this as non-fatal
// since the service might auto-reload, and node-side retry logic handles propagation delays.

// deleteVanishedTolerant reports whether a failed delete can be treated as
// success because the object no longer exists. TrueNAS surfaces deletes of
// nonexistent ids as a bare "Invalid params" (-32602) over the WebSocket API
// (validated live on 26.0), so the error text alone cannot distinguish a bad
// call from an already-deleted object — existence is checked by query.
func (c *Client) deleteVanishedTolerant(ctx context.Context, method string, id int) bool {
	result, err := c.Call(ctx, method, [][]interface{}{{"id", "=", id}}, map[string]interface{}{})
	if err != nil {
		return false
	}
	items, ok := result.([]interface{})
	return ok && len(items) == 0
}

func (c *Client) ServiceReload(ctx context.Context, service string) error {
	klog.V(4).Infof("Reloading service: %s", service)
	_, err := c.Call(ctx, "service.reload", service)
	if err != nil {
		// TrueNAS 26.0 removed service.reload in favor of
		// service.control(verb, service, options) (validated live).
		if isMethodNotFoundError(err) || strings.Contains(err.Error(), "Method call error") {
			if _, ctlErr := c.Call(ctx, "service.control", "RELOAD", service, map[string]interface{}{}); ctlErr == nil {
				klog.Infof("Service %s reloaded successfully (service.control)", service)
				return nil
			} else {
				err = ctlErr
			}
		}
		return fmt.Errorf("failed to reload service %s: %w", service, err)
	}
	klog.Infof("Service %s reloaded successfully", service)
	return nil
}

// GetSystemInfo retrieves TrueNAS system information including version.
// The result is cached to avoid repeated API calls.
func (c *Client) GetSystemInfo(ctx context.Context) (*SystemInfo, error) {
	// Check cache first
	c.versionMu.RLock()
	if c.versionCache != nil {
		cached := c.versionCache
		c.versionMu.RUnlock()
		return cached, nil
	}
	c.versionMu.RUnlock()

	// Not cached, fetch from API
	c.versionMu.Lock()
	defer c.versionMu.Unlock()

	// Double-check after acquiring write lock
	if c.versionCache != nil {
		return c.versionCache, nil
	}

	result, err := c.Call(ctx, "system.info")
	if err != nil {
		return nil, fmt.Errorf("failed to get system info: %w", err)
	}

	info, err := parseSystemInfo(result)
	if err != nil {
		return nil, err
	}

	// Cache the result
	c.versionCache = info
	return info, nil
}

// parseSystemInfo parses the system.info API response.
func parseSystemInfo(data interface{}) (*SystemInfo, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected system info format")
	}

	info := &SystemInfo{}

	if v, ok := m["version"].(string); ok {
		info.Version = v
		// Parse version string (e.g., "TrueNAS-SCALE-25.10.0" or "25.10.0")
		parseTrueNASVersion(v, info)
	}
	if v, ok := m["hostname"].(string); ok {
		info.Hostname = v
	}
	if v, ok := m["uptime_seconds"].(float64); ok {
		info.UptimeSeconds = int64(v)
	}
	if v, ok := m["system_product"].(string); ok {
		info.SystemProduct = v
	}
	if v, ok := m["system_serial"].(string); ok {
		info.SystemSerial = v
	}

	return info, nil
}

// parseTrueNASVersion extracts major.minor.patch from TrueNAS version string.
// Handles formats like "TrueNAS-SCALE-25.10.0", "25.10.0", "25.10", etc.
func parseTrueNASVersion(version string, info *SystemInfo) {
	// Strip "TrueNAS-SCALE-" prefix if present
	version = strings.TrimPrefix(version, "TrueNAS-SCALE-")
	version = strings.TrimPrefix(version, "TrueNAS-")

	// Split by dots and parse integers
	parts := strings.Split(version, ".")
	if len(parts) > 0 {
		if v, err := strconv.Atoi(parts[0]); err == nil {
			info.VersionMajor = v
		}
	}
	if len(parts) > 1 {
		if v, err := strconv.Atoi(parts[1]); err == nil {
			info.VersionMinor = v
		}
	}
	if len(parts) > 2 {
		// Handle patch versions with suffixes (e.g., "0-MASTER", "0.1")
		patchStr := strings.Split(parts[2], "-")[0]
		if v, err := strconv.Atoi(patchStr); err == nil {
			info.VersionPatch = v
		}
	}
}

// CheckNVMeoFSupport checks if the TrueNAS version supports NVMe-oF (25.10+).
// Returns an error if NVMe-oF is not supported.
func (c *Client) CheckNVMeoFSupport(ctx context.Context) error {
	info, err := c.GetSystemInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to detect TrueNAS version: %w", err)
	}

	// NVMe-oF requires TrueNAS SCALE 25.10 or later
	if info.VersionMajor < 25 || (info.VersionMajor == 25 && info.VersionMinor < 10) {
		return fmt.Errorf("NVMe-oF is not supported on TrueNAS SCALE %d.%d (requires 25.10 or later)",
			info.VersionMajor, info.VersionMinor)
	}

	klog.V(4).Infof("TrueNAS SCALE version %s supports NVMe-oF", info.Version)
	return nil
}
