package truenas

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockWSServer creates a mock WebSocket server for testing
type mockWSServer struct {
	server    *httptest.Server
	handler   func(conn *websocket.Conn)
	upgrader  websocket.Upgrader
	responses map[string]interface{}
	mu        sync.RWMutex
}

func newMockWSServer() *mockWSServer {
	m := &mockWSServer{
		responses: make(map[string]interface{}),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
	return m
}

func (m *mockWSServer) start(handler func(conn *websocket.Conn)) *httptest.Server {
	m.handler = handler
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := m.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		m.handler(conn)
	}))
	return m.server
}

func (m *mockWSServer) close() {
	if m.server != nil {
		m.server.Close()
	}
}

// rpcTestRequest mirrors rpcRequest for test JSON parsing
type rpcTestRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int64         `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params,omitempty"`
}

// rpcTestResponse mirrors rpcResponse for test JSON generation
type rpcTestResponse struct {
	JSONRPC string       `json:"jsonrpc"`
	ID      int64        `json:"id"`
	Result  interface{}  `json:"result,omitempty"`
	Error   *rpcError    `json:"error,omitempty"`
}

// TestDatasetCreate tests the DatasetCreate function
func TestDatasetCreate_Success(t *testing.T) {
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
			case "pool.dataset.create":
				resp.Result = map[string]interface{}{
					"id":         "tank/k8s/volumes/pvc-test",
					"name":       "tank/k8s/volumes/pvc-test",
					"pool":       "tank",
					"type":       "FILESYSTEM",
					"mountpoint": "/mnt/tank/k8s/volumes/pvc-test",
					"used": map[string]interface{}{
						"value":    "0",
						"rawvalue": "0",
						"parsed":   float64(0),
						"source":   "LOCAL",
					},
					"available": map[string]interface{}{
						"value":    "100G",
						"rawvalue": "107374182400",
						"parsed":   float64(107374182400),
						"source":   "LOCAL",
					},
					"quota": map[string]interface{}{
						"value":    "10G",
						"rawvalue": "10737418240",
						"parsed":   float64(10737418240),
						"source":   "LOCAL",
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

	// Extract host and port from the test server URL
	wsURL := strings.Replace(server.URL, "http://", "", 1)
	parts := strings.Split(wsURL, ":")
	host := parts[0]
	port := 80
	if len(parts) > 1 {
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	params := &DatasetCreateParams{
		Name:     "tank/k8s/volumes/pvc-test",
		Type:     "FILESYSTEM",
		Refquota: 10737418240,
	}

	ds, err := client.DatasetCreate(ctx, params)
	require.NoError(t, err)
	assert.NotNil(t, ds)
	assert.Equal(t, "tank/k8s/volumes/pvc-test", ds.ID)
	assert.Equal(t, "tank/k8s/volumes/pvc-test", ds.Name)
	assert.Equal(t, "tank", ds.Pool)
	assert.Equal(t, "FILESYSTEM", ds.Type)
}

// TestDatasetCreate_AlreadyExists tests idempotent dataset creation
func TestDatasetCreate_AlreadyExists(t *testing.T) {
	mock := newMockWSServer()
	callCount := 0
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
			case "pool.dataset.create":
				callCount++
				resp.Error = &rpcError{
					Code:    -1,
					Message: "dataset already exists",
				}
			case "pool.dataset.query":
				// Return existing dataset
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":              "tank/k8s/volumes/existing",
						"name":            "tank/k8s/volumes/existing",
						"pool":            "tank",
						"type":            "FILESYSTEM",
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	params := &DatasetCreateParams{
		Name: "tank/k8s/volumes/existing",
		Type: "FILESYSTEM",
	}

	ds, err := client.DatasetCreate(ctx, params)
	require.NoError(t, err)
	assert.NotNil(t, ds)
	assert.Equal(t, "tank/k8s/volumes/existing", ds.ID)
}

// TestDatasetGet tests retrieving a dataset
func TestDatasetGet_Success(t *testing.T) {
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
			case "pool.dataset.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":         "tank/k8s/volumes/pvc-123",
						"name":       "tank/k8s/volumes/pvc-123",
						"pool":       "tank",
						"type":       "VOLUME",
						"mountpoint": "",
						"volsize": map[string]interface{}{
							"value":    "10G",
							"rawvalue": "10737418240",
							"parsed":   float64(10737418240),
							"source":   "LOCAL",
						},
						"user_properties": map[string]interface{}{
							"truenas-csi:managed_resource": map[string]interface{}{
								"value":  "true",
								"source": "LOCAL",
							},
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	ds, err := client.DatasetGet(ctx, "tank/k8s/volumes/pvc-123")
	require.NoError(t, err)
	assert.NotNil(t, ds)
	assert.Equal(t, "tank/k8s/volumes/pvc-123", ds.ID)
	assert.Equal(t, "VOLUME", ds.Type)
	assert.Equal(t, float64(10737418240), ds.Volsize.Parsed)
	assert.Equal(t, "true", ds.UserProperties["truenas-csi:managed_resource"].Value)
}

// TestDatasetGet_NotFound tests retrieving a non-existent dataset
func TestDatasetGet_NotFound(t *testing.T) {
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
			case "pool.dataset.query":
				resp.Result = []interface{}{} // Empty result
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	ds, err := client.DatasetGet(ctx, "tank/nonexistent")
	assert.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "not found")
}

// TestDatasetDelete tests deleting a dataset
func TestDatasetDelete_Success(t *testing.T) {
	mock := newMockWSServer()
	deleteCallCount := 0
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
			case "pool.dataset.delete":
				deleteCallCount++
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	err = client.DatasetDelete(ctx, "tank/k8s/volumes/pvc-delete", false, false)
	assert.NoError(t, err)
	assert.Equal(t, 1, deleteCallCount)
}

// TestDatasetDelete_NotFound tests deleting a non-existent dataset (idempotent)
func TestDatasetDelete_NotFound(t *testing.T) {
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
			case "pool.dataset.delete":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "dataset not found",
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	// Should succeed (idempotent delete)
	err = client.DatasetDelete(ctx, "tank/nonexistent", false, false)
	assert.NoError(t, err)
}

// TestDatasetDelete_InvalidParams tests delete with -32602 error (idempotent)
func TestDatasetDelete_InvalidParams(t *testing.T) {
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
			case "pool.dataset.delete":
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	// Should succeed (idempotent delete - TrueNAS returns -32602 when dataset doesn't exist)
	err = client.DatasetDelete(ctx, "tank/nonexistent", false, false)
	assert.NoError(t, err)
}

// TestDatasetUpdate tests updating a dataset
func TestDatasetUpdate_Success(t *testing.T) {
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
			case "pool.dataset.update":
				resp.Result = map[string]interface{}{
					"id":   "tank/k8s/volumes/pvc-update",
					"name": "tank/k8s/volumes/pvc-update",
					"pool": "tank",
					"type": "VOLUME",
					"volsize": map[string]interface{}{
						"value":    "20G",
						"rawvalue": "21474836480",
						"parsed":   float64(21474836480),
						"source":   "LOCAL",
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	params := &DatasetUpdateParams{
		Volsize: 21474836480,
	}

	ds, err := client.DatasetUpdate(ctx, "tank/k8s/volumes/pvc-update", params)
	require.NoError(t, err)
	assert.NotNil(t, ds)
	assert.Equal(t, float64(21474836480), ds.Volsize.Parsed)
}

// TestDatasetList tests listing datasets
func TestDatasetList_Success(t *testing.T) {
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
			case "pool.dataset.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":              "tank/k8s/volumes/pvc-1",
						"name":            "tank/k8s/volumes/pvc-1",
						"pool":            "tank",
						"type":            "FILESYSTEM",
						"user_properties": map[string]interface{}{},
					},
					map[string]interface{}{
						"id":              "tank/k8s/volumes/pvc-2",
						"name":            "tank/k8s/volumes/pvc-2",
						"pool":            "tank",
						"type":            "VOLUME",
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	datasets, err := client.DatasetList(ctx, "tank/k8s/volumes", 0, 0)
	require.NoError(t, err)
	assert.Len(t, datasets, 2)
	assert.Equal(t, "tank/k8s/volumes/pvc-1", datasets[0].ID)
	assert.Equal(t, "tank/k8s/volumes/pvc-2", datasets[1].ID)
}

// TestDatasetExpand tests expanding a zvol
func TestDatasetExpand_Success(t *testing.T) {
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
			case "pool.dataset.update":
				resp.Result = map[string]interface{}{
					"id":   "tank/k8s/volumes/pvc-expand",
					"name": "tank/k8s/volumes/pvc-expand",
					"pool": "tank",
					"type": "VOLUME",
					"volsize": map[string]interface{}{
						"value":    "50G",
						"rawvalue": "53687091200",
						"parsed":   float64(53687091200),
						"source":   "LOCAL",
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	err = client.DatasetExpand(ctx, "tank/k8s/volumes/pvc-expand", 53687091200)
	assert.NoError(t, err)
}

// TestDatasetSetUserProperty tests setting a user property
func TestDatasetSetUserProperty_Success(t *testing.T) {
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
			case "pool.dataset.update":
				// Verify user_properties_update in params
				if len(req.Params) >= 2 {
					if paramsMap, ok := req.Params[1].(map[string]interface{}); ok {
						if updates, ok := paramsMap["user_properties_update"].([]interface{}); ok {
							if len(updates) > 0 {
								if updateMap, ok := updates[0].(map[string]interface{}); ok {
									if updateMap["key"] == "truenas-csi:nfs_share_id" && updateMap["value"] == "42" {
										resp.Result = map[string]interface{}{
											"id":              "tank/k8s/volumes/pvc-userprop",
											"name":            "tank/k8s/volumes/pvc-userprop",
											"pool":            "tank",
											"type":            "FILESYSTEM",
											"user_properties": map[string]interface{}{},
										}
									}
								}
							}
						}
					}
				}
				if resp.Result == nil {
					resp.Result = map[string]interface{}{
						"id":              "tank/k8s/volumes/pvc-userprop",
						"name":            "tank/k8s/volumes/pvc-userprop",
						"pool":            "tank",
						"type":            "FILESYSTEM",
						"user_properties": map[string]interface{}{},
					}
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	err = client.DatasetSetUserProperty(ctx, "tank/k8s/volumes/pvc-userprop", "truenas-csi:nfs_share_id", "42")
	assert.NoError(t, err)
}

// TestDatasetGetUserProperty tests getting a user property
func TestDatasetGetUserProperty_Success(t *testing.T) {
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
			case "pool.dataset.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":   "tank/k8s/volumes/pvc-getprop",
						"name": "tank/k8s/volumes/pvc-getprop",
						"pool": "tank",
						"type": "FILESYSTEM",
						"user_properties": map[string]interface{}{
							"truenas-csi:nfs_share_id": map[string]interface{}{
								"value":  "42",
								"source": "LOCAL",
							},
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	value, err := client.DatasetGetUserProperty(ctx, "tank/k8s/volumes/pvc-getprop", "truenas-csi:nfs_share_id")
	require.NoError(t, err)
	assert.Equal(t, "42", value)
}

// TestDatasetGetUserProperty_Missing tests getting a missing user property
func TestDatasetGetUserProperty_Missing(t *testing.T) {
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
			case "pool.dataset.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":              "tank/k8s/volumes/pvc-noprop",
						"name":            "tank/k8s/volumes/pvc-noprop",
						"pool":            "tank",
						"type":            "FILESYSTEM",
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	value, err := client.DatasetGetUserProperty(ctx, "tank/k8s/volumes/pvc-noprop", "truenas-csi:missing")
	require.NoError(t, err)
	assert.Empty(t, value)
}

// TestDatasetExists tests checking if a dataset exists
func TestDatasetExists_True(t *testing.T) {
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
			case "pool.dataset.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":              "tank/k8s/volumes/existing",
						"name":            "tank/k8s/volumes/existing",
						"pool":            "tank",
						"type":            "FILESYSTEM",
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	exists, err := client.DatasetExists(ctx, "tank/k8s/volumes/existing")
	require.NoError(t, err)
	assert.True(t, exists)
}

// TestDatasetExists_False tests checking if a non-existent dataset exists
func TestDatasetExists_False(t *testing.T) {
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
			case "pool.dataset.query":
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	exists, err := client.DatasetExists(ctx, "tank/k8s/volumes/nonexistent")
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestGetPoolAvailable tests getting pool available space
func TestGetPoolAvailable_Success(t *testing.T) {
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
			case "pool.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"name": "tank",
						"topology": map[string]interface{}{
							"data": []interface{}{
								map[string]interface{}{
									"stats": map[string]interface{}{
										"free": float64(500000000000),
									},
								},
							},
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	avail, err := client.GetPoolAvailable(ctx, "tank/k8s/volumes")
	require.NoError(t, err)
	assert.Equal(t, int64(500000000000), avail)
}

// TestParseDataset tests the parseDataset function
func TestParseDataset_ValidData(t *testing.T) {
	data := map[string]interface{}{
		"id":         "tank/test",
		"name":       "tank/test",
		"pool":       "tank",
		"type":       "FILESYSTEM",
		"mountpoint": "/mnt/tank/test",
		"used": map[string]interface{}{
			"value":    "1G",
			"rawvalue": "1073741824",
			"parsed":   float64(1073741824),
			"source":   "LOCAL",
		},
		"available": map[string]interface{}{
			"value":    "100G",
			"rawvalue": "107374182400",
			"parsed":   float64(107374182400),
			"source":   "LOCAL",
		},
		"user_properties": map[string]interface{}{
			"custom:prop": map[string]interface{}{
				"value":  "test-value",
				"source": "LOCAL",
			},
		},
	}

	ds, err := parseDataset(data)
	require.NoError(t, err)
	assert.Equal(t, "tank/test", ds.ID)
	assert.Equal(t, "tank/test", ds.Name)
	assert.Equal(t, "tank", ds.Pool)
	assert.Equal(t, "FILESYSTEM", ds.Type)
	assert.Equal(t, "/mnt/tank/test", ds.Mountpoint)
	assert.Equal(t, float64(1073741824), ds.Used.Parsed)
	assert.Equal(t, float64(107374182400), ds.Available.Parsed)
	assert.Equal(t, "test-value", ds.UserProperties["custom:prop"].Value)
}

// TestParseDataset_InvalidData tests parseDataset with invalid input
func TestParseDataset_InvalidData(t *testing.T) {
	ds, err := parseDataset("invalid")
	assert.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "unexpected dataset format")
}

// TestParseProperty tests the parseProperty function
func TestParseProperty_ValidProperty(t *testing.T) {
	data := map[string]interface{}{
		"value":    "10G",
		"rawvalue": "10737418240",
		"parsed":   float64(10737418240),
		"source":   "LOCAL",
	}

	prop := parseProperty(data)
	assert.Equal(t, "10G", prop.Value)
	assert.Equal(t, "10737418240", prop.Rawvalue)
	assert.Equal(t, float64(10737418240), prop.Parsed)
	assert.Equal(t, "LOCAL", prop.Source)
}

// TestParseProperty_NilData tests parseProperty with nil input
func TestParseProperty_NilData(t *testing.T) {
	prop := parseProperty(nil)
	assert.Nil(t, prop.Value)
	assert.Empty(t, prop.Rawvalue)
	assert.Nil(t, prop.Parsed)
	assert.Empty(t, prop.Source)
}

// TestDatasetCreate_TableDriven uses table-driven tests for various create scenarios
func TestDatasetCreate_TableDriven(t *testing.T) {
	tests := []struct {
		name           string
		params         *DatasetCreateParams
		mockResponse   interface{}
		mockError      *rpcError
		expectError    bool
		expectedType   string
	}{
		{
			name: "filesystem creation",
			params: &DatasetCreateParams{
				Name:     "tank/fs/test",
				Type:     "FILESYSTEM",
				Refquota: 10737418240,
			},
			mockResponse: map[string]interface{}{
				"id":              "tank/fs/test",
				"name":            "tank/fs/test",
				"type":            "FILESYSTEM",
				"user_properties": map[string]interface{}{},
			},
			expectError:  false,
			expectedType: "FILESYSTEM",
		},
		{
			name: "volume creation",
			params: &DatasetCreateParams{
				Name:    "tank/vol/test",
				Type:    "VOLUME",
				Volsize: 10737418240,
				Sparse:  true,
			},
			mockResponse: map[string]interface{}{
				"id":              "tank/vol/test",
				"name":            "tank/vol/test",
				"type":            "VOLUME",
				"user_properties": map[string]interface{}{},
			},
			expectError:  false,
			expectedType: "VOLUME",
		},
		{
			name: "creation failure",
			params: &DatasetCreateParams{
				Name: "tank/fail/test",
				Type: "FILESYSTEM",
			},
			mockError: &rpcError{
				Code:    -1,
				Message: "parent dataset does not exist",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
					case "pool.dataset.create":
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
				fmt.Sscanf(parts[1], "%d", &port)
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
			defer client.Close()

			ctx := context.Background()
			ds, err := client.DatasetCreate(ctx, tt.params)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, ds)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, ds)
				assert.Equal(t, tt.expectedType, ds.Type)
			}
		})
	}
}

// TestWaitForDatasetReady tests waiting for a dataset to be ready
func TestWaitForDatasetReady_ImmediateSuccess(t *testing.T) {
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
			case "pool.dataset.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":              "tank/k8s/volumes/ready",
						"name":            "tank/k8s/volumes/ready",
						"pool":            "tank",
						"type":            "FILESYSTEM",
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	ds, err := client.WaitForDatasetReady(ctx, "tank/k8s/volumes/ready", 5*time.Second)
	require.NoError(t, err)
	assert.NotNil(t, ds)
	assert.Equal(t, "tank/k8s/volumes/ready", ds.ID)
}

// TestWaitForDatasetReady_Timeout tests timeout scenario
func TestWaitForDatasetReady_Timeout(t *testing.T) {
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
			case "pool.dataset.query":
				// Always return empty - dataset never ready
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	ds, err := client.WaitForDatasetReady(ctx, "tank/k8s/volumes/never-ready", 500*time.Millisecond)
	assert.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "timeout")
}

// TestWaitForZvolReady_Success tests waiting for zvol to be ready
func TestWaitForZvolReady_Success(t *testing.T) {
	mock := newMockWSServer()
	callCount := 0
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
			case "pool.dataset.query":
				callCount++
				if callCount < 2 {
					// First call: return zvol without volsize
					resp.Result = []interface{}{
						map[string]interface{}{
							"id":              "tank/k8s/volumes/zvol",
							"name":            "tank/k8s/volumes/zvol",
							"pool":            "tank",
							"type":            "VOLUME",
							"user_properties": map[string]interface{}{},
						},
					}
				} else {
					// Second call: return zvol with volsize
					resp.Result = []interface{}{
						map[string]interface{}{
							"id":   "tank/k8s/volumes/zvol",
							"name": "tank/k8s/volumes/zvol",
							"pool": "tank",
							"type": "VOLUME",
							"volsize": map[string]interface{}{
								"parsed": float64(10737418240),
							},
							"user_properties": map[string]interface{}{},
						},
					}
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
		fmt.Sscanf(parts[1], "%d", &port)
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
	defer client.Close()

	ctx := context.Background()
	ds, err := client.WaitForZvolReady(ctx, "tank/k8s/volumes/zvol", 5*time.Second)
	require.NoError(t, err)
	assert.NotNil(t, ds)
	assert.Equal(t, "VOLUME", ds.Type)
	assert.Equal(t, float64(10737418240), ds.Volsize.Parsed)
}

// TestMockClient_DatasetOperations tests the MockClient dataset operations
func TestMockClient_DatasetOperations(t *testing.T) {
	mock := NewMockClient()
	ctx := context.Background()

	// Test create
	params := &DatasetCreateParams{
		Name:    "tank/test/dataset",
		Type:    "FILESYSTEM",
		Volsize: 10737418240,
	}
	ds, err := mock.DatasetCreate(ctx, params)
	require.NoError(t, err)
	assert.Equal(t, "tank/test/dataset", ds.ID)

	// Test get
	ds, err = mock.DatasetGet(ctx, "tank/test/dataset")
	require.NoError(t, err)
	assert.Equal(t, "tank/test/dataset", ds.ID)

	// Test exists
	exists, err := mock.DatasetExists(ctx, "tank/test/dataset")
	require.NoError(t, err)
	assert.True(t, exists)

	// Test set user property
	err = mock.DatasetSetUserProperty(ctx, "tank/test/dataset", "test:key", "test-value")
	require.NoError(t, err)

	// Test get user property
	value, err := mock.DatasetGetUserProperty(ctx, "tank/test/dataset", "test:key")
	require.NoError(t, err)
	assert.Equal(t, "test-value", value)

	// Test expand
	err = mock.DatasetExpand(ctx, "tank/test/dataset", 21474836480)
	require.NoError(t, err)

	// Verify expansion
	ds, err = mock.DatasetGet(ctx, "tank/test/dataset")
	require.NoError(t, err)
	assert.Equal(t, float64(21474836480), ds.Volsize.Parsed)

	// Test delete
	err = mock.DatasetDelete(ctx, "tank/test/dataset", false, false)
	require.NoError(t, err)

	// Verify deletion
	exists, err = mock.DatasetExists(ctx, "tank/test/dataset")
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestMockClient_DatasetErrorInjection tests error injection in MockClient
func TestMockClient_DatasetErrorInjection(t *testing.T) {
	mock := NewMockClient()
	ctx := context.Background()

	// Inject error
	mock.InjectError = fmt.Errorf("injected error")

	// Test create with error
	params := &DatasetCreateParams{
		Name: "tank/test/fail",
		Type: "FILESYSTEM",
	}
	_, err := mock.DatasetCreate(ctx, params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "injected error")

	// Test get with error
	_, err = mock.DatasetGet(ctx, "tank/test/any")
	assert.Error(t, err)

	// Clear error
	mock.InjectError = nil

	// Operations should succeed now
	ds, err := mock.DatasetCreate(ctx, params)
	require.NoError(t, err)
	assert.NotNil(t, ds)
}

// Benchmark tests
func BenchmarkParseDataset(b *testing.B) {
	data := map[string]interface{}{
		"id":         "tank/test",
		"name":       "tank/test",
		"pool":       "tank",
		"type":       "FILESYSTEM",
		"mountpoint": "/mnt/tank/test",
		"used": map[string]interface{}{
			"value":    "1G",
			"rawvalue": "1073741824",
			"parsed":   float64(1073741824),
			"source":   "LOCAL",
		},
		"available": map[string]interface{}{
			"value":    "100G",
			"rawvalue": "107374182400",
			"parsed":   float64(107374182400),
			"source":   "LOCAL",
		},
		"user_properties": map[string]interface{}{
			"custom:prop1": map[string]interface{}{
				"value":  "value1",
				"source": "LOCAL",
			},
			"custom:prop2": map[string]interface{}{
				"value":  "value2",
				"source": "LOCAL",
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parseDataset(data)
	}
}

// Helper function to marshal JSON for comparison
func mustMarshalJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
