package truenas

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNFSShareCreate_Success tests creating an NFS share
func TestNFSShareCreate_Success(t *testing.T) {
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
			case "sharing.nfs.create":
				resp.Result = map[string]interface{}{
					"id":       float64(42),
					"path":     "/mnt/tank/k8s/volumes/pvc-test",
					"paths":    []interface{}{"/mnt/tank/k8s/volumes/pvc-test"},
					"comment":  "CSI volume",
					"networks": []interface{}{},
					"hosts":    []interface{}{},
					"ro":       false,
					"enabled":  true,
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
	params := &NFSShareCreateParams{
		Path:    "/mnt/tank/k8s/volumes/pvc-test",
		Comment: "CSI volume",
	}

	share, err := client.NFSShareCreate(ctx, params)
	require.NoError(t, err)
	assert.NotNil(t, share)
	assert.Equal(t, 42, share.ID)
	assert.Equal(t, "/mnt/tank/k8s/volumes/pvc-test", share.Path)
	assert.True(t, share.Enabled)
}

// TestNFSShareCreate_AlreadyExists tests idempotent share creation
func TestNFSShareCreate_AlreadyExists(t *testing.T) {
	mock := newMockWSServer()
	createCalled := false
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
			case "sharing.nfs.create":
				createCalled = true
				resp.Error = &rpcError{
					Code:    -1,
					Message: "path already exports this dataset",
				}
			case "sharing.nfs.query":
				// Return existing share
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":       float64(99),
						"path":     "/mnt/tank/k8s/volumes/existing",
						"paths":    []interface{}{"/mnt/tank/k8s/volumes/existing"},
						"comment":  "Existing share",
						"networks": []interface{}{},
						"hosts":    []interface{}{},
						"ro":       false,
						"enabled":  true,
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
	params := &NFSShareCreateParams{
		Path: "/mnt/tank/k8s/volumes/existing",
	}

	share, err := client.NFSShareCreate(ctx, params)
	require.NoError(t, err)
	assert.NotNil(t, share)
	assert.Equal(t, 99, share.ID)
	assert.True(t, createCalled)
}

// TestNFSShareCreate_AlreadySharedError tests handling "already shared" error
func TestNFSShareCreate_AlreadySharedError(t *testing.T) {
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
			case "sharing.nfs.create":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "path is already shared",
				}
			case "sharing.nfs.query":
				// Return existing share
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":      float64(77),
						"path":    "/mnt/tank/k8s/volumes/shared",
						"paths":   []interface{}{"/mnt/tank/k8s/volumes/shared"},
						"enabled": true,
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
	params := &NFSShareCreateParams{
		Path: "/mnt/tank/k8s/volumes/shared",
	}

	share, err := client.NFSShareCreate(ctx, params)
	require.NoError(t, err)
	assert.NotNil(t, share)
	assert.Equal(t, 77, share.ID)
}

// TestNFSShareCreate_Failure tests create failure
func TestNFSShareCreate_Failure(t *testing.T) {
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
			case "sharing.nfs.create":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "path does not exist",
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
	params := &NFSShareCreateParams{
		Path: "/mnt/nonexistent/path",
	}

	share, err := client.NFSShareCreate(ctx, params)
	assert.Error(t, err)
	assert.Nil(t, share)
	assert.Contains(t, err.Error(), "failed to create NFS share")
}

// TestNFSShareDelete_Success tests deleting an NFS share
func TestNFSShareDelete_Success(t *testing.T) {
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
			case "sharing.nfs.delete":
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
	err = client.NFSShareDelete(ctx, 42)
	assert.NoError(t, err)
	assert.True(t, deleteCalled)
}

// TestNFSShareDelete_NotFound tests deleting a non-existent share (idempotent)
func TestNFSShareDelete_NotFound(t *testing.T) {
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
			case "sharing.nfs.delete":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "share does not exist",
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
	err = client.NFSShareDelete(ctx, 9999)
	assert.NoError(t, err)
}

// TestNFSShareDelete_NotFoundError tests "not found" error handling
func TestNFSShareDelete_NotFoundError(t *testing.T) {
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
			case "sharing.nfs.delete":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "NFS share not found",
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
	err = client.NFSShareDelete(ctx, 9999)
	assert.NoError(t, err)
}

// TestNFSShareGet_Success tests getting an NFS share by ID
func TestNFSShareGet_Success(t *testing.T) {
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
			case "sharing.nfs.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":           float64(42),
						"path":         "/mnt/tank/k8s/volumes/pvc-get",
						"paths":        []interface{}{"/mnt/tank/k8s/volumes/pvc-get"},
						"comment":      "Test share",
						"networks":     []interface{}{"10.0.0.0/8"},
						"hosts":        []interface{}{"node1", "node2"},
						"ro":           false,
						"maproot_user": "root",
						"mapall_user":  "",
						"security":     []interface{}{"sys"},
						"enabled":      true,
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
	share, err := client.NFSShareGet(ctx, 42)
	require.NoError(t, err)
	assert.NotNil(t, share)
	assert.Equal(t, 42, share.ID)
	assert.Equal(t, "/mnt/tank/k8s/volumes/pvc-get", share.Path)
	assert.Equal(t, "Test share", share.Comment)
	assert.Contains(t, share.Networks, "10.0.0.0/8")
	assert.Contains(t, share.Hosts, "node1")
	assert.Contains(t, share.Hosts, "node2")
	assert.Equal(t, "root", share.MaprootUser)
	assert.Contains(t, share.Security, "sys")
	assert.True(t, share.Enabled)
}

// TestNFSShareGet_NotFound tests getting a non-existent share
func TestNFSShareGet_NotFound(t *testing.T) {
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
			case "sharing.nfs.query":
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
	share, err := client.NFSShareGet(ctx, 9999)
	assert.Error(t, err)
	assert.Nil(t, share)
	assert.Contains(t, err.Error(), "not found")
}

// TestNFSShareFindByPath_Success tests finding a share by path
func TestNFSShareFindByPath_Success(t *testing.T) {
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
			case "sharing.nfs.query":
				// Return share matching path filter
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":      float64(55),
						"path":    "/mnt/tank/k8s/volumes/find-by-path",
						"paths":   []interface{}{"/mnt/tank/k8s/volumes/find-by-path"},
						"enabled": true,
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
	share, err := client.NFSShareFindByPath(ctx, "/mnt/tank/k8s/volumes/find-by-path")
	require.NoError(t, err)
	assert.NotNil(t, share)
	assert.Equal(t, 55, share.ID)
}

// TestNFSShareFindByPath_NotFound tests finding a share that doesn't exist
func TestNFSShareFindByPath_NotFound(t *testing.T) {
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
			case "sharing.nfs.query":
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
	share, err := client.NFSShareFindByPath(ctx, "/mnt/nonexistent/path")
	require.NoError(t, err) // Not an error, just returns nil
	assert.Nil(t, share)
}

// TestNFSShareFindByPath_MultiPath tests finding a share by path in paths array
func TestNFSShareFindByPath_MultiPath(t *testing.T) {
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
			case "sharing.nfs.query":
				callCount++
				if callCount == 1 {
					// First call (with path filter) returns empty
					resp.Result = []interface{}{}
				} else {
					// Second call (all shares) returns share with path in paths array
					resp.Result = []interface{}{
						map[string]interface{}{
							"id":      float64(66),
							"path":    "/mnt/tank/main",
							"paths":   []interface{}{"/mnt/tank/main", "/mnt/tank/k8s/volumes/multi-path"},
							"enabled": true,
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
	share, err := client.NFSShareFindByPath(ctx, "/mnt/tank/k8s/volumes/multi-path")
	require.NoError(t, err)
	assert.NotNil(t, share)
	assert.Equal(t, 66, share.ID)
}

// TestNFSShareList_Success tests listing all NFS shares
func TestNFSShareList_Success(t *testing.T) {
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
			case "sharing.nfs.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":      float64(1),
						"path":    "/mnt/tank/share1",
						"paths":   []interface{}{"/mnt/tank/share1"},
						"enabled": true,
					},
					map[string]interface{}{
						"id":      float64(2),
						"path":    "/mnt/tank/share2",
						"paths":   []interface{}{"/mnt/tank/share2"},
						"enabled": true,
					},
					map[string]interface{}{
						"id":      float64(3),
						"path":    "/mnt/tank/share3",
						"paths":   []interface{}{"/mnt/tank/share3"},
						"enabled": false,
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
	shares, err := client.NFSShareList(ctx)
	require.NoError(t, err)
	assert.Len(t, shares, 3)
	assert.Equal(t, 1, shares[0].ID)
	assert.Equal(t, 2, shares[1].ID)
	assert.Equal(t, 3, shares[2].ID)
	assert.True(t, shares[0].Enabled)
	assert.False(t, shares[2].Enabled)
}

// TestNFSShareList_Empty tests listing shares when none exist
func TestNFSShareList_Empty(t *testing.T) {
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
			case "sharing.nfs.query":
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
	shares, err := client.NFSShareList(ctx)
	require.NoError(t, err)
	assert.Empty(t, shares)
}

// TestNFSShareUpdate_Success tests updating an NFS share
func TestNFSShareUpdate_Success(t *testing.T) {
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
			case "sharing.nfs.update":
				resp.Result = map[string]interface{}{
					"id":       float64(42),
					"path":     "/mnt/tank/k8s/volumes/updated",
					"paths":    []interface{}{"/mnt/tank/k8s/volumes/updated"},
					"comment":  "Updated comment",
					"networks": []interface{}{"192.168.1.0/24"},
					"enabled":  true,
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
	params := map[string]interface{}{
		"comment":  "Updated comment",
		"networks": []string{"192.168.1.0/24"},
	}

	share, err := client.NFSShareUpdate(ctx, 42, params)
	require.NoError(t, err)
	assert.NotNil(t, share)
	assert.Equal(t, "Updated comment", share.Comment)
	assert.Contains(t, share.Networks, "192.168.1.0/24")
}

// TestParseNFSShare tests the parseNFSShare function
func TestParseNFSShare_ValidData(t *testing.T) {
	data := map[string]interface{}{
		"id":           float64(42),
		"path":         "/mnt/tank/test",
		"paths":        []interface{}{"/mnt/tank/test", "/mnt/tank/test2"},
		"comment":      "Test share",
		"networks":     []interface{}{"10.0.0.0/8", "192.168.0.0/16"},
		"hosts":        []interface{}{"host1", "host2"},
		"ro":           true,
		"maproot_user": "root",
		"mapall_user":  "nobody",
		"security":     []interface{}{"sys", "krb5"},
		"enabled":      true,
	}

	share, err := parseNFSShare(data)
	require.NoError(t, err)
	assert.Equal(t, 42, share.ID)
	assert.Equal(t, "/mnt/tank/test", share.Path)
	assert.Len(t, share.Paths, 2)
	assert.Equal(t, "Test share", share.Comment)
	assert.Len(t, share.Networks, 2)
	assert.Contains(t, share.Networks, "10.0.0.0/8")
	assert.Len(t, share.Hosts, 2)
	assert.True(t, share.Ro)
	assert.Equal(t, "root", share.MaprootUser)
	assert.Equal(t, "nobody", share.MapallUser)
	assert.Len(t, share.Security, 2)
	assert.True(t, share.Enabled)
}

// TestParseNFSShare_InvalidData tests parseNFSShare with invalid input
func TestParseNFSShare_InvalidData(t *testing.T) {
	share, err := parseNFSShare("invalid")
	assert.Error(t, err)
	assert.Nil(t, share)
	assert.Contains(t, err.Error(), "unexpected NFS share format")
}

// TestParseNFSShare_MinimalData tests parseNFSShare with minimal data
func TestParseNFSShare_MinimalData(t *testing.T) {
	data := map[string]interface{}{
		"id":   float64(1),
		"path": "/mnt/tank/minimal",
	}

	share, err := parseNFSShare(data)
	require.NoError(t, err)
	assert.Equal(t, 1, share.ID)
	assert.Equal(t, "/mnt/tank/minimal", share.Path)
	assert.Empty(t, share.Paths)
	assert.Empty(t, share.Comment)
	assert.Empty(t, share.Networks)
	assert.Empty(t, share.Hosts)
	assert.False(t, share.Ro)
	assert.False(t, share.Enabled)
}

// TestNFSShareCreate_TableDriven uses table-driven tests for various create scenarios
func TestNFSShareCreate_TableDriven(t *testing.T) {
	tests := []struct {
		name         string
		params       *NFSShareCreateParams
		mockResponse interface{}
		mockError    *rpcError
		expectError  bool
		expectedID   int
	}{
		{
			name: "basic share creation",
			params: &NFSShareCreateParams{
				Path:    "/mnt/tank/basic",
				Comment: "Basic share",
			},
			mockResponse: map[string]interface{}{
				"id":      float64(1),
				"path":    "/mnt/tank/basic",
				"enabled": true,
			},
			expectError: false,
			expectedID:  1,
		},
		{
			name: "share with networks",
			params: &NFSShareCreateParams{
				Path:     "/mnt/tank/networks",
				Networks: []string{"10.0.0.0/8"},
			},
			mockResponse: map[string]interface{}{
				"id":       float64(2),
				"path":     "/mnt/tank/networks",
				"networks": []interface{}{"10.0.0.0/8"},
				"enabled":  true,
			},
			expectError: false,
			expectedID:  2,
		},
		{
			name: "readonly share",
			params: &NFSShareCreateParams{
				Path: "/mnt/tank/readonly",
				Ro:   true,
			},
			mockResponse: map[string]interface{}{
				"id":      float64(3),
				"path":    "/mnt/tank/readonly",
				"ro":      true,
				"enabled": true,
			},
			expectError: false,
			expectedID:  3,
		},
		{
			name: "share with maproot",
			params: &NFSShareCreateParams{
				Path:        "/mnt/tank/maproot",
				MaprootUser: "root",
			},
			mockResponse: map[string]interface{}{
				"id":           float64(4),
				"path":         "/mnt/tank/maproot",
				"maproot_user": "root",
				"enabled":      true,
			},
			expectError: false,
			expectedID:  4,
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
					case "sharing.nfs.create":
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
			share, err := client.NFSShareCreate(ctx, tt.params)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, share)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, share)
				assert.Equal(t, tt.expectedID, share.ID)
			}
		})
	}
}

// TestMockClient_NFSOperations tests the MockClient NFS operations
func TestMockClient_NFSOperations(t *testing.T) {
	mock := NewMockClient()
	ctx := context.Background()

	// Test create
	params := &NFSShareCreateParams{
		Path:    "/mnt/tank/test/nfs",
		Comment: "Test NFS share",
	}
	share, err := mock.NFSShareCreate(ctx, params)
	require.NoError(t, err)
	assert.NotNil(t, share)
	assert.Equal(t, "/mnt/tank/test/nfs", share.Path)
	shareID := share.ID

	// Test get
	share, err = mock.NFSShareGet(ctx, shareID)
	require.NoError(t, err)
	assert.Equal(t, "/mnt/tank/test/nfs", share.Path)

	// Test find by path
	share, err = mock.NFSShareFindByPath(ctx, "/mnt/tank/test/nfs")
	require.NoError(t, err)
	assert.NotNil(t, share)
	assert.Equal(t, shareID, share.ID)

	// Test list
	shares, err := mock.NFSShareList(ctx)
	require.NoError(t, err)
	assert.Len(t, shares, 1)

	// Test delete
	err = mock.NFSShareDelete(ctx, shareID)
	require.NoError(t, err)

	// Verify deletion
	share, err = mock.NFSShareFindByPath(ctx, "/mnt/tank/test/nfs")
	require.NoError(t, err)
	assert.Nil(t, share)
}

// TestMockClient_NFSErrorInjection tests error injection in MockClient for NFS
func TestMockClient_NFSErrorInjection(t *testing.T) {
	mock := NewMockClient()
	ctx := context.Background()

	// Inject error
	mock.InjectError = fmt.Errorf("injected NFS error")

	params := &NFSShareCreateParams{
		Path: "/mnt/tank/fail",
	}
	_, err := mock.NFSShareCreate(ctx, params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "injected NFS error")

	// Clear error
	mock.InjectError = nil

	// Should succeed now
	share, err := mock.NFSShareCreate(ctx, params)
	require.NoError(t, err)
	assert.NotNil(t, share)
}

// Benchmark tests
func BenchmarkParseNFSShare(b *testing.B) {
	data := map[string]interface{}{
		"id":           float64(42),
		"path":         "/mnt/tank/test",
		"paths":        []interface{}{"/mnt/tank/test", "/mnt/tank/test2"},
		"comment":      "Test share",
		"networks":     []interface{}{"10.0.0.0/8", "192.168.0.0/16"},
		"hosts":        []interface{}{"host1", "host2", "host3"},
		"ro":           true,
		"maproot_user": "root",
		"mapall_user":  "nobody",
		"security":     []interface{}{"sys", "krb5"},
		"enabled":      true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parseNFSShare(data)
	}
}

