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

// =============================================================================
// Tests for "Invalid params" Verification Fix
// These tests validate that when TrueNAS returns "Invalid params", the code
// properly verifies whether the resource exists before treating it as deleted.
// =============================================================================

// TestISCSITargetCreate_InvalidParams_ExistingTarget tests that when iSCSI target
// creation returns "Invalid params", we check if the target already exists.
// This tests the fix in iscsi.go for ambiguous error handling.
func TestISCSITargetCreate_InvalidParams_ExistingTarget(t *testing.T) {
	mock := newMockWSServer()
	targetExists := true // Simulates target already exists

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
			case "iscsi.target.create":
				// TrueNAS returns "Invalid params" when target already exists
				resp.Error = &rpcError{
					Code:    -1,
					Message: "Invalid params",
				}
			case "iscsi.target.query":
				if targetExists {
					// Return existing target
					resp.Result = []interface{}{
						map[string]interface{}{
							"id":     float64(42),
							"name":   "existing-target",
							"alias":  "",
							"mode":   "ISCSI",
							"groups": []interface{}{},
						},
					}
				} else {
					resp.Result = []interface{}{}
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
	target, err := client.ISCSITargetCreate(ctx, "existing-target", "", "ISCSI", nil)

	// Should succeed by returning the existing target
	require.NoError(t, err)
	require.NotNil(t, target)
	assert.Equal(t, 42, target.ID)
	assert.Equal(t, "existing-target", target.Name)
}

// TestISCSIExtentCreate_InvalidParams_ExistingExtent tests extent creation with
// "Invalid params" when extent already exists.
func TestISCSIExtentCreate_InvalidParams_ExistingExtent(t *testing.T) {
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
			case "iscsi.extent.create":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "Invalid params",
				}
			case "iscsi.extent.query":
				// Return existing extent
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":      float64(99),
						"name":   "existing-extent",
						"type":   "DISK",
						"disk":   "zvol/tank/vol1",
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
	extent, err := client.ISCSIExtentCreate(ctx, "existing-extent", "zvol/tank/vol1", "comment", 512, "SSD")

	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 99, extent.ID)
	assert.Equal(t, "existing-extent", extent.Name)
}

// TestISCSITargetExtentCreate_InvalidParams_ExistingAssociation tests target-extent
// association creation with "Invalid params".
func TestISCSITargetExtentCreate_InvalidParams_ExistingAssociation(t *testing.T) {
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
			case "iscsi.targetextent.create":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "Invalid params",
				}
			case "iscsi.targetextent.query":
				resp.Result = []interface{}{
					map[string]interface{}{
						"id":     float64(77),
						"target": float64(1),
						"extent": float64(2),
						"lunid":  float64(0),
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
	te, err := client.ISCSITargetExtentCreate(ctx, 1, 2, 0)

	require.NoError(t, err)
	require.NotNil(t, te)
	assert.Equal(t, 77, te.ID)
	assert.Equal(t, 1, te.Target)
	assert.Equal(t, 2, te.Extent)
}

// =============================================================================
// NVMe-oF "Invalid params" Verification Tests
// =============================================================================

// TestNVMeoFSubsystemDelete_InvalidParams_StillExists tests that when NVMe-oF
// subsystem delete returns "Invalid params", we verify if the resource still exists.
// This tests the fix for: "Handle Invalid params as not found in NVMe-oF delete operations"
func TestNVMeoFSubsystemDelete_InvalidParams_StillExists(t *testing.T) {
	mock := newMockWSServer()
	subsystemStillExists := true

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
			case "system.info":
				resp.Result = map[string]interface{}{
					"version":  "TrueNAS-SCALE-25.10.0",
					"hostname": "truenas-test",
				}
			case "nvmet.subsys.delete":
				// TrueNAS returns "Invalid params" which is ambiguous
				resp.Error = &rpcError{
					Code:    -1,
					Message: "Invalid params",
				}
			case "nvmet.subsys.query":
				if subsystemStillExists {
					// Subsystem still exists - delete failed for parameter error
					resp.Result = []interface{}{
						map[string]interface{}{
							"id":             float64(5),
							"name":           "test-subsys",
							"subnqn":         "nqn.2011-06.com.truenas:test-subsys",
							"allow_any_host": true,
						},
					}
				} else {
					// Subsystem doesn't exist - delete succeeded
					resp.Result = []interface{}{}
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
	err = client.NVMeoFSubsystemDelete(ctx, 5)

	// Since subsystem still exists, should return error (not treat as success)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete NVMe-oF subsystem")
}

// TestNVMeoFSubsystemDelete_InvalidParams_AlreadyDeleted tests that when the
// subsystem no longer exists, "Invalid params" is treated as success.
func TestNVMeoFSubsystemDelete_InvalidParams_AlreadyDeleted(t *testing.T) {
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
			case "system.info":
				resp.Result = map[string]interface{}{
					"version":  "TrueNAS-SCALE-25.10.0",
					"hostname": "truenas-test",
				}
			case "nvmet.subsys.delete":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "Invalid params",
				}
			case "nvmet.subsys.query":
				// Subsystem doesn't exist
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
	err = client.NVMeoFSubsystemDelete(ctx, 999)

	// Subsystem doesn't exist, so "Invalid params" should be treated as success
	assert.NoError(t, err)
}

// TestNVMeoFNamespaceDelete_InvalidParams_Verification tests namespace delete
// with "Invalid params" verification.
func TestNVMeoFNamespaceDelete_InvalidParams_Verification(t *testing.T) {
	tests := []struct {
		name             string
		namespaceExists  bool
		expectError      bool
	}{
		{
			name:            "namespace still exists - should error",
			namespaceExists: true,
			expectError:     true,
		},
		{
			name:            "namespace already deleted - should succeed",
			namespaceExists: false,
			expectError:     false,
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

					resp := rpcTestResponse{
						JSONRPC: "2.0",
						ID:      req.ID,
					}

					switch req.Method {
					case "auth.login_with_api_key":
						resp.Result = true
					case "system.info":
						resp.Result = map[string]interface{}{
							"version":  "TrueNAS-SCALE-25.10.0",
							"hostname": "truenas-test",
						}
					case "nvmet.namespace.delete":
						resp.Error = &rpcError{
							Code:    -1,
							Message: "Invalid params",
						}
					case "nvmet.namespace.query":
						if tt.namespaceExists {
							resp.Result = []interface{}{
								map[string]interface{}{
									"id":          float64(10),
									"device_path": "zvol/tank/vol1",
									"device_type": "ZVOL",
									"enabled":     true,
								},
							}
						} else {
							resp.Result = []interface{}{}
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
			err = client.NVMeoFNamespaceDelete(ctx, 10)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestNVMeoFPortSubsysDelete_InvalidParams_Verification tests port-subsystem
// association delete with "Invalid params" verification.
func TestNVMeoFPortSubsysDelete_InvalidParams_Verification(t *testing.T) {
	tests := []struct {
		name            string
		assocExists     bool
		expectError     bool
	}{
		{
			name:        "association still exists - should error",
			assocExists: true,
			expectError: true,
		},
		{
			name:        "association already deleted - should succeed",
			assocExists: false,
			expectError: false,
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

					resp := rpcTestResponse{
						JSONRPC: "2.0",
						ID:      req.ID,
					}

					switch req.Method {
					case "auth.login_with_api_key":
						resp.Result = true
					case "system.info":
						resp.Result = map[string]interface{}{
							"version":  "TrueNAS-SCALE-25.10.0",
							"hostname": "truenas-test",
						}
					case "nvmet.port_subsys.delete":
						resp.Error = &rpcError{
							Code:    -1,
							Message: "Invalid params",
						}
					case "nvmet.port_subsys.query":
						if tt.assocExists {
							resp.Result = []interface{}{
								map[string]interface{}{
									"id": float64(100),
									"port": map[string]interface{}{
										"id": float64(1),
									},
									"subsys": map[string]interface{}{
										"id": float64(5),
									},
								},
							}
						} else {
							resp.Result = []interface{}{}
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
			err = client.NVMeoFPortSubsysDelete(ctx, 100)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// =============================================================================
// Additional iSCSI Delete Tests with Explicit "does not exist" handling
// =============================================================================

// TestISCSITargetDelete_DoesNotExist tests that "does not exist" error is
// properly handled as success (idempotent delete).
func TestISCSITargetDelete_DoesNotExist(t *testing.T) {
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
			case "iscsi.target.delete":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "Target does not exist",
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
	err = client.ISCSITargetDelete(ctx, 999, false)

	// Should succeed (idempotent)
	assert.NoError(t, err)
}

// TestISCSIExtentDelete_NotFound tests that "not found" error is properly
// handled as success (idempotent delete).
func TestISCSIExtentDelete_NotFound(t *testing.T) {
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
			case "iscsi.extent.delete":
				resp.Error = &rpcError{
					Code:    -1,
					Message: "Extent not found",
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
	err = client.ISCSIExtentDelete(ctx, 999, false, false)

	// Should succeed (idempotent)
	assert.NoError(t, err)
}
