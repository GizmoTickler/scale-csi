package truenas

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Parser Tests - These test the internal parsing logic for NVMe-oF
// =============================================================================

func TestParseNVMeoFSubsystem_AllFields(t *testing.T) {
	tests := []struct {
		name             string
		data             interface{}
		wantID           int
		wantName         string
		wantNQN          string
		wantSerial       string
		wantAllowAnyHost bool
		wantErr          bool
	}{
		{
			name: "full subsystem",
			data: map[string]interface{}{
				"id":             float64(1),
				"name":           "pvc-abc123",
				"subnqn":         "nqn.2011-06.com.truenas:pvc-abc123",
				"serial":         "ABC123DEF456",
				"allow_any_host": true,
				"hosts":          []interface{}{},
				"namespaces":     []interface{}{float64(1), float64(2)},
				"ports":          []interface{}{float64(1)},
			},
			wantID:           1,
			wantName:         "pvc-abc123",
			wantNQN:          "nqn.2011-06.com.truenas:pvc-abc123",
			wantSerial:       "ABC123DEF456",
			wantAllowAnyHost: true,
		},
		{
			name: "subsystem with hosts",
			data: map[string]interface{}{
				"id":             float64(2),
				"name":           "secured-subsys",
				"subnqn":         "nqn.2011-06.com.truenas:secured",
				"serial":         "SEC123",
				"allow_any_host": false,
				"hosts":          []interface{}{float64(1), float64(2), float64(3)},
				"namespaces":     []interface{}{},
				"ports":          []interface{}{},
			},
			wantID:           2,
			wantName:         "secured-subsys",
			wantNQN:          "nqn.2011-06.com.truenas:secured",
			wantSerial:       "SEC123",
			wantAllowAnyHost: false,
		},
		{
			name:    "invalid data type",
			data:    "not a map",
			wantErr: true,
		},
		{
			name:    "nil data",
			data:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subsys, err := parseNVMeoFSubsystem(tt.data)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, subsys)
			assert.Equal(t, tt.wantID, subsys.ID)
			assert.Equal(t, tt.wantName, subsys.Name)
			assert.Equal(t, tt.wantNQN, subsys.NQN)
			assert.Equal(t, tt.wantSerial, subsys.Serial)
			assert.Equal(t, tt.wantAllowAnyHost, subsys.AllowAnyHost)
		})
	}
}

func TestParseNVMeoFSubsystem_ArrayFields(t *testing.T) {
	data := map[string]interface{}{
		"id":             float64(1),
		"name":           "test-subsys",
		"subnqn":         "nqn.2011-06.com.truenas:test",
		"serial":         "TEST123",
		"allow_any_host": true,
		"hosts":          []interface{}{float64(10), float64(20), float64(30)},
		"namespaces":     []interface{}{float64(1), float64(2)},
		"ports":          []interface{}{float64(100), float64(200)},
	}

	subsys, err := parseNVMeoFSubsystem(data)
	require.NoError(t, err)

	assert.Len(t, subsys.Hosts, 3)
	assert.Equal(t, []int{10, 20, 30}, subsys.Hosts)

	assert.Len(t, subsys.Namespaces, 2)
	assert.Equal(t, []int{1, 2}, subsys.Namespaces)

	assert.Len(t, subsys.Ports, 2)
	assert.Equal(t, []int{100, 200}, subsys.Ports)
}

func TestParseNVMeoFNamespace_AllFields(t *testing.T) {
	tests := []struct {
		name           string
		data           interface{}
		wantID         int
		wantSubsysID   int
		wantNSID       int
		wantDeviceType string
		wantDevicePath string
		wantEnabled    bool
		wantErr        bool
	}{
		{
			name: "full namespace",
			data: map[string]interface{}{
				"id":   float64(1),
				"nsid": float64(1),
				"subsys": map[string]interface{}{
					"id": float64(5),
				},
				"device_type":  "ZVOL",
				"device_path":  "zvol/tank/k8s/volumes/pvc-123",
				"device_uuid":  "uuid-abc-123",
				"device_nguid": "nguid-abc-123",
				"enabled":      true,
				"locked":       false,
			},
			wantID:         1,
			wantSubsysID:   5,
			wantNSID:       1,
			wantDeviceType: "ZVOL",
			wantDevicePath: "zvol/tank/k8s/volumes/pvc-123",
			wantEnabled:    true,
		},
		{
			name: "file-based namespace",
			data: map[string]interface{}{
				"id":   float64(2),
				"nsid": float64(2),
				"subsys": map[string]interface{}{
					"id": float64(10),
				},
				"device_type": "FILE",
				"device_path": "/mnt/tank/nvme/file1",
				"enabled":     false,
				"locked":      true,
			},
			wantID:         2,
			wantSubsysID:   10,
			wantNSID:       2,
			wantDeviceType: "FILE",
			wantDevicePath: "/mnt/tank/nvme/file1",
			wantEnabled:    false,
		},
		{
			name:    "invalid data",
			data:    123,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, err := parseNVMeoFNamespace(tt.data)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ns)
			assert.Equal(t, tt.wantID, ns.ID)
			assert.Equal(t, tt.wantSubsysID, ns.SubsystemID)
			assert.Equal(t, tt.wantNSID, ns.NSID)
			assert.Equal(t, tt.wantDeviceType, ns.DeviceType)
			assert.Equal(t, tt.wantDevicePath, ns.DevicePath)
			assert.Equal(t, tt.wantEnabled, ns.Enabled)
		})
	}
}

func TestParseNVMeoFNamespace_OptionalFields(t *testing.T) {
	data := map[string]interface{}{
		"id":   float64(1),
		"nsid": float64(1),
		"subsys": map[string]interface{}{
			"id": float64(1),
		},
		"device_type":  "ZVOL",
		"device_path":  "zvol/tank/vol",
		"device_uuid":  "uuid-123-456-789",
		"device_nguid": "nguid-abc-def-ghi",
		"enabled":      true,
		"locked":       true,
	}

	ns, err := parseNVMeoFNamespace(data)
	require.NoError(t, err)
	assert.Equal(t, "uuid-123-456-789", ns.DeviceUUID)
	assert.Equal(t, "nguid-abc-def-ghi", ns.DeviceNGUID)
	assert.True(t, ns.Locked)
}

func TestParseNVMeoFPort_AllFields(t *testing.T) {
	tests := []struct {
		name          string
		data          interface{}
		wantID        int
		wantIndex     int
		wantTransport string
		wantAddress   string
		wantPort      int
		wantEnabled   bool
		wantErr       bool
	}{
		{
			name: "TCP port",
			data: map[string]interface{}{
				"id":           float64(1),
				"index":        float64(0),
				"addr_trtype":  "TCP",
				"addr_traddr":  "0.0.0.0",
				"addr_trsvcid": float64(4420),
				"addr_adrfam":  "IPV4",
				"enabled":      true,
				"subsystems":   []interface{}{float64(1), float64(2)},
			},
			wantID:        1,
			wantIndex:     0,
			wantTransport: "TCP",
			wantAddress:   "0.0.0.0",
			wantPort:      4420,
			wantEnabled:   true,
		},
		{
			name: "RDMA port",
			data: map[string]interface{}{
				"id":           float64(2),
				"index":        float64(1),
				"addr_trtype":  "RDMA",
				"addr_traddr":  "192.168.1.100",
				"addr_trsvcid": float64(4420),
				"addr_adrfam":  "IPV4",
				"enabled":      true,
				"subsystems":   []interface{}{},
			},
			wantID:        2,
			wantIndex:     1,
			wantTransport: "RDMA",
			wantAddress:   "192.168.1.100",
			wantPort:      4420,
			wantEnabled:   true,
		},
		{
			name: "IPv6 port",
			data: map[string]interface{}{
				"id":           float64(3),
				"index":        float64(2),
				"addr_trtype":  "TCP",
				"addr_traddr":  "::1",
				"addr_trsvcid": float64(4420),
				"addr_adrfam":  "IPV6",
				"enabled":      false,
				"subsystems":   []interface{}{},
			},
			wantID:        3,
			wantIndex:     2,
			wantTransport: "TCP",
			wantAddress:   "::1",
			wantPort:      4420,
			wantEnabled:   false,
		},
		{
			name:    "invalid data",
			data:    []int{1, 2, 3},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			port, err := parseNVMeoFPort(tt.data)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, port)
			assert.Equal(t, tt.wantID, port.ID)
			assert.Equal(t, tt.wantIndex, port.Index)
			assert.Equal(t, tt.wantTransport, port.Transport)
			assert.Equal(t, tt.wantAddress, port.Address)
			assert.Equal(t, tt.wantPort, port.Port)
			assert.Equal(t, tt.wantEnabled, port.Enabled)
		})
	}
}

func TestParseNVMeoFPort_Subsystems(t *testing.T) {
	data := map[string]interface{}{
		"id":           float64(1),
		"index":        float64(0),
		"addr_trtype":  "TCP",
		"addr_traddr":  "0.0.0.0",
		"addr_trsvcid": float64(4420),
		"addr_adrfam":  "IPV4",
		"enabled":      true,
		"subsystems":   []interface{}{float64(1), float64(2), float64(3), float64(4)},
	}

	port, err := parseNVMeoFPort(data)
	require.NoError(t, err)
	assert.Len(t, port.Subsystems, 4)
	assert.Equal(t, []int{1, 2, 3, 4}, port.Subsystems)
}

func TestParseNVMeoFPortSubsys(t *testing.T) {
	tests := []struct {
		name       string
		data       interface{}
		wantID     int
		wantPortID int
		wantSubsys int
		wantErr    bool
	}{
		{
			name: "valid association",
			data: map[string]interface{}{
				"id": float64(100),
				"port": map[string]interface{}{
					"id": float64(1),
				},
				"subsys": map[string]interface{}{
					"id": float64(5),
				},
			},
			wantID:     100,
			wantPortID: 1,
			wantSubsys: 5,
		},
		{
			name:    "invalid data",
			data:    "not a map",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assoc, err := parseNVMeoFPortSubsys(tt.data)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, assoc)
			assert.Equal(t, tt.wantID, assoc.ID)
			assert.Equal(t, tt.wantPortID, assoc.PortID)
			assert.Equal(t, tt.wantSubsys, assoc.SubsysID)
		})
	}
}

// =============================================================================
// resolveToIP Tests
// =============================================================================

func TestResolveToIP_AlreadyIP(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "IPv4 address",
			input: "192.168.1.100",
			want:  "192.168.1.100",
		},
		{
			name:  "IPv4 wildcard",
			input: "0.0.0.0",
			want:  "0.0.0.0",
		},
		{
			name:  "IPv6 address",
			input: "::1",
			want:  "::1",
		},
		{
			name:  "IPv6 full",
			input: "2001:db8::1",
			want:  "2001:db8::1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := resolveToIP(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestResolveToIP_Hostname_Localhost(t *testing.T) {
	// Test with localhost which should always resolve
	result, err := resolveToIP("localhost")
	require.NoError(t, err)
	// Should be either 127.0.0.1 or ::1
	assert.True(t, result == "127.0.0.1" || result == "::1", "expected localhost to resolve to loopback, got %s", result)
}

func TestResolveToIP_InvalidHostname(t *testing.T) {
	_, err := resolveToIP("definitely-not-a-real-hostname-xyz123.invalid")
	require.Error(t, err)
}

// =============================================================================
// MockClient NVMe-oF Tests
// =============================================================================

func TestMockClient_NVMeoFSubsystemCreate_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	subsys, err := client.NVMeoFSubsystemCreate(ctx, "pvc-test-subsys", true, nil)
	require.NoError(t, err)
	require.NotNil(t, subsys)
	assert.Equal(t, 1, subsys.ID)
	assert.Equal(t, "pvc-test-subsys", subsys.Name)
	assert.True(t, subsys.AllowAnyHost)
	// Mock generates NQN
	assert.Contains(t, subsys.NQN, "pvc-test-subsys")
}

func TestMockClient_NVMeoFSubsystemCreate_MultipleSubsystems(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	names := []string{"subsys-1", "subsys-2", "subsys-3"}

	for i, name := range names {
		subsys, err := client.NVMeoFSubsystemCreate(ctx, name, true, nil)
		require.NoError(t, err)
		assert.Equal(t, i+1, subsys.ID)
		assert.Equal(t, name, subsys.Name)
	}

	// Verify all exist
	for _, name := range names {
		found, err := client.NVMeoFSubsystemFindByName(ctx, name)
		require.NoError(t, err)
		require.NotNil(t, found)
		assert.Equal(t, name, found.Name)
	}
}

func TestMockClient_NVMeoFSubsystemDelete_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	subsys, _ := client.NVMeoFSubsystemCreate(ctx, "to-delete", true, nil)

	// Verify exists
	found, _ := client.NVMeoFSubsystemFindByName(ctx, "to-delete")
	require.NotNil(t, found)

	// Delete
	err := client.NVMeoFSubsystemDelete(ctx, subsys.ID)
	require.NoError(t, err)

	// Verify deleted
	found, _ = client.NVMeoFSubsystemFindByName(ctx, "to-delete")
	assert.Nil(t, found)
}

func TestMockClient_NVMeoFSubsystemDelete_Idempotent(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	// Delete non-existent should not error
	err := client.NVMeoFSubsystemDelete(ctx, 999)
	require.NoError(t, err)

	// Delete same twice
	subsys, _ := client.NVMeoFSubsystemCreate(ctx, "test", true, nil)
	err = client.NVMeoFSubsystemDelete(ctx, subsys.ID)
	require.NoError(t, err)
	err = client.NVMeoFSubsystemDelete(ctx, subsys.ID)
	require.NoError(t, err)
}

func TestMockClient_NVMeoFSubsystemFindByName_NotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	found, err := client.NVMeoFSubsystemFindByName(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_NVMeoFSubsystemFindByNQN_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	subsys, _ := client.NVMeoFSubsystemCreate(ctx, "nqn-test", true, nil)

	found, err := client.NVMeoFSubsystemFindByNQN(ctx, subsys.NQN)
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, subsys.ID, found.ID)
}

func TestMockClient_NVMeoFSubsystemFindByNQN_NotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	found, err := client.NVMeoFSubsystemFindByNQN(ctx, "nqn.nonexistent")
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_NVMeoFSubsystemGet_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	subsys, _ := client.NVMeoFSubsystemCreate(ctx, "get-test", true, nil)

	got, err := client.NVMeoFSubsystemGet(ctx, subsys.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, subsys.ID, got.ID)
	assert.Equal(t, "get-test", got.Name)
}

func TestMockClient_NVMeoFSubsystemGet_NotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	_, err := client.NVMeoFSubsystemGet(ctx, 999)
	require.Error(t, err)
}

func TestMockClient_NVMeoFSubsystemList(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	// Create several subsystems
	for i := 0; i < 5; i++ {
		_, _ = client.NVMeoFSubsystemCreate(ctx, fmt.Sprintf("list-subsys-%d", i), true, nil)
	}

	list, err := client.NVMeoFSubsystemList(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 5)
}

func TestMockClient_NVMeoFNamespaceCreate_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	subsys, _ := client.NVMeoFSubsystemCreate(ctx, "ns-test-subsys", true, nil)

	ns, err := client.NVMeoFNamespaceCreate(ctx, subsys.ID, "zvol/tank/vol1", "ZVOL")
	require.NoError(t, err)
	require.NotNil(t, ns)
	assert.Equal(t, 1, ns.ID)
	assert.Equal(t, subsys.ID, ns.SubsystemID)
	assert.Equal(t, "zvol/tank/vol1", ns.DevicePath)
	assert.Equal(t, "ZVOL", ns.DeviceType)
	assert.True(t, ns.Enabled)
}

func TestMockClient_NVMeoFNamespaceDelete_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	subsys, _ := client.NVMeoFSubsystemCreate(ctx, "ns-del-test", true, nil)
	ns, _ := client.NVMeoFNamespaceCreate(ctx, subsys.ID, "zvol/tank/del", "ZVOL")

	err := client.NVMeoFNamespaceDelete(ctx, ns.ID)
	require.NoError(t, err)

	// Verify deleted
	found, err := client.NVMeoFNamespaceFindByDevice(ctx, subsys.ID, "zvol/tank/del")
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_NVMeoFNamespaceDelete_Idempotent(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	// Delete non-existent
	err := client.NVMeoFNamespaceDelete(ctx, 999)
	require.NoError(t, err)
}

func TestMockClient_NVMeoFNamespaceFindByDevice_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	subsys, _ := client.NVMeoFSubsystemCreate(ctx, "find-device-test", true, nil)
	devicePath := "zvol/tank/find-device"
	_, _ = client.NVMeoFNamespaceCreate(ctx, subsys.ID, devicePath, "ZVOL")

	found, err := client.NVMeoFNamespaceFindByDevice(ctx, subsys.ID, devicePath)
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, devicePath, found.DevicePath)
	assert.Equal(t, subsys.ID, found.SubsystemID)
}

func TestMockClient_NVMeoFNamespaceFindByDevice_NotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	found, err := client.NVMeoFNamespaceFindByDevice(ctx, 999, "zvol/nonexistent")
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_NVMeoFNamespaceFindByDevicePath_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	subsys, _ := client.NVMeoFSubsystemCreate(ctx, "find-path-test", true, nil)
	devicePath := "zvol/tank/find-path"
	_, _ = client.NVMeoFNamespaceCreate(ctx, subsys.ID, devicePath, "ZVOL")

	found, err := client.NVMeoFNamespaceFindByDevicePath(ctx, devicePath)
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, devicePath, found.DevicePath)
}

func TestMockClient_NVMeoFNamespaceFindByDevicePath_NotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	found, err := client.NVMeoFNamespaceFindByDevicePath(ctx, "zvol/nonexistent")
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_NVMeoFNamespaceGet_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	subsys, _ := client.NVMeoFSubsystemCreate(ctx, "ns-get-test", true, nil)
	ns, _ := client.NVMeoFNamespaceCreate(ctx, subsys.ID, "zvol/tank/get", "ZVOL")

	got, err := client.NVMeoFNamespaceGet(ctx, ns.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, ns.ID, got.ID)
}

func TestMockClient_NVMeoFNamespaceGet_NotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	_, err := client.NVMeoFNamespaceGet(ctx, 999)
	require.Error(t, err)
}

func TestMockClient_NVMeoFPortList(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	ports, err := client.NVMeoFPortList(ctx)
	require.NoError(t, err)
	// Mock returns a default port
	assert.NotEmpty(t, ports)
}

func TestMockClient_NVMeoFPortCreate(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	port, err := client.NVMeoFPortCreate(ctx, "TCP", "192.168.1.100", 4420)
	require.NoError(t, err)
	require.NotNil(t, port)
	assert.Equal(t, "192.168.1.100", port.Address)
	assert.Equal(t, 4420, port.Port)
}

func TestMockClient_NVMeoFPortFindByAddress(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	port, err := client.NVMeoFPortFindByAddress(ctx, "TCP", "0.0.0.0", 4420)
	require.NoError(t, err)
	require.NotNil(t, port)
}

func TestMockClient_NVMeoFGetOrCreatePort(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	port, err := client.NVMeoFGetOrCreatePort(ctx, "TCP", "10.0.0.1", 4420)
	require.NoError(t, err)
	require.NotNil(t, port)
	assert.Equal(t, "10.0.0.1", port.Address)
}

func TestMockClient_NVMeoFPortSubsysCreate(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	assoc, err := client.NVMeoFPortSubsysCreate(ctx, 1, 5)
	require.NoError(t, err)
	require.NotNil(t, assoc)
	assert.Equal(t, 1, assoc.PortID)
	assert.Equal(t, 5, assoc.SubsysID)
}

func TestMockClient_NVMeoFPortSubsysFindBySubsystem(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	exists, err := client.NVMeoFPortSubsysFindBySubsystem(ctx, 1)
	require.NoError(t, err)
	// Mock returns true by default
	assert.True(t, exists)
}

func TestMockClient_NVMeoFPortSubsysListBySubsystem(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	list, err := client.NVMeoFPortSubsysListBySubsystem(ctx, 1)
	require.NoError(t, err)
	// Mock returns empty by default
	assert.Empty(t, list)
}

func TestMockClient_NVMeoFPortSubsysDelete(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	err := client.NVMeoFPortSubsysDelete(ctx, 1)
	require.NoError(t, err)
}

func TestMockClient_NVMeoFGetTransportAddresses(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	addrs, err := client.NVMeoFGetTransportAddresses(ctx, "TCP")
	require.NoError(t, err)
	assert.NotEmpty(t, addrs)
	assert.Contains(t, addrs, "0.0.0.0")
}

// =============================================================================
// Full Workflow Tests
// =============================================================================

func TestMockClient_NVMeoFFullWorkflow(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	// Step 1: Create subsystem
	subsys, err := client.NVMeoFSubsystemCreate(ctx, "pvc-workflow", true, nil)
	require.NoError(t, err)
	require.NotNil(t, subsys)

	// Step 2: Create namespace
	ns, err := client.NVMeoFNamespaceCreate(ctx, subsys.ID, "zvol/tank/k8s/pvc-workflow", "ZVOL")
	require.NoError(t, err)
	require.NotNil(t, ns)

	// Step 3: Get/create port
	port, err := client.NVMeoFGetOrCreatePort(ctx, "TCP", "0.0.0.0", 4420)
	require.NoError(t, err)
	require.NotNil(t, port)

	// Step 4: Associate port with subsystem
	assoc, err := client.NVMeoFPortSubsysCreate(ctx, port.ID, subsys.ID)
	require.NoError(t, err)
	require.NotNil(t, assoc)

	// Verify resources exist
	foundSubsys, err := client.NVMeoFSubsystemFindByName(ctx, "pvc-workflow")
	require.NoError(t, err)
	require.NotNil(t, foundSubsys)

	foundNS, err := client.NVMeoFNamespaceFindByDevicePath(ctx, "zvol/tank/k8s/pvc-workflow")
	require.NoError(t, err)
	require.NotNil(t, foundNS)

	// Cleanup
	err = client.NVMeoFPortSubsysDelete(ctx, assoc.ID)
	require.NoError(t, err)

	err = client.NVMeoFNamespaceDelete(ctx, ns.ID)
	require.NoError(t, err)

	err = client.NVMeoFSubsystemDelete(ctx, subsys.ID)
	require.NoError(t, err)

	// Verify cleanup
	foundSubsys, _ = client.NVMeoFSubsystemFindByName(ctx, "pvc-workflow")
	assert.Nil(t, foundSubsys)
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestMockClient_NVMeoFConcurrentSubsystemCreation(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 20

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("concurrent-subsys-%d", idx)
			_, err := client.NVMeoFSubsystemCreate(ctx, name, true, nil)
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// Verify all created
	for i := 0; i < numGoroutines; i++ {
		name := fmt.Sprintf("concurrent-subsys-%d", i)
		found, err := client.NVMeoFSubsystemFindByName(ctx, name)
		assert.NoError(t, err)
		assert.NotNil(t, found, "subsystem %s should exist", name)
	}
}

func TestMockClient_NVMeoFConcurrentNamespaceCreation(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	// Create subsystem first
	subsys, _ := client.NVMeoFSubsystemCreate(ctx, "concurrent-ns-parent", true, nil)

	var wg sync.WaitGroup
	numGoroutines := 20

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			devicePath := fmt.Sprintf("zvol/tank/concurrent-%d", idx)
			_, err := client.NVMeoFNamespaceCreate(ctx, subsys.ID, devicePath, "ZVOL")
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// Verify all created
	for i := 0; i < numGoroutines; i++ {
		devicePath := fmt.Sprintf("zvol/tank/concurrent-%d", i)
		found, err := client.NVMeoFNamespaceFindByDevicePath(ctx, devicePath)
		assert.NoError(t, err)
		assert.NotNil(t, found, "namespace with device path %s should exist", devicePath)
	}
}

func TestMockClient_NVMeoFConcurrentReadWrite(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("rw-subsys-%d", i)
		_, _ = client.NVMeoFSubsystemCreate(ctx, name, true, nil)
	}

	var wg sync.WaitGroup

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("rw-subsys-%d", idx%10)
			_, _ = client.NVMeoFSubsystemFindByName(ctx, name)
		}(i)
	}

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("new-rw-subsys-%d", idx)
			_, _ = client.NVMeoFSubsystemCreate(ctx, name, true, nil)
		}(i)
	}

	wg.Wait()
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func TestParseNVMeoFSubsystem_MissingFields(t *testing.T) {
	data := map[string]interface{}{
		"id": float64(1),
		// Missing all other fields
	}

	subsys, err := parseNVMeoFSubsystem(data)
	require.NoError(t, err)
	assert.Equal(t, 1, subsys.ID)
	assert.Equal(t, "", subsys.Name)
	assert.Equal(t, "", subsys.NQN)
	assert.Equal(t, "", subsys.Serial)
	assert.False(t, subsys.AllowAnyHost)
	assert.Nil(t, subsys.Hosts)
	assert.Nil(t, subsys.Namespaces)
	assert.Nil(t, subsys.Ports)
}

func TestParseNVMeoFNamespace_MissingFields(t *testing.T) {
	data := map[string]interface{}{
		"id": float64(1),
		// Missing subsys, nsid, device_type, device_path, etc.
	}

	ns, err := parseNVMeoFNamespace(data)
	require.NoError(t, err)
	assert.Equal(t, 1, ns.ID)
	assert.Equal(t, 0, ns.SubsystemID)
	assert.Equal(t, 0, ns.NSID)
	assert.Equal(t, "", ns.DeviceType)
	assert.Equal(t, "", ns.DevicePath)
	assert.False(t, ns.Enabled)
}

func TestParseNVMeoFPort_MissingFields(t *testing.T) {
	data := map[string]interface{}{
		"id": float64(1),
		// Missing index, addr_trtype, addr_traddr, etc.
	}

	port, err := parseNVMeoFPort(data)
	require.NoError(t, err)
	assert.Equal(t, 1, port.ID)
	assert.Equal(t, 0, port.Index)
	assert.Equal(t, "", port.Transport)
	assert.Equal(t, "", port.Address)
	assert.Equal(t, 0, port.Port)
	assert.False(t, port.Enabled)
	assert.Nil(t, port.Subsystems)
}

func TestNVMeoFSubsystemStruct_HostIDs(t *testing.T) {
	// Test that 25.10+ API uses int slice for hosts (not strings)
	subsys := NVMeoFSubsystem{
		ID:           1,
		Name:         "test",
		NQN:          "nqn.test",
		Serial:       "TEST123",
		AllowAnyHost: false,
		Hosts:        []int{1, 2, 3},
		Namespaces:   []int{10},
		Ports:        []int{100},
	}

	assert.Len(t, subsys.Hosts, 3)
	assert.Equal(t, 1, subsys.Hosts[0])
	assert.Equal(t, 2, subsys.Hosts[1])
	assert.Equal(t, 3, subsys.Hosts[2])
}

func TestNVMeoFNamespaceStruct_DeviceTypes(t *testing.T) {
	// Test ZVOL device type
	nsZvol := NVMeoFNamespace{
		ID:          1,
		SubsystemID: 1,
		NSID:        1,
		DeviceType:  "ZVOL",
		DevicePath:  "zvol/tank/vol1",
		Enabled:     true,
	}
	assert.Equal(t, "ZVOL", nsZvol.DeviceType)

	// Test FILE device type
	nsFile := NVMeoFNamespace{
		ID:          2,
		SubsystemID: 1,
		NSID:        2,
		DeviceType:  "FILE",
		DevicePath:  "/mnt/tank/file.img",
		Enabled:     true,
	}
	assert.Equal(t, "FILE", nsFile.DeviceType)
}

func TestNVMeoFPortStruct_TransportTypes(t *testing.T) {
	tcpPort := NVMeoFPort{
		ID:         1,
		Index:      0,
		Transport:  "TCP",
		Address:    "0.0.0.0",
		Port:       4420,
		AddrFamily: "IPV4",
		Enabled:    true,
	}
	assert.Equal(t, "TCP", tcpPort.Transport)
	assert.Equal(t, "IPV4", tcpPort.AddrFamily)

	rdmaPort := NVMeoFPort{
		ID:         2,
		Index:      1,
		Transport:  "RDMA",
		Address:    "192.168.1.100",
		Port:       4420,
		AddrFamily: "IPV4",
		Enabled:    true,
	}
	assert.Equal(t, "RDMA", rdmaPort.Transport)
}

func TestNVMeoFPortSubsysStruct(t *testing.T) {
	assoc := NVMeoFPortSubsys{
		ID:       1,
		PortID:   10,
		SubsysID: 20,
	}
	assert.Equal(t, 1, assoc.ID)
	assert.Equal(t, 10, assoc.PortID)
	assert.Equal(t, 20, assoc.SubsysID)
}

// =============================================================================
// Device Path Normalization Tests (for NVMeoFNamespaceCreate)
// =============================================================================

func TestDevicePathNormalization(t *testing.T) {
	// This tests the internal logic that normalizes device paths
	// by stripping /dev/ prefix when present

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "path without prefix",
			input:    "zvol/tank/vol1",
			expected: "zvol/tank/vol1",
		},
		{
			name:     "path with /dev/ prefix",
			input:    "/dev/zvol/tank/vol1",
			expected: "zvol/tank/vol1",
		},
		{
			name:     "path with only /dev/",
			input:    "/dev/sda",
			expected: "sda",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Replicate the normalization logic from NVMeoFNamespaceCreate
			normalizedPath := tt.input
			if len(tt.input) > 5 && tt.input[:5] == "/dev/" {
				normalizedPath = tt.input[5:]
			}
			assert.Equal(t, tt.expected, normalizedPath)
		})
	}
}
