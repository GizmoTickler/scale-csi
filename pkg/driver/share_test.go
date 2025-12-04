package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Tests for Share Cleanup Error Propagation
// These tests validate that cleanup errors are properly propagated and that
// property errors trigger cleanup as per the bug fixes.
// =============================================================================

// ErrorInjectingMockClient extends MockClient to allow injecting errors for
// specific operations during cleanup sequences.
type ErrorInjectingMockClient struct {
	*truenas.MockClient

	// Error injection flags
	InjectNFSShareDeleteError          error
	InjectISCSITargetDeleteError       error
	InjectISCSIExtentDeleteError       error
	InjectISCSITargetExtentDeleteError error
	InjectNVMeoFSubsystemDeleteError   error
	InjectNVMeoFNamespaceDeleteError   error
	InjectNVMeoFPortSubsysDeleteError  error
	InjectPropertySetError             error

	// Track cleanup call order
	CleanupCalls []string
}

func NewErrorInjectingMockClient() *ErrorInjectingMockClient {
	return &ErrorInjectingMockClient{
		MockClient:   truenas.NewMockClient(),
		CleanupCalls: make([]string, 0),
	}
}

func (m *ErrorInjectingMockClient) NFSShareDelete(ctx context.Context, id int) error {
	m.CleanupCalls = append(m.CleanupCalls, fmt.Sprintf("NFSShareDelete(%d)", id))
	if m.InjectNFSShareDeleteError != nil {
		return m.InjectNFSShareDeleteError
	}
	return m.MockClient.NFSShareDelete(ctx, id)
}

func (m *ErrorInjectingMockClient) ISCSITargetDelete(ctx context.Context, id int, force bool) error {
	m.CleanupCalls = append(m.CleanupCalls, fmt.Sprintf("ISCSITargetDelete(%d)", id))
	if m.InjectISCSITargetDeleteError != nil {
		return m.InjectISCSITargetDeleteError
	}
	return m.MockClient.ISCSITargetDelete(ctx, id, force)
}

func (m *ErrorInjectingMockClient) ISCSIExtentDelete(ctx context.Context, id int, remove bool, force bool) error {
	m.CleanupCalls = append(m.CleanupCalls, fmt.Sprintf("ISCSIExtentDelete(%d)", id))
	if m.InjectISCSIExtentDeleteError != nil {
		return m.InjectISCSIExtentDeleteError
	}
	return m.MockClient.ISCSIExtentDelete(ctx, id, remove, force)
}

func (m *ErrorInjectingMockClient) ISCSITargetExtentDelete(ctx context.Context, id int, force bool) error {
	m.CleanupCalls = append(m.CleanupCalls, fmt.Sprintf("ISCSITargetExtentDelete(%d)", id))
	if m.InjectISCSITargetExtentDeleteError != nil {
		return m.InjectISCSITargetExtentDeleteError
	}
	return m.MockClient.ISCSITargetExtentDelete(ctx, id, force)
}

func (m *ErrorInjectingMockClient) NVMeoFSubsystemDelete(ctx context.Context, id int) error {
	m.CleanupCalls = append(m.CleanupCalls, fmt.Sprintf("NVMeoFSubsystemDelete(%d)", id))
	if m.InjectNVMeoFSubsystemDeleteError != nil {
		return m.InjectNVMeoFSubsystemDeleteError
	}
	return m.MockClient.NVMeoFSubsystemDelete(ctx, id)
}

func (m *ErrorInjectingMockClient) NVMeoFNamespaceDelete(ctx context.Context, id int) error {
	m.CleanupCalls = append(m.CleanupCalls, fmt.Sprintf("NVMeoFNamespaceDelete(%d)", id))
	if m.InjectNVMeoFNamespaceDeleteError != nil {
		return m.InjectNVMeoFNamespaceDeleteError
	}
	return m.MockClient.NVMeoFNamespaceDelete(ctx, id)
}

func (m *ErrorInjectingMockClient) NVMeoFPortSubsysDelete(ctx context.Context, id int) error {
	m.CleanupCalls = append(m.CleanupCalls, fmt.Sprintf("NVMeoFPortSubsysDelete(%d)", id))
	if m.InjectNVMeoFPortSubsysDeleteError != nil {
		return m.InjectNVMeoFPortSubsysDeleteError
	}
	return m.MockClient.NVMeoFPortSubsysDelete(ctx, id)
}

func (m *ErrorInjectingMockClient) DatasetSetUserProperty(ctx context.Context, name string, key string, value string) error {
	if m.InjectPropertySetError != nil {
		return m.InjectPropertySetError
	}
	return m.MockClient.DatasetSetUserProperty(ctx, name, key, value)
}

// =============================================================================
// Test deleteNFSShare error propagation
// =============================================================================

// TestDeleteNFSShare_PropagatesDeleteError tests that when NFSShareDelete fails,
// the error is properly propagated instead of being swallowed.
func TestDeleteNFSShare_PropagatesDeleteError(t *testing.T) {
	mockClient := NewErrorInjectingMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
			},
		},
		truenasClient: mockClient,
	}

	// Setup: Create a dataset with NFS share ID property
	datasetName := "tank/k8s/volumes/test-vol"
	ctx := context.Background()
	_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "FILESYSTEM",
	})
	require.NoError(t, err)

	// Store NFS share ID in properties
	err = mockClient.DatasetSetUserProperty(ctx, datasetName, PropNFSShareID, "42")
	require.NoError(t, err)

	// Inject error for NFSShareDelete
	mockClient.InjectNFSShareDeleteError = fmt.Errorf("TrueNAS API error: share in use")

	// Call deleteNFSShare - should propagate error
	err = d.deleteNFSShare(ctx, datasetName)

	// Verify error is propagated (not swallowed)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete NFS share")
	assert.Contains(t, err.Error(), "42")
}

// TestDeleteNFSShare_SucceedsForMissingProperty tests idempotent deletion
// when no share ID property exists.
func TestDeleteNFSShare_SucceedsForMissingProperty(t *testing.T) {
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
			},
		},
		truenasClient: mockClient,
	}

	// Setup: Create a dataset WITHOUT NFS share ID property
	datasetName := "tank/k8s/volumes/test-vol-no-share"
	ctx := context.Background()
	_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "FILESYSTEM",
	})
	require.NoError(t, err)

	// Call deleteNFSShare - should succeed (nothing to delete)
	err = d.deleteNFSShare(ctx, datasetName)
	assert.NoError(t, err)
}

// =============================================================================
// Test deleteISCSIShare error propagation
// =============================================================================

// TestDeleteISCSIShare_PropagatesCleanupErrors tests that when iSCSI cleanup
// fails, errors are properly collected and propagated.
func TestDeleteISCSIShare_PropagatesCleanupErrors(t *testing.T) {
	mockClient := NewErrorInjectingMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
			},
			ISCSI: ISCSIConfig{},
		},
		truenasClient: mockClient,
	}

	// Setup: Create a dataset with iSCSI properties
	datasetName := "tank/k8s/volumes/test-iscsi-vol"
	ctx := context.Background()
	_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "VOLUME",
	})
	require.NoError(t, err)

	// Store iSCSI resource IDs in properties
	_ = mockClient.DatasetSetUserProperty(ctx, datasetName, PropISCSITargetID, "1")
	_ = mockClient.DatasetSetUserProperty(ctx, datasetName, PropISCSIExtentID, "2")
	_ = mockClient.DatasetSetUserProperty(ctx, datasetName, PropISCSITargetExtentID, "3")

	// Inject error for extent deletion
	mockClient.InjectISCSIExtentDeleteError = fmt.Errorf("extent still in use")

	// Call deleteISCSIShare - should propagate cleanup errors
	err = d.deleteISCSIShare(ctx, datasetName)

	// Verify errors are collected and propagated
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "iSCSI cleanup errors")
}

// TestDeleteISCSIShare_MultipleErrors tests that multiple cleanup errors are
// all collected and reported.
func TestDeleteISCSIShare_MultipleErrors(t *testing.T) {
	mockClient := NewErrorInjectingMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
			},
			ISCSI: ISCSIConfig{},
		},
		truenasClient: mockClient,
	}

	// Setup
	datasetName := "tank/k8s/volumes/test-iscsi-multi-err"
	ctx := context.Background()
	_, _ = mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "VOLUME",
	})
	_ = mockClient.DatasetSetUserProperty(ctx, datasetName, PropISCSITargetID, "1")
	_ = mockClient.DatasetSetUserProperty(ctx, datasetName, PropISCSIExtentID, "2")
	_ = mockClient.DatasetSetUserProperty(ctx, datasetName, PropISCSITargetExtentID, "3")

	// Inject multiple errors
	mockClient.InjectISCSITargetDeleteError = fmt.Errorf("target delete failed")
	mockClient.InjectISCSIExtentDeleteError = fmt.Errorf("extent delete failed")
	mockClient.InjectISCSITargetExtentDeleteError = fmt.Errorf("association delete failed")

	// Call deleteISCSIShare
	err := d.deleteISCSIShare(ctx, datasetName)

	// Verify error contains multiple errors
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "iSCSI cleanup errors")
}

// =============================================================================
// Test deleteNVMeoFShare error propagation
// =============================================================================

// TestDeleteNVMeoFShare_PropagatesCleanupErrors tests that NVMe-oF cleanup
// errors are properly propagated.
func TestDeleteNVMeoFShare_PropagatesCleanupErrors(t *testing.T) {
	mockClient := NewErrorInjectingMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
			},
			NVMeoF: NVMeoFConfig{},
		},
		truenasClient: mockClient,
	}

	// Setup
	datasetName := "tank/k8s/volumes/test-nvme-vol"
	ctx := context.Background()
	_, _ = mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "VOLUME",
	})
	_ = mockClient.DatasetSetUserProperty(ctx, datasetName, PropNVMeoFSubsystemID, "1")
	_ = mockClient.DatasetSetUserProperty(ctx, datasetName, PropNVMeoFNamespaceID, "2")
	_ = mockClient.DatasetSetUserProperty(ctx, datasetName, PropNVMeoFPortSubsysID, "3")

	// Inject error for subsystem deletion
	mockClient.InjectNVMeoFSubsystemDeleteError = fmt.Errorf("subsystem deletion failed")

	// Call deleteNVMeoFShare
	err := d.deleteNVMeoFShare(ctx, datasetName)

	// Verify error is propagated
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NVMe-oF cleanup errors")
}

// =============================================================================
// Test property error handling triggers cleanup
// =============================================================================

// TestCreateNFSShare_PropertyError_NoCleanup tests that NFS share creation
// that fails to store property ID returns error (property storage is non-fatal
// for NFS as share exists - this validates the current behavior).
func TestCreateNFSShare_PropertyError_ReturnsError(t *testing.T) {
	mockClient := NewErrorInjectingMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
			},
			NFS: NFSConfig{
				ShareHost: "192.168.1.100",
			},
		},
		truenasClient: mockClient,
	}

	// Setup: Create a dataset
	datasetName := "tank/k8s/volumes/test-nfs-prop-err"
	ctx := context.Background()
	_, _ = mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "FILESYSTEM",
	})

	// Inject error for property set
	mockClient.InjectPropertySetError = fmt.Errorf("property storage failed")

	// Call createNFSShare - should return error when property storage fails
	err := d.createNFSShare(ctx, datasetName, "test-vol")

	// Property storage failure for NFS should return error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to store NFS share ID")
}

// TestCreateISCSIShare_TargetCreation_Success tests that iSCSI share creation
// works correctly when all components succeed.
func TestCreateISCSIShare_TargetCreation_Success(t *testing.T) {
	mockClient := truenas.NewMockClient()
	// Create a service reload debouncer with a no-op reload function
	debouncer := NewServiceReloadDebouncer(100, func(ctx context.Context, service string) error {
		return nil // No-op for testing
	})
	_ = debouncer // Mark as used

	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
				ZvolReadyTimeout:  5,
			},
			ISCSI: ISCSIConfig{
				TargetGroups: []ISCSITargetGroup{
					{Portal: 1, Initiator: 1, AuthMethod: "NONE"},
				},
			},
		},
		truenasClient:          mockClient,
		serviceReloadDebouncer: debouncer,
	}

	// Setup: Create a zvol
	datasetName := "tank/k8s/volumes/test-iscsi-create"
	ctx := context.Background()
	_, _ = mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name:    datasetName,
		Type:    "VOLUME",
		Volsize: 1024 * 1024 * 1024,
	})

	// Call createISCSIShare
	err := d.createISCSIShare(ctx, datasetName, "test-vol")

	// Should succeed
	assert.NoError(t, err)

	// Verify resources were created
	target, err := mockClient.ISCSITargetFindByName(ctx, "test-iscsi-create")
	assert.NoError(t, err)
	assert.NotNil(t, target)
}

// =============================================================================
// Test ensureShareExists idempotency
// =============================================================================

// TestEnsureShareExists_AlreadyExists tests that ensureShareExists returns
// immediately when the share already exists.
func TestEnsureShareExists_AlreadyExists(t *testing.T) {
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
			},
			NFS: NFSConfig{
				ShareHost: "192.168.1.100",
			},
		},
		truenasClient: mockClient,
	}

	// Setup: Create a dataset with existing share ID
	datasetName := "tank/k8s/volumes/test-existing-share"
	ctx := context.Background()
	_, _ = mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "FILESYSTEM",
	})
	_ = mockClient.DatasetSetUserProperty(ctx, datasetName, PropNFSShareID, "42")

	// Get the dataset
	ds, err := mockClient.DatasetGet(ctx, datasetName)
	require.NoError(t, err)

	// Call ensureShareExists - should return immediately
	err = d.ensureShareExists(ctx, ds, datasetName, "test-vol", ShareTypeNFS)
	assert.NoError(t, err)
}

// TestEnsureShareExists_MissingShare tests that ensureShareExists creates
// the share when it's missing.
func TestEnsureShareExists_MissingShare(t *testing.T) {
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
			},
			NFS: NFSConfig{
				ShareHost: "192.168.1.100",
			},
		},
		truenasClient: mockClient,
	}

	// Setup: Create a dataset WITHOUT share ID property
	datasetName := "tank/k8s/volumes/test-missing-share"
	ctx := context.Background()
	_, _ = mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "FILESYSTEM",
	})

	// Get the dataset
	ds, err := mockClient.DatasetGet(ctx, datasetName)
	require.NoError(t, err)

	// Call ensureShareExists - should create the share
	err = d.ensureShareExists(ctx, ds, datasetName, "test-vol", ShareTypeNFS)
	assert.NoError(t, err)

	// Verify share was created by checking property was set
	propValue, _ := mockClient.DatasetGetUserProperty(ctx, datasetName, PropNFSShareID)
	assert.NotEmpty(t, propValue)
	assert.NotEqual(t, "-", propValue)
}

// TestEnsureShareExists_DashValue tests that ensureShareExists treats "-" as
// missing (this is how ZFS returns unset user properties).
func TestEnsureShareExists_DashValue(t *testing.T) {
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
			},
			NFS: NFSConfig{
				ShareHost: "192.168.1.100",
			},
		},
		truenasClient: mockClient,
	}

	// Setup: Create a dataset with "-" value for share ID
	datasetName := "tank/k8s/volumes/test-dash-share"
	ctx := context.Background()
	_, _ = mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "FILESYSTEM",
	})
	_ = mockClient.DatasetSetUserProperty(ctx, datasetName, PropNFSShareID, "-")

	// Get the dataset
	ds, err := mockClient.DatasetGet(ctx, datasetName)
	require.NoError(t, err)

	// Call ensureShareExists - should create share since "-" means unset
	err = d.ensureShareExists(ctx, ds, datasetName, "test-vol", ShareTypeNFS)
	assert.NoError(t, err)

	// Verify share was created
	propValue, _ := mockClient.DatasetGetUserProperty(ctx, datasetName, PropNFSShareID)
	assert.NotEmpty(t, propValue)
	assert.NotEqual(t, "-", propValue)
}

// =============================================================================
// Test createShareWithOptions
// =============================================================================

// TestCreateShareWithOptions_UnsupportedType tests that unsupported share types
// return an appropriate error.
func TestCreateShareWithOptions_UnsupportedType(t *testing.T) {
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
			},
		},
		truenasClient: mockClient,
	}

	ctx := context.Background()
	err := d.createShareWithOptions(ctx, "tank/k8s/volumes/test", "test-vol", ShareType("unknown"), false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported share type")
}

// TestDeleteShare_UnknownType tests that deleting an unknown share type is
// a no-op (returns nil).
func TestDeleteShare_UnknownType(t *testing.T) {
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "tank/k8s/volumes",
			},
		},
		truenasClient: mockClient,
	}

	ctx := context.Background()
	err := d.deleteShare(ctx, "tank/k8s/volumes/test", ShareType("unknown"))

	// Should succeed (no-op for unknown types)
	assert.NoError(t, err)
}
