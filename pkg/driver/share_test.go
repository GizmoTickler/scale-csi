package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
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

type shareCallCountingMock struct {
	*truenas.MockClient
	datasetGets        int
	zvolWaits          int
	idempotencyLookups int
	propertyUpdates    []map[string]string
}

type nvmeDeleteAssociationCountingMock struct {
	*truenas.MockClient
	portSubsysListCalls   int
	portSubsysDeleteCalls int
}

type nvmeHostCountingMock struct {
	*truenas.MockClient
	hostFindCalls       int
	hostCreateCalls     int
	subsystemAllowAny   []bool
	subsystemHostIDs    [][]int
	subsystemCreateFail error
}

func (m *nvmeHostCountingMock) NVMeoFHostFindByNQN(ctx context.Context, nqn string) (*truenas.NVMeoFHost, error) {
	m.hostFindCalls++
	return m.MockClient.NVMeoFHostFindByNQN(ctx, nqn)
}

func (m *nvmeHostCountingMock) NVMeoFHostCreate(ctx context.Context, nqn string) (*truenas.NVMeoFHost, error) {
	m.hostCreateCalls++
	return m.MockClient.NVMeoFHostCreate(ctx, nqn)
}

func (m *nvmeHostCountingMock) NVMeoFSubsystemCreate(ctx context.Context, name string, allowAnyHost bool, hostIDs []int) (*truenas.NVMeoFSubsystem, error) {
	m.subsystemAllowAny = append(m.subsystemAllowAny, allowAnyHost)
	m.subsystemHostIDs = append(m.subsystemHostIDs, append([]int(nil), hostIDs...))
	if m.subsystemCreateFail != nil {
		err := m.subsystemCreateFail
		m.subsystemCreateFail = nil
		return nil, err
	}
	return m.MockClient.NVMeoFSubsystemCreate(ctx, name, allowAnyHost, hostIDs)
}

func (m *nvmeDeleteAssociationCountingMock) NVMeoFPortSubsysList(ctx context.Context) ([]*truenas.NVMeoFPortSubsys, error) {
	m.portSubsysListCalls++
	return []*truenas.NVMeoFPortSubsys{{ID: 11, PortID: 1, SubsysID: 1}}, nil
}

func (m *nvmeDeleteAssociationCountingMock) NVMeoFSubsystemDelete(ctx context.Context, id int) error {
	return fmt.Errorf("subsystem deletion failed")
}

func (m *nvmeDeleteAssociationCountingMock) NVMeoFPortSubsysDelete(ctx context.Context, id int) error {
	m.portSubsysDeleteCalls++
	return nil
}

func newShareCallCountingMock() *shareCallCountingMock {
	return &shareCallCountingMock{MockClient: truenas.NewMockClient()}
}

func (m *shareCallCountingMock) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	m.datasetGets++
	return m.MockClient.DatasetGet(ctx, name)
}

func (m *shareCallCountingMock) WaitForZvolReady(ctx context.Context, name string, timeout time.Duration) (*truenas.Dataset, error) {
	m.zvolWaits++
	return m.MockClient.WaitForZvolReady(ctx, name, timeout)
}

func (m *shareCallCountingMock) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	copyOfProperties := make(map[string]string, len(properties))
	for key, value := range properties {
		copyOfProperties[key] = value
	}
	m.propertyUpdates = append(m.propertyUpdates, copyOfProperties)
	return m.MockClient.DatasetSetUserProperties(ctx, name, properties)
}

func (m *shareCallCountingMock) ISCSITargetGet(ctx context.Context, id int) (*truenas.ISCSITarget, error) {
	m.idempotencyLookups++
	return m.MockClient.ISCSITargetGet(ctx, id)
}

func (m *shareCallCountingMock) ISCSITargetFindByName(ctx context.Context, name string) (*truenas.ISCSITarget, error) {
	m.idempotencyLookups++
	return m.MockClient.ISCSITargetFindByName(ctx, name)
}

func (m *shareCallCountingMock) ISCSIExtentGet(ctx context.Context, id int) (*truenas.ISCSIExtent, error) {
	m.idempotencyLookups++
	return m.MockClient.ISCSIExtentGet(ctx, id)
}

func (m *shareCallCountingMock) ISCSIExtentFindByName(ctx context.Context, name string) (*truenas.ISCSIExtent, error) {
	m.idempotencyLookups++
	return m.MockClient.ISCSIExtentFindByName(ctx, name)
}

func (m *shareCallCountingMock) ISCSIExtentFindByDisk(ctx context.Context, diskPath string) (*truenas.ISCSIExtent, error) {
	m.idempotencyLookups++
	return m.MockClient.ISCSIExtentFindByDisk(ctx, diskPath)
}

func (m *shareCallCountingMock) ISCSITargetExtentGet(ctx context.Context, id int) (*truenas.ISCSITargetExtent, error) {
	m.idempotencyLookups++
	return m.MockClient.ISCSITargetExtentGet(ctx, id)
}

func (m *shareCallCountingMock) ISCSITargetExtentFind(ctx context.Context, targetID, extentID int) (*truenas.ISCSITargetExtent, error) {
	m.idempotencyLookups++
	return m.MockClient.ISCSITargetExtentFind(ctx, targetID, extentID)
}

func (m *shareCallCountingMock) NVMeoFSubsystemFindByName(ctx context.Context, name string) (*truenas.NVMeoFSubsystem, error) {
	m.idempotencyLookups++
	return m.MockClient.NVMeoFSubsystemFindByName(ctx, name)
}

func (m *shareCallCountingMock) NVMeoFNamespaceFindByDevice(ctx context.Context, subsystemID int, devicePath string) (*truenas.NVMeoFNamespace, error) {
	m.idempotencyLookups++
	return m.MockClient.NVMeoFNamespaceFindByDevice(ctx, subsystemID, devicePath)
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

func (m *ErrorInjectingMockClient) ISCSIExtentDelete(ctx context.Context, id int, remove, force bool) error {
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

func (m *ErrorInjectingMockClient) DatasetSetUserProperty(ctx context.Context, name, key, value string) error {
	if m.InjectPropertySetError != nil {
		return m.InjectPropertySetError
	}
	return m.MockClient.DatasetSetUserProperty(ctx, name, key, value)
}

func (m *ErrorInjectingMockClient) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	if m.InjectPropertySetError != nil {
		return m.InjectPropertySetError
	}
	return m.MockClient.DatasetSetUserProperties(ctx, name, properties)
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

func TestDeleteNVMeoFShare_FetchesPortSubsysAssociationsOnce(t *testing.T) {
	mockClient := &nvmeDeleteAssociationCountingMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "tank/k8s/volumes"},
		},
		truenasClient: mockClient,
	}

	datasetName := "tank/k8s/volumes/test-nvme-assoc-cache"
	ds, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "VOLUME",
	})
	require.NoError(t, err)
	require.NoError(t, mockClient.DatasetSetUserProperty(context.Background(), datasetName, PropNVMeoFSubsystemID, "1"))
	mockClient.NVMeSubsystems[1] = &truenas.NVMeoFSubsystem{ID: 1, Name: "test-nvme-assoc-cache"}

	err = d.deleteNVMeoFShareForDataset(context.Background(), ds, datasetName)
	require.Error(t, err)
	assert.Equal(t, 2, mockClient.portSubsysDeleteCalls)
	assert.Equal(t, 1, mockClient.portSubsysListCalls)
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
	err := d.createNFSShare(ctx, datasetName, "test-vol", "/mnt/tank/k8s/volumes/test-nfs-prop-err")

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

func TestCreateISCSIShareFreshDatasetSkipsLookupsAndBatchesProperties(t *testing.T) {
	mockClient := newShareCallCountingMock()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "tank/k8s/volumes", ZvolReadyTimeout: 5},
			ISCSI: ISCSIConfig{TargetGroups: []ISCSITargetGroup{
				{Portal: 1, Initiator: 1, AuthMethod: "NONE"},
			}},
		},
		truenasClient: mockClient,
		serviceReloadDebouncer: NewServiceReloadDebouncer(time.Nanosecond, func(context.Context, string) error {
			return nil
		}),
	}
	ctx := context.Background()
	datasetName := "tank/k8s/volumes/fresh-iscsi"
	ds, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "VOLUME", Volsize: 1024 * 1024 * 1024,
	})
	require.NoError(t, err)

	err = d.createISCSIShareForDataset(ctx, ds, datasetName, "fresh-iscsi", true, true)
	require.NoError(t, err)
	assert.Zero(t, mockClient.datasetGets)
	assert.Zero(t, mockClient.zvolWaits)
	assert.Zero(t, mockClient.idempotencyLookups)
	require.Len(t, mockClient.propertyUpdates, 1)
	assert.Equal(t, map[string]string{
		PropISCSITargetID:       "1",
		PropISCSIExtentID:       "1",
		PropISCSITargetExtentID: "1",
	}, mockClient.propertyUpdates[0])
}

func TestCreateNVMeoFShareFreshDatasetSkipsLookupsAndBatchesProperties(t *testing.T) {
	mockClient := newShareCallCountingMock()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "tank/k8s/volumes", ZvolReadyTimeout: 5},
			NVMeoF: NVMeoFConfig{
				Transport:             "TCP",
				TransportAddress:      "10.0.0.10",
				TransportServiceID:    4420,
				SubsystemAllowAnyHost: true,
			},
		},
		truenasClient: mockClient,
	}
	ctx := context.Background()
	datasetName := "tank/k8s/volumes/fresh-nvme"
	ds, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "VOLUME", Volsize: 1024 * 1024 * 1024,
	})
	require.NoError(t, err)

	err = d.createNVMeoFShareForDataset(ctx, ds, datasetName, "fresh-nvme", true, true)
	require.NoError(t, err)
	assert.Zero(t, mockClient.datasetGets)
	assert.Zero(t, mockClient.zvolWaits)
	assert.Zero(t, mockClient.idempotencyLookups)
	require.Len(t, mockClient.propertyUpdates, 1)
	assert.Equal(t, map[string]string{
		PropNVMeoFSubsystemID:  "1",
		PropNVMeoFPortSubsysID: "1",
		PropNVMeoFNamespaceID:  "1",
	}, mockClient.propertyUpdates[0])
}

func TestCreateNVMeoFShareAllowAnyHostSkipsHostResolution(t *testing.T) {
	mockClient := &nvmeHostCountingMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "tank/k8s/volumes", ZvolReadyTimeout: 5},
			NVMeoF: NVMeoFConfig{
				Transport:             "TCP",
				TransportAddress:      "10.0.0.10",
				TransportServiceID:    4420,
				SubsystemAllowAnyHost: true,
				SubsystemHosts:        []string{"nqn.ignored.when.allow-any-is-enabled"},
			},
		},
		truenasClient: mockClient,
	}
	ctx := context.Background()
	datasetName := "tank/k8s/volumes/allow-any-nvme"
	ds, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "VOLUME", Volsize: 1024 * 1024 * 1024,
	})
	require.NoError(t, err)

	err = d.createNVMeoFShareForDataset(ctx, ds, datasetName, "allow-any-nvme", true, true)
	require.NoError(t, err)
	assert.Zero(t, mockClient.hostFindCalls)
	assert.Zero(t, mockClient.hostCreateCalls)
	require.Equal(t, []bool{true}, mockClient.subsystemAllowAny)
	require.Len(t, mockClient.subsystemHostIDs, 1)
	assert.Empty(t, mockClient.subsystemHostIDs[0])
}

func TestCreateNVMeoFShareRestrictedHostsResolveAndCache(t *testing.T) {
	baseClient := truenas.NewMockClient()
	existingNQN := "nqn.2014-08.org.nvmexpress:existing-node"
	createdNQN := "nqn.2014-08.org.nvmexpress:new-node"
	existingHost, err := baseClient.NVMeoFHostCreate(context.Background(), existingNQN)
	require.NoError(t, err)
	mockClient := &nvmeHostCountingMock{MockClient: baseClient}
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "tank/k8s/volumes", ZvolReadyTimeout: 5},
			NVMeoF: NVMeoFConfig{
				Transport:             "TCP",
				TransportAddress:      "10.0.0.10",
				TransportServiceID:    4420,
				SubsystemAllowAnyHost: false,
				SubsystemHosts:        []string{existingNQN, createdNQN},
			},
		},
		truenasClient: mockClient,
	}
	ctx := context.Background()

	for _, volume := range []string{"restricted-nvme-1", "restricted-nvme-2"} {
		datasetName := "tank/k8s/volumes/" + volume
		ds, createErr := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
			Name: datasetName, Type: "VOLUME", Volsize: 1024 * 1024 * 1024,
		})
		require.NoError(t, createErr)
		require.NoError(t, d.createNVMeoFShareForDataset(ctx, ds, datasetName, volume, true, true))
	}

	createdHost := baseClient.NVMeHosts[createdNQN]
	require.NotNil(t, createdHost)
	assert.Equal(t, 2, mockClient.hostFindCalls)
	assert.Equal(t, 1, mockClient.hostCreateCalls)
	assert.Equal(t, []bool{false, false}, mockClient.subsystemAllowAny)
	require.Len(t, mockClient.subsystemHostIDs, 2)
	assert.Equal(t, []int{existingHost.ID, createdHost.ID}, mockClient.subsystemHostIDs[0])
	assert.Equal(t, mockClient.subsystemHostIDs[0], mockClient.subsystemHostIDs[1])
}

func TestCreateNVMeoFShareRestrictedHostsRequiresNQN(t *testing.T) {
	d := &Driver{
		config:        &Config{NVMeoF: NVMeoFConfig{SubsystemAllowAnyHost: false}},
		truenasClient: truenas.NewMockClient(),
	}

	err := d.createNVMeoFShareForDataset(context.Background(), nil, "tank/k8s/volumes/restricted-empty", "restricted-empty", true, true)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "nvmeof.subsystemAllowAnyHost is false but nvmeof.subsystemHosts is empty")
	assert.Contains(t, err.Error(), "set allow-any-host or provide at least one host NQN")
}

func TestCreateNVMeoFShareRestrictedHostsInvalidatesCacheOnSubsystemError(t *testing.T) {
	nqn := "nqn.2014-08.org.nvmexpress:retry-node"
	mockClient := &nvmeHostCountingMock{
		MockClient:          truenas.NewMockClient(),
		subsystemCreateFail: fmt.Errorf("stale host ID"),
	}
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "tank/k8s/volumes", ZvolReadyTimeout: 5},
			NVMeoF: NVMeoFConfig{
				Transport:             "TCP",
				TransportAddress:      "10.0.0.10",
				TransportServiceID:    4420,
				SubsystemAllowAnyHost: false,
				SubsystemHosts:        []string{nqn},
			},
		},
		truenasClient: mockClient,
	}
	ctx := context.Background()

	firstDataset := "tank/k8s/volumes/restricted-retry-1"
	first, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: firstDataset, Type: "VOLUME"})
	require.NoError(t, err)
	err = d.createNVMeoFShareForDataset(ctx, first, firstDataset, "restricted-retry-1", true, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stale host ID")

	secondDataset := "tank/k8s/volumes/restricted-retry-2"
	second, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: secondDataset, Type: "VOLUME"})
	require.NoError(t, err)
	require.NoError(t, d.createNVMeoFShareForDataset(ctx, second, secondDataset, "restricted-retry-2", true, true))

	assert.Equal(t, 2, mockClient.hostFindCalls, "retry must re-query after cache invalidation")
	assert.Equal(t, 1, mockClient.hostCreateCalls, "existing host record must be reused on retry")
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
	err := d.createShareWithOptions(ctx, nil, "tank/k8s/volumes/test", "test-vol", ShareType("unknown"), false, false)

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
	err := d.deleteShare(ctx, nil, "tank/k8s/volumes/test", ShareType("unknown"))

	// Should succeed (no-op for unknown types)
	assert.NoError(t, err)
}

// TestProtocolShareName pins the iSCSI/NVMe name sanitization contract:
// legal names pass through unchanged (existing deployments keep their target
// names); illegal names are lowercased/charset-mapped with a deterministic
// disambiguator so distinct originals cannot collide.
func TestProtocolShareName(t *testing.T) {
	legal := []string{"pvc-0a1b2c3d", "vol.1:a-b", "x"}
	for _, name := range legal {
		if got := protocolShareName(name); got != name {
			t.Fatalf("legal name %q changed to %q", name, got)
		}
	}

	upper := protocolShareName("Vol-A")
	lower := protocolShareName("vol-a")
	if upper == lower {
		t.Fatalf("sanitized collision: %q == %q", upper, lower)
	}
	if upper != strings.ToLower(upper) {
		t.Fatalf("sanitized name not lowercase: %q", upper)
	}
	for _, r := range protocolShareName("has spaces_and_UNDERSCORES!") {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == ':' || r == '-'
		if !valid {
			t.Fatalf("illegal rune %q survived sanitization", r)
		}
	}
	if got := protocolShareName("Vol-A"); got != upper {
		t.Fatalf("sanitization not deterministic: %q vs %q", got, upper)
	}
	long := protocolShareName(strings.Repeat("A", 300))
	if len(long) > 64 {
		t.Fatalf("sanitized name exceeds the 64-char extent limit: %d", len(long))
	}
	longLegal := protocolShareName(strings.Repeat("a", 80))
	if len(longLegal) > 64 {
		t.Fatalf("legal-but-long name not capped: %d", len(longLegal))
	}
	if longLegal == protocolShareName(strings.Repeat("a", 81)) {
		t.Fatalf("truncated names must not collide")
	}
}

// TestResolveISCSITargetGroup verifies portal/initiator auto-resolution when
// targetGroups is unset: the portal must match the configured targetPortal and
// an allow-all initiator group is reused; no portal match fails loudly.
func TestResolveISCSITargetGroup(t *testing.T) {
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ISCSI: ISCSIConfig{TargetPortal: "192.0.2.10:3260"},
		},
		truenasClient: mockClient,
	}

	group, err := d.resolveISCSITargetGroup(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, group.Portal)
	assert.Equal(t, 1, group.Initiator)
	assert.Equal(t, "NONE", group.AuthMethod)

	// Cached on second call even if the mock is emptied.
	mockClient.ISCSIPortals = map[int]*truenas.ISCSIPortal{}
	again, err := d.resolveISCSITargetGroup(context.Background())
	require.NoError(t, err)
	assert.Same(t, group, again)

	// No matching portal → loud failure, retried (not cached).
	d2 := &Driver{
		config: &Config{
			ISCSI: ISCSIConfig{TargetPortal: "203.0.113.9:3260"},
		},
		truenasClient: mockClient,
	}
	_, err = d2.resolveISCSITargetGroup(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no TrueNAS iSCSI portal")
}
