package driver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// apiCallCountingClient wraps the complete ClientInterface surface. Keeping the
// counter at this boundary makes the tests sensitive to extra driver-to-TrueNAS
// round trips without coupling them to MockClient's implementation details.
type apiCallCountingClient struct {
	*truenas.MockClient

	mu      sync.Mutex
	total   int
	methods map[string]int
}

func newAPICallCountingClient() *apiCallCountingClient {
	return &apiCallCountingClient{
		MockClient: truenas.NewMockClient(),
		methods:    make(map[string]int),
	}
}

func (c *apiCallCountingClient) record(method string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.total++
	c.methods[method]++
}

func (c *apiCallCountingClient) resetCalls() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.total = 0
	clear(c.methods)
}

func (c *apiCallCountingClient) callSnapshot() (int, map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	methods := make(map[string]int, len(c.methods))
	for method, count := range c.methods {
		methods[method] = count
	}
	return c.total, methods
}

func (c *apiCallCountingClient) Close() error {
	c.record("Close")
	return c.MockClient.Close()
}

func (c *apiCallCountingClient) IsConnected() bool {
	c.record("IsConnected")
	return c.MockClient.IsConnected()
}

func (c *apiCallCountingClient) Call(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	c.record("Call")
	return c.MockClient.Call(ctx, method, params...)
}

func (c *apiCallCountingClient) CallWithContext(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	c.record("CallWithContext")
	return c.MockClient.CallWithContext(ctx, method, params...)
}

func (c *apiCallCountingClient) CircuitBreakerStats() *truenas.CircuitBreakerStats {
	c.record("CircuitBreakerStats")
	return c.MockClient.CircuitBreakerStats()
}

func (c *apiCallCountingClient) ResetCircuitBreaker() {
	c.record("ResetCircuitBreaker")
	c.MockClient.ResetCircuitBreaker()
}

func (c *apiCallCountingClient) DatasetCreate(ctx context.Context, params *truenas.DatasetCreateParams) (*truenas.Dataset, error) {
	c.record("DatasetCreate")
	return c.MockClient.DatasetCreate(ctx, params)
}

func (c *apiCallCountingClient) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	c.record("DatasetDelete")
	return c.MockClient.DatasetDelete(ctx, name, recursive, force)
}

func (c *apiCallCountingClient) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	c.record("DatasetGet")
	return c.MockClient.DatasetGet(ctx, name)
}

func (c *apiCallCountingClient) DatasetUpdate(ctx context.Context, name string, params *truenas.DatasetUpdateParams) (*truenas.Dataset, error) {
	c.record("DatasetUpdate")
	return c.MockClient.DatasetUpdate(ctx, name, params)
}

func (c *apiCallCountingClient) DatasetList(ctx context.Context, parentName string, limit, offset int) ([]*truenas.Dataset, error) {
	c.record("DatasetList")
	return c.MockClient.DatasetList(ctx, parentName, limit, offset)
}

func (c *apiCallCountingClient) DatasetSetUserProperty(ctx context.Context, name, key, value string) error {
	c.record("DatasetSetUserProperty")
	return c.MockClient.DatasetSetUserProperty(ctx, name, key, value)
}

func (c *apiCallCountingClient) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	c.record("DatasetSetUserProperties")
	return c.MockClient.DatasetSetUserProperties(ctx, name, properties)
}

func (c *apiCallCountingClient) DatasetRemoveUserProperties(ctx context.Context, name string, keys []string) error {
	c.record("DatasetRemoveUserProperties")
	return c.MockClient.DatasetRemoveUserProperties(ctx, name, keys)
}

func (c *apiCallCountingClient) DatasetGetUserProperty(ctx context.Context, name, key string) (string, error) {
	c.record("DatasetGetUserProperty")
	return c.MockClient.DatasetGetUserProperty(ctx, name, key)
}

func (c *apiCallCountingClient) DatasetExpand(ctx context.Context, name string, newSize int64) error {
	c.record("DatasetExpand")
	return c.MockClient.DatasetExpand(ctx, name, newSize)
}

func (c *apiCallCountingClient) DatasetExists(ctx context.Context, name string) (bool, error) {
	c.record("DatasetExists")
	return c.MockClient.DatasetExists(ctx, name)
}

func (c *apiCallCountingClient) DatasetHasDependentClones(ctx context.Context, name string) (bool, error) {
	c.record("DatasetHasDependentClones")
	return c.MockClient.DatasetHasDependentClones(ctx, name)
}

func (c *apiCallCountingClient) GetPoolAvailable(ctx context.Context, poolName string) (int64, error) {
	c.record("GetPoolAvailable")
	return c.MockClient.GetPoolAvailable(ctx, poolName)
}

func (c *apiCallCountingClient) WaitForDatasetReady(ctx context.Context, name string, timeout time.Duration) (*truenas.Dataset, error) {
	c.record("WaitForDatasetReady")
	return c.MockClient.WaitForDatasetReady(ctx, name, timeout)
}

func (c *apiCallCountingClient) WaitForZvolReady(ctx context.Context, name string, timeout time.Duration) (*truenas.Dataset, error) {
	c.record("WaitForZvolReady")
	return c.MockClient.WaitForZvolReady(ctx, name, timeout)
}

func (c *apiCallCountingClient) SnapshotCreate(ctx context.Context, dataset, name string, userProperties map[string]string) (*truenas.Snapshot, error) {
	c.record("SnapshotCreate")
	return c.MockClient.SnapshotCreate(ctx, dataset, name, userProperties)
}

func (c *apiCallCountingClient) SnapshotDelete(ctx context.Context, snapshotID string, defer_, recursive bool) error {
	c.record("SnapshotDelete")
	return c.MockClient.SnapshotDelete(ctx, snapshotID, defer_, recursive)
}

func (c *apiCallCountingClient) SnapshotRename(ctx context.Context, snapshotID, newName string) error {
	c.record("SnapshotRename")
	return c.MockClient.SnapshotRename(ctx, snapshotID, newName)
}

func (c *apiCallCountingClient) SnapshotGet(ctx context.Context, snapshotID string) (*truenas.Snapshot, error) {
	c.record("SnapshotGet")
	return c.MockClient.SnapshotGet(ctx, snapshotID)
}

func (c *apiCallCountingClient) SnapshotList(ctx context.Context, dataset string) ([]*truenas.Snapshot, error) {
	c.record("SnapshotList")
	return c.MockClient.SnapshotList(ctx, dataset)
}

func (c *apiCallCountingClient) SnapshotListAll(ctx context.Context, parentDataset string, limit, offset int) ([]*truenas.Snapshot, error) {
	c.record("SnapshotListAll")
	return c.MockClient.SnapshotListAll(ctx, parentDataset, limit, offset)
}

func (c *apiCallCountingClient) SnapshotFindByName(ctx context.Context, parentDataset, name string) (*truenas.Snapshot, error) {
	c.record("SnapshotFindByName")
	return c.MockClient.SnapshotFindByName(ctx, parentDataset, name)
}

func (c *apiCallCountingClient) SnapshotSetUserProperty(ctx context.Context, snapshotID, key, value string) error {
	c.record("SnapshotSetUserProperty")
	return c.MockClient.SnapshotSetUserProperty(ctx, snapshotID, key, value)
}

func (c *apiCallCountingClient) SnapshotRemoveUserProperties(ctx context.Context, snapshotID string, keys []string) error {
	c.record("SnapshotRemoveUserProperties")
	return c.MockClient.SnapshotRemoveUserProperties(ctx, snapshotID, keys)
}

func (c *apiCallCountingClient) SnapshotClone(ctx context.Context, snapshotID, newDatasetName string) error {
	c.record("SnapshotClone")
	return c.MockClient.SnapshotClone(ctx, snapshotID, newDatasetName)
}

func (c *apiCallCountingClient) CopyDatasetFromSnapshotLocal(ctx context.Context, sourceDataset, snapshotShortName, targetDataset string) error {
	c.record("CopyDatasetFromSnapshotLocal")
	return c.MockClient.CopyDatasetFromSnapshotLocal(ctx, sourceDataset, snapshotShortName, targetDataset)
}

func (c *apiCallCountingClient) DestroyReplicatedTargetSnapshot(ctx context.Context, targetDataset, snapshotShortName string) error {
	c.record("DestroyReplicatedTargetSnapshot")
	return c.MockClient.DestroyReplicatedTargetSnapshot(ctx, targetDataset, snapshotShortName)
}

func (c *apiCallCountingClient) SnapshotRollback(ctx context.Context, snapshotID string, force, recursive, recursiveClones bool) error {
	c.record("SnapshotRollback")
	return c.MockClient.SnapshotRollback(ctx, snapshotID, force, recursive, recursiveClones)
}

func (c *apiCallCountingClient) NFSShareCreate(ctx context.Context, params *truenas.NFSShareCreateParams) (*truenas.NFSShare, error) {
	c.record("NFSShareCreate")
	return c.MockClient.NFSShareCreate(ctx, params)
}

func (c *apiCallCountingClient) NFSShareDelete(ctx context.Context, id int) error {
	c.record("NFSShareDelete")
	return c.MockClient.NFSShareDelete(ctx, id)
}

func (c *apiCallCountingClient) NFSShareGet(ctx context.Context, id int) (*truenas.NFSShare, error) {
	c.record("NFSShareGet")
	return c.MockClient.NFSShareGet(ctx, id)
}

func (c *apiCallCountingClient) NFSShareFindByPath(ctx context.Context, path string) (*truenas.NFSShare, error) {
	c.record("NFSShareFindByPath")
	return c.MockClient.NFSShareFindByPath(ctx, path)
}

func (c *apiCallCountingClient) NFSShareList(ctx context.Context) ([]*truenas.NFSShare, error) {
	c.record("NFSShareList")
	return c.MockClient.NFSShareList(ctx)
}

func (c *apiCallCountingClient) NFSShareUpdate(ctx context.Context, id int, params map[string]interface{}) (*truenas.NFSShare, error) {
	c.record("NFSShareUpdate")
	return c.MockClient.NFSShareUpdate(ctx, id, params)
}

func (c *apiCallCountingClient) ServiceReload(ctx context.Context, service string) error {
	c.record("ServiceReload")
	return c.MockClient.ServiceReload(ctx, service)
}

func (c *apiCallCountingClient) GetSystemInfo(ctx context.Context) (*truenas.SystemInfo, error) {
	c.record("GetSystemInfo")
	return c.MockClient.GetSystemInfo(ctx)
}

func (c *apiCallCountingClient) CheckNVMeoFSupport(ctx context.Context) error {
	c.record("CheckNVMeoFSupport")
	return c.MockClient.CheckNVMeoFSupport(ctx)
}

func (c *apiCallCountingClient) ISCSITargetCreate(ctx context.Context, name, alias, mode string, groups []truenas.ISCSITargetGroup) (*truenas.ISCSITarget, error) {
	c.record("ISCSITargetCreate")
	return c.MockClient.ISCSITargetCreate(ctx, name, alias, mode, groups)
}

func (c *apiCallCountingClient) ISCSITargetDelete(ctx context.Context, id int, force bool) error {
	c.record("ISCSITargetDelete")
	return c.MockClient.ISCSITargetDelete(ctx, id, force)
}

func (c *apiCallCountingClient) ISCSITargetGet(ctx context.Context, id int) (*truenas.ISCSITarget, error) {
	c.record("ISCSITargetGet")
	return c.MockClient.ISCSITargetGet(ctx, id)
}

func (c *apiCallCountingClient) ISCSITargetFindByName(ctx context.Context, name string) (*truenas.ISCSITarget, error) {
	c.record("ISCSITargetFindByName")
	return c.MockClient.ISCSITargetFindByName(ctx, name)
}

func (c *apiCallCountingClient) ISCSIExtentCreate(ctx context.Context, name, diskPath, comment string, blocksize int, physicalBlocksize bool, rpm string) (*truenas.ISCSIExtent, error) {
	c.record("ISCSIExtentCreate")
	return c.MockClient.ISCSIExtentCreate(ctx, name, diskPath, comment, blocksize, physicalBlocksize, rpm)
}

func (c *apiCallCountingClient) ISCSIExtentDelete(ctx context.Context, id int, remove, force bool) error {
	c.record("ISCSIExtentDelete")
	return c.MockClient.ISCSIExtentDelete(ctx, id, remove, force)
}

func (c *apiCallCountingClient) ISCSIExtentGet(ctx context.Context, id int) (*truenas.ISCSIExtent, error) {
	c.record("ISCSIExtentGet")
	return c.MockClient.ISCSIExtentGet(ctx, id)
}

func (c *apiCallCountingClient) ISCSIExtentFindByName(ctx context.Context, name string) (*truenas.ISCSIExtent, error) {
	c.record("ISCSIExtentFindByName")
	return c.MockClient.ISCSIExtentFindByName(ctx, name)
}

func (c *apiCallCountingClient) ISCSIExtentFindByDisk(ctx context.Context, diskPath string) (*truenas.ISCSIExtent, error) {
	c.record("ISCSIExtentFindByDisk")
	return c.MockClient.ISCSIExtentFindByDisk(ctx, diskPath)
}

func (c *apiCallCountingClient) ISCSITargetExtentCreate(ctx context.Context, targetID, extentID, lunID int) (*truenas.ISCSITargetExtent, error) {
	c.record("ISCSITargetExtentCreate")
	return c.MockClient.ISCSITargetExtentCreate(ctx, targetID, extentID, lunID)
}

func (c *apiCallCountingClient) ISCSITargetExtentDelete(ctx context.Context, id int, force bool) error {
	c.record("ISCSITargetExtentDelete")
	return c.MockClient.ISCSITargetExtentDelete(ctx, id, force)
}

func (c *apiCallCountingClient) ISCSITargetExtentGet(ctx context.Context, id int) (*truenas.ISCSITargetExtent, error) {
	c.record("ISCSITargetExtentGet")
	return c.MockClient.ISCSITargetExtentGet(ctx, id)
}

func (c *apiCallCountingClient) ISCSITargetExtentFind(ctx context.Context, targetID, extentID int) (*truenas.ISCSITargetExtent, error) {
	c.record("ISCSITargetExtentFind")
	return c.MockClient.ISCSITargetExtentFind(ctx, targetID, extentID)
}

func (c *apiCallCountingClient) ISCSITargetExtentFindByTarget(ctx context.Context, targetID int) ([]*truenas.ISCSITargetExtent, error) {
	c.record("ISCSITargetExtentFindByTarget")
	return c.MockClient.ISCSITargetExtentFindByTarget(ctx, targetID)
}

func (c *apiCallCountingClient) ISCSITargetExtentFindByExtent(ctx context.Context, extentID int) ([]*truenas.ISCSITargetExtent, error) {
	c.record("ISCSITargetExtentFindByExtent")
	return c.MockClient.ISCSITargetExtentFindByExtent(ctx, extentID)
}

func (c *apiCallCountingClient) ISCSIGlobalConfigGet(ctx context.Context) (*truenas.ISCSIGlobalConfig, error) {
	c.record("ISCSIGlobalConfigGet")
	return c.MockClient.ISCSIGlobalConfigGet(ctx)
}

func (c *apiCallCountingClient) NVMeoFHostFindByNQN(ctx context.Context, nqn string) (*truenas.NVMeoFHost, error) {
	c.record("NVMeoFHostFindByNQN")
	return c.MockClient.NVMeoFHostFindByNQN(ctx, nqn)
}

func (c *apiCallCountingClient) NVMeoFHostCreate(ctx context.Context, nqn string) (*truenas.NVMeoFHost, error) {
	c.record("NVMeoFHostCreate")
	return c.MockClient.NVMeoFHostCreate(ctx, nqn)
}

func (c *apiCallCountingClient) NVMeoFSubsystemCreate(ctx context.Context, name string, allowAnyHost bool, hostIDs []int) (*truenas.NVMeoFSubsystem, error) {
	c.record("NVMeoFSubsystemCreate")
	return c.MockClient.NVMeoFSubsystemCreate(ctx, name, allowAnyHost, hostIDs)
}

func (c *apiCallCountingClient) NVMeoFSubsystemDelete(ctx context.Context, id int) error {
	c.record("NVMeoFSubsystemDelete")
	return c.MockClient.NVMeoFSubsystemDelete(ctx, id)
}

func (c *apiCallCountingClient) NVMeoFSubsystemGet(ctx context.Context, id int) (*truenas.NVMeoFSubsystem, error) {
	c.record("NVMeoFSubsystemGet")
	return c.MockClient.NVMeoFSubsystemGet(ctx, id)
}

func (c *apiCallCountingClient) NVMeoFSubsystemFindByNQN(ctx context.Context, nqn string) (*truenas.NVMeoFSubsystem, error) {
	c.record("NVMeoFSubsystemFindByNQN")
	return c.MockClient.NVMeoFSubsystemFindByNQN(ctx, nqn)
}

func (c *apiCallCountingClient) NVMeoFSubsystemFindByName(ctx context.Context, name string) (*truenas.NVMeoFSubsystem, error) {
	c.record("NVMeoFSubsystemFindByName")
	return c.MockClient.NVMeoFSubsystemFindByName(ctx, name)
}

func (c *apiCallCountingClient) NVMeoFNamespaceCreate(ctx context.Context, subsystemID int, devicePath, deviceType string) (*truenas.NVMeoFNamespace, error) {
	c.record("NVMeoFNamespaceCreate")
	return c.MockClient.NVMeoFNamespaceCreate(ctx, subsystemID, devicePath, deviceType)
}

func (c *apiCallCountingClient) NVMeoFNamespaceDelete(ctx context.Context, id int) error {
	c.record("NVMeoFNamespaceDelete")
	return c.MockClient.NVMeoFNamespaceDelete(ctx, id)
}

func (c *apiCallCountingClient) NVMeoFNamespaceGet(ctx context.Context, id int) (*truenas.NVMeoFNamespace, error) {
	c.record("NVMeoFNamespaceGet")
	return c.MockClient.NVMeoFNamespaceGet(ctx, id)
}

func (c *apiCallCountingClient) NVMeoFNamespaceFindByDevice(ctx context.Context, subsystemID int, devicePath string) (*truenas.NVMeoFNamespace, error) {
	c.record("NVMeoFNamespaceFindByDevice")
	return c.MockClient.NVMeoFNamespaceFindByDevice(ctx, subsystemID, devicePath)
}

func (c *apiCallCountingClient) NVMeoFNamespaceFindByDevicePath(ctx context.Context, devicePath string) (*truenas.NVMeoFNamespace, error) {
	c.record("NVMeoFNamespaceFindByDevicePath")
	return c.MockClient.NVMeoFNamespaceFindByDevicePath(ctx, devicePath)
}

func (c *apiCallCountingClient) NVMeoFPortList(ctx context.Context) ([]*truenas.NVMeoFPort, error) {
	c.record("NVMeoFPortList")
	return c.MockClient.NVMeoFPortList(ctx)
}

func (c *apiCallCountingClient) NVMeoFPortCreate(ctx context.Context, transport, address string, port int) (*truenas.NVMeoFPort, error) {
	c.record("NVMeoFPortCreate")
	return c.MockClient.NVMeoFPortCreate(ctx, transport, address, port)
}

func (c *apiCallCountingClient) NVMeoFPortFindByAddress(ctx context.Context, transport, address string, port int) (*truenas.NVMeoFPort, error) {
	c.record("NVMeoFPortFindByAddress")
	return c.MockClient.NVMeoFPortFindByAddress(ctx, transport, address, port)
}

func (c *apiCallCountingClient) NVMeoFPortSubsysCreate(ctx context.Context, portID, subsysID int) (*truenas.NVMeoFPortSubsys, error) {
	c.record("NVMeoFPortSubsysCreate")
	return c.MockClient.NVMeoFPortSubsysCreate(ctx, portID, subsysID)
}

func (c *apiCallCountingClient) NVMeoFPortSubsysFindBySubsystem(ctx context.Context, subsysID int) (bool, error) {
	c.record("NVMeoFPortSubsysFindBySubsystem")
	return c.MockClient.NVMeoFPortSubsysFindBySubsystem(ctx, subsysID)
}

func (c *apiCallCountingClient) NVMeoFPortSubsysList(ctx context.Context) ([]*truenas.NVMeoFPortSubsys, error) {
	c.record("NVMeoFPortSubsysList")
	return c.MockClient.NVMeoFPortSubsysList(ctx)
}

func (c *apiCallCountingClient) NVMeoFPortSubsysListBySubsystem(ctx context.Context, subsysID int) ([]*truenas.NVMeoFPortSubsys, error) {
	c.record("NVMeoFPortSubsysListBySubsystem")
	return c.MockClient.NVMeoFPortSubsysListBySubsystem(ctx, subsysID)
}

func (c *apiCallCountingClient) NVMeoFPortSubsysDelete(ctx context.Context, id int) error {
	c.record("NVMeoFPortSubsysDelete")
	return c.MockClient.NVMeoFPortSubsysDelete(ctx, id)
}

func (c *apiCallCountingClient) NVMeoFSubsystemList(ctx context.Context) ([]*truenas.NVMeoFSubsystem, error) {
	c.record("NVMeoFSubsystemList")
	return c.MockClient.NVMeoFSubsystemList(ctx)
}

func (c *apiCallCountingClient) NVMeoFGetOrCreatePort(ctx context.Context, transport, address string, port int) (*truenas.NVMeoFPort, error) {
	c.record("NVMeoFGetOrCreatePort")
	return c.MockClient.NVMeoFGetOrCreatePort(ctx, transport, address, port)
}

func (c *apiCallCountingClient) NVMeoFGetTransportAddresses(ctx context.Context, transport string) ([]string, error) {
	c.record("NVMeoFGetTransportAddresses")
	return c.MockClient.NVMeoFGetTransportAddresses(ctx, transport)
}

var _ truenas.ClientInterface = (*apiCallCountingClient)(nil)

func newAPICallCountDriver(t *testing.T, client *apiCallCountingClient, protocol string) *Driver {
	t.Helper()
	d := &Driver{
		name: "org.scale.csi.test",
		config: &Config{
			DriverName: "org.scale.csi." + protocol,
			ZFS: ZFSConfig{
				DatasetParentName:   "pool/parent",
				DatasetEnableQuotas: true,
				ZvolReadyTimeout:    1,
			},
			NFS: NFSConfig{ShareHost: "192.0.2.10"},
			ISCSI: ISCSIConfig{
				TargetPortal:    "192.0.2.10:3260",
				ExtentBlocksize: 512,
				ExtentRpm:       "SSD",
			},
		},
		truenasClient: client,
	}
	d.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(ctx context.Context, service string) error {
		return client.ServiceReload(ctx, service)
	})
	t.Cleanup(d.serviceReloadDebouncer.Stop)
	return d
}

func apiCallCountVolumeRequest(name, protocol string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:               name,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": protocol},
	}
}

func assertAPICallCount(t *testing.T, operation string, client *apiCallCountingClient, want int) {
	t.Helper()
	got, methods := client.callSnapshot()
	if got != want {
		t.Errorf("%s API call count = %d, want %d (delta %+d); calls by method: %v", operation, got, want, got-want, methods)
	}
}

func TestControllerGoldenPathAPICallCounts(t *testing.T) {
	tests := []struct {
		name string
		want int
		run  func(*testing.T, *apiCallCountingClient, *Driver)
	}{
		// Five calls preserve the fresh NFS path's single lookup, create, share,
		// share-property update, and final batched ownership-property update.
		// TrueNAS 26.0 requires a post-create user-property update and an
		// authoritative re-read because inline create properties are silently lost.
		{name: "CreateVolume fresh NFS", want: 7, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
			_, err := d.CreateVolume(context.Background(), apiCallCountVolumeRequest("fresh-nfs", "nfs"))
			require.NoError(t, err)
		}},
		// The iSCSI baseline protects the no-lookup fresh path while retaining the
		// target, extent, association, property, reload, and response queries.
		{name: "CreateVolume fresh iSCSI", want: 12, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
			_, err := d.CreateVolume(context.Background(), apiCallCountVolumeRequest("fresh-iscsi", "iscsi"))
			require.NoError(t, err)
		}},
		// A fully provisioned NFS retry re-reads the dataset and verifies that the
		// stored TrueNAS share object still exists.
		{name: "CreateVolume idempotent retry", want: 2, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
			req := apiCallCountVolumeRequest("existing-nfs", "nfs")
			_, err := d.CreateVolume(context.Background(), req)
			require.NoError(t, err)
			client.resetCalls()
			_, err = d.CreateVolume(context.Background(), req)
			require.NoError(t, err)
		}},
		// NFS deletion validates the cached share ID's export-path backreference
		// before the dependency guards and destructive calls.
		{name: "DeleteVolume NFS", want: 6, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
			_, err := d.CreateVolume(context.Background(), apiCallCountVolumeRequest("delete-nfs", "nfs"))
			require.NoError(t, err)
			client.resetCalls()
			_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "delete-nfs"})
			require.NoError(t, err)
		}},
		// iSCSI deletion validates target, extent, and association backreferences
		// before cleanup, then retains the two dataset dependency guards.
		{name: "DeleteVolume iSCSI", want: 10, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
			_, err := d.CreateVolume(context.Background(), apiCallCountVolumeRequest("delete-iscsi", "iscsi"))
			require.NoError(t, err)
			client.resetCalls()
			_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "delete-iscsi"})
			require.NoError(t, err)
		}},
		// Fresh snapshot creation performs one source lookup, one global-name
		// lookup, and one atomic create carrying all identity properties.
		{name: "CreateSnapshot fresh", want: 3, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
			_, err := client.MockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
				Name: "pool/parent/snapshot-source", Type: "FILESYSTEM", Refquota: testGiB,
			})
			require.NoError(t, err)
			_, err = d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "fresh-snapshot", SourceVolumeId: "snapshot-source"})
			require.NoError(t, err)
		}},
		// An idempotent snapshot retry should only re-read the source and resolve the
		// globally unique short name; it must not rewrite properties.
		{name: "CreateSnapshot idempotent retry", want: 2, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
			_, err := client.MockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
				Name: "pool/parent/snapshot-retry-source", Type: "FILESYSTEM", Refquota: testGiB,
			})
			require.NoError(t, err)
			req := &csi.CreateSnapshotRequest{Name: "retry-snapshot", SourceVolumeId: "snapshot-retry-source"}
			_, err = d.CreateSnapshot(context.Background(), req)
			require.NoError(t, err)
			client.resetCalls()
			_, err = d.CreateSnapshot(context.Background(), req)
			require.NoError(t, err)
		}},
		// Simple deletion is one name resolution followed by one destroy call.
		{name: "DeleteSnapshot simple", want: 2, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
			_, err := client.MockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
				Name: "pool/parent/delete-snapshot-source", Type: "FILESYSTEM", Refquota: testGiB,
			})
			require.NoError(t, err)
			_, err = d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "delete-snapshot", SourceVolumeId: "delete-snapshot-source"})
			require.NoError(t, err)
			client.resetCalls()
			_, err = d.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "delete-snapshot"})
			require.NoError(t, err)
		}},
		// Clone-backed deletion adds the tombstone-ledger write with its verifying
		// re-read, the tombstone rename, property strip, deferred destroy after the
		// initial non-deferred destroy reports clones, and the ledger retirement
		// once the backend accepts the deferred destroy.
		{name: "DeleteSnapshot with clones", want: 8, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
			_, err := client.MockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
				Name: "pool/parent", Type: "FILESYSTEM",
			})
			require.NoError(t, err)
			_, err = client.MockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
				Name: "pool/parent/tombstone-source", Type: "FILESYSTEM", Refquota: testGiB,
			})
			require.NoError(t, err)
			created, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "tombstone-snapshot", SourceVolumeId: "tombstone-source"})
			require.NoError(t, err)
			snapshotID := "pool/parent/tombstone-source@" + created.GetSnapshot().GetSnapshotId()
			require.NoError(t, client.MockClient.SnapshotClone(context.Background(), snapshotID, "pool/parent/restored"))
			client.resetCalls()
			_, err = d.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "tombstone-snapshot"})
			require.NoError(t, err)
		}},
		// Snapshot cloning pins direct name resolution, the durable in-flight
		// marker write with its verifying re-read, one clone and readiness wait,
		// quota/share setup, ownership stamping with an authoritative re-read,
		// final identity updates, and marker retirement after the ownership stamp.
		{name: "CreateVolume clone from snapshot", want: 15, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
			_, err := client.MockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
				Name: "pool/parent", Type: "FILESYSTEM",
			})
			require.NoError(t, err)
			_, err = client.MockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
				Name: "pool/parent/clone-source", Type: "FILESYSTEM", Refquota: testGiB,
			})
			require.NoError(t, err)
			_, err = client.MockClient.SnapshotCreate(context.Background(), "pool/parent/clone-source", "clone-point", nil)
			require.NoError(t, err)
			req := apiCallCountVolumeRequest("restored-from-snapshot", "nfs")
			req.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "clone-point"},
			}}
			_, err = d.CreateVolume(context.Background(), req)
			require.NoError(t, err)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newAPICallCountingClient()
			protocol := "nfs"
			if tc.name == "CreateVolume fresh iSCSI" || tc.name == "DeleteVolume iSCSI" {
				protocol = "iscsi"
			}
			d := newAPICallCountDriver(t, client, protocol)
			tc.run(t, client, d)
			assertAPICallCount(t, tc.name, client, tc.want)
		})
	}
}
