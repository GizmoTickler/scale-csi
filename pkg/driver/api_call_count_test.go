package driver

import (
	"context"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
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

func (c *apiCallCountingClient) DatasetGetByNames(ctx context.Context, names []string) (map[string]*truenas.Dataset, error) {
	c.record("DatasetGetByNames")
	return c.MockClient.DatasetGetByNames(ctx, names)
}

func (c *apiCallCountingClient) DatasetUpdate(ctx context.Context, name string, params *truenas.DatasetUpdateParams) (*truenas.Dataset, error) {
	c.record("DatasetUpdate")
	return c.MockClient.DatasetUpdate(ctx, name, params)
}

func (c *apiCallCountingClient) DatasetList(ctx context.Context, parentName string, limit, offset int) ([]*truenas.Dataset, error) {
	c.record("DatasetList")
	return c.MockClient.DatasetList(ctx, parentName, limit, offset)
}

func (c *apiCallCountingClient) DatasetQueryByParent(ctx context.Context, parentDataset string) ([]*truenas.Dataset, error) {
	c.record("DatasetQueryByParent")
	return c.MockClient.DatasetQueryByParent(ctx, parentDataset)
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

func (c *apiCallCountingClient) CopyDatasetFromSnapshotLocal(ctx context.Context, sourceDataset, snapshotShortName, targetDataset string) (int64, error) {
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

func (c *apiCallCountingClient) ISCSITargetList(ctx context.Context) ([]*truenas.ISCSITarget, error) {
	c.record("ISCSITargetList")
	return c.MockClient.ISCSITargetList(ctx)
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

func (c *apiCallCountingClient) ISCSIExtentList(ctx context.Context) ([]*truenas.ISCSIExtent, error) {
	c.record("ISCSIExtentList")
	return c.MockClient.ISCSIExtentList(ctx)
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

func (c *apiCallCountingClient) NVMeoFNamespaceListBySubsystem(ctx context.Context, subsysID int) ([]*truenas.NVMeoFNamespace, error) {
	c.record("NVMeoFNamespaceListBySubsystem")
	return c.MockClient.NVMeoFNamespaceListBySubsystem(ctx, subsysID)
}

func (c *apiCallCountingClient) NVMeoFNamespaceList(ctx context.Context) ([]*truenas.NVMeoFNamespace, error) {
	c.record("NVMeoFNamespaceList")
	return c.MockClient.NVMeoFNamespaceList(ctx)
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

func (c *apiCallCountingClient) InvalidateNVMeoFPort(transport, address string, port int) {
	c.record("InvalidateNVMeoFPort")
	c.MockClient.InvalidateNVMeoFPort(transport, address, port)
}

func (c *apiCallCountingClient) NVMeoFGetTransportAddresses(ctx context.Context, transport string) ([]string, error) {
	c.record("NVMeoFGetTransportAddresses")
	return c.MockClient.NVMeoFGetTransportAddresses(ctx, transport)
}

func (c *apiCallCountingClient) ISCSIPortalList(ctx context.Context) ([]*truenas.ISCSIPortal, error) {
	c.record("ISCSIPortalList")
	return c.MockClient.ISCSIPortalList(ctx)
}

func (c *apiCallCountingClient) ISCSIInitiatorList(ctx context.Context) ([]*truenas.ISCSIInitiator, error) {
	c.record("ISCSIInitiatorList")
	return c.MockClient.ISCSIInitiatorList(ctx)
}

func (c *apiCallCountingClient) ISCSIInitiatorCreate(ctx context.Context, comment string) (*truenas.ISCSIInitiator, error) {
	c.record("ISCSIInitiatorCreate")
	return c.MockClient.ISCSIInitiatorCreate(ctx, comment)
}

func (c *apiCallCountingClient) ISCSIInitiatorCreateWithInitiators(ctx context.Context, initiators []string, comment string) (*truenas.ISCSIInitiator, error) {
	c.record("ISCSIInitiatorCreateWithInitiators")
	return c.MockClient.ISCSIInitiatorCreateWithInitiators(ctx, initiators, comment)
}

func (c *apiCallCountingClient) ISCSIInitiatorGet(ctx context.Context, id int) (*truenas.ISCSIInitiator, error) {
	c.record("ISCSIInitiatorGet")
	return c.MockClient.ISCSIInitiatorGet(ctx, id)
}

func (c *apiCallCountingClient) ISCSIInitiatorUpdate(ctx context.Context, id int, initiators []string, comment string) (*truenas.ISCSIInitiator, error) {
	c.record("ISCSIInitiatorUpdate")
	return c.MockClient.ISCSIInitiatorUpdate(ctx, id, initiators, comment)
}

func (c *apiCallCountingClient) ISCSIInitiatorDelete(ctx context.Context, id int) error {
	c.record("ISCSIInitiatorDelete")
	return c.MockClient.ISCSIInitiatorDelete(ctx, id)
}

func (c *apiCallCountingClient) ISCSITargetUpdate(ctx context.Context, id int, groups []truenas.ISCSITargetGroup) (*truenas.ISCSITarget, error) {
	c.record("ISCSITargetUpdate")
	return c.MockClient.ISCSITargetUpdate(ctx, id, groups)
}

func (c *apiCallCountingClient) NVMeoFHostSubsysCreate(ctx context.Context, hostID, subsysID int) (*truenas.NVMeoFHostSubsys, error) {
	c.record("NVMeoFHostSubsysCreate")
	return c.MockClient.NVMeoFHostSubsysCreate(ctx, hostID, subsysID)
}

func (c *apiCallCountingClient) NVMeoFHostSubsysFind(ctx context.Context, hostID, subsysID int) (*truenas.NVMeoFHostSubsys, error) {
	c.record("NVMeoFHostSubsysFind")
	return c.MockClient.NVMeoFHostSubsysFind(ctx, hostID, subsysID)
}

func (c *apiCallCountingClient) NVMeoFHostSubsysListBySubsystem(ctx context.Context, subsysID int) ([]*truenas.NVMeoFHostSubsys, error) {
	c.record("NVMeoFHostSubsysListBySubsystem")
	return c.MockClient.NVMeoFHostSubsysListBySubsystem(ctx, subsysID)
}

func (c *apiCallCountingClient) NVMeoFHostSubsysDelete(ctx context.Context, id int) error {
	c.record("NVMeoFHostSubsysDelete")
	return c.MockClient.NVMeoFHostSubsysDelete(ctx, id)
}

func (c *apiCallCountingClient) NVMeoFSubsystemUpdateAllowAnyHost(ctx context.Context, id int, allowAnyHost bool) (*truenas.NVMeoFSubsystem, error) {
	c.record("NVMeoFSubsystemUpdateAllowAnyHost")
	return c.MockClient.NVMeoFSubsystemUpdateAllowAnyHost(ctx, id, allowAnyHost)
}

func (c *apiCallCountingClient) ReplicationJobList(ctx context.Context) ([]*truenas.ReplicationJob, error) {
	c.record("ReplicationJobList")
	return c.MockClient.ReplicationJobList(ctx)
}

func (c *apiCallCountingClient) ReplicationJobAbort(ctx context.Context, jobID int64, reason string) error {
	c.record("ReplicationJobAbort")
	return c.MockClient.ReplicationJobAbort(ctx, jobID, reason)
}

var _ truenas.ClientInterface = (*apiCallCountingClient)(nil)

func TestAPICallCountingClientWrapsEveryClientInterfaceMethod(t *testing.T) {
	interfaceType := reflect.TypeOf((*truenas.ClientInterface)(nil)).Elem()
	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
	for i := 0; i < interfaceType.NumMethod(); i++ {
		interfaceMethod := interfaceType.Method(i)
		t.Run(interfaceMethod.Name, func(t *testing.T) {
			client := newAPICallCountingClient()
			method := reflect.ValueOf(client).MethodByName(interfaceMethod.Name)
			require.True(t, method.IsValid())
			cancelledContext, cancel := context.WithCancel(context.Background())
			cancel()
			args := make([]reflect.Value, method.Type().NumIn())
			for argIndex := range args {
				argType := method.Type().In(argIndex)
				switch {
				case argType.Implements(contextType):
					args[argIndex] = reflect.ValueOf(cancelledContext)
				case argType.Kind() == reflect.Pointer:
					args[argIndex] = reflect.New(argType.Elem())
				case argType.Kind() == reflect.Slice:
					args[argIndex] = reflect.MakeSlice(argType, 0, 0)
				case argType.Kind() == reflect.Map:
					args[argIndex] = reflect.MakeMap(argType)
				default:
					args[argIndex] = reflect.Zero(argType)
				}
			}
			func() {
				defer func() {
					_ = recover()
				}()
				if method.Type().IsVariadic() {
					method.CallSlice(args)
				} else {
					method.Call(args)
				}
			}()
			_, calls := client.callSnapshot()
			assert.Equal(t, 1, calls[interfaceMethod.Name],
				"ClientInterface method %s is silently promoted instead of explicitly recorded", interfaceMethod.Name)
		})
	}
}

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
		// Six calls: existence lookup; DatasetCreate; the createDataset ownership
		// stamp via pool.dataset.update; the one-time post-connect re-read that
		// verifies that first update against a fresh query (every later update in
		// this Driver's life trusts the update response alone); NFSShareCreate; and
		// a single post-share update that folds the share-ID stamp together with the
		// managed/ownership/provision/name stamps. TrueNAS 26.0 requires a
		// post-create user-property update because inline create properties are
		// silently lost; the share-ID and volume-property stamps share one
		// pool.dataset.update because both sit on the same side of the
		// NFSShareCreate boundary. Dropping below six would require removing either
		// the one-time paranoia re-read or the existence lookup, both crash-safety
		// guards, so 6 is the safe floor.
		{name: "CreateVolume fresh NFS", want: 6, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
			_, err := d.CreateVolume(context.Background(), apiCallCountVolumeRequest("fresh-nfs", "nfs"))
			require.NoError(t, err)
		}},
		// Fourteen calls: existence DatasetGet; DatasetCreate (zvol); the
		// createDataset ownership stamp via pool.dataset.update (DatasetUpdate) plus
		// the one-time post-connect verifying re-read (DatasetGet); the automatic
		// iSCSI target-group resolution (ISCSIPortalList + ISCSIInitiatorList — these
		// two were invisible until the counting client wrapped the full surface);
		// ISCSITargetCreate; ISCSIExtentCreate; ISCSITargetExtentCreate; the in-share
		// resource-ID stamp (DatasetSetUserProperties); the debounced ServiceReload;
		// getVolumeContext's ISCSITargetGet + ISCSIGlobalConfigGet; and the final
		// managed/ownership/provision/name stamp (DatasetSetUserProperties).
		{name: "CreateVolume fresh iSCSI", want: 14, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
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
		// Thirteen calls: existence lookup; snapshot name resolution; the durable
		// in-flight marker write on the parent dataset via pool.dataset.update plus
		// its one-time post-connect verifying re-read (the marker mechanism stays
		// intact); one clone and readiness wait; the quota-setting update; the
		// content-source identity update; the ownership stamp via pool.dataset.update
		// (now verified against the update response, so no separate re-read — the
		// one-time re-read was already spent on the marker write); marker retirement
		// after the durable ownership stamp; NFS share resolution + create; and a
		// single post-share update that folds the share-ID stamp together with the
		// managed/ownership/provision/name stamps. The remaining writes are separated
		// by crash boundaries (content-source vs ownership vs share-ID) or are the
		// protected marker mechanism, so they are not merged; 13 is the safe floor
		// short of crossing those boundaries.
		{name: "CreateVolume clone from snapshot", want: 13, run: func(t *testing.T, client *apiCallCountingClient, d *Driver) {
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

// newFencedAPICallCountDriver builds a Driver for the publish/unpublish golden
// counts. Unlike newAPICallCountDriver it configures a fencing mode and every
// protocol's backend settings (plus the NVMe-oF host-ID cache) so the same
// builder serves the NFS off/additive and NVMe-oF strict cases.
func newFencedAPICallCountDriver(t *testing.T, client *apiCallCountingClient, protocol string, mode FencingMode) *Driver {
	t.Helper()
	d := &Driver{
		name: "org.scale.csi.test",
		config: &Config{
			DriverName: "org.scale.csi." + protocol,
			Fencing:    FencingConfig{Mode: mode},
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", DatasetEnableQuotas: true, ZvolReadyTimeout: 1},
			NFS:        NFSConfig{ShareHost: "192.0.2.10", ShareAllowedNetworks: []string{"192.0.2.0/24"}},
			ISCSI:      ISCSIConfig{TargetPortal: "192.0.2.10:3260", ExtentBlocksize: 512, ExtentRpm: "SSD"},
			NVMeoF: NVMeoFConfig{
				Transport:          "TCP",
				TransportAddress:   "192.0.2.20",
				TransportServiceID: 4420,
			},
		},
		truenasClient:     client,
		nvmeResolvedHosts: make(map[string]int),
	}
	d.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(ctx context.Context, service string) error {
		return client.ServiceReload(ctx, service)
	})
	t.Cleanup(d.serviceReloadDebouncer.Stop)
	return d
}

func nfsPublishRequest(volumeID, nodeID string) *csi.ControllerPublishVolumeRequest {
	return &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   nodeID,
		VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		}},
		VolumeContext: map[string]string{"node_attach_driver": "nfs"},
	}
}

func nvmeoFPublishRequest(volumeID, nodeID string) *csi.ControllerPublishVolumeRequest {
	return &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeID,
		NodeId:   nodeID,
		VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		}},
		VolumeContext: map[string]string{"node_attach_driver": "nvmeof"},
	}
}

// TestControllerPublishUnpublishGoldenAPICallCounts pins the round-trip cost of
// the publish/unpublish path (the P1 optimization target). Each case documents
// every driver-to-TrueNAS call so a regression that re-introduces a duplicate
// resolution or a wasted write fails with an explicit per-method delta.
func TestControllerPublishUnpublishGoldenAPICallCounts(t *testing.T) {
	ctx := context.Background()

	// (a) fencing OFF + NFS — the records-only floor. Backend allowlist
	// enforcement is disabled, so the publish maintains ONLY the durable
	// publication record and the unpublish clears it; no share mutation occurs.
	t.Run("off NFS publish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newFencedAPICallCountDriver(t, client, "nfs", FencingModeOff)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("off-nfs", "nfs"))
		require.NoError(t, err)
		nodeA, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.31")}})
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.ControllerPublishVolume(ctx, nfsPublishRequest("off-nfs", nodeA))
		require.NoError(t, err)
		// Three calls:
		// 1. DatasetGet                      — ControllerPublishVolume volume read.
		// 2. NFSShareGet                     — ensureShareExists verifies the stored
		//                                      share ID's export-path backreference.
		// 3. DatasetSetUserProperties        — storePublicationRecord writes the
		//                                      durable publication record (off mode
		//                                      skips validateBackend/applyBackendFence
		//                                      entirely, so this is the floor).
		assertAPICallCount(t, "off NFS publish", client, 3)
	})
	t.Run("off NFS unpublish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newFencedAPICallCountDriver(t, client, "nfs", FencingModeOff)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("off-nfs-unpub", "nfs"))
		require.NoError(t, err)
		nodeA, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.31")}})
		require.NoError(t, err)
		_, err = d.ControllerPublishVolume(ctx, nfsPublishRequest("off-nfs-unpub", nodeA))
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "off-nfs-unpub", NodeId: nodeA})
		require.NoError(t, err)
		// Three calls:
		// 1. DatasetGet                      — ControllerUnpublishVolume volume read.
		// 2. DatasetSetUserProperties        — flip the record to "unpublishing"
		//                                      BEFORE access is removed (crash-safe
		//                                      tombstone; a restart can never re-add).
		// 3. DatasetRemoveUserProperties     — removePublicationRecords clears the
		//                                      durable record (off mode never touches
		//                                      a backend allowlist).
		assertAPICallCount(t, "off NFS unpublish", client, 3)
	})

	// (b) additive + NFS — backend enforcement on, static policy preserved. The
	// publish resolves the share once for the compatibility check and reuses it
	// (memoized) for the fence, then converges the host list; the unpublish
	// revokes the CSI-added host.
	t.Run("additive NFS publish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newFencedAPICallCountDriver(t, client, "nfs", FencingModeAdditive)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("additive-nfs", "nfs"))
		require.NoError(t, err)
		nodeA, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.31")}})
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.ControllerPublishVolume(ctx, nfsPublishRequest("additive-nfs", nodeA))
		require.NoError(t, err)
		// Five calls:
		// 1. DatasetGet                      — ControllerPublishVolume volume read.
		// 2. NFSShareGet                     — ensureShareExists backreference check.
		// 3. NFSShareGet                     — validateBackendSingleNodeCompatibility
		//                                      reads the allowlist (the ensure path
		//                                      does not memoize the NFS share, so this
		//                                      is a fresh read; applyNFSFence then
		//                                      reuses THIS one via the request memo).
		// 4. DatasetSetUserProperties        — storePublicationRecord (carries the
		//                                      additive CSI-added-host provenance).
		// 5. NFSShareUpdate                  — applyNFSFence converges the host list.
		assertAPICallCount(t, "additive NFS publish", client, 5)
	})
	t.Run("additive NFS unpublish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newFencedAPICallCountDriver(t, client, "nfs", FencingModeAdditive)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("additive-nfs-unpub", "nfs"))
		require.NoError(t, err)
		nodeA, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", IPs: []net.IP{net.ParseIP("192.0.2.31")}})
		require.NoError(t, err)
		_, err = d.ControllerPublishVolume(ctx, nfsPublishRequest("additive-nfs-unpub", nodeA))
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "additive-nfs-unpub", NodeId: nodeA})
		require.NoError(t, err)
		// Five calls:
		// 1. DatasetGet                      — ControllerUnpublishVolume volume read.
		// 2. DatasetSetUserProperties        — flip the record to "unpublishing".
		// 3. NFSShareGet                     — applyNFSFence resolves the share (fresh
		//                                      memo for this request).
		// 4. NFSShareUpdate                  — applyNFSFence revokes the CSI-added
		//                                      host (only durable scale-csi grants are
		//                                      removable in additive mode).
		// 5. DatasetRemoveUserProperties     — removePublicationRecords.
		assertAPICallCount(t, "additive NFS unpublish", client, 5)
	})

	// (c) strict + NVMe-oF single node — the live hot path P1 optimizes. The
	// measured publish is a steady-state republish: the setup publish associates
	// the node and warms the per-driver host-ID cache. Namespace/subsystem
	// identity remains memoized within the request, but enforcement deliberately
	// fresh-lists at its mutation boundary, unconditionally asserts the desired
	// association, and fresh-lists again before removals. The unpublish uses the
	// same enforcement-boundary freshness before revoking the association.
	t.Run("strict NVMe-oF publish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newFencedAPICallCountDriver(t, client, "nvmeof", FencingModeStrict)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("strict-nvme", "nvmeof"))
		require.NoError(t, err)
		nodeA, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-a"})
		require.NoError(t, err)
		// Setup publish: associates worker-a and warms the host-ID cache so the
		// measured republish reflects the cached steady state (not measured).
		_, err = d.ControllerPublishVolume(ctx, nvmeoFPublishRequest("strict-nvme", nodeA))
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.ControllerPublishVolume(ctx, nvmeoFPublishRequest("strict-nvme", nodeA))
		require.NoError(t, err)
		// Nine calls (steady-state republish; ~13 before P1):
		// 1. DatasetGet                      — ControllerPublishVolume volume read.
		// 2. NVMeoFNamespaceGet              — ensureShare resolves the namespace
		//                                      (memoized for the rest of the request).
		// 3. NVMeoFSubsystemGet              — ensureShare resolves the subsystem.
		//    (repair-stamp write SKIPPED: the dataset already carries both IDs.)
		// 4. NVMeoFHostFindByNQN             — validateBackend resolves the exempt
		//                                      node NQN to a host ID.
		// 5. NVMeoFHostSubsysListBySubsystem — validateBackend reads the allowlist
		//                                      for compatibility/classification.
		// 6. DatasetSetUserProperties        — storePublicationRecord.
		// 7. NVMeoFHostSubsysListBySubsystem — enforcement-boundary fresh read;
		//                                      compatibility state is not reused.
		// 8. NVMeoFHostSubsysCreate          — unconditional idempotent assertion
		//                                      of the desired association.
		// 9. NVMeoFHostSubsysListBySubsystem — fresh post-create removal view.
		assertAPICallCount(t, "strict NVMe-oF publish", client, 9)
	})
	t.Run("strict NVMe-oF unpublish", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newFencedAPICallCountDriver(t, client, "nvmeof", FencingModeStrict)
		_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("strict-nvme-unpub", "nvmeof"))
		require.NoError(t, err)
		nodeA, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-a"})
		require.NoError(t, err)
		_, err = d.ControllerPublishVolume(ctx, nvmeoFPublishRequest("strict-nvme-unpub", nodeA))
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "strict-nvme-unpub", NodeId: nodeA})
		require.NoError(t, err)
		// Nine calls:
		// 1. DatasetGet                      — ControllerUnpublishVolume volume read.
		// 2. DatasetSetUserProperties        — flip the record to "unpublishing".
		// 3. NVMeoFNamespaceGet              — applyNVMeFence resolves the namespace
		//                                      (fresh memo for this request).
		// 4. NVMeoFSubsystemGet              — applyNVMeFence resolves the subsystem.
		// 5. NVMeoFHostSubsysListBySubsystem — enforcement-boundary fresh read.
		// 6. NVMeoFHostFindByNQN             — resolve the removing NQN to a host ID.
		// 7. NVMeoFHostSubsysListBySubsystem — fresh post-create removal view
		//                                      (there are no desired creates here).
		// 8. NVMeoFHostSubsysDelete          — revoke worker-a's association.
		// 9. DatasetRemoveUserProperties     — removePublicationRecords.
		assertAPICallCount(t, "strict NVMe-oF unpublish", client, 9)
	})
}

// TestCreateVolumeCloneScrubInheritedProtocolProperties proves the P7 scrub: a
// clone inherits its source dataset's backend share-object IDs (ZFS copies user
// properties into the clone), and CreateVolume scrubs them right after the
// ownership stamp so ensureShareExists never validates the clone against the
// SOURCE volume's share objects. Foreign protocol IDs are removed entirely; the
// clone's own protocol gets a fresh share ID.
func TestCreateVolumeCloneScrubInheritedProtocolProperties(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newAPICallCountDriver(t, client, "nfs")

	_, err := client.MockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	// Source volume dataset carrying backend share-object IDs (as a live, shared
	// volume would). The clone inherits these with a non-local source.
	source, err := client.MockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/clone-src", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, client.MockClient.DatasetSetUserProperties(ctx, source.Name, map[string]string{
		PropNFSShareID:        "999",
		PropISCSITargetID:     "888",
		PropNVMeoFSubsystemID: "777",
	}))
	_, err = client.MockClient.SnapshotCreate(ctx, source.Name, "clone-point", nil)
	require.NoError(t, err)

	req := apiCallCountVolumeRequest("restored-clone", "nfs")
	req.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "clone-point"},
	}}
	_, err = d.CreateVolume(ctx, req)
	require.NoError(t, err)

	clone, err := client.MockClient.DatasetGet(ctx, "pool/parent/restored-clone")
	require.NoError(t, err)
	_, hasISCSI := clone.UserProperties[PropISCSITargetID]
	assert.False(t, hasISCSI, "the inherited iSCSI target ID must be scrubbed from the NFS clone")
	_, hasNVMe := clone.UserProperties[PropNVMeoFSubsystemID]
	assert.False(t, hasNVMe, "the inherited NVMe-oF subsystem ID must be scrubbed from the NFS clone")
	assert.NotEqual(t, "999", clone.UserProperties[PropNFSShareID].Value,
		"the clone must stamp its own NFS share ID, not the inherited stale one")

	// Source-aware/current-protocol belt: a local foreign-protocol property and
	// an inherited property belonging to the target protocol both survive. Only
	// the provably inherited foreign-protocol property is removed.
	scrubTarget, err := client.MockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/source-aware-scrub", Type: "FILESYSTEM",
	})
	require.NoError(t, err)
	require.NoError(t, client.MockClient.DatasetSetUserProperties(ctx, scrubTarget.Name, map[string]string{
		PropNFSShareID:         "current-protocol",
		PropISCSITargetID:      "local-foreign",
		PropNVMeoFSubsystemID:  "inherited-foreign",
		PropNVMeoFPortSubsysID: "unknown-source",
	}))
	scrubTarget, err = client.MockClient.DatasetGet(ctx, scrubTarget.Name)
	require.NoError(t, err)
	scrubTarget.UserProperties[PropNFSShareID] = truenas.UserProperty{
		Value: "current-protocol", Source: "pool/parent/source@snap",
	}
	scrubTarget.UserProperties[PropNVMeoFSubsystemID] = truenas.UserProperty{
		Value: "inherited-foreign", Source: "pool/parent/source@snap",
	}
	scrubTarget.UserProperties[PropNVMeoFPortSubsysID] = truenas.UserProperty{
		Value: "unknown-source", Source: "",
	}
	d.scrubInheritedProtocolProperties(ctx, scrubTarget, scrubTarget.Name, ShareTypeNFS)
	scrubbed, err := client.MockClient.DatasetGet(ctx, scrubTarget.Name)
	require.NoError(t, err)
	assert.Equal(t, "current-protocol", scrubbed.UserProperties[PropNFSShareID].Value,
		"all properties of the volume's current protocol survive regardless of source")
	assert.Equal(t, "local-foreign", scrubbed.UserProperties[PropISCSITargetID].Value,
		"local properties survive even when they belong to another protocol")
	_, hasInheritedForeign := scrubbed.UserProperties[PropNVMeoFSubsystemID]
	assert.False(t, hasInheritedForeign, "only provably inherited foreign-protocol properties are scrubbed")
	assert.Equal(t, "unknown-source", scrubbed.UserProperties[PropNVMeoFPortSubsysID].Value,
		"sourceless properties are not provably inherited and must survive")
}
