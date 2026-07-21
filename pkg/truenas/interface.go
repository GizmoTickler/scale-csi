package truenas

import (
	"context"
	"time"
)

// ClientInterface defines the interface for the TrueNAS API client.
// This allows for mocking the client in unit tests.
type ClientInterface interface {
	// Core methods
	Close() error
	IsConnected() bool
	Call(ctx context.Context, method string, params ...interface{}) (interface{}, error)
	CallWithContext(ctx context.Context, method string, params ...interface{}) (interface{}, error) // Deprecated: Use Call instead

	// Circuit breaker methods
	CircuitBreakerStats() *CircuitBreakerStats
	ResetCircuitBreaker()

	// Dataset methods
	DatasetCreate(ctx context.Context, params *DatasetCreateParams) (*Dataset, error)
	DatasetDelete(ctx context.Context, name string, recursive, force bool) error
	DatasetGet(ctx context.Context, name string) (*Dataset, error)
	DatasetUpdate(ctx context.Context, name string, params *DatasetUpdateParams) (*Dataset, error)
	DatasetList(ctx context.Context, parentName string, limit, offset int) ([]*Dataset, error)
	DatasetSetUserProperty(ctx context.Context, name, key, value string) error
	DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error
	DatasetRemoveUserProperties(ctx context.Context, name string, keys []string) error
	DatasetGetUserProperty(ctx context.Context, name, key string) (string, error)
	DatasetExpand(ctx context.Context, name string, newSize int64) error
	DatasetExists(ctx context.Context, name string) (bool, error)
	DatasetHasDependentClones(ctx context.Context, datasetName string) (bool, error)
	GetPoolAvailable(ctx context.Context, poolName string) (int64, error)
	WaitForDatasetReady(ctx context.Context, name string, timeout time.Duration) (*Dataset, error)
	WaitForZvolReady(ctx context.Context, name string, timeout time.Duration) (*Dataset, error)

	// Snapshot methods
	SnapshotCreate(ctx context.Context, dataset, name string, userProperties map[string]string) (*Snapshot, error)
	SnapshotDelete(ctx context.Context, snapshotID string, defer_, recursive bool) error
	SnapshotRename(ctx context.Context, snapshotID, newName string) error
	SnapshotGet(ctx context.Context, snapshotID string) (*Snapshot, error)
	SnapshotList(ctx context.Context, dataset string) ([]*Snapshot, error)
	SnapshotListAll(ctx context.Context, parentDataset string, limit, offset int) ([]*Snapshot, error)
	SnapshotFindByName(ctx context.Context, parentDataset, name string) (*Snapshot, error)
	SnapshotSetUserProperty(ctx context.Context, snapshotID, key, value string) error
	SnapshotRemoveUserProperties(ctx context.Context, snapshotID string, keys []string) error
	SnapshotClone(ctx context.Context, snapshotID, newDatasetName string) error
	CopyDatasetFromSnapshotLocal(ctx context.Context, sourceDataset, snapshotShortName, targetDataset string) error
	DestroyReplicatedTargetSnapshot(ctx context.Context, targetDataset, snapshotShortName string) error
	SnapshotRollback(ctx context.Context, snapshotID string, force, recursive, recursiveClones bool) error

	// NFS methods
	NFSShareCreate(ctx context.Context, params *NFSShareCreateParams) (*NFSShare, error)
	NFSShareDelete(ctx context.Context, id int) error
	NFSShareGet(ctx context.Context, id int) (*NFSShare, error)
	NFSShareFindByPath(ctx context.Context, path string) (*NFSShare, error)
	NFSShareList(ctx context.Context) ([]*NFSShare, error)
	NFSShareUpdate(ctx context.Context, id int, params map[string]interface{}) (*NFSShare, error)

	// Service methods
	ServiceReload(ctx context.Context, service string) error

	// System information methods
	GetSystemInfo(ctx context.Context) (*SystemInfo, error)
	CheckNVMeoFSupport(ctx context.Context) error

	// iSCSI methods
	ISCSIPortalList(ctx context.Context) ([]*ISCSIPortal, error)
	ISCSIInitiatorList(ctx context.Context) ([]*ISCSIInitiator, error)
	ISCSIInitiatorCreate(ctx context.Context, comment string) (*ISCSIInitiator, error)
	ISCSIInitiatorCreateWithInitiators(ctx context.Context, initiators []string, comment string) (*ISCSIInitiator, error)
	ISCSIInitiatorGet(ctx context.Context, id int) (*ISCSIInitiator, error)
	ISCSIInitiatorUpdate(ctx context.Context, id int, initiators []string, comment string) (*ISCSIInitiator, error)
	ISCSIInitiatorDelete(ctx context.Context, id int) error
	ISCSITargetCreate(ctx context.Context, name, alias, mode string, groups []ISCSITargetGroup) (*ISCSITarget, error)
	ISCSITargetUpdate(ctx context.Context, id int, groups []ISCSITargetGroup) (*ISCSITarget, error)
	ISCSITargetDelete(ctx context.Context, id int, force bool) error
	ISCSITargetGet(ctx context.Context, id int) (*ISCSITarget, error)
	ISCSITargetFindByName(ctx context.Context, name string) (*ISCSITarget, error)
	ISCSIExtentCreate(ctx context.Context, name, diskPath, comment string, blocksize int, physicalBlocksize bool, rpm string) (*ISCSIExtent, error)
	ISCSIExtentDelete(ctx context.Context, id int, remove, force bool) error
	ISCSIExtentGet(ctx context.Context, id int) (*ISCSIExtent, error)
	ISCSIExtentFindByName(ctx context.Context, name string) (*ISCSIExtent, error)
	ISCSIExtentFindByDisk(ctx context.Context, diskPath string) (*ISCSIExtent, error)
	ISCSITargetExtentCreate(ctx context.Context, targetID, extentID, lunID int) (*ISCSITargetExtent, error)
	ISCSITargetExtentDelete(ctx context.Context, id int, force bool) error
	ISCSITargetExtentGet(ctx context.Context, id int) (*ISCSITargetExtent, error)
	ISCSITargetExtentFind(ctx context.Context, targetID, extentID int) (*ISCSITargetExtent, error)
	ISCSITargetExtentFindByTarget(ctx context.Context, targetID int) ([]*ISCSITargetExtent, error)
	ISCSITargetExtentFindByExtent(ctx context.Context, extentID int) ([]*ISCSITargetExtent, error)
	ISCSIGlobalConfigGet(ctx context.Context) (*ISCSIGlobalConfig, error)

	// NVMe-oF methods (updated for TrueNAS SCALE 25.10+)
	NVMeoFHostFindByNQN(ctx context.Context, nqn string) (*NVMeoFHost, error)
	NVMeoFHostCreate(ctx context.Context, nqn string) (*NVMeoFHost, error)
	NVMeoFHostSubsysCreate(ctx context.Context, hostID, subsysID int) (*NVMeoFHostSubsys, error)
	NVMeoFHostSubsysFind(ctx context.Context, hostID, subsysID int) (*NVMeoFHostSubsys, error)
	NVMeoFHostSubsysListBySubsystem(ctx context.Context, subsysID int) ([]*NVMeoFHostSubsys, error)
	NVMeoFHostSubsysDelete(ctx context.Context, id int) error
	NVMeoFSubsystemCreate(ctx context.Context, name string, allowAnyHost bool, hostIDs []int) (*NVMeoFSubsystem, error)
	NVMeoFSubsystemUpdateAllowAnyHost(ctx context.Context, id int, allowAnyHost bool) (*NVMeoFSubsystem, error)
	NVMeoFSubsystemDelete(ctx context.Context, id int) error
	NVMeoFSubsystemGet(ctx context.Context, id int) (*NVMeoFSubsystem, error)
	NVMeoFSubsystemFindByNQN(ctx context.Context, nqn string) (*NVMeoFSubsystem, error)
	NVMeoFSubsystemFindByName(ctx context.Context, name string) (*NVMeoFSubsystem, error)
	NVMeoFNamespaceCreate(ctx context.Context, subsystemID int, devicePath, deviceType string) (*NVMeoFNamespace, error)
	NVMeoFNamespaceDelete(ctx context.Context, id int) error
	NVMeoFNamespaceGet(ctx context.Context, id int) (*NVMeoFNamespace, error)
	NVMeoFNamespaceFindByDevice(ctx context.Context, subsystemID int, devicePath string) (*NVMeoFNamespace, error)
	NVMeoFNamespaceFindByDevicePath(ctx context.Context, devicePath string) (*NVMeoFNamespace, error)
	NVMeoFPortList(ctx context.Context) ([]*NVMeoFPort, error)
	NVMeoFPortCreate(ctx context.Context, transport, address string, port int) (*NVMeoFPort, error)
	NVMeoFPortFindByAddress(ctx context.Context, transport, address string, port int) (*NVMeoFPort, error)
	NVMeoFPortSubsysCreate(ctx context.Context, portID, subsysID int) (*NVMeoFPortSubsys, error)
	NVMeoFPortSubsysFindBySubsystem(ctx context.Context, subsysID int) (bool, error)
	NVMeoFPortSubsysList(ctx context.Context) ([]*NVMeoFPortSubsys, error)
	NVMeoFPortSubsysListBySubsystem(ctx context.Context, subsysID int) ([]*NVMeoFPortSubsys, error)
	NVMeoFPortSubsysDelete(ctx context.Context, id int) error
	NVMeoFSubsystemList(ctx context.Context) ([]*NVMeoFSubsystem, error)
	NVMeoFGetOrCreatePort(ctx context.Context, transport string, address string, port int) (*NVMeoFPort, error)
	InvalidateNVMeoFPort(transport, address string, port int)
	NVMeoFGetTransportAddresses(ctx context.Context, transport string) ([]string, error)
}
