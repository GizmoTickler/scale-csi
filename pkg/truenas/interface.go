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

	// Dataset methods
	DatasetCreate(ctx context.Context, params *DatasetCreateParams) (*Dataset, error)
	DatasetDelete(ctx context.Context, name string, recursive bool, force bool) error
	DatasetGet(ctx context.Context, name string) (*Dataset, error)
	DatasetUpdate(ctx context.Context, name string, params *DatasetUpdateParams) (*Dataset, error)
	DatasetList(ctx context.Context, parentName string, limit int, offset int) ([]*Dataset, error)
	DatasetSetUserProperty(ctx context.Context, name string, key string, value string) error
	DatasetGetUserProperty(ctx context.Context, name string, key string) (string, error)
	DatasetExpand(ctx context.Context, name string, newSize int64) error
	DatasetExists(ctx context.Context, name string) (bool, error)
	GetPoolAvailable(ctx context.Context, poolName string) (int64, error)
	WaitForDatasetReady(ctx context.Context, name string, timeout time.Duration) (*Dataset, error)
	WaitForZvolReady(ctx context.Context, name string, timeout time.Duration) (*Dataset, error)

	// Snapshot methods
	SnapshotCreate(ctx context.Context, dataset string, name string) (*Snapshot, error)
	SnapshotDelete(ctx context.Context, snapshotID string, defer_ bool, recursive bool) error
	SnapshotGet(ctx context.Context, snapshotID string) (*Snapshot, error)
	SnapshotList(ctx context.Context, dataset string) ([]*Snapshot, error)
	SnapshotListAll(ctx context.Context, parentDataset string, limit int, offset int) ([]*Snapshot, error)
	SnapshotFindByName(ctx context.Context, parentDataset string, name string) (*Snapshot, error)
	SnapshotSetUserProperty(ctx context.Context, snapshotID string, key string, value string) error
	SnapshotClone(ctx context.Context, snapshotID string, newDatasetName string) error
	SnapshotRollback(ctx context.Context, snapshotID string, force bool, recursive bool, recursiveClones bool) error

	// NFS methods
	NFSShareCreate(ctx context.Context, params *NFSShareCreateParams) (*NFSShare, error)
	NFSShareDelete(ctx context.Context, id int) error
	NFSShareGet(ctx context.Context, id int) (*NFSShare, error)
	NFSShareFindByPath(ctx context.Context, path string) (*NFSShare, error)
	NFSShareList(ctx context.Context) ([]*NFSShare, error)
	NFSShareUpdate(ctx context.Context, id int, params map[string]interface{}) (*NFSShare, error)

	// Service methods
	ServiceReload(ctx context.Context, service string) error

	// iSCSI methods
	ISCSITargetCreate(ctx context.Context, name string, alias string, mode string, groups []ISCSITargetGroup) (*ISCSITarget, error)
	ISCSITargetDelete(ctx context.Context, id int, force bool) error
	ISCSITargetGet(ctx context.Context, id int) (*ISCSITarget, error)
	ISCSITargetFindByName(ctx context.Context, name string) (*ISCSITarget, error)
	ISCSIExtentCreate(ctx context.Context, name string, diskPath string, comment string, blocksize int, rpm string) (*ISCSIExtent, error)
	ISCSIExtentDelete(ctx context.Context, id int, remove bool, force bool) error
	ISCSIExtentGet(ctx context.Context, id int) (*ISCSIExtent, error)
	ISCSIExtentFindByName(ctx context.Context, name string) (*ISCSIExtent, error)
	ISCSIExtentFindByDisk(ctx context.Context, diskPath string) (*ISCSIExtent, error)
	ISCSITargetExtentCreate(ctx context.Context, targetID int, extentID int, lunID int) (*ISCSITargetExtent, error)
	ISCSITargetExtentDelete(ctx context.Context, id int, force bool) error
	ISCSITargetExtentFind(ctx context.Context, targetID int, extentID int) (*ISCSITargetExtent, error)
	ISCSITargetExtentFindByTarget(ctx context.Context, targetID int) ([]*ISCSITargetExtent, error)
	ISCSITargetExtentFindByExtent(ctx context.Context, extentID int) ([]*ISCSITargetExtent, error)
	ISCSIGlobalConfigGet(ctx context.Context) (*ISCSIGlobalConfig, error)

	// NVMe-oF methods
	NVMeoFSubsystemCreate(ctx context.Context, nqn string, serial string, allowAnyHost bool, hosts []string) (*NVMeoFSubsystem, error)
	NVMeoFSubsystemDelete(ctx context.Context, id int) error
	NVMeoFSubsystemGet(ctx context.Context, id int) (*NVMeoFSubsystem, error)
	NVMeoFSubsystemFindByNQN(ctx context.Context, nqn string) (*NVMeoFSubsystem, error)
	NVMeoFNamespaceCreate(ctx context.Context, subsystemID int, devicePath string) (*NVMeoFNamespace, error)
	NVMeoFNamespaceDelete(ctx context.Context, id int) error
	NVMeoFNamespaceGet(ctx context.Context, id int) (*NVMeoFNamespace, error)
	NVMeoFNamespaceFindByDevice(ctx context.Context, subsystemID int, devicePath string) (*NVMeoFNamespace, error)
	NVMeoFPortList(ctx context.Context) ([]*NVMeoFPort, error)
	NVMeoFGetTransportAddresses(ctx context.Context, transport string) ([]string, error)
}
