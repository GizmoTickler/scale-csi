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

	// AnyConnectionJobSubscribed reports whether a pooled connection holds a live
	// core.get_jobs subscription (false = pure-poll fallback).
	AnyConnectionJobSubscribed() bool

	// Dataset methods
	DatasetCreate(ctx context.Context, params *DatasetCreateParams) (*Dataset, error)
	DatasetDelete(ctx context.Context, name string, recursive, force bool) error
	DatasetGet(ctx context.Context, name string) (*Dataset, error)
	DatasetGetByNames(ctx context.Context, names []string) (map[string]*Dataset, error)
	DatasetUpdate(ctx context.Context, name string, params *DatasetUpdateParams) (*Dataset, error)
	DatasetList(ctx context.Context, parentName string, limit, offset int) ([]*Dataset, error)
	DatasetQueryByParent(ctx context.Context, parentDataset string) ([]*Dataset, error)
	DatasetSetUserProperty(ctx context.Context, name, key, value string) error
	DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error
	DatasetRemoveUserProperties(ctx context.Context, name string, keys []string) error
	DatasetGetUserProperty(ctx context.Context, name, key string) (string, error)
	DatasetExpand(ctx context.Context, name string, newSize int64) error
	DatasetExists(ctx context.Context, name string) (bool, error)
	DatasetHasDependentClones(ctx context.Context, datasetName string) (bool, error)
	SnapshotDependentClones(ctx context.Context, snapshotID string) ([]string, error)
	DatasetPromote(ctx context.Context, datasetName string) error
	DatasetGetQuotaUsage(ctx context.Context, datasetName string) (*DatasetQuotaUsage, error)
	// Encryption at rest (GF-Sprint 1). All are TrueNAS @jobs; a FAILED job is
	// returned as an error. DatasetUnlock additionally asserts on the job's RESULT
	// payload, because a WRONG PASSPHRASE IS A SUCCESSFUL JOB on 26.0 — the
	// failure lives only in {"failed": {...}} (live drill 2026-08-02, D-1). Its
	// error therefore carries backend text about a key operation and must be
	// scrubbed by anything that logs or surfaces it. DatasetLock is
	// test/drill-only — no live control path locks a dataset. DatasetUnlock is NOT
	// idempotent (P-8, drill-confirmed as a hard call error): gate on the
	// summary's locked==true before calling.
	DatasetLock(ctx context.Context, name string) error
	DatasetUnlock(ctx context.Context, name, passphrase string) error
	DatasetChangeKey(ctx context.Context, name, passphrase string) error
	DatasetEncryptionSummary(ctx context.Context, name string) ([]EncryptionSummaryEntry, error)
	GetPoolAvailable(ctx context.Context, poolName string) (int64, error)
	WaitForDatasetReady(ctx context.Context, name string, timeout time.Duration) (*Dataset, error)
	WaitForZvolReady(ctx context.Context, name string, timeout time.Duration) (*Dataset, error)

	// Snapshot methods
	SnapshotCreate(ctx context.Context, dataset, name string, userProperties map[string]string) (*Snapshot, error)
	SnapshotDelete(ctx context.Context, snapshotID string, defer_, recursive bool) error
	SnapshotRename(ctx context.Context, snapshotID, newName string) error
	SnapshotHold(ctx context.Context, snapshotID string) error
	SnapshotRelease(ctx context.Context, snapshotID string) error
	SnapshotIsHeld(ctx context.Context, snapshotID string) (bool, error)
	SnapshotGet(ctx context.Context, snapshotID string) (*Snapshot, error)
	SnapshotList(ctx context.Context, dataset string) ([]*Snapshot, error)
	SnapshotListAll(ctx context.Context, parentDataset string, limit, offset int) ([]*Snapshot, error)
	SnapshotFindByName(ctx context.Context, parentDataset, name string) (*Snapshot, error)
	SnapshotSetUserProperty(ctx context.Context, snapshotID, key, value string) error
	SnapshotRemoveUserProperties(ctx context.Context, snapshotID string, keys []string) error
	SnapshotClone(ctx context.Context, snapshotID, newDatasetName string) error
	CopyDatasetFromSnapshotLocal(ctx context.Context, sourceDataset, snapshotShortName, targetDataset string) (int64, error)
	DestroyReplicatedTargetSnapshot(ctx context.Context, targetDataset, snapshotShortName string) error
	SnapshotRollback(ctx context.Context, snapshotID string, force, recursive, recursiveClones bool) error
	ReplicationJobList(ctx context.Context) ([]*ReplicationJob, error)
	ReplicationJobAbort(ctx context.Context, jobID int64, reason string) error

	// Snapshot task methods (GF2/E2 driver-managed periodic snapshots)
	SnapshotTaskCreate(ctx context.Context, params *SnapshotTaskCreateParams) (*SnapshotTask, error)
	SnapshotTaskListByDataset(ctx context.Context, dataset string) ([]*SnapshotTask, error)
	SnapshotTaskListByParent(ctx context.Context, parentDataset string) ([]*SnapshotTask, error)
	SnapshotTaskUpdate(ctx context.Context, id int, params *SnapshotTaskCreateParams) error
	SnapshotTaskDelete(ctx context.Context, id int) error

	// NFS methods
	NFSShareCreate(ctx context.Context, params *NFSShareCreateParams) (*NFSShare, error)
	NFSShareDelete(ctx context.Context, id int) error
	NFSShareGet(ctx context.Context, id int) (*NFSShare, error)
	NFSShareFindByPath(ctx context.Context, path string) (*NFSShare, error)
	NFSShareList(ctx context.Context) ([]*NFSShare, error)
	NFSShareUpdate(ctx context.Context, id int, params map[string]interface{}) (*NFSShare, error)
	NFSServiceConfig(ctx context.Context) (*NFSServiceConfig, error)
	NFSServiceUpdate(ctx context.Context, params map[string]interface{}) (*NFSServiceConfig, error)

	// ZFS property choice / topology introspection (curated performance classes).
	ZFSPropertyChoices(ctx context.Context) (*ZFSPropertyChoices, error)
	RecommendedZvolBlocksize(ctx context.Context, pool string) (string, error)
	PoolHasSpecialVdev(ctx context.Context, pool string) (bool, error)

	// Backend health (read-only).
	PoolHealth(ctx context.Context, pool string) (*PoolHealthSnapshot, error)
	DiskTemperatureAlerts(ctx context.Context, names []string) ([]string, error)

	// Filesystem ACL methods (NFSv4 ACLs). FilesystemSetACL is a @job.
	FilesystemGetACL(ctx context.Context, path string) (*FilesystemACL, error)
	FilesystemSetACL(ctx context.Context, opts *SetACLOptions) error
	ACLTemplateDACL(ctx context.Context, name string) ([]ACLEntry, error)

	// Service methods
	ServiceReload(ctx context.Context, service string) error

	// System information methods
	GetSystemInfo(ctx context.Context) (*SystemInfo, error)
	// SystemTimezone returns the NAS's configured IANA civil timezone
	// (system.general.config -> timezone). Cached with a TTL and dropped on
	// reconnect, so callers never pay a per-operation round trip.
	SystemTimezone(ctx context.Context) (*time.Location, error)
	CheckNVMeoFSupport(ctx context.Context) error

	// iSCSI methods
	ISCSIPortalList(ctx context.Context) ([]*ISCSIPortal, error)
	ISCSIInitiatorList(ctx context.Context) ([]*ISCSIInitiator, error)
	ISCSIInitiatorCreate(ctx context.Context, comment string) (*ISCSIInitiator, error)
	ISCSIInitiatorCreateWithInitiators(ctx context.Context, initiators []string, comment string) (*ISCSIInitiator, error)
	ISCSIInitiatorGet(ctx context.Context, id int) (*ISCSIInitiator, error)
	ISCSIInitiatorUpdate(ctx context.Context, id int, initiators []string, comment string) (*ISCSIInitiator, error)
	ISCSIInitiatorDelete(ctx context.Context, id int) error
	ISCSITargetCreate(ctx context.Context, name, alias, mode string, groups []ISCSITargetGroup, opts ...ISCSITargetCreateOptions) (*ISCSITarget, error)
	ISCSITargetUpdate(ctx context.Context, id int, groups []ISCSITargetGroup) (*ISCSITarget, error)
	ISCSITargetDelete(ctx context.Context, id int, force bool) error
	ISCSITargetGet(ctx context.Context, id int) (*ISCSITarget, error)
	ISCSITargetFindByName(ctx context.Context, name string) (*ISCSITarget, error)
	ISCSITargetList(ctx context.Context) ([]*ISCSITarget, error)
	ISCSIExtentCreate(ctx context.Context, name, diskPath, comment string, blocksize int, physicalBlocksize bool, rpm string, opts ...ISCSIExtentCreateOptions) (*ISCSIExtent, error)
	ISCSIExtentDelete(ctx context.Context, id int, remove, force bool) error
	ISCSIExtentGet(ctx context.Context, id int) (*ISCSIExtent, error)
	ISCSIExtentFindByName(ctx context.Context, name string) (*ISCSIExtent, error)
	ISCSIExtentFindByDisk(ctx context.Context, diskPath string) (*ISCSIExtent, error)
	ISCSIExtentList(ctx context.Context) ([]*ISCSIExtent, error)
	ISCSITargetExtentCreate(ctx context.Context, targetID, extentID, lunID int) (*ISCSITargetExtent, error)
	ISCSITargetExtentDelete(ctx context.Context, id int, force bool) error
	ISCSITargetExtentGet(ctx context.Context, id int) (*ISCSITargetExtent, error)
	ISCSITargetExtentFind(ctx context.Context, targetID, extentID int) (*ISCSITargetExtent, error)
	ISCSITargetExtentFindByTarget(ctx context.Context, targetID int) ([]*ISCSITargetExtent, error)
	ISCSITargetExtentFindByExtent(ctx context.Context, extentID int) ([]*ISCSITargetExtent, error)
	ISCSIGlobalConfigGet(ctx context.Context) (*ISCSIGlobalConfig, error)
	ISCSIAuthCreate(ctx context.Context, tag int, user, secret, peerUser, peerSecret string) (*ISCSIAuth, error)
	ISCSIAuthQueryByTag(ctx context.Context, tag int) ([]*ISCSIAuth, error)
	ISCSIAuthGet(ctx context.Context, id int) (*ISCSIAuth, error)
	ISCSIAuthUpdate(ctx context.Context, id int, user, secret, peerUser, peerSecret string) (*ISCSIAuth, error)
	ISCSIAuthDelete(ctx context.Context, id int) error

	// NVMe-oF methods (updated for TrueNAS SCALE 25.10+)
	NVMeoFHostFindByNQN(ctx context.Context, nqn string) (*NVMeoFHost, error)
	NVMeoFHostCreate(ctx context.Context, nqn string) (*NVMeoFHost, error)
	NVMeoFHostSubsysCreate(ctx context.Context, hostID, subsysID int) (*NVMeoFHostSubsys, error)
	NVMeoFHostSubsysFind(ctx context.Context, hostID, subsysID int) (*NVMeoFHostSubsys, error)
	NVMeoFHostSubsysListBySubsystem(ctx context.Context, subsysID int) ([]*NVMeoFHostSubsys, error)
	NVMeoFHostSubsysDelete(ctx context.Context, id int) error
	NVMeoFSubsystemCreate(ctx context.Context, name string, allowAnyHost bool, hostIDs []int, opts ...NVMeoFSubsystemCreateOptions) (*NVMeoFSubsystem, error)
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
	NVMeoFNamespaceListBySubsystem(ctx context.Context, subsysID int) ([]*NVMeoFNamespace, error)
	NVMeoFNamespaceList(ctx context.Context) ([]*NVMeoFNamespace, error)
	NVMeoFPortList(ctx context.Context) ([]*NVMeoFPort, error)
	NVMeoFPortCreate(ctx context.Context, transport, address string, port int, opts ...NVMeoFPortCreateOptions) (*NVMeoFPort, error)
	NVMeoFPortFindByAddress(ctx context.Context, transport, address string, port int) (*NVMeoFPort, error)
	NVMeoFPortSubsysCreate(ctx context.Context, portID, subsysID int) (*NVMeoFPortSubsys, error)
	NVMeoFPortSubsysFindBySubsystem(ctx context.Context, subsysID int) (bool, error)
	NVMeoFPortSubsysList(ctx context.Context) ([]*NVMeoFPortSubsys, error)
	NVMeoFPortSubsysListBySubsystem(ctx context.Context, subsysID int) ([]*NVMeoFPortSubsys, error)
	NVMeoFPortSubsysDelete(ctx context.Context, id int) error
	NVMeoFSubsystemList(ctx context.Context) ([]*NVMeoFSubsystem, error)
	NVMeoFGetOrCreatePort(ctx context.Context, transport string, address string, port int, opts ...NVMeoFPortCreateOptions) (*NVMeoFPort, error)
	InvalidateNVMeoFPort(transport, address string, port int)
	NVMeoFGetTransportAddresses(ctx context.Context, transport string) ([]string, error)
}
