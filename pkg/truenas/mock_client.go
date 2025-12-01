package truenas

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MockClient is a mock implementation of ClientInterface for testing.
type MockClient struct {
	mu sync.RWMutex

	// Mock data
	Datasets       map[string]*Dataset
	Snapshots      map[string]*Snapshot
	NFSShares      map[int]*NFSShare
	ISCSITargets   map[int]*ISCSITarget
	ISCSIExtents   map[int]*ISCSIExtent
	TargetExtents  map[int]*ISCSITargetExtent
	NVMeSubsystems map[int]*NVMeoFSubsystem
	NVMeNamespaces map[int]*NVMeoFNamespace
	PoolAvailable  int64

	// Error injection
	InjectError error
}

// NewMockClient creates a new MockClient.
func NewMockClient() *MockClient {
	return &MockClient{
		Datasets:       make(map[string]*Dataset),
		Snapshots:      make(map[string]*Snapshot),
		NFSShares:      make(map[int]*NFSShare),
		ISCSITargets:   make(map[int]*ISCSITarget),
		ISCSIExtents:   make(map[int]*ISCSIExtent),
		TargetExtents:  make(map[int]*ISCSITargetExtent),
		NVMeSubsystems: make(map[int]*NVMeoFSubsystem),
		NVMeNamespaces: make(map[int]*NVMeoFNamespace),
		PoolAvailable:  100 * 1024 * 1024 * 1024, // 100 GiB default
	}
}

// Core methods
func (m *MockClient) Close() error      { return nil }
func (m *MockClient) IsConnected() bool { return true }
func (m *MockClient) Call(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	return nil, nil
}
func (m *MockClient) CallWithContext(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	return nil, nil
}

// Dataset methods
func (m *MockClient) DatasetCreate(ctx context.Context, params *DatasetCreateParams) (*Dataset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	if _, exists := m.Datasets[params.Name]; exists {
		// Simulate "already exists" behavior if needed, or return error
		// For now, let's just overwrite or return existing
		return m.Datasets[params.Name], nil
	}

	ds := &Dataset{
		ID:             params.Name,
		Name:           params.Name,
		Type:           params.Type,
		UserProperties: make(map[string]UserProperty),
		Volsize:        DatasetProperty{Parsed: float64(params.Volsize)},
		Refquota:       DatasetProperty{Parsed: float64(params.Refquota)},
	}
	m.Datasets[params.Name] = ds
	return ds, nil
}

func (m *MockClient) DatasetDelete(ctx context.Context, name string, recursive bool, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	delete(m.Datasets, name)
	return nil
}

func (m *MockClient) DatasetGet(ctx context.Context, name string) (*Dataset, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	if ds, ok := m.Datasets[name]; ok {
		return ds, nil
	}
	return nil, &APIError{Code: -1, Message: "dataset not found"}
}

func (m *MockClient) DatasetUpdate(ctx context.Context, name string, params *DatasetUpdateParams) (*Dataset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	ds, ok := m.Datasets[name]
	if !ok {
		return nil, &APIError{Code: -1, Message: "dataset not found"}
	}

	if params.Volsize > 0 {
		ds.Volsize = DatasetProperty{Parsed: float64(params.Volsize)}
	}
	// Handle other updates as needed
	return ds, nil
}

func (m *MockClient) DatasetList(ctx context.Context, parentName string, limit int, offset int) ([]*Dataset, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []*Dataset
	for _, ds := range m.Datasets {
		list = append(list, ds)
	}
	return list, nil
}

func (m *MockClient) DatasetSetUserProperty(ctx context.Context, name string, key string, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	ds, ok := m.Datasets[name]
	if !ok {
		return &APIError{Code: -1, Message: "dataset not found"}
	}
	ds.UserProperties[key] = UserProperty{Value: value}
	return nil
}

func (m *MockClient) DatasetGetUserProperty(ctx context.Context, name string, key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ds, ok := m.Datasets[name]
	if !ok {
		return "", &APIError{Code: -1, Message: "dataset not found"}
	}
	if prop, ok := ds.UserProperties[key]; ok {
		return prop.Value, nil
	}
	return "", nil
}

func (m *MockClient) DatasetExpand(ctx context.Context, name string, newSize int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	ds, ok := m.Datasets[name]
	if !ok {
		return &APIError{Code: -1, Message: "dataset not found"}
	}
	ds.Volsize = DatasetProperty{Parsed: float64(newSize)}
	return nil
}

func (m *MockClient) GetPoolAvailable(ctx context.Context, poolName string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.PoolAvailable, nil
}

func (m *MockClient) DatasetExists(ctx context.Context, name string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.Datasets[name]
	return ok, nil
}

func (m *MockClient) WaitForDatasetReady(ctx context.Context, name string, timeout time.Duration) (*Dataset, error) {
	return m.DatasetGet(ctx, name)
}

func (m *MockClient) WaitForZvolReady(ctx context.Context, name string, timeout time.Duration) (*Dataset, error) {
	return m.DatasetGet(ctx, name)
}

// Snapshot methods
func (m *MockClient) SnapshotCreate(ctx context.Context, dataset string, name string) (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	id := fmt.Sprintf("%s@%s", dataset, name)
	snap := &Snapshot{
		ID:             id,
		Name:           name,
		Dataset:        dataset,
		UserProperties: make(map[string]UserProperty),
	}
	m.Snapshots[id] = snap
	return snap, nil
}

func (m *MockClient) SnapshotDelete(ctx context.Context, snapshotID string, defer_ bool, recursive bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	delete(m.Snapshots, snapshotID)
	return nil
}

func (m *MockClient) SnapshotGet(ctx context.Context, snapshotID string) (*Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	if snap, ok := m.Snapshots[snapshotID]; ok {
		return snap, nil
	}
	return nil, &APIError{Code: -1, Message: "snapshot not found"}
}

func (m *MockClient) SnapshotList(ctx context.Context, dataset string) ([]*Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []*Snapshot
	for _, snap := range m.Snapshots {
		if snap.Dataset == dataset {
			list = append(list, snap)
		}
	}
	return list, nil
}

func (m *MockClient) SnapshotListAll(ctx context.Context, parentDataset string, limit int, offset int) ([]*Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []*Snapshot
	for _, snap := range m.Snapshots {
		list = append(list, snap)
	}
	return list, nil
}

func (m *MockClient) SnapshotFindByName(ctx context.Context, parentDataset string, name string) (*Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	for _, snap := range m.Snapshots {
		if snap.Name == name && snap.Dataset == parentDataset {
			return snap, nil
		}
	}
	return nil, nil // Not found, not an error
}

func (m *MockClient) SnapshotSetUserProperty(ctx context.Context, snapshotID string, key string, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	snap, ok := m.Snapshots[snapshotID]
	if !ok {
		return &APIError{Code: -1, Message: "snapshot not found"}
	}
	snap.UserProperties[key] = UserProperty{Value: value}
	return nil
}

func (m *MockClient) SnapshotClone(ctx context.Context, snapshotID string, newDatasetName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	// Create a new dataset as a clone
	m.Datasets[newDatasetName] = &Dataset{
		ID:             newDatasetName,
		Name:           newDatasetName,
		UserProperties: make(map[string]UserProperty),
	}
	return nil
}

func (m *MockClient) SnapshotRollback(ctx context.Context, snapshotID string, force bool, recursive bool, recursiveClones bool) error {
	return nil
}

// NFS methods
func (m *MockClient) NFSShareCreate(ctx context.Context, params *NFSShareCreateParams) (*NFSShare, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	id := len(m.NFSShares) + 1
	share := &NFSShare{
		ID:   id,
		Path: params.Path,
	}
	m.NFSShares[id] = share
	return share, nil
}

func (m *MockClient) NFSShareDelete(ctx context.Context, id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.NFSShares, id)
	return nil
}

func (m *MockClient) NFSShareGet(ctx context.Context, id int) (*NFSShare, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if share, ok := m.NFSShares[id]; ok {
		return share, nil
	}
	return nil, fmt.Errorf("share not found")
}

func (m *MockClient) NFSShareFindByPath(ctx context.Context, path string) (*NFSShare, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, share := range m.NFSShares {
		if share.Path == path {
			return share, nil
		}
	}
	return nil, nil
}

func (m *MockClient) NFSShareList(ctx context.Context) ([]*NFSShare, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []*NFSShare
	for _, share := range m.NFSShares {
		list = append(list, share)
	}
	return list, nil
}

func (m *MockClient) NFSShareUpdate(ctx context.Context, id int, params map[string]interface{}) (*NFSShare, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.NFSShares[id], nil
}

// Service methods
func (m *MockClient) ServiceReload(ctx context.Context, service string) error {
	return nil
}

// iSCSI methods
func (m *MockClient) ISCSITargetCreate(ctx context.Context, name string, alias string, mode string, groups []ISCSITargetGroup) (*ISCSITarget, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := len(m.ISCSITargets) + 1
	target := &ISCSITarget{ID: id, Name: name, Alias: alias, Mode: mode, Groups: groups}
	m.ISCSITargets[id] = target
	return target, nil
}
func (m *MockClient) ISCSITargetDelete(ctx context.Context, id int, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.ISCSITargets, id)
	return nil
}
func (m *MockClient) ISCSITargetGet(ctx context.Context, id int) (*ISCSITarget, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if t, ok := m.ISCSITargets[id]; ok {
		return t, nil
	}
	return nil, fmt.Errorf("not found")
}
func (m *MockClient) ISCSITargetFindByName(ctx context.Context, name string) (*ISCSITarget, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, t := range m.ISCSITargets {
		if t.Name == name {
			return t, nil
		}
	}
	return nil, nil
}
func (m *MockClient) ISCSIExtentCreate(ctx context.Context, name string, diskPath string, comment string, blocksize int, rpm string) (*ISCSIExtent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := len(m.ISCSIExtents) + 1
	ext := &ISCSIExtent{ID: id, Name: name, Disk: diskPath}
	m.ISCSIExtents[id] = ext
	return ext, nil
}
func (m *MockClient) ISCSIExtentDelete(ctx context.Context, id int, remove bool, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.ISCSIExtents, id)
	return nil
}
func (m *MockClient) ISCSIExtentGet(ctx context.Context, id int) (*ISCSIExtent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if e, ok := m.ISCSIExtents[id]; ok {
		return e, nil
	}
	return nil, fmt.Errorf("not found")
}
func (m *MockClient) ISCSIExtentFindByName(ctx context.Context, name string) (*ISCSIExtent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, e := range m.ISCSIExtents {
		if e.Name == name {
			return e, nil
		}
	}
	return nil, nil
}
func (m *MockClient) ISCSIExtentFindByDisk(ctx context.Context, diskPath string) (*ISCSIExtent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, e := range m.ISCSIExtents {
		if e.Disk == diskPath {
			return e, nil
		}
	}
	return nil, nil
}
func (m *MockClient) ISCSITargetExtentCreate(ctx context.Context, targetID int, extentID int, lunID int) (*ISCSITargetExtent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := len(m.TargetExtents) + 1
	te := &ISCSITargetExtent{ID: id, Target: targetID, Extent: extentID, LunID: lunID}
	m.TargetExtents[id] = te
	return te, nil
}
func (m *MockClient) ISCSITargetExtentDelete(ctx context.Context, id int, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.TargetExtents, id)
	return nil
}
func (m *MockClient) ISCSITargetExtentFind(ctx context.Context, targetID int, extentID int) (*ISCSITargetExtent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, te := range m.TargetExtents {
		if te.Target == targetID && te.Extent == extentID {
			return te, nil
		}
	}
	return nil, nil
}
func (m *MockClient) ISCSITargetExtentFindByTarget(ctx context.Context, targetID int) ([]*ISCSITargetExtent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*ISCSITargetExtent
	for _, te := range m.TargetExtents {
		if te.Target == targetID {
			results = append(results, te)
		}
	}
	return results, nil
}
func (m *MockClient) ISCSITargetExtentFindByExtent(ctx context.Context, extentID int) ([]*ISCSITargetExtent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*ISCSITargetExtent
	for _, te := range m.TargetExtents {
		if te.Extent == extentID {
			results = append(results, te)
		}
	}
	return results, nil
}
func (m *MockClient) ISCSIGlobalConfigGet(ctx context.Context) (*ISCSIGlobalConfig, error) {
	return &ISCSIGlobalConfig{Basename: "iqn.2005-10.org.freenas.ctl"}, nil
}

// NVMe-oF methods
func (m *MockClient) NVMeoFSubsystemCreate(ctx context.Context, nqn string, serial string, allowAnyHost bool, hosts []string) (*NVMeoFSubsystem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := len(m.NVMeSubsystems) + 1
	sub := &NVMeoFSubsystem{ID: id, NQN: nqn}
	m.NVMeSubsystems[id] = sub
	return sub, nil
}
func (m *MockClient) NVMeoFSubsystemDelete(ctx context.Context, id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.NVMeSubsystems, id)
	return nil
}
func (m *MockClient) NVMeoFSubsystemGet(ctx context.Context, id int) (*NVMeoFSubsystem, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if s, ok := m.NVMeSubsystems[id]; ok {
		return s, nil
	}
	return nil, fmt.Errorf("not found")
}
func (m *MockClient) NVMeoFSubsystemFindByNQN(ctx context.Context, nqn string) (*NVMeoFSubsystem, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.NVMeSubsystems {
		if s.NQN == nqn {
			return s, nil
		}
	}
	return nil, nil
}
func (m *MockClient) NVMeoFNamespaceCreate(ctx context.Context, subsystemID int, devicePath string) (*NVMeoFNamespace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := len(m.NVMeNamespaces) + 1
	ns := &NVMeoFNamespace{ID: id, Subsystem: subsystemID, DevicePath: devicePath}
	m.NVMeNamespaces[id] = ns
	return ns, nil
}
func (m *MockClient) NVMeoFNamespaceDelete(ctx context.Context, id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.NVMeNamespaces, id)
	return nil
}
func (m *MockClient) NVMeoFNamespaceGet(ctx context.Context, id int) (*NVMeoFNamespace, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if n, ok := m.NVMeNamespaces[id]; ok {
		return n, nil
	}
	return nil, fmt.Errorf("not found")
}
func (m *MockClient) NVMeoFNamespaceFindByDevice(ctx context.Context, subsystemID int, devicePath string) (*NVMeoFNamespace, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, n := range m.NVMeNamespaces {
		if n.Subsystem == subsystemID && n.DevicePath == devicePath {
			return n, nil
		}
	}
	return nil, nil
}
func (m *MockClient) NVMeoFPortList(ctx context.Context) ([]*NVMeoFPort, error) {
	return []*NVMeoFPort{{ID: 1, Transport: "tcp", Address: "0.0.0.0", Port: 4420}}, nil
}
func (m *MockClient) NVMeoFGetTransportAddresses(ctx context.Context, transport string) ([]string, error) {
	return []string{"0.0.0.0"}, nil
}
