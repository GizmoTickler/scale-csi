package truenas

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// MockClient is a mock implementation of ClientInterface for testing.
type MockClient struct {
	mu sync.RWMutex

	// Mock data
	Datasets           map[string]*Dataset
	Snapshots          map[string]*Snapshot
	NFSShares          map[int]*NFSShare
	ISCSITargets       map[int]*ISCSITarget
	ISCSIExtents       map[int]*ISCSIExtent
	TargetExtents      map[int]*ISCSITargetExtent
	NVMeHosts          map[string]*NVMeoFHost
	NVMeSubsystems     map[int]*NVMeoFSubsystem
	NVMeNamespaces     map[int]*NVMeoFNamespace
	ISCSIPortals       map[int]*ISCSIPortal
	ISCSIInitiators    map[int]*ISCSIInitiator
	PoolAvailable      int64
	deferredSnapshots  map[string]struct{}
	DatasetDeleteCalls []DatasetDeleteCall

	// Error injection
	InjectError error
	// SimulateUpdateNoOp models TrueNAS 26.0 pool.snapshot.update returning
	// success without applying user-property additions or removals.
	SimulateUpdateNoOp bool
}

// DatasetDeleteCall records the deletion mode requested by a test.
type DatasetDeleteCall struct {
	Name      string
	Recursive bool
	Force     bool
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
		NVMeHosts:      make(map[string]*NVMeoFHost),
		NVMeSubsystems: make(map[int]*NVMeoFSubsystem),
		NVMeNamespaces: make(map[int]*NVMeoFNamespace),
		// Default portal/initiator fixtures cover the portal addresses used
		// across the test suites so target-group auto-resolution succeeds
		// without per-test setup. Tests may replace these maps.
		ISCSIPortals: map[int]*ISCSIPortal{
			1: {ID: 1, Tag: 1, Listen: []ISCSIPortalListen{
				{IP: "192.0.2.10", Port: 3260},
				{IP: "192.168.1.100", Port: 3260},
				{IP: "127.0.0.1", Port: 3260},
			}},
		},
		ISCSIInitiators: map[int]*ISCSIInitiator{
			1: {ID: 1, Initiators: nil, Comment: "allow-all (mock)"},
		},
		deferredSnapshots: make(map[string]struct{}),
		PoolAvailable:     100 * 1024 * 1024 * 1024, // 100 GiB default
	}
}

// ISCSIPortalList lists mock iSCSI portals.
func (m *MockClient) ISCSIPortalList(ctx context.Context) ([]*ISCSIPortal, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	portals := make([]*ISCSIPortal, 0, len(m.ISCSIPortals))
	for _, p := range m.ISCSIPortals {
		portals = append(portals, p)
	}
	return portals, nil
}

// ISCSIInitiatorList lists mock iSCSI initiator groups.
func (m *MockClient) ISCSIInitiatorList(ctx context.Context) ([]*ISCSIInitiator, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	groups := make([]*ISCSIInitiator, 0, len(m.ISCSIInitiators))
	for _, g := range m.ISCSIInitiators {
		groups = append(groups, g)
	}
	return groups, nil
}

// ISCSIInitiatorCreate creates a mock allow-all initiator group.
func (m *MockClient) ISCSIInitiatorCreate(ctx context.Context, comment string) (*ISCSIInitiator, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	id := len(m.ISCSIInitiators) + 1
	for m.ISCSIInitiators[id] != nil {
		id++
	}
	group := &ISCSIInitiator{ID: id, Comment: comment}
	m.ISCSIInitiators[id] = group
	return group, nil
}

// Core methods
func (m *MockClient) Close() error      { return nil }
func (m *MockClient) IsConnected() bool { return true }
func (m *MockClient) ActiveConnectionCount() int {
	return 1
}
func (m *MockClient) Call(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	return nil, nil
}
func (m *MockClient) CallWithContext(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	return nil, nil
}

// Circuit breaker methods (return nil/no-op for mock)
func (m *MockClient) CircuitBreakerStats() *CircuitBreakerStats { return nil }
func (m *MockClient) ResetCircuitBreaker()                      {}

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
		Refreservation: DatasetProperty{Parsed: float64(params.Refreservation)},
		Volblocksize:   DatasetProperty{Parsed: params.Volblocksize},
	}
	for _, property := range params.UserProperties {
		ds.UserProperties[property.Key] = UserProperty{Value: property.Value, Source: "local"}
	}
	if params.Type != "VOLUME" {
		ds.Mountpoint = "/mnt/" + strings.TrimPrefix(params.Name, "/")
	}
	m.Datasets[params.Name] = ds
	return ds, nil
}

func (m *MockClient) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DatasetDeleteCalls = append(m.DatasetDeleteCalls, DatasetDeleteCall{
		Name:      name,
		Recursive: recursive,
		Force:     force,
	})

	if m.InjectError != nil {
		return m.InjectError
	}
	originPrefix := name + "@"
	for _, dataset := range m.Datasets {
		if strings.HasPrefix(datasetPropertyString(dataset.Origin), originPrefix) {
			return &APIError{Code: -1, Message: "dataset has dependent clones"}
		}
	}
	if !recursive {
		for _, snapshot := range m.Snapshots {
			if snapshot.Dataset == name {
				return &APIError{Code: -1, Message: "dataset has snapshots"}
			}
		}
	}
	origin := ""
	if dataset, ok := m.Datasets[name]; ok {
		origin = datasetPropertyString(dataset.Origin)
	}
	delete(m.Datasets, name)
	if recursive {
		for snapshotID, snapshot := range m.Snapshots {
			if snapshot.Dataset == name {
				delete(m.deferredSnapshots, snapshotID)
				delete(m.Snapshots, snapshotID)
			}
		}
	}
	if origin != "" {
		m.reclaimDeferredSnapshotLocked(origin)
	}
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
	if params.Refquota != nil {
		switch refquota := params.Refquota.(type) {
		case int:
			ds.Refquota = DatasetProperty{Parsed: float64(refquota)}
		case int64:
			ds.Refquota = DatasetProperty{Parsed: float64(refquota)}
		case float64:
			ds.Refquota = DatasetProperty{Parsed: refquota}
		}
	}
	if params.Refreservation != nil {
		switch refreservation := params.Refreservation.(type) {
		case int:
			ds.Refreservation = DatasetProperty{Parsed: float64(refreservation)}
		case int64:
			ds.Refreservation = DatasetProperty{Parsed: float64(refreservation)}
		case float64:
			ds.Refreservation = DatasetProperty{Parsed: refreservation}
		}
	}
	for _, update := range params.UserPropertiesUpdate {
		if update.Remove {
			delete(ds.UserProperties, update.Key)
			continue
		}
		ds.UserProperties[update.Key] = UserProperty{Value: update.Value}
	}
	return ds, nil
}

func (m *MockClient) DatasetList(ctx context.Context, parentName string, limit, offset int) ([]*Dataset, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []*Dataset
	for _, ds := range m.Datasets {
		if parentName != "" && !strings.HasPrefix(ds.Name, parentName+"/") {
			continue
		}
		if prop, ok := ds.UserProperties[datasetManagedResourceProperty]; !ok || prop.Value != "true" {
			continue
		}
		list = append(list, ds)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	if offset >= len(list) {
		return []*Dataset{}, nil
	}
	end := len(list)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	return list[offset:end], nil
}

func (m *MockClient) DatasetHasDependentClones(ctx context.Context, datasetName string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectError != nil {
		return false, m.InjectError
	}
	originPrefix := datasetName + "@"
	for _, dataset := range m.Datasets {
		if strings.HasPrefix(datasetPropertyString(dataset.Origin), originPrefix) {
			return true, nil
		}
	}
	return false, nil
}

func (m *MockClient) DatasetSetUserProperty(ctx context.Context, name, key, value string) error {
	return m.DatasetSetUserProperties(ctx, name, map[string]string{key: value})
}

func (m *MockClient) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	ds, ok := m.Datasets[name]
	if !ok {
		return &APIError{Code: -1, Message: "dataset not found"}
	}
	for key, value := range properties {
		ds.UserProperties[key] = UserProperty{Value: value}
	}
	return nil
}

func (m *MockClient) DatasetGetUserProperty(ctx context.Context, name, key string) (string, error) {
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
func (m *MockClient) SnapshotCreate(ctx context.Context, dataset, name string, userProperties map[string]string) (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	id := fmt.Sprintf("%s@%s", dataset, name)
	snap := &Snapshot{
		ID:      id,
		Name:    name,
		Dataset: dataset,
		Properties: map[string]interface{}{
			"creation": map[string]interface{}{"parsed": float64(time.Now().Unix())},
		},
		UserProperties: make(map[string]UserProperty, len(userProperties)),
	}
	for key, value := range userProperties {
		snap.UserProperties[key] = UserProperty{Value: value, Source: "local"}
	}
	m.Snapshots[id] = snap
	return snap, nil
}

// SetSnapshotUsedBytes is a test helper to set the "used" property on a snapshot.
// This simulates the size that GetSnapshotSize() would return.
func (m *MockClient) SetSnapshotUsedBytes(snapshotID string, usedBytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if snap, ok := m.Snapshots[snapshotID]; ok {
		if snap.Properties == nil {
			snap.Properties = make(map[string]interface{})
		}
		snap.Properties["used"] = map[string]interface{}{
			"parsed": float64(usedBytes),
		}
	}
}

func (m *MockClient) SnapshotDelete(ctx context.Context, snapshotID string, defer_, recursive bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	if _, ok := m.Snapshots[snapshotID]; !ok {
		return nil
	}
	clones := m.snapshotClonesLocked(snapshotID)
	if len(clones) > 0 {
		if !defer_ {
			return &ErrSnapshotHasClones{SnapshotID: snapshotID, Clones: clones}
		}
		m.deferredSnapshots[snapshotID] = struct{}{}
		return nil
	}
	delete(m.deferredSnapshots, snapshotID)
	delete(m.Snapshots, snapshotID)
	return nil
}

func (m *MockClient) SnapshotRename(ctx context.Context, snapshotID, newName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	if m.SimulateUpdateNoOp {
		return nil
	}
	snap, ok := m.Snapshots[snapshotID]
	if !ok {
		return &APIError{Code: -1, Message: "snapshot not found"}
	}
	dataset, _, ok := strings.Cut(snapshotID, "@")
	if !ok || newName == "" {
		return &APIError{Code: -32602, Message: "invalid snapshot rename"}
	}
	newSnapshotID := dataset + "@" + newName
	if _, exists := m.Snapshots[newSnapshotID]; exists {
		return &APIError{Code: -1, Message: "snapshot already exists"}
	}

	delete(m.Snapshots, snapshotID)
	snap.ID = newSnapshotID
	snap.Name = newName
	m.Snapshots[newSnapshotID] = snap
	if _, deferred := m.deferredSnapshots[snapshotID]; deferred {
		delete(m.deferredSnapshots, snapshotID)
		m.deferredSnapshots[newSnapshotID] = struct{}{}
	}
	for _, dataset := range m.Datasets {
		if datasetPropertyString(dataset.Origin) == snapshotID {
			dataset.Origin = DatasetProperty{Value: newSnapshotID, Parsed: newSnapshotID, Rawvalue: newSnapshotID, Source: "LOCAL"}
		}
	}
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

func (m *MockClient) SnapshotListAll(ctx context.Context, parentDataset string, limit, offset int) ([]*Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	parentDataset = strings.TrimSuffix(parentDataset, "/")
	prefix := parentDataset + "/"
	var list []*Snapshot
	for _, snap := range m.Snapshots {
		if strings.HasPrefix(snap.Dataset, prefix) {
			list = append(list, snap)
		}
	}
	sort.SliceStable(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return paginateSnapshots(list, limit, offset), nil
}

func (m *MockClient) SnapshotFindByName(ctx context.Context, parentDataset, name string) (*Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	for _, snap := range m.Snapshots {
		if snap.Name == name && (snap.Dataset == parentDataset || strings.HasPrefix(snap.Dataset, parentDataset+"/")) {
			return snap, nil
		}
	}
	return nil, nil // Not found, not an error
}

func (m *MockClient) SnapshotSetUserProperty(ctx context.Context, snapshotID, key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	if m.SimulateUpdateNoOp {
		return nil
	}
	snap, ok := m.Snapshots[snapshotID]
	if !ok {
		return &APIError{Code: -1, Message: "snapshot not found"}
	}
	snap.UserProperties[key] = UserProperty{Value: value}
	return nil
}

func (m *MockClient) SnapshotRemoveUserProperties(ctx context.Context, snapshotID string, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	if m.SimulateUpdateNoOp {
		return nil
	}
	snap, ok := m.Snapshots[snapshotID]
	if !ok {
		return &APIError{Code: -1, Message: "snapshot not found"}
	}
	for _, key := range keys {
		delete(snap.UserProperties, key)
	}
	return nil
}

func (m *MockClient) snapshotClonesLocked(snapshotID string) []string {
	var clones []string
	for name, dataset := range m.Datasets {
		if datasetPropertyString(dataset.Origin) == snapshotID {
			clones = append(clones, name)
		}
	}
	sort.Strings(clones)
	return clones
}

func (m *MockClient) reclaimDeferredSnapshotLocked(snapshotID string) {
	if _, deferred := m.deferredSnapshots[snapshotID]; !deferred {
		return
	}
	if len(m.snapshotClonesLocked(snapshotID)) != 0 {
		return
	}
	delete(m.deferredSnapshots, snapshotID)
	delete(m.Snapshots, snapshotID)
}

func (m *MockClient) SnapshotClone(ctx context.Context, snapshotID, newDatasetName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	// Create a new dataset as a clone, preserving the source dataset's type and
	// capacity properties so controller tests observe realistic clone state.
	clone := &Dataset{
		ID:             newDatasetName,
		Name:           newDatasetName,
		UserProperties: make(map[string]UserProperty),
	}
	if snapshot, ok := m.Snapshots[snapshotID]; ok {
		clone.Origin = DatasetProperty{Value: snapshotID, Parsed: snapshotID, Rawvalue: snapshotID, Source: "LOCAL"}
		if source, ok := m.Datasets[snapshot.Dataset]; ok {
			clone.Type = source.Type
			clone.Mountpoint = source.Mountpoint
			clone.Volsize = source.Volsize
			clone.Refquota = source.Refquota
			clone.Available = source.Available
		}
	}
	m.Datasets[newDatasetName] = clone
	return nil
}

func (m *MockClient) CopyDatasetFromSnapshotLocal(ctx context.Context, sourceDataset, snapshotShortName, targetDataset string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	if _, exists := m.Datasets[targetDataset]; exists {
		return nil
	}
	snapshotID := sourceDataset + "@" + snapshotShortName
	if _, exists := m.Snapshots[snapshotID]; !exists {
		return &APIError{Code: -1, Message: "snapshot not found"}
	}
	source, exists := m.Datasets[sourceDataset]
	if !exists {
		return &APIError{Code: -1, Message: "source dataset not found"}
	}

	properties := make(map[string]UserProperty, len(source.UserProperties))
	for key, value := range source.UserProperties {
		properties[key] = value
	}
	copy := &Dataset{
		ID:             targetDataset,
		Name:           targetDataset,
		Pool:           source.Pool,
		Type:           source.Type,
		Mountpoint:     source.Mountpoint,
		Used:           source.Used,
		Available:      source.Available,
		Quota:          source.Quota,
		Refquota:       source.Refquota,
		Reservation:    source.Reservation,
		Refreservation: source.Refreservation,
		Volsize:        source.Volsize,
		Volblocksize:   source.Volblocksize,
		UserProperties: properties,
	}
	if copy.Type != "VOLUME" {
		copy.Mountpoint = "/mnt/" + strings.TrimPrefix(targetDataset, "/")
	}
	m.Datasets[targetDataset] = copy
	return nil
}

func (m *MockClient) DestroyReplicatedTargetSnapshot(ctx context.Context, targetDataset, snapshotShortName string) error {
	err := m.SnapshotDelete(ctx, targetDataset+"@"+snapshotShortName, false, false)
	if IsNotFoundError(err) {
		return nil
	}
	return err
}

func (m *MockClient) SnapshotRollback(ctx context.Context, snapshotID string, force, recursive, recursiveClones bool) error {
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

// System information methods
func (m *MockClient) GetSystemInfo(ctx context.Context) (*SystemInfo, error) {
	// Return a mock TrueNAS SCALE 25.10+ version for testing
	return &SystemInfo{
		Version:      "TrueNAS-SCALE-25.10.0",
		VersionMajor: 25,
		VersionMinor: 10,
		VersionPatch: 0,
		Hostname:     "truenas-mock",
	}, nil
}

func (m *MockClient) CheckNVMeoFSupport(ctx context.Context) error {
	// Mock always supports NVMe-oF (returns 25.10+)
	return nil
}

// iSCSI methods
func (m *MockClient) ISCSITargetCreate(ctx context.Context, name, alias, mode string, groups []ISCSITargetGroup) (*ISCSITarget, error) {
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
func (m *MockClient) ISCSIExtentCreate(ctx context.Context, name, diskPath, comment string, blocksize int, physicalBlocksize bool, rpm string) (*ISCSIExtent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := len(m.ISCSIExtents) + 1
	ext := &ISCSIExtent{ID: id, Name: name, Disk: diskPath}
	m.ISCSIExtents[id] = ext
	return ext, nil
}
func (m *MockClient) ISCSIExtentDelete(ctx context.Context, id int, remove, force bool) error {
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
func (m *MockClient) ISCSITargetExtentCreate(ctx context.Context, targetID, extentID, lunID int) (*ISCSITargetExtent, error) {
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

func (m *MockClient) ISCSITargetExtentGet(ctx context.Context, id int) (*ISCSITargetExtent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if te, ok := m.TargetExtents[id]; ok {
		return te, nil
	}
	return nil, nil
}

func (m *MockClient) ISCSITargetExtentFind(ctx context.Context, targetID, extentID int) (*ISCSITargetExtent, error) {
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

// NVMe-oF methods (updated for TrueNAS SCALE 25.10+)
func (m *MockClient) NVMeoFHostFindByNQN(ctx context.Context, nqn string) (*NVMeoFHost, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	return m.NVMeHosts[nqn], nil
}

func (m *MockClient) NVMeoFHostCreate(ctx context.Context, nqn string) (*NVMeoFHost, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	if existing := m.NVMeHosts[nqn]; existing != nil {
		return existing, nil
	}
	id := 1
	for _, host := range m.NVMeHosts {
		if host.ID >= id {
			id = host.ID + 1
		}
	}
	host := &NVMeoFHost{ID: id, HostNQN: nqn}
	m.NVMeHosts[nqn] = host
	return host, nil
}

func (m *MockClient) NVMeoFHostSubsysCreate(ctx context.Context, hostID, subsysID int) (*NVMeoFHostSubsys, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	subsys := m.NVMeSubsystems[subsysID]
	if subsys == nil {
		return nil, fmt.Errorf("NVMe-oF subsystem ID %d not found", subsysID)
	}
	hostFound := false
	for _, host := range m.NVMeHosts {
		if host.ID == hostID {
			hostFound = true
			break
		}
	}
	if !hostFound {
		return nil, fmt.Errorf("NVMe-oF host ID %d not found", hostID)
	}
	for _, existingHostID := range subsys.Hosts {
		if existingHostID == hostID {
			return &NVMeoFHostSubsys{ID: hostID, HostID: hostID, SubsysID: subsysID}, nil
		}
	}
	subsys.Hosts = append(subsys.Hosts, hostID)
	return &NVMeoFHostSubsys{ID: hostID, HostID: hostID, SubsysID: subsysID}, nil
}

func (m *MockClient) NVMeoFHostSubsysFind(ctx context.Context, hostID, subsysID int) (*NVMeoFHostSubsys, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	if subsys := m.NVMeSubsystems[subsysID]; subsys != nil {
		for _, existingHostID := range subsys.Hosts {
			if existingHostID == hostID {
				return &NVMeoFHostSubsys{ID: hostID, HostID: hostID, SubsysID: subsysID}, nil
			}
		}
	}
	return nil, nil
}

func (m *MockClient) NVMeoFSubsystemCreate(ctx context.Context, name string, allowAnyHost bool, hostIDs []int) (*NVMeoFSubsystem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	if !allowAnyHost {
		if len(hostIDs) == 0 {
			return nil, fmt.Errorf("restricted NVMe-oF subsystem requires at least one host ID")
		}
		for _, hostID := range hostIDs {
			found := false
			for _, host := range m.NVMeHosts {
				if host.ID == hostID {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("NVMe-oF host ID %d not found", hostID)
			}
		}
	}

	id := len(m.NVMeSubsystems) + 1
	hosts := append([]int(nil), hostIDs...)
	sub := &NVMeoFSubsystem{
		ID:           id,
		Name:         name,
		NQN:          fmt.Sprintf("nqn.2011-06.com.truenas:%s", name), // Mock auto-generated NQN
		AllowAnyHost: allowAnyHost,
		Hosts:        hosts,
	}
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
func (m *MockClient) NVMeoFSubsystemFindByName(ctx context.Context, name string) (*NVMeoFSubsystem, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.NVMeSubsystems {
		if s.Name == name {
			return s, nil
		}
	}
	return nil, nil
}
func (m *MockClient) NVMeoFNamespaceCreate(ctx context.Context, subsystemID int, devicePath, deviceType string) (*NVMeoFNamespace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := len(m.NVMeNamespaces) + 1
	ns := &NVMeoFNamespace{
		ID:          id,
		SubsystemID: subsystemID,
		DevicePath:  devicePath,
		DeviceType:  deviceType,
		Enabled:     true,
	}
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
		if n.SubsystemID == subsystemID && n.DevicePath == devicePath {
			return n, nil
		}
	}
	return nil, nil
}
func (m *MockClient) NVMeoFNamespaceFindByDevicePath(ctx context.Context, devicePath string) (*NVMeoFNamespace, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, n := range m.NVMeNamespaces {
		if n.DevicePath == devicePath {
			return n, nil
		}
	}
	return nil, nil
}
func (m *MockClient) NVMeoFPortList(ctx context.Context) ([]*NVMeoFPort, error) {
	return []*NVMeoFPort{{ID: 1, Transport: "TCP", Address: "0.0.0.0", Port: 4420}}, nil
}
func (m *MockClient) NVMeoFPortCreate(ctx context.Context, transport, address string, port int) (*NVMeoFPort, error) {
	return &NVMeoFPort{ID: 1, Transport: "TCP", Address: address, Port: port}, nil
}
func (m *MockClient) NVMeoFPortFindByAddress(ctx context.Context, transport, address string, port int) (*NVMeoFPort, error) {
	return &NVMeoFPort{ID: 1, Transport: "TCP", Address: address, Port: port}, nil
}
func (m *MockClient) NVMeoFPortSubsysCreate(ctx context.Context, portID, subsysID int) (*NVMeoFPortSubsys, error) {
	return &NVMeoFPortSubsys{ID: 1, PortID: portID, SubsysID: subsysID}, nil
}
func (m *MockClient) NVMeoFPortSubsysFindBySubsystem(ctx context.Context, subsysID int) (bool, error) {
	return true, nil
}
func (m *MockClient) NVMeoFPortSubsysList(ctx context.Context) ([]*NVMeoFPortSubsys, error) {
	return nil, nil
}
func (m *MockClient) NVMeoFPortSubsysListBySubsystem(ctx context.Context, subsysID int) ([]*NVMeoFPortSubsys, error) {
	// Return empty list for mock
	return nil, nil
}
func (m *MockClient) NVMeoFPortSubsysDelete(ctx context.Context, id int) error {
	return nil
}
func (m *MockClient) NVMeoFSubsystemList(ctx context.Context) ([]*NVMeoFSubsystem, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []*NVMeoFSubsystem
	for _, s := range m.NVMeSubsystems {
		list = append(list, s)
	}
	return list, nil
}
func (m *MockClient) NVMeoFGetOrCreatePort(ctx context.Context, transport, address string, port int) (*NVMeoFPort, error) {
	return &NVMeoFPort{ID: 1, Transport: "TCP", Address: address, Port: port}, nil
}
func (m *MockClient) InvalidateNVMeoFPort(transport, address string, port int) {}
func (m *MockClient) NVMeoFGetTransportAddresses(ctx context.Context, transport string) ([]string, error) {
	return []string{"0.0.0.0"}, nil
}
