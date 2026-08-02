package truenas

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// notFoundAPIError builds the not-found error shape TrueNAS 26.0 emits: a
// negative JSON-RPC code plus an AUTHORITATIVE ENOENT errno carried in Data. The
// human-readable message is preserved. Before this, the mock returned a bare
// message-only APIError, so IsNotFoundError tests only ever exercised the
// substring fallback and never the errno path production actually emits.
func notFoundAPIError(message string) *APIError {
	return &APIError{Code: -1, Message: message, Data: map[string]interface{}{"errno": "ENOENT"}}
}

// MockClient is a mock implementation of ClientInterface for testing.
type MockClient struct {
	mu                     sync.RWMutex
	setUserPropertiesCalls int

	// Mock data
	Datasets                   map[string]*Dataset
	Snapshots                  map[string]*Snapshot
	NFSShares                  map[int]*NFSShare
	ISCSITargets               map[int]*ISCSITarget
	ISCSIExtents               map[int]*ISCSIExtent
	TargetExtents              map[int]*ISCSITargetExtent
	NVMeHosts                  map[string]*NVMeoFHost
	NVMeHostSubsystems         map[int]*NVMeoFHostSubsys
	NVMeSubsystems             map[int]*NVMeoFSubsystem
	NVMeNamespaces             map[int]*NVMeoFNamespace
	ISCSIPortals               map[int]*ISCSIPortal
	ISCSIInitiators            map[int]*ISCSIInitiator
	ISCSIAuths                 map[int]*ISCSIAuth
	PoolAvailable              int64
	ReplicationJobs            map[int64]*ReplicationJob
	SnapshotTasks              map[int]*SnapshotTask
	SnapshotTaskDeleteCalls    []int
	nextSnapshotTaskID         int
	deferredSnapshots          map[string]struct{}
	DatasetDeleteCalls         []DatasetDeleteCall
	DatasetPromoteCalls        []string
	ReplicationJobAbortCalls   []int64
	ReplicationJobAbortReasons []string
	SnapshotSetCalls           int
	SnapshotRemoveCalls        int
	SnapshotHoldCalls          int
	SnapshotReleaseCalls       int
	snapshotHolds              map[string]struct{}
	nextSnapshotCreateTXG      uint64
	nextReplicationJobID       int64

	// datasetPassphrases is the MOCK's model of the appliance's own key
	// knowledge, keyed by ENCRYPTION ROOT dataset name. It lives here, not on the
	// shared *Dataset struct, so no passphrase can ride into driver code under
	// test (and into a %+v in a failing test's output) on an object the driver
	// legitimately holds. Used only to validate unlock (P-5) and to re-key on
	// change_key (P-6).
	datasetPassphrases map[string]string

	// FailUserPropertyKeys makes DatasetSetUserProperties fail for these exact
	// property keys (test hook for binding-write failures).
	FailUserPropertyKeys map[string]struct{}

	// FailDatasetDelete makes DatasetDelete fail for these exact dataset names
	// with a non-dependency backend error, so a test can model a DeleteVolume
	// that fails AFTER its earlier best-effort cleanup steps already ran and must
	// then be retried.
	FailDatasetDelete map[string]struct{}

	// FailSnapshotListAfter makes SnapshotList succeed for this many calls and
	// fail on every call after that; 0 never fails and a NEGATIVE value fails
	// every call. It models the DeleteVolume sequence where the up-front snapshot
	// guard reads a good list and the backend then stops answering (the only way
	// to reach the post-destroy "snapshot state cannot be verified" branch), and
	// the promote path's unobtainable corroborating inventory.
	FailSnapshotListAfter int
	snapshotListCalls     int

	// SystemTimezoneName is the IANA zone SystemTimezone reports (default UTC).
	// SystemTimezoneErr makes it unreadable, for the fail-closed path.
	SystemTimezoneName string
	SystemTimezoneErr  error

	// Error injection
	InjectError error
	// SimulateUpdateNoOp models TrueNAS 26.0 pool.snapshot.update returning
	// success without applying user-property additions or removals.
	SimulateUpdateNoOp bool
	// DropDatasetCreateUserProperties models TrueNAS 26.0 accepting inline
	// pool.dataset.create user_properties while silently writing none of them.
	DropDatasetCreateUserProperties bool
	// EmptyNVMeHostNQN models defensive compatibility with backends that omit the
	// otherwise expanded host.hostnqn field from nvmet.host_subsys.query.
	EmptyNVMeHostNQN bool
	// RejectEmptyISCSITargetGroups catches invalid zero-portal target updates.
	RejectEmptyISCSITargetGroups bool
	// NoDeferredSnapshotDestroy models TrueNAS 26.0, whose
	// zfs.resource.snapshot.destroy has no deferred-destroy mode: a snapshot with
	// live clones always fails with ErrSnapshotHasClones regardless of the defer
	// flag, so the driver's tombstone is retained until its last clone is gone.
	NoDeferredSnapshotDestroy bool
	// JobSubscribed is the value AnyConnectionJobSubscribed reports, letting a
	// health test drive the scale_csi_job_dispatcher_subscribed gauge.
	JobSubscribed bool

	// GF5 NFS/ACL/health test surfaces.
	//
	// NFSShareCreateParams records every sharing.nfs.create payload verbatim so a
	// test can assert the DEFAULT payload is byte-identical to the pre-GF5 one.
	NFSShareCreateParams []NFSShareCreateParams
	// NFSShareUpdateParams records every sharing.nfs.update payload verbatim so a
	// test can pin the R4 invariant: an idempotent replay must NEVER rewrite an
	// existing export's security or squash mapping.
	NFSShareUpdateParams  []map[string]interface{}
	NFSServiceConfigValue *NFSServiceConfig
	NFSServiceConfigCalls int
	NFSServiceUpdateCalls []map[string]interface{}

	// ACLs is the per-path ACL state filesystem.setacl mutates; SetACLCalls is
	// the verbatim call log. ACLTemplates overrides the builtin template set.
	ACLs           map[string]*FilesystemACL
	SetACLCalls    []SetACLOptions
	ACLTemplates   map[string][]ACLEntry
	InjectACLError error

	// ZFS choice / topology surfaces for the curated performance classes.
	ZFSChoicesValue    *ZFSPropertyChoices
	ZFSChoicesCalls    int
	InjectChoicesError error
	SpecialVdevPresent bool
	SpecialVdevCalls   int
	InjectPoolError    error

	// Backend health surfaces.
	PoolHealthValue    *PoolHealthSnapshot
	PoolHealthCalls    int
	TemperatureAlerts  []string
	TempAlertCalls     int
	InjectHealthError  error
	InjectTempAlertErr error
	// PoolQueryResult is a RAW pool.query result decoded by the production
	// decoder, so a test can exercise a real middleware response — in particular
	// an empty result, which is a valid answer that must still fail the sample as
	// "pool ... not found". Set PoolQueryResultSet to use it (an empty result is
	// itself meaningful, so nil cannot be the switch).
	PoolQueryResult    interface{}
	PoolQueryResultSet bool
	// PoolHealthEntered / PoolHealthRelease and TempAlertEntered /
	// TempAlertRelease turn either backend read into a call the test can hold
	// IN FLIGHT: the mock signals Entered once it is inside the call and then
	// blocks until Release is closed. That is what lets a test observe what the
	// driver publishes WHILE a backend call has not returned.
	PoolHealthEntered chan struct{}
	PoolHealthRelease chan struct{}
	TempAlertEntered  chan struct{}
	TempAlertRelease  chan struct{}
}

func (m *MockClient) PoolHealth(ctx context.Context, pool string) (*PoolHealthSnapshot, error) {
	m.mu.Lock()
	m.PoolHealthCalls++
	entered, release := m.PoolHealthEntered, m.PoolHealthRelease
	injected := m.InjectHealthError
	queryResult, queryResultSet := m.PoolQueryResult, m.PoolQueryResultSet
	var clone *PoolHealthSnapshot
	if m.PoolHealthValue != nil {
		copied := *m.PoolHealthValue
		copied.Pool = pool
		copied.Disks = append([]string(nil), m.PoolHealthValue.Disks...)
		clone = &copied
	}
	m.mu.Unlock()

	// Gate OUTSIDE the mutex so a held call does not deadlock unrelated mock use.
	if entered != nil {
		entered <- struct{}{}
	}
	if release != nil {
		<-release
	}

	if injected != nil {
		return nil, injected
	}
	if queryResultSet {
		return poolHealthFromQueryResult(pool, queryResult)
	}
	if clone != nil {
		return clone, nil
	}
	return &PoolHealthSnapshot{
		Pool:         pool,
		Status:       PoolStatusOnline,
		Healthy:      true,
		ScanFunction: PoolScanFunctionScrub,
		ScanState:    PoolScanStateFinished,
		Disks:        []string{"nvme0n1", "nvme1n1"},
		SampledAt:    time.Now(),
	}, nil
}

// SetPoolHealthValue updates the health fixture through the mock's lock so a
// test can change the backend response while a real production poll is in
// flight.
func (m *MockClient) SetPoolHealthValue(snapshot *PoolHealthSnapshot) {
	m.mu.Lock()
	m.PoolHealthValue = snapshot
	m.mu.Unlock()
}

// SetTemperatureAlerts updates the temperature fixture through the mock's lock
// for concurrent production-path sampling tests.
func (m *MockClient) SetTemperatureAlerts(alerts []string) {
	m.mu.Lock()
	m.TemperatureAlerts = append([]string(nil), alerts...)
	m.mu.Unlock()
}

func (m *MockClient) DiskTemperatureAlerts(ctx context.Context, names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	m.mu.Lock()
	m.TempAlertCalls++
	entered, release := m.TempAlertEntered, m.TempAlertRelease
	injected := m.InjectTempAlertErr
	alerts := append([]string(nil), m.TemperatureAlerts...)
	m.mu.Unlock()

	if entered != nil {
		entered <- struct{}{}
	}
	if release != nil {
		<-release
	}
	if injected != nil {
		return nil, injected
	}
	return alerts, nil
}

func (m *MockClient) ZFSPropertyChoices(ctx context.Context) (*ZFSPropertyChoices, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ZFSChoicesCalls++
	if m.InjectChoicesError != nil {
		return nil, m.InjectChoicesError
	}
	if m.ZFSChoicesValue != nil {
		return m.ZFSChoicesValue, nil
	}
	// The live nas01 lists, trimmed to what the curated classes use.
	return &ZFSPropertyChoices{
		Recordsize:  []string{"512", "512B", "1K", "2K", "4K", "8K", "16K", "32K", "64K", "128K", "256K", "512K", "1M", "2M", "4M", "8M", "16M"},
		Compression: []string{"ON", "OFF", "LZ4", "GZIP", "ZSTD", "ZLE", "LZJB"},
		Checksum:    []string{"ON", "FLETCHER2", "FLETCHER4", "SHA256", "SHA512", "SKEIN", "EDONR", "BLAKE3"},
	}, nil
}

func (m *MockClient) RecommendedZvolBlocksize(ctx context.Context, pool string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectPoolError != nil {
		return "", m.InjectPoolError
	}
	return "16K", nil
}

func (m *MockClient) PoolHasSpecialVdev(ctx context.Context, pool string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SpecialVdevCalls++
	if m.InjectPoolError != nil {
		return false, m.InjectPoolError
	}
	return m.SpecialVdevPresent, nil
}

// builtinACLTemplates mirrors the NFS4 templates TrueNAS ships. Only the shape
// matters for driver tests: a non-empty NFS4 dacl resolved by name.
var builtinACLTemplates = map[string][]ACLEntry{
	"NFS4_OPEN": {
		{Tag: "owner@", Type: "ALLOW", Perms: map[string]interface{}{"BASIC": "FULL_CONTROL"}},
		{Tag: "group@", Type: "ALLOW", Perms: map[string]interface{}{"BASIC": "MODIFY"}},
		{Tag: "everyone@", Type: "ALLOW", Perms: map[string]interface{}{"BASIC": "MODIFY"}},
	},
	"NFS4_RESTRICTED": {
		{Tag: "owner@", Type: "ALLOW", Perms: map[string]interface{}{"BASIC": "FULL_CONTROL"}},
		{Tag: "group@", Type: "ALLOW", Perms: map[string]interface{}{"BASIC": "MODIFY"}},
	},
	"NFS4_HOME": {
		{Tag: "owner@", Type: "ALLOW", Perms: map[string]interface{}{"BASIC": "FULL_CONTROL"}},
		{Tag: "group@", Type: "ALLOW", Perms: map[string]interface{}{"BASIC": "TRAVERSE"}},
	},
	"NFS4_DOMAIN_HOME": {
		{Tag: "owner@", Type: "ALLOW", Perms: map[string]interface{}{"BASIC": "FULL_CONTROL"}},
	},
	"NFS4_ADMIN": {
		{Tag: "owner@", Type: "ALLOW", Perms: map[string]interface{}{"BASIC": "FULL_CONTROL"}},
	},
}

func (m *MockClient) FilesystemGetACL(ctx context.Context, path string) (*FilesystemACL, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectACLError != nil {
		return nil, m.InjectACLError
	}
	if acl, ok := m.ACLs[path]; ok {
		return acl, nil
	}
	// An un-ACLed dataset reports the mode-derived trivial 3-ACE ACL.
	return &FilesystemACL{
		Path:    path,
		ACLType: "NFS4",
		Trivial: true,
		ACL:     append([]ACLEntry(nil), builtinACLTemplates["NFS4_OPEN"]...),
	}, nil
}

func (m *MockClient) FilesystemSetACL(ctx context.Context, opts *SetACLOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if opts == nil || opts.Path == "" {
		return fmt.Errorf("filesystem.setacl requires a path")
	}
	if len(opts.DACL) == 0 {
		return fmt.Errorf("filesystem.setacl requires a non-empty dacl for %s", opts.Path)
	}
	m.SetACLCalls = append(m.SetACLCalls, *opts)
	if m.InjectACLError != nil {
		return m.InjectACLError
	}
	if m.ACLs == nil {
		m.ACLs = make(map[string]*FilesystemACL)
	}
	m.ACLs[opts.Path] = &FilesystemACL{
		Path:       opts.Path,
		ACLType:    "NFS4",
		Trivial:    false,
		ACL:        append([]ACLEntry(nil), opts.DACL...),
		NFS41Flags: opts.NFS41Flags,
	}
	return nil
}

func (m *MockClient) ACLTemplateDACL(ctx context.Context, name string) ([]ACLEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectACLError != nil {
		return nil, m.InjectACLError
	}
	if dacl, ok := m.ACLTemplates[name]; ok {
		return append([]ACLEntry(nil), dacl...), nil
	}
	if dacl, ok := builtinACLTemplates[name]; ok {
		return append([]ACLEntry(nil), dacl...), nil
	}
	return nil, fmt.Errorf("ACL template %q not found", name)
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
		Datasets:           make(map[string]*Dataset),
		Snapshots:          make(map[string]*Snapshot),
		NFSShares:          make(map[int]*NFSShare),
		ISCSITargets:       make(map[int]*ISCSITarget),
		ISCSIExtents:       make(map[int]*ISCSIExtent),
		TargetExtents:      make(map[int]*ISCSITargetExtent),
		ISCSIAuths:         make(map[int]*ISCSIAuth),
		NVMeHosts:          make(map[string]*NVMeoFHost),
		NVMeHostSubsystems: make(map[int]*NVMeoFHostSubsys),
		NVMeSubsystems:     make(map[int]*NVMeoFSubsystem),
		NVMeNamespaces:     make(map[int]*NVMeoFNamespace),
		ReplicationJobs:    make(map[int64]*ReplicationJob),
		SnapshotTasks:      make(map[int]*SnapshotTask),
		nextSnapshotTaskID: 1,
		// Default portal/initiator fixtures cover the portal addresses used
		// across the test suites so target-group auto-resolution succeeds
		// without per-test setup. Tests may replace these maps.
		ISCSIPortals: map[int]*ISCSIPortal{
			1: {ID: 1, Tag: 1, Listen: []ISCSIPortalListen{
				{IP: "192.0.2.10", Port: 3260},
				{IP: "192.0.2.100", Port: 3260},
				{IP: "127.0.0.1", Port: 3260},
			}},
		},
		ISCSIInitiators: map[int]*ISCSIInitiator{
			1: {ID: 1, Initiators: nil, Comment: "allow-all (mock)"},
		},
		deferredSnapshots:    make(map[string]struct{}),
		snapshotHolds:        make(map[string]struct{}),
		datasetPassphrases:   make(map[string]string),
		PoolAvailable:        100 * 1024 * 1024 * 1024, // 100 GiB default
		nextReplicationJobID: 1,
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
	return m.ISCSIInitiatorCreateWithInitiators(ctx, nil, comment)
}

func (m *MockClient) ISCSIInitiatorCreateWithInitiators(ctx context.Context, initiators []string, comment string) (*ISCSIInitiator, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	id := len(m.ISCSIInitiators) + 1
	for m.ISCSIInitiators[id] != nil {
		id++
	}
	group := &ISCSIInitiator{ID: id, Initiators: cloneStringsPreservingNil(initiators), Comment: comment}
	m.ISCSIInitiators[id] = group
	return group, nil
}

func (m *MockClient) ISCSIInitiatorGet(ctx context.Context, id int) (*ISCSIInitiator, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ISCSIInitiators[id], nil
}

func (m *MockClient) ISCSIInitiatorUpdate(ctx context.Context, id int, initiators []string, comment string) (*ISCSIInitiator, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	group := m.ISCSIInitiators[id]
	if group == nil {
		return nil, notFoundAPIError("iSCSI initiator group not found")
	}
	group.Initiators = cloneStringsPreservingNil(initiators)
	if comment != "" {
		group.Comment = comment
	}
	return group, nil
}

func cloneStringsPreservingNil(values []string) []string {
	if values == nil {
		return nil
	}
	return append([]string{}, values...)
}

func (m *MockClient) ISCSIInitiatorDelete(ctx context.Context, id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.ISCSIInitiators, id)
	return nil
}

// ISCSIAuthCreate stores a mock iSCSI CHAP auth peer. Secret arguments are
// accepted to satisfy the interface but are never retained.
func (m *MockClient) ISCSIAuthCreate(ctx context.Context, tag int, user, secret, peerUser, peerSecret string) (*ISCSIAuth, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	id := len(m.ISCSIAuths) + 1
	for m.ISCSIAuths[id] != nil {
		id++
	}
	auth := &ISCSIAuth{
		ID:                    id,
		Tag:                   tag,
		User:                  user,
		PeerUser:              peerUser,
		CredentialFingerprint: ISCSIAuthCredentialFingerprint(user, secret, peerUser, peerSecret),
	}
	m.ISCSIAuths[id] = auth
	return auth, nil
}

func (m *MockClient) ISCSIAuthQueryByTag(ctx context.Context, tag int) ([]*ISCSIAuth, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	peers := make([]*ISCSIAuth, 0)
	for _, auth := range m.ISCSIAuths {
		if auth.Tag == tag {
			peers = append(peers, auth)
		}
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	return peers, nil
}

func (m *MockClient) ISCSIAuthGet(ctx context.Context, id int) (*ISCSIAuth, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ISCSIAuths[id], nil
}

func (m *MockClient) ISCSIAuthUpdate(ctx context.Context, id int, user, secret, peerUser, peerSecret string) (*ISCSIAuth, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	auth := m.ISCSIAuths[id]
	if auth == nil {
		return nil, notFoundAPIError("iSCSI auth peer not found")
	}
	auth.User = user
	auth.PeerUser = peerUser
	auth.CredentialFingerprint = ISCSIAuthCredentialFingerprint(user, secret, peerUser, peerSecret)
	return auth, nil
}

func (m *MockClient) ISCSIAuthDelete(ctx context.Context, id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.ISCSIAuths, id)
	return nil
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

func (m *MockClient) ReplicationJobList(ctx context.Context) ([]*ReplicationJob, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	jobs := make([]*ReplicationJob, 0, len(m.ReplicationJobs))
	for _, job := range m.ReplicationJobs {
		if job == nil || job.Method != ReplicationRunOnetimeMethod || !isActiveReplicationJobState(job.State) {
			continue
		}
		copy := *job
		copy.SourceDatasets = append([]string(nil), job.SourceDatasets...)
		jobs = append(jobs, &copy)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
	return jobs, nil
}

func (m *MockClient) ReplicationJobAbort(ctx context.Context, jobID int64, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplicationJobAbortCalls = append(m.ReplicationJobAbortCalls, jobID)
	m.ReplicationJobAbortReasons = append(m.ReplicationJobAbortReasons, reason)
	if m.InjectError != nil {
		return m.InjectError
	}
	if job := m.ReplicationJobs[jobID]; job != nil {
		job.State = "ABORTED"
	}
	return nil
}

// AddReplicationJob and ReplicationJobAbortHistory provide race-safe setup and
// inspection for controller-loop tests.
func (m *MockClient) AddReplicationJob(job *ReplicationJob) {
	if job == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	copy := *job
	copy.SourceDatasets = append([]string(nil), job.SourceDatasets...)
	m.ReplicationJobs[job.ID] = &copy
	if job.ID >= m.nextReplicationJobID {
		m.nextReplicationJobID = job.ID + 1
	}
}

func (m *MockClient) ReplicationJobAbortHistory() ([]int64, []string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]int64(nil), m.ReplicationJobAbortCalls...), append([]string(nil), m.ReplicationJobAbortReasons...)
}

// Circuit breaker methods (return nil/no-op for mock)
func (m *MockClient) CircuitBreakerStats() *CircuitBreakerStats { return nil }
func (m *MockClient) ResetCircuitBreaker()                      {}

// AnyConnectionJobSubscribed returns the test-configurable subscription bit.
func (m *MockClient) AnyConnectionJobSubscribed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.JobSubscribed
}

// Dataset methods
func (m *MockClient) DatasetCreate(ctx context.Context, params *DatasetCreateParams) (*Dataset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	// TrueNAS 26.0 validates the payload SCHEMA before it looks at anything else,
	// including whether the dataset already exists. Reproduce that ordering so a
	// key 26.0 rejects fails here instead of being masked by the idempotent
	// already-exists arm below. The model is per dataset TYPE, so params.Type
	// selects which keys are legal — recordsize on a VOLUME is as fatal as
	// logbias on either. An omitted type defaults to FILESYSTEM, exactly as
	// pool.dataset.create does, so a typeless payload cannot slip volume-only
	// keys past the gate (and the stored dataset never carries an empty type).
	datasetType := params.Type
	if datasetType == "" {
		datasetType = "FILESYSTEM"
	}
	if err := validateDatasetPayloadKeys(params, datasetCreateKeyScopes, datasetType); err != nil {
		return nil, fmt.Errorf("failed to create dataset: %w", err)
	}
	if existing, exists := m.Datasets[params.Name]; exists {
		// Match the real client's idempotent AlreadyExists fallback while
		// preserving the response-local fact that this call did not create the
		// object. Never mutate the winner's response through shared mock storage.
		return mockDatasetResponse(existing, false), nil
	}

	ds := &Dataset{
		ID:             params.Name,
		Name:           params.Name,
		Type:           datasetType,
		UserProperties: make(map[string]UserProperty),
		Creation:       DatasetProperty{Parsed: float64(time.Now().Unix())},
		Volsize:        DatasetProperty{Parsed: float64(params.Volsize)},
		Refquota:       DatasetProperty{Parsed: float64(params.Refquota)},
		Refreservation: DatasetProperty{Parsed: float64(params.Refreservation)},
		Volblocksize:   DatasetProperty{Parsed: params.Volblocksize},
		CreatedByCall:  false,
	}
	if !m.DropDatasetCreateUserProperties {
		for _, property := range params.UserProperties {
			ds.UserProperties[property.Key] = UserProperty{Value: property.Value, Source: "local"}
		}
	}
	if datasetType != "VOLUME" {
		ds.Mountpoint = "/mnt/" + strings.TrimPrefix(params.Name, "/")
	}
	// Encryption at rest (GF-Sprint 1). A create with encryption:true comes up
	// UNLOCKED with the key loaded and is its own encryption_root (P-1/P-2:
	// encrypted:true, key_loaded:true, locked:false, encryption_root:<self>). The
	// mock records the create passphrase as the dataset's current key so unlock /
	// change_key can validate against it faithfully (P-5/P-6).
	if params.Encryption != nil && *params.Encryption {
		ds.Encrypted = true
		ds.KeyLoaded = true
		ds.Locked = false
		ds.EncryptionRoot = params.Name
		// P-10: a passphrase create reports key_format PASSPHRASE. It is what tells
		// the driver this dataset needs a key from IT, rather than one the appliance
		// stores and auto-loads.
		ds.KeyFormat = KeyFormatPassphrase
		if opts := params.EncryptionOptions; opts != nil {
			// The key goes into the mock's own side table, never onto the *Dataset
			// the driver receives.
			m.datasetPassphrases[params.Name] = opts.Passphrase
			ds.EncryptionAlgorithm = opts.Algorithm
		}
		if ds.EncryptionAlgorithm == "" {
			ds.EncryptionAlgorithm = "AES-256-GCM"
		}
	}
	// ZFS ENCRYPTION IS INHERITED (P-10). A dataset created with no encryption of
	// its own under an ENCRYPTED ancestor comes out encrypted:true with the
	// ANCESTOR as its encryption_root and the ancestor's key format, and it is
	// locked exactly when the ancestor is. Modeling this is what makes the
	// encrypted-parent deployment testable at all: without it every such dataset
	// looked plaintext, and a driver that equated encrypted:true with "has its own
	// key" destroyed restored data on those deployments with nothing to catch it.
	m.applyInheritedEncryptionLocked(ds)
	m.Datasets[params.Name] = ds
	return mockDatasetResponse(ds, true), nil
}

// applyInheritedEncryptionLocked gives a dataset the encryption identity it
// inherits from its nearest ENCRYPTED ancestor, when it has none of its own.
// Callers hold m.mu.
func (m *MockClient) applyInheritedEncryptionLocked(ds *Dataset) {
	if ds == nil || ds.Encrypted {
		return
	}
	name := ds.Name
	for {
		cut := strings.LastIndex(name, "/")
		if cut <= 0 {
			return
		}
		name = name[:cut]
		ancestor, ok := m.Datasets[name]
		if !ok {
			continue
		}
		if !ancestor.Encrypted {
			return
		}
		ds.Encrypted = true
		ds.EncryptionRoot = ancestor.EncryptionRoot
		if ds.EncryptionRoot == "" {
			ds.EncryptionRoot = ancestor.Name
		}
		ds.KeyFormat = ancestor.KeyFormat
		ds.EncryptionAlgorithm = ancestor.EncryptionAlgorithm
		ds.Locked = ancestor.Locked
		ds.KeyLoaded = ancestor.KeyLoaded
		if ds.Locked {
			ds.Mountpoint = ""
		}
		return
	}
}

func mockDatasetResponse(dataset *Dataset, created bool) *Dataset {
	if dataset == nil {
		return nil
	}
	response := *dataset
	response.UserProperties = make(map[string]UserProperty, len(dataset.UserProperties))
	for key, property := range dataset.UserProperties {
		response.UserProperties[key] = property
	}
	response.CreatedByCall = created
	// P-4: a locked dataset reports mountpoint:null (filesystem) and has no backing
	// zvol device. The row survives; the mountpoint/device does not.
	if response.Locked {
		response.Mountpoint = ""
	}
	return &response
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
	if _, fail := m.FailDatasetDelete[name]; fail {
		return &APIError{Code: -1, Message: "injected dataset delete failure"}
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
	// A recursive destroy cannot remove a HELD snapshot: ZFS returns EBUSY for
	// the whole operation (P1). Modeling this is what makes the DeleteVolume
	// held-tombstone recovery path testable — the previous mock silently deleted
	// held snapshots and hid the wedge entirely (GF2-fix/H4).
	if recursive {
		for snapshotID, snapshot := range m.Snapshots {
			if snapshot.Dataset != name {
				continue
			}
			if _, held := m.snapshotHolds[snapshotID]; held {
				return &APIError{
					Code:    int(syscall.EBUSY),
					Message: fmt.Sprintf("'%s' has the following holds: truenas", snapshotID),
				}
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
		return mockDatasetResponse(ds, false), nil
	}
	return nil, notFoundAPIError("dataset not found")
}

func (m *MockClient) DatasetGetByNames(ctx context.Context, names []string) (map[string]*Dataset, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	result := make(map[string]*Dataset, len(names))
	for _, name := range names {
		if ds, ok := m.Datasets[name]; ok {
			result[name] = mockDatasetResponse(ds, false)
		}
	}
	return result, nil
}

func (m *MockClient) DatasetUpdate(ctx context.Context, name string, params *DatasetUpdateParams) (*Dataset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	// Schema first, exactly as on create: the middleware rejects an out-of-model
	// key before it does anything with the dataset. pool.dataset.update is typed
	// too, so the stored type is looked up (a read only — the not-found arm below
	// still owns the ordering) and drives the per-type key set. A name the mock
	// does not hold resolves to the empty type, which validates permissively:
	// only the keys NEITHER model accepts are rejected, and the error path is the
	// bare "data.<key>" form.
	updateType := ""
	if stored, held := m.Datasets[name]; held {
		updateType = stored.Type
	}
	if err := validateDatasetPayloadKeys(params, datasetUpdateKeyScopes, updateType); err != nil {
		return nil, fmt.Errorf("failed to update dataset: %w", err)
	}
	ds, ok := m.Datasets[name]
	if !ok {
		return nil, notFoundAPIError("dataset not found")
	}

	if params.Volsize > 0 {
		ds.Volsize = DatasetProperty{Parsed: float64(params.Volsize)}
	}
	if params.Refquota != nil {
		switch refquota := params.Refquota.(type) {
		case int:
			ds.Refquota = DatasetProperty{Parsed: float64(refquota), Source: "LOCAL"}
		case int64:
			ds.Refquota = DatasetProperty{Parsed: float64(refquota), Source: "LOCAL"}
		case float64:
			ds.Refquota = DatasetProperty{Parsed: refquota, Source: "LOCAL"}
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
		ds.UserProperties[update.Key] = UserProperty{Value: update.Value, Source: "local"}
	}
	return mockDatasetResponse(ds, false), nil
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
		list = append(list, mockDatasetResponse(ds, false))
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

// DatasetQueryByParent mirrors the real zfs.resource.query path: it returns ALL
// datasets stored under parentDataset (no managed_resource filter), letting the
// driver apply the same client-side managed_resource filter it uses against the
// real client.
//
// Fidelity with live TrueNAS 26.0 (probe-confirmed): zfs.resource.query returns
// per-dataset user_properties as a FLAT string map with NO per-property source —
// native properties carry source, user_properties do not. The mock therefore
// strips Source to "" on every returned user property and marks the dataset
// ResourceQuery, so source-sensitive callers (publicationRecordsFromDataset)
// exercise the same sourceless path they hit in production instead of being
// masked by a source-bearing mock. DatasetGet (pool.dataset.query) stays
// source-bearing, exactly as the real split behaves.
func (m *MockClient) DatasetQueryByParent(ctx context.Context, parentDataset string) ([]*Dataset, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}

	parent := strings.TrimSuffix(parentDataset, "/")
	var list []*Dataset
	for _, ds := range m.Datasets {
		if parent != "" && !strings.HasPrefix(ds.Name, parent+"/") {
			continue
		}
		response := mockDatasetResponse(ds, false)
		for key, property := range response.UserProperties {
			property.Source = ""
			response.UserProperties[key] = property
		}
		// Same fidelity discipline for every encryption field: zfs.resource.query
		// returns NO encryption, key or lock fields AT ALL (P-11, nas01
		// 26.0.0-BETA.1, 2026-08-02) and parseDatasetResource reads none of them, so
		// a dataset that arrives through this path carries none of them in
		// production. Zero them here so no caller can build on a
		// signal the resource path does not actually deliver — the exact class of
		// mistake that made the unlock reconciler a silent no-op.
		response.Encrypted = false
		response.Locked = false
		response.KeyLoaded = false
		response.EncryptionRoot = ""
		response.KeyFormat = ""
		response.EncryptionAlgorithm = ""
		response.ResourceQuery = true
		list = append(list, response)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return list, nil
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

// DatasetPromote models pool.dataset.promote with FULL live fidelity (P3):
//
//   - the promoted clone becomes independent (its origin is cleared);
//   - EVERY snapshot of the source dataset older-or-equal to the origin
//     (createtxg <= the origin's) MIGRATES onto the promoted clone — not just
//     the origin. Real ZFS moves the whole older-or-equal set, which is exactly
//     how an unrelated LIVE CSI VolumeSnapshot silently changes backend ID
//     (GF2-fix/H1). The previous mock migrated only the origin, so no test could
//     observe that class of defect;
//   - the SOURCE dataset itself is re-parented onto the migrated origin (the
//     live-probed dependency inversion), as is every sibling clone;
//   - holds and deferred markers travel with each migrated snapshot (P1).
func (m *MockClient) DatasetPromote(ctx context.Context, datasetName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	ds, ok := m.Datasets[datasetName]
	if !ok {
		return notFoundAPIError("dataset not found")
	}
	origin := datasetPropertyString(ds.Origin)
	if origin == "" {
		return &APIError{Code: -1, Message: "dataset is not a clone; nothing to promote"}
	}
	m.DatasetPromoteCalls = append(m.DatasetPromoteCalls, datasetName)

	sourceDataset, originSnapName, _ := strings.Cut(origin, "@")
	migratedID := datasetName + "@" + originSnapName

	originTXG := uint64(0)
	if snap, ok := m.Snapshots[origin]; ok {
		originTXG = snap.CreateTXG
	}

	for id, snap := range m.Snapshots {
		if snap.Dataset != sourceDataset {
			continue
		}
		olderOrEqual := snap.CreateTXG != 0 && originTXG != 0 && snap.CreateTXG <= originTXG
		if id != origin && !olderOrEqual {
			continue
		}
		newID := datasetName + "@" + snap.Name
		delete(m.Snapshots, id)
		snap.Dataset = datasetName
		snap.ID = newID
		m.Snapshots[newID] = snap
		if _, held := m.snapshotHolds[id]; held {
			delete(m.snapshotHolds, id)
			m.snapshotHolds[newID] = struct{}{}
		}
		if _, deferred := m.deferredSnapshots[id]; deferred {
			delete(m.deferredSnapshots, id)
			m.deferredSnapshots[newID] = struct{}{}
		}
	}

	// Re-parent sibling clones onto the migrated origin.
	for _, other := range m.Datasets {
		if datasetPropertyString(other.Origin) == origin {
			other.Origin = DatasetProperty{Value: migratedID, Parsed: migratedID, Rawvalue: migratedID, Source: "LOCAL"}
		}
	}
	// The SOURCE itself becomes a clone of the promoted dataset (P3 inversion).
	if source, ok := m.Datasets[sourceDataset]; ok && source != ds {
		source.Origin = DatasetProperty{Value: migratedID, Parsed: migratedID, Rawvalue: migratedID, Source: "LOCAL"}
	}
	ds.Origin = DatasetProperty{}
	return nil
}

// SnapshotDependentClones mirrors the real client's authoritative per-snapshot
// dependent-clone query: it walks ALL datasets, not only driver-managed ones,
// so a test can seed an unmanaged sibling clone and see it counted.
func (m *MockClient) SnapshotDependentClones(ctx context.Context, snapshotID string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	var clones []string
	for name, dataset := range m.Datasets {
		if datasetPropertyString(dataset.Origin) == snapshotID {
			clones = append(clones, name)
		}
	}
	sort.Strings(clones)
	return clones, nil
}

func (m *MockClient) DatasetGetQuotaUsage(ctx context.Context, datasetName string) (*DatasetQuotaUsage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	ds, ok := m.Datasets[datasetName]
	if !ok {
		return nil, notFoundAPIError("dataset not found")
	}
	// Project through the SAME helper the real client uses, so a test can never
	// observe a usage shape production does not produce.
	return ds.QuotaUsage(), nil
}

// setEncryptionRootStateLocked flips an encryption root and EVERY dataset that
// inherits its key to the same lock state. P-7: a clone's encryption_root is the
// ORIGIN, so it shares the origin's key — locking the origin locks the clone.
// Callers hold m.mu.
func (m *MockClient) setEncryptionRootStateLocked(root string, locked bool) {
	for _, candidate := range m.Datasets {
		if !candidate.Encrypted || candidate.EncryptionRoot != root {
			continue
		}
		candidate.Locked = locked
		candidate.KeyLoaded = !locked
	}
}

// DatasetLock models pool.dataset.lock (a @job). Test/drill only — no driver
// control path locks a dataset. It flips the dataset to the P-4 locked state:
// locked:true, key_loaded:false, and (via mockDatasetResponse) no mountpoint /
// backing device. A dataset whose key is INHERITED (encryption_root != itself,
// i.e. a clone, P-7) cannot be locked on its own — the operation belongs to its
// root.
func (m *MockClient) DatasetLock(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InjectError != nil {
		return m.InjectError
	}
	ds, ok := m.Datasets[name]
	if !ok {
		return notFoundAPIError("dataset not found")
	}
	if !ds.Encrypted {
		return &jobTerminalError{state: "FAILED", detail: "dataset is not encrypted"}
	}
	if ds.EncryptionRoot != "" && ds.EncryptionRoot != name {
		return &jobTerminalError{state: "FAILED",
			detail: "dataset is not an encryption root; its key is inherited from " + ds.EncryptionRoot}
	}
	m.setEncryptionRootStateLocked(name, true)
	ds.Locked = true
	ds.KeyLoaded = false
	return nil
}

// DatasetUnlock models pool.dataset.unlock (a @job), including its sharp edges:
// it is NOT idempotent — unlocking an already-unlocked dataset is a FAILED job
// (P-8) — a wrong passphrase is a FAILED job that leaves the dataset locked
// (P-5, fail-closed native), and a dataset whose key is INHERITED (a clone,
// P-7) has no key of its own to load.
func (m *MockClient) DatasetUnlock(ctx context.Context, name, passphrase string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InjectError != nil {
		return m.InjectError
	}
	ds, ok := m.Datasets[name]
	if !ok {
		return notFoundAPIError("dataset not found")
	}
	if !ds.Encrypted {
		return &jobTerminalError{state: "FAILED", detail: "dataset is not encrypted"}
	}
	if ds.EncryptionRoot != "" && ds.EncryptionRoot != name {
		return &jobTerminalError{state: "FAILED",
			detail: "dataset is not an encryption root; its key is inherited from " + ds.EncryptionRoot}
	}
	if !ds.Locked {
		// P-8: unlock on an already-unlocked dataset fails.
		return &jobTerminalError{state: "FAILED", detail: "dataset is already unlocked"}
	}
	if passphrase != m.datasetPassphrases[name] {
		// P-5: wrong passphrase -> FAILED, dataset stays locked, no device.
		return &jobTerminalError{state: "FAILED", detail: "failed to unlock dataset: invalid passphrase"}
	}
	m.setEncryptionRootStateLocked(name, false)
	ds.Locked = false
	ds.KeyLoaded = true
	return nil
}

// DatasetChangeKey models pool.dataset.change_key (a @job, P-6): it re-keys an
// UNLOCKED dataset and requires the key already loaded. Afterward the old
// passphrase is dead. Re-keying to the SAME passphrase SUCCEEDS and leaves that
// key valid (probed live on nas01 26.0.0-BETA.1, 2026-08-02: same-key change_key
// returns job SUCCESS and a following lock -> unlock with that passphrase
// succeeds) — this is what makes the driver's rotation-completion arm safe to
// call unconditionally on an unlocked dataset. An inheriting child (a clone,
// P-7) cannot be re-keyed at all.
func (m *MockClient) DatasetChangeKey(ctx context.Context, name, passphrase string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InjectError != nil {
		return m.InjectError
	}
	ds, ok := m.Datasets[name]
	if !ok {
		return notFoundAPIError("dataset not found")
	}
	if !ds.Encrypted {
		return &jobTerminalError{state: "FAILED", detail: "dataset is not encrypted"}
	}
	if ds.EncryptionRoot != "" && ds.EncryptionRoot != name {
		return &jobTerminalError{state: "FAILED",
			detail: "dataset is not an encryption root; its key is inherited from " + ds.EncryptionRoot}
	}
	if ds.Locked || !ds.KeyLoaded {
		return &jobTerminalError{state: "FAILED", detail: "dataset must be unlocked before changing its key"}
	}
	m.datasetPassphrases[name] = passphrase
	return nil
}

// DatasetEncryptionSummary models pool.dataset.encryption_summary (a @job whose
// result is the P-3 list). The driver reads Locked and ValidKey to gate unlock
// and to report health.
func (m *MockClient) DatasetEncryptionSummary(ctx context.Context, name string) ([]EncryptionSummaryEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	ds, ok := m.Datasets[name]
	if !ok {
		return nil, notFoundAPIError("dataset not found")
	}
	if !ds.Encrypted {
		return nil, notFoundAPIError("dataset is not encrypted")
	}
	keyFormat := "PASSPHRASE"
	return []EncryptionSummaryEntry{{
		Name:                 name,
		KeyFormat:            keyFormat,
		KeyPresentInDatabase: false, // P-3: TrueNAS does not persist a passphrase
		ValidKey:             ds.KeyLoaded,
		Locked:               ds.Locked,
		UnlockSuccessful:     ds.KeyLoaded,
	}}, nil
}

func (m *MockClient) DatasetSetUserProperty(ctx context.Context, name, key, value string) error {
	return m.DatasetSetUserProperties(ctx, name, map[string]string{key: value})
}

// SetUserPropertiesCallCount reports how many DatasetSetUserProperties calls
// the mock has served (including single-property delegations).
func (m *MockClient) SetUserPropertiesCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setUserPropertiesCalls
}

func (m *MockClient) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.setUserPropertiesCalls++
	if m.InjectError != nil {
		return m.InjectError
	}
	// FailUserPropertyKeys lets a test reproduce a partial/failed binding write
	// for a specific property, which is how the create-then-stamp orderings that
	// strand backend objects are exercised.
	for key := range properties {
		if _, fail := m.FailUserPropertyKeys[key]; fail {
			return &APIError{Code: -1, Message: "injected user-property write failure for " + key}
		}
	}
	ds, ok := m.Datasets[name]
	if !ok {
		return notFoundAPIError("dataset not found")
	}
	for key, value := range properties {
		ds.UserProperties[key] = UserProperty{Value: value, Source: "local"}
	}
	return nil
}

func (m *MockClient) DatasetRemoveUserProperties(ctx context.Context, name string, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InjectError != nil {
		return m.InjectError
	}
	ds, ok := m.Datasets[name]
	if !ok {
		return notFoundAPIError("dataset not found")
	}
	for _, key := range keys {
		delete(ds.UserProperties, key)
	}
	return nil
}

func (m *MockClient) DatasetGetUserProperty(ctx context.Context, name, key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ds, ok := m.Datasets[name]
	if !ok {
		return "", notFoundAPIError("dataset not found")
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
		return notFoundAPIError("dataset not found")
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
	// P-4: a locked zvol has NO backing device (/dev/zvol/<name> is gone), so it
	// can never become ready. Model that so a publish/share build over a locked
	// zvol fails the way it does on a real appliance.
	m.mu.RLock()
	locked := false
	if ds, ok := m.Datasets[name]; ok {
		locked = ds.Locked
	}
	m.mu.RUnlock()
	if locked {
		return nil, fmt.Errorf("zvol %s has no backing device: dataset is locked", name)
	}
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
	// Model ZFS's monotonic, never-reused creation transaction group. Promote's
	// "migrate every snapshot older-or-equal to the origin" semantics are defined
	// in terms of createtxg, so the mock must issue one per snapshot or that
	// behavior cannot be modeled faithfully (GF2-fix/H1).
	m.nextSnapshotCreateTXG++
	snap := &Snapshot{
		ID:        id,
		Name:      name,
		Dataset:   dataset,
		CreateTXG: m.nextSnapshotCreateTXG,
		Properties: map[string]interface{}{
			"creation": map[string]interface{}{"parsed": float64(time.Now().Unix())},
		},
		UserProperties: make(map[string]UserProperty, len(userProperties)),
		// The mock models the TrueNAS 26.0 zfs.resource.snapshot.query read path,
		// matching DatasetGet's ResourceQuery=true. This flag must accompany the
		// full-property inheritance below: the 26.0 flat read returns inherited
		// values AND sets this flag, so identity sniffs (isCSISnapshot) trust only
		// snapshot-only properties on it. Inherited properties WITHOUT the flag
		// would model a hybrid no real API version produces — the legacy path
		// returns local-only properties.
		ResourceQuery: true,
	}
	// A ZFS snapshot holds the dataset's user properties as of the instant it was
	// taken — ALL of them, not a chosen subset. The driver's geometry-provenance
	// rule depends on exactly that (the block-geometry stamp a snapshot CAPTURED is
	// the only record of the layout of the bytes inside it), and so does every
	// witness- and identity-sniffing rule that reads a snapshot back.
	//
	// ROUND 6 WIDENING. Round 5 modeled the capture for the two geometry keys ONLY,
	// on the reasoning that widening it would disturb identity sniffs elsewhere.
	// That reasoning had the fidelity argument backwards: the narrowing did not
	// prevent a behavior, it prevented the SUITE FROM SEEING one, so no test could
	// expose a bug that depends on a witness or identity property riding a snapshot
	// — which is precisely the class this driver's restore path reasons about.
	// The mock now inherits every user property, exactly like ZFS. Callers that
	// need a snapshot to look like a NON-CSI snapshot must set up a dataset that
	// does not carry the CSI markers, rather than relying on the mock to drop them.
	//
	// A ZFS user property whose value is "-" is still a local property and is
	// inherited by a snapshot. Removing a property is a separate remove update;
	// the mock follows that distinction here.
	for key, prop := range m.datasetUserPropertiesLocked(dataset) {
		snap.UserProperties[key] = prop
	}
	for key, value := range userProperties {
		snap.UserProperties[key] = UserProperty{Value: value, Source: "local"}
	}
	m.Snapshots[id] = snap
	return snap, nil
}

// datasetUserPropertiesLocked snapshots the inheritable user properties of a
// dataset. The caller holds m.mu.
func (m *MockClient) datasetUserPropertiesLocked(dataset string) map[string]UserProperty {
	ds, ok := m.Datasets[dataset]
	if !ok || ds == nil || ds.UserProperties == nil {
		return nil
	}
	inherited := make(map[string]UserProperty, len(ds.UserProperties))
	for key, prop := range ds.UserProperties {
		if prop.Value == "" {
			continue
		}
		inherited[key] = prop
	}
	return inherited
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
	// A held snapshot cannot be destroyed (P1): model the EBUSY a foreign actor
	// hits so tests can prove a hold blocks deletion until the driver releases it.
	if _, held := m.snapshotHolds[snapshotID]; held {
		return &APIError{Code: int(syscall.EBUSY), Message: fmt.Sprintf("'%s' has the following holds: truenas", snapshotID)}
	}
	clones := m.snapshotClonesLocked(snapshotID)
	if len(clones) > 0 {
		// TrueNAS 26.0 has no deferred destroy: the request fails with has-clones
		// no matter what defer_ says, and nothing is marked for later reclamation.
		if !defer_ || m.NoDeferredSnapshotDestroy {
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
	snap, ok := m.Snapshots[snapshotID]
	if !ok {
		return notFoundAPIError("snapshot not found")
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
	// A hold survives the rename (P1): carry it onto the tombstone ID so the
	// reaper interaction (release-before-reap) is exercisable.
	if _, held := m.snapshotHolds[snapshotID]; held {
		delete(m.snapshotHolds, snapshotID)
		m.snapshotHolds[newSnapshotID] = struct{}{}
	}
	return nil
}

func (m *MockClient) SnapshotHold(ctx context.Context, snapshotID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SnapshotHoldCalls++

	if m.InjectError != nil {
		return m.InjectError
	}
	if _, ok := m.Snapshots[snapshotID]; !ok {
		return notFoundAPIError("snapshot not found")
	}
	// Re-holding an already-held snapshot is idempotent success (P1: EEXIST/17).
	m.snapshotHolds[snapshotID] = struct{}{}
	return nil
}

func (m *MockClient) SnapshotRelease(ctx context.Context, snapshotID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SnapshotReleaseCalls++

	if m.InjectError != nil {
		return m.InjectError
	}
	// Releasing a non-held (or absent) snapshot is idempotent success.
	delete(m.snapshotHolds, snapshotID)
	return nil
}

func (m *MockClient) SnapshotIsHeld(ctx context.Context, snapshotID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.InjectError != nil {
		return false, m.InjectError
	}
	_, held := m.snapshotHolds[snapshotID]
	return held, nil
}

func (m *MockClient) SnapshotTaskCreate(ctx context.Context, params *SnapshotTaskCreateParams) (*SnapshotTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	task := &SnapshotTask{
		ID:            m.nextSnapshotTaskID,
		Dataset:       params.Dataset,
		Recursive:     params.Recursive,
		NamingSchema:  params.NamingSchema,
		Schedule:      params.Schedule,
		LifetimeValue: params.LifetimeValue,
		LifetimeUnit:  params.LifetimeUnit,
		Enabled:       params.Enabled,
		AllowEmpty:    params.AllowEmpty,
	}
	m.SnapshotTasks[task.ID] = task
	m.nextSnapshotTaskID++
	return task, nil
}

// FireSnapshotTasks models the TrueNAS middleware taking the snapshots its
// enabled periodic-snapshot tasks are due for, at wall-clock instant `at` in
// `zone` (the NAS's local timezone — pass nil for UTC).
//
// It exists so a test can obtain a task-created snapshot the way production
// obtains one, instead of hand-writing a name with the driver's own rendering
// helper and then asserting the driver accepts it (which asserts nothing). The
// strftime expansion here is the MOCK's own, deliberately independent of
// pkg/driver's schema algorithm: if the driver's rendering and its verification
// ever drift apart, this is what notices.
//
// It also models the split the driver cannot see through: the NAME is rendered
// from LOCAL civil time while the `creation` property is UTC epoch seconds.
func (m *MockClient) FireSnapshotTasks(ctx context.Context, at time.Time, zone *time.Location) ([]*Snapshot, error) {
	if zone == nil {
		zone = time.UTC
	}
	m.mu.RLock()
	due := make([]*SnapshotTask, 0, len(m.SnapshotTasks))
	for _, task := range m.SnapshotTasks {
		if task != nil && task.Enabled {
			due = append(due, task)
		}
	}
	m.mu.RUnlock()
	sort.Slice(due, func(i, j int) bool { return due[i].ID < due[j].ID })

	created := make([]*Snapshot, 0, len(due))
	for _, task := range due {
		name := expandStrftimeNamingSchema(task.NamingSchema, at.In(zone))
		snap, err := m.SnapshotCreate(ctx, task.Dataset, name, nil)
		if err != nil {
			return created, err
		}
		m.SetSnapshotCreationTime(snap.ID, at.Unix())
		created = append(created, snap)
	}
	return created, nil
}

// expandStrftimeNamingSchema expands the strftime directives TrueNAS supports in
// a periodic-snapshot task naming schema. Written from the strftime spec rather
// than from the driver's schema constants on purpose (see FireSnapshotTasks).
func expandStrftimeNamingSchema(schema string, at time.Time) string {
	replacements := []struct{ directive, layout string }{
		{"%Y", "2006"},
		{"%m", "01"},
		{"%d", "02"},
		{"%H", "15"},
		{"%M", "04"},
		{"%S", "05"},
	}
	var out strings.Builder
	for i := 0; i < len(schema); {
		if schema[i] == '%' && i+1 < len(schema) {
			directive := schema[i : i+2]
			expanded := false
			for _, r := range replacements {
				if directive == r.directive {
					out.WriteString(at.Format(r.layout))
					expanded = true
					break
				}
			}
			if expanded {
				i += 2
				continue
			}
		}
		out.WriteByte(schema[i])
		i++
	}
	return out.String()
}

// SetSnapshotCreationTime is a test helper that sets a snapshot's `creation`
// property to a specific UTC epoch second, in the exact wire shape TrueNAS 26.0
// returns from zfs.resource.snapshot.query (see
// pkg/truenas/testdata/snapshot-resource-26.0.json).
func (m *MockClient) SetSnapshotCreationTime(snapshotID string, creationUnix int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap, ok := m.Snapshots[snapshotID]
	if !ok {
		return
	}
	if snap.Properties == nil {
		snap.Properties = make(map[string]interface{})
	}
	snap.Properties["creation"] = map[string]interface{}{
		"value": float64(creationUnix),
		"raw":   strconv.FormatInt(creationUnix, 10),
	}
}

func (m *MockClient) SnapshotTaskListByDataset(ctx context.Context, dataset string) ([]*SnapshotTask, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	var tasks []*SnapshotTask
	for _, task := range m.SnapshotTasks {
		if task.Dataset == dataset {
			tasks = append(tasks, task)
		}
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	return tasks, nil
}

func (m *MockClient) SnapshotTaskListByParent(ctx context.Context, parentDataset string) ([]*SnapshotTask, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.InjectError != nil {
		return nil, m.InjectError
	}
	prefix := strings.TrimSuffix(parentDataset, "/") + "/"
	var tasks []*SnapshotTask
	for _, task := range m.SnapshotTasks {
		if strings.HasPrefix(task.Dataset, prefix) {
			tasks = append(tasks, task)
		}
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	return tasks, nil
}

func (m *MockClient) SnapshotTaskUpdate(ctx context.Context, id int, params *SnapshotTaskCreateParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	task, ok := m.SnapshotTasks[id]
	if !ok {
		return notFoundAPIError("snapshot task not found")
	}
	task.Dataset = params.Dataset
	task.Recursive = params.Recursive
	task.NamingSchema = params.NamingSchema
	task.Schedule = params.Schedule
	task.LifetimeValue = params.LifetimeValue
	task.LifetimeUnit = params.LifetimeUnit
	task.Enabled = params.Enabled
	task.AllowEmpty = params.AllowEmpty
	return nil
}

func (m *MockClient) SnapshotTaskDelete(ctx context.Context, id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return m.InjectError
	}
	m.SnapshotTaskDeleteCalls = append(m.SnapshotTaskDeleteCalls, id)
	delete(m.SnapshotTasks, id)
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
	return nil, notFoundAPIError("snapshot not found")
}

func (m *MockClient) SnapshotList(ctx context.Context, dataset string) ([]*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.snapshotListCalls++
	if m.FailSnapshotListAfter != 0 && m.snapshotListCalls > m.FailSnapshotListAfter {
		return nil, &APIError{Code: -1, Message: "injected snapshot list failure"}
	}

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
	m.SnapshotSetCalls++

	if m.InjectError != nil {
		return m.InjectError
	}
	if m.SimulateUpdateNoOp {
		return nil
	}
	snap, ok := m.Snapshots[snapshotID]
	if !ok {
		return notFoundAPIError("snapshot not found")
	}
	snap.UserProperties[key] = UserProperty{Value: value, Source: "local"}
	return nil
}

func (m *MockClient) SnapshotRemoveUserProperties(ctx context.Context, snapshotID string, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SnapshotRemoveCalls++

	if m.InjectError != nil {
		return m.InjectError
	}
	if m.SimulateUpdateNoOp {
		return nil
	}
	snap, ok := m.Snapshots[snapshotID]
	if !ok {
		return notFoundAPIError("snapshot not found")
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
	if existing, exists := m.Datasets[newDatasetName]; exists {
		return &ErrDatasetDestinationExists{
			Destination: newDatasetName, ExpectedOrigin: snapshotID,
			ActualOrigin: datasetPropertyString(existing.Origin),
		}
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
			clone.Volsize = source.Volsize
			clone.Refquota = source.Refquota
			clone.Available = source.Available
			// A ZFS clone gets its own mountpoint, not the source's.
			if source.Type != "VOLUME" {
				clone.Mountpoint = "/mnt/" + strings.TrimPrefix(newDatasetName, "/")
			}
			// Model ZFS clone inheritance: the clone inherits the source dataset's
			// user properties, but their source is the ORIGIN SNAPSHOT NAME rather
			// than "local". Live TrueNAS 26.0 reports clone-inherited user
			// properties this way, which is exactly why local-vs-inherited checks
			// must compare source == "local" and must not adopt these values.
			for key, property := range source.UserProperties {
				clone.UserProperties[key] = UserProperty{Value: property.Value, Source: snapshotID}
			}
			// P-7 ENCRYPTION INHERITANCE (probed): a clone of an encrypted dataset
			// comes out encrypted:true with encryption_root == the ORIGIN, NOT
			// itself. It shares the origin's key: it is not independently keyed, it
			// cannot be re-keyed, and locking the origin locks the clone. Modeling
			// this is what makes an encrypted-content-source test possible at all —
			// without it, a clone looked plaintext to every test and the driver's
			// missing source-side guard was invisible.
			if source.Encrypted {
				clone.Encrypted = true
				clone.EncryptionAlgorithm = source.EncryptionAlgorithm
				clone.KeyFormat = source.KeyFormat
				clone.EncryptionRoot = source.EncryptionRoot
				if clone.EncryptionRoot == "" {
					clone.EncryptionRoot = source.Name
				}
				clone.Locked = source.Locked
				clone.KeyLoaded = source.KeyLoaded
				if clone.Locked {
					clone.Mountpoint = ""
				}
			}
		}
	}
	m.Datasets[newDatasetName] = clone
	return nil
}

func (m *MockClient) CopyDatasetFromSnapshotLocal(
	ctx context.Context,
	sourceDataset, snapshotShortName, targetDataset string,
) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.InjectError != nil {
		return UnknownReplicationJobID, m.InjectError
	}
	if _, exists := m.Datasets[targetDataset]; exists {
		return UnknownReplicationJobID, &ErrDatasetDestinationExists{Destination: targetDataset}
	}
	snapshotID := sourceDataset + "@" + snapshotShortName
	if _, exists := m.Snapshots[snapshotID]; !exists {
		return UnknownReplicationJobID, notFoundAPIError("snapshot not found")
	}
	source, exists := m.Datasets[sourceDataset]
	if !exists {
		return UnknownReplicationJobID, notFoundAPIError("source dataset not found")
	}
	jobID := m.nextReplicationJobID
	m.nextReplicationJobID++

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
	// A received dataset takes the destination parent's encryption, exactly like
	// any other create under that parent (P-10 inheritance). Whether a send from
	// an ENCRYPTED source is raw (an encrypted, independently-rooted target) or
	// plain is UNPROBED as of 2026-08-02 — drill step 6b settles it — so the mock
	// deliberately models only the inheritance half, which is probed.
	m.applyInheritedEncryptionLocked(copy)
	m.Datasets[targetDataset] = copy
	m.ReplicationJobs[jobID] = &ReplicationJob{
		ID:             jobID,
		Method:         ReplicationRunOnetimeMethod,
		State:          "SUCCESS",
		SourceDatasets: []string{sourceDataset},
		TargetDataset:  targetDataset,
	}
	return jobID, nil
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
		ID:           id,
		Path:         params.Path,
		Networks:     append([]string(nil), params.Networks...),
		Hosts:        append([]string(nil), params.Hosts...),
		Comment:      params.Comment,
		Ro:           params.Ro,
		MaprootUser:  params.MaprootUser,
		MaprootGroup: params.MaprootGroup,
		MapallUser:   params.MapallUser,
		MapallGroup:  params.MapallGroup,
		Enabled:      params.Enabled,

		Security:        append([]string(nil), params.Security...),
		ExposeSnapshots: params.ExposeSnapshots,
	}
	m.NFSShares[id] = share
	m.NFSShareCreateParams = append(m.NFSShareCreateParams, *params)
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
	return nil, notFoundAPIError("share not found")
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.NFSShareUpdateParams = append(m.NFSShareUpdateParams, params)
	share := m.NFSShares[id]
	if share == nil {
		return nil, notFoundAPIError("share not found")
	}
	if hosts, ok := params["hosts"].([]string); ok {
		share.Hosts = append([]string(nil), hosts...)
	}
	if networks, ok := params["networks"].([]string); ok {
		share.Networks = append([]string(nil), networks...)
	}
	if enabled, ok := params["enabled"].(bool); ok {
		share.Enabled = enabled
	}
	return share, nil
}

// NFSServiceConfigValue is the global nfs.config the mock reports. Nil means
// "backend did not answer" and NFSServiceConfig returns an error.
func (m *MockClient) NFSServiceConfig(ctx context.Context) (*NFSServiceConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.NFSServiceConfigCalls++
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	if m.NFSServiceConfigValue == nil {
		return &NFSServiceConfig{Protocols: []string{NFSProtocolV3, NFSProtocolV4}, ProtocolsComplete: true, Servers: 64}, nil
	}
	clone := *m.NFSServiceConfigValue
	clone.Protocols = append([]string(nil), m.NFSServiceConfigValue.Protocols...)
	return &clone, nil
}

func (m *MockClient) NFSServiceUpdate(ctx context.Context, params map[string]interface{}) (*NFSServiceConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.NFSServiceUpdateCalls = append(m.NFSServiceUpdateCalls, params)
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	if m.NFSServiceConfigValue == nil {
		m.NFSServiceConfigValue = &NFSServiceConfig{Protocols: []string{NFSProtocolV3, NFSProtocolV4}, ProtocolsComplete: true, Servers: 64}
	}
	if protocols, ok := params["protocols"].([]string); ok {
		m.NFSServiceConfigValue.Protocols = append([]string(nil), protocols...)
	}
	clone := *m.NFSServiceConfigValue
	clone.Protocols = append([]string(nil), m.NFSServiceConfigValue.Protocols...)
	return &clone, nil
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

// SystemTimezone returns the mock NAS's civil timezone. SystemTimezoneName
// selects it (default UTC) and SystemTimezoneErr injects an unreadable-zone
// failure, so a test can model both a non-UTC NAS and the fail-closed path.
func (m *MockClient) SystemTimezone(ctx context.Context) (*time.Location, error) {
	m.mu.RLock()
	name, injected := m.SystemTimezoneName, m.SystemTimezoneErr
	m.mu.RUnlock()
	if injected != nil {
		return nil, injected
	}
	if name == "" {
		name = "UTC"
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("mock NAS timezone %q is not loadable: %w", name, err)
	}
	return loc, nil
}

func (m *MockClient) CheckNVMeoFSupport(ctx context.Context) error {
	// Mock always supports NVMe-oF (returns 25.10+)
	return nil
}

// iSCSI methods
func (m *MockClient) ISCSITargetCreate(ctx context.Context, name, alias, mode string, groups []ISCSITargetGroup, opts ...ISCSITargetCreateOptions) (*ISCSITarget, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.RejectEmptyISCSITargetGroups && len(groups) == 0 {
		return nil, fmt.Errorf("iSCSI target requires at least one portal group")
	}

	id := len(m.ISCSITargets) + 1
	target := &ISCSITarget{ID: id, Name: name, Alias: alias, Mode: mode, Groups: groups}
	if len(opts) > 0 {
		target.QueuedCommands = opts[0].QueuedCommands
		target.AuthNetworks = append([]string(nil), opts[0].AuthNetworks...)
	}
	m.ISCSITargets[id] = target
	return target, nil
}
func (m *MockClient) ISCSITargetUpdate(ctx context.Context, id int, groups []ISCSITargetGroup) (*ISCSITarget, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.RejectEmptyISCSITargetGroups && len(groups) == 0 {
		return nil, fmt.Errorf("iSCSI target requires at least one portal group")
	}
	target := m.ISCSITargets[id]
	if target == nil {
		return nil, notFoundAPIError("iSCSI target not found")
	}
	target.Groups = append([]ISCSITargetGroup(nil), groups...)
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
	return nil, notFoundAPIError("not found")
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
func (m *MockClient) ISCSITargetList(ctx context.Context) ([]*ISCSITarget, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []*ISCSITarget
	for _, t := range m.ISCSITargets {
		list = append(list, t)
	}
	return list, nil
}
func (m *MockClient) ISCSIExtentCreate(ctx context.Context, name, diskPath, comment string, blocksize int, physicalBlocksize bool, rpm string, opts ...ISCSIExtentCreateOptions) (*ISCSIExtent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := len(m.ISCSIExtents) + 1
	// The mock mirrors a backend that DOES report pblocksize / insecure_tpc / ro
	// on iscsi.extent.query (the normal case). The pointer model exists so the
	// abnormal case — a response that omits one — stays distinguishable from a
	// reported false; see parseISCSIExtent.
	pblocksize, insecureTpc, readOnly := physicalBlocksize, true, false
	ext := &ISCSIExtent{
		ID:          id,
		Name:        name,
		Disk:        diskPath,
		Comment:     comment,
		Blocksize:   blocksize,
		Pblocksize:  &pblocksize,
		Rpm:         rpm,
		InsecureTpc: &insecureTpc,
		Ro:          &readOnly,
		Enabled:     true,
	}
	if len(opts) > 0 {
		opt := opts[0]
		if opt.InsecureTpc != nil {
			insecureTpc = *opt.InsecureTpc
		}
		if opt.ReadOnly != nil {
			readOnly = *opt.ReadOnly
		}
		if opt.AvailThreshold != nil {
			threshold := *opt.AvailThreshold
			ext.AvailThreshold = &threshold
		}
		ext.Serial = opt.Serial
	}
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
	return nil, notFoundAPIError("not found")
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
func (m *MockClient) ISCSIExtentList(ctx context.Context) ([]*ISCSIExtent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []*ISCSIExtent
	for _, e := range m.ISCSIExtents {
		list = append(list, e)
	}
	return list, nil
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
		return nil, notFoundAPIError(fmt.Sprintf("NVMe-oF subsystem ID %d not found", subsysID))
	}
	hostFound := false
	for _, host := range m.NVMeHosts {
		if host.ID == hostID {
			hostFound = true
			break
		}
	}
	if !hostFound {
		return nil, notFoundAPIError(fmt.Sprintf("NVMe-oF host ID %d not found", hostID))
	}
	for _, association := range m.NVMeHostSubsystems {
		if association.HostID == hostID && association.SubsysID == subsysID {
			return association, nil
		}
	}
	subsys.Hosts = append(subsys.Hosts, hostID)
	id := len(m.NVMeHostSubsystems) + 1
	for m.NVMeHostSubsystems[id] != nil {
		id++
	}
	association := &NVMeoFHostSubsys{ID: id, HostID: hostID, HostNQN: mockNVMeHostNQN(m.NVMeHosts, hostID), SubsysID: subsysID}
	m.NVMeHostSubsystems[id] = association
	return association, nil
}

func (m *MockClient) NVMeoFHostSubsysFind(ctx context.Context, hostID, subsysID int) (*NVMeoFHostSubsys, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	for _, association := range m.NVMeHostSubsystems {
		if association.HostID == hostID && association.SubsysID == subsysID {
			return association, nil
		}
	}
	return nil, nil
}

func mockNVMeHostNQN(hosts map[string]*NVMeoFHost, id int) string {
	for nqn, host := range hosts {
		if host.ID == id {
			return nqn
		}
	}
	return ""
}

func (m *MockClient) NVMeoFHostSubsysListBySubsystem(ctx context.Context, subsysID int) ([]*NVMeoFHostSubsys, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	associations := make([]*NVMeoFHostSubsys, 0)
	for _, association := range m.NVMeHostSubsystems {
		if association.SubsysID == subsysID {
			copy := *association
			if m.EmptyNVMeHostNQN {
				copy.HostNQN = ""
			}
			associations = append(associations, &copy)
		}
	}
	return associations, nil
}

func (m *MockClient) NVMeoFHostSubsysDelete(ctx context.Context, id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	association := m.NVMeHostSubsystems[id]
	if association == nil {
		return nil
	}
	if subsystem := m.NVMeSubsystems[association.SubsysID]; subsystem != nil {
		hosts := subsystem.Hosts[:0]
		for _, hostID := range subsystem.Hosts {
			if hostID != association.HostID {
				hosts = append(hosts, hostID)
			}
		}
		subsystem.Hosts = hosts
	}
	delete(m.NVMeHostSubsystems, id)
	return nil
}

func (m *MockClient) NVMeoFSubsystemCreate(ctx context.Context, name string, allowAnyHost bool, hostIDs []int, opts ...NVMeoFSubsystemCreateOptions) (*NVMeoFSubsystem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InjectError != nil {
		return nil, m.InjectError
	}
	if !allowAnyHost {
		for _, hostID := range hostIDs {
			found := false
			for _, host := range m.NVMeHosts {
				if host.ID == hostID {
					found = true
					break
				}
			}
			if !found {
				return nil, notFoundAPIError(fmt.Sprintf("NVMe-oF host ID %d not found", hostID))
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
	if len(opts) > 0 {
		if opts[0].QidMax != nil {
			qidMax := *opts[0].QidMax
			sub.QidMax = &qidMax
		}
		if opts[0].PiEnable != nil {
			piEnable := *opts[0].PiEnable
			sub.PiEnable = &piEnable
		}
	}
	m.NVMeSubsystems[id] = sub
	for _, hostID := range hostIDs {
		associationID := len(m.NVMeHostSubsystems) + 1
		for m.NVMeHostSubsystems[associationID] != nil {
			associationID++
		}
		m.NVMeHostSubsystems[associationID] = &NVMeoFHostSubsys{ID: associationID, HostID: hostID, HostNQN: mockNVMeHostNQN(m.NVMeHosts, hostID), SubsysID: id}
	}
	return sub, nil
}

func (m *MockClient) NVMeoFSubsystemUpdateAllowAnyHost(ctx context.Context, id int, allowAnyHost bool) (*NVMeoFSubsystem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	subsystem := m.NVMeSubsystems[id]
	if subsystem == nil {
		return nil, notFoundAPIError("not found")
	}
	subsystem.AllowAnyHost = allowAnyHost
	return subsystem, nil
}
func (m *MockClient) NVMeoFSubsystemDelete(ctx context.Context, id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.NVMeSubsystems, id)
	for associationID, association := range m.NVMeHostSubsystems {
		if association.SubsysID == id {
			delete(m.NVMeHostSubsystems, associationID)
		}
	}
	return nil
}
func (m *MockClient) NVMeoFSubsystemGet(ctx context.Context, id int) (*NVMeoFSubsystem, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if s, ok := m.NVMeSubsystems[id]; ok {
		return s, nil
	}
	return nil, notFoundAPIError("not found")
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
	return nil, notFoundAPIError("not found")
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
func (m *MockClient) NVMeoFNamespaceListBySubsystem(ctx context.Context, subsysID int) ([]*NVMeoFNamespace, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var list []*NVMeoFNamespace
	for _, n := range m.NVMeNamespaces {
		if n.SubsystemID == subsysID {
			list = append(list, n)
		}
	}
	return list, nil
}
func (m *MockClient) NVMeoFNamespaceList(ctx context.Context) ([]*NVMeoFNamespace, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]*NVMeoFNamespace, 0, len(m.NVMeNamespaces))
	for _, n := range m.NVMeNamespaces {
		list = append(list, n)
	}
	return list, nil
}
func (m *MockClient) NVMeoFPortList(ctx context.Context) ([]*NVMeoFPort, error) {
	return []*NVMeoFPort{{ID: 1, Transport: "TCP", Address: "0.0.0.0", Port: 4420}}, nil
}
func (m *MockClient) NVMeoFPortCreate(ctx context.Context, transport, address string, port int, opts ...NVMeoFPortCreateOptions) (*NVMeoFPort, error) {
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
func (m *MockClient) NVMeoFGetOrCreatePort(ctx context.Context, transport, address string, port int, opts ...NVMeoFPortCreateOptions) (*NVMeoFPort, error) {
	return &NVMeoFPort{ID: 1, Transport: "TCP", Address: address, Port: port}, nil
}
func (m *MockClient) InvalidateNVMeoFPort(transport, address string, port int) {}
func (m *MockClient) NVMeoFGetTransportAddresses(ctx context.Context, transport string) ([]string, error) {
	return []string{"0.0.0.0"}, nil
}
