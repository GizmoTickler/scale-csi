package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"github.com/GizmoTickler/scale-csi/pkg/util"
)

// ownNVMeSessions gives d a temporary session registry recording nqns as
// connected by this plugin, the precondition for session GC to collect them.
func ownNVMeSessions(t *testing.T, d *Driver, nqns ...string) *sessionRegistry {
	t.Helper()
	reg, err := newSessionRegistry(filepath.Join(t.TempDir(), "sessions", "nvmeof"))
	require.NoError(t, err)
	for _, nqn := range nqns {
		require.NoError(t, reg.record(nqn))
	}
	d.nvmeSessions = reg
	return reg
}

func installNVMeGCFakes(t *testing.T, sessions []util.NVMeoFSessionInfo, staged map[string]string, nvmeInfo map[string]string) *[]string {
	t.Helper()
	originalList := gcListNVMeoFSessions
	originalDisconnect := gcDisconnectNVMeoF
	originalMounted := getMountedBlockDevices
	originalStaged := getStagedBlockDevices
	originalNVMeInfo := getNVMeInfoFromDevice
	t.Cleanup(func() {
		gcListNVMeoFSessions = originalList
		gcDisconnectNVMeoF = originalDisconnect
		getMountedBlockDevices = originalMounted
		getStagedBlockDevices = originalStaged
		getNVMeInfoFromDevice = originalNVMeInfo
	})
	gcListNVMeoFSessions = func() ([]util.NVMeoFSessionInfo, error) { return sessions, nil }
	var disconnected []string
	gcDisconnectNVMeoF = func(nqn string) error {
		disconnected = append(disconnected, nqn)
		return nil
	}
	getMountedBlockDevices = func() (map[string]string, error) { return map[string]string{}, nil }
	if staged == nil {
		staged = map[string]string{}
	}
	getStagedBlockDevices = func() (map[string]string, error) { return staged, nil }
	getNVMeInfoFromDevice = func(device string) (string, error) {
		if nqn, ok := nvmeInfo[device]; ok {
			return nqn, nil
		}
		return "", os.ErrNotExist
	}
	return &disconnected
}

func sessionAt(nqn, addr string) util.NVMeoFSessionInfo {
	a := "traddr=" + addr + ",trsvcid=4420"
	return util.NVMeoFSessionInfo{NQN: nqn, Address: a, Addresses: []string{a}}
}

// The regression: a session to this driver's own target portals that the
// plugin did not connect (an administrator's or a benchmark's) was treated as
// a leaked CSI session and disconnected after the grace period.
func TestNVMeoFGCLeavesSessionsThisPluginDidNotConnect(t *testing.T) {
	d := &Driver{config: &Config{NVMeoF: NVMeoFConfig{TransportAddress: "192.0.2.20"}}}
	const foreign = "nqn.2011-06.com.truenas:uuid:0000:manual-test"
	ownNVMeSessions(t, d) // registry present, but foreign was never recorded
	disconnected := installNVMeGCFakes(t, []util.NVMeoFSessionInfo{sessionAt(foreign, "192.0.2.20")}, nil, nil)
	d.orphanedNVMeSessionsSeen.Store(foreign, time.Now().Add(-time.Hour))

	d.gcNVMeoFSessions(context.Background(), 0, false)

	assert.Empty(t, *disconnected, "a session this plugin never connected must not be garbage collected")
}

func TestNVMeoFGCCollectsOwnOrphanAndForgetsIt(t *testing.T) {
	d := &Driver{config: &Config{NVMeoF: NVMeoFConfig{TransportAddress: "192.0.2.20"}}}
	const own = "nqn.2011-06.com.truenas:uuid:0000:pvc-own"
	reg := ownNVMeSessions(t, d, own)
	disconnected := installNVMeGCFakes(t, []util.NVMeoFSessionInfo{sessionAt(own, "192.0.2.20")}, nil, nil)
	d.orphanedNVMeSessionsSeen.Store(own, time.Now().Add(-time.Hour))

	d.gcNVMeoFSessions(context.Background(), 0, false)

	assert.Equal(t, []string{own}, *disconnected)
	assert.False(t, reg.has(own), "a collected session's record must be forgotten")
}

func TestNVMeoFGCRecordsStagedSessions(t *testing.T) {
	d := &Driver{config: &Config{NVMeoF: NVMeoFConfig{TransportAddress: "192.0.2.20"}}}
	const staged = "nqn.2011-06.com.truenas:uuid:0000:pvc-staged"
	reg := ownNVMeSessions(t, d)
	disconnected := installNVMeGCFakes(t,
		[]util.NVMeoFSessionInfo{sessionAt(staged, "192.0.2.20")},
		map[string]string{"/dev/nvme3n1": "/staging/globalmount"},
		map[string]string{"/dev/nvme3n1": staged})

	d.gcNVMeoFSessions(context.Background(), time.Hour, false)

	assert.Empty(t, *disconnected)
	assert.True(t, reg.has(staged), "a staged volume's session is this plugin's and must be recorded")
}

func TestNVMeoFGCDryRunDoesNotTouchRegistry(t *testing.T) {
	d := &Driver{config: &Config{NVMeoF: NVMeoFConfig{TransportAddress: "192.0.2.20"}}}
	const own, staged = "nqn.test:own", "nqn.test:staged"
	reg := ownNVMeSessions(t, d, own)
	disconnected := installNVMeGCFakes(t,
		[]util.NVMeoFSessionInfo{sessionAt(own, "192.0.2.20"), sessionAt(staged, "192.0.2.20")},
		map[string]string{"/dev/nvme3n1": "/staging/globalmount"},
		map[string]string{"/dev/nvme3n1": staged})
	d.orphanedNVMeSessionsSeen.Store(own, time.Now().Add(-time.Hour))

	d.gcNVMeoFSessions(context.Background(), 0, true)

	assert.Empty(t, *disconnected)
	assert.True(t, reg.has(own))
	assert.False(t, reg.has(staged), "dry run must not write the registry")
}

func TestNVMeoFGCSkipsWithoutRegistry(t *testing.T) {
	d := &Driver{config: &Config{NVMeoF: NVMeoFConfig{TransportAddress: "192.0.2.20"}}}
	const nqn = "nqn.test:unprovable"
	disconnected := installNVMeGCFakes(t, []util.NVMeoFSessionInfo{sessionAt(nqn, "192.0.2.20")}, nil, nil)
	d.orphanedNVMeSessionsSeen.Store(nqn, time.Now().Add(-time.Hour))

	d.gcNVMeoFSessions(context.Background(), 0, false)

	assert.Empty(t, *disconnected, "without a registry ownership cannot be proven, so nothing is disconnected")
}

func TestPruneSessionRegistry(t *testing.T) {
	reg, err := newSessionRegistry(filepath.Join(t.TempDir(), "reg"))
	require.NoError(t, err)
	for _, nqn := range []string{"gone-old", "gone-young", "live", "staged"} {
		require.NoError(t, reg.record(nqn))
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, nqn := range []string{"gone-old", "live", "staged"} {
		require.NoError(t, os.Chtimes(reg.path(nqn), old, old))
	}

	pruneSessionRegistry(reg, map[string]struct{}{"live": {}}, map[string]struct{}{"staged": {}}, time.Hour)

	assert.False(t, reg.has("gone-old"), "an old record with no session and no staged volume is pruned")
	assert.True(t, reg.has("gone-young"), "a young record may be a stage in progress")
	assert.True(t, reg.has("live"))
	assert.True(t, reg.has("staged"))
}

func TestSessionRegistryRoundTripsAndForgets(t *testing.T) {
	reg, err := newSessionRegistry(filepath.Join(t.TempDir(), "reg"))
	require.NoError(t, err)
	const nqn = "nqn.2011-06.com.truenas:uuid:ab/cd:pvc-1"
	require.NoError(t, reg.record(nqn))
	require.NoError(t, reg.record(nqn), "recording twice is fine")
	entries, err := reg.entries()
	require.NoError(t, err)
	assert.Contains(t, entries, nqn)
	require.NoError(t, reg.forget(nqn))
	require.NoError(t, reg.forget(nqn), "forgetting a missing record is fine")
	assert.False(t, reg.has(nqn))
}

func TestNVMeSessionRegistryDir(t *testing.T) {
	assert.Equal(t, "/csi/sessions/nvmeof", nvmeSessionRegistryDir("unix:///csi/csi.sock"))
	assert.Equal(t, "/var/lib/kubelet/plugins/x/sessions/nvmeof", nvmeSessionRegistryDir("unix:/var/lib/kubelet/plugins/x/csi.sock"))
	assert.Equal(t, "", nvmeSessionRegistryDir("tcp://127.0.0.1:10000"))
	_, err := newSessionRegistry(nvmeSessionRegistryDir("tcp://127.0.0.1:10000"))
	assert.Error(t, err)
}

func TestStageNVMeoFRecordsSessionBeforeConnecting(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "nvme")
	originalConnect := nvmeConnectWithSubsystems
	originalFormat := nodeFormatAndMount
	t.Cleanup(func() {
		nvmeConnectWithSubsystems = originalConnect
		nodeFormatAndMount = originalFormat
	})
	nodeFormatAndMount = func(context.Context, string, string, string, []string) error { return nil }
	d := newTestNodeDriver(ShareTypeNVMeoF)
	reg := ownNVMeSessions(t, d)
	const nqn = "nqn.test:stage-records"
	recordedAtConnect := false
	nvmeConnectWithSubsystems = func(_ context.Context, gotNQN, _ string, _ *util.NVMeoFConnectOptions, _ []util.NVMeSubsystem) (string, error) {
		recordedAtConnect = reg.has(gotNQN)
		return "/dev/nvme9n1", nil
	}
	capability := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}}}

	err := d.stageNVMeoFVolume(context.Background(), map[string]string{
		"nqn": nqn, "transport": "tcp", "address": "192.0.2.20", "port": "4420",
	}, filepath.Join(t.TempDir(), "stage"), capability)

	require.NoError(t, err)
	assert.True(t, recordedAtConnect, "the session must be recorded before the connect, so a crash cannot leave it unrecorded")
}

func TestSessionRegistryNilAndEmptyIDContract(t *testing.T) {
	var nilReg *sessionRegistry
	assert.Error(t, nilReg.record("nqn.test:x"), "a missing registry cannot record")
	assert.NoError(t, nilReg.forget("nqn.test:x"))
	assert.False(t, nilReg.has("nqn.test:x"), "a missing registry proves nothing")
	_, err := nilReg.entries()
	assert.Error(t, err)

	reg, err := newSessionRegistry(filepath.Join(t.TempDir(), "reg"))
	require.NoError(t, err)
	assert.Error(t, reg.record(""))
	assert.NoError(t, reg.forget(""))
	assert.False(t, reg.has(""))

	_, err = newSessionRegistry("relative/dir")
	assert.Error(t, err, "a relative directory would follow the plugin's working directory")
}

func TestSessionRegistryCreatesItsDirectoryLazily(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sessions", "nvmeof")
	reg, err := newSessionRegistry(dir)
	require.NoError(t, err)
	_, statErr := os.Stat(dir)
	assert.True(t, os.IsNotExist(statErr), "constructing a registry must not touch the filesystem")
	assert.False(t, reg.has("nqn.test:x"))
	entries, err := reg.entries()
	require.NoError(t, err, "a registry that has recorded nothing yet is empty, not broken")
	assert.Empty(t, entries)

	require.NoError(t, reg.record("nqn.test:x"))
	assert.True(t, reg.has("nqn.test:x"))
}

func TestSessionRegistryEntriesIgnoreForeignFiles(t *testing.T) {
	reg, err := newSessionRegistry(filepath.Join(t.TempDir(), "reg"))
	require.NoError(t, err)
	require.NoError(t, reg.record("nqn.test:real"))
	require.NoError(t, os.Mkdir(filepath.Join(reg.dir, "subdir"), 0o700))
	for _, name := range []string{"6e716e.tmp", "not-hex", "zz"} {
		require.NoError(t, os.WriteFile(filepath.Join(reg.dir, name), nil, 0o600))
	}

	entries, err := reg.entries()
	require.NoError(t, err)
	assert.Equal(t, []string{"nqn.test:real"}, keysOf(entries), "leftover temp files, stray names and directories are not records")
}

func TestSessionRegistryFailsClosedWhenItsDirectoryIsAFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "reg")
	require.NoError(t, os.WriteFile(dir, []byte("not a directory"), 0o600))
	reg, err := newSessionRegistry(dir)
	require.NoError(t, err)

	assert.Error(t, reg.record("nqn.test:x"))
	assert.False(t, reg.has("nqn.test:x"))
	_, err = reg.entries()
	assert.Error(t, err, "an unreadable registry is an error, not an empty one")
}

func TestStageNVMeoFSucceedsWhenTheSessionCannotBeRecorded(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "nvme")
	originalConnect := nvmeConnectWithSubsystems
	originalFormat := nodeFormatAndMount
	t.Cleanup(func() {
		nvmeConnectWithSubsystems = originalConnect
		nodeFormatAndMount = originalFormat
	})
	nodeFormatAndMount = func(context.Context, string, string, string, []string) error { return nil }
	nvmeConnectWithSubsystems = func(context.Context, string, string, *util.NVMeoFConnectOptions, []util.NVMeSubsystem) (string, error) {
		return "/dev/nvme9n1", nil
	}
	d := newTestNodeDriver(ShareTypeNVMeoF)
	dir := filepath.Join(t.TempDir(), "reg")
	require.NoError(t, os.WriteFile(dir, nil, 0o600))
	reg, err := newSessionRegistry(dir)
	require.NoError(t, err)
	d.nvmeSessions = reg
	capability := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}}}

	err = d.stageNVMeoFVolume(context.Background(), map[string]string{
		"nqn": "nqn.test:unrecordable", "transport": "tcp", "address": "192.0.2.20", "port": "4420",
	}, filepath.Join(t.TempDir(), "stage"), capability)

	require.NoError(t, err, "failing to record must not fail the stage: the session is merely never collected")
	assert.False(t, reg.has("nqn.test:unrecordable"))
}

func TestNVMeoFGCWithAnUnwritableRegistryDisconnectsNothing(t *testing.T) {
	d := &Driver{config: &Config{NVMeoF: NVMeoFConfig{TransportAddress: "192.0.2.20"}}}
	dir := filepath.Join(t.TempDir(), "reg")
	require.NoError(t, os.WriteFile(dir, nil, 0o600))
	reg, err := newSessionRegistry(dir)
	require.NoError(t, err)
	d.nvmeSessions = reg
	const orphan, staged = "nqn.test:orphan", "nqn.test:staged"
	disconnected := installNVMeGCFakes(t,
		[]util.NVMeoFSessionInfo{sessionAt(orphan, "192.0.2.20"), sessionAt(staged, "192.0.2.20")},
		map[string]string{"/dev/nvme3n1": "/staging/globalmount"},
		map[string]string{"/dev/nvme3n1": staged})
	d.orphanedNVMeSessionsSeen.Store(orphan, time.Now().Add(-time.Hour))

	d.gcNVMeoFSessions(context.Background(), 0, false)

	assert.Empty(t, *disconnected, "with no readable ownership record nothing may be disconnected")
}

func TestNVMeoFGCStillCountsADisconnectWhoseRecordCannotBeRemoved(t *testing.T) {
	d := &Driver{config: &Config{NVMeoF: NVMeoFConfig{TransportAddress: "192.0.2.20"}}}
	const own = "nqn.test:stuck-record"
	reg := ownNVMeSessions(t, d)
	// A non-empty directory where the record file should be: Stat succeeds
	// (owned) but Remove fails, as an unexpected filesystem state would.
	require.NoError(t, os.MkdirAll(filepath.Join(reg.path(own), "x"), 0o700))
	disconnected := installNVMeGCFakes(t, []util.NVMeoFSessionInfo{sessionAt(own, "192.0.2.20")}, nil, nil)
	d.orphanedNVMeSessionsSeen.Store(own, time.Now().Add(-time.Hour))

	d.gcNVMeoFSessions(context.Background(), 0, false)

	assert.Equal(t, []string{own}, *disconnected, "a disconnect that happened is not undone by a failed record removal")
}

func TestUnstageOrphanCleanupForgetsTheRecordOnlyAfterDisconnecting(t *testing.T) {
	installFakeNodeCommands(t, "nvme")
	t.Setenv("FAKE_NODE_NVME_LIST_SUBSYS_OUTPUT", `{"Subsystems":[{"NQN":"nqn.test:test-vol","Name":"nvme2","Paths":[{"Name":"nvme2"}]}]}`)
	originalDisconnect := nodeNVMeDisconnect
	t.Cleanup(func() { nodeNVMeDisconnect = originalDisconnect })

	t.Run("disconnect fails: record kept", func(t *testing.T) {
		nodeNVMeDisconnect = func(context.Context, string) error { return os.ErrPermission }
		d := newTestNodeDriver(ShareTypeNVMeoF)
		reg := ownNVMeSessions(t, d, "nqn.test:test-vol")
		err := d.cleanupOrphanedSessionByVolumeID(context.Background(), "test-vol", ShareTypeNVMeoF)
		require.Error(t, err)
		assert.True(t, reg.has("nqn.test:test-vol"), "a session still connected stays this plugin's")
	})
	t.Run("disconnect succeeds: record forgotten", func(t *testing.T) {
		nodeNVMeDisconnect = func(context.Context, string) error { return nil }
		d := newTestNodeDriver(ShareTypeNVMeoF)
		reg := ownNVMeSessions(t, d, "nqn.test:test-vol")
		require.NoError(t, d.cleanupOrphanedSessionByVolumeID(context.Background(), "test-vol", ShareTypeNVMeoF))
		assert.False(t, reg.has("nqn.test:test-vol"))
	})
}

func TestNewDriverNodeSetsUpTheSessionRegistryBesideItsSocket(t *testing.T) {
	originalClient := newTrueNASClient
	t.Cleanup(func() { newTrueNASClient = originalClient })
	newTrueNASClient = func(*truenas.ClientConfig) (truenas.ClientInterface, error) { return truenas.NewMockClient(), nil }
	cfg := func() *Config {
		return &Config{
			TrueNAS: TrueNASConfig{Host: "unreachable.example.test", RequestTimeout: 1, ConnectTimeout: 1, WriteTimeout: 1, MaxConcurrentRequests: 1},
			ZFS:     ZFSConfig{DatasetParentName: "tank/csi"},
			Node:    NodeConfig{Topology: TopologyConfig{Enabled: true}},
		}
	}
	sockDir := t.TempDir()

	drv, err := NewDriver(&DriverConfig{Name: "csi.scale.io", Version: "test", NodeID: "node-a",
		Endpoint: "unix://" + filepath.Join(sockDir, "csi.sock"), RunNode: true, Config: cfg()})
	require.NoError(t, err)
	require.NotNil(t, drv.nvmeSessions)
	assert.Equal(t, filepath.Join(sockDir, "sessions", "nvmeof"), drv.nvmeSessions.dir)
	_, statErr := os.Stat(drv.nvmeSessions.dir)
	assert.True(t, os.IsNotExist(statErr), "driver construction creates nothing on disk")
	drv.Stop()

	drv, err = NewDriver(&DriverConfig{Name: "csi.scale.io", Version: "test", NodeID: "node-a",
		Endpoint: "tcp://127.0.0.1:10000", RunNode: true, Config: cfg()})
	require.NoError(t, err)
	assert.Nil(t, drv.nvmeSessions, "without a socket directory there is no registry, and NVMe-oF GC stays off")
	drv.Stop()
}

func keysOf(m map[string]time.Time) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
