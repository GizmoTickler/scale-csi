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
