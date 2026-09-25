package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

const (
	testUblkHostNQN = "nqn.2014-08.org.nvmexpress:uuid:0A1B2C3D-4E5F-6071-8293-A4B5C6D7E8F9"
	testUblkHostID  = "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9"
	testUblkVolume  = "pvc-ublk-1"
	testUblkNQN     = "nqn.2011-06.com.example:pvc-ublk-1"
)

// fakeNodeUblkDaemon is an in-memory nvmeublkd behind the newNVMeUblkDaemon
// seam. Attached devices are regular files in devDir so the staging symlink
// resolves the way a real /dev/ublkbN would.
type fakeNodeUblkDaemon struct {
	t         *testing.T
	mu        sync.Mutex
	devDir    string
	nextID    int
	devices   map[string]util.NVMeUblkDevice
	attaches  []util.NVMeUblkAttachRequest
	detaches  []string
	lists     int
	sockets   []string
	attachErr error
	detachErr error
	listErr   error
	pathsDown bool
}

func installFakeNodeUblkDaemon(t *testing.T) *fakeNodeUblkDaemon {
	t.Helper()
	fake := &fakeNodeUblkDaemon{t: t, devDir: t.TempDir(), devices: map[string]util.NVMeUblkDevice{}}
	originalDaemon, originalDevDir := newNVMeUblkDaemon, nodeNVMeUblkDevDir
	t.Cleanup(func() { newNVMeUblkDaemon, nodeNVMeUblkDevDir = originalDaemon, originalDevDir })
	nodeNVMeUblkDevDir = fake.devDir
	newNVMeUblkDaemon = func(socketPath string) nvmeUblkDaemon {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		fake.sockets = append(fake.sockets, socketPath)
		return fake
	}
	return fake
}

func (f *fakeNodeUblkDaemon) Attach(_ context.Context, req util.NVMeUblkAttachRequest) (util.NVMeUblkDevice, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attaches = append(f.attaches, req)
	if f.attachErr != nil {
		return util.NVMeUblkDevice{}, f.attachErr
	}
	if existing, ok := f.devices[req.Volume]; ok {
		if existing.SubNQN != req.SubNQN {
			return util.NVMeUblkDevice{}, fmt.Errorf("nvmeublkd: volume %s is already attached to a different subsystem (%s)", req.Volume, existing.SubNQN)
		}
		existing.Existing = true
		return existing, nil
	}
	id := f.nextID
	f.nextID++
	path := filepath.Join(f.devDir, fmt.Sprintf("ublkb%d", id))
	require.NoError(f.t, os.WriteFile(path, nil, 0o600))
	device := util.NVMeUblkDevice{Volume: req.Volume, SubNQN: req.SubNQN, DevID: id, Path: path}
	for _, addr := range req.Addrs {
		device.Paths = append(device.Paths, util.NVMeUblkPath{Addr: addr, Up: !f.pathsDown})
	}
	f.devices[req.Volume] = device
	return device, nil
}

func (f *fakeNodeUblkDaemon) Detach(_ context.Context, volume string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.detaches = append(f.detaches, volume)
	if f.detachErr != nil {
		return false, f.detachErr
	}
	device, ok := f.devices[volume]
	if !ok {
		return true, nil
	}
	delete(f.devices, volume)
	_ = os.Remove(device.Path)
	return false, nil
}

func (f *fakeNodeUblkDaemon) List(context.Context) ([]util.NVMeUblkDevice, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists++
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]util.NVMeUblkDevice, 0, len(f.devices))
	for _, device := range f.devices {
		out = append(out, device)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DevID < out[j].DevID })
	return out, nil
}

func (f *fakeNodeUblkDaemon) snapshot() ([]util.NVMeUblkAttachRequest, []string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]util.NVMeUblkAttachRequest(nil), f.attaches...), append([]string(nil), f.detaches...), f.lists
}

// forbidNewNVMeUblkDaemon fails the test if anything even constructs a daemon
// client.
func forbidNewNVMeUblkDaemon(t *testing.T) {
	t.Helper()
	original := newNVMeUblkDaemon
	t.Cleanup(func() { newNVMeUblkDaemon = original })
	newNVMeUblkDaemon = func(string) nvmeUblkDaemon {
		t.Errorf("nvmeublkd must not be contacted here")
		return nil
	}
}

// forbidKernelNVMe fails the test if any kernel NVMe seam runs. The fake nvme
// command log (see installUblkNodeCommands) covers what is not a seam.
func forbidKernelNVMe(t *testing.T) {
	t.Helper()
	origConnect, origConnectPath := nvmeConnectWithSubsystems, nvmeConnectPathWithSubsystems
	origList, origDisconnect, origPolicy := nodeListNVMeSubsystems, nodeNVMeDisconnect, nodeSetNVMeIOPolicy
	origNodeInfo, origInfo, origRescan := nodeGetNVMeInfo, getNVMeInfoFromDevice, nodeNVMeRescan
	t.Cleanup(func() {
		nvmeConnectWithSubsystems, nvmeConnectPathWithSubsystems = origConnect, origConnectPath
		nodeListNVMeSubsystems, nodeNVMeDisconnect, nodeSetNVMeIOPolicy = origList, origDisconnect, origPolicy
		nodeGetNVMeInfo, getNVMeInfoFromDevice, nodeNVMeRescan = origNodeInfo, origInfo, origRescan
	})
	forbidden := func(name string) error {
		t.Errorf("kernel NVMe seam %s must not run on the ublk data path", name)
		return errors.New("forbidden")
	}
	nvmeConnectWithSubsystems = func(context.Context, string, string, *util.NVMeoFConnectOptions, []util.NVMeSubsystem) (string, error) {
		return "", forbidden("nvme connect")
	}
	nvmeConnectPathWithSubsystems = func(context.Context, string, string, *util.NVMeoFConnectOptions, []util.NVMeSubsystem) (string, error) {
		return "", forbidden("nvme connect (path)")
	}
	nodeListNVMeSubsystems = func(context.Context) ([]util.NVMeSubsystem, error) { return nil, forbidden("list-subsys") }
	nodeNVMeDisconnect = func(context.Context, string) error { return forbidden("nvme disconnect") }
	nodeSetNVMeIOPolicy = func(string, string) error { return forbidden("iopolicy") }
	nodeGetNVMeInfo = func(string) (string, error) { return "", forbidden("sysfs NQN lookup (node)") }
	getNVMeInfoFromDevice = func(string) (string, error) { return "", forbidden("sysfs NQN lookup") }
	nodeNVMeRescan = func(context.Context, string) error { return forbidden("nvme ns-rescan") }
}

// installUblkNodeCommands installs fake host commands and returns their log.
func installUblkNodeCommands(t *testing.T) string {
	t.Helper()
	installFakeNodeCommands(t, "findmnt", "nvme", "umount", "iscsiadm")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	t.Setenv("FAKE_NODE_COMMAND_LOG", logPath)
	return logPath
}

func assertNoNVMeCLI(t *testing.T, logPath string) {
	t.Helper()
	for _, line := range strings.Split(readNodeCommandLog(t, logPath), "\n") {
		assert.False(t, strings.HasPrefix(line, "nvme "), "nvme-cli must not run on the ublk data path: %q", line)
	}
}

// newTestUblkNodeDriver is an NVMe-oF node driver with the ublk path enabled,
// a node identity carrying a UUID-form host NQN, and no readable
// /etc/nvme/hostid (so the host ID is derived from the NQN).
func newTestUblkNodeDriver(t *testing.T) *Driver {
	t.Helper()
	d := newTestNodeDriver(ShareTypeNVMeoF)
	d.config.NVMeoF.Ublk = NVMeoFUblkConfig{Enabled: true, SocketPath: filepath.Join(t.TempDir(), "run", "nvmeublkd.sock")}
	encoded, err := encodeNodeIdentity(NodeIdentity{Name: "test-node-1", NVMeNQN: testUblkHostNQN})
	require.NoError(t, err)
	d.encodedNodeID = encoded
	originalRead := nodeReadIdentityFile
	t.Cleanup(func() { nodeReadIdentityFile = originalRead })
	nodeReadIdentityFile = func(string) ([]byte, error) { return nil, os.ErrNotExist }
	return d
}

func ublkVolumeContext(extra map[string]string) map[string]string {
	volumeContext := map[string]string{
		"node_attach_driver": "nvmeof",
		"nqn":                testUblkNQN,
		"transport":          "tcp",
		"address":            "192.0.2.20",
		"port":               "4420",
		nvmeoFDataPathKey:    NVMeoFDataPathUblk,
	}
	for key, value := range extra {
		volumeContext[key] = value
	}
	return volumeContext
}

func blockCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}

func mountCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}

func TestNodeStageUblkBlockRoundTrip(t *testing.T) {
	logPath := installUblkNodeCommands(t)
	forbidKernelNVMe(t)
	fake := installFakeNodeUblkDaemon(t)
	d := newTestUblkNodeDriver(t)
	successBefore := testutil.ToFloat64(nodeConnectTotal.WithLabelValues(nvmeUblkTransportLabel, "success"))
	kernelBefore := testutil.ToFloat64(nodeConnectTotal.WithLabelValues("nvmeof", "success"))

	stagingPath := filepath.Join(t.TempDir(), "staging", "volume-device")
	require.NoError(t, os.MkdirAll(stagingPath, 0o750))
	req := &csi.NodeStageVolumeRequest{
		VolumeId:          testUblkVolume,
		StagingTargetPath: stagingPath,
		VolumeCapability:  blockCapability(),
		VolumeContext:     ublkVolumeContext(nil),
		// Multipath hint from ControllerPublish: the daemon gets exactly the
		// address set the kernel path would connect.
		PublishContext: map[string]string{"addresses": `["192.0.2.20","192.0.2.21","2001:db8::22"]`},
	}

	_, err := d.NodeStageVolume(context.Background(), req)
	require.NoError(t, err)

	attaches, detaches, _ := fake.snapshot()
	require.Len(t, attaches, 1)
	assert.Equal(t, util.NVMeUblkAttachRequest{
		Volume:   testUblkVolume,
		SubNQN:   testUblkNQN,
		Addrs:    []string{"192.0.2.20:4420", "192.0.2.21:4420", "[2001:db8::22]:4420"},
		HostNQN:  testUblkHostNQN,
		HostID:   testUblkHostID,
		Queues:   8,
		Depth:    64,
		ZeroCopy: true,
		NapiUs:   0,
	}, attaches[0])
	assert.Empty(t, detaches)
	assert.Equal(t, []string{d.config.NVMeoF.Ublk.SocketPath}, unique(fake.sockets), "the configured socket is used")
	target, err := os.Readlink(stagingPath)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(fake.devDir, "ublkb0"), target)
	assert.Equal(t, successBefore+1, testutil.ToFloat64(nodeConnectTotal.WithLabelValues(nvmeUblkTransportLabel, "success")))
	assert.Equal(t, kernelBefore, testutil.ToFloat64(nodeConnectTotal.WithLabelValues("nvmeof", "success")),
		"the kernel transport label must not count a ublk attach")
	marked, err := d.nvmeUblkMarkerExists(testUblkVolume)
	require.NoError(t, err)
	assert.True(t, marked, "a ublk stage leaves evidence for unstage")

	// Idempotent re-stage: identity is verified through the daemon, nothing is
	// attached again and no kernel convergence runs.
	info, err := os.Lstat(stagingPath)
	require.NoError(t, err)
	_, err = d.NodeStageVolume(context.Background(), req)
	require.NoError(t, err)
	attaches, _, lists := fake.snapshot()
	assert.Len(t, attaches, 1, "a compatible re-stage must not attach again")
	assert.Positive(t, lists, "re-stage identity comes from the daemon")
	restaged, err := os.Lstat(stagingPath)
	require.NoError(t, err)
	assert.True(t, os.SameFile(info, restaged), "a correct staging symlink must not be replaced")

	_, err = d.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{VolumeId: testUblkVolume, StagingTargetPath: stagingPath})
	require.NoError(t, err)
	_, detaches, _ = fake.snapshot()
	assert.Equal(t, []string{testUblkVolume}, detaches)
	_, err = os.Lstat(stagingPath)
	assert.True(t, os.IsNotExist(err), "unstage removes the staging symlink")
	marked, err = d.nvmeUblkMarkerExists(testUblkVolume)
	require.NoError(t, err)
	assert.False(t, marked, "a completed detach clears the evidence")

	assertNoNVMeCLI(t, logPath)
}

func TestNodeStageUblkFilesystemFormatsTheDaemonDevice(t *testing.T) {
	logPath := installUblkNodeCommands(t)
	forbidKernelNVMe(t)
	fake := installFakeNodeUblkDaemon(t)
	d := newTestUblkNodeDriver(t)
	// The install default selects ublk; the volume context does not pin it.
	d.config.NVMeoF.DataPath = NVMeoFDataPathUblk
	d.config.NVMeoF.Ublk.Enabled = false
	napi := 200
	zeroCopy := false
	d.config.NVMeoF.Ublk.Queues, d.config.NVMeoF.Ublk.Depth = 4, 128
	d.config.NVMeoF.Ublk.ZeroCopy, d.config.NVMeoF.Ublk.NapiUs = &zeroCopy, napi

	originalFormat := nodeFormatAndMount
	t.Cleanup(func() { nodeFormatAndMount = originalFormat })
	var formatted string
	nodeFormatAndMount = func(_ context.Context, device, _, fsType string, _ []string) error {
		formatted = device + " " + fsType
		return nil
	}
	volumeContext := ublkVolumeContext(nil)
	delete(volumeContext, nvmeoFDataPathKey)

	_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          testUblkVolume,
		StagingTargetPath: filepath.Join(t.TempDir(), "stage"),
		VolumeCapability:  mountCapability(),
		VolumeContext:     volumeContext,
	})
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(fake.devDir, "ublkb0")+" ext4", formatted, "the daemon's device is formatted and mounted like a kernel one")
	attaches, _, _ := fake.snapshot()
	require.Len(t, attaches, 1)
	assert.Equal(t, []string{"192.0.2.20:4420"}, attaches[0].Addrs, "no multipath hint falls back to address:port")
	assert.Equal(t, 4, attaches[0].Queues)
	assert.Equal(t, 128, attaches[0].Depth)
	assert.False(t, attaches[0].ZeroCopy)
	assert.Equal(t, 200, attaches[0].NapiUs)
	assertNoNVMeCLI(t, logPath)
}

// A mounted ublk volume re-stages through the daemon's identity, and a mount
// of a different subsystem or volume is refused.
func TestNodeStageUblkMountedReplayVerifiesThroughTheDaemon(t *testing.T) {
	tests := []struct {
		name     string
		volume   string
		subNQN   string
		wantCode codes.Code
	}{
		{name: "same volume", volume: testUblkVolume, subNQN: testUblkNQN, wantCode: codes.OK},
		{name: "different subsystem", volume: testUblkVolume, subNQN: "nqn.2011-06.com.example:other", wantCode: codes.AlreadyExists},
		{name: "different daemon volume", volume: "pvc-other", subNQN: testUblkNQN, wantCode: codes.AlreadyExists},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logPath := installUblkNodeCommands(t)
			forbidKernelNVMe(t)
			fake := installFakeNodeUblkDaemon(t)
			d := newTestUblkNodeDriver(t)
			device := filepath.Join(fake.devDir, "ublkb5")
			fake.devices[tc.volume] = util.NVMeUblkDevice{Volume: tc.volume, SubNQN: tc.subNQN, DevID: 5, Path: device}
			stateFile := filepath.Join(t.TempDir(), "mounted")
			require.NoError(t, os.WriteFile(stateFile, nil, 0o600))
			t.Setenv("FAKE_NODE_MOUNT_STATE_FILE", stateFile)
			t.Setenv("FAKE_NODE_FINDMNT_INFO", device+" ext4 rw")

			_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
				VolumeId:          testUblkVolume,
				StagingTargetPath: t.TempDir(),
				VolumeCapability:  mountCapability(),
				VolumeContext:     ublkVolumeContext(nil),
			})
			assert.Equal(t, tc.wantCode, status.Code(err), "%v", err)
			attaches, _, lists := fake.snapshot()
			assert.Empty(t, attaches)
			assert.Positive(t, lists)
			assertNoNVMeCLI(t, logPath)
		})
	}
}

func TestNodeStageUblkFailures(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(d *Driver, fake *fakeNodeUblkDaemon)
		context    map[string]string
		wantCode   codes.Code
		wantMsg    string
		wantAttach bool
	}{
		{
			name:     "ublk not enabled on this node",
			setup:    func(d *Driver, _ *fakeNodeUblkDaemon) { d.config.NVMeoF.Ublk.Enabled = false },
			wantCode: codes.FailedPrecondition, wantMsg: "not enabled on this node",
		},
		{
			name: "no host NQN",
			setup: func(d *Driver, _ *fakeNodeUblkDaemon) {
				d.encodedNodeID, _ = encodeNodeIdentity(NodeIdentity{Name: "n"})
			},
			wantCode: codes.FailedPrecondition, wantMsg: "no NVMe host NQN",
		},
		{
			name: "no host ID",
			setup: func(d *Driver, _ *fakeNodeUblkDaemon) {
				d.encodedNodeID, _ = encodeNodeIdentity(NodeIdentity{Name: "n", NVMeNQN: "nqn.2014-08.com.example:node"})
			},
			wantCode: codes.FailedPrecondition, wantMsg: "no NVMe host ID",
		},
		{
			name:     "rdma transport",
			context:  map[string]string{"transport": "rdma"},
			wantCode: codes.InvalidArgument, wantMsg: "supports only the tcp transport",
		},
		{
			name:     "malformed pinned data path",
			context:  map[string]string{nvmeoFDataPathKey: "spdk"},
			wantCode: codes.InvalidArgument, wantMsg: "nvmeof/dataPath",
		},
		{
			name: "daemon refuses",
			setup: func(_ *Driver, fake *fakeNodeUblkDaemon) {
				fake.attachErr = errors.New("nvmeublkd: zero copy unsupported")
			},
			wantCode: codes.Internal, wantMsg: "zero copy unsupported", wantAttach: true,
		},
		{
			name: "daemon not running",
			setup: func(_ *Driver, fake *fakeNodeUblkDaemon) {
				fake.attachErr = fmt.Errorf("attach: %w", util.ErrNVMeUblkDaemonUnavailable)
			},
			wantCode: codes.Unavailable, wantMsg: "unavailable", wantAttach: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logPath := installUblkNodeCommands(t)
			forbidKernelNVMe(t)
			fake := installFakeNodeUblkDaemon(t)
			d := newTestUblkNodeDriver(t)
			if tc.setup != nil {
				tc.setup(d, fake)
			}
			errorsBefore := testutil.ToFloat64(nodeConnectTotal.WithLabelValues(nvmeUblkTransportLabel, "error"))
			_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
				VolumeId:          testUblkVolume,
				StagingTargetPath: filepath.Join(t.TempDir(), "stage"),
				VolumeCapability:  blockCapability(),
				VolumeContext:     ublkVolumeContext(tc.context),
			})
			require.Error(t, err)
			assert.Equal(t, tc.wantCode, status.Code(err), "%v", err)
			assert.Contains(t, err.Error(), tc.wantMsg)
			attaches, _, _ := fake.snapshot()
			assert.Equal(t, tc.wantAttach, len(attaches) > 0)
			if tc.wantAttach {
				assert.Equal(t, errorsBefore+1, testutil.ToFloat64(nodeConnectTotal.WithLabelValues(nvmeUblkTransportLabel, "error")))
			}
			assertNoNVMeCLI(t, logPath)
		})
	}
}

func TestNodeStageUblkRejectsAForeignDevicePath(t *testing.T) {
	tests := []struct {
		name   string
		device util.NVMeUblkDevice
		stat   error
	}{
		{name: "path outside the ublk namespace", device: util.NVMeUblkDevice{DevID: 0, Path: "/dev/sda"}},
		{name: "path for a different dev_id", device: util.NVMeUblkDevice{DevID: 1, Path: "/dev/ublkb0"}},
		{name: "device node missing", device: util.NVMeUblkDevice{DevID: 2, Path: "/dev/ublkb2"}, stat: os.ErrNotExist},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			originalDir, originalStat := nodeNVMeUblkDevDir, nodeNVMeUblkStat
			t.Cleanup(func() { nodeNVMeUblkDevDir, nodeNVMeUblkStat = originalDir, originalStat })
			nodeNVMeUblkDevDir = "/dev"
			nodeNVMeUblkStat = func(string) error { return tc.stat }
			err := validateNVMeUblkAttachedDevice(tc.device)
			require.Error(t, err)
		})
	}
	originalStat := nodeNVMeUblkStat
	t.Cleanup(func() { nodeNVMeUblkStat = originalStat })
	nodeNVMeUblkStat = func(string) error { return nil }
	require.NoError(t, validateNVMeUblkAttachedDevice(util.NVMeUblkDevice{DevID: 3, Path: "/dev/ublkb3"}))
}

func TestNodeStageUblkReportsDownPaths(t *testing.T) {
	installUblkNodeCommands(t)
	forbidKernelNVMe(t)
	fake := installFakeNodeUblkDaemon(t)
	fake.pathsDown = true
	d := newTestUblkNodeDriver(t)
	fakeRecorder := record.NewFakeRecorder(4)
	d.eventRecorder = &EventRecorder{recorder: fakeRecorder, enabled: true}

	_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          testUblkVolume,
		StagingTargetPath: filepath.Join(t.TempDir(), "stage"),
		VolumeCapability:  blockCapability(),
		VolumeContext:     ublkVolumeContext(nil),
	})
	require.NoError(t, err, "a volume with paths down still stages")
	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonNVMePathDegraded)
		assert.Contains(t, event, "192.0.2.20:4420: path is down in nvmeublkd")
	default:
		t.Fatal("a path the daemon reports down must emit the degraded-path event")
	}
}

// The kernel stage helper refuses a volume that selected ublk, so no caller
// can connect it with nvme-cli by accident.
func TestKernelNVMeoFStageRefusesUblkVolumes(t *testing.T) {
	logPath := installUblkNodeCommands(t)
	forbidKernelNVMe(t)
	forbidNewNVMeUblkDaemon(t)
	d := newTestUblkNodeDriver(t)
	err := d.stageNVMeoFVolume(context.Background(), ublkVolumeContext(nil), filepath.Join(t.TempDir(), "stage"), blockCapability())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to connect it with the kernel initiator")
	assertNoNVMeCLI(t, logPath)
}

func TestNodeUnstageUblk(t *testing.T) {
	tests := []struct {
		name string
		// setup prepares the staging path and returns it.
		setup      func(t *testing.T, d *Driver, fake *fakeNodeUblkDaemon) string
		detachErr  error
		wantCode   codes.Code
		wantDetach bool
		// wantKernel is true when the kernel session cleanup must still run.
		wantKernel bool
	}{
		{
			name: "filesystem mount of a ublk device",
			setup: func(t *testing.T, _ *Driver, _ *fakeNodeUblkDaemon) string {
				t.Helper()
				t.Helper()
				t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/ublkb3")
				return t.TempDir()
			},
			wantCode: codes.OK, wantDetach: true,
		},
		{
			name: "block symlink to a ublk device",
			setup: func(t *testing.T, _ *Driver, _ *fakeNodeUblkDaemon) string {
				t.Helper()
				t.Helper()
				stagingPath := filepath.Join(t.TempDir(), "staging")
				require.NoError(t, os.Symlink("/dev/ublkb3", stagingPath))
				return stagingPath
			},
			wantCode: codes.OK, wantDetach: true,
		},
		{
			name: "only the marker survives (an earlier attempt unmounted, then failed)",
			setup: func(t *testing.T, d *Driver, _ *fakeNodeUblkDaemon) string {
				t.Helper()
				t.Helper()
				require.NoError(t, d.writeNVMeUblkMarker(testUblkVolume))
				return filepath.Join(t.TempDir(), "gone")
			},
			wantCode: codes.OK, wantDetach: true,
		},
		{
			name: "ublk device with the daemon down fails closed",
			setup: func(t *testing.T, _ *Driver, _ *fakeNodeUblkDaemon) string {
				t.Helper()
				t.Helper()
				t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/ublkb3")
				return t.TempDir()
			},
			detachErr: fmt.Errorf("detach: %w", util.ErrNVMeUblkDaemonUnavailable),
			wantCode:  codes.Unavailable, wantDetach: true,
		},
		{
			name: "marker only with the daemon down is never silently skipped",
			setup: func(t *testing.T, d *Driver, _ *fakeNodeUblkDaemon) string {
				t.Helper()
				t.Helper()
				require.NoError(t, d.writeNVMeUblkMarker(testUblkVolume))
				return filepath.Join(t.TempDir(), "gone")
			},
			detachErr: fmt.Errorf("detach: %w", util.ErrNVMeUblkDaemonUnavailable),
			wantCode:  codes.Unavailable, wantDetach: true,
		},
		{
			name: "daemon refuses the detach",
			setup: func(t *testing.T, _ *Driver, _ *fakeNodeUblkDaemon) string {
				t.Helper()
				t.Helper()
				stagingPath := filepath.Join(t.TempDir(), "staging")
				require.NoError(t, os.Symlink("/dev/ublkb3", stagingPath))
				return stagingPath
			},
			detachErr: errors.New("nvmeublkd: stop ublk device 3: busy"),
			wantCode:  codes.Internal, wantDetach: true,
		},
		{
			name: "marker beside a live kernel device detaches and still cleans up the kernel session",
			setup: func(t *testing.T, d *Driver, _ *fakeNodeUblkDaemon) string {
				t.Helper()
				t.Helper()
				require.NoError(t, d.writeNVMeUblkMarker(testUblkVolume))
				t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/nvme7n1")
				return t.TempDir()
			},
			wantCode: codes.OK, wantDetach: true, wantKernel: true,
		},
		{
			name: "no evidence of ublk takes the kernel path untouched",
			setup: func(t *testing.T, _ *Driver, _ *fakeNodeUblkDaemon) string {
				t.Helper()
				t.Helper()
				t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/nvme7n1")
				return t.TempDir()
			},
			wantCode: codes.OK, wantDetach: false, wantKernel: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logPath := installUblkNodeCommands(t)
			fake := installFakeNodeUblkDaemon(t)
			fake.detachErr = tc.detachErr
			d := newTestUblkNodeDriver(t)
			stagingPath := tc.setup(t, d, fake)

			origInfo, origDisconnect := nodeGetNVMeInfo, nodeNVMeDisconnect
			t.Cleanup(func() { nodeGetNVMeInfo, nodeNVMeDisconnect = origInfo, origDisconnect })
			var kernelDisconnects []string
			nodeGetNVMeInfo = func(device string) (string, error) {
				if util.IsNVMeUblkDevice(device) {
					t.Errorf("sysfs NQN lookup must never see a ublk device: %s", device)
				}
				return testUblkNQN, nil
			}
			nodeNVMeDisconnect = func(_ context.Context, nqn string) error {
				kernelDisconnects = append(kernelDisconnects, nqn)
				return nil
			}

			_, err := d.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{VolumeId: testUblkVolume, StagingTargetPath: stagingPath})
			assert.Equal(t, tc.wantCode, status.Code(err), "%v", err)
			_, detaches, _ := fake.snapshot()
			if tc.wantDetach {
				assert.Equal(t, []string{testUblkVolume}, detaches)
			} else {
				assert.Empty(t, detaches)
			}
			marked, markerErr := d.nvmeUblkMarkerExists(testUblkVolume)
			require.NoError(t, markerErr)
			if tc.wantCode == codes.OK {
				assert.False(t, marked, "a successful unstage leaves no marker")
			} else if tc.name != "ublk device with the daemon down fails closed" && tc.name != "daemon refuses the detach" {
				assert.True(t, marked, "a failed detach keeps the evidence for the retry")
			}
			if tc.wantKernel {
				assert.Equal(t, []string{testUblkNQN}, kernelDisconnects, "the kernel session is still disconnected")
			} else {
				assert.Empty(t, kernelDisconnects, "no kernel disconnect for a ublk volume")
				assertNoNVMeCLI(t, logPath)
			}
		})
	}
}

// A kernel-only install never contacts the daemon or looks for markers, even
// when it has no device evidence at all.
func TestNodeUnstageKernelOnlyInstallNeverContactsTheDaemon(t *testing.T) {
	installUblkNodeCommands(t)
	forbidNewNVMeUblkDaemon(t)
	d := newTestNodeDriver(ShareTypeNVMeoF)
	// A socket directory that cannot be inspected would fail closed if the
	// marker were consulted.
	d.config.NVMeoF.Ublk.SocketPath = "/proc/self/fdinfo/nonexistent/d.sock"
	_, err := d.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId: testUblkVolume, StagingTargetPath: filepath.Join(t.TempDir(), "gone"),
	})
	require.NoError(t, err)
}

func TestNodePublishUblkRawBlockOwnership(t *testing.T) {
	tests := []struct {
		name     string
		device   util.NVMeUblkDevice
		wantCode codes.Code
	}{
		{name: "owned", device: util.NVMeUblkDevice{Volume: "pvc-own", SubNQN: "nqn.2011-06.com.example:pvc-own"}, wantCode: codes.OK},
		{name: "foreign subsystem", device: util.NVMeUblkDevice{Volume: "pvc-own", SubNQN: "nqn.2011-06.com.example:pvc-other"}, wantCode: codes.FailedPrecondition},
		{name: "foreign daemon volume", device: util.NVMeUblkDevice{Volume: "pvc-other", SubNQN: "nqn.2011-06.com.example:pvc-own"}, wantCode: codes.FailedPrecondition},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			forbidKernelNVMe(t)
			fake := installFakeNodeUblkDaemon(t)
			tc.device.DevID, tc.device.Path = 4, "/dev/ublkb4"
			fake.devices[tc.device.Volume] = tc.device
			d := newTestUblkNodeDriver(t)
			err := d.validateRawBlockDeviceOwnership(context.Background(), "pvc-own", "/dev/ublkb4", ShareTypeNVMeoF)
			assert.Equal(t, tc.wantCode, status.Code(err), "%v", err)
		})
	}

	t.Run("daemon down", func(t *testing.T) {
		forbidKernelNVMe(t)
		fake := installFakeNodeUblkDaemon(t)
		fake.listErr = util.ErrNVMeUblkDaemonUnavailable
		err := newTestUblkNodeDriver(t).validateRawBlockDeviceOwnership(context.Background(), "pvc-own", "/dev/ublkb4", ShareTypeNVMeoF)
		// Unavailable: retryable, so kubelet keeps the volume's obligations
		// while the daemon restarts.
		assert.Equal(t, codes.Unavailable, status.Code(err))
	})
}

func TestNodeExpandUblkVolume(t *testing.T) {
	tests := []struct {
		name       string
		deviceSize int64
		request    int64
		wantCode   codes.Code
		wantResize bool
	}{
		{name: "device already covers the request", deviceSize: 20 << 30, request: 20 << 30, wantCode: codes.OK, wantResize: true},
		{name: "live device cannot grow", deviceSize: 10 << 30, request: 20 << 30, wantCode: codes.FailedPrecondition},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			installUblkNodeCommands(t)
			forbidKernelNVMe(t)
			forbidNewNVMeUblkDaemon(t)
			t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "/dev/ublkb2")
			origSize, origResize := nodeGetDeviceSize, nodeResizeFilesystem
			t.Cleanup(func() { nodeGetDeviceSize, nodeResizeFilesystem = origSize, origResize })
			nodeGetDeviceSize = func(device string) (int64, error) {
				assert.Equal(t, "/dev/ublkb2", device)
				return tc.deviceSize, nil
			}
			resized := false
			nodeResizeFilesystem = func(context.Context, string) error {
				resized = true
				return nil
			}
			resp, err := newTestUblkNodeDriver(t).NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
				VolumeId:      testUblkVolume,
				VolumePath:    t.TempDir(),
				CapacityRange: &csi.CapacityRange{RequiredBytes: tc.request},
			})
			assert.Equal(t, tc.wantCode, status.Code(err), "%v", err)
			assert.Equal(t, tc.wantResize, resized)
			if tc.wantCode == codes.OK {
				assert.Equal(t, tc.deviceSize, resp.GetCapacityBytes())
			} else {
				assert.Contains(t, err.Error(), "next staged")
			}
		})
	}

	t.Run("raw block checks ownership through the daemon", func(t *testing.T) {
		forbidKernelNVMe(t)
		fake := installFakeNodeUblkDaemon(t)
		fake.devices["pvc-other"] = util.NVMeUblkDevice{Volume: "pvc-other", SubNQN: "nqn.x:pvc-other", DevID: 2, Path: "/dev/ublkb2"}
		_, err := newTestUblkNodeDriver(t).expandNVMeUblkVolume(context.Background(), testUblkVolume, t.TempDir(), "/dev/ublkb2", true, 0)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	})
}

func TestNodeNVMeHostID(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		hostNQN string
		want    string
		wantErr string
	}{
		{name: "host file wins", files: map[string]string{"/host/etc/nvme/hostid": "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE\n"}, hostNQN: testUblkHostNQN, want: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"},
		{name: "container file is the fallback", files: map[string]string{"/etc/nvme/hostid": "aaaaaaaabbbbccccddddeeeeeeeeeeee"}, hostNQN: testUblkHostNQN, want: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"},
		{name: "derived from a UUID host NQN", hostNQN: testUblkHostNQN, want: testUblkHostID},
		{name: "blank file is ignored", files: map[string]string{"/host/etc/nvme/hostid": "\n"}, hostNQN: testUblkHostNQN, want: testUblkHostID},
		{name: "malformed file fails closed", files: map[string]string{"/host/etc/nvme/hostid": "not-a-uuid"}, hostNQN: testUblkHostNQN, wantErr: "does not hold a UUID"},
		{name: "no file and a non-UUID NQN", hostNQN: "nqn.2014-08.com.example:node-1", wantErr: "no NVMe host ID"},
		{name: "malformed UUID in the NQN", hostNQN: "nqn.2014-08.org.nvmexpress:uuid:1234", wantErr: "no NVMe host ID"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			original := nodeReadIdentityFile
			t.Cleanup(func() { nodeReadIdentityFile = original })
			nodeReadIdentityFile = func(path string) ([]byte, error) {
				if contents, ok := tc.files[path]; ok {
					return []byte(contents), nil
				}
				return nil, os.ErrNotExist
			}
			got, err := nodeNVMeHostID(tc.hostNQN)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func unique(values []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

// After a reboot, ublk numbering restarts at 0: a block staging link that
// survived under /var/lib/kubelet resolves to whichever volume attached first.
// Its re-stage must attach its own device and replace the link, never fail
// AlreadyExists forever, and never touch the other volume's device.
func TestNodeStageUblkBlockStaleLinkToReusedDeviceReattaches(t *testing.T) {
	logPath := installUblkNodeCommands(t)
	forbidKernelNVMe(t)
	fake := installFakeNodeUblkDaemon(t)
	d := newTestUblkNodeDriver(t)

	// Volume B stages first after the reboot and takes ublkb0.
	other, err := fake.Attach(context.Background(), util.NVMeUblkAttachRequest{Volume: "pvc-other", SubNQN: "nqn.2011-06.com.example:pvc-other"})
	require.NoError(t, err)
	require.Equal(t, filepath.Join(fake.devDir, "ublkb0"), other.Path)

	// Volume A's pre-reboot link still points at ublkb0.
	stagingPath := filepath.Join(t.TempDir(), "staging", "volume-device")
	require.NoError(t, os.MkdirAll(filepath.Dir(stagingPath), 0o750))
	require.NoError(t, os.Symlink(other.Path, stagingPath))
	req := &csi.NodeStageVolumeRequest{
		VolumeId:          testUblkVolume,
		StagingTargetPath: stagingPath,
		VolumeCapability:  blockCapability(),
		VolumeContext:     ublkVolumeContext(nil),
	}

	_, err = d.NodeStageVolume(context.Background(), req)
	require.NoError(t, err)
	target, err := os.Readlink(stagingPath)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(fake.devDir, "ublkb1"), target, "the link now points at this volume's own device")
	attaches, detaches, _ := fake.snapshot()
	require.Len(t, attaches, 2)
	assert.Equal(t, testUblkVolume, attaches[1].Volume)
	assert.Empty(t, detaches, "the other volume's device is never detached")
	assert.FileExists(t, other.Path)
	assertNoNVMeCLI(t, logPath)
}

// A stage record naming a DIFFERENT volume at this path is a real collision:
// it stays fail-closed.
func TestNodeStageUblkBlockLinkOwnedByRecordedOtherVolumeStaysAlreadyExists(t *testing.T) {
	installUblkNodeCommands(t)
	forbidKernelNVMe(t)
	fake := installFakeNodeUblkDaemon(t)
	d := newTestUblkNodeDriver(t)

	other, err := fake.Attach(context.Background(), util.NVMeUblkAttachRequest{Volume: "pvc-other", SubNQN: "nqn.2011-06.com.example:pvc-other"})
	require.NoError(t, err)
	stagingPath := filepath.Join(t.TempDir(), "staging", "volume-device")
	require.NoError(t, os.MkdirAll(filepath.Dir(stagingPath), 0o750))
	require.NoError(t, os.Symlink(other.Path, stagingPath))
	d.storeStageRecord(nodeMountRecord{TargetPath: stagingPath, VolumeID: "pvc-other"})

	_, err = d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          testUblkVolume,
		StagingTargetPath: stagingPath,
		VolumeCapability:  blockCapability(),
		VolumeContext:     ublkVolumeContext(nil),
	})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	attaches, _, _ := fake.snapshot()
	assert.Len(t, attaches, 1, "no attach for the colliding volume")
}

// A daemon call that runs out of time may still complete in the daemon, so it
// must surface as a code kubelet treats as uncertain (it then keeps the
// NodeUnstage obligation), never as a final Internal.
func TestNVMeUblkStatusCodeKeepsTimeoutsUncertain(t *testing.T) {
	assert.Equal(t, codes.Unavailable, nvmeUblkStatusCode(fmt.Errorf("attach: %w", util.ErrNVMeUblkDaemonUnavailable)))
	assert.Equal(t, codes.DeadlineExceeded, nvmeUblkStatusCode(fmt.Errorf("attach: read response: %w", context.DeadlineExceeded)))
	assert.Equal(t, codes.DeadlineExceeded, nvmeUblkStatusCode(fmt.Errorf("attach: %w", context.Canceled)))
	assert.Equal(t, codes.Internal, nvmeUblkStatusCode(errors.New("nvmeublkd: bad address")))
}

func TestNodeStageUblkAttachTimeoutIsDeadlineExceeded(t *testing.T) {
	installUblkNodeCommands(t)
	forbidKernelNVMe(t)
	fake := installFakeNodeUblkDaemon(t)
	fake.attachErr = fmt.Errorf("attach: read response: %w", context.DeadlineExceeded)
	d := newTestUblkNodeDriver(t)
	stagingPath := filepath.Join(t.TempDir(), "staging", "volume-device")
	require.NoError(t, os.MkdirAll(filepath.Dir(stagingPath), 0o750))
	_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          testUblkVolume,
		StagingTargetPath: stagingPath,
		VolumeCapability:  blockCapability(),
		VolumeContext:     ublkVolumeContext(nil),
	})
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	marked, markErr := d.nvmeUblkMarkerExists(testUblkVolume)
	require.NoError(t, markErr)
	assert.True(t, marked, "the marker stays so the unstage kubelet still owes can detach a late attach")
}
