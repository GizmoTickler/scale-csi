package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

// The other NVMe-oF stage tests run on a fake node that has no NVMe sysfs, so
// the clamp is stubbed out for them here. The clamp itself is exercised for
// real against a fake sysfs in pkg/util (nvme_max_sectors_test.go, including
// the vectors the Rust agent shares), and its place in the stage by the tests
// below.
func init() {
	nodeClampNVMeMaxSectors = stubNVMeMaxSectorsClamp
}

func stubNVMeMaxSectorsClamp(_ context.Context, nqn string, opts util.NVMeMaxSectorsClampOptions) (*util.NVMeMaxSectorsClampResult, error) {
	return &util.NVMeMaxSectorsClampResult{NQN: nqn, Policy: opts.Policy, Skipped: "stubbed in tests"}, nil
}

type clampCall struct {
	nqn  string
	opts util.NVMeMaxSectorsClampOptions
}

// recordStageSteps replaces the connect, the clamp and format+mount with fakes
// that append to one ordered log.
func recordStageSteps(t *testing.T, clampErr error) (steps *[]string, calls *[]clampCall, stagingPath string) {
	t.Helper()
	installFakeNodeCommands(t, "findmnt", "nvme")
	originalConnect := nvmeConnectWithSubsystems
	originalFormat := nodeFormatAndMount
	originalClamp := nodeClampNVMeMaxSectors
	originalList := nodeListNVMeSubsystems
	t.Cleanup(func() {
		nvmeConnectWithSubsystems = originalConnect
		nodeFormatAndMount = originalFormat
		nodeClampNVMeMaxSectors = originalClamp
		nodeListNVMeSubsystems = originalList
	})
	stagingPath = filepath.Join(t.TempDir(), "staging")
	var mu sync.Mutex
	log := []string{}
	recorded := []clampCall{}
	nodeListNVMeSubsystems = func(context.Context) ([]util.NVMeSubsystem, error) { return nil, nil }
	nvmeConnectWithSubsystems = func(context.Context, string, string, *util.NVMeoFConnectOptions, []util.NVMeSubsystem) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		log = append(log, "connect")
		return "/dev/nvme3n1", nil
	}
	nodeClampNVMeMaxSectors = func(_ context.Context, nqn string, opts util.NVMeMaxSectorsClampOptions) (*util.NVMeMaxSectorsClampResult, error) {
		mu.Lock()
		defer mu.Unlock()
		_, linkErr := os.Lstat(stagingPath)
		log = append(log, fmt.Sprintf("clamp(staging link present=%t)", linkErr == nil))
		recorded = append(recorded, clampCall{nqn: nqn, opts: opts})
		result := &util.NVMeMaxSectorsClampResult{NQN: nqn, Policy: opts.Policy, LimitKB: 4096}
		return result, clampErr
	}
	nodeFormatAndMount = func(context.Context, string, string, string, []string) error {
		mu.Lock()
		defer mu.Unlock()
		log = append(log, "format+mount")
		return nil
	}
	return &log, &recorded, stagingPath
}

var nvmeStageContext = map[string]string{"nqn": "nqn.test:clamp", "transport": "tcp", "address": "192.0.2.20", "port": "4420"}

func filesystemCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}}}
}

// The incident: a fresh stage mounted and failed within seconds because the
// path devices still allowed 32 MiB commands. The clamp must run after the
// connect and before format+mount.
func TestStageNVMeoFClampsBeforeFormatAndMount(t *testing.T) {
	steps, calls, stagingPath := recordStageSteps(t, nil)
	d := newTestNodeDriver(ShareTypeNVMeoF)

	require.NoError(t, d.stageNVMeoFVolume(context.Background(), nvmeStageContext, stagingPath, filesystemCapability()))

	assert.Equal(t, []string{"connect", "clamp(staging link present=false)", "format+mount"}, *steps)
	require.Len(t, *calls, 1)
	call := (*calls)[0]
	assert.Equal(t, "nqn.test:clamp", call.nqn)
	assert.Equal(t, util.NVMeMaxSectorsPolicy{Mode: util.NVMeMaxSectorsAuto}, call.opts.Policy, "auto is the default")
	assert.Equal(t, "tcp", call.opts.Transport)
	assert.Equal(t, nvmeMaxSectorsPathWait, call.opts.PathWait, "a fresh stage waits for late path devices")
}

func TestStageNVMeoFClampsBeforeTheRawBlockLink(t *testing.T) {
	steps, calls, stagingPath := recordStageSteps(t, nil)
	d := newTestNodeDriver(ShareTypeNVMeoF)

	require.NoError(t, d.stageNVMeoFVolume(context.Background(), nvmeStageContext, stagingPath, blockCapability()))

	assert.Equal(t, []string{"connect", "clamp(staging link present=false)"}, *steps, "the raw-block link a pod publishes from appears only after the clamp")
	assert.Len(t, *calls, 1)
	target, err := os.Readlink(stagingPath)
	require.NoError(t, err)
	assert.Equal(t, "/dev/nvme3n1", target)
}

func TestStageNVMeoFRefusesToMountWithoutTheClamp(t *testing.T) {
	for name, capability := range map[string]*csi.VolumeCapability{"filesystem": filesystemCapability(), "block": blockCapability()} {
		t.Run(name, func(t *testing.T) {
			clampErr := fmt.Errorf("%w for nqn.test:clamp: write .../nvme3c5n1/queue/max_sectors_kb: permission denied", util.ErrNVMeMaxSectorsClamp)
			steps, _, stagingPath := recordStageSteps(t, clampErr)
			d := newTestNodeDriver(ShareTypeNVMeoF)

			err := d.stageNVMeoFVolume(context.Background(), nvmeStageContext, stagingPath, capability)

			require.Error(t, err)
			assert.Equal(t, codes.Internal, status.Code(err))
			assert.Contains(t, err.Error(), "refusing to stage NVMe-oF subsystem nqn.test:clamp without its request-size clamp")
			assert.Contains(t, err.Error(), "nvme3c5n1")
			assert.Equal(t, []string{"connect", "clamp(staging link present=false)"}, *steps, "nothing formatted, mounted or linked")
			_, statErr := os.Lstat(stagingPath)
			assert.True(t, os.IsNotExist(statErr), "no raw-block link for a pod to publish")
		})
	}
}

func TestStageNVMeoFClampOff(t *testing.T) {
	steps, calls, stagingPath := recordStageSteps(t, errors.New("must not be called"))
	d := newTestNodeDriver(ShareTypeNVMeoF)
	d.config.NVMeoF.MaxSectorsKB = "off"

	require.NoError(t, d.stageNVMeoFVolume(context.Background(), nvmeStageContext, stagingPath, filesystemCapability()))

	assert.Empty(t, *calls)
	assert.Equal(t, []string{"connect", "format+mount"}, *steps)
}

func TestStageNVMeoFFixedClampValue(t *testing.T) {
	_, calls, stagingPath := recordStageSteps(t, nil)
	d := newTestNodeDriver(ShareTypeNVMeoF)
	d.config.NVMeoF.MaxSectorsKB = "1024"

	require.NoError(t, d.stageNVMeoFVolume(context.Background(), nvmeStageContext, stagingPath, filesystemCapability()))

	require.Len(t, *calls, 1)
	assert.Equal(t, util.NVMeMaxSectorsPolicy{Mode: util.NVMeMaxSectorsFixed, LimitKB: 1024}, (*calls)[0].opts.Policy)
}

// A replay of an already-staged raw-block volume (kubelet retry, plugin
// restart, or a volume the Rust agent staged) converges the clamp too, and
// fails rather than hand the link to another pod unprotected.
func TestStageNVMeoFReplayClamps(t *testing.T) {
	for _, clampErr := range []error{nil, fmt.Errorf("%w: boom", util.ErrNVMeMaxSectorsClamp)} {
		t.Run(fmt.Sprintf("clampErr=%v", clampErr), func(t *testing.T) {
			steps, calls, stagingPath := recordStageSteps(t, clampErr)
			nodeListNVMeSubsystems = func(context.Context) ([]util.NVMeSubsystem, error) {
				return []util.NVMeSubsystem{{NQN: "nqn.test:clamp", Name: "nvme-subsys3", Paths: []util.NVMePath{{Name: "nvme3", State: "live"}}}}, nil
			}
			originalInfo := getNVMeInfoFromDevice
			t.Cleanup(func() { getNVMeInfoFromDevice = originalInfo })
			getNVMeInfoFromDevice = func(string) (string, error) { return "nqn.test:clamp", nil }
			require.NoError(t, os.Symlink("/dev/null", stagingPath))
			d := newTestNodeDriver(ShareTypeNVMeoF)

			err := d.stageNVMeoFVolume(context.Background(), nvmeStageContext, stagingPath, blockCapability())

			assert.NotContains(t, *steps, "connect", "a replay does not reconnect")
			require.Len(t, *calls, 1)
			if clampErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, codes.Internal, status.Code(err))
			}
		})
	}
}

func withStagedNQNs(t *testing.T, nqns map[string]string) {
	t.Helper()
	origMounted, origStaged, origInfo := getBlockDeviceMounts, getStagedBlockDevices, getNVMeInfoFromDevice
	t.Cleanup(func() {
		getBlockDeviceMounts, getStagedBlockDevices, getNVMeInfoFromDevice = origMounted, origStaged, origInfo
	})
	root := filepath.Join(kubeletCSIStagingRoot, "org.scale.csi.nvmeof")
	mounts := map[string][]string{}
	for device := range nqns {
		mounts[device] = []string{filepath.Join(root, device, "globalmount")}
	}
	mounts["/dev/nvme9n1"] = []string{"/var/lib/kubelet/plugins/kubernetes.io/csi/other.driver/x/globalmount"}
	getBlockDeviceMounts = func() (map[string][]string, error) { return mounts, nil }
	getStagedBlockDevices = func() (map[string]string, error) { return map[string]string{}, nil }
	getNVMeInfoFromDevice = func(device string) (string, error) {
		if device == "/dev/nvme9n1" {
			return "nqn.someone-else", nil
		}
		return nqns[device], nil
	}
}

func TestReconcileNVMeoFMaxSectorsCoversOnlyThisDriversSubsystems(t *testing.T) {
	withStagedNQNs(t, map[string]string{"/dev/nvme1n1": "nqn.a", "/dev/nvme2n1": "nqn.b"})
	originalClamp := nodeClampNVMeMaxSectors
	t.Cleanup(func() { nodeClampNVMeMaxSectors = originalClamp })
	var mu sync.Mutex
	got := map[string]util.NVMeMaxSectorsClampOptions{}
	nodeClampNVMeMaxSectors = func(_ context.Context, nqn string, opts util.NVMeMaxSectorsClampOptions) (*util.NVMeMaxSectorsClampResult, error) {
		mu.Lock()
		defer mu.Unlock()
		got[nqn] = opts
		if nqn == "nqn.b" {
			return &util.NVMeMaxSectorsClampResult{NQN: nqn}, util.ErrNVMeMaxSectorsClamp
		}
		return &util.NVMeMaxSectorsClampResult{NQN: nqn, Devices: []util.NVMeMaxSectorsDevice{{Name: "nvme1c4n1", BeforeKB: 32768, AfterKB: 4096}}}, nil
	}
	d := newTestNodeDriver(ShareTypeNVMeoF)
	corrected := testutilCounterValue(t, "max_sectors_kb", "corrected")
	failed := testutilCounterValue(t, "max_sectors_kb", "error")

	d.reconcileNVMeoFMaxSectors(context.Background(), false)

	assert.Len(t, got, 2, "both staged subsystems, and not another driver's")
	assert.Contains(t, got, "nqn.a")
	assert.Contains(t, got, "nqn.b")
	assert.Equal(t, time.Duration(0), got["nqn.a"].PathWait, "the periodic pass never waits")
	assert.InDelta(t, corrected+1, testutilCounterValue(t, "max_sectors_kb", "corrected"), 0)
	assert.InDelta(t, failed+1, testutilCounterValue(t, "max_sectors_kb", "error"), 0, "a failing subsystem does not stop the pass")

	got = map[string]util.NVMeMaxSectorsClampOptions{}
	d.reconcileNVMeoFMaxSectors(context.Background(), true)
	assert.Empty(t, got, "dry run writes nothing")

	d.config.NVMeoF.MaxSectorsKB = "off"
	d.reconcileNVMeoFMaxSectors(context.Background(), false)
	assert.Empty(t, got, "off disables the pass")
}

func testutilCounterValue(t *testing.T, tunable, result string) float64 {
	t.Helper()
	return testutil.ToFloat64(nvmeControllerTunableCorrections.WithLabelValues(tunable, result))
}

func TestLoadConfigMaxSectorsKB(t *testing.T) {
	for raw, want := range map[string]util.NVMeMaxSectorsPolicy{
		"":                       {Mode: util.NVMeMaxSectorsAuto},
		"  maxSectorsKB: auto":   {Mode: util.NVMeMaxSectorsAuto},
		"  maxSectorsKB: off":    {Mode: util.NVMeMaxSectorsOff},
		"  maxSectorsKB: 0":      {Mode: util.NVMeMaxSectorsOff},
		"  maxSectorsKB: false":  {Mode: util.NVMeMaxSectorsOff},
		"  maxSectorsKB: 4096":   {Mode: util.NVMeMaxSectorsFixed, LimitKB: 4096},
		`  maxSectorsKB: "2048"`: {Mode: util.NVMeMaxSectorsFixed, LimitKB: 2048},
	} {
		cfg, err := loadTestConfig(t, requiredTestConfig+"nfs:\n  shareHost: 192.0.2.10\nnvmeof:\n  enabled: false\n"+raw+"\n")
		require.NoError(t, err, raw)
		d := &Driver{config: cfg}
		assert.Equal(t, want, d.nvmeMaxSectorsPolicy(), raw)
	}
	for _, bad := range []string{"3", "-1", "4k", "on"} {
		_, err := loadTestConfig(t, requiredTestConfig+"nfs:\n  shareHost: 192.0.2.10\nnvmeof:\n  enabled: false\n  maxSectorsKB: "+bad+"\n")
		require.Error(t, err, bad)
		assert.Contains(t, err.Error(), "maxSectorsKB", bad)
	}
}

// NodeStageVolume's compatible-replay path (the volume is already mounted at
// the staging path, by this plugin, an older release or the Rust agent)
// converges the clamp, and fails the replay when it cannot.
func TestNodeStageVolumeCompatibleReplayClamps(t *testing.T) {
	for _, clampErr := range []error{nil, fmt.Errorf("%w: boom", util.ErrNVMeMaxSectorsClamp)} {
		t.Run(fmt.Sprintf("clampErr=%v", clampErr), func(t *testing.T) {
			steps, calls, _ := recordStageSteps(t, clampErr)
			originalIsMounted, originalInfo, originalNQN := nodeIsMounted, nodeGetMountInfo, nodeGetNVMeInfo
			t.Cleanup(func() {
				nodeIsMounted, nodeGetMountInfo, nodeGetNVMeInfo = originalIsMounted, originalInfo, originalNQN
			})
			nodeIsMounted = func(string) (bool, error) { return true, nil }
			nodeGetMountInfo = func(path string) (util.MountInfo, error) {
				return util.MountInfo{Source: "/dev/nvme3n1", Target: path, FSType: "ext4"}, nil
			}
			nodeGetNVMeInfo = func(string) (string, error) { return "nqn.test:clamp", nil }
			d := newTestNodeDriver(ShareTypeNVMeoF)
			volumeContext := map[string]string{"node_attach_driver": "nvmeof"}
			for k, v := range nvmeStageContext {
				volumeContext[k] = v
			}

			_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
				VolumeId:          "pool/test/vol",
				StagingTargetPath: t.TempDir(),
				VolumeContext:     volumeContext,
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
				},
			})

			assert.NotContains(t, *steps, "connect")
			assert.NotContains(t, *steps, "format+mount")
			require.Len(t, *calls, 1, "the replay clamps")
			assert.Equal(t, "nqn.test:clamp", (*calls)[0].nqn)
			if clampErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, codes.Internal, status.Code(err))
			}
		})
	}
}
