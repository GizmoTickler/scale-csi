package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

func TestStageNVMeoFVolumeAlreadyLiveEmptyDeviceFailsWithoutSuccessMetric(t *testing.T) {
	installFakeNodeCommands(t, "findmnt")
	originalList := nodeListNVMeSubsystems
	originalConnect := nvmeConnectPathWithSubsystems
	originalPolicy := nodeSetNVMeIOPolicy
	t.Cleanup(func() {
		nodeListNVMeSubsystems = originalList
		nvmeConnectPathWithSubsystems = originalConnect
		nodeSetNVMeIOPolicy = originalPolicy
	})

	const nqn = "nqn.gf5:live-empty"
	subsystems := []util.NVMeSubsystem{{NQN: nqn, Name: "nvme-subsys4", Paths: []util.NVMePath{
		{Address: "traddr=192.0.2.44,trsvcid=4420", State: "live"},
	}}}
	nodeListNVMeSubsystems = func(context.Context) ([]util.NVMeSubsystem, error) { return subsystems, nil }
	deviceWaitCalls := 0
	nvmeConnectPathWithSubsystems = func(_ context.Context, gotNQN, uri string, _ *util.NVMeoFConnectOptions, got []util.NVMeSubsystem) (string, error) {
		deviceWaitCalls++
		assert.Equal(t, nqn, gotNQN)
		assert.Equal(t, "tcp://192.0.2.44:4420", uri)
		assert.Equal(t, subsystems, got)
		return "", nil
	}
	nodeSetNVMeIOPolicy = func(string, string) error {
		t.Fatal("iopolicy must not be set when no requested live path yields a device")
		return nil
	}

	d := newTestNodeDriver(ShareTypeNVMeoF)
	errorCounter := nodeConnectTotal.WithLabelValues("nvmeof", "error")
	successCounter := nodeConnectTotal.WithLabelValues("nvmeof", "success")
	errorBefore := testutil.ToFloat64(errorCounter)
	successBefore := testutil.ToFloat64(successCounter)
	err := d.stageNVMeoFVolume(context.Background(), map[string]string{
		"nqn": nqn, "address": "192.0.2.44", "addresses": `["192.0.2.44"]`,
	}, filepath.Join(t.TempDir(), "stage"), blockVolumeCapability())

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "no device path available")
	assert.Equal(t, 1, deviceWaitCalls)
	assert.Equal(t, errorBefore+1, testutil.ToFloat64(errorCounter))
	assert.Equal(t, successBefore, testutil.ToFloat64(successCounter))
}

func TestConvergeNVMeoFPathsRejectsEmptyDeviceFromSuccessfulConnect(t *testing.T) {
	originalConnect := nvmeConnectPathWithSubsystems
	t.Cleanup(func() { nvmeConnectPathWithSubsystems = originalConnect })
	nvmeConnectPathWithSubsystems = func(context.Context, string, string, *util.NVMeoFConnectOptions, []util.NVMeSubsystem) (string, error) {
		return "", nil
	}

	errorCounter := nvmePathConnectTotal.WithLabelValues("192.0.2.45", "error")
	successCounter := nvmePathConnectTotal.WithLabelValues("192.0.2.45", "success")
	errorBefore := testutil.ToFloat64(errorCounter)
	successBefore := testutil.ToFloat64(successCounter)
	devicePath, failures, err := convergeNVMeoFPaths(context.Background(), "nqn.gf5:empty-connect", "tcp", "4420",
		[]string{"192.0.2.45"}, &util.NVMeoFConnectOptions{DeviceTimeout: time.Second}, nil, true)

	require.Error(t, err)
	assert.Empty(t, devicePath)
	require.Len(t, failures, 1)
	assert.Contains(t, failures[0].Error(), "empty device path")
	assert.Equal(t, errorBefore+1, testutil.ToFloat64(errorCounter))
	assert.Equal(t, successBefore, testutil.ToFloat64(successCounter))
}

func TestConvergeNVMeoFPathsClampsSecondaryDeviceTimeoutToConfiguredValue(t *testing.T) {
	originalConnect := nvmeConnectPathWithSubsystems
	t.Cleanup(func() { nvmeConnectPathWithSubsystems = originalConnect })

	const configuredTimeout = 2 * time.Second
	connectCalls := 0
	nvmeConnectPathWithSubsystems = func(connectCtx context.Context, _ string, _ string, opts *util.NVMeoFConnectOptions, _ []util.NVMeSubsystem) (string, error) {
		connectCalls++
		if connectCalls == 2 {
			assert.Equal(t, configuredTimeout, opts.DeviceTimeout)
			deadline, bounded := connectCtx.Deadline()
			assert.True(t, bounded)
			assert.LessOrEqual(t, time.Until(deadline), nvmeSecondaryPathConvergeBudget+time.Second)
		}
		return "/dev/null", nil
	}

	devicePath, failures, err := convergeNVMeoFPaths(context.Background(), "nqn.gf5:short-timeout", "tcp", "4420",
		[]string{"192.0.2.46", "192.0.2.47"}, &util.NVMeoFConnectOptions{DeviceTimeout: configuredTimeout}, nil, true)
	require.NoError(t, err)
	assert.Equal(t, "/dev/null", devicePath)
	assert.Empty(t, failures)
	assert.Equal(t, 2, connectCalls)
}

func TestStageNVMeoFVolumeFreshStageWaitsForAlreadyLiveDevice(t *testing.T) {
	installFakeNodeCommands(t, "findmnt")
	originalList := nodeListNVMeSubsystems
	originalConnect := nvmeConnectPathWithSubsystems
	originalPolicy := nodeSetNVMeIOPolicy
	t.Cleanup(func() {
		nodeListNVMeSubsystems = originalList
		nvmeConnectPathWithSubsystems = originalConnect
		nodeSetNVMeIOPolicy = originalPolicy
	})

	const nqn = "nqn.gf5:live-recovery"
	subsystems := []util.NVMeSubsystem{{NQN: nqn, Name: "nvme-subsys5", Paths: []util.NVMePath{
		{Address: "traddr=192.0.2.48,trsvcid=4420", State: "live"},
	}}}
	nodeListNVMeSubsystems = func(context.Context) ([]util.NVMeSubsystem, error) { return subsystems, nil }
	deviceWaitCalls := 0
	nvmeConnectPathWithSubsystems = func(_ context.Context, _ string, uri string, _ *util.NVMeoFConnectOptions, _ []util.NVMeSubsystem) (string, error) {
		deviceWaitCalls++
		assert.Equal(t, "tcp://192.0.2.48:4420", uri)
		return "/dev/null", nil
	}
	nodeSetNVMeIOPolicy = func(subsystem, policy string) error {
		assert.Equal(t, "nvme-subsys5", subsystem)
		assert.Equal(t, "queue-depth", policy)
		return nil
	}

	stagingPath := filepath.Join(t.TempDir(), "stage")
	err := newTestNodeDriver(ShareTypeNVMeoF).stageNVMeoFVolume(context.Background(), map[string]string{
		"nqn": nqn, "address": "192.0.2.48", "addresses": `["192.0.2.48"]`,
	}, stagingPath, blockVolumeCapability())
	require.NoError(t, err)
	assert.Equal(t, 1, deviceWaitCalls, "the exact live controller needs a device wait but no new connect")
	target, readErr := os.Readlink(stagingPath)
	require.NoError(t, readErr)
	assert.Equal(t, "/dev/null", target)
}

func TestConvergeNVMeoFPathsTriesEveryAlreadyLiveDevice(t *testing.T) {
	originalConnect := nvmeConnectPathWithSubsystems
	t.Cleanup(func() { nvmeConnectPathWithSubsystems = originalConnect })

	const nqn = "nqn.gf5:live-fallback"
	subsystems := []util.NVMeSubsystem{{NQN: nqn, Paths: []util.NVMePath{
		{Address: "traddr=192.0.2.49,trsvcid=4420", State: "live"},
		{Address: "traddr=192.0.2.50,trsvcid=4420", State: "live"},
	}}}
	var waitedURIs []string
	nvmeConnectPathWithSubsystems = func(_ context.Context, _ string, uri string, _ *util.NVMeoFConnectOptions, _ []util.NVMeSubsystem) (string, error) {
		waitedURIs = append(waitedURIs, uri)
		if uri == "tcp://192.0.2.49:4420" {
			return "", nil
		}
		return "/dev/null", nil
	}

	devicePath, failures, err := convergeNVMeoFPaths(context.Background(), nqn, "tcp", "4420",
		[]string{"192.0.2.49", "192.0.2.50"}, &util.NVMeoFConnectOptions{DeviceTimeout: time.Second}, subsystems, true)
	require.NoError(t, err)
	assert.Equal(t, "/dev/null", devicePath)
	require.Len(t, failures, 1)
	assert.Contains(t, failures[0].Error(), "192.0.2.49")
	assert.Equal(t, []string{"tcp://192.0.2.49:4420", "tcp://192.0.2.50:4420"}, waitedURIs)
}

func TestConvergeExistingNVMeoFPathsReportsDegradedRestage(t *testing.T) {
	originalList := nodeListNVMeSubsystems
	originalConnect := nvmeConnectPathWithSubsystems
	originalPolicy := nodeSetNVMeIOPolicy
	t.Cleanup(func() {
		nodeListNVMeSubsystems = originalList
		nvmeConnectPathWithSubsystems = originalConnect
		nodeSetNVMeIOPolicy = originalPolicy
	})

	const nqn = "nqn.gf5:restage-degraded"
	subsystems := []util.NVMeSubsystem{{NQN: nqn, Name: "nvme-subsys6", Paths: []util.NVMePath{
		{Address: "traddr=192.0.2.51,trsvcid=4420", State: "live"},
	}}}
	nodeListNVMeSubsystems = func(context.Context) ([]util.NVMeSubsystem, error) { return subsystems, nil }
	nvmeConnectPathWithSubsystems = func(context.Context, string, string, *util.NVMeoFConnectOptions, []util.NVMeSubsystem) (string, error) {
		return "", errors.New("secondary path unavailable")
	}
	nodeSetNVMeIOPolicy = func(string, string) error { return nil }

	fakeRecorder := record.NewFakeRecorder(4)
	d := newTestNodeDriver(ShareTypeNVMeoF)
	d.eventRecorder = &EventRecorder{recorder: fakeRecorder, enabled: true}
	pathError := nvmePathConnectTotal.WithLabelValues("192.0.2.52", "error")
	pathErrorBefore := testutil.ToFloat64(pathError)
	d.convergeExistingNVMeoFPaths(context.Background(), map[string]string{
		"nqn": nqn, "address": "192.0.2.51", "addresses": `["192.0.2.51","192.0.2.52"]`,
	}, PVRef("gf5-restage-degraded"))

	assert.Equal(t, pathErrorBefore+1, testutil.ToFloat64(pathError))
	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonNVMePathDegraded)
		assert.Contains(t, event, "192.0.2.52")
	default:
		t.Fatal("steady-state convergence must emit the degraded-path event")
	}
}

func TestConvergeExistingNVMeoFPathsRelistsBeforeIOPolicy(t *testing.T) {
	originalList := nodeListNVMeSubsystems
	originalConnect := nvmeConnectPathWithSubsystems
	originalPolicy := nodeSetNVMeIOPolicy
	t.Cleanup(func() {
		nodeListNVMeSubsystems = originalList
		nvmeConnectPathWithSubsystems = originalConnect
		nodeSetNVMeIOPolicy = originalPolicy
	})

	const nqn = "nqn.gf5:restage-relist"
	listCalls := 0
	nodeListNVMeSubsystems = func(context.Context) ([]util.NVMeSubsystem, error) {
		listCalls++
		if listCalls == 1 {
			return nil, errors.New("transient list failure")
		}
		return []util.NVMeSubsystem{{NQN: nqn, Name: "nvme-subsys7"}}, nil
	}
	nvmeConnectPathWithSubsystems = func(context.Context, string, string, *util.NVMeoFConnectOptions, []util.NVMeSubsystem) (string, error) {
		return "", nil
	}
	policyCalls := 0
	nodeSetNVMeIOPolicy = func(subsystem, policy string) error {
		policyCalls++
		assert.Equal(t, "nvme-subsys7", subsystem)
		assert.Equal(t, "queue-depth", policy)
		return nil
	}

	newTestNodeDriver(ShareTypeNVMeoF).convergeExistingNVMeoFPaths(context.Background(), map[string]string{
		"nqn": nqn, "address": "192.0.2.53", "addresses": `["192.0.2.53"]`,
	})
	assert.Equal(t, 2, listCalls)
	assert.Equal(t, 1, policyCalls)
}

func TestMalformedNVMeoFMultipathFallbackIsObservable(t *testing.T) {
	fakeRecorder := record.NewFakeRecorder(4)
	d := newTestNodeDriver(ShareTypeNVMeoF)
	d.eventRecorder = &EventRecorder{recorder: fakeRecorder, enabled: true}
	errorCounter := nvmePathConnectTotal.WithLabelValues(invalidNVMeMultipathAddressMetricLabel, "error")
	errorBefore := testutil.ToFloat64(errorCounter)
	rawAddresses := `["192.0.2.54","storage.invalid"]`

	addresses := d.nodeNVMeMultipathAddresses(map[string]string{"addresses": rawAddresses}, "nqn.gf5:bad-publish", PVRef("gf5-bad-publish"))
	assert.Nil(t, addresses)
	assert.Equal(t, errorBefore+1, testutil.ToFloat64(errorCounter))
	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonNVMePathDegraded)
		assert.Contains(t, event, rawAddresses)
		assert.Contains(t, event, "single-address fallback")
	default:
		t.Fatal("discarding the advertised multipath list must emit a warning event")
	}
}

func TestStageNVMeoFVolumeWarnsWhenKernelDoesNotAggregateSubsystems(t *testing.T) {
	installFakeNodeCommands(t, "findmnt")
	originalRoot := nodeNVMeSubsystemSysfsRoot
	originalList := nodeListNVMeSubsystems
	originalConnect := nvmeConnectPathWithSubsystems
	originalPolicy := nodeSetNVMeIOPolicy
	t.Cleanup(func() {
		nodeNVMeSubsystemSysfsRoot = originalRoot
		nodeListNVMeSubsystems = originalList
		nvmeConnectPathWithSubsystems = originalConnect
		nodeSetNVMeIOPolicy = originalPolicy
	})

	const nqn = "nqn.gf5:unaggregated"
	nodeNVMeSubsystemSysfsRoot = t.TempDir()
	for name, subsystemNQN := range map[string]string{
		"nvme-subsys8":  nqn,
		"nvme-subsys9":  nqn,
		"nvme-subsys10": "nqn.gf5:other",
	} {
		dir := filepath.Join(nodeNVMeSubsystemSysfsRoot, name)
		require.NoError(t, os.MkdirAll(dir, 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "subsysnqn"), []byte(subsystemNQN+"\n"), 0o600))
	}
	listCalls := 0
	nodeListNVMeSubsystems = func(context.Context) ([]util.NVMeSubsystem, error) {
		listCalls++
		if listCalls == 1 {
			return nil, nil
		}
		return []util.NVMeSubsystem{{NQN: nqn, Name: "nvme-subsys8"}, {NQN: nqn, Name: "nvme-subsys9"}}, nil
	}
	nvmeConnectPathWithSubsystems = func(context.Context, string, string, *util.NVMeoFConnectOptions, []util.NVMeSubsystem) (string, error) {
		return "/dev/null", nil
	}
	var policySubsystems []string
	nodeSetNVMeIOPolicy = func(subsystem, policy string) error {
		assert.Equal(t, "queue-depth", policy)
		policySubsystems = append(policySubsystems, subsystem)
		return nil
	}

	fakeRecorder := record.NewFakeRecorder(4)
	d := newTestNodeDriver(ShareTypeNVMeoF)
	d.eventRecorder = &EventRecorder{recorder: fakeRecorder, enabled: true}
	err := d.stageNVMeoFVolume(context.Background(), map[string]string{
		"nqn": nqn, "address": "192.0.2.55", "addresses": `["192.0.2.55","192.0.2.56"]`,
	}, filepath.Join(t.TempDir(), "stage"), blockVolumeCapability(), PVRef("gf5-unaggregated"))
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"nvme-subsys8", "nvme-subsys9"}, policySubsystems)
	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonNVMeMultipathUnaggregated)
		assert.Contains(t, event, "nvme_core.multipath=Y")
	default:
		t.Fatal("split sysfs subsystem directories must emit an aggregation warning")
	}
}

func blockVolumeCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{
		Block: &csi.VolumeCapability_BlockVolume{},
	}}
}
