package driver

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

func installGF6ISCSINodeSeams(t *testing.T) {
	t.Helper()
	installFakeNodeCommands(t, "findmnt")
	originalList := listISCSISessions
	originalConnect := iscsiConnectWithSessions
	originalPrerequisites := nodeCheckISCSIMultipathHost
	originalWWID := nodeGetSCSIWWID
	originalMapWWID := nodeGetISCSIMultipathWWID
	originalFindMap := nodeFindISCSIMultipath
	originalOwnership := nodeCheckISCSIMultipath
	originalDisconnect := nodeISCSIDisconnect
	originalStagedDevice := nodeStagedDevicePath
	t.Cleanup(func() {
		listISCSISessions = originalList
		iscsiConnectWithSessions = originalConnect
		nodeCheckISCSIMultipathHost = originalPrerequisites
		nodeGetSCSIWWID = originalWWID
		nodeGetISCSIMultipathWWID = originalMapWWID
		nodeFindISCSIMultipath = originalFindMap
		nodeCheckISCSIMultipath = originalOwnership
		nodeISCSIDisconnect = originalDisconnect
		nodeStagedDevicePath = originalStagedDevice
	})
	listISCSISessions = func() ([]util.ISCSISessionInfo, error) { return nil, nil }
	nodeCheckISCSIMultipathHost = func() error { return nil }
	nodeGetSCSIWWID = func(string) (string, error) { return "36001405a123456789abcdef000000001", nil }
	nodeFindISCSIMultipath = func(string) (string, error) { return "/dev/null", nil }
	nodeCheckISCSIMultipath = func(string) error { return nil }
	nodeISCSIDisconnect = func(string, string) error { return nil }
}

func gf6ISCSIBlockCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}

func TestStageISCSIMultipathLogsIntoEveryPortalAndDegradesOnPartialFailure(t *testing.T) {
	installGF6ISCSINodeSeams(t)
	portals := []string{"192.0.2.10:3260", "192.0.2.11:3260", "192.0.2.12:3260"}
	var connected []string
	iscsiConnectWithSessions = func(_ context.Context, portal, _ string, _ int, _ *util.ISCSIConnectOptions, _ []util.ISCSISessionInfo) (string, error) {
		connected = append(connected, portal)
		if portal == "192.0.2.11:3260" {
			return "", errors.New("injected storage VLAN failure")
		}
		return "/dev/sda", nil
	}
	recorder := record.NewFakeRecorder(2)
	d := newTestNodeDriver(ShareTypeISCSI)
	d.eventRecorder = &EventRecorder{recorder: recorder, enabled: true}

	stageErr := d.stageISCSIVolume(context.Background(), map[string]string{
		"portal":  portals[0],
		"portals": `["192.0.2.10:3260","192.0.2.11:3260","192.0.2.12:3260"]`,
		"iqn":     "iqn.2005-10.org.freenas.ctl:pvc-gf6",
		"lun":     "0",
	}, nil, filepath.Join(t.TempDir(), "stage"), gf6ISCSIBlockCapability(), PVRef("gf6-iscsi"))
	require.NoError(t, stageErr)
	assert.Equal(t, portals, connected, "the first portal is mandatory and every remaining portal is attempted")
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonISCSIPathDegraded)
		assert.Contains(t, event, "192.0.2.11:3260")
	default:
		t.Fatal("partial iSCSI path failure must emit a degraded event")
	}
}

func TestStageISCSIMultipathUnavailableFallsBackToPrimaryOnly(t *testing.T) {
	installGF6ISCSINodeSeams(t)
	nodeCheckISCSIMultipathHost = func() error { return errors.New("multipathd socket absent") }
	var connected []string
	iscsiConnectWithSessions = func(_ context.Context, portal, _ string, _ int, _ *util.ISCSIConnectOptions, _ []util.ISCSISessionInfo) (string, error) {
		connected = append(connected, portal)
		return "/dev/null", nil
	}
	recorder := record.NewFakeRecorder(1)
	d := newTestNodeDriver(ShareTypeISCSI)
	d.eventRecorder = &EventRecorder{recorder: recorder, enabled: true}

	stageErr := d.stageISCSIVolume(context.Background(), map[string]string{
		"portal":  "192.0.2.20:3260",
		"portals": `["192.0.2.20:3260","192.0.2.21:3260"]`,
		"iqn":     "iqn.2005-10.org.freenas.ctl:pvc-fallback",
		"lun":     "0",
	}, nil, filepath.Join(t.TempDir(), "stage"), gf6ISCSIBlockCapability(), PVRef("gf6-fallback"))
	require.NoError(t, stageErr)
	assert.Equal(t, []string{"192.0.2.20:3260"}, connected)
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonISCSIMultipathUnavailable)
	default:
		t.Fatal("missing dm-multipath prerequisites must be observable")
	}
}

func TestStageISCSIMultipathLogsOutMismatchedWWIDPath(t *testing.T) {
	installGF6ISCSINodeSeams(t)
	iscsiConnectWithSessions = func(_ context.Context, portal, _ string, _ int, _ *util.ISCSIConnectOptions, _ []util.ISCSISessionInfo) (string, error) {
		if portal == "192.0.2.61:3260" {
			return "/dev/sdb", nil
		}
		return "/dev/sda", nil
	}
	nodeGetSCSIWWID = func(devicePath string) (string, error) {
		if devicePath == "/dev/sdb" {
			return "36001405a000000000000000000000099", nil
		}
		return "36001405a000000000000000000000001", nil
	}
	var disconnected []string
	nodeISCSIDisconnect = func(portal, _ string) error {
		disconnected = append(disconnected, portal)
		return nil
	}
	d := newTestNodeDriver(ShareTypeISCSI)

	stageErr := d.stageISCSIVolume(context.Background(), map[string]string{
		"portal":  "192.0.2.60:3260",
		"portals": `["192.0.2.60:3260","192.0.2.61:3260"]`,
		"iqn":     "iqn.2005-10.org.freenas.ctl:pvc-wwid-mismatch",
		"lun":     "0",
	}, nil, filepath.Join(t.TempDir(), "stage"), gf6ISCSIBlockCapability())
	require.NoError(t, stageErr)
	assert.Equal(t, []string{"192.0.2.61:3260"}, disconnected)
}

func TestStageISCSIWithoutHintPreservesLegacySinglePortalPath(t *testing.T) {
	installGF6ISCSINodeSeams(t)
	nodeCheckISCSIMultipathHost = func() error { t.Fatal("legacy path must not inspect dm-multipath prerequisites"); return nil }
	var connected []string
	iscsiConnectWithSessions = func(_ context.Context, portal, _ string, _ int, _ *util.ISCSIConnectOptions, _ []util.ISCSISessionInfo) (string, error) {
		connected = append(connected, portal)
		return "/dev/null", nil
	}
	d := newTestNodeDriver(ShareTypeISCSI)
	require.NoError(t, d.stageISCSIVolume(context.Background(), map[string]string{
		"portal": "192.0.2.30:3260",
		"iqn":    "iqn.2005-10.org.freenas.ctl:pvc-legacy",
		"lun":    "0",
	}, nil, filepath.Join(t.TempDir(), "stage"), gf6ISCSIBlockCapability()))
	assert.Equal(t, []string{"192.0.2.30:3260"}, connected)
}

func TestEmptyISCSIPublishHintWinsAndDegradesToSinglePath(t *testing.T) {
	installGF6ISCSINodeSeams(t)
	var connected []string
	iscsiConnectWithSessions = func(_ context.Context, portal, _ string, _ int, _ *util.ISCSIConnectOptions, _ []util.ISCSISessionInfo) (string, error) {
		connected = append(connected, portal)
		return "/dev/null", nil
	}
	recorder := record.NewFakeRecorder(1)
	d := newTestNodeDriver(ShareTypeISCSI)
	d.eventRecorder = &EventRecorder{recorder: recorder, enabled: true}
	volumeContext := map[string]string{
		"portal":  "192.0.2.32:3260",
		"portals": `["192.0.2.32:3260","192.0.2.33:3260"]`,
		"iqn":     "iqn.2005-10.org.freenas.ctl:pvc-empty-publish-iscsi",
		"lun":     "0",
	}
	selected := publishHintStageContext(volumeContext, map[string]string{"portals": ""}, "portals")

	require.NoError(t, d.stageISCSIVolume(context.Background(), selected, nil,
		filepath.Join(t.TempDir(), "stage"), gf6ISCSIBlockCapability(), PVRef("gf6-empty-iscsi")))
	assert.Equal(t, []string{"192.0.2.32:3260"}, connected)
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonISCSIPathDegraded)
		assert.Contains(t, event, "using the primary portal only")
	default:
		t.Fatal("empty winning iSCSI publish hint must be observably degraded")
	}
}

func TestConvergeExistingISCSIPathsRefusesRawStagedDeviceWithoutAddingSessions(t *testing.T) {
	installGF6ISCSINodeSeams(t)
	nodeStagedDevicePath = func(string) (string, bool) { return "/dev/sda", true }
	nodeGetISCSIMultipathWWID = func(string) (string, error) { return "", errors.New("raw component") }
	listISCSISessions = func() ([]util.ISCSISessionInfo, error) {
		t.Fatal("raw-stage refusal must happen before listing or adding sessions")
		return nil, nil
	}
	iscsiConnectWithSessions = func(context.Context, string, string, int, *util.ISCSIConnectOptions, []util.ISCSISessionInfo) (string, error) {
		t.Fatal("raw-stage refusal must not connect a secondary session")
		return "", nil
	}
	recorder := record.NewFakeRecorder(1)
	d := newTestNodeDriver(ShareTypeISCSI)
	d.eventRecorder = &EventRecorder{recorder: recorder, enabled: true}
	d.convergeExistingISCSIPaths(context.Background(), map[string]string{
		"portal":  "192.0.2.34:3260",
		"portals": `["192.0.2.34:3260","192.0.2.35:3260"]`,
		"iqn":     "iqn.2005-10.org.freenas.ctl:pvc-raw-restage",
		"lun":     "0",
	}, nil, "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/staging/gf6-raw", PVRef("gf6-raw-restage"))
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonISCSIMultipathUnavailable)
		assert.Contains(t, event, "full unstage/restage")
	default:
		t.Fatal("raw-stage refusal must be observable")
	}
}

func TestConvergeExistingISCSIPathsTopsUpExistingDMMap(t *testing.T) {
	installGF6ISCSINodeSeams(t)
	const wwid = "36001405a123456789abcdef000000035"
	nodeStagedDevicePath = func(string) (string, bool) { return "/dev/dm-5", true }
	mapWWIDCalls := 0
	nodeGetISCSIMultipathWWID = func(devicePath string) (string, error) {
		mapWWIDCalls++
		assert.Equal(t, "/dev/dm-5", devicePath)
		return wwid, nil
	}
	nodeFindISCSIMultipath = func(gotWWID string) (string, error) {
		assert.Equal(t, wwid, gotWWID)
		return "/dev/dm-5", nil
	}
	nodeGetSCSIWWID = func(string) (string, error) { return wwid, nil }
	const iqn = "iqn.2005-10.org.freenas.ctl:pvc-dm-restage"
	listISCSISessions = func() ([]util.ISCSISessionInfo, error) {
		return []util.ISCSISessionInfo{{Portal: "192.0.2.36:3260", IQN: iqn, SessionID: "36"}}, nil
	}
	var connected []string
	iscsiConnectWithSessions = func(_ context.Context, portal, _ string, _ int, _ *util.ISCSIConnectOptions, _ []util.ISCSISessionInfo) (string, error) {
		connected = append(connected, portal)
		if portal == "192.0.2.36:3260" {
			return "/dev/sda", nil
		}
		return "/dev/sdb", nil
	}
	d := newTestNodeDriver(ShareTypeISCSI)
	d.convergeExistingISCSIPaths(context.Background(), map[string]string{
		"portal":  "192.0.2.36:3260",
		"portals": `["192.0.2.36:3260","192.0.2.37:3260"]`,
		"iqn":     iqn,
		"lun":     "0",
	}, nil, "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/staging/gf6-dm")
	assert.Equal(t, 1, mapWWIDCalls)
	assert.Equal(t, []string{"192.0.2.36:3260", "192.0.2.37:3260"}, connected)
}

func TestNodeStageISCSIPreExistingPVUsesPublishContextPortals(t *testing.T) {
	installGF6ISCSINodeSeams(t)
	originalInfo := getISCSIInfoFromDeviceWithSessions
	originalDeviceInfo := nodeGetISCSIInfo
	t.Cleanup(func() {
		getISCSIInfoFromDeviceWithSessions = originalInfo
		nodeGetISCSIInfo = originalDeviceInfo
	})
	getISCSIInfoFromDeviceWithSessions = func(string, []util.ISCSISessionInfo) (string, string, error) {
		return "192.0.2.70:3260", "iqn.2005-10.org.freenas.ctl:pvc-pre-existing", nil
	}
	nodeGetISCSIInfo = func(string) (string, string, error) {
		return "192.0.2.70:3260", "iqn.2005-10.org.freenas.ctl:pvc-pre-existing", nil
	}
	var connected []string
	iscsiConnectWithSessions = func(_ context.Context, portal, _ string, _ int, _ *util.ISCSIConnectOptions, _ []util.ISCSISessionInfo) (string, error) {
		connected = append(connected, portal)
		return "/dev/null", nil
	}
	d := newTestNodeDriver(ShareTypeISCSI)
	_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "gf6-pre-existing-iscsi",
		StagingTargetPath: filepath.Join(t.TempDir(), "stage"),
		VolumeContext: map[string]string{
			"node_attach_driver": "iscsi",
			"portal":             "192.0.2.70:3260",
			"iqn":                "iqn.2005-10.org.freenas.ctl:pvc-pre-existing",
			"lun":                "0",
		},
		PublishContext: map[string]string{
			"portals": `["192.0.2.70:3260","192.0.2.71:3260"]`,
		},
		VolumeCapability: gf6ISCSIBlockCapability(),
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.70:3260", "192.0.2.71:3260"}, connected)
}

func TestDisconnectAllISCSISessionsLogsOutEveryPortal(t *testing.T) {
	originalDisconnect := nodeISCSIDisconnect
	t.Cleanup(func() { nodeISCSIDisconnect = originalDisconnect })
	var portals []string
	nodeISCSIDisconnect = func(portal, _ string) error {
		portals = append(portals, portal)
		return nil
	}
	iqn := "iqn.2005-10.org.freenas.ctl:pvc-logout"
	succeeded := disconnectAllISCSISessionsForIQNWithSnapshot(iqn, []util.ISCSISessionInfo{
		{Portal: "192.0.2.40:3260", IQN: iqn},
		{Portal: "192.0.2.41:3260", IQN: iqn},
		{Portal: "192.0.2.41:3260", IQN: iqn},
		{Portal: "192.0.2.99:3260", IQN: "iqn.other"},
	})
	assert.True(t, succeeded)
	assert.Equal(t, []string{"192.0.2.40:3260", "192.0.2.41:3260"}, portals)
}

func TestConfiguredNFSMountFlagsExactRendering(t *testing.T) {
	nconnect := 4
	capability := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{
		Mount: &csi.VolumeCapability_MountVolume{MountFlags: []string{"hard", "timeo=600", "nconnect=2", "max_connect=9"}},
	}}
	assert.Equal(t,
		[]string{"hard", "timeo=600", "nconnect=4", "max_connect=3"},
		configuredNFSMountFlags(&nconnect, 3, capability),
	)
	assert.Equal(t,
		[]string{"hard", "timeo=600", "nconnect=2", "max_connect=9"},
		configuredNFSMountFlags(nil, 0, capability),
		"unset GF6 knobs must preserve the legacy mount option slice exactly",
	)
}

func TestNodeStageNFSPreExistingPVUsesPublishContextAddresses(t *testing.T) {
	installFakeNodeCommands(t, "findmnt")
	originalInfo := nodeGetMountInfo
	originalMount := nodeMountNFS
	originalUnmount := nodeUnmount
	t.Cleanup(func() {
		nodeGetMountInfo = originalInfo
		nodeMountNFS = originalMount
		nodeUnmount = originalUnmount
	})
	nodeGetMountInfo = func(string) (util.MountInfo, error) {
		return util.MountInfo{FSType: "nfs4", Options: []string{"rw", "vers=4.1"}}, nil
	}
	var sources []string
	var flags [][]string
	nodeMountNFS = func(_ context.Context, source, _ string, mountFlags []string) error {
		sources = append(sources, source)
		flags = append(flags, append([]string(nil), mountFlags...))
		return nil
	}
	nodeUnmount = func(context.Context, string) error { return nil }
	d := newTestNodeDriver(ShareTypeNFS)
	_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "gf6-pre-existing-nfs",
		StagingTargetPath: filepath.Join(t.TempDir(), "stage"),
		VolumeContext: map[string]string{
			"node_attach_driver": "nfs",
			"server":             "192.0.2.80",
			"share":              "/pool/pre-existing",
		},
		PublishContext: map[string]string{
			"addresses": `["192.0.2.80","192.0.2.81"]`,
		},
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{MountFlags: []string{"hard"}}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"192.0.2.80:/pool/pre-existing", "192.0.2.81:/pool/pre-existing"}, sources)
	assert.Equal(t, [][]string{{"hard", "max_connect=2"}, {"hard", "max_connect=2"}}, flags)
}

func TestEmptyNFSPublishHintWinsAndDegradesToSinglePath(t *testing.T) {
	installFakeNodeCommands(t, "findmnt")
	originalMount := nodeMountNFS
	t.Cleanup(func() { nodeMountNFS = originalMount })
	var sources []string
	nodeMountNFS = func(_ context.Context, source, _ string, _ []string) error {
		sources = append(sources, source)
		return nil
	}
	recorder := record.NewFakeRecorder(1)
	d := newTestNodeDriver(ShareTypeNFS)
	d.eventRecorder = &EventRecorder{recorder: recorder, enabled: true}
	volumeContext := map[string]string{
		"server":    "192.0.2.82",
		"share":     "/pool/empty-publish",
		"addresses": `["192.0.2.82","192.0.2.83"]`,
	}
	selected := publishHintStageContext(volumeContext, map[string]string{"addresses": ""}, "addresses")

	require.NoError(t, d.stageNFSVolume(context.Background(), selected, filepath.Join(t.TempDir(), "stage"),
		&csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}},
		PVRef("gf6-empty-nfs")))
	assert.Equal(t, []string{"192.0.2.82:/pool/empty-publish"}, sources)
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonNFSTrunkingDegraded)
		assert.Contains(t, event, "primary server only")
	default:
		t.Fatal("empty winning NFS publish hint must be observably degraded")
	}
}

func TestStageNFSLegacyIPv6SourceUsesBrackets(t *testing.T) {
	installFakeNodeCommands(t, "findmnt")
	originalMount := nodeMountNFS
	t.Cleanup(func() { nodeMountNFS = originalMount })
	var source string
	nodeMountNFS = func(_ context.Context, gotSource, _ string, _ []string) error {
		source = gotSource
		return nil
	}
	d := newTestNodeDriver(ShareTypeNFS)
	require.NoError(t, d.stageNFSVolume(context.Background(), map[string]string{
		"server": "2001:db8::84",
		"share":  "/pool/legacy-ipv6",
	}, filepath.Join(t.TempDir(), "stage"), &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
	}))
	assert.Equal(t, "[2001:db8::84]:/pool/legacy-ipv6", source)
}

func TestStageNFSTrunkingOptionFailureRetriesUntrunkedPrimary(t *testing.T) {
	installFakeNodeCommands(t, "findmnt")
	originalMount := nodeMountNFS
	t.Cleanup(func() { nodeMountNFS = originalMount })
	var flags [][]string
	nodeMountNFS = func(_ context.Context, _ string, _ string, mountFlags []string) error {
		flags = append(flags, append([]string(nil), mountFlags...))
		for _, flag := range mountFlags {
			if flag == "max_connect=2" {
				return errors.New("unknown mount option max_connect")
			}
		}
		return nil
	}
	recorder := record.NewFakeRecorder(1)
	d := newTestNodeDriver(ShareTypeNFS)
	d.eventRecorder = &EventRecorder{recorder: recorder, enabled: true}

	stageErr := d.stageNFSVolume(context.Background(), map[string]string{
		"server":    "192.0.2.90",
		"share":     "/pool/fallback",
		"addresses": `["192.0.2.90","192.0.2.91"]`,
	}, filepath.Join(t.TempDir(), "stage"), &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{MountFlags: []string{"hard"}}},
	}, PVRef("gf6-nfs-option-fallback"))
	require.NoError(t, stageErr)
	assert.Equal(t, [][]string{{"hard", "max_connect=2"}, {"hard"}}, flags)
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonNFSTrunkingUnavailable)
	default:
		t.Fatal("max_connect rejection must emit an unavailable event before untrunked fallback")
	}
}

func TestNFSTrunkingVersionFallbackAndPartialProbeFailure(t *testing.T) {
	originalInfo := nodeGetMountInfo
	originalMount := nodeMountNFS
	originalUnmount := nodeUnmount
	t.Cleanup(func() {
		nodeGetMountInfo = originalInfo
		nodeMountNFS = originalMount
		nodeUnmount = originalUnmount
	})
	d := newTestNodeDriver(ShareTypeNFS)
	recorder := record.NewFakeRecorder(3)
	d.eventRecorder = &EventRecorder{recorder: recorder, enabled: true}
	var sources []string
	nodeMountNFS = func(_ context.Context, source, _ string, _ []string) error {
		sources = append(sources, source)
		if source == "192.0.2.52:/pool/share" {
			return errors.New("injected secondary failure")
		}
		return nil
	}
	nodeUnmount = func(context.Context, string) error { return nil }
	stagePath := filepath.Join(t.TempDir(), "stage")
	addresses := []string{"192.0.2.50", "192.0.2.51", "192.0.2.52"}

	nodeGetMountInfo = func(string) (util.MountInfo, error) {
		return util.MountInfo{FSType: "nfs", Options: []string{"rw", "vers=4.0"}}, nil
	}
	d.convergeNFSTrunks(context.Background(), addresses, "/pool/share", stagePath, []string{"max_connect=3"}, PVRef("gf6-nfs-v40"))
	assert.Empty(t, sources, "NFS <4.1 remains mounted but must not attempt trunk transports")
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonNFSTrunkingUnavailable)
	default:
		t.Fatal("version fallback must emit an unavailable event")
	}

	nodeGetMountInfo = func(string) (util.MountInfo, error) {
		return util.MountInfo{FSType: "nfs", Options: []string{"rw", "vers=4.1"}}, nil
	}
	d.convergeNFSTrunks(context.Background(), addresses, "/pool/share", stagePath, []string{"max_connect=3"}, PVRef("gf6-nfs-v41"))
	assert.Equal(t, []string{"192.0.2.51:/pool/share", "192.0.2.52:/pool/share"}, sources)
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonNFSTrunkingDegraded)
	default:
		t.Fatal("partial NFS trunk failure must emit a degraded event")
	}
}
