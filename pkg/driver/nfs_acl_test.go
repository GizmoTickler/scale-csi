package driver

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func TestParseNFSACLOptions(t *testing.T) {
	t.Run("no parameters is a no-op", func(t *testing.T) {
		opts, err := parseNFSACLOptions(map[string]string{"protocol": "nfs"})
		require.NoError(t, err)
		assert.True(t, opts.empty())
		assert.Equal(t, context.Background(), withNFSACLOptions(context.Background(), opts))
	})

	t.Run("builtin template is accepted case-insensitively", func(t *testing.T) {
		opts, err := parseNFSACLOptions(map[string]string{nfsACLTemplateParam: "nfs4_restricted"})
		require.NoError(t, err)
		assert.Equal(t, "NFS4_RESTRICTED", opts.template)
		assert.False(t, opts.empty())
	})

	t.Run("unknown template is rejected", func(t *testing.T) {
		_, err := parseNFSACLOptions(map[string]string{nfsACLTemplateParam: "NFS4_WIDE_OPEN"})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "NFS4_RESTRICTED")
	})

	t.Run("inline dacl is parsed", func(t *testing.T) {
		opts, err := parseNFSACLOptions(map[string]string{
			nfsACLParam: `[{"tag":"owner@","type":"ALLOW","perms":{"BASIC":"FULL_CONTROL"}}]`,
		})
		require.NoError(t, err)
		require.Len(t, opts.dacl, 1)
		assert.Equal(t, "owner@", opts.dacl[0].Tag)
	})

	t.Run("malformed inline dacl is rejected", func(t *testing.T) {
		for _, raw := range []string{`not json`, `[]`, `[{"type":"ALLOW"}]`} {
			_, err := parseNFSACLOptions(map[string]string{nfsACLParam: raw})
			require.Error(t, err, raw)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		}
	})

	t.Run("template and inline dacl are mutually exclusive", func(t *testing.T) {
		_, err := parseNFSACLOptions(map[string]string{
			nfsACLTemplateParam: "NFS4_OPEN",
			nfsACLParam:         `[{"tag":"owner@"}]`,
		})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}

// TestApplyDatasetACLParamsDefaultOff proves the dataset create payload is
// untouched unless an ACL was requested — the acltype/aclmode keep inheriting
// from the parent exactly as before GF5.
func TestApplyDatasetACLParamsDefaultOff(t *testing.T) {
	base := truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"}

	params := base
	applyDatasetACLParams(&params, nil)
	assert.Equal(t, base, params)
	applyDatasetACLParams(&params, &nfsACLOptions{})
	assert.Equal(t, base, params)

	applyDatasetACLParams(&params, &nfsACLOptions{template: "NFS4_RESTRICTED"})
	assert.Equal(t, "NFSV4", params.Acltype)
	assert.Equal(t, "PASSTHROUGH", params.Aclmode)

	// Zvols have no filesystem ACL; the stamp must not leak onto them.
	zvol := truenas.DatasetCreateParams{Name: "tank/k8s/zvol", Type: "VOLUME"}
	applyDatasetACLParams(&zvol, &nfsACLOptions{template: "NFS4_RESTRICTED"})
	assert.Empty(t, zvol.Acltype)
	assert.Empty(t, zvol.Aclmode)

	// An explicit operator-set acltype (via zfs.datasetProperties) is respected.
	explicit := truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM", Acltype: "POSIX", Aclmode: "RESTRICTED"}
	applyDatasetACLParams(&explicit, &nfsACLOptions{template: "NFS4_OPEN"})
	assert.Equal(t, "POSIX", explicit.Acltype)
	assert.Equal(t, "RESTRICTED", explicit.Aclmode)
}

func aclTestDriver(t *testing.T, mock *truenas.MockClient) (*Driver, *record.FakeRecorder) {
	t.Helper()
	recorder := record.NewFakeRecorder(16)
	return &Driver{
		name: "org.scale.csi",
		config: &Config{
			DriverName: "org.scale.csi",
			ZFS:        ZFSConfig{DatasetParentName: "tank/k8s"},
			NFS:        NFSConfig{Enabled: true, ShareHost: "10.0.0.1"},
		},
		truenasClient: mock,
		eventRecorder: &EventRecorder{recorder: recorder, enabled: true},
	}, recorder
}

// aclEventRef is the PVC the ACL events are attributed to in tests.
func aclEventRef() runtime.Object { return PVCRef("storage", "acl-claim") }

func drainEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		case <-time.After(50 * time.Millisecond):
			return events
		}
	}
}

func TestApplyNFSVolumeACL(t *testing.T) {
	ds := &truenas.Dataset{Name: "tank/k8s/vol", Mountpoint: "/mnt/tank/k8s/vol"}

	t.Run("no ACL requested: zero setacl calls, zero events", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d, recorder := aclTestDriver(t, mock)
		d.applyNFSVolumeACL(context.Background(), ds, "tank/k8s/vol", aclEventRef())
		assert.Empty(t, mock.SetACLCalls)
		assert.Empty(t, drainEvents(recorder))
	})

	t.Run("template resolves and applies with protected=true", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d, recorder := aclTestDriver(t, mock)
		ctx := withNFSACLOptions(context.Background(), &nfsACLOptions{template: "NFS4_RESTRICTED"})

		d.applyNFSVolumeACL(ctx, ds, "tank/k8s/vol", aclEventRef())
		require.Len(t, mock.SetACLCalls, 1)
		call := mock.SetACLCalls[0]
		assert.Equal(t, "/mnt/tank/k8s/vol", call.Path)
		assert.NotEmpty(t, call.DACL)
		assert.True(t, call.NFS41Flags["protected"], "protected must be set so a chmod cannot recompute the ACL away")

		// The applied ACL is non-trivial afterwards.
		acl, err := mock.FilesystemGetACL(context.Background(), "/mnt/tank/k8s/vol")
		require.NoError(t, err)
		assert.False(t, acl.Trivial)

		events := drainEvents(recorder)
		require.Len(t, events, 2, "expected an Applied event plus the fsGroup hazard warning")
		assert.Contains(t, events[0], EventReasonNFSACLApplied)
		assert.Contains(t, events[1], "Warning "+EventReasonNFSACLFsGroup)
		assert.Contains(t, events[1], "fsGroupPolicy")
	})

	t.Run("inline dacl is applied verbatim", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d, _ := aclTestDriver(t, mock)
		dacl := []truenas.ACLEntry{{Tag: "owner@", Type: "ALLOW", Perms: map[string]interface{}{"BASIC": "FULL_CONTROL"}}}
		ctx := withNFSACLOptions(context.Background(), &nfsACLOptions{dacl: dacl})

		d.applyNFSVolumeACL(ctx, ds, "tank/k8s/vol", aclEventRef())
		require.Len(t, mock.SetACLCalls, 1)
		assert.Equal(t, dacl, mock.SetACLCalls[0].DACL)
	})

	t.Run("a setacl job failure warns but never blocks provisioning", func(t *testing.T) {
		mock := truenas.NewMockClient()
		mock.InjectACLError = errors.New("simulated setacl job failure")
		d, recorder := aclTestDriver(t, mock)
		ctx := withNFSACLOptions(context.Background(), &nfsACLOptions{dacl: []truenas.ACLEntry{{Tag: "owner@"}}})

		// Returns nothing: an ACL failure cannot fail CreateVolume.
		d.applyNFSVolumeACL(ctx, ds, "tank/k8s/vol", aclEventRef())
		events := drainEvents(recorder)
		require.Len(t, events, 1)
		assert.Contains(t, events[0], "Warning "+EventReasonNFSACLFailed)
	})

	t.Run("a mountpoint-less dataset warns instead of dispatching", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d, recorder := aclTestDriver(t, mock)
		ctx := withNFSACLOptions(context.Background(), &nfsACLOptions{template: "NFS4_OPEN"})

		d.applyNFSVolumeACL(ctx, &truenas.Dataset{Name: "tank/k8s/vol"}, "tank/k8s/vol", aclEventRef())
		assert.Empty(t, mock.SetACLCalls)
		events := drainEvents(recorder)
		require.Len(t, events, 1)
		assert.Contains(t, events[0], "Warning "+EventReasonNFSACLFailed)
	})
}

// TestCreateVolumeRejectsInvalidACLParameter proves the ACL parameters are
// validated at the CreateVolume boundary, before any backend mutation.
func TestCreateVolumeRejectsInvalidACLParameter(t *testing.T) {
	mock := truenas.NewMockClient()
	d, _ := aclTestDriver(t, mock)

	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "acl-bad",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs", nfsACLTemplateParam: "NOT_A_TEMPLATE"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Empty(t, mock.Datasets, "an invalid ACL parameter must be rejected before any dataset is created")
}

// TestCreateVolumeAppliesACLEndToEnd exercises the full create path: the dataset
// is stamped acltype=NFSV4 and the ACL lands on its mountpoint.
func TestCreateVolumeAppliesACLEndToEnd(t *testing.T) {
	mock := truenas.NewMockClient()
	d, recorder := aclTestDriver(t, mock)

	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "acl-vol",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters: map[string]string{
			"protocol": "nfs", nfsACLTemplateParam: "NFS4_RESTRICTED",
			pvcNamespaceKey: "storage", pvcNameKey: "acl-claim",
		},
	})
	require.NoError(t, err)
	require.Len(t, mock.SetACLCalls, 1)
	assert.True(t, mock.SetACLCalls[0].NFS41Flags["protected"])

	sawFsGroupWarning := false

	for _, event := range drainEvents(recorder) {
		assert.NotContains(t, event, EventReasonNFSACLFailed)
		if strings.Contains(event, EventReasonNFSACLFsGroup) {
			sawFsGroupWarning = true
		}
	}
	assert.True(t, sawFsGroupWarning, "an ACL-carrying volume must warn about the fsGroupPolicy=File clobber")
}

// TestCreateVolumeWithoutACLTouchesNothing is the default-off guard for the
// whole epic: a normal NFS volume issues no ACL call at all.
func TestCreateVolumeWithoutACLTouchesNothing(t *testing.T) {
	mock := truenas.NewMockClient()
	d, _ := aclTestDriver(t, mock)

	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "plain-vol",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs"},
	})
	require.NoError(t, err)
	assert.Empty(t, mock.SetACLCalls)
	require.NotNil(t, mock.Datasets["tank/k8s/plain-vol"])
}
