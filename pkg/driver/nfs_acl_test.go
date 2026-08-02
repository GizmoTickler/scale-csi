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
		d.applyNFSVolumeACL(context.Background(), ds, "tank/k8s/vol", aclEventRef(), nil)
		assert.Empty(t, mock.SetACLCalls)
		assert.Empty(t, drainEvents(recorder))
	})

	t.Run("template resolves and applies with protected=true", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d, recorder := aclTestDriver(t, mock)
		ctx := withNFSACLOptions(context.Background(), &nfsACLOptions{template: "NFS4_RESTRICTED"})

		d.applyNFSVolumeACL(ctx, ds, "tank/k8s/vol", aclEventRef(), nil)
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

		d.applyNFSVolumeACL(ctx, ds, "tank/k8s/vol", aclEventRef(), nil)
		require.Len(t, mock.SetACLCalls, 1)
		assert.Equal(t, dacl, mock.SetACLCalls[0].DACL)
	})

	t.Run("a setacl job failure warns but never blocks provisioning", func(t *testing.T) {
		mock := truenas.NewMockClient()
		mock.InjectACLError = errors.New("simulated setacl job failure")
		d, recorder := aclTestDriver(t, mock)
		ctx := withNFSACLOptions(context.Background(), &nfsACLOptions{dacl: []truenas.ACLEntry{{Tag: "owner@"}}})

		// Returns nothing: an ACL failure cannot fail CreateVolume.
		d.applyNFSVolumeACL(ctx, ds, "tank/k8s/vol", aclEventRef(), nil)
		events := drainEvents(recorder)
		require.Len(t, events, 1)
		assert.Contains(t, events[0], "Warning "+EventReasonNFSACLFailed)
	})

	t.Run("a mountpoint-less dataset warns instead of dispatching", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d, recorder := aclTestDriver(t, mock)
		ctx := withNFSACLOptions(context.Background(), &nfsACLOptions{template: "NFS4_OPEN"})

		d.applyNFSVolumeACL(ctx, &truenas.Dataset{Name: "tank/k8s/vol"}, "tank/k8s/vol", aclEventRef(), nil)
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

// ---------------------------------------------------------------------------
// H3 — aclmode is the real chmod lever; nfs41_flags.protected is not
// ---------------------------------------------------------------------------

// TestNFSACLModeParameter covers the opt-in aclmode selector. The DEFAULT stays
// PASSTHROUGH: flipping it would turn a silent, recoverable ACL degradation into
// a hard publish failure for every fsGroup Pod (and for any in-container chmod)
// on an explicitly best-effort feature.
func TestNFSACLModeParameter(t *testing.T) {
	t.Run("unset resolves to the unchanged historical default", func(t *testing.T) {
		opts, err := parseNFSACLOptions(map[string]string{nfsACLTemplateParam: "NFS4_RESTRICTED"})
		require.NoError(t, err)
		assert.Empty(t, opts.aclMode)
		assert.Equal(t, "PASSTHROUGH", opts.resolvedACLMode())

		params := &truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"}
		applyDatasetACLParams(params, opts)
		assert.Equal(t, "NFSV4", params.Acltype)
		assert.Equal(t, "PASSTHROUGH", params.Aclmode)
	})

	t.Run("RESTRICTED is honored on the create payload", func(t *testing.T) {
		opts, err := parseNFSACLOptions(map[string]string{
			nfsACLTemplateParam: "NFS4_RESTRICTED",
			nfsACLModeParam:     "restricted",
		})
		require.NoError(t, err)
		assert.Equal(t, "RESTRICTED", opts.resolvedACLMode())

		params := &truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"}
		applyDatasetACLParams(params, opts)
		assert.Equal(t, "RESTRICTED", params.Aclmode)
	})

	t.Run("DISCARD is not offered", func(t *testing.T) {
		_, err := parseNFSACLOptions(map[string]string{
			nfsACLTemplateParam: "NFS4_RESTRICTED",
			nfsACLModeParam:     "DISCARD",
		})
		require.Error(t, err, "DISCARD deletes the whole ACL on the first chmod and must not be selectable")
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("aclmode without an ACL is rejected", func(t *testing.T) {
		_, err := parseNFSACLOptions(map[string]string{nfsACLModeParam: "RESTRICTED"})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), nfsACLTemplateParam)
	})

	t.Run("no ACL parameter still leaves both properties inherited", func(t *testing.T) {
		params := &truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"}
		applyDatasetACLParams(params, &nfsACLOptions{})
		assert.Empty(t, params.Acltype)
		assert.Empty(t, params.Aclmode)
	})
}

// TestACLEventTextDoesNotOverclaimProtected is the H3 revert-proof on the
// operator-facing strings: nfs41_flags.protected is ACL4_PROTECTED
// (inheritance suppression), NOT a chmod guard, and neither the applied-event
// nor the fsGroup warning may claim otherwise.
func TestACLEventTextDoesNotOverclaimProtected(t *testing.T) {
	ctx := context.Background()
	mock := truenas.NewMockClient()
	d, recorder := aclTestDriver(t, mock)

	opts, err := parseNFSACLOptions(map[string]string{nfsACLTemplateParam: "NFS4_RESTRICTED"})
	require.NoError(t, err)
	d.applyNFSVolumeACL(withNFSACLOptions(ctx, opts), &truenas.Dataset{
		Name: "tank/k8s/vol", Mountpoint: "/mnt/tank/k8s/vol",
	}, "tank/k8s/vol", aclEventRef(), nil)

	messages := drainEvents(recorder)
	joined := strings.Join(messages, "\n")
	require.NotEmpty(t, joined)

	// The claim the reviewer proved false must not come back.
	assert.NotContains(t, joined, "chmod cannot",
		"protected does not stop a chmod; the docs and events must not say it does")
	assert.NotContains(t, joined, "recompute the ACL from the mode")
	// And the truth must be present.
	assert.Contains(t, joined, "aclmode=PASSTHROUGH")
	assert.Contains(t, joined, "not a chmod guard")
	assert.Contains(t, joined, "nfsACLMode=RESTRICTED",
		"the fsGroup warning must name the only ZFS lever that actually works")
}

// ---------------------------------------------------------------------------
// H3 round 2 — aclmode on CONTENT-SOURCE volumes
// ---------------------------------------------------------------------------

// aclContentSourceDriver provisions from the "pool/parent" tree the shared
// content-source helpers seed, and records events so the ACL claims can be read.
func aclContentSourceDriver(mock *truenas.MockClient) (*Driver, *record.FakeRecorder) {
	recorder := record.NewFakeRecorder(32)
	d := perfContentSourceDriver(mock)
	d.eventRecorder = &EventRecorder{recorder: recorder, enabled: true}
	return d, recorder
}

func aclContentSourceRequest(source *csi.VolumeContentSource, params map[string]string) *csi.CreateVolumeRequest {
	parameters := map[string]string{"protocol": "nfs"}
	for key, value := range params {
		parameters[key] = value
	}
	return &csi.CreateVolumeRequest{
		Name:                "acl-restored",
		CapacityRange:       &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:          parameters,
		VolumeContentSource: source,
	}
}

// TestNFSACLModeIsRejectedOnContentSourceRequests pins the first half of H3.
// aclmode is fixed in the pool.dataset.create payload, which a clone/restore
// never issues — so nfsACLMode cannot be applied to such a volume. Since the
// parameter exists ONLY to opt into the loud aclmode=RESTRICTED behavior,
// silently giving the operator whatever the ORIGIN had is the worst outcome
// available; the request is refused before anything is created.
func TestNFSACLModeIsRejectedOnContentSourceRequests(t *testing.T) {
	for _, tc := range contentSourceClassCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mock := truenas.NewMockClient()
			d, _ := aclContentSourceDriver(mock)
			d.config.ZFS.DetachedVolumesFromSnapshots = tc.detached
			seedStampedPerformanceClassSource(t, mock)

			_, err := d.CreateVolume(ctx, aclContentSourceRequest(tc.source, map[string]string{
				nfsACLTemplateParam: "NFS4_RESTRICTED",
				nfsACLModeParam:     "RESTRICTED",
			}))
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), nfsACLModeParam)
			_, getErr := mock.DatasetGet(ctx, "pool/parent/acl-restored")
			assert.Error(t, getErr, "the request must be refused BEFORE any mutation")

			// The default direction is refused identically: the driver cannot
			// establish PASSTHROUGH on such a volume either.
			_, err = d.CreateVolume(ctx, aclContentSourceRequest(tc.source, map[string]string{
				nfsACLTemplateParam: "NFS4_RESTRICTED",
				nfsACLModeParam:     "PASSTHROUGH",
			}))
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

// TestACLEventsOnContentSourceDoNotClaimAnAclmode pins the second half of H3.
// nfsACL / nfsACLTemplate stay ALLOWED on a restore (filesystem.setacl acts on
// the materialized path and genuinely applies, and a VolSync restore into an
// ACL-managed StorageClass has to keep working) — but the driver set no
// acltype/aclmode there, so the events must not report one as fact.
func TestACLEventsOnContentSourceDoNotClaimAnAclmode(t *testing.T) {
	for _, tc := range contentSourceClassCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mock := truenas.NewMockClient()
			d, recorder := aclContentSourceDriver(mock)
			d.config.ZFS.DetachedVolumesFromSnapshots = tc.detached
			seedStampedPerformanceClassSource(t, mock)

			_, err := d.CreateVolume(ctx, aclContentSourceRequest(tc.source, map[string]string{
				nfsACLTemplateParam: "NFS4_RESTRICTED",
			}))
			require.NoError(t, err, "the ACL itself still applies to a restored volume")

			joined := strings.Join(drainEvents(recorder), "\n")
			require.NotEmpty(t, joined)
			assert.NotContains(t, joined, "aclmode=PASSTHROUGH",
				"the driver did not set this volume's aclmode and must not report one as fact")
			assert.NotContains(t, joined, "aclmode=RESTRICTED")
			assert.NotContains(t, joined, "The dataset's aclmode is",
				"the fsGroup warning must not state an aclmode the driver never set")
			assert.Contains(t, joined, "NOT set by the driver")
			assert.Contains(t, joined, "inherits the ORIGIN dataset's acltype and aclmode")
		})
	}
}

// TestACLEventsOnFreshCreateStillReportTheAppliedMode is the positive half: on
// the ordinary path the reported mode IS the applied state — it is the value
// sent in the pool.dataset.create payload that created the dataset — so H3's fix
// cannot be mistaken for "stop reporting aclmode".
func TestACLEventsOnFreshCreateStillReportTheAppliedMode(t *testing.T) {
	ctx := context.Background()
	mock := truenas.NewMockClient()
	d, recorder := aclContentSourceDriver(mock)
	mustCreateParentDataset(t, mock)

	_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "acl-fresh",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters: map[string]string{
			"protocol":          "nfs",
			nfsACLTemplateParam: "NFS4_RESTRICTED",
			nfsACLModeParam:     "RESTRICTED",
		},
	})
	require.NoError(t, err)

	joined := strings.Join(drainEvents(recorder), "\n")
	assert.Contains(t, joined, "aclmode=RESTRICTED and acltype=NFSV4 set by the driver in the dataset create payload")
	assert.Contains(t, joined, "The dataset's aclmode is RESTRICTED")
	created := mock.Datasets["pool/parent/acl-fresh"]
	require.NotNil(t, created)
}
