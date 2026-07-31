package driver

import (
	"context"
	"encoding/json"
	"testing"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func nfsOptionsTestDriver(t *testing.T, mock *truenas.MockClient, cfg NFSConfig) *Driver {
	t.Helper()
	cfg.Enabled = true
	if cfg.ShareHost == "" {
		cfg.ShareHost = "10.0.0.1"
	}
	return &Driver{
		name:          "org.scale.csi",
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "tank/k8s"}, NFS: cfg},
		truenasClient: mock,
	}
}

// TestNFSShareCreatePayloadUnchangedByDefault is the byte-identity guard for the
// share-create wire payload: with no GF5 option set, the marshaled
// sharing.nfs.create body must contain neither `security` nor `expose_snapshots`
// — exactly what the pre-GF5 driver sent.
func TestNFSShareCreatePayloadUnchangedByDefault(t *testing.T) {
	params := &truenas.NFSShareCreateParams{
		Path:         "/mnt/tank/k8s/vol",
		Comment:      "truenas-csi (org.scale.csi): tank/k8s/vol",
		MaprootUser:  "root",
		MaprootGroup: "wheel",
		Enabled:      true,
	}
	encoded, err := json.Marshal(params)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "security")
	assert.NotContains(t, string(encoded), "expose_snapshots")

	params.Security = []string{"SYS"}
	params.ExposeSnapshots = true
	encoded, err = json.Marshal(params)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"security":["SYS"]`)
	assert.Contains(t, string(encoded), `"expose_snapshots":true`)
}

// TestCreateNFSShareDefaultPayload proves the driver's actual create call is
// unchanged when nothing opts in, and that the global config defaults land.
func TestCreateNFSShareDefaultPayload(t *testing.T) {
	ctx := context.Background()

	t.Run("no options leaves security nil and expose_snapshots false", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{ShareMaprootUser: "root", ShareMaprootGroup: "wheel"})
		_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"})
		require.NoError(t, err)

		require.NoError(t, d.createNFSShareForDataset(ctx, nil, "tank/k8s/vol", "vol", true, nil))
		require.Len(t, mock.NFSShareCreateParams, 1)
		assert.Nil(t, mock.NFSShareCreateParams[0].Security)
		assert.False(t, mock.NFSShareCreateParams[0].ExposeSnapshots)
	})

	t.Run("global config defaults are applied", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{
			ShareSecurity:        []string{"SYS"},
			ShareExposeSnapshots: true,
		})
		_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"})
		require.NoError(t, err)

		require.NoError(t, d.createNFSShareForDataset(ctx, nil, "tank/k8s/vol", "vol", true, nil))
		require.Len(t, mock.NFSShareCreateParams, 1)
		assert.Equal(t, []string{"SYS"}, mock.NFSShareCreateParams[0].Security)
		assert.True(t, mock.NFSShareCreateParams[0].ExposeSnapshots)
	})

	t.Run("per-StorageClass options override the global default", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{ShareSecurity: []string{"SYS"}})
		_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"})
		require.NoError(t, err)

		_, err = d.parseNFSShareOptions(map[string]string{
			nfsSecurityParam:        "sys,krb5",
			nfsExposeSnapshotsParam: "true",
			nfsReadOnlyParam:        "true",
		})
		require.Error(t, err, "krb5 without nfs.krbEnabled must fail closed")

		d.config.NFS.KrbEnabled = true
		opts, err := d.parseNFSShareOptions(map[string]string{
			nfsSecurityParam:        "sys,krb5",
			nfsExposeSnapshotsParam: "true",
			nfsReadOnlyParam:        "true",
			nfsMapallUserParam:      "nobody",
		})
		require.NoError(t, err)

		require.NoError(t, d.createNFSShareForDataset(withNFSShareOptions(ctx, opts), nil, "tank/k8s/vol", "vol", true, nil))
		require.Len(t, mock.NFSShareCreateParams, 1)
		created := mock.NFSShareCreateParams[0]
		assert.Equal(t, []string{"SYS", "KRB5"}, created.Security)
		assert.True(t, created.ExposeSnapshots)
		assert.True(t, created.Ro)
		assert.Equal(t, "nobody", created.MapallUser)
	})
}

// TestParseNFSShareOptionsValidation covers the enum, boolean and fail-closed
// Kerberos rules.
func TestParseNFSShareOptionsValidation(t *testing.T) {
	d := nfsOptionsTestDriver(t, truenas.NewMockClient(), NFSConfig{})

	t.Run("unset parameters produce an empty option set", func(t *testing.T) {
		opts, err := d.parseNFSShareOptions(map[string]string{"protocol": "nfs"})
		require.NoError(t, err)
		assert.True(t, opts.empty())
		assert.Equal(t, context.Background(), withNFSShareOptions(context.Background(), opts),
			"an empty option set must not attach anything to the context")
	})

	t.Run("invalid security mode is rejected", func(t *testing.T) {
		_, err := d.parseNFSShareOptions(map[string]string{nfsSecurityParam: "krb9"})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("empty security value is rejected", func(t *testing.T) {
		_, err := d.parseNFSShareOptions(map[string]string{nfsSecurityParam: " , "})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("krb5 fails closed without the acknowledgement", func(t *testing.T) {
		for _, mode := range []string{"krb5", "KRB5I", "krb5p"} {
			_, err := d.parseNFSShareOptions(map[string]string{nfsSecurityParam: mode})
			require.Error(t, err, mode)
			assert.Contains(t, err.Error(), "nfs.krbEnabled")
		}
	})

	t.Run("non-boolean flag is rejected", func(t *testing.T) {
		_, err := d.parseNFSShareOptions(map[string]string{nfsExposeSnapshotsParam: "yes-please"})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("duplicate security modes collapse", func(t *testing.T) {
		opts, err := d.parseNFSShareOptions(map[string]string{nfsSecurityParam: "sys,SYS, sys "})
		require.NoError(t, err)
		assert.Equal(t, []string{"SYS"}, opts.security)
	})
}

// TestApplyNFSShareOptionsIsNoOpWhenEmpty locks the invariant that keeps
// existing volumes untouched.
func TestApplyNFSShareOptionsIsNoOpWhenEmpty(t *testing.T) {
	base := truenas.NFSShareCreateParams{
		Path:        "/mnt/tank/k8s/vol",
		MaprootUser: "root",
		Enabled:     true,
	}
	params := base
	applyNFSShareOptions(&params, nil)
	assert.Equal(t, base, params)
	applyNFSShareOptions(&params, &nfsShareOptions{})
	assert.Equal(t, base, params)
}

func TestNFSMountVersionFromFlags(t *testing.T) {
	cases := []struct {
		flags []string
		token string
		raw   string
	}{
		{nil, "", ""},
		{[]string{"noatime", "hard"}, "", ""},
		{[]string{"nfsvers=4"}, truenas.NFSProtocolV4, "4"},
		{[]string{"vers=4.1", "nconnect=8"}, truenas.NFSProtocolV4, "4.1"},
		{[]string{"nfsvers=3"}, truenas.NFSProtocolV3, "3"},
		// The kernel applies the LAST version option; so does the preflight.
		{[]string{"vers=3", "nfsvers=4.1"}, truenas.NFSProtocolV4, "4.1"},
		{[]string{"vers=2"}, "", ""},
	}
	for _, tc := range cases {
		token, raw := nfsMountVersionFromFlags(tc.flags)
		assert.Equal(t, tc.token, token, tc.flags)
		assert.Equal(t, tc.raw, raw, tc.flags)
	}
}

func TestPreflightNFSVersion(t *testing.T) {
	ctx := context.Background()

	t.Run("disabled by default: zero API calls", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{})
		require.NoError(t, d.preflightNFSVersion(ctx, []string{"nfsvers=4.1"}))
		assert.Zero(t, mock.NFSServiceConfigCalls, "preflight must not call nfs.config when disabled")
	})

	t.Run("enabled but no pinned version: zero API calls", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{VersionPreflight: true})
		require.NoError(t, d.preflightNFSVersion(ctx, []string{"noatime"}))
		assert.Zero(t, mock.NFSServiceConfigCalls)
	})

	t.Run("supported version passes and the config is cached", func(t *testing.T) {
		mock := truenas.NewMockClient()
		mock.NFSServiceConfigValue = &truenas.NFSServiceConfig{Protocols: []string{"NFSV3", "NFSV4"}}
		d := nfsOptionsTestDriver(t, mock, NFSConfig{VersionPreflight: true})
		require.NoError(t, d.preflightNFSVersion(ctx, []string{"nfsvers=4.1"}))
		require.NoError(t, d.preflightNFSVersion(ctx, []string{"nfsvers=3"}))
		assert.Equal(t, 1, mock.NFSServiceConfigCalls, "nfs.config must be read at most once per controller lifetime")
	})

	t.Run("unsupported version fails with a clear precondition error", func(t *testing.T) {
		mock := truenas.NewMockClient()
		mock.NFSServiceConfigValue = &truenas.NFSServiceConfig{Protocols: []string{"NFSV3"}}
		d := nfsOptionsTestDriver(t, mock, NFSConfig{VersionPreflight: true})
		err := d.preflightNFSVersion(ctx, []string{"nfsvers=4.1"})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "NFSV3")
	})

	t.Run("an unreadable service config never becomes a new failure mode", func(t *testing.T) {
		mock := truenas.NewMockClient()
		mock.InjectError = assert.AnError
		d := nfsOptionsTestDriver(t, mock, NFSConfig{VersionPreflight: true})
		require.NoError(t, d.preflightNFSVersion(ctx, []string{"nfsvers=4.1"}))
	})
}

func TestEnsureNFSProtocols(t *testing.T) {
	ctx := context.Background()

	t.Run("default empty makes no write at all", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{})
		require.NoError(t, d.ensureNFSProtocols(ctx))
		assert.Zero(t, mock.NFSServiceConfigCalls)
		assert.Empty(t, mock.NFSServiceUpdateCalls)
	})

	t.Run("already-enabled protocols are a read-only no-op", func(t *testing.T) {
		mock := truenas.NewMockClient()
		mock.NFSServiceConfigValue = &truenas.NFSServiceConfig{Protocols: []string{"NFSV3", "NFSV4"}}
		d := nfsOptionsTestDriver(t, mock, NFSConfig{EnsureProtocols: []string{"NFSV4"}})
		require.NoError(t, d.ensureNFSProtocols(ctx))
		assert.Empty(t, mock.NFSServiceUpdateCalls, "no write when the server already enables the version")
	})

	t.Run("missing protocol is added, never removed", func(t *testing.T) {
		mock := truenas.NewMockClient()
		mock.NFSServiceConfigValue = &truenas.NFSServiceConfig{Protocols: []string{"NFSV3"}}
		d := nfsOptionsTestDriver(t, mock, NFSConfig{EnsureProtocols: []string{"NFSV4"}})
		require.NoError(t, d.ensureNFSProtocols(ctx))
		require.Len(t, mock.NFSServiceUpdateCalls, 1)
		assert.Equal(t, []string{"NFSV3", "NFSV4"}, mock.NFSServiceUpdateCalls[0]["protocols"])
	})

	t.Run("invalid protocol token is rejected before any write", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{EnsureProtocols: []string{"NFSV2"}})
		require.Error(t, d.ensureNFSProtocols(ctx))
		assert.Empty(t, mock.NFSServiceUpdateCalls)
	})
}

// TestNFSMountFlagsPreservesExistingBehavior proves the node-side sanity pass is
// advisory only: the returned flags are byte-for-byte what volumeMountFlags has
// always produced, including its de-duplication.
func TestNFSMountFlagsPreservesExistingBehavior(t *testing.T) {
	cases := [][]string{
		nil,
		{"nfsvers=4", "noatime"},
		{"nfsvers=4", "noatime", "nfsvers=4", "", "  "},
		{"vers=3", "nconnect=8"},
		{"vers=3", "nfsvers=4.1", "nconnect=8"},
		{"nfsvers=4.1", "nconnect=8", "hard", "rsize=1048576"},
	}
	for _, flags := range cases {
		volCap := &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{MountFlags: flags}},
		}
		assert.Equal(t, volumeMountFlags(volCap), nfsMountFlags(volCap), flags)
	}
	assert.Nil(t, nfsMountFlags(nil))
}

func TestMountFlagsFromCapabilities(t *testing.T) {
	caps := []*csi.VolumeCapability{
		nil,
		{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}},
		{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{MountFlags: []string{"nfsvers=4.1"}}}},
	}
	assert.Equal(t, []string{"nfsvers=4.1"}, mountFlagsFromCapabilities(caps))
	assert.Nil(t, mountFlagsFromCapabilities(nil))
}
