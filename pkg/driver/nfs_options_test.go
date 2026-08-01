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

// ---------------------------------------------------------------------------
// H2 — the GLOBAL nfs.shareSecurity path must fail closed too
// ---------------------------------------------------------------------------

// TestGlobalShareSecurityFailsClosedOnKerberos is the H2 regression. Before the
// fix, validateNFSSecurity was reachable ONLY from the StorageClass parameter
// path, so `nfs.shareSecurity: [KRB5]` with `krbEnabled: false` sailed through
// and stamped KRB5 on EVERY newly created export — dead mounts fleet-wide on a
// box with no KDC or keytab.
func TestGlobalShareSecurityFailsClosedOnKerberos(t *testing.T) {
	ctx := context.Background()

	t.Run("the share builder refuses rather than stamping KRB5", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{ShareSecurity: []string{"KRB5"}}) // krbEnabled=false
		_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"})
		require.NoError(t, err)

		err = d.createNFSShareForDataset(ctx, nil, "tank/k8s/vol", "vol", true, nil)
		require.Error(t, err, "a Kerberos global default without nfs.krbEnabled must never reach sharing.nfs.create")
		assert.Contains(t, err.Error(), "nfs.shareSecurity")
		assert.Contains(t, err.Error(), "krbEnabled")
		assert.Empty(t, mock.NFSShareCreateParams, "no export may be created at all")
	})

	t.Run("lower-case spelling cannot evade the prefix check", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{ShareSecurity: []string{"krb5p"}})
		_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"})
		require.NoError(t, err)
		require.Error(t, d.createNFSShareForDataset(ctx, nil, "tank/k8s/vol", "vol", true, nil))
		assert.Empty(t, mock.NFSShareCreateParams)
	})

	t.Run("an unknown mode in the global list is rejected too", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{ShareSecurity: []string{"KRB9"}})
		_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"})
		require.NoError(t, err)
		require.Error(t, d.createNFSShareForDataset(ctx, nil, "tank/k8s/vol", "vol", true, nil))
		assert.Empty(t, mock.NFSShareCreateParams)
	})

	t.Run("acknowledged Kerberos still works and is normalized", func(t *testing.T) {
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{ShareSecurity: []string{"krb5i", "KRB5I", "sys"}, KrbEnabled: true})
		_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"})
		require.NoError(t, err)
		require.NoError(t, d.createNFSShareForDataset(ctx, nil, "tank/k8s/vol", "vol", true, nil))
		require.Len(t, mock.NFSShareCreateParams, 1)
		assert.Equal(t, []string{"KRB5I", "SYS"}, mock.NFSShareCreateParams[0].Security)
	})
}

// TestValidateConfigRejectsUnusableShareSecurity is the config-LOAD half of H2:
// the controller must refuse to start rather than provision a fleet of
// unmountable exports. A hand-written ConfigMap bypasses the chart schema, so
// this check is not optional.
func TestValidateConfigRejectsUnusableShareSecurity(t *testing.T) {
	base := func() *Config {
		return &Config{
			TrueNAS: TrueNASConfig{Host: "10.0.0.1", MaxConnections: 5},
			ZFS:     ZFSConfig{DatasetParentName: "tank/k8s"},
			NFS:     NFSConfig{Enabled: true, ShareHost: "10.0.0.1"},
			Reconcile: ReconcileConfig{
				Interval:     "1h",
				MinOrphanAge: "24h",
				Delete:       ReconcileDeleteConfig{MaxPerRun: 5},
			},
		}
	}

	t.Run("KRB5 without krbEnabled", func(t *testing.T) {
		cfg := base()
		cfg.NFS.ShareSecurity = []string{"KRB5"}
		err := validateConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nfs.shareSecurity")
		assert.Contains(t, err.Error(), "krbEnabled")
	})

	t.Run("unknown mode", func(t *testing.T) {
		cfg := base()
		cfg.NFS.ShareSecurity = []string{"SYS", "KRB6"}
		require.Error(t, validateConfig(cfg))
	})

	t.Run("KRB5 with krbEnabled is accepted and normalized in place", func(t *testing.T) {
		cfg := base()
		cfg.NFS.KrbEnabled = true
		cfg.NFS.ShareSecurity = []string{"krb5", "sys"}
		require.NoError(t, validateConfig(cfg))
		assert.Equal(t, []string{"KRB5", "SYS"}, cfg.NFS.ShareSecurity)
	})

	t.Run("SYS alone needs no acknowledgement", func(t *testing.T) {
		cfg := base()
		cfg.NFS.ShareSecurity = []string{"SYS"}
		require.NoError(t, validateConfig(cfg))
	})

	t.Run("maproot and mapall together are refused at load", func(t *testing.T) {
		cfg := base()
		cfg.NFS.ShareMaprootUser = "root"
		cfg.NFS.ShareMapallUser = "nobody"
		err := validateConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mutually exclusive")
	})
}

// ---------------------------------------------------------------------------
// M1 — strict fencing must not silently void the per-class allowlists
// ---------------------------------------------------------------------------

// TestStrictFencingRejectsExportAllowlistParams pins M1. createNFSShareForDataset
// applies params.Networks/Hosts and then, under strict fencing, unconditionally
// resets both to [] — so before the fix the two parameters were a pure no-op the
// operator had no way to detect.
func TestStrictFencingRejectsExportAllowlistParams(t *testing.T) {
	strict := func() *Driver {
		d := nfsOptionsTestDriver(t, truenas.NewMockClient(), NFSConfig{})
		d.config.Fencing = FencingConfig{Mode: FencingModeStrict}
		return d
	}

	for _, param := range []string{nfsAllowedNetworksParam, nfsAllowedHostsParam} {
		t.Run(param+" is rejected under strict fencing", func(t *testing.T) {
			_, err := strict().parseNFSShareOptions(map[string]string{param: "10.0.0.0/8"})
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), "strict")
			assert.Contains(t, err.Error(), param)
		})
	}

	t.Run("additive fencing still accepts them", func(t *testing.T) {
		d := nfsOptionsTestDriver(t, truenas.NewMockClient(), NFSConfig{})
		d.config.Fencing = FencingConfig{Mode: FencingModeAdditive}
		opts, err := d.parseNFSShareOptions(map[string]string{
			nfsAllowedNetworksParam: "10.0.0.0/8",
			nfsAllowedHostsParam:    "node-a",
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"10.0.0.0/8"}, opts.allowedNetworks)
		assert.Equal(t, []string{"node-a"}, opts.allowedHosts)
	})

	t.Run("strict fencing without the params is unaffected", func(t *testing.T) {
		opts, err := strict().parseNFSShareOptions(map[string]string{nfsReadOnlyParam: "true"})
		require.NoError(t, err)
		assert.Nil(t, opts.allowedNetworks)
	})
}

// ---------------------------------------------------------------------------
// M2 — maproot_* / mapall_* mutual exclusion
// ---------------------------------------------------------------------------

// TestMaprootMapallMutualExclusion pins M2. The shipped default config sets
// maproot_user=root / maproot_group=wheel, so a class that sets only
// nfsMapallUser resolves to BOTH mappings and sharing.nfs.create rejects the
// payload with an opaque middleware error.
func TestMaprootMapallMutualExclusion(t *testing.T) {
	withMaproot := func() *Driver {
		return nfsOptionsTestDriver(t, truenas.NewMockClient(), NFSConfig{
			ShareMaprootUser: "root", ShareMaprootGroup: "wheel",
		})
	}

	t.Run("mapall over the inherited maproot default is rejected", func(t *testing.T) {
		_, err := withMaproot().parseNFSShareOptions(map[string]string{nfsMapallUserParam: "nobody"})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "mutually exclusive")
		assert.Contains(t, err.Error(), "root")
		assert.Contains(t, err.Error(), "nobody")
	})

	t.Run("mapall group alone trips it too", func(t *testing.T) {
		_, err := withMaproot().parseNFSShareOptions(map[string]string{nfsMapallGroupParam: "nogroup"})
		require.Error(t, err)
	})

	t.Run("explicitly clearing maproot resolves the conflict", func(t *testing.T) {
		opts, err := withMaproot().parseNFSShareOptions(map[string]string{
			nfsMapallUserParam:   "nobody",
			nfsMaprootUserParam:  "",
			nfsMaprootGroupParam: "",
		})
		require.NoError(t, err)
		require.NotNil(t, opts.maprootUser)
		assert.Empty(t, *opts.maprootUser)
	})

	t.Run("maproot alone is the historical default and stays valid", func(t *testing.T) {
		_, err := withMaproot().parseNFSShareOptions(map[string]string{nfsReadOnlyParam: "true"})
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// M3 — ensureNFSProtocols must never write on an unreadable service config
// ---------------------------------------------------------------------------

// TestEnsureNFSProtocolsFailsClosedOnEmptyProtocols pins M3. nfs.update SETS the
// protocol list rather than unioning with it, so merging into an EMPTY base
// would write exactly the configured list and disable every major version
// missing from it for every export on the appliance — the precise blast radius
// the "only adds, never removes" HARD RULE claims to exclude.
func TestEnsureNFSProtocolsFailsClosedOnEmptyProtocols(t *testing.T) {
	ctx := context.Background()
	mock := truenas.NewMockClient()
	mock.NFSServiceConfigValue = &truenas.NFSServiceConfig{Protocols: nil}
	d := nfsOptionsTestDriver(t, mock, NFSConfig{EnsureProtocols: []string{"NFSV4"}})

	err := d.ensureNFSProtocols(ctx)
	require.Error(t, err, "an unparseable service config is never a safe basis for a service-wide write")
	assert.Contains(t, err.Error(), "DISABLE")
	assert.Empty(t, mock.NFSServiceUpdateCalls, "no nfs.update may be issued")

	t.Run("a readable list still widens normally", func(t *testing.T) {
		mock := truenas.NewMockClient()
		mock.NFSServiceConfigValue = &truenas.NFSServiceConfig{Protocols: []string{"NFSV3"}}
		d := nfsOptionsTestDriver(t, mock, NFSConfig{EnsureProtocols: []string{"NFSV4"}})
		require.NoError(t, d.ensureNFSProtocols(ctx))
		assert.Len(t, mock.NFSServiceUpdateCalls, 1)
	})

	t.Run("an already-satisfied list writes nothing", func(t *testing.T) {
		mock := truenas.NewMockClient()
		mock.NFSServiceConfigValue = &truenas.NFSServiceConfig{Protocols: []string{"NFSV3", "NFSV4"}}
		d := nfsOptionsTestDriver(t, mock, NFSConfig{EnsureProtocols: []string{"NFSV4"}})
		require.NoError(t, d.ensureNFSProtocols(ctx))
		assert.Empty(t, mock.NFSServiceUpdateCalls)
	})
}

// ---------------------------------------------------------------------------
// L5 — the R4 invariant the sprint's safety actually rests on
// ---------------------------------------------------------------------------

// TestExistingShareIsNeverUpdatedOnReplay pins the invariant that makes R4
// ("a StorageClass edit can never rewrite a live export's security or squash
// mapping") true. The stated reason in the sprint summary was wrong:
// createVolumeExisting DOES pass the CreateVolume context — nfsShareOptions and
// all — into ensureShareExists. What actually keeps it safe is that the share
// builder has no update path: it early-returns when a share already exists.
//
// This test fails the moment someone adds an update branch there.
func TestExistingShareIsNeverUpdatedOnReplay(t *testing.T) {
	ctx := context.Background()

	// The class later gains security/squash/read-only options; the replay path
	// carries them in its context all the way down.
	optionParams := map[string]string{
		nfsSecurityParam:   "sys",
		nfsReadOnlyParam:   "true",
		nfsMapallUserParam: "nobody",
	}

	seed := func(t *testing.T) (*truenas.MockClient, *Driver, *truenas.Dataset) {
		t.Helper()
		mock := truenas.NewMockClient()
		d := nfsOptionsTestDriver(t, mock, NFSConfig{})
		_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "tank/k8s/vol", Type: "FILESYSTEM"})
		require.NoError(t, err)
		// First provision, with no options at all.
		require.NoError(t, d.createNFSShareForDataset(ctx, nil, "tank/k8s/vol", "vol", true, nil))
		require.Len(t, mock.NFSShareCreateParams, 1)
		require.Nil(t, mock.NFSShareCreateParams[0].Security)
		ds, err := mock.DatasetGet(ctx, "tank/k8s/vol")
		require.NoError(t, err)
		return mock, d, ds
	}

	assertUntouched := func(t *testing.T, mock *truenas.MockClient) {
		t.Helper()
		assert.Len(t, mock.NFSShareCreateParams, 1, "the existing export must not be re-created")
		assert.Empty(t, mock.NFSShareUpdateParams, "the existing export must NEVER be updated with the new options")
		share := mock.NFSShares[1]
		require.NotNil(t, share)
		assert.Empty(t, share.Security, "a live export's security must be untouched by a StorageClass edit")
		assert.Empty(t, share.MapallUser)
		assert.False(t, share.Ro)
	}

	t.Run("share ID property present: ensureShareExists verifies and returns", func(t *testing.T) {
		mock, d, ds := seed(t)
		opts, err := d.parseNFSShareOptions(optionParams)
		require.NoError(t, err)
		replayCtx := withNFSShareOptions(ctx, opts)
		require.NotNil(t, nfsShareOptionsFromContext(replayCtx), "the replay context must genuinely carry the options")

		require.NoError(t, d.ensureNFSShareExists(replayCtx, ds, "tank/k8s/vol", "vol"))
		assertUntouched(t, mock)
	})

	// THE LOAD-BEARING CASE. When the share-ID property is missing (a replicated
	// dataset, or a crash between share create and property stamp),
	// ensureShareExists falls through to createNFSShareForDataset — WITH the
	// CreateVolume context and its options. What keeps R4 true is that the share
	// builder has NO update path: it early-returns on an already-resolved share.
	// This sub-test fails the moment someone adds one.
	t.Run("share ID property missing: the builder resolves and returns without updating", func(t *testing.T) {
		mock, d, ds := seed(t)
		delete(ds.UserProperties, PropNFSShareID)

		opts, err := d.parseNFSShareOptions(optionParams)
		require.NoError(t, err)
		replayCtx := withNFSShareOptions(ctx, opts)

		require.NoError(t, d.ensureNFSShareExists(replayCtx, ds, "tank/k8s/vol", "vol"))
		assertUntouched(t, mock)
	})
}
