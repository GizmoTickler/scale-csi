package driver

import (
	"context"
	"errors"
	"testing"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func perfTestDriver(mock *truenas.MockClient) *Driver {
	return &Driver{
		name: "org.scale.csi",
		config: &Config{
			DriverName: "org.scale.csi",
			ZFS:        ZFSConfig{DatasetParentName: "flashstor/scale-csi", ZvolBlocksize: "16K"},
			NFS:        NFSConfig{Enabled: true, ShareHost: "10.0.0.1"},
		},
		truenasClient: mock,
	}
}

func TestZFSPerformanceClassFromParams(t *testing.T) {
	t.Run("absent parameter is the default", func(t *testing.T) {
		class, err := zfsPerformanceClassFromParams(map[string]string{"protocol": "nfs"})
		require.NoError(t, err)
		assert.Empty(t, class)
		assert.Equal(t, context.Background(), withZFSPerformanceClass(context.Background(), class))
	})

	t.Run("every documented class resolves case-insensitively", func(t *testing.T) {
		for _, name := range []string{"database", "MEDIA", " vm ", "Backup", "general"} {
			class, err := zfsPerformanceClassFromParams(map[string]string{zfsPerformanceClassParam: name})
			require.NoError(t, err, name)
			assert.Contains(t, zfsPerformanceClasses, class, name)
		}
	})

	t.Run("unknown and empty values are rejected", func(t *testing.T) {
		for _, name := range []string{"ludicrous", "", "   "} {
			_, err := zfsPerformanceClassFromParams(map[string]string{zfsPerformanceClassParam: name})
			require.Error(t, err, name)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		}
	})
}

// TestResolvePerformanceClassProperties pins the exact property map each class
// contributes, per dataset type.
func TestResolvePerformanceClassProperties(t *testing.T) {
	ctx := context.Background()
	mock := truenas.NewMockClient()
	mock.SpecialVdevPresent = true
	d := perfTestDriver(mock)

	t.Run("filesystem drops the volume-only key", func(t *testing.T) {
		props, err := d.resolvePerformanceClassProperties(ctx, "database", "FILESYSTEM")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			zfsPropRecordsize:            "16K",
			zfsPropSync:                  "STANDARD",
			zfsPropLogbias:               "LATENCY",
			zfsPropCompression:           "LZ4",
			zfsPropPrimarycache:          "ALL",
			zfsPropSpecialSmallBlockSize: "16K",
			zfsPropAtime:                 "OFF",
		}, props)
	})

	t.Run("volume drops the filesystem-only keys", func(t *testing.T) {
		props, err := d.resolvePerformanceClassProperties(ctx, "vm", "VOLUME")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			zfsPropVolblocksize: "16K",
			zfsPropSync:         "STANDARD",
			zfsPropLogbias:      "LATENCY",
			zfsPropCompression:  "LZ4",
			zfsPropPrimarycache: "ALL",
		}, props)
	})

	t.Run("backup keeps data out of ARC", func(t *testing.T) {
		props, err := d.resolvePerformanceClassProperties(ctx, "backup", "FILESYSTEM")
		require.NoError(t, err)
		assert.Equal(t, "METADATA", props[zfsPropPrimarycache])
		assert.Equal(t, "1M", props[zfsPropRecordsize])
		assert.Equal(t, "ZSTD", props[zfsPropCompression])
	})

	t.Run("special_small_block_size is dropped without a special vdev (R8)", func(t *testing.T) {
		noSpecial := truenas.NewMockClient()
		noSpecial.SpecialVdevPresent = false
		props, err := perfTestDriver(noSpecial).resolvePerformanceClassProperties(ctx, "database", "FILESYSTEM")
		require.NoError(t, err)
		assert.NotContains(t, props, zfsPropSpecialSmallBlockSize)
	})

	t.Run("a value the backend rejects is InvalidArgument, not an opaque create failure", func(t *testing.T) {
		strict := truenas.NewMockClient()
		strict.ZFSChoicesValue = &truenas.ZFSPropertyChoices{
			Recordsize:  []string{"128K"},
			Compression: []string{"LZ4"},
		}
		_, err := perfTestDriver(strict).resolvePerformanceClassProperties(ctx, "media", "FILESYSTEM")
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "recordsize=1M")
	})

	t.Run("an unreadable choice list fails open rather than blocking provisioning", func(t *testing.T) {
		broken := truenas.NewMockClient()
		broken.InjectChoicesError = errors.New("simulated choices failure")
		props, err := perfTestDriver(broken).resolvePerformanceClassProperties(ctx, "general", "FILESYSTEM")
		require.NoError(t, err)
		assert.Equal(t, "128K", props[zfsPropRecordsize])
	})

	t.Run("choice lists are read at most once per controller lifetime", func(t *testing.T) {
		counted := truenas.NewMockClient()
		cd := perfTestDriver(counted)
		for i := 0; i < 3; i++ {
			_, err := cd.resolvePerformanceClassProperties(ctx, "general", "FILESYSTEM")
			require.NoError(t, err)
		}
		assert.Equal(t, 1, counted.ZFSChoicesCalls)
	})
}

// TestApplyPerformanceClassPropertiesUsesCorrectKeys locks the wire spelling —
// special_small_block_size, NOT special_small_blocks.
func TestApplyPerformanceClassPropertiesUsesCorrectKeys(t *testing.T) {
	params := &truenas.DatasetCreateParams{Name: "flashstor/scale-csi/vol", Type: "FILESYSTEM"}
	applyPerformanceClassProperties(params, map[string]string{
		zfsPropRecordsize:            "16K",
		zfsPropSync:                  "STANDARD",
		zfsPropLogbias:               "LATENCY",
		zfsPropCompression:           "LZ4",
		zfsPropChecksum:              "BLAKE3",
		zfsPropPrimarycache:          "ALL",
		zfsPropSecondarycache:        "ALL",
		zfsPropSpecialSmallBlockSize: "16K",
		zfsPropAtime:                 "OFF",
		zfsPropReadonly:              "OFF",
	})
	assert.Equal(t, "16K", params.Recordsize)
	assert.Equal(t, "STANDARD", params.Sync)
	assert.Equal(t, "LATENCY", params.Logbias)
	assert.Equal(t, "LZ4", params.Compression)
	assert.Equal(t, "BLAKE3", params.Checksum)
	assert.Equal(t, "ALL", params.Primarycache)
	assert.Equal(t, "ALL", params.Secondarycache)
	assert.Equal(t, "16K", params.SpecialSmallBlockSize)
	assert.Equal(t, "OFF", params.Atime)

	// Empty property set is a strict no-op.
	base := truenas.DatasetCreateParams{Name: "flashstor/scale-csi/vol", Type: "FILESYSTEM"}
	untouched := base
	applyPerformanceClassProperties(&untouched, nil)
	assert.Equal(t, base, untouched)
}

// TestApplyDatasetPropertiesSpecialSmallBlockSizeKey covers the driver bug the
// design called out: the property key is special_small_block_size, and it was
// previously dropped as an unknown key.
func TestApplyDatasetPropertiesSpecialSmallBlockSizeKey(t *testing.T) {
	d := perfTestDriver(truenas.NewMockClient())
	d.config.ZFS.DatasetProperties = map[string]string{
		"special_small_block_size": "32k",
		"checksum":                 "blake3",
		"secondarycache":           "metadata",
		"snapdir":                  "visible",
	}
	params := &truenas.DatasetCreateParams{Name: "flashstor/scale-csi/vol", Type: "FILESYSTEM"}
	d.applyDatasetProperties(params)
	assert.Equal(t, "32K", params.SpecialSmallBlockSize)
	assert.Equal(t, "BLAKE3", params.Checksum)
	assert.Equal(t, "METADATA", params.Secondarycache)
	assert.Equal(t, "VISIBLE", params.Snapdir)
}

// TestExplicitDatasetPropertiesWinOverPerformanceClass proves the layering
// order: the curated preset is the floor, an operator key is the override.
func TestExplicitDatasetPropertiesWinOverPerformanceClass(t *testing.T) {
	ctx := context.Background()
	d := perfTestDriver(truenas.NewMockClient())
	d.config.ZFS.DatasetProperties = map[string]string{"compression": "zstd-19", "sync": "always"}

	params := &truenas.DatasetCreateParams{Name: "flashstor/scale-csi/vol", Type: "FILESYSTEM"}
	curated, err := d.resolvePerformanceClassProperties(ctx, "general", "FILESYSTEM")
	require.NoError(t, err)
	applyPerformanceClassProperties(params, curated)
	d.applyDatasetProperties(params)

	assert.Equal(t, "ZSTD-19", params.Compression, "explicit datasetProperties must win")
	assert.Equal(t, "ALWAYS", params.Sync)
	assert.Equal(t, "128K", params.Recordsize, "unopposed curated values still apply")
	assert.Equal(t, "LATENCY", params.Logbias)
}

// ---------------------------------------------------------------------------
// Immutability guard
// ---------------------------------------------------------------------------

func TestGuardImmutableZFSProperties(t *testing.T) {
	t.Run("names exactly the create-only properties that would change", func(t *testing.T) {
		changed := guardImmutableZFSProperties(
			map[string]string{zfsPropVolblocksize: "16K", zfsPropLogbias: "LATENCY", zfsPropPrimarycache: "ALL", zfsPropRecordsize: "128K"},
			map[string]string{zfsPropVolblocksize: "128K", zfsPropLogbias: "THROUGHPUT", zfsPropPrimarycache: "ALL", zfsPropRecordsize: "1M"},
		)
		assert.Equal(t, []string{zfsPropLogbias, zfsPropVolblocksize}, changed,
			"recordsize is live-tunable and must not be reported as immutable")
	})

	t.Run("identical values are not a change", func(t *testing.T) {
		props := map[string]string{zfsPropVolblocksize: "16K", zfsPropLogbias: "latency"}
		assert.Empty(t, guardImmutableZFSProperties(props, map[string]string{zfsPropVolblocksize: "16k", zfsPropLogbias: "LATENCY"}))
	})

	t.Run("secondarycache is guarded too", func(t *testing.T) {
		assert.Equal(t, []string{zfsPropSecondarycache},
			guardImmutableZFSProperties(map[string]string{}, map[string]string{zfsPropSecondarycache: "ALL"}))
	})
}

func TestLiveTunableZFSPropertyDiff(t *testing.T) {
	changed := liveTunableZFSPropertyDiff(
		map[string]string{zfsPropRecordsize: "128K", zfsPropCompression: "LZ4", zfsPropLogbias: "LATENCY"},
		map[string]string{zfsPropRecordsize: "1M", zfsPropCompression: "LZ4", zfsPropLogbias: "THROUGHPUT"},
	)
	assert.Equal(t, []string{zfsPropRecordsize}, changed,
		"only live-tunable properties belong in this diff")
}

func TestGuardPerformanceClassChange(t *testing.T) {
	ctx := context.Background()
	d := perfTestDriver(truenas.NewMockClient())

	t.Run("no class requested is a no-op", func(t *testing.T) {
		require.NoError(t, d.guardPerformanceClassChange(ctx, "vol", "database", "", "FILESYSTEM"))
	})

	t.Run("same class is a no-op", func(t *testing.T) {
		require.NoError(t, d.guardPerformanceClassChange(ctx, "vol", "media", "media", "FILESYSTEM"))
	})

	t.Run("legacy unstamped volume warns instead of wedging", func(t *testing.T) {
		require.NoError(t, d.guardPerformanceClassChange(ctx, "vol", "", "media", "FILESYSTEM"))
	})

	t.Run("changing an immutable property is rejected", func(t *testing.T) {
		err := d.guardPerformanceClassChange(ctx, "vol", "general", "media", "FILESYSTEM")
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), zfsPropLogbias)
		assert.Contains(t, err.Error(), "fixed when the dataset is created")
	})

	t.Run("zvol geometry change is rejected", func(t *testing.T) {
		err := d.guardPerformanceClassChange(ctx, "vol", "vm", "backup", "VOLUME")
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), zfsPropVolblocksize)
	})

	t.Run("a live-tunable-only difference is allowed", func(t *testing.T) {
		// database -> vm on a FILESYSTEM: logbias/primarycache/compression match,
		// only recordsize and special_small_block_size differ.
		noSpecial := perfTestDriver(truenas.NewMockClient())
		require.NoError(t, noSpecial.guardPerformanceClassChange(ctx, "vol", "database", "vm", "FILESYSTEM"))
	})

	t.Run("an unresolvable stored class refuses rather than guesses", func(t *testing.T) {
		err := d.guardPerformanceClassChange(ctx, "vol", "from-the-future", "media", "FILESYSTEM")
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}

// TestCreateVolumeStampsAndGuardsPerformanceClass is the end-to-end proof: the
// class is stamped at create, and re-creating the same volume under a class with
// different immutable properties is refused.
func TestCreateVolumeStampsAndGuardsPerformanceClass(t *testing.T) {
	ctx := context.Background()
	mock := truenas.NewMockClient()
	d := perfTestDriver(mock)

	capabilities := []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)}
	_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "perf-vol",
		VolumeCapabilities: capabilities,
		Parameters:         map[string]string{"protocol": "nfs", zfsPerformanceClassParam: "general"},
	})
	require.NoError(t, err)

	ds := mock.Datasets["flashstor/scale-csi/perf-vol"]
	require.NotNil(t, ds)
	assert.Equal(t, "general", ds.UserProperties[PropZFSPerformanceClass].Value)

	// Idempotent replay with the same class still succeeds.
	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "perf-vol",
		VolumeCapabilities: capabilities,
		Parameters:         map[string]string{"protocol": "nfs", zfsPerformanceClassParam: "general"},
	})
	require.NoError(t, err)

	// A class whose create-only properties differ is refused.
	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "perf-vol",
		VolumeCapabilities: capabilities,
		Parameters:         map[string]string{"protocol": "nfs", zfsPerformanceClassParam: "media"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), zfsPropLogbias)
}

// TestCreateVolumeWithoutPerformanceClassIsUnchanged is the default-off guard:
// no class means no curated property, no stamp, and no choice-list lookup.
func TestCreateVolumeWithoutPerformanceClassIsUnchanged(t *testing.T) {
	mock := truenas.NewMockClient()
	d := perfTestDriver(mock)

	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "plain-perf-vol",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs"},
	})
	require.NoError(t, err)

	ds := mock.Datasets["flashstor/scale-csi/plain-perf-vol"]
	require.NotNil(t, ds)
	_, stamped := ds.UserProperties[PropZFSPerformanceClass]
	assert.False(t, stamped, "a volume that did not use the feature must carry no class stamp")
	assert.Zero(t, mock.ZFSChoicesCalls, "no class means zero extra API calls")
	assert.Zero(t, mock.SpecialVdevCalls)
}

// TestCreateVolumeRejectsUnknownPerformanceClass proves validation happens
// before any backend mutation.
func TestCreateVolumeRejectsUnknownPerformanceClass(t *testing.T) {
	mock := truenas.NewMockClient()
	d := perfTestDriver(mock)

	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "bad-class",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs", zfsPerformanceClassParam: "ludicrous"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Empty(t, mock.Datasets)
}

func TestParentPoolName(t *testing.T) {
	assert.Equal(t, "flashstor", perfTestDriver(truenas.NewMockClient()).parentPoolName())
	d := perfTestDriver(truenas.NewMockClient())
	d.config.ZFS.DatasetParentName = "tank"
	assert.Equal(t, "tank", d.parentPoolName())
}

// ---------------------------------------------------------------------------
// Performance class x VolumeContentSource (H1)
// ---------------------------------------------------------------------------

// perfContentSourceDriver mirrors perfTestDriver on the parent dataset the
// shared snapshot-clone helpers seed ("pool/parent").
func perfContentSourceDriver(mock *truenas.MockClient) *Driver {
	return &Driver{
		name: "org.scale.csi",
		config: &Config{
			DriverName: "org.scale.csi",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", ZvolBlocksize: "16K", ZvolReadyTimeout: 1},
			NFS:        NFSConfig{Enabled: true, ShareHost: "192.0.2.10"},
		},
		truenasClient: mock,
	}
}

// TestCreateVolumeFromContentSourceDoesNotStampPerformanceClass is the H1
// regression: a clone/restore inherits the ORIGIN dataset's geometry and accepts
// no property payload, so the curated class is never applied to it. Stamping it
// anyway would make the immutability guard treat a fiction as ground truth.
//
// Both failure directions are asserted:
//   - FALSE ACCEPT — a clone must not carry a class stamp it does not satisfy;
//   - FALSE REJECT — a later replay under a DIFFERENT class must not be refused
//     with "logbias ... fixed when the dataset is created" for a property the
//     driver never set on this dataset.
func TestCreateVolumeFromContentSourceDoesNotStampPerformanceClass(t *testing.T) {
	ctx := context.Background()
	mock := truenas.NewMockClient()
	d := perfContentSourceDriver(mock)
	seedSnapshotCloneSource(t, mock, d, "FILESYSTEM", 1<<30)

	request := snapshotCloneRequest("restored-clone", "snap-1", "nfs", 1<<30)
	request.Parameters[zfsPerformanceClassParam] = "media"
	_, err := d.CreateVolume(ctx, request)
	require.NoError(t, err)

	ds := mock.Datasets["pool/parent/restored-clone"]
	require.NotNil(t, ds)
	_, stamped := ds.UserProperties[PropZFSPerformanceClass]
	assert.False(t, stamped,
		"a clone/restore carries the ORIGIN's geometry, so it must NOT be stamped with a class that was never applied")

	// The clone genuinely came from the origin dataset (so it carries the origin's
	// geometry, which is precisely why a class stamp on it would be a lie).
	assert.NotEmpty(t, datasetUserProperty(ds, PropVolumeContentSourceID),
		"the volume under test must actually be a content-source clone")

	// FALSE REJECT: replaying under a class whose create-only properties differ
	// must NOT be refused, because nothing was ever fixed at create here.
	replay := snapshotCloneRequest("restored-clone", "snap-1", "nfs", 1<<30)
	replay.Parameters[zfsPerformanceClassParam] = "database"
	_, err = d.CreateVolume(ctx, replay)
	require.NoError(t, err,
		"an unstamped clone must not be wedged by the immutability guard for a property the driver never set")
}

// TestCreateVolumeAppliesAndStampsClassOnlyOnFreshCreate is the positive half:
// the stamp still happens on the ordinary create path, so H1's fix cannot be
// mistaken for "disable the feature".
func TestCreateVolumeAppliesAndStampsClassOnlyOnFreshCreate(t *testing.T) {
	ctx := context.Background()
	mock := truenas.NewMockClient()
	d := perfContentSourceDriver(mock)
	mustCreateParentDataset(t, mock)

	_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "fresh-vol",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs", zfsPerformanceClassParam: "media"},
	})
	require.NoError(t, err)

	ds := mock.Datasets["pool/parent/fresh-vol"]
	require.NotNil(t, ds)
	assert.Equal(t, "media", ds.UserProperties[PropZFSPerformanceClass].Value)
}

// ---------------------------------------------------------------------------
// H1 round 2 — a STAMPED source must not launder its class into the clone
// ---------------------------------------------------------------------------

// seedStampedPerformanceClassSource creates a source volume that legitimately
// carries a class stamp ("database"), i.e. a volume the driver itself created
// with the curated properties applied. Everything downstream of it inherits that
// stamp through ordinary ZFS behavior, which is exactly the hazard under test.
func seedStampedPerformanceClassSource(t *testing.T, mock *truenas.MockClient) {
	t.Helper()
	ctx := context.Background()
	mustCreateParentDataset(t, mock)
	source, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/stamped-source", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, mock.DatasetSetUserProperties(ctx, source.Name, map[string]string{
		PropManagedResource:     "true",
		PropCSIVolumeName:       "stamped-source",
		PropZFSPerformanceClass: "database",
	}))
	_, err = mock.SnapshotCreate(ctx, source.Name, "stamped-snap", nil)
	require.NoError(t, err)
}

// contentSourceClassCases enumerates the THREE ways CreateVolume materializes a
// volume from existing content. Round 1 fixed only the stamp WRITE on the fresh
// path; every one of these still let a SOURCE-INHERITED stamp survive.
func contentSourceClassCases() []struct {
	name     string
	detached bool
	source   *csi.VolumeContentSource
} {
	return []struct {
		name     string
		detached bool
		source   *csi.VolumeContentSource
	}{
		{
			name: "snapshot clone",
			source: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "stamped-snap"},
			}},
		},
		{
			name:     "detached snapshot copy",
			detached: true,
			source: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "stamped-snap"},
			}},
		},
		{
			name: "volume clone",
			source: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "stamped-source"},
			}},
		},
	}
}

// TestContentSourceVolumeNeverInheritsPerformanceClassStamp is the H1 round-2
// regression. A ZFS clone copies the source's user properties (with the origin
// snapshot as their source), and a detached replication copy reproduces them as
// LOCAL values — so a clone/restore of a legitimately stamped volume came out
// carrying a class stamp the driver never applied to it.
//
// That stamp then drove the immutability guard, which produces a CSI IDEMPOTENCY
// VIOLATION: replaying an identical, previously SUCCESSFUL CreateVolume compared
// the requested class against the ORIGIN's stored class and returned
// FailedPrecondition. All three materialization paths are covered, in both
// failure directions.
func TestContentSourceVolumeNeverInheritsPerformanceClassStamp(t *testing.T) {
	for _, tc := range contentSourceClassCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mock := truenas.NewMockClient()
			d := perfContentSourceDriver(mock)
			d.config.ZFS.DetachedVolumesFromSnapshots = tc.detached
			seedStampedPerformanceClassSource(t, mock)

			request := func(class string) *csi.CreateVolumeRequest {
				return &csi.CreateVolumeRequest{
					Name:               "restored",
					CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
					VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
					Parameters:         map[string]string{"protocol": "nfs", zfsPerformanceClassParam: class},
					VolumeContentSource: &csi.VolumeContentSource{
						Type: tc.source.GetType(),
					},
				}
			}

			_, err := d.CreateVolume(ctx, request("media"))
			require.NoError(t, err)

			target, err := mock.DatasetGet(ctx, "pool/parent/restored")
			require.NoError(t, err)
			require.True(t, datasetHasDurableContentSource(target),
				"the volume under test must actually be a content-source volume")
			_, stamped := target.UserProperties[PropZFSPerformanceClass]
			assert.False(t, stamped,
				"a class stamp copied from the source asserts curated geometry that was never applied to THIS volume")

			// IDEMPOTENCY: the exact same successful request, replayed.
			_, err = d.CreateVolume(ctx, request("media"))
			require.NoError(t, err, "an identical replay of a successful CreateVolume must never fail")

			// And a later StorageClass edit must not wedge it either.
			_, err = d.CreateVolume(ctx, request("backup"))
			require.NoError(t, err,
				"the guard must not refuse a class change for create-only properties the driver never set here")
		})
	}
}

// TestPerformanceClassGuardIgnoresContentSourceStamp is the defense-in-depth
// half: the SCRUB is best-effort (one pool.dataset.update that can fail), so the
// guard itself must never treat a content-source volume's class stamp as
// authoritative. This simulates a volume whose scrub did not land — or one
// provisioned by a driver version that predates it.
func TestPerformanceClassGuardIgnoresContentSourceStamp(t *testing.T) {
	ctx := context.Background()
	mock := truenas.NewMockClient()
	d := perfContentSourceDriver(mock)
	seedStampedPerformanceClassSource(t, mock)

	request := func(class string) *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name:               "restored",
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			Parameters:         map[string]string{"protocol": "nfs", zfsPerformanceClassParam: class},
			VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "stamped-snap"},
			}},
		}
	}
	_, err := d.CreateVolume(ctx, request("media"))
	require.NoError(t, err)

	// Put the lie back, exactly as a failed scrub would leave it.
	require.NoError(t, mock.DatasetSetUserProperties(ctx, "pool/parent/restored", map[string]string{
		PropZFSPerformanceClass: "database",
	}))

	_, err = d.CreateVolume(ctx, request("media"))
	require.NoError(t, err, "an inherited stamp must not be able to refuse an exact request replay")

	healed, err := mock.DatasetGet(ctx, "pool/parent/restored")
	require.NoError(t, err)
	_, stamped := healed.UserProperties[PropZFSPerformanceClass]
	assert.False(t, stamped, "the existing-volume arm heals a content-source volume's stale class stamp")
}
