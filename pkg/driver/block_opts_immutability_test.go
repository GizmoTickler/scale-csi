package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// Round-2 regression tests for the two blockers the re-verification left open:
//
//   - N-1: a snapshot restore into a StorageClass with a DIFFERENT explicit
//     blocksize returned SUCCESS and laid that geometry over data cloned
//     byte-for-byte from a volume written against another one. The clone fold
//     stamps the REQUEST's own options onto the destination BEFORE the share
//     builder's guard reads them, so the guard compared 512 against 512 and saw
//     no conflict — it was structurally defeated on the clone path.
//   - codex gate #1: changed queuedCommands / availThreshold / insecureTpc /
//     readOnly / authNetworks / qidMax / piEnable on an EXISTING volume returned
//     success while the backend stayed exactly as provisioned.
//
// Every test below is written to FAIL on the pre-fix tree: each drives the real
// CreateVolume entry point and asserts the gRPC status, not an internal helper.

// newBlockImmutabilityDriver builds an iSCSI+NVMe-oF driver over the in-memory
// mock. The controller-default blocksize is 512, which is the value a defeated
// geometry guard silently writes.
func newBlockImmutabilityDriver(t *testing.T) (*Driver, *truenas.MockClient) {
	t.Helper()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "org.scale.csi.test",
		config: &Config{
			DriverName: "org.scale.csi.block",
			ZFS: ZFSConfig{
				DatasetParentName:   "pool/parent",
				DatasetEnableQuotas: true,
				ZvolReadyTimeout:    1,
			},
			ISCSI: ISCSIConfig{
				Enabled:         true,
				TargetPortal:    "192.0.2.10:3260",
				ExtentBlocksize: 512,
				ExtentRpm:       "SSD",
			},
			NVMeoF: NVMeoFConfig{
				Enabled:               true,
				Transport:             "TCP",
				TransportAddress:      "192.0.2.20",
				TransportServiceID:    4420,
				SubsystemAllowAnyHost: true,
			},
		},
		truenasClient: client,
	}
	d.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(ctx context.Context, service string) error {
		return client.ServiceReload(ctx, service)
	})
	t.Cleanup(d.serviceReloadDebouncer.Stop)
	return d, client
}

// blockTuningRequest builds a CreateVolume request for the given protocol with
// the supplied block-tuning StorageClass parameters.
func blockTuningRequest(name, protocol string, tuning map[string]string) *csi.CreateVolumeRequest {
	params := map[string]string{"protocol": protocol}
	for key, value := range tuning {
		params[key] = value
	}
	return &csi.CreateVolumeRequest{
		Name:               name,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         params,
	}
}

// restoreFromSnapshot points a CreateVolume request at a snapshot.
func restoreFromSnapshot(req *csi.CreateVolumeRequest, snapshotID string) *csi.CreateVolumeRequest {
	req.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapshotID},
	}}
	return req
}

// provision4096Source creates a real 4096-blocksize iSCSI volume through
// CreateVolume and snapshots it, returning the snapshot's CSI ID.
func provision4096Source(t *testing.T, d *Driver, client *truenas.MockClient, volumeName, snapshotName string) string {
	t.Helper()
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, blockTuningRequest(volumeName, "iscsi", map[string]string{
		paramISCSIBlocksize: "4096",
	}))
	require.NoError(t, err)

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/"+volumeName)
	require.NoError(t, err)
	require.NotNil(t, extent)
	require.Equal(t, 4096, extent.Blocksize, "the source volume must really be 4096 for this test to mean anything")

	_, err = client.SnapshotCreate(ctx, "pool/parent/"+volumeName, snapshotName, map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           snapshotName,
		PropCSISnapshotSourceVolumeID: volumeName,
	})
	require.NoError(t, err)
	return snapshotName
}

// TestSnapshotRestoreIntoConflictingBlocksizeClassFailsClosed is the N-1
// regression test.
//
// Pre-fix behavior (reproduced by the re-verification): CreateVolume returned
// OK, the destination was stamped blocksize=512, and a 512-byte logical extent
// was created over a ZFS clone whose filesystem and partition table were laid
// out against 4096-byte blocks. That is the F-2 corruption class on a second
// path.
func TestSnapshotRestoreIntoConflictingBlocksizeClassFailsClosed(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	snapshotID := provision4096Source(t, d, client, "pvc-src-4k", "restore-point")

	_, err := d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-restore-512", "iscsi", map[string]string{paramISCSIBlocksize: "512"}),
		snapshotID,
	))
	require.Error(t, err, "restoring a 4096 volume into an explicit 512 class must not succeed")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err),
		"a geometry conflict against the clone SOURCE must fail closed, got: %v", err)
	assert.Contains(t, status.Convert(err).Message(), paramISCSIBlocksize)

	// And it must fail BEFORE any destination mutation: no dataset, and above all
	// no 512-byte extent over 4096-geometry data.
	_, getErr := client.DatasetGet(ctx, "pool/parent/pvc-restore-512")
	assert.True(t, truenas.IsNotFoundError(getErr),
		"the rejected restore must not have created the destination dataset (err=%v)", getErr)
	extent, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-restore-512")
	require.NoError(t, findErr)
	assert.Nil(t, extent, "the rejected restore must not have created an extent at the wrong geometry")
}

// TestSnapshotRestoreWithNoOptsStillInheritsSourceGeometry pins the case the
// fix must NOT regress: a restore into a class that opts into NOTHING inherits
// the source's 4096 geometry (the conservative direction), rather than silently
// reverting to the 512 controller default.
func TestSnapshotRestoreWithNoOptsStillInheritsSourceGeometry(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	snapshotID := provision4096Source(t, d, client, "pvc-src-inherit", "inherit-point")

	_, err := d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-restore-inherit", "iscsi", nil), snapshotID))
	require.NoError(t, err, "a no-opts restore has no geometry opinion and must still succeed")

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-restore-inherit")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 4096, extent.Blocksize,
		"a no-opts restore must inherit the source's 4096 geometry; 512 here is the controller default written over cloned 4096 data")
}

// TestSnapshotRestoreIntoMatchingBlocksizeClassSucceeds proves the guard fires
// on a genuine CONFLICT only, not on any explicit geometry.
func TestSnapshotRestoreIntoMatchingBlocksizeClassSucceeds(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	snapshotID := provision4096Source(t, d, client, "pvc-src-match", "match-point")

	_, err := d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-restore-match", "iscsi", map[string]string{paramISCSIBlocksize: "4096"}),
		snapshotID,
	))
	require.NoError(t, err, "restoring into a class that agrees with the source must succeed")

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-restore-match")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 4096, extent.Blocksize)
}

// TestVolumeCloneIntoConflictingBlocksizeClassFailsClosed is the PVC-to-PVC
// flavor of N-1. Kubernetes restricts this one to a single StorageClass, but the
// CSI RPC does not, and the driver must not depend on the CO for a
// data-corruption guard.
func TestVolumeCloneIntoConflictingBlocksizeClassFailsClosed(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-clone-src", "iscsi", map[string]string{
		paramISCSIBlocksize: "4096",
	}))
	require.NoError(t, err)

	req := blockTuningRequest("pvc-clone-512", "iscsi", map[string]string{paramISCSIBlocksize: "512"})
	req.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
		Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "pvc-clone-src"},
	}}
	_, err = d.CreateVolume(ctx, req)
	require.Error(t, err, "cloning a 4096 volume into an explicit 512 class must not succeed")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	// The guard runs off the source-existence get that the clone path already
	// issued, so it must reject before the temporary source snapshot is taken.
	snapshots, listErr := client.SnapshotList(ctx, "pool/parent/pvc-clone-src")
	require.NoError(t, listErr)
	assert.Empty(t, snapshots, "the rejected clone must not have snapshotted the source")
}

// TestPblocksizeCloneConflictFailsClosed covers the second geometry field on the
// clone path.
func TestPblocksizeCloneConflictFailsClosed(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-pblock-src", "iscsi", map[string]string{
		paramISCSIPblocksize: "true",
	}))
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, "pool/parent/pvc-pblock-src", "pblock-point", nil)
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-pblock-restore", "iscsi", map[string]string{paramISCSIPblocksize: "false"}),
		"pblock-point",
	))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), paramISCSIPblocksize)
}

// TestCloneSourceGeometryProbeAPICallCost pins the round-trip cost of the N-1
// guard, so it stays honest about what it charges:
//
//   - a class with NO geometry opinion short-circuits before any API call, so the
//     default clone path's golden counts are unchanged (+0);
//   - a class that opts in against a STAMPED source pays exactly one DatasetGet;
//   - a class that opts in against an UNSTAMPED source (every volume provisioned
//     before these knobs existed) pays that DatasetGet plus one
//     ISCSIExtentFindByDisk to read the geometry the data was really written
//     against. That second call is what closes the corruption path for the
//     installed base, and it is charged only in the case that needs it.
func TestCloneSourceGeometryProbeAPICallCost(t *testing.T) {
	measure := func(t *testing.T, name string, tuning map[string]string, stamp map[string]string) (int, map[string]int) {
		t.Helper()
		client := newAPICallCountingClient()
		d := newAPICallCountDriver(t, client, "iscsi")
		ctx := context.Background()
		_, err := client.MockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
			Name: "pool/parent", Type: "FILESYSTEM",
		})
		require.NoError(t, err)
		_, err = client.MockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
			Name: "pool/parent/clone-source", Type: "VOLUME", Volsize: testGiB,
		})
		require.NoError(t, err)
		if len(stamp) > 0 {
			require.NoError(t, client.MockClient.DatasetSetUserProperties(ctx, "pool/parent/clone-source", stamp))
		}
		_, err = client.MockClient.SnapshotCreate(ctx, "pool/parent/clone-source", "clone-point", nil)
		require.NoError(t, err)

		client.resetCalls()
		_, err = d.CreateVolume(ctx, restoreFromSnapshot(blockTuningRequest(name, "iscsi", tuning), "clone-point"))
		require.NoError(t, err)
		return client.callSnapshot()
	}

	// The provisioning path itself resolves the DESTINATION's extent by disk, so
	// the source probe is measured as a DELTA against the default path.
	base, baseMethods := measure(t, "restore-default", nil, nil)

	stamped, stampedMethods := measure(t, "restore-stamped", map[string]string{paramISCSIBlocksize: "512"},
		map[string]string{PropBlockISCSIBlocksize: "512"})
	assert.Equal(t, base+1, stamped,
		"a STAMPED source answers from its own properties: exactly one extra DatasetGet")
	assert.Equal(t, baseMethods["ISCSIExtentFindByDisk"], stampedMethods["ISCSIExtentFindByDisk"],
		"a stamped source must not pay for the live-geometry fallback")

	unstamped, unstampedMethods := measure(t, "restore-unstamped", map[string]string{paramISCSIBlocksize: "512"}, nil)
	assert.Equal(t, base+2, unstamped,
		"an UNSTAMPED source costs the DatasetGet plus one live-extent read — the price of not corrupting the pre-GF4 fleet")
	assert.Equal(t, baseMethods["ISCSIExtentFindByDisk"]+1, unstampedMethods["ISCSIExtentFindByDisk"],
		"the live-geometry fallback must be a single lookup, issued only when the stamp cannot answer")
}

// provisionUnstamped4096Source creates a 4096-geometry iSCSI volume the way the
// ENTIRE pre-GF4 fleet exists: the controller-wide default supplies the
// geometry, the StorageClass sets no block parameters, and the dataset therefore
// carries no block stamp at all. It returns the snapshot's CSI ID.
func provisionUnstamped4096Source(t *testing.T, d *Driver, client *truenas.MockClient, volumeName, snapshotName string) string {
	t.Helper()
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	// No block parameters: this is a legacy volume, provisioned before the knobs
	// existed, on an install whose controller-wide default is 4096. The
	// controller default STAYS 4096 — that is why the fleet is 4096 — and the
	// conflict comes from a StorageClass that later opts into something else,
	// which is exactly the rollout these parameters enable.
	d.config.ISCSI.ExtentBlocksize = 4096
	_, err = d.CreateVolume(ctx, blockTuningRequest(volumeName, "iscsi", nil))
	require.NoError(t, err)

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/"+volumeName)
	require.NoError(t, err)
	require.NotNil(t, extent)
	require.Equal(t, 4096, extent.Blocksize, "the source volume must really be 4096 for this test to mean anything")

	ds, err := client.DatasetGet(ctx, "pool/parent/"+volumeName)
	require.NoError(t, err)
	require.Nil(t, blockOptsFromDataset(ds),
		"the source must be UNSTAMPED — that is the whole point of this fixture")

	_, err = client.SnapshotCreate(ctx, "pool/parent/"+volumeName, snapshotName, map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           snapshotName,
		PropCSISnapshotSourceVolumeID: volumeName,
	})
	require.NoError(t, err)
	return snapshotName
}

// TestSnapshotRestoreOfUnstampedSourceIntoConflictingBlocksizeFailsClosed is the
// round-3 N-1 regression test.
//
// The round-2 fix compared the request against the SOURCE's stamp, which closed
// the corruption for volumes provisioned by a knobbed StorageClass — and left it
// wide open for every volume that predates the knobs, because an unstamped
// source has no stored geometry to contradict. That is the entire installed
// base, and it is exactly what a newly added per-class blocksize gets pointed
// at: restoring a legacy 4096 snapshot into a class that says 512 returned
// SUCCESS with a 512-byte extent over 4096-geometry data.
func TestSnapshotRestoreOfUnstampedSourceIntoConflictingBlocksizeFailsClosed(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	snapshotID := provisionUnstamped4096Source(t, d, client, "pvc-legacy-4k", "legacy-point")

	_, err := d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-legacy-restore", "iscsi", map[string]string{paramISCSIBlocksize: "512"}),
		snapshotID,
	))
	require.Error(t, err, "restoring an unstamped 4096 volume into an explicit 512 class must not succeed")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err),
		"the source's LIVE geometry must be consulted when it carries no stamp, got: %v", err)
	assert.Contains(t, status.Convert(err).Message(), paramISCSIBlocksize)

	// Zero orphans: the rejection precedes the first destination mutation.
	_, getErr := client.DatasetGet(ctx, "pool/parent/pvc-legacy-restore")
	assert.True(t, truenas.IsNotFoundError(getErr),
		"the rejected restore must not have created the destination dataset (err=%v)", getErr)
	extent, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-legacy-restore")
	require.NoError(t, findErr)
	assert.Nil(t, extent, "the rejected restore must not have created an extent at the wrong geometry")
}

// TestDetachedRestoreOfUnstampedSourceFailsClosed is the same corruption on the
// independent-copy flavor (snapshotRestoreMode: detached), which takes a
// different branch through handleVolumeContentSource.
func TestDetachedRestoreOfUnstampedSourceFailsClosed(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	snapshotID := provisionUnstamped4096Source(t, d, client, "pvc-legacy-detach-src", "legacy-detach-point")

	req := restoreFromSnapshot(
		blockTuningRequest("pvc-legacy-detach", "iscsi", map[string]string{paramISCSIBlocksize: "512"}),
		snapshotID,
	)
	req.Parameters["snapshotRestoreMode"] = "detached"
	_, err := d.CreateVolume(ctx, req)
	require.Error(t, err, "a detached copy carries the source's byte layout too and must be guarded identically")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), paramISCSIBlocksize)

	_, getErr := client.DatasetGet(ctx, "pool/parent/pvc-legacy-detach")
	assert.True(t, truenas.IsNotFoundError(getErr),
		"the rejected detached restore must not have created the destination dataset (err=%v)", getErr)
	extent, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-legacy-detach")
	require.NoError(t, findErr)
	assert.Nil(t, extent, "the rejected detached restore must not have created an extent")
}

// TestVolumeCloneOfUnstampedSourceFailsClosed is the PVC-to-PVC flavor.
func TestVolumeCloneOfUnstampedSourceFailsClosed(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	provisionUnstamped4096Source(t, d, client, "pvc-legacy-clone-src", "unused-point")

	req := blockTuningRequest("pvc-legacy-clone", "iscsi", map[string]string{paramISCSIBlocksize: "512"})
	req.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
		Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "pvc-legacy-clone-src"},
	}}
	_, err := d.CreateVolume(ctx, req)
	require.Error(t, err, "cloning an unstamped 4096 volume into an explicit 512 class must not succeed")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), paramISCSIBlocksize)

	_, getErr := client.DatasetGet(ctx, "pool/parent/pvc-legacy-clone")
	assert.True(t, truenas.IsNotFoundError(getErr),
		"the rejected clone must not have created the destination dataset (err=%v)", getErr)
	extent, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-legacy-clone")
	require.NoError(t, findErr)
	assert.Nil(t, extent, "the rejected clone must not have created an extent")
	// And it rejects off the source-existence get, before the temp snapshot.
	snapshots, listErr := client.SnapshotList(ctx, "pool/parent/pvc-legacy-clone-src")
	require.NoError(t, listErr)
	for _, snap := range snapshots {
		assert.NotContains(t, snap.Name, "clone-source-",
			"the rejected clone must not have snapshotted the source")
	}
}

// TestUnstampedSourceRestoreWithNoOptsIsNeverRejected pins the case the
// live-geometry fallback must NOT break: an unstamped source restored by a class
// that opts into nothing has no opinion to contradict, so it must succeed (and,
// per TestCloneSourceGeometryProbeAPICallCost, must not pay for the probe at
// all) and come out at the source's 4096 geometry.
func TestUnstampedSourceRestoreWithNoOptsIsNeverRejected(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	snapshotID := provisionUnstamped4096Source(t, d, client, "pvc-legacy-inherit-src", "legacy-inherit-point")

	_, err := d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-legacy-inherit", "iscsi", nil), snapshotID))
	require.NoError(t, err, "a no-opts restore of an unstamped source has no geometry opinion and must still succeed")

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-legacy-inherit")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 4096, extent.Blocksize,
		"a no-opts restore must inherit the source's 4096 geometry from the ZFS clone, not the 512 controller default")
}

// TestUnstampedSourceRestoreIntoMatchingClassSucceeds proves the fallback fires
// on a genuine conflict only: the same unstamped 4096 source restored into a
// class that agrees is accepted.
func TestUnstampedSourceRestoreIntoMatchingClassSucceeds(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	snapshotID := provisionUnstamped4096Source(t, d, client, "pvc-legacy-match-src", "legacy-match-point")

	_, err := d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-legacy-match", "iscsi", map[string]string{paramISCSIBlocksize: "4096"}),
		snapshotID,
	))
	require.NoError(t, err, "restoring an unstamped source into a class that matches its live geometry must succeed")

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-legacy-match")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 4096, extent.Blocksize)
}

// TestUnstampedSourceWithNoExtentIsNotAConflict covers the remaining shape: a
// source that carries neither a stamp NOR a live extent (an NVMe-oF volume, or
// one whose share objects are gone) yields no geometry to contradict, and the
// restore must not be rejected on a guess.
func TestUnstampedSourceWithNoExtentIsNotAConflict(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/bare-source", Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, "pool/parent/bare-source", "bare-point", nil)
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-bare-restore", "iscsi", map[string]string{paramISCSIBlocksize: "512"}),
		"bare-point",
	))
	require.NoError(t, err, "no stamp and no live extent means no recorded geometry — nothing to contradict")
}

// ---------------------------------------------------------------------------
// codex gate #1 — every per-volume knob is immutable and says so
// ---------------------------------------------------------------------------

// TestChangedISCSITuningOnExistingVolumeFailsClosed is the gate #1 regression
// test. Pre-fix, EVERY case below returned success with the backend still
// carrying the originally provisioned value — a silent no-op that was neither
// documented nor tested, and for insecureTpc / readOnly / authNetworks a
// safety-contract violation (the target stayed permissive/writable while the
// caller was told the restrictive class had been applied).
func TestChangedISCSITuningOnExistingVolumeFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		param   string
		created string
		changed string
	}{
		{name: "queuedCommands", param: paramISCSIQueuedCommands, created: "32", changed: "128"},
		{name: "availThreshold", param: paramISCSIAvailThreshold, created: "50", changed: "90"},
		{name: "insecureTpc", param: paramISCSIInsecureTpc, created: "true", changed: "false"},
		{name: "readOnly", param: paramISCSIReadOnly, created: "false", changed: "true"},
		{name: "authNetworks", param: paramISCSIAuthNetworks, created: "10.0.0.0/8", changed: "192.168.0.0/16"},
		{name: "pblocksize", param: paramISCSIPblocksize, created: "true", changed: "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, client := newBlockImmutabilityDriver(t)
			ctx := context.Background()
			_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
			require.NoError(t, err)

			volumeName := "pvc-mutate-" + tc.name
			_, err = d.CreateVolume(ctx, blockTuningRequest(volumeName, "iscsi", map[string]string{tc.param: tc.created}))
			require.NoError(t, err)

			// Same value: an idempotent replay must still succeed.
			_, err = d.CreateVolume(ctx, blockTuningRequest(volumeName, "iscsi", map[string]string{tc.param: tc.created}))
			require.NoError(t, err, "a same-value replay must remain idempotently successful")

			// Changed value: must fail closed, naming the parameter.
			_, err = d.CreateVolume(ctx, blockTuningRequest(volumeName, "iscsi", map[string]string{tc.param: tc.changed}))
			require.Error(t, err, "a changed %s must not return success over an unchanged backend", tc.param)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tc.param,
				"the error must name the offending parameter so the operator can act on it")
		})
	}
}

// TestUnappliableISCSITuningOnUntunedVolumeFailsClosed covers the other half of
// "no field may silently accept-and-ignore": a volume provisioned WITHOUT a knob
// cannot have it applied afterwards either, so requesting it on an existing
// volume must fail closed rather than return success over an extent that never
// got the value.
func TestUnappliableISCSITuningOnUntunedVolumeFailsClosed(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-untuned", "iscsi", nil))
	require.NoError(t, err)

	for _, tc := range []struct{ name, param, value string }{
		{name: "stableSerial", param: paramISCSIStableSerial, value: "true"},
		{name: "availThreshold", param: paramISCSIAvailThreshold, value: "80"},
		{name: "authNetworks", param: paramISCSIAuthNetworks, value: "10.0.0.0/8"},
		{name: "queuedCommands", param: paramISCSIQueuedCommands, value: "128"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, replayErr := d.CreateVolume(ctx, blockTuningRequest("pvc-untuned", "iscsi", map[string]string{tc.param: tc.value}))
			require.Error(t, replayErr, "%s cannot be applied to an existing extent/target and must not return success", tc.param)
			assert.Equal(t, codes.FailedPrecondition, status.Code(replayErr))
			assert.Contains(t, status.Convert(replayErr).Message(), tc.param)
		})
	}

	// The backend is genuinely untouched by any of the rejected replays.
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-untuned")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Nil(t, extent.AvailThreshold)
	assert.Empty(t, extent.Serial)
}

// TestUnchangedISCSITuningReplayLeavesBackendAlone proves the accepted half of
// the contract: a same-value replay of a fully tuned volume succeeds and changes
// nothing on the backend (no update churn, no re-created objects).
func TestUnchangedISCSITuningReplayLeavesBackendAlone(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	tuning := map[string]string{
		paramISCSIBlocksize:      "4096",
		paramISCSIPblocksize:     "true",
		paramISCSIQueuedCommands: "128",
		paramISCSIInsecureTpc:    "false",
		paramISCSIReadOnly:       "true",
		paramISCSIAvailThreshold: "80",
		paramISCSIStableSerial:   "true",
		paramISCSIAuthNetworks:   "10.0.0.0/8,192.168.0.0/16",
	}
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-full-tune", "iscsi", tuning))
	require.NoError(t, err)

	before, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-full-tune")
	require.NoError(t, err)
	require.NotNil(t, before)
	beforeCopy := *before

	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-full-tune", "iscsi", tuning))
	require.NoError(t, err, "replaying every knob at the same value must remain successful")

	after, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-full-tune")
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, beforeCopy.ID, after.ID, "the replay must not have re-created the extent")
	assert.Equal(t, beforeCopy.Blocksize, after.Blocksize)
	assert.Equal(t, beforeCopy.Serial, after.Serial)
	assert.Equal(t, beforeCopy.Ro, after.Ro)
	assert.Equal(t, beforeCopy.InsecureTpc, after.InsecureTpc)
	require.NotNil(t, after.AvailThreshold)
	assert.Equal(t, 80, *after.AvailThreshold)

	target, err := client.ISCSITargetFindByName(ctx, d.iscsiShareName("pvc-full-tune"))
	require.NoError(t, err)
	require.NotNil(t, target)
	require.NotNil(t, target.QueuedCommands)
	assert.Equal(t, 128, *target.QueuedCommands)
	assert.Equal(t, []string{"10.0.0.0/8", "192.168.0.0/16"}, target.AuthNetworks)
}

// TestChangedNVMeoFTuningOnExistingVolumeFailsClosed is the NVMe-oF half of gate
// #1. The already-exists fast path used to return success BEFORE it evaluated
// the requested subsystem options, so a changed qid_max — or pi_enable, a
// data-integrity control — was unconditionally ignored.
func TestChangedNVMeoFTuningOnExistingVolumeFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		param   string
		created string
		changed string
	}{
		{name: "qidMax", param: paramNVMeoFQidMax, created: "64", changed: "128"},
		{name: "piEnable", param: paramNVMeoFPiEnable, created: "false", changed: "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, client := newBlockImmutabilityDriver(t)
			ctx := context.Background()
			_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
			require.NoError(t, err)

			volumeName := "pvc-nvme-" + tc.name
			_, err = d.CreateVolume(ctx, blockTuningRequest(volumeName, "nvmeof", map[string]string{tc.param: tc.created}))
			require.NoError(t, err)

			_, err = d.CreateVolume(ctx, blockTuningRequest(volumeName, "nvmeof", map[string]string{tc.param: tc.created}))
			require.NoError(t, err, "a same-value replay must remain idempotently successful")

			_, err = d.CreateVolume(ctx, blockTuningRequest(volumeName, "nvmeof", map[string]string{tc.param: tc.changed}))
			require.Error(t, err, "a changed %s must not return success over an unchanged subsystem", tc.param)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tc.param)

			subsys, findErr := client.NVMeoFSubsystemFindByName(ctx, d.nvmeSubsystemName("pool/parent/"+volumeName))
			require.NoError(t, findErr)
			require.NotNil(t, subsys)
			if tc.param == paramNVMeoFQidMax {
				require.NotNil(t, subsys.QidMax)
				assert.Equal(t, 64, *subsys.QidMax, "the rejected replay must not have mutated the subsystem")
			} else {
				// Deliberately written WITHOUT a pointer dereference so this file
				// still COMPILES against the pre-fix tree, where PiEnable was a plain
				// bool. A revert-proof that cannot be built on the tree it is meant to
				// indict proves nothing; the substance (both subtests fail pre-fix at
				// the require.Error above) is unchanged, and the assertion is exactly
				// as strong — non-nil and false.
				assert.Equal(t, boolPtr(false), subsys.PiEnable, "the rejected replay must not have enabled T10-PI")
			}
		})
	}
}

// TestTurningAKnobOffIsAChangeToo closes the last accepted-and-ignored corner of
// gate #1. iscsi/stableSerial and iscsi/authNetworks are the only two knobs whose
// "off" value degrades to an empty one ("" and an empty CIDR list), so the
// value-based conflict helpers read them as "no opinion" and let the request
// through — the volume kept its pinned serial and its network ACL while the
// caller was told the new class had been applied. Every other knob is a pointer
// and already fails closed in both directions.
func TestTurningAKnobOffIsAChangeToo(t *testing.T) {
	t.Run("stableSerial true -> false", func(t *testing.T) {
		d, client := newBlockImmutabilityDriver(t)
		ctx := context.Background()
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, err)
		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-serial-off", "iscsi",
			map[string]string{paramISCSIStableSerial: "true"}))
		require.NoError(t, err)

		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-serial-off", "iscsi",
			map[string]string{paramISCSIStableSerial: "false"}))
		require.Error(t, err, "un-pinning a volume's SCSI identity must not be accepted and ignored")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, status.Convert(err).Message(), paramISCSIStableSerial)

		extent, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-serial-off")
		require.NoError(t, findErr)
		require.NotNil(t, extent)
		assert.Equal(t, stableISCSISerial("pvc-serial-off"), extent.Serial,
			"the rejected replay must have left the pinned serial in place")
	})

	t.Run("authNetworks set -> empty", func(t *testing.T) {
		d, client := newBlockImmutabilityDriver(t)
		ctx := context.Background()
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, err)
		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-acl-off", "iscsi",
			map[string]string{paramISCSIAuthNetworks: "10.0.0.0/8"}))
		require.NoError(t, err)

		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-acl-off", "iscsi",
			map[string]string{paramISCSIAuthNetworks: ""}))
		require.Error(t, err, "dropping a target's network ACL must not be accepted and ignored")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, status.Convert(err).Message(), paramISCSIAuthNetworks)

		target, findErr := client.ISCSITargetFindByName(ctx, d.iscsiShareName("pvc-acl-off"))
		require.NoError(t, findErr)
		require.NotNil(t, target)
		assert.Equal(t, []string{"10.0.0.0/8"}, target.AuthNetworks,
			"the rejected replay must have left the ACL in place")
	})

	// The other direction of the same rule: a volume that was created with the
	// knob OFF replays at OFF idempotently. This is what stops the fix from
	// turning every stableSerial: "false" class into a permanent failure — note
	// TrueNAS auto-generates a serial for every extent, so "the extent has a
	// serial" is not evidence that the volume was pinned.
	t.Run("off stays idempotent", func(t *testing.T) {
		d, client := newBlockImmutabilityDriver(t)
		ctx := context.Background()
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, err)
		off := map[string]string{paramISCSIStableSerial: "false", paramISCSIAuthNetworks: ""}
		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-off-off", "iscsi", off))
		require.NoError(t, err)
		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-off-off", "iscsi", off))
		require.NoError(t, err, "a same-value replay of an OFF knob must remain idempotently successful")

		// Even against a backend-assigned serial the volume was never pinned to.
		extent, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-off-off")
		require.NoError(t, findErr)
		require.NotNil(t, extent)
		extent.Serial = "AUTOGENERATED123"
		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-off-off", "iscsi", off))
		require.NoError(t, err,
			"an auto-generated serial is not a pinned one and must not be read as a stableSerial conflict")
	})

	// And a no-opts publish is still never rejected by the new checks.
	t.Run("no-opts rebuild unaffected", func(t *testing.T) {
		d, client := newBlockImmutabilityDriver(t)
		ctx := context.Background()
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, err)
		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-off-noopts", "iscsi", map[string]string{
			paramISCSIStableSerial: "true", paramISCSIAuthNetworks: "10.0.0.0/8",
		}))
		require.NoError(t, err)
		require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, "pool/parent/pvc-off-noopts", "pvc-off-noopts", nil),
			"a publish carries no request opts and must not trip the off-direction checks")
	})
}

// TestOffTransitionsAlsoFailClosedOnTheAbsentObjectPath is the DR-rebuild flavor
// of the same rule, where the stamp is the only record of what the volume has.
func TestOffTransitionsAlsoFailClosedOnTheAbsentObjectPath(t *testing.T) {
	stored := &blockOpts{
		iscsiSerial:       "0123456789abcdef",
		iscsiAuthNetworks: []string{"10.0.0.0/8"},
	}
	serialOff := &blockOpts{iscsiStableSerial: boolPtr(false)}
	err := guardStoredBlockTuning(stored, serialOff, "pool/parent/pvc")
	require.Error(t, err, "turning stableSerial off must fail closed on the absent-object path too")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), paramISCSIStableSerial)

	networksOff := &blockOpts{iscsiAuthNetworksSet: true}
	err = guardStoredBlockTuning(stored, networksOff, "pool/parent/pvc")
	require.Error(t, err, "emptying authNetworks must fail closed on the absent-object path too")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), paramISCSIAuthNetworks)

	// Nothing stamped: the same "off" request is a plain create and must pass.
	assert.NoError(t, guardStoredBlockTuning(&blockOpts{iscsiQueuedCommands: intPtr(32)}, serialOff, "pool/parent/pvc"))
	assert.NoError(t, guardStoredBlockTuning(&blockOpts{iscsiQueuedCommands: intPtr(32)}, networksOff, "pool/parent/pvc"))
}

// TestExtentBoolFallsBackToStampWhenBackendOmitsIt is the residual-B regression
// test. pblocksize / insecure_tpc / ro used to be plain bools on the response
// model, so the live object was ALWAYS authoritative for them and the documented
// "stamp is the fallback when the backend omits the field" rule genuinely held
// for only 7 of the 10 knobs. An omitted field parsed as false and turned a
// same-value replay into a false-positive FailedPrecondition.
func TestExtentBoolFallsBackToStampWhenBackendOmitsIt(t *testing.T) {
	// An extent as parsed from a response that did not report the three bools.
	silent := &truenas.ISCSIExtent{ID: 1, Name: "pvc", Blocksize: 4096}
	stamped := &blockOpts{
		iscsiPblocksize:  boolPtr(true),
		iscsiInsecureTpc: boolPtr(true),
		iscsiReadOnly:    boolPtr(true),
	}
	assert.NoError(t, guardExistingISCSIExtentOpts(silent, stamped, stamped, "pool/parent/pvc"),
		"with the backend silent the stamp decides, and a same-value replay must not be rejected")

	for name, request := range map[string]*blockOpts{
		paramISCSIPblocksize:  {iscsiPblocksize: boolPtr(false)},
		paramISCSIInsecureTpc: {iscsiInsecureTpc: boolPtr(false)},
		paramISCSIReadOnly:    {iscsiReadOnly: boolPtr(false)},
	} {
		err := guardExistingISCSIExtentOpts(silent, request, stamped, "pool/parent/pvc")
		require.Error(t, err, "%s: a genuine divergence from the stamp must still fail closed", name)
		assert.Contains(t, status.Convert(err).Message(), name)
		// With neither a live value nor a stamp, the request cannot be confirmed
		// as already in effect and fails closed rather than being acknowledged.
		err = guardExistingISCSIExtentOpts(silent, request, nil, "pool/parent/pvc")
		require.Error(t, err, "%s: a value that was never applied must not be acknowledged", name)
		assert.Contains(t, status.Convert(err).Message(), "unset")
	}

	// And when the backend DOES report the field it stays authoritative. The
	// extent is built through the client so this pins the real reported shape,
	// not a hand-written struct.
	client := truenas.NewMockClient()
	reported, err := client.ISCSIExtentCreate(context.Background(), "pvc", "zvol/pool/parent/pvc", "", 4096, true, "SSD",
		truenas.ISCSIExtentCreateOptions{InsecureTpc: boolPtr(false)})
	require.NoError(t, err)
	guardErr := guardExistingISCSIExtentOpts(reported, &blockOpts{iscsiInsecureTpc: boolPtr(true)}, stamped, "pool/parent/pvc")
	require.Error(t, guardErr, "a reported live value must win over a disagreeing stamp")
	assert.Contains(t, status.Convert(guardErr).Message(), paramISCSIInsecureTpc)
}

// TestSubsystemPiEnableDistinguishesNullFromFalse pins the response-model fix
// codex asked for: nvmet.subsys.query returns boolean-or-null, and collapsing
// null into false would let a replay conclude that a requested pi_enable=false
// was "already in effect" on a subsystem that never reported the field.
func TestSubsystemPiEnableDistinguishesNullFromFalse(t *testing.T) {
	client := truenas.NewMockClient()
	ctx := context.Background()

	unset, err := client.NVMeoFSubsystemCreate(ctx, "subsys-unset", true, nil)
	require.NoError(t, err)
	assert.Nil(t, unset.PiEnable, "an omitted pi_enable must stay nil, not collapse to false")

	explicitFalse, err := client.NVMeoFSubsystemCreate(ctx, "subsys-false", true, nil,
		truenas.NVMeoFSubsystemCreateOptions{PiEnable: boolPtr(false)})
	require.NoError(t, err)
	require.NotNil(t, explicitFalse.PiEnable)
	assert.False(t, *explicitFalse.PiEnable)

	// The guard consumes that distinction: with the backend silent, the stamp
	// decides; with neither, the request cannot be confirmed and fails closed.
	assert.NoError(t, guardExistingNVMeoFSubsystemOpts(explicitFalse,
		&blockOpts{nvmeofPiEnable: boolPtr(false)}, nil, "pool/parent/pvc"))
	assert.Error(t, guardExistingNVMeoFSubsystemOpts(unset,
		&blockOpts{nvmeofPiEnable: boolPtr(false)}, nil, "pool/parent/pvc"))
	assert.NoError(t, guardExistingNVMeoFSubsystemOpts(unset,
		&blockOpts{nvmeofPiEnable: boolPtr(false)},
		&blockOpts{nvmeofPiEnable: boolPtr(false)}, "pool/parent/pvc"))
}

// TestNoOptsReplayNeverTripsTheImmutabilityGuards is the F-1 lesson applied to
// the new guards: a publish / startup-reconcile / DR rebuild carries NO request
// opts, so it has no opinion and must never be rejected — no matter how tuned
// the volume is.
func TestNoOptsReplayNeverTripsTheImmutabilityGuards(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-noopts", "iscsi", map[string]string{
		paramISCSIBlocksize:      "4096",
		paramISCSIQueuedCommands: "128",
		paramISCSIInsecureTpc:    "false",
		paramISCSIReadOnly:       "true",
		paramISCSIAvailThreshold: "80",
		paramISCSIStableSerial:   "true",
		paramISCSIAuthNetworks:   "10.0.0.0/8",
	}))
	require.NoError(t, err)

	// Plain ctx — exactly what ControllerPublishVolume and startup_reconcile.go
	// pass. Both the live-object and the stored-tuning guard must no-op.
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, "pool/parent/pvc-noopts", "pvc-noopts", nil),
		"a no-opts publish of a fully tuned volume must not be rejected")

	// Same after the objects are lost (the DR rebuild path).
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-noopts")
	require.NoError(t, err)
	require.NoError(t, client.ISCSIExtentDelete(ctx, extent.ID, false, true))
	require.NoError(t, client.DatasetSetUserProperties(ctx, "pool/parent/pvc-noopts",
		map[string]string{PropISCSIExtentID: "-"}))
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, "pool/parent/pvc-noopts", "pvc-noopts", nil),
		"a no-opts rebuild of a fully tuned volume must not be rejected")

	rebuilt, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-noopts")
	require.NoError(t, err)
	require.NotNil(t, rebuilt)
	assert.Equal(t, 4096, rebuilt.Blocksize, "the rebuild still replays the volume's own stored geometry")
}

// TestStoredTuningGuardRejectsChangeOnAbsentObject covers the rebuild path where
// there is no live object left to compare against, so the stamp is the only
// record of what the volume was provisioned with.
func TestStoredTuningGuardRejectsChangeOnAbsentObject(t *testing.T) {
	stored := &blockOpts{
		iscsiQueuedCommands: intPtr(32),
		iscsiInsecureTpc:    boolPtr(false),
		iscsiReadOnly:       boolPtr(true),
		iscsiAvailThreshold: intPtr(50),
		iscsiSerial:         "0123456789abcdef",
		iscsiAuthNetworks:   []string{"10.0.0.0/8"},
		nvmeofQidMax:        intPtr(64),
		nvmeofPiEnable:      boolPtr(true),
	}
	for name, request := range map[string]*blockOpts{
		paramISCSIQueuedCommands: {iscsiQueuedCommands: intPtr(128)},
		paramISCSIInsecureTpc:    {iscsiInsecureTpc: boolPtr(true)},
		paramISCSIReadOnly:       {iscsiReadOnly: boolPtr(false)},
		paramISCSIAvailThreshold: {iscsiAvailThreshold: intPtr(90)},
		paramISCSIStableSerial:   {iscsiSerial: "fedcba9876543210"},
		paramISCSIAuthNetworks:   {iscsiAuthNetworks: []string{"192.168.0.0/16"}},
		paramNVMeoFQidMax:        {nvmeofQidMax: intPtr(128)},
		paramNVMeoFPiEnable:      {nvmeofPiEnable: boolPtr(false)},
	} {
		err := guardStoredBlockTuning(stored, request, "pool/parent/pvc")
		require.Error(t, err, "%s: a stored-vs-request divergence must fail closed on the absent-object path", name)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, status.Convert(err).Message(), name)
	}

	// Same values, and either side absent, are all no-ops.
	assert.NoError(t, guardStoredBlockTuning(stored, stored, "pool/parent/pvc"))
	assert.NoError(t, guardStoredBlockTuning(nil, &blockOpts{iscsiQueuedCommands: intPtr(128)}, "pool/parent/pvc"))
	assert.NoError(t, guardStoredBlockTuning(stored, nil, "pool/parent/pvc"))
	// Order-insensitive CIDR comparison is not a conflict.
	assert.NoError(t, guardStoredBlockTuning(
		&blockOpts{iscsiAuthNetworks: []string{"10.0.0.0/8", "192.168.0.0/16"}},
		&blockOpts{iscsiAuthNetworks: []string{"192.168.0.0/16", "10.0.0.0/8"}},
		"pool/parent/pvc"))
}
