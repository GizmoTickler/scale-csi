package driver

import (
	"context"
	"fmt"
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

// TestCloneSourceGeometryProbeAPICallCost pins the round-trip cost of the
// clone-source geometry resolution, so it stays honest about what it charges.
//
// Round 4 CHANGED this cost deliberately, and the change IS the fix. The probe
// used to be gated on a StorageClass opting into a geometry — which is exactly
// how the controller-wide default became invisible to it (N-1e): a class that
// names no geometry still produces an extent, and that extent used to be created
// at whatever `iscsi.extentBlocksize` said, over data cloned byte-for-byte from
// a source that may have been written against something else. A guard that is
// cheap because it is blind is not cheap.
//
// A BLOCK clone/restore therefore resolves the source's real geometry ALWAYS:
// one DatasetGet for the source's stamp (skipped where the caller already holds
// the source dataset — the volume-clone path does) plus one ISCSIExtentFindByDisk
// for its live extent, which is the only thing that can answer for the pre-GF4
// fleet. The answer is stamped on the destination, so it is paid once per clone
// and never again per rebuild. Everything else is untouched: NFS short-circuits
// on share type before any call, and fresh provisioning / publish / unpublish /
// reconcile keep the golden counts in TestControllerGoldenPathAPICallCounts and
// TestControllerPublishUnpublishGoldenAPICallCounts exactly as they were.
func TestCloneSourceGeometryProbeAPICallCost(t *testing.T) {
	// (1) The resolution measured in isolation, which is the only way to state
	// its cost without arithmetic on somebody else's baseline.
	t.Run("resolution in isolation", func(t *testing.T) {
		newSource := func(t *testing.T, stamp map[string]string, withExtent bool) (*Driver, *apiCallCountingClient) {
			t.Helper()
			client := newAPICallCountingClient()
			d := newAPICallCountDriver(t, client, "iscsi")
			ctx := context.Background()
			_, err := client.MockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
				Name: "pool/parent/src", Type: "VOLUME", Volsize: testGiB,
			})
			require.NoError(t, err)
			if len(stamp) > 0 {
				require.NoError(t, client.MockClient.DatasetSetUserProperties(ctx, "pool/parent/src", stamp))
			}
			if withExtent {
				_, err = client.MockClient.ISCSIExtentCreate(ctx, "src", "zvol/pool/parent/src", "", 4096, true, "SSD")
				require.NoError(t, err)
			}
			client.resetCalls()
			return d, client
		}

		// A PVC-to-PVC clone asks about the source AS IT IS NOW, because its
		// temporary snapshot is taken from that state moments later. Stamp read
		// plus live-extent read.
		d, client := newSource(t, nil, true)
		_, err := d.resolveCloneSourceBlockGeometry(context.Background(), "pool/parent/src", nil, nil, "vol", "pool/parent/dst", ShareTypeISCSI)
		require.NoError(t, err)
		_, methods := client.callSnapshot()
		assert.Equal(t, map[string]int{"DatasetGet": 1, "ISCSIExtentFindByDisk": 1}, methods,
			"an UNSTAMPED volume-clone source costs exactly one stamp read plus one live-extent read")

		d, client = newSource(t, map[string]string{PropBlockISCSIBlocksize: "4096", PropBlockISCSIPblocksize: "true"}, true)
		_, err = d.resolveCloneSourceBlockGeometry(context.Background(), "pool/parent/src", nil, nil, "vol", "pool/parent/dst", ShareTypeISCSI)
		require.NoError(t, err)
		_, methods = client.callSnapshot()
		assert.Equal(t, map[string]int{"DatasetGet": 1, "ISCSIExtentFindByDisk": 1}, methods,
			"a STAMPED source costs the same — the live read is what catches a source whose stamp has drifted, so it is not optional")

		// The volume-clone path already holds the source dataset, so it pays only
		// for the live extent.
		d, client = newSource(t, nil, true)
		sourceDS, err := client.MockClient.DatasetGet(context.Background(), "pool/parent/src")
		require.NoError(t, err)
		client.resetCalls()
		_, err = d.resolveCloneSourceBlockGeometry(context.Background(), "pool/parent/src", sourceDS, nil, "vol", "pool/parent/dst", ShareTypeISCSI)
		require.NoError(t, err)
		_, methods = client.callSnapshot()
		assert.Equal(t, map[string]int{"ISCSIExtentFindByDisk": 1}, methods,
			"a caller that already read the source must not be charged for reading it again")

		// Round 5: a SNAPSHOT restore is a different question, and a snapshot that
		// captured its own geometry answers it with NO read of the source at all —
		// cheaper than round 4, which paid two calls to ask the wrong thing.
		d, client = newSource(t, map[string]string{PropBlockISCSIBlocksize: "4096", PropBlockISCSIPblocksize: "true"}, true)
		capturedSnap := &truenas.Snapshot{
			ID: "pool/parent/src@point", Name: "point", Dataset: "pool/parent/src",
			UserProperties: map[string]truenas.UserProperty{
				PropBlockISCSIBlocksize:  {Value: "4096"},
				PropBlockISCSIPblocksize: {Value: "true"},
			},
		}
		resolved, err := d.resolveCloneSourceBlockGeometry(context.Background(), "pool/parent/src", nil, capturedSnap, "point", "pool/parent/dst", ShareTypeISCSI)
		require.NoError(t, err)
		require.Equal(t, geometryKnown, resolved.knowledge)
		require.Equal(t, 4096, *resolved.blocksize)
		_, methods = client.callSnapshot()
		assert.Empty(t, methods, "a snapshot that carries its own geometry needs no source read at all")

		// A snapshot that captured nothing costs exactly one DatasetGet — the
		// history check — and then fails closed. It never reads the source's live
		// extent, because the source's live extent cannot answer for the snapshot.
		d, client = newSource(t, map[string]string{PropISCSIExtentID: "7"}, true)
		bareSnap := &truenas.Snapshot{ID: "pool/parent/src@bare", Name: "bare", Dataset: "pool/parent/src"}
		_, err = d.resolveCloneSourceBlockGeometry(context.Background(), "pool/parent/src", nil, bareSnap, "bare", "pool/parent/dst", ShareTypeISCSI)
		require.Error(t, err)
		_, methods = client.callSnapshot()
		assert.Equal(t, map[string]int{"DatasetGet": 1}, methods,
			"an uncaptured snapshot costs one history read and no live-extent read")

		// NFS pays nothing at all: the short-circuit precedes every API call, so a
		// filesystem deployment does not fund a block-only guard.
		d, client = newSource(t, nil, true)
		_, err = d.resolveCloneSourceBlockGeometry(context.Background(), "pool/parent/src", nil, nil, "vol", "pool/parent/dst", ShareTypeNFS)
		require.NoError(t, err)
		_, methods = client.callSnapshot()
		assert.Empty(t, methods, "an NFS clone must issue no call for the block geometry resolution")
	})

	// (2) The end-to-end totals, pinned so a future change has to argue with a
	// number. Every block restore costs the SAME whatever the class says — the
	// uniformity is the point: no shape of request can buy its way past the
	// resolution by declining to have an opinion.
	t.Run("end-to-end restore totals", func(t *testing.T) {
		measure := func(t *testing.T, name, protocol string, tuning map[string]string, stamp map[string]string) (int, map[string]int) {
			t.Helper()
			client := newAPICallCountingClient()
			d := newAPICallCountDriver(t, client, protocol)
			ctx := context.Background()
			sourceType := "VOLUME"
			if protocol == "nfs" {
				sourceType = "FILESYSTEM"
			}
			_, err := client.MockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
				Name: "pool/parent", Type: "FILESYSTEM",
			})
			require.NoError(t, err)
			_, err = client.MockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
				Name: "pool/parent/clone-source", Type: sourceType, Volsize: testGiB,
			})
			require.NoError(t, err)
			stampDriverOwnership(t, client.MockClient, d, "pool/parent/clone-source")
			if len(stamp) > 0 {
				require.NoError(t, client.MockClient.DatasetSetUserProperties(ctx, "pool/parent/clone-source", stamp))
			}
			_, err = client.MockClient.SnapshotCreate(ctx, "pool/parent/clone-source", "clone-point", nil)
			require.NoError(t, err)

			client.resetCalls()
			_, err = d.CreateVolume(ctx, restoreFromSnapshot(blockTuningRequest(name, protocol, tuning), "clone-point"))
			require.NoError(t, err)
			return client.callSnapshot()
		}

		// Round 5 MOVED these numbers, downward, and the movement is the fix: a
		// snapshot restore no longer interrogates the source's CURRENT state,
		// because the source's current state cannot answer for the snapshot's
		// bytes. It asks the snapshot.
		//
		// 22 calls for a restore whose source has no block history at all: one
		// DatasetGet establishes that (no live-extent read follows, because there
		// is nothing to preserve). Round 4 spent 23 here, the extra one being an
		// ISCSIExtentFindByDisk asking the wrong question.
		base, baseMethods := measure(t, "restore-default", "iscsi", nil, nil)
		assert.Equal(t, 22, base, "the block snapshot-restore total, geometry resolution included")
		assert.Equal(t, 4, baseMethods["DatasetGet"])
		assert.Equal(t, 1, baseMethods["ISCSIExtentFindByDisk"],
			"the one remaining find-by-disk belongs to the DESTINATION's share build, not to the source probe")

		// A snapshot that carries its own COMPLETE geometry record is resolved with
		// no source read whatsoever — one call cheaper still.
		stamped, stampedMethods := measure(t, "restore-stamped", "iscsi",
			map[string]string{paramISCSIBlocksize: "512"},
			map[string]string{PropBlockISCSIBlocksize: "512", PropBlockISCSIPblocksize: "true"})
		assert.Equal(t, base-1, stamped,
			"a snapshot that captured its own geometry needs no source read at all")
		assert.Equal(t, 3, stampedMethods["DatasetGet"])

		unstamped, _ := measure(t, "restore-unstamped", "iscsi", map[string]string{paramISCSIBlocksize: "512"}, nil)
		assert.Equal(t, base, unstamped,
			"a source with no block history costs the one history read, whatever the class says")

		nfs, nfsMethods := measure(t, "restore-nfs", "nfs", nil, nil)
		assert.Equal(t, 10, nfs, "the NFS clone golden is untouched by any of this")
		assert.Zero(t, nfsMethods["ISCSIExtentFindByDisk"])
	})
}

// provisionUnstamped4096Source creates a 4096-geometry iSCSI volume the way the
// ENTIRE pre-GF4 fleet exists: the controller-wide default supplied the
// geometry, the StorageClass set no block parameters, and NOTHING on the dataset
// records what the data was written against. It returns the snapshot's CSI ID.
//
// controllerDefaultAtRestore is the value the controller-wide default is left at
// when the fixture returns, and it is the whole reason this helper takes a
// parameter.
//
// THE TAUTOLOGY THIS REPLACES: the round-3 version set
// d.config.ISCSI.ExtentBlocksize = 4096 and never restored it, on a driver
// shared with the caller. Every downstream assertion that "the restore came out
// at 4096, not the 512 controller default" was therefore reading the controller
// default straight back — at restore time the default WAS 4096. The assertion
// could not fail, its failure message was factually wrong, and it is why the
// controller-default form of this corruption (N-1e) survived three rounds of
// review inside a test that claimed to cover it. Callers now say what the
// default is at restore time; the ones that assert inheritance say 512, so 4096
// can only have come from the source.
//
// Post-round-4 the driver stamps every extent it creates or sees
// (observedGeometryProps), so a volume it provisions is no longer unstamped by
// construction. The stamp is therefore stripped here — that is what "pre-GF4
// volume" now means, and it is stripped BEFORE the snapshot so the snapshot and
// any clone of it inherit nothing either.
// ROUND 5 SPLIT. The fixture now comes in two flavors, because the round-5
// provenance rule makes them behave differently ON PURPOSE:
//
//   - provisionUnstamped4096Source takes the snapshot with the RAW backend call
//     and no properties, which is what a snapshot taken before this mechanism
//     existed looks like: it captured no geometry. Restoring it FAILS CLOSED,
//     because the source's current extent describes the source now, not the
//     bytes inside the snapshot.
//   - provisionUnstamped4096SourceCaptured takes it through the DRIVER's
//     CreateSnapshot, which captures the source's live geometry onto the
//     snapshot. That is the round-5 mechanism that keeps the pre-GF4 fleet
//     restorable, and every "an unstamped source still restores at 4096" test
//     uses it.
//
// Both leave the source DATASET unstamped: "pre-GF4 volume" still means the
// dataset records nothing.
func provisionUnstamped4096Volume(t *testing.T, d *Driver, client *truenas.MockClient, volumeName string) {
	t.Helper()
	ctx := context.Background()
	// Save/restore the driver config this fixture mutates. Each caller builds its
	// own driver today, so nothing leaks — but a fixture that silently leaves a
	// mutated controller default behind is precisely how the round-3 tautology
	// happened, and hygiene here is cheaper than re-deriving that argument.
	original := d.config.ISCSI.ExtentBlocksize
	t.Cleanup(func() { d.config.ISCSI.ExtentBlocksize = original })

	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	// No block parameters: a legacy volume, provisioned before the knobs existed,
	// on an install whose controller-wide default was 4096.
	d.config.ISCSI.ExtentBlocksize = 4096
	_, err = d.CreateVolume(ctx, blockTuningRequest(volumeName, "iscsi", nil))
	require.NoError(t, err)

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/"+volumeName)
	require.NoError(t, err)
	require.NotNil(t, extent)
	// NOT a proof that the fixture works — the controller default IS 4096 at this
	// instant, so this line can only echo it. It is a guard against a future
	// change silently making CreateVolume ignore the default; the discrimination
	// proof is TestUnstampedSourceFixtureActuallyDiscriminates, which varies the
	// default at RESTORE time while the source stays 4096.
	require.Equal(t, 4096, extent.Blocksize, "sanity: CreateVolume must honor the controller default on a fresh zvol")

	// Strip the geometry record: a pre-GF4 volume has one nowhere.
	require.NoError(t, client.DatasetRemoveUserProperties(ctx, "pool/parent/"+volumeName,
		[]string{PropBlockISCSIBlocksize, PropBlockISCSIPblocksize}))
	ds, err := client.DatasetGet(ctx, "pool/parent/"+volumeName)
	require.NoError(t, err)
	require.Nil(t, blockOptsFromDataset(ds),
		"the source must be UNSTAMPED — that is the whole point of this fixture")
}

func provisionUnstamped4096Source(
	t *testing.T,
	d *Driver,
	client *truenas.MockClient,
	volumeName, snapshotName string,
	controllerDefaultAtRestore int,
) string {
	t.Helper()
	ctx := context.Background()
	provisionUnstamped4096Volume(t, d, client, volumeName)

	// The RAW backend call with no properties: a snapshot from before geometry
	// capture existed. It captures nothing, and the dataset is unstamped, so the
	// mock's ZFS-capture modeling gives it nothing either.
	_, err := client.SnapshotCreate(ctx, "pool/parent/"+volumeName, snapshotName, map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           snapshotName,
		PropCSISnapshotSourceVolumeID: volumeName,
	})
	require.NoError(t, err)

	// The controller-wide default at RESTORE time is the caller's business, and
	// stating it is what makes the caller's assertions discriminate.
	d.config.ISCSI.ExtentBlocksize = controllerDefaultAtRestore
	return snapshotName
}

func provisionUnstamped4096SourceCaptured(
	t *testing.T,
	d *Driver,
	client *truenas.MockClient,
	volumeName, snapshotName string,
	controllerDefaultAtRestore int,
) string {
	t.Helper()
	ctx := context.Background()
	provisionUnstamped4096Volume(t, d, client, volumeName)

	// ROUND 6 TAUTOLOGY REPAIR. The controller default is moved to the RESTORE
	// value BEFORE the capture, not after it. Round 5 left it at 4096 — the value
	// the source extent was created from — so an implementation that stamped the
	// controller default instead of reading the live extent produced 4096 and the
	// capture assertion below could not tell the two apart.
	d.config.ISCSI.ExtentBlocksize = controllerDefaultAtRestore

	// Through the DRIVER: CreateSnapshot reads the unstamped zvol's LIVE extent
	// and captures its geometry onto the snapshot, which is what "now IS the
	// snapshot's content" buys.
	_, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: volumeName,
		Name:           snapshotName,
	})
	require.NoError(t, err)
	snap, err := client.SnapshotFindByName(ctx, "pool/parent", snapshotName)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, "4096", snap.UserProperties[PropBlockISCSIBlocksize].Value,
		"the driver must capture the source's LIVE geometry onto the snapshot it takes, not the controller default (%d)",
		controllerDefaultAtRestore)
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
	snapshotID := provisionUnstamped4096SourceCaptured(t, d, client, "pvc-legacy-4k", "legacy-point", 512)

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
	snapshotID := provisionUnstamped4096SourceCaptured(t, d, client, "pvc-legacy-detach-src", "legacy-detach-point", 512)

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
	provisionUnstamped4096Source(t, d, client, "pvc-legacy-clone-src", "unused-point", 512)

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
	snapshotID := provisionUnstamped4096SourceCaptured(t, d, client, "pvc-legacy-inherit-src", "legacy-inherit-point", 512)

	_, err := d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-legacy-inherit", "iscsi", nil), snapshotID))
	require.NoError(t, err, "a no-opts restore of an unstamped source has no geometry opinion and must still succeed")

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-legacy-inherit")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 4096, extent.Blocksize,
		"a no-opts restore must come out at the SOURCE's 4096 geometry; 512 here is the controller default (which the fixture "+
			"deliberately leaves at 512 at restore time) written over 4096-geometry data")
}

// TestUnstampedSourceRestoreIntoMatchingClassSucceeds proves the fallback fires
// on a genuine conflict only: the same unstamped 4096 source restored into a
// class that agrees is accepted.
func TestUnstampedSourceRestoreIntoMatchingClassSucceeds(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	snapshotID := provisionUnstamped4096SourceCaptured(t, d, client, "pvc-legacy-match-src", "legacy-match-point", 512)

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
// source that carries neither a stamp NOR a live extent yields no geometry to
// contradict, and the restore must not be rejected on a guess.
//
// WHAT IT DOES NOT PROVE (round 5, stated because the previous comment implied
// otherwise): the source here is an explicitly EMPTY bare zvol with no CSI
// bookkeeping at all, so it does not establish that "no stamp and no extent"
// generally means data-free. It does not, and assuming it did was round-5 HIGH
// (c). The general rule is the WITNESS SET, and the two tests that actually
// exercise it are TestDetachedCopySentinelCountsAsBlockHistory (a dataset whose
// extent-ID is the "-" sentinel holds somebody else's bytes and is refused) and
// TestBareZvolWithNoWitnessIsStillDataFree (no witness at all, so the default is
// honest). This one is the narrow no-regression pin for the empty case.
func TestUnstampedSourceWithNoExtentIsNotAConflict(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	newOwnedBareZvol(t, client, d, "pool/parent/bare-source")
	_, err = client.SnapshotCreate(ctx, "pool/parent/bare-source", "bare-point", nil)
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-bare-restore", "iscsi", map[string]string{paramISCSIBlocksize: "512"}),
		"bare-point",
	))
	require.NoError(t, err, "no stamp and no live extent means no recorded geometry — nothing to contradict")

	// This is the ONE case where the controller default is not a guess: nothing
	// has ever exported this data, so there is no logical block size its bytes
	// were laid out against. The request's own 512 is honored and recorded.
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-bare-restore")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 512, extent.Blocksize)
	ds, err := client.DatasetGet(ctx, "pool/parent/pvc-bare-restore")
	require.NoError(t, err)
	assert.Equal(t, "512", ds.UserProperties[PropBlockISCSIBlocksize].Value,
		"even here the geometry the volume got is recorded, so no later rebuild has to re-derive it")
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

// ---------------------------------------------------------------------------
// Round 4 — N-1e: the controller-wide default IS a geometry opinion
//
// Four rounds each closed the trigger in front of them (request-vs-nothing,
// request-vs-clone, request-vs-stamp, request-vs-live) while the invariant
// underneath stayed broken: an unstamped volume's real geometry was recorded
// nowhere the driver consults, so `iscsi.extentBlocksize` — a helm value, not a
// StorageClass parameter, and therefore invisible to every guard — silently
// supplied it. The tests below drive the two reachable shapes of that fifth
// form, plus the mechanisms that close the invariant rather than the trigger.
//
// EVERY test in this section fails on 327a878.
// ---------------------------------------------------------------------------

// TestNoOptsRestoreOfUnstampedSourceAfterControllerDefaultChange is N-1e shape
// (a): a restore that names NO geometry, from an unstamped 4096 source, on an
// install whose controller-wide default has since moved to 512.
//
// On 327a878 guardCloneSourceBlockGeometry short-circuited on
// hasGeometryOpinion() before any probe, so the destination got a 512-byte
// extent over 4096-geometry data and CreateVolume returned SUCCESS. It is the
// round-3 finding with the trigger moved from a StorageClass parameter to a helm
// value — which is why closing triggers never ended.
func TestNoOptsRestoreOfUnstampedSourceAfterControllerDefaultChange(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	snapshotID := provisionUnstamped4096SourceCaptured(t, d, client, "pvc-n1e-a-src", "n1e-a-point", 512)
	require.Equal(t, 512, d.config.ISCSI.ExtentBlocksize,
		"the controller default must have MOVED for this test to mean anything")

	_, err := d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-n1e-a", "iscsi", nil), snapshotID))
	require.NoError(t, err, "a no-opts restore has nothing to reject and must still succeed")

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-n1e-a")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 4096, extent.Blocksize,
		"the clone shares its source's bytes: 512 here is the CURRENT controller default laid over 4096-geometry data")

	// And the destination now RECORDS that geometry, so the next rebuild of this
	// volume never has to consult the controller default either.
	ds, err := client.DatasetGet(ctx, "pool/parent/pvc-n1e-a")
	require.NoError(t, err)
	assert.Equal(t, "4096", ds.UserProperties[PropBlockISCSIBlocksize].Value,
		"a clone must record the geometry its data is actually addressed through")
}

// TestPlainRebuildOfUnstampedVolumeAfterControllerDefaultChange is N-1e shape
// (b), the realistic half: no clone, no snapshot, no StorageClass parameter
// anywhere. A legacy 4096 volume, the helm default later moved to 512, and the
// extent re-created (DR restore, orphan reconcile, share teardown).
//
// On 327a878 the extent came back at 512 over 4096 data, silently — a strictly
// larger blast radius than round 3's, because it needs no content source at all.
// It is closed by mechanism (1): the driver back-stamps the geometry of the live
// extent the first time it sees this volume, so by the time the extent is lost
// the volume answers for itself.
func TestPlainRebuildOfUnstampedVolumeAfterControllerDefaultChange(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	datasetName := "pool/parent/pvc-n1e-b"

	// A pre-GF4 volume: created when the install default was 4096, with nothing
	// recording that fact.
	provisionUnstamped4096Source(t, d, client, "pvc-n1e-b", "n1e-b-point", 512)
	ds, err := client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	require.Nil(t, blockOptsFromDataset(ds), "the volume must start UNSTAMPED")

	// The driver sees it alive exactly once — a publish, a startup reconcile, an
	// idempotent replay. That is where it learns the geometry.
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-n1e-b", nil))
	ds, err = client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	assert.Equal(t, "4096", ds.UserProperties[PropBlockISCSIBlocksize].Value,
		"seeing a live extent for an unrecorded volume must back-stamp its geometry")

	// Now the extent is lost and the helm default has moved.
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NoError(t, client.ISCSIExtentDelete(ctx, extent.ID, false, true))
	require.Equal(t, 512, d.config.ISCSI.ExtentBlocksize)

	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-n1e-b", nil),
		"a rebuild of a volume whose geometry IS recorded must succeed")
	rebuilt, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)
	assert.Equal(t, 4096, rebuilt.Blocksize,
		"the rebuild must replay the volume's own geometry; 512 here is the changed helm default written over 4096 data")
}

// TestRebuildWithNoDiscoverableGeometryFailsClosed is the other half of
// mechanism (2), and the one case where the driver refuses rather than answers:
// the volume exists, its extent is gone (so there is no live geometry to read),
// and nothing records what its data was written against. Every earlier round
// silently wrote the current helm default here.
//
// Refusing is the only honest option — the driver cannot know, and a guess is
// the corruption. The error names the property an operator can set to recover.
func TestRebuildWithNoDiscoverableGeometryFailsClosed(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	datasetName := "pool/parent/pvc-n1e-unknown"
	provisionUnstamped4096Source(t, d, client, "pvc-n1e-unknown", "unknown-point", 512)

	// The extent disappears BEFORE the driver ever observes it — the only window
	// mechanism (1) cannot cover, because it never got to look.
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NoError(t, client.ISCSIExtentDelete(ctx, extent.ID, false, true))

	err = iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-n1e-unknown", nil)
	require.Error(t, err, "an unrecorded geometry must not be guessed from the controller default")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), PropBlockISCSIBlocksize,
		"the refusal must name the property that recovers it")
	assert.Contains(t, status.Convert(err).Message(), PropBlockISCSIPblocksize,
		"and the PHYSICAL one, because a half-resolved record is exactly how pblocksize came from the mutable default")

	// Nothing was created at the guessed geometry.
	rebuilt, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, findErr)
	assert.Nil(t, rebuilt, "the refused rebuild must not have created a 512-byte extent over 4096 data")

	// ROUND 5 / MEDIUM-2. Recording only the LOGICAL half is NOT enough: the
	// record has to be COMPLETE, or physical silently comes from
	// !iscsi.extentDisablePhysicalBlocksize — a second mutable install-wide value
	// reaching existing data by a different door. Round 4 accepted this and
	// created the extent with a defaulted pblocksize.
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName,
		map[string]string{PropBlockISCSIBlocksize: "4096"}))
	err = iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-n1e-unknown", nil)
	require.Error(t, err, "logical alone is a HALF record; physical must not be filled in from the controller default")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), PropBlockISCSIPblocksize)
	rebuilt, findErr = client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, findErr)
	assert.Nil(t, rebuilt, "the half-recorded rebuild must not have created an extent either")

	// Recording the COMPLETE geometry is all it takes to recover.
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName,
		map[string]string{PropBlockISCSIBlocksize: "4096", PropBlockISCSIPblocksize: "true"}))
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-n1e-unknown", nil))
	recovered, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NotNil(t, recovered)
	assert.Equal(t, 4096, recovered.Blocksize)
	require.NotNil(t, recovered.Pblocksize)
	assert.True(t, *recovered.Pblocksize)
}

// TestBackStampingAVolumeCostsNoExtraRoundTrip pins mechanism (1)'s price. The
// back-stamp is folded into the resource-ID dataset update the share builder
// already performs, so learning a legacy volume's geometry is free — which is
// what makes it acceptable to do it on the publish hot path.
// THE TAUTOLOGY THIS REPLACES (round 5): the previous version asserted the
// back-stamped value was "512" while the fixture's controller default WAS 512,
// so an implementation that stamped the default instead of the live extent's
// geometry would have passed identically. The volume is now created at 4096 and
// the controller default is left at 512 for the publish, so 4096 in the stamp
// can only have come from the live extent.
func TestBackStampingAVolumeCostsNoExtraRoundTrip(t *testing.T) {
	measure := func(t *testing.T, name string, stripStamp bool) (int, map[string]int, *truenas.Dataset) {
		t.Helper()
		client := newAPICallCountingClient()
		d := newAPICallCountDriver(t, client, "iscsi")
		ctx := context.Background()
		_, err := client.MockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, err)
		req := apiCallCountVolumeRequest(name, "iscsi")
		req.Parameters[paramISCSIBlocksize] = "4096"
		req.Parameters[paramISCSIPblocksize] = "true"
		_, err = d.CreateVolume(ctx, req)
		require.NoError(t, err)
		// Strip or keep the geometry record to make the two runs differ ONLY in
		// whether the publish has something to learn.
		if stripStamp {
			require.NoError(t, client.MockClient.DatasetRemoveUserProperties(ctx, "pool/parent/"+name,
				[]string{PropBlockISCSIBlocksize, PropBlockISCSIPblocksize}))
		}
		// The controller default is something the live extent is NOT. Anything
		// that stamps 512 stamped the default.
		require.NotEqual(t, 4096, d.config.ISCSI.ExtentBlocksize,
			"the controller default must differ from the volume's real geometry, or the assertion below proves nothing")
		client.resetCalls()
		require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, "pool/parent/"+name, name, nil))
		total, methods := client.callSnapshot()
		ds, err := client.MockClient.DatasetGet(ctx, "pool/parent/"+name)
		require.NoError(t, err)
		return total, methods, ds
	}

	stampedTotal, stampedMethods, _ := measure(t, "already-stamped", false)
	legacyTotal, legacyMethods, legacyDS := measure(t, "legacy-unstamped", true)

	// It really did learn something — otherwise "it cost nothing" is trivially
	// true and this test proves only that nothing happened.
	require.NotNil(t, legacyDS)
	assert.Equal(t, "4096", legacyDS.UserProperties[PropBlockISCSIBlocksize].Value,
		"the publish must back-stamp the LIVE extent's geometry; 512 here is the controller default being stamped instead")
	assert.Equal(t, "true", legacyDS.UserProperties[PropBlockISCSIPblocksize].Value,
		"and the physical half with it — a half-stamped volume is refused on its next rebuild")
	assert.Equal(t, stampedTotal, legacyTotal,
		"and back-stamping it must cost exactly what re-ensuring an already-recorded volume costs")
	assert.Equal(t, stampedMethods["DatasetSetUserProperties"], legacyMethods["DatasetSetUserProperties"],
		"the geometry must ride in the resource-ID update, not in a write of its own")
}

// TestStampVsLiveGeometryDisagreementIsRefused is mechanism (4) on a volume, and
// the first half of the drift LOW: the stamp used to beat the live extent, so a
// volume whose extent had been re-created at another geometry — out of band, or
// by an earlier defaulted rebuild — kept certifying its own wrong geometry to
// every downstream guard.
//
// The two fields were NOT equally covered before, which is the point of running
// both subtests:
//
//   - blocksize drift was already refused on 327a878, incidentally: the
//     immutability guard compares the live extent against the volume's stamp, so
//     it happened to catch this. That subtest is a no-regression pin, not a
//     revert-proof, and it is labeled as such.
//   - pblocksize drift was refused by NOTHING. guardExistingISCSIExtentOpts
//     compares the REQUEST against the live extent, and a publish carries no
//     request, so on 327a878 the disagreement passed silently. That subtest
//     fails pre-fix.
func TestStampVsLiveGeometryDisagreementIsRefused(t *testing.T) {
	// Pre-existing coverage, pinned so the explicit rule does not weaken it.
	t.Run("blocksize", func(t *testing.T) {
		d, client := newBlockImmutabilityDriver(t)
		ctx := context.Background()
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, err)
		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-drift", "iscsi", map[string]string{paramISCSIBlocksize: "4096"}))
		require.NoError(t, err)

		// Out of band: the extent is now 512 while the volume records 4096.
		extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-drift")
		require.NoError(t, err)
		require.NotNil(t, extent)
		extent.Blocksize = 512

		err = iscsiShareBackend{d}.EnsureShare(ctx, nil, "pool/parent/pvc-drift", "pvc-drift", nil)
		require.Error(t, err, "a volume whose record and extent disagree must not be quietly published either way")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		message := status.Convert(err).Message()
		assert.Contains(t, message, "4096", "the refusal must name the recorded value")
		assert.Contains(t, message, "512", "and the live one, so the operator can decide which is true")
	})

	// The half nothing caught: a no-opts publish carries no request, so the
	// request-vs-live comparison has nothing to compare and the stamp was never
	// consulted. FAILS on 327a878.
	t.Run("pblocksize", func(t *testing.T) {
		d, client := newBlockImmutabilityDriver(t)
		ctx := context.Background()
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, err)
		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-drift-pb", "iscsi", map[string]string{paramISCSIPblocksize: "true"}))
		require.NoError(t, err)

		extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-drift-pb")
		require.NoError(t, err)
		require.NotNil(t, extent)
		extent.Pblocksize = boolPtr(false)

		err = iscsiShareBackend{d}.EnsureShare(ctx, nil, "pool/parent/pvc-drift-pb", "pvc-drift-pb", nil)
		require.Error(t, err,
			"a no-opts publish carries no request to compare, so only a stamp-vs-live rule can see this drift")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, status.Convert(err).Message(), PropBlockISCSIPblocksize)
	})
}

// TestDriftedCloneSourceIsRefused is mechanism (4) on a clone source. A source
// whose stamp and live extent disagree has no establishable geometry, so the
// clone is refused rather than resolved by whichever field the code happens to
// read first. On 327a878 the stamp won silently and the destination inherited
// the wrong one.
//
// ROUND 5 RETARGETED THIS TO THE PVC-TO-PVC PATH, deliberately. A volume clone
// snapshots the source's CURRENT state, so the source's current drift is the
// clone's problem and refusing is right. A SNAPSHOT restore is the opposite: the
// snapshot's own captured stamp describes the snapshot's bytes, and the source's
// later fate — drift, or being destroyed altogether — says nothing about them.
// Refusing a DR restore because the source is now broken would deny exactly the
// operation the operator needs. The second subtest pins that rule so it cannot
// be "tidied" back into a refusal.
func TestDriftedCloneSourceIsRefused(t *testing.T) {
	t.Run("volume clone", func(t *testing.T) {
		d, client := newBlockImmutabilityDriver(t)
		ctx := context.Background()
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, err)
		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-drift-src", "iscsi", map[string]string{paramISCSIBlocksize: "512"}))
		require.NoError(t, err)
		// The source records 512; its extent actually reports 4096.
		extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-drift-src")
		require.NoError(t, err)
		extent.Blocksize = 4096

		req := blockTuningRequest("pvc-drift-dst", "iscsi", map[string]string{paramISCSIBlocksize: "512"})
		req.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "pvc-drift-src"},
		}}
		_, err = d.CreateVolume(ctx, req)
		require.Error(t, err, "a source whose geometry cannot be established must not be cloned on a guess")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		message := status.Convert(err).Message()
		assert.Contains(t, message, "512", "the refusal must name the recorded value")
		assert.Contains(t, message, "4096", "and the live one")
		_, getErr := client.DatasetGet(ctx, "pool/parent/pvc-drift-dst")
		assert.True(t, truenas.IsNotFoundError(getErr), "the refusal must precede the first destination mutation")
	})

	t.Run("snapshot restore uses the snapshot's own capture, not the source's later drift", func(t *testing.T) {
		d, client := newBlockImmutabilityDriver(t)
		ctx := context.Background()
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, err)
		_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-snapdrift-src", "iscsi", map[string]string{paramISCSIBlocksize: "512"}))
		require.NoError(t, err)
		// Snapshot FIRST — it captures the 512 the data really was written through.
		_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{SourceVolumeId: "pvc-snapdrift-src", Name: "snapdrift-point"})
		require.NoError(t, err)
		// THEN the source's extent is re-created at 4096 behind the driver's back.
		extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-snapdrift-src")
		require.NoError(t, err)
		extent.Blocksize = 4096

		// This is the round-4 corruption sequence, and it is the whole reason
		// provenance moved onto the snapshot: round 4 read the source's CURRENT
		// 4096 extent and would have created the destination at 4096 over
		// 512-layout bytes.
		_, err = d.CreateVolume(ctx, restoreFromSnapshot(
			blockTuningRequest("pvc-snapdrift-dst", "iscsi", nil), "snapdrift-point"))
		require.NoError(t, err, "the snapshot's own record answers for the snapshot's bytes")
		restored, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-snapdrift-dst")
		require.NoError(t, err)
		require.NotNil(t, restored)
		assert.Equal(t, 512, restored.Blocksize,
			"4096 here is the source's POST-snapshot geometry laid over the snapshot's 512-layout data")
	})
}

// TestNoOptsHopCannotLaunderGeometry is the second LOW. On 327a878 a no-opts
// restore created the destination at the controller default, and the NEXT
// restore then read that wrong extent as ground truth and agreed with it: the
// corruption became self-certifying and every downstream guard endorsed it.
//
// With the destination recording its source's real geometry, hop 1 comes out at
// 4096 and hop 2's conflicting request is rejected on the strength of it.
func TestNoOptsHopCannotLaunderGeometry(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	snapshotID := provisionUnstamped4096SourceCaptured(t, d, client, "pvc-launder-a", "launder-point", 512)

	// Hop 1: no opts. Pre-fix this produced a 512 extent over 4096 data.
	_, err := d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-launder-b", "iscsi", nil), snapshotID))
	require.NoError(t, err)
	hop1, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-launder-b")
	require.NoError(t, err)
	require.NotNil(t, hop1)
	require.Equal(t, 4096, hop1.Blocksize, "hop 1 must not launder the controller default onto the source's data")

	// Hop 2: restore hop 1 into an explicit 512 class. Pre-fix this was ACCEPTED,
	// because hop 1's laundered 512 extent was now the recorded truth.
	_, err = client.SnapshotCreate(ctx, "pool/parent/pvc-launder-b", "launder-point-2", nil)
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-launder-c", "iscsi", map[string]string{paramISCSIBlocksize: "512"}),
		"launder-point-2",
	))
	require.Error(t, err, "a laundered geometry must not become the next restore's ground truth")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), paramISCSIBlocksize)
}

// TestUnstampedSourceFixtureActuallyDiscriminates is the anti-tautology proof.
//
// The round-3 helper set the controller default to 4096 and never restored it,
// so "the restore came out at 4096" was the default echoing back and the
// assertion could not fail. The helper now leaves the default where the caller
// says, and this test shows the outcome tracks the SOURCE and not the default by
// varying the default across three values while the source stays 4096.
//
// CORRECTION (round 5): the previous comment here claimed "exactly one of the
// three would pass" on a tree where a no-opts restore inherits the default. That
// was factually WRONG — none of 512/1024/2048 is 4096, so ALL THREE fail on such
// a tree. The property is stronger than the comment claimed, and stating it
// correctly matters: a reader who believes one subtest is expected to pass will
// not notice when three do.
func TestUnstampedSourceFixtureActuallyDiscriminates(t *testing.T) {
	for _, controllerDefault := range []int{512, 1024, 2048} {
		t.Run(fmt.Sprintf("controller default %d", controllerDefault), func(t *testing.T) {
			d, client := newBlockImmutabilityDriver(t)
			ctx := context.Background()
			snapshotID := provisionUnstamped4096SourceCaptured(t, d, client, "pvc-disc-src", "disc-point", controllerDefault)
			require.Equal(t, controllerDefault, d.config.ISCSI.ExtentBlocksize,
				"the fixture must leave the controller default where the caller asked — the round-3 version did not, which is what made its callers tautologies")
			require.NotEqual(t, 4096, d.config.ISCSI.ExtentBlocksize,
				"and it must differ from the source geometry, or the assertion below proves nothing")

			_, err := d.CreateVolume(ctx, restoreFromSnapshot(
				blockTuningRequest("pvc-disc-dst", "iscsi", nil), snapshotID))
			require.NoError(t, err)
			extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-disc-dst")
			require.NoError(t, err)
			require.NotNil(t, extent)
			assert.Equal(t, 4096, extent.Blocksize,
				"the restored geometry must track the SOURCE across every controller default, not echo one of them")
		})
	}
}
