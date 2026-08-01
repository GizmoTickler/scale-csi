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
// guard, so it stays honest about what it charges: a class with no geometry
// opinion short-circuits before any API call (the default clone path's golden
// counts are therefore unchanged), and a class that opts in pays exactly one
// DatasetGet to read the source's stamp.
func TestCloneSourceGeometryProbeAPICallCost(t *testing.T) {
	measure := func(t *testing.T, name string, tuning map[string]string) int {
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
		_, err = client.MockClient.SnapshotCreate(ctx, "pool/parent/clone-source", "clone-point", nil)
		require.NoError(t, err)

		client.resetCalls()
		_, err = d.CreateVolume(ctx, restoreFromSnapshot(blockTuningRequest(name, "iscsi", tuning), "clone-point"))
		require.NoError(t, err)
		total, _ := client.callSnapshot()
		return total
	}

	base := measure(t, "restore-default", nil)
	tuned := measure(t, "restore-tuned", map[string]string{paramISCSIBlocksize: "512"})
	assert.Equal(t, base+1, tuned,
		"the clone-source geometry probe must cost exactly one DatasetGet, and only for a class that opts into a geometry")
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
				require.NotNil(t, subsys.PiEnable)
				assert.False(t, *subsys.PiEnable, "the rejected replay must not have enabled T10-PI")
			}
		})
	}
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
