package driver

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// ---------------------------------------------------------------------------
// GF-4 round 8 — the final adversarial verification of v1.5.0
//
//  1. HIGH-1: a FAILED live-extent read let CreateSnapshot capture the volume's
//     unverified stamp, and a restore then laid it over data addressed the other
//     way. The contract is that a snapshot whose geometry could not be read
//     records NOTHING and its restores fail closed.
//  2. MEDIUM-1: both geometry resolvers short-circuit on the DESTINATION share
//     type, so an iSCSI source restored into an NVMe-oF class was examined not
//     at all — while the reverse direction fails closed.
// ---------------------------------------------------------------------------

// extentProbeInjector makes the live-extent read fail on demand, which is the
// transient TrueNAS API failure (a query timeout under a VolSync burst) HIGH-1
// depends on. Only ISCSIExtentFindByDisk is intercepted; everything else runs
// against the real mock, so the volume is a genuine 4096 volume with a genuine
// stamp and a genuine live extent.
type extentProbeInjector struct {
	*truenas.MockClient
	fail  atomic.Bool
	calls atomic.Int32
}

func (c *extentProbeInjector) ISCSIExtentFindByDisk(ctx context.Context, diskPath string) (*truenas.ISCSIExtent, error) {
	c.calls.Add(1)
	if c.fail.Load() {
		return nil, fmt.Errorf("injected: iscsi.extent.query timed out for %s", diskPath)
	}
	return c.MockClient.ISCSIExtentFindByDisk(ctx, diskPath)
}

// TestSnapshotLiveReadFailureRecordsNoGeometryAndFailsTheRestoreClosed is the
// HIGH-1 regression.
//
// Shape: a 4096 volume whose ISCSIExtentFindByDisk fails exactly once, during
// CreateSnapshot. The snapshot is still taken (capture is best-effort), but it
// must carry NO geometry the driver will act on, and a later restore of it into
// a no-opts iSCSI class must fail closed rather than create a 4096 extent from a
// record nothing verified.
//
// FAILS PRE-FIX: yes, on BOTH pre-fix shapes of the read-error arm, verified
// empirically in this worktree by editing snapshotGeometryProps and re-running:
//
//   - `return stamped.props(), nil` (the shape HIGH-1 reported, bb9f6b2): the
//     snapshot carries 4096/true, the restore SUCCEEDS and the destination
//     extent is created at 4096. Every assertion below fails.
//   - `return nil, nil` (cc258eb's fix): the snapshot still reads 4096/true,
//     because a ZFS snapshot inherits its dataset's user properties and the
//     stamped volume's dataset carries them — capturing an empty map removes
//     nothing. The restore SUCCEEDS at 4096 exactly as before. This is why the
//     arm writes the no-value sentinel instead of writing nothing.
func TestSnapshotLiveReadFailureRecordsNoGeometryAndFailsTheRestoreClosed(t *testing.T) {
	d, mock := newBlockImmutabilityDriver(t)
	client := &extentProbeInjector{MockClient: mock}
	d.truenasClient = client
	ctx := context.Background()
	_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-probe-fail-src", "iscsi", map[string]string{
		paramISCSIBlocksize: "4096", paramISCSIPblocksize: "true",
	}))
	require.NoError(t, err)
	source, err := mock.DatasetGet(ctx, "pool/parent/pvc-probe-fail-src")
	require.NoError(t, err)
	require.Equal(t, "4096", source.UserProperties[PropBlockISCSIBlocksize].Value,
		"the fixture is only meaningful if the volume really carries the stamp the snapshot could inherit")

	client.fail.Store(true)
	_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: "pvc-probe-fail-src", Name: "probe-fail-point",
	})
	require.NoError(t, err, "an unreadable live geometry costs the capture, never the snapshot")
	client.fail.Store(false)

	snap, err := mock.SnapshotFindByName(ctx, "pool/parent", "probe-fail-point")
	require.NoError(t, err)
	require.NotNil(t, snap)
	captured := snapshotGeometry(snap)
	assert.Nil(t, captured.blocksize, "an unverified stamp must not reach the snapshot as a logical block size")
	assert.Nil(t, captured.pblocksize, "an unverified stamp must not reach the snapshot as a physical-blocksize flag")
	assert.NotEqual(t, geometryKnown, captured.knowledge,
		"a snapshot whose geometry could not be read records no geometry of its own")

	// And the restore of that snapshot fails CLOSED — availability, not integrity.
	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-probe-fail-dst", "iscsi", nil), "probe-fail-point"))
	require.Error(t, err, "a snapshot that records no geometry may not be restored onto a guessed one")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "records no "+PropBlockISCSIBlocksize,
		"the refusal must be the snapshot-records-nothing arm, not some other failure")

	destination, findErr := mock.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-probe-fail-dst")
	require.NoError(t, findErr)
	assert.Nil(t, destination,
		"no extent may be created from a stamp that no live read ever confirmed")
}

// TestHealthyLiveReadStillCapturesGeometry is the other side of the same arm: the
// sentinel is written ONLY when the read failed. A healthy probe still captures
// the real geometry, so the fix costs no availability on the normal path.
func TestHealthyLiveReadStillCapturesGeometry(t *testing.T) {
	d, mock := newBlockImmutabilityDriver(t)
	client := &extentProbeInjector{MockClient: mock}
	d.truenasClient = client
	ctx := context.Background()
	_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-probe-ok-src", "iscsi", map[string]string{
		paramISCSIBlocksize: "4096", paramISCSIPblocksize: "true",
	}))
	require.NoError(t, err)
	_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: "pvc-probe-ok-src", Name: "probe-ok-point",
	})
	require.NoError(t, err)

	snap, err := mock.SnapshotFindByName(ctx, "pool/parent", "probe-ok-point")
	require.NoError(t, err)
	require.NotNil(t, snap)
	captured := snapshotGeometry(snap)
	require.NotNil(t, captured.blocksize)
	assert.Equal(t, 4096, *captured.blocksize)
	assert.Equal(t, geometryKnown, captured.knowledge)

	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-probe-ok-dst", "iscsi", nil), "probe-ok-point"))
	require.NoError(t, err)
	destination, findErr := mock.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-probe-ok-dst")
	require.NoError(t, findErr)
	require.NotNil(t, destination)
	assert.Equal(t, 4096, destination.Blocksize, "a verified capture still drives the restore")
}

// ---------------------------------------------------------------------------
// MEDIUM-1 — the cross-protocol restore
// ---------------------------------------------------------------------------

// TestNonDefaultISCSIGeometryRestoredIntoNVMeoFIsRefused is the MEDIUM-1
// regression. Kubernetes places no same-class restriction on restoring a
// VolumeSnapshot, so a 4096-byte iSCSI volume — whose filesystem and partition
// table are laid out for 4096-byte logical blocks — can be pointed at an NVMe-oF
// class. Both resolvers short-circuited on the DESTINATION share type, so that
// restore was examined not at all and the namespace was created over the cloned
// bytes with whatever LBA format the platform derives.
//
// FAILS PRE-FIX: yes. VERIFIED EMPIRICALLY by `git stash`-ing this worktree's
// changes (restoring the bare `if shareType != ShareTypeISCSI { return ... }`
// short-circuit) and re-running: CreateVolume SUCCEEDS and an NVMe-oF namespace
// is created over the 4096-layout bytes, so require.Error fails.
func TestNonDefaultISCSIGeometryRestoredIntoNVMeoFIsRefused(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-xproto-src", "iscsi", map[string]string{
		paramISCSIBlocksize: "4096", paramISCSIPblocksize: "true",
	}))
	require.NoError(t, err)
	_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: "pvc-xproto-src", Name: "xproto-point",
	})
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-xproto-dst", "nvmeof", nil), "xproto-point"))
	require.Error(t, err, "an NVMe-oF namespace makes no geometry claim, so it may not be laid over 4096-layout bytes")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	message := status.Convert(err).Message()
	assert.Contains(t, message, "4096", "the refusal must name the geometry it refused to change")
	assert.Contains(t, message, "iSCSI StorageClass", "the refusal must name the recovery")

	namespace, findErr := client.NVMeoFNamespaceFindByDevicePath(ctx, "zvol/pool/parent/pvc-xproto-dst")
	require.NoError(t, findErr)
	assert.Nil(t, namespace, "the refusal must precede the namespace create")
	_, getErr := client.DatasetGet(ctx, "pool/parent/pvc-xproto-dst")
	assert.True(t, truenas.IsNotFoundError(getErr),
		"the refusal must precede the first destination mutation (err=%v)", getErr)
}

// TestNonDefaultISCSIGeometryClonedIntoNVMeoFIsRefused covers the volume-clone
// arm of the same guard, which answers from the SOURCE DATASET's stamp (already
// in the caller's hand) rather than from a snapshot's capture.
func TestNonDefaultISCSIGeometryClonedIntoNVMeoFIsRefused(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-xproto-clone-src", "iscsi", map[string]string{
		paramISCSIBlocksize: "4096", paramISCSIPblocksize: "true",
	}))
	require.NoError(t, err)

	request := blockTuningRequest("pvc-xproto-clone-dst", "nvmeof", nil)
	request.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
		Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "pvc-xproto-clone-src"},
	}}
	_, err = d.CreateVolume(ctx, request)
	require.Error(t, err, "the clone of a 4096 source into an NVMe-oF class re-addresses the same bytes")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "4096")
	_, getErr := client.DatasetGet(ctx, "pool/parent/pvc-xproto-clone-dst")
	assert.True(t, truenas.IsNotFoundError(getErr),
		"the refusal must precede the first destination mutation (err=%v)", getErr)
}

// TestUnrecordedAndDefaultGeometrySourcesRestoreIntoNVMeoFUnchanged is the
// no-regression half of MEDIUM-1: the guard refuses on a POSITIVE record of a
// non-default logical size and on nothing else. A source that records 512 (what
// an unclaimed zvol is addressed through anyway) and a source that records no
// geometry at all both restore into an NVMe-oF class exactly as before — and
// neither pays an extra API call for the question, since the guard reads only
// the record the caller already holds.
func TestUnrecordedAndDefaultGeometrySourcesRestoreIntoNVMeoFUnchanged(t *testing.T) {
	d, mock := newBlockImmutabilityDriver(t)
	client := &extentProbeInjector{MockClient: mock}
	d.truenasClient = client
	ctx := context.Background()
	_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	// (a) A 512 iSCSI volume: the default geometry, recorded by the back-stamp.
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-xproto-512-src", "iscsi", nil))
	require.NoError(t, err)
	_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: "pvc-xproto-512-src", Name: "xproto-512-point",
	})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-xproto-512-dst", "nvmeof", nil), "xproto-512-point"))
	require.NoError(t, err, "a source addressed at the platform's own block size claims nothing to contradict")
	namespace, findErr := mock.NVMeoFNamespaceFindByDevicePath(ctx, "zvol/pool/parent/pvc-xproto-512-dst")
	require.NoError(t, findErr)
	assert.NotNil(t, namespace, "the restored volume must still get its namespace")

	// (b) A source carrying no geometry record at all: an unstamped zvol and a
	// snapshot of it that captured nothing.
	_, err = mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/unstamped-source", Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	_, err = mock.SnapshotCreate(ctx, "pool/parent/unstamped-source", "unstamped-point", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "unstamped-point",
		PropCSISnapshotSourceVolumeID: "unstamped-source",
	})
	require.NoError(t, err)
	probesBefore := client.calls.Load()
	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-xproto-unstamped-dst", "nvmeof", nil), "unstamped-point"))
	require.NoError(t, err, "an unrecorded source is the pre-existing NVMe-oF behavior and must be unchanged")
	assert.Equal(t, probesBefore, client.calls.Load(),
		"the cross-protocol question must cost no extra backend read")
	namespace, findErr = mock.NVMeoFNamespaceFindByDevicePath(ctx, "zvol/pool/parent/pvc-xproto-unstamped-dst")
	require.NoError(t, findErr)
	assert.NotNil(t, namespace)
	extent, findErr := mock.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-xproto-unstamped-dst")
	require.NoError(t, findErr)
	assert.Nil(t, extent, "an NVMe-oF destination never grows an iSCSI extent")
}
