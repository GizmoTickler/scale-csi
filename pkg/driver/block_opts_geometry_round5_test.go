package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// ---------------------------------------------------------------------------
// GF-4 round 5 — the SIXTH form, and the mechanism rather than the trigger
//
// Round 4 asserted a geometry invariant in a comment while production kept five
// separate geometry semantics. These tests drive the choke point that replaces
// them, and each one names what it would do on 8cec385 (the round-4 HEAD).
// ---------------------------------------------------------------------------

// stripGeometryAndExtent turns a driver-provisioned iSCSI volume into the exact
// shape the reviewer's HIGH findings live in: the dataset still carries the
// driver's bookkeeping (so its bytes may be block-addressed) but records no
// geometry, and the extent is gone (so there is no live geometry to read).
func stripGeometryAndExtent(t *testing.T, client *truenas.MockClient, datasetName string) {
	t.Helper()
	ctx := context.Background()
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NotNil(t, extent)
	require.NoError(t, client.ISCSIExtentDelete(ctx, extent.ID, false, true))
	require.NoError(t, client.DatasetRemoveUserProperties(ctx, datasetName,
		[]string{PropBlockISCSIBlocksize, PropBlockISCSIPblocksize}))
}

// TestExplicitRequestIsIntentNotEvidence is round-5 HIGH (b).
//
// On 8cec385, resolveExtentCreateBlocksize returned the REQUEST's blocksize
// before it ever tested freshlyCreated or datasetRecordsAPriorISCSIExtent
// (block_opts.go:853-855). A replayed CreateVolume carrying iscsi/blocksize=512
// therefore created a 512-byte extent over data of unknown layout and returned
// SUCCESS — the fail-closed added in round 4 was unreachable for every request
// that had an opinion.
//
// FAILS ON 8cec385: yes. Verified by reverting the choke point to the round-4
// resolveExtentCreateBlocksize body (request-first) and re-running: CreateVolume
// returns OK with a 512 extent, so both the error assertion and the "no extent
// was created" assertion fail.
func TestExplicitRequestIsIntentNotEvidence(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	datasetName := "pool/parent/pvc-intent"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-intent", "iscsi", map[string]string{paramISCSIBlocksize: "4096"}))
	require.NoError(t, err)
	stripGeometryAndExtent(t, client, datasetName)

	// A replay with an explicit, DIFFERENT geometry. Kubernetes cannot edit a
	// StorageClass's parameters in place, but a deleted-and-recreated class or a
	// second class colliding on the same volume name delivers exactly this.
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-intent", "iscsi", map[string]string{paramISCSIBlocksize: "512"}))
	require.Error(t, err, "a StorageClass parameter is intent; it may not define the layout of bytes that already exist")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	rebuilt, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, findErr)
	assert.Nil(t, rebuilt, "the refused replay must not have created a 512-byte extent over data of unknown layout")

	// The same replay with NO geometry opinion is refused too — the point is that
	// nothing establishes the layout, not that the request disagreed with
	// something.
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-intent", "iscsi", nil))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestDetachedCopySentinelCountsAsBlockHistory is round-5 HIGH (c).
//
// datasetRecordsAPriorISCSIExtent was datasetUserPropertyHasValue(ds,
// PropISCSIExtentID), which reads BOTH "absent" and the ZFS "-" sentinel as "no
// history". A detached snapshot copy always resets PropISCSIExtentID to "-"
// (provenance.go) precisely BECAUSE the dataset now holds somebody else's bytes,
// so the one shape most certain to hold foreign data was classified as
// data-free and took the controller default.
//
// FAILS ON 8cec385: yes. datasetUserPropertyHasValue("-") is false there, so
// resolveExtentCreateBlocksize returns d.config.ISCSI.ExtentBlocksize and the
// rebuild succeeds with a 512 extent; the error assertion fails.
func TestDetachedCopySentinelCountsAsBlockHistory(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	datasetName := "pool/parent/pvc-detached-sentinel"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	// Exactly what prepareDetachedSnapshotCopy leaves behind: the share IDs of the
	// SOURCE scrubbed to the sentinel, and no geometry record.
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropCSIVolumeName:   "pvc-detached-sentinel",
		PropISCSITargetID:   "-",
		PropISCSIExtentID:   "-",
		PropManagedResource: "true",
	}))

	err = iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-detached-sentinel", nil)
	require.Error(t, err,
		"a dataset whose extent-ID was reset to the ZFS sentinel is a COPY of somebody else's bytes, not a blank zvol")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), PropISCSIExtentID,
		"the refusal must name the witness it relied on")

	created, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, findErr)
	assert.Nil(t, created, "no extent may be created at the controller default over copied data")
}

// TestBareZvolWithNoWitnessIsStillDataFree is the other side of the same rule,
// and the reason the witness set has to be a set rather than a mood: a zvol with
// NO CSI bookkeeping at all has never been block-addressed by anything the
// driver can see, so refusing there would wedge every legitimate fresh build.
//
// FAILS ON 8cec385: no. This is a no-regression pin for the broadened witness
// set, and it is labeled as such rather than presented as a proof.
func TestBareZvolWithNoWitnessIsStillDataFree(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	datasetName := "pool/parent/pvc-bare-zvol"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)

	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-bare-zvol", nil))
	created, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.Equal(t, 512, created.Blocksize, "with no history to contradict, the controller default is the honest answer")
}

// TestUncapturedSnapshotRestoreFailsClosed is round-5 HIGH 1: a snapshot's data
// is not the source's current data.
//
// Sequence: a pre-GF4 source writes through 4096, a snapshot is taken, the
// source's extent is later re-created at 512. On 8cec385 the restore read the
// source's CURRENT extent, resolved 512, and laid it over the snapshot's
// 4096-layout bytes with SUCCESS.
//
// FAILS ON 8cec385: yes. There resolveCloneSourceBlockGeometry took snap.Dataset
// and the live extent, resolved 512, and CreateVolume returned OK — so both the
// error assertion and the destination-absent assertion fail.
func TestUncapturedSnapshotRestoreFailsClosed(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	// A pre-GF4 snapshot: taken with the raw backend call, capturing nothing.
	snapshotID := provisionUnstamped4096Source(t, d, client, "pvc-hist-src", "hist-point", 512)

	// The source's extent is re-created at 512 AFTER the snapshot. The old
	// snapshot still holds 4096-layout bytes.
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-hist-src")
	require.NoError(t, err)
	extent.Blocksize = 512

	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-hist-dst", "iscsi", nil), snapshotID))
	require.Error(t, err,
		"the source's CURRENT extent cannot establish the layout of bytes captured before it existed")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	message := status.Convert(err).Message()
	assert.Contains(t, message, PropBlockISCSIBlocksize, "the refusal must name the property that recovers it")
	assert.Contains(t, message, "pool/parent/pvc-hist-src@hist-point",
		"and the SNAPSHOT to record it on, since that is where the provenance belongs")

	_, getErr := client.DatasetGet(ctx, "pool/parent/pvc-hist-dst")
	assert.True(t, truenas.IsNotFoundError(getErr),
		"the refusal must precede the first destination mutation (err=%v)", getErr)

	// Recovery is a single property on the SNAPSHOT — and then the restore comes
	// out at the snapshot's real geometry, not the source's current one.
	require.NoError(t, client.SnapshotSetUserProperty(ctx, "pool/parent/pvc-hist-src@hist-point", PropBlockISCSIBlocksize, "4096"))
	require.NoError(t, client.SnapshotSetUserProperty(ctx, "pool/parent/pvc-hist-src@hist-point", PropBlockISCSIPblocksize, "true"))
	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-hist-dst", "iscsi", nil), snapshotID))
	require.NoError(t, err, "recording the snapshot's real geometry must be all it takes")
	restored, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-hist-dst")
	require.NoError(t, err)
	require.NotNil(t, restored)
	assert.Equal(t, 4096, restored.Blocksize,
		"512 here is the source's POST-snapshot geometry laid over the snapshot's 4096-layout data")
}

// TestSnapshotWithNoHistorySourceRestoresFreely pins the narrow case the
// fail-closed must NOT swallow: a snapshot of a zvol nothing has ever exported
// carries no geometry because there is none, and refusing there would be a
// refusal to provision from an empty source.
//
// FAILS ON 8cec385: no — no-regression pin.
func TestSnapshotWithNoHistorySourceRestoresFreely(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/empty-source", Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, "pool/parent/empty-source", "empty-point", nil)
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-from-empty", "iscsi", map[string]string{paramISCSIBlocksize: "512"}), "empty-point"))
	require.NoError(t, err, "a source with no block history has no layout to preserve")
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-from-empty")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 512, extent.Blocksize)
}

// TestSnapshotOfABlockVolumeCapturesItsGeometry is the mechanism that keeps the
// fail-closed above from being a fleet-wide outage: CreateSnapshot captures the
// source's live geometry onto every snapshot it takes, including for a source
// the driver has never stamped.
//
// FAILS ON 8cec385: yes. CreateSnapshot wrote only the four identity properties
// there, so the snapshot carries no geometry and the assertions on
// PropBlockISCSIBlocksize / PropBlockISCSIPblocksize fail.
func TestSnapshotOfABlockVolumeCapturesItsGeometry(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	provisionUnstamped4096Volume(t, d, client, "pvc-capture-src")

	_, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: "pvc-capture-src", Name: "capture-point",
	})
	require.NoError(t, err)

	snap, err := client.SnapshotFindByName(ctx, "pool/parent", "capture-point")
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, "4096", snap.UserProperties[PropBlockISCSIBlocksize].Value,
		"a snapshot of an UNSTAMPED volume must still capture what its live extent reports")
	assert.Equal(t, "true", snap.UserProperties[PropBlockISCSIPblocksize].Value,
		"and the physical half, because a half record is refused on restore")
}

// foreignExtentClient makes ISCSIExtentCreate behave the way the TrueNAS client
// does on an ambiguous "already exists"/"invalid params": it returns an EXISTING
// object found by name instead of the one that was asked for.
type foreignExtentClient struct {
	*truenas.MockClient
	foreign *truenas.ISCSIExtent
}

func (c *foreignExtentClient) ISCSIExtentCreate(
	ctx context.Context, name, diskPath, comment string, blocksize int, physicalBlocksize bool, rpm string,
	opts ...truenas.ISCSIExtentCreateOptions,
) (*truenas.ISCSIExtent, error) {
	if c.foreign != nil {
		found := *c.foreign
		c.foreign = nil
		return &found, nil
	}
	return c.MockClient.ISCSIExtentCreate(ctx, name, diskPath, comment, blocksize, physicalBlocksize, rpm, opts...)
}

// TestCreateErrorRecoveryRejectsAForeignExtent is round-5 MEDIUM 3.
//
// The create-error / idempotency arms adopted whatever object came back —
// ISCSIExtentFindByDisk after an ambiguous error in the share builder, and the
// client's own find-by-name fallback — with NO geometry check. A concurrent
// controller or a stale same-name extent could therefore win the race at a
// different geometry, and the next resource update back-stamped it as this
// volume's truth.
//
// FAILS ON 8cec385: yes. Nothing there compares the returned extent against the
// geometry the create was authorized at, so CreateVolume returns OK and the
// dataset is back-stamped with the foreign 512.
func TestCreateErrorRecoveryRejectsAForeignExtent(t *testing.T) {
	d, mock := newBlockImmutabilityDriver(t)
	client := &foreignExtentClient{MockClient: mock}
	d.truenasClient = client
	ctx := context.Background()
	_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	// Somebody else's extent, at a geometry this volume was never authorized for.
	client.foreign = &truenas.ISCSIExtent{
		ID: 9001, Name: "pvc-foreign", Disk: "zvol/pool/parent/pvc-foreign",
		Blocksize: 512, Pblocksize: boolPtr(true),
	}

	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-foreign", "iscsi", map[string]string{paramISCSIBlocksize: "4096"}))
	require.Error(t, err, "an extent the driver did not create must be validated before it is adopted")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "4096",
		"the refusal must name the geometry the create was authorized at")

	// And nothing back-stamped the foreign geometry onto the volume.
	ds, getErr := mock.DatasetGet(ctx, "pool/parent/pvc-foreign")
	if getErr == nil {
		assert.NotEqual(t, "512", ds.UserProperties[PropBlockISCSIBlocksize].Value,
			"a rejected foreign extent must never become this volume's recorded truth")
	} else {
		assert.True(t, truenas.IsNotFoundError(getErr), "unexpected error reading the destination: %v", getErr)
	}
}

// crashAfterCloneClient aborts CreateVolume immediately after the clone lands,
// before the ownership fold, leaving exactly the marker-proven in-flight remnant
// completeResumedCloneRemnant is built to recover.
type crashAfterCloneClient struct {
	*truenas.MockClient
	armed bool
}

var errRound5SimulatedCrash = errors.New("round-5 simulated crash after clone")

func (c *crashAfterCloneClient) SnapshotClone(ctx context.Context, snapshotID, newDatasetName string) error {
	if err := c.MockClient.SnapshotClone(ctx, snapshotID, newDatasetName); err != nil {
		return err
	}
	if c.armed {
		c.armed = false
		panic(errRound5SimulatedCrash)
	}
	return nil
}

// TestResumedCloneRemnantCarriesSourceGeometry is round-5 MEDIUM 4.
//
// completeResumedCloneRemnant stamped identity, content source and capacity but
// NO geometry: it relied on whatever user properties the ZFS clone happened to
// inherit. That is safe only by accident, and not at all when the source's own
// record was incomplete. Recovery now resolves the source exactly as the
// un-crashed path does and folds the answer into the same stamp.
//
// FAILS ON 8cec385: yes — for a different reason on each half. There the
// recovery stamp carries no geometry at all, so the first assertion (the
// destination RECORDS the source's 4096) fails outright.
func TestResumedCloneRemnantCarriesSourceGeometry(t *testing.T) {
	d, mock := newBlockImmutabilityDriver(t)
	client := &crashAfterCloneClient{MockClient: mock}
	d.truenasClient = client
	ctx := context.Background()

	// A 4096 source and a driver-taken snapshot that captured its geometry.
	_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-remnant-src", "iscsi", map[string]string{
		paramISCSIBlocksize: "4096", paramISCSIPblocksize: "true",
	}))
	require.NoError(t, err)
	_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{SourceVolumeId: "pvc-remnant-src", Name: "remnant-point"})
	require.NoError(t, err)

	// The destination's geometry stamp is scrubbed after the crash, so the
	// recovery cannot pass by inheriting it — which is exactly the accident the
	// old code depended on.
	req := restoreFromSnapshot(blockTuningRequest("pvc-remnant-dst", "iscsi", nil), "remnant-point")
	client.armed = true
	func() {
		defer func() {
			require.Equal(t, errRound5SimulatedCrash, recover(), "the first attempt must crash right after the clone")
		}()
		_, _ = d.CreateVolume(ctx, req)
		t.Fatal("CreateVolume returned without crashing; the crash simulation is invalid")
	}()
	require.NoError(t, mock.DatasetRemoveUserProperties(ctx, "pool/parent/pvc-remnant-dst",
		[]string{PropBlockISCSIBlocksize, PropBlockISCSIPblocksize}))

	// The recovery arm is exercised DIRECTLY. Going through CreateVolume would
	// also pass, because the existing-volume replay re-resolves the content source
	// too — two independent mechanisms cover this finding — but then the assertion
	// would not isolate the arm the finding is about.
	remnant, err := mock.DatasetGet(ctx, "pool/parent/pvc-remnant-dst")
	require.NoError(t, err)
	recovered, err := d.completeResumedCloneRemnant(
		ctx, remnant, "pool/parent/pvc-remnant-dst", "pvc-remnant-dst", req.GetVolumeContentSource(), testGiB, ShareTypeISCSI)
	require.NoError(t, err, "a marker-proven remnant must be recoverable")
	require.NotNil(t, recovered)
	assert.Equal(t, "4096", recovered.UserProperties[PropBlockISCSIBlocksize].Value,
		"recovery must resolve and record the SOURCE's geometry rather than hope the clone inherited it")
	assert.Equal(t, "true", recovered.UserProperties[PropBlockISCSIPblocksize].Value,
		"both halves, or the share build that follows refuses a half record")

	// And the whole path still completes end to end.
	_, err = d.CreateVolume(ctx, req)
	require.NoError(t, err)
	extent, err := mock.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-remnant-dst")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 4096, extent.Blocksize,
		"the recovered volume's extent must be built from it; 512 here is the controller default over cloned 4096 data")
}

// TestGeometryAndWitnessRideInTheFatalUpdate is round-5 (c), third bullet: the
// share builder's resource-ID write is warning-only, so a volume could end up
// with data, no extent-ID witness and no geometry stamp — the precise state in
// which a later rebuild has nothing to resolve from. Both now ride in
// CreateVolume's FATAL managed-property update as well, at zero extra cost.
//
// FAILS ON 8cec385: yes. There the iSCSI backend ignored finalProperties
// entirely, so a failing resource-ID write left the dataset with neither key.
func TestGeometryAndWitnessRideInTheFatalUpdate(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	finalProperties := map[string]string{PropManagedResource: "true"}
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/pvc-fatal-fold", Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, iscsiShareBackend{d}.CreateShare(
		ctx, nil, "pool/parent/pvc-fatal-fold", "pvc-fatal-fold", true, true, finalProperties))

	assert.Equal(t, "512", finalProperties[PropBlockISCSIBlocksize],
		"the geometry the extent really came out at must ride in the caller's fatal update")
	assert.Contains(t, finalProperties, PropBlockISCSIPblocksize,
		"both halves, or the next rebuild refuses a half record")
	assert.Contains(t, finalProperties, PropISCSIExtentID,
		"and the witness, so it cannot be lost by the warning-only write")
}
