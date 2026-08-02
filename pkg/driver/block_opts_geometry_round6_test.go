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
// GF-4 round 6 — the three things round 5 claimed and did not have
//
//  1. mergeGeometry never rejected a disagreement, so a "complete" record could
//     be MANUFACTURED out of conflicting halves.
//  2. CreateSnapshot captured a stamp without ever asking the live extent.
//  3. "Data-free" was still the ABSENCE of the driver's own bookkeeping.
//
// Plus the ordering claim (an unresolvable geometry refused only after a target
// had been created) and the missing domain validation on stored geometry.
// ---------------------------------------------------------------------------

// stampDriverOwnership writes the LOCAL ownership property createDataset writes
// in production onto a dataset a fixture built with a raw DatasetCreate.
//
// Round 6 makes it load-bearing rather than cosmetic: a dataset with no local
// ownership stamp is no longer treated as provably free of block-addressed data
// (blockDataFreeProof), because the driver cannot account for what has been
// written to storage it did not create. Every clone SOURCE in production is a
// volume this driver provisioned and therefore carries this stamp; a fixture
// that omits it is modeling an imported/foreign zvol, which is exactly the
// shape that must now fail closed.
func stampDriverOwnership(t *testing.T, client *truenas.MockClient, d *Driver, datasetNames ...string) {
	t.Helper()
	for _, name := range datasetNames {
		require.NoError(t, client.DatasetSetUserProperties(context.Background(), name, map[string]string{
			PropDriverInstanceID: d.driverInstanceID(),
		}))
	}
}

// newOwnedBareZvol is stampDriverOwnership plus the DatasetCreate, for the
// common "an empty source volume this driver provisioned" fixture.
func newOwnedBareZvol(t *testing.T, client *truenas.MockClient, d *Driver, datasetName string) {
	t.Helper()
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	stampDriverOwnership(t, client, d, datasetName)
}

// ---------------------------------------------------------------------------
// 1. CONFLICT-REJECTING MERGE
// ---------------------------------------------------------------------------

// TestConflictingHalvesCannotManufactureAKnownGeometry is round-6 HIGH 1, driven
// through the REAL existing-volume replay arm rather than through mergeGeometry
// directly.
//
// Shape: a destination restored from a 4096/true snapshot has its physical
// property stripped and its logical property corrupted to 512 — the state a
// half-completed write or an operator "fix" leaves behind. The replay finds the
// destination record incomplete, re-resolves the content source (which answers
// 4096/true), and round 5 then MERGED them: it kept the destination's 512
// because mergeGeometry never overrode a value primary already held, filled
// physical from the source, recomputed geometryKnown from field presence, and
// handed ISCSIExtentCreate a 512 geometry over 4096-layout bytes.
//
// FAILS ON bdf3c36: yes. VERIFIED EMPIRICALLY by reverting mergeGeometry to its
// round-5 body (delete the two disagreement returns; keep the gap-filling) and
// re-running in this worktree: this test fails, as does
// TestMergeGeometryRefusesDisagreementAndAttributesEachField.
func TestConflictingHalvesCannotManufactureAKnownGeometry(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	// A 4096 source, and a driver-taken snapshot that captured 4096/true.
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-conflict-src", "iscsi", map[string]string{
		paramISCSIBlocksize: "4096", paramISCSIPblocksize: "true",
	}))
	require.NoError(t, err)
	_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: "pvc-conflict-src", Name: "conflict-point",
	})
	require.NoError(t, err)

	req := restoreFromSnapshot(blockTuningRequest("pvc-conflict-dst", "iscsi", nil), "conflict-point")
	_, err = d.CreateVolume(ctx, req)
	require.NoError(t, err)

	// Now the destination is left holding HALF a record, and the half it holds
	// contradicts the source it was cloned from.
	destination := "pool/parent/pvc-conflict-dst"
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+destination)
	require.NoError(t, err)
	require.NotNil(t, extent)
	require.NoError(t, client.ISCSIExtentDelete(ctx, extent.ID, false, true))
	require.NoError(t, client.DatasetRemoveUserProperties(ctx, destination, []string{PropBlockISCSIPblocksize}))
	require.NoError(t, client.DatasetSetUserProperties(ctx, destination, map[string]string{
		PropBlockISCSIBlocksize: "512",
		PropISCSIExtentID:       "-",
	}))

	_, err = d.CreateVolume(ctx, req)
	require.Error(t, err, "two records of the same bytes that disagree may not be combined into one 'known' record")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	message := status.Convert(err).Message()
	assert.Contains(t, message, "512", "the refusal must name both values it refused to choose between")
	assert.Contains(t, message, "4096")

	rebuilt, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/"+destination)
	require.NoError(t, findErr)
	assert.Nil(t, rebuilt, "no extent may be created from a geometry assembled out of contradictory halves")
}

// TestMergeGeometryRefusesDisagreementAndAttributesEachField is the unit-level
// statement of the same rule, including the part the end-to-end test cannot
// show: that a merged record names BOTH sources rather than presenting one
// source's name for both halves.
//
// FAILS ON bdf3c36: yes — it does not compile there. mergeGeometry had the
// signature (blockGeometry, blockGeometry) blockGeometry with no error return
// and no per-field provenance, so the error assertions and the
// blocksizeFrom/pblocksizeFrom references have no counterpart.
func TestMergeGeometryRefusesDisagreementAndAttributesEachField(t *testing.T) {
	stamp := blockGeometry{blocksize: intPtr(512)}.attribute("the volume's recorded geometry stamp")
	source := blockGeometry{blocksize: intPtr(4096), pblocksize: boolPtr(true)}.attribute("the geometry the snapshot captured")

	_, err := mergeGeometry(stamp, source, "pool/parent/pvc")
	require.Error(t, err, "a field both records hold, with different values, is a refusal")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "the volume's recorded geometry stamp")
	assert.Contains(t, status.Convert(err).Message(), "the geometry the snapshot captured")

	// Gap-filling is still allowed — but the filled field keeps the provenance of
	// the record it came FROM.
	partial := blockGeometry{blocksize: intPtr(4096)}.attribute("the volume's recorded geometry stamp")
	merged, mergeErr := mergeGeometry(partial, source, "pool/parent/pvc")
	require.NoError(t, mergeErr)
	assert.Equal(t, geometryKnown, merged.knowledge)
	assert.Equal(t, "the volume's recorded geometry stamp", merged.blocksizeFrom)
	assert.Equal(t, "the geometry the snapshot captured", merged.pblocksizeFrom)
	assert.Contains(t, merged.provenance, "and",
		"a record assembled from two sources must SAY it was assembled from two sources")

	// A half-record remains unresolved after a merge, so physical cannot be
	// supplied by a controller default.
	partialOnly := blockGeometry{blocksize: intPtr(4096)}.attribute("the volume's recorded geometry stamp")
	combined, combineErr := mergeGeometry(partialOnly, blockGeometry{}, "pool/parent/pvc")
	require.NoError(t, combineErr)
	assert.NotEqual(t, geometryKnown, combined.knowledge,
		"a partial record must not claim known geometry")
}

// ---------------------------------------------------------------------------
// 2. CREATESNAPSHOT MUST CONSULT THE LIVE EXTENT
// ---------------------------------------------------------------------------

// TestCreateSnapshotRefusesToCaptureAStaleStamp is round-6 HIGH 2.
//
// Shape: the volume records 4096; its extent is later re-created at 512 out of
// band. Round 5's snapshotGeometryProps returned the complete stamp immediately
// (block_opts.go:1407-1410) and never queried the extent, so the snapshot
// captured 4096 over bytes that are now addressed at 512, and a restore would
// then build a 4096 extent over them. The live-vs-stamp guard existed but only
// the share-rebuild path called it.
//
// FAILS ON bdf3c36: yes. VERIFIED EMPIRICALLY by restoring the round-5 early
// return (`if stamped.complete() { return stamped.props() }` plus the
// storedBlockProtocol gate) and re-running: CreateSnapshot succeeds and
// require.Error fails.
func TestCreateSnapshotRefusesToCaptureAStaleStamp(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-drifted", "iscsi", map[string]string{
		paramISCSIBlocksize: "4096", paramISCSIPblocksize: "true",
	}))
	require.NoError(t, err)

	// The extent is re-created at 512 out of band. The stamp still says 4096.
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-drifted")
	require.NoError(t, err)
	require.NotNil(t, extent)
	extent.Blocksize = 512

	_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: "pvc-drifted", Name: "drifted-point",
	})
	require.Error(t, err, "a snapshot must carry the geometry of the bytes inside it, and the driver knows the stamp is contradicted")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	message := status.Convert(err).Message()
	assert.Contains(t, message, "4096", "the refusal must name both records")
	assert.Contains(t, message, "512")

	snap, findErr := client.SnapshotFindByName(ctx, "pool/parent", "drifted-point")
	require.NoError(t, findErr)
	assert.Nil(t, snap, "no snapshot may be taken carrying a geometry the driver knows to be contradicted")
}

// TestCreateSnapshotCapturesLiveGeometryWithoutTargetMarkers is the second half
// of round-6 HIGH 2: an iSCSI zvol missing its target markers still has a live
// extent and must be probed for snapshot capture. Protocol detection accepts
// that extent identity while the geometry read remains driven by iSCSI.
//
// FAILS ON bdf3c36: yes. VERIFIED EMPIRICALLY by restoring the
// storedBlockProtocol gate and re-running: the snapshot carries no geometry key
// and both value assertions fail.
func TestCreateSnapshotCapturesLiveGeometryWithoutTargetMarkers(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-nomarkers", "iscsi", map[string]string{
		paramISCSIBlocksize: "4096", paramISCSIPblocksize: "true",
	}))
	require.NoError(t, err)

	// Strip the target markers and geometry stamp, but leave the extent identity
	// and live extent. This is the imported / partially-stripped zvol shape.
	require.NoError(t, client.DatasetRemoveUserProperties(ctx, "pool/parent/pvc-nomarkers", []string{
		PropISCSITargetID, PropISCSITargetExtentID, PropBlockISCSIBlocksize, PropBlockISCSIPblocksize,
	}))
	// The controller default is moved AWAY from the live value, so an
	// implementation that stamps the default instead of reading the extent
	// produces 512 and fails these assertions.
	d.config.ISCSI.ExtentBlocksize = 512

	_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: "pvc-nomarkers", Name: "nomarkers-point",
	})
	require.NoError(t, err)
	snap, err := client.SnapshotFindByName(ctx, "pool/parent", "nomarkers-point")
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, "4096", snap.UserProperties[PropBlockISCSIBlocksize].Value,
		"the live extent must be consulted whatever bookkeeping the dataset happens to still carry")
	assert.Equal(t, "true", snap.UserProperties[PropBlockISCSIPblocksize].Value)
}

// ---------------------------------------------------------------------------
// 3. DATA-FREE IS POSITIVE KNOWLEDGE
// ---------------------------------------------------------------------------

// TestUnownedZvolIsNotProvablyDataFree is round-6 HIGH 3, and it is the direct
// inversion of round 5's TestBareZvolWithNoWitnessIsStillDataFree.
//
// An imported or admin-created zvol carrying a foreign filesystem has none of
// the driver's eleven witness properties, so every absence check ever written
// classified it as blank and created its extent at the controller default. The
// proof is now POSITIVE — this driver instance's LOCAL ownership stamp — and a
// zvol the driver did not create cannot supply it.
//
// FAILS ON bdf3c36: yes. VERIFIED EMPIRICALLY by reverting blockDataFreeProof to
// round 5's absence check and re-running: EnsureShare succeeds and require.Error
// fails. (On bdf3c36 itself this same fixture is
// TestBareZvolWithNoWitnessIsStillDataFree, asserting the opposite outcome.)
func TestUnownedZvolIsNotProvablyDataFree(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	datasetName := "pool/parent/pvc-imported-zvol"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)

	err = iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-imported-zvol", nil)
	require.Error(t, err,
		"a zvol this driver did not create may be carrying a foreign filesystem laid out at some other block size")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), PropDriverInstanceID,
		"the refusal must name the positive proof that is missing")

	created, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, findErr)
	assert.Nil(t, created, "no extent may be created at the controller default over storage of unknown provenance")

	// And the proof is exactly what unwedges it: stamping the dataset as this
	// driver instance's own makes the default honest again.
	stampDriverOwnership(t, client, d, datasetName)
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-imported-zvol", nil))
	rebuilt, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, findErr)
	require.NotNil(t, rebuilt)
	assert.Equal(t, 512, rebuilt.Blocksize)
}

// TestInheritedOwnershipIsNotProofOfDataFreedom pins the reason the proof
// insists on ZFS source=="local": a clone inherits its source's ownership stamp,
// and a clone destination is the one thing certain to hold somebody else's
// bytes. An inherited stamp would otherwise be a self-certifying proof of
// data-freedom for exactly the wrong dataset.
//
// FAILS ON bdf3c36: it does not COMPILE there — blockDataFreeProof did not
// exist. This is a construction pin on the new rule, not a regression proof.
// Verified as load-bearing by reverting blockDataFreeProof to round 5's
// absence check (`no witness -> true`) and re-running: all three cases fail.
func TestInheritedOwnershipIsNotProofOfDataFreedom(t *testing.T) {
	d, _ := newBlockImmutabilityDriver(t)
	inherited := &truenas.Dataset{Type: "VOLUME", UserProperties: map[string]truenas.UserProperty{
		PropDriverInstanceID: {Value: d.driverInstanceID(), Source: "pool/parent/source@snap"},
	}}
	proof, ok := d.blockDataFreeProof(inherited)
	assert.False(t, ok, "an inherited ownership stamp is the SOURCE's fact, not this dataset's")
	assert.Contains(t, proof, "INHERITED")

	local := &truenas.Dataset{Type: "VOLUME", UserProperties: map[string]truenas.UserProperty{
		PropDriverInstanceID: {Value: d.driverInstanceID(), Source: "local"},
	}}
	_, ok = d.blockDataFreeProof(local)
	assert.True(t, ok)

	foreign := &truenas.Dataset{Type: "VOLUME", UserProperties: map[string]truenas.UserProperty{
		PropDriverInstanceID: {Value: "some.other.driver@tank/other", Source: "local"},
	}}
	proof, ok = d.blockDataFreeProof(foreign)
	assert.False(t, ok, "another instance's history is not this instance's to account for")
	assert.Contains(t, proof, "some.other.driver@tank/other")
}

// TestUnownedSnapshotSourceRestoreFailsClosed carries the same rule onto the
// restore path, which is where the reverify's concrete fail-open sequence ended:
// live source extent at 4096, all eleven witness keys absent, so the restore saw
// "no witness" and applied the 512 default over the snapshot's bytes.
//
// FAILS ON bdf3c36: yes. VERIFIED EMPIRICALLY by reverting blockDataFreeProof to
// round 5's absence check and re-running: resolveCloneSourceBlockGeometry
// returns geometryNoHistory, CreateVolume succeeds, and the error assertion
// fails.
func TestUnownedSnapshotSourceRestoreFailsClosed(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	// An imported 4096 zvol with a live extent and NO driver bookkeeping at all.
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/imported-source", Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	_, err = client.ISCSIExtentCreate(ctx, "imported-source", "zvol/pool/parent/imported-source",
		"", 4096, true, "SSD")
	require.NoError(t, err)
	// A snapshot taken outside the driver: it captured no geometry either.
	_, err = client.SnapshotCreate(ctx, "pool/parent/imported-source", "imported-point", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "imported-point",
		PropCSISnapshotSourceVolumeID: "imported-source",
	})
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-from-imported", "iscsi", nil), "imported-point"))
	require.Error(t, err, "absence of the driver's bookkeeping is not proof that the snapshot's bytes are unaddressed")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, getErr := client.DatasetGet(ctx, "pool/parent/pvc-from-imported")
	assert.True(t, truenas.IsNotFoundError(getErr),
		"the refusal must precede the first destination mutation (err=%v)", getErr)
}

// ---------------------------------------------------------------------------
// 4. ORDERING — THE REFUSAL PRECEDES THE FIRST DESTINATION MUTATION
// ---------------------------------------------------------------------------

// TestUnknownGeometryRefusalLeavesNoTargetRemnant is round-6 item 3. Round 4
// shipped an overclaiming ordering comment and round 5 shipped the same pattern
// again: in the generic existing-volume/publish/startup path the iSCSI target
// (and, in strict mode, an initiator group plus a dataset property write) were
// created BEFORE resolveExtentGeometry could return unknown, and the error path
// deletes none of them. Every retried publish or reconcile pass therefore
// accreted remnants on a volume that could never be published.
//
// FAILS ON bdf3c36: yes. VERIFIED EMPIRICALLY by moving resolveExtentGeometry
// back inside the `if extent == nil` create block (its round-5 position, after
// ISCSITargetCreate) and re-running: ISCSITargetFindByName returns a live target
// after the refusal and the nil assertion fails on the first pass.
func TestUnknownGeometryRefusalLeavesNoTargetRemnant(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	datasetName := "pool/parent/pvc-remnant-order"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-remnant-order", "iscsi", map[string]string{
		paramISCSIBlocksize: "4096",
	}))
	require.NoError(t, err)

	// Delete the whole share and the geometry record: the extent is gone, the
	// extent-ID witness says the bytes HAVE been block-addressed, and nothing
	// establishes their layout. This is the DR-rebuild shape.
	require.NoError(t, d.deleteISCSIShare(ctx, datasetName))
	require.NoError(t, client.DatasetRemoveUserProperties(ctx, datasetName,
		[]string{PropBlockISCSIBlocksize, PropBlockISCSIPblocksize}))
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropISCSIExtentID: "-", PropISCSITargetID: "-", PropISCSITargetExtentID: "-",
	}))

	for attempt := 1; attempt <= 2; attempt++ {
		err = iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-remnant-order", nil)
		require.Error(t, err, "attempt %d: the geometry is unestablishable", attempt)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		target, findErr := client.ISCSITargetFindByName(ctx, d.iscsiShareName("pvc-remnant-order"))
		require.NoError(t, findErr)
		assert.Nil(t, target,
			"attempt %d: an unpublishable volume must not accrete an iSCSI target on every reconcile pass", attempt)
	}
}

// TestResumedCloneRecoveryRefusesBeforeExpandingTheRemnant is the provenance.go
// half of the same ordering rule: completeResumedCloneRemnant expanded the
// remnant's capacity (a destination MUTATION) before it resolved the source's
// geometry, so an unresolvable source still grew the remnant before the recovery
// failed closed.
//
// FAILS ON bdf3c36: yes. VERIFIED EMPIRICALLY by swapping ensureCloneCapacity
// back in front of contentSourceBlockGeometry and re-running: DatasetExpand is
// called once before the refusal and the zero-expand assertion fails.
func TestResumedCloneRecoveryRefusesBeforeExpandingTheRemnant(t *testing.T) {
	mock := truenas.NewMockClient()
	client := &expandCountingClient{MockClient: mock}
	d, _ := newBlockImmutabilityDriver(t)
	d.truenasClient = client
	ctx := context.Background()

	_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	// An imported source with a live extent, no driver bookkeeping, and a
	// snapshot that captured no geometry: unresolvable.
	_, err = mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/order-source", Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	_, err = mock.SnapshotCreate(ctx, "pool/parent/order-source", "order-point", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "order-point",
		PropCSISnapshotSourceVolumeID: "order-source",
	})
	require.NoError(t, err)
	// The remnant: a clone that landed before the ownership fold.
	require.NoError(t, mock.SnapshotClone(ctx, "pool/parent/order-source@order-point", "pool/parent/order-remnant"))
	remnant, err := mock.DatasetGet(ctx, "pool/parent/order-remnant")
	require.NoError(t, err)

	source := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "order-point"},
	}}
	_, err = d.completeResumedCloneRemnant(
		ctx, remnant, "pool/parent/order-remnant", "order-remnant", source, 4*testGiB, ShareTypeISCSI)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Zero(t, client.expandCalls,
		"a recovery that is going to be refused must not first widen the remnant it is refusing")
}

// expandCountingClient counts destination capacity mutations.
type expandCountingClient struct {
	*truenas.MockClient
	expandCalls int
}

func (c *expandCountingClient) DatasetExpand(ctx context.Context, name string, size int64) error {
	c.expandCalls++
	return c.MockClient.DatasetExpand(ctx, name, size)
}

// ---------------------------------------------------------------------------
// 5. STORED GEOMETRY HAS A DOMAIN
// ---------------------------------------------------------------------------

// TestOutOfDomainStoredGeometryIsUntrusted is round-6 item 4, driven through the
// real rebuild rather than through the parser.
//
// Round 5 validated iscsi/blocksize as a StorageClass parameter but parsed a
// STORED value as any integer at all, so the documented `zfs set` recovery
// turned an operator typo into a "known" geometry that a rebuild then created an
// extent from. An out-of-domain value is now UNTRUSTED: the volume reads as
// unrecorded, the property still counts as a block-data witness, and the rebuild
// fails closed naming both properties instead of acting on 1234.
//
// FAILS ON bdf3c36: yes. VERIFIED EMPIRICALLY by dropping the storedInDomain
// guard from the PropBlockISCSIBlocksize read and re-running: the stamp parses
// as complete, EnsureShare creates a 1234-byte extent and returns nil, so
// require.Error fails.
func TestOutOfDomainStoredGeometryIsUntrusted(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	datasetName := "pool/parent/pvc-typo"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-typo", "iscsi", nil))
	require.NoError(t, err)
	stripGeometryAndExtent(t, client, datasetName)

	// The operator recovery command, mistyped.
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropBlockISCSIBlocksize:  "1234",
		PropBlockISCSIPblocksize: "true",
	}))

	err = iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-typo", nil)
	require.Error(t, err, "1234 is not a logical block size any extent can be created at, so it records nothing")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), validISCSIBlocksizeList,
		"the refusal must state the domain the operator has to record a value from")

	rebuilt, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, findErr)
	assert.Nil(t, rebuilt, "an out-of-domain stamp must never drive a create")

	// The in-domain correction is all it takes.
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropBlockISCSIBlocksize: "4096",
	}))
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-typo", nil))
	fixed, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, findErr)
	require.NotNil(t, fixed)
	assert.Equal(t, 4096, fixed.Blocksize)
}

// ---------------------------------------------------------------------------
// 6. TEST-HONESTY REPAIRS — the round-5 tests that proved less than they said
// ---------------------------------------------------------------------------

// createErrorThenFindClient is the HONEST version of round 5's
// foreignExtentClient. Round 5's wrapper returned the foreign object with a NIL
// error, which exercises the post-create validation arm — not the
// createErr + ISCSIExtentFindByDisk recovery arm the finding was about
// (iscsi_share.go). A regression that removed the validation from only that arm
// passed. This one fails the create for real and plants the foreign object where
// the recovery arm looks for it.
type createErrorThenFindClient struct {
	*truenas.MockClient
	foreign      *truenas.ISCSIExtent
	createErrors int
}

var errRound6CreateAmbiguous = errors.New("round-6 ambiguous extent create failure")

func (c *createErrorThenFindClient) ISCSIExtentCreate(
	ctx context.Context, name, diskPath, comment string, blocksize int, physicalBlocksize bool, rpm string,
	opts ...truenas.ISCSIExtentCreateOptions,
) (*truenas.ISCSIExtent, error) {
	if c.foreign != nil {
		c.createErrors++
		return nil, errRound6CreateAmbiguous
	}
	return c.MockClient.ISCSIExtentCreate(ctx, name, diskPath, comment, blocksize, physicalBlocksize, rpm, opts...)
}

// The foreign object only becomes visible AFTER the create fails, so the
// pre-create resolveISCSIExtent lookup finds nothing and the builder genuinely
// takes the create path. Modeling it any other way would exercise the
// existing-extent guards instead of the recovery arm — which is exactly the
// mistake round 5's wrapper made in the other direction.
func (c *createErrorThenFindClient) ISCSIExtentFindByDisk(ctx context.Context, diskPath string) (*truenas.ISCSIExtent, error) {
	if c.foreign != nil && c.createErrors > 0 && diskPath == c.foreign.Disk {
		found := *c.foreign
		return &found, nil
	}
	return c.MockClient.ISCSIExtentFindByDisk(ctx, diskPath)
}

// TestCreateErrorRecoveryArmRejectsAForeignExtent exercises the REAL
// createErr + find-by-disk recovery arm.
//
// FAILS ON bdf3c36: yes. VERIFIED EMPIRICALLY, and this is the discrimination
// round 5's wrapper could not make: deleting the validateExtentAgainstGeometry
// call from ONLY the `if !freshlyCreated || IsAlreadyExistsError` recovery arm
// (leaving the post-create one intact) fails THIS test while
// TestCreateSuccessArmRejectsAForeignExtent stays green.
func TestCreateErrorRecoveryArmRejectsAForeignExtent(t *testing.T) {
	d, mock := newBlockImmutabilityDriver(t)
	client := &createErrorThenFindClient{MockClient: mock}
	d.truenasClient = client
	ctx := context.Background()
	_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	datasetName := "pool/parent/pvc-recovery-arm"
	newOwnedBareZvol(t, mock, d, datasetName)

	client.foreign = &truenas.ISCSIExtent{
		ID: 9002, Name: "pvc-recovery-arm", Disk: "zvol/" + datasetName,
		Blocksize: 512, Pblocksize: boolPtr(true),
	}

	// The volume's own stamp authorizes 4096; the object the recovery arm finds
	// is 512.
	require.NoError(t, mock.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropBlockISCSIBlocksize:  "4096",
		PropBlockISCSIPblocksize: "true",
	}))

	err = iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-recovery-arm", nil)
	require.Error(t, err, "the recovery arm must validate the object it found before adopting it")
	assert.Positive(t, client.createErrors, "the create must actually have failed; otherwise the arm was not exercised")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "4096",
		"the refusal must name the geometry the create was authorized at")

	ds, getErr := mock.DatasetGet(ctx, datasetName)
	require.NoError(t, getErr)
	assert.Equal(t, "4096", ds.UserProperties[PropBlockISCSIBlocksize].Value,
		"a rejected foreign extent must never become this volume's recorded truth")
}

// resourceIDWriteFailureClient fails EXACTLY the warning-only in-share
// resource-ID write (setDatasetUserProperties from createISCSIShareForDataset)
// and lets every other property write through, which is the state round 5's
// TestGeometryAndWitnessRideInTheFatalUpdate described but never produced: that
// test called the backend directly with a synthetic finalProperties map, so it
// proved map folding, not that a REAL CreateVolume persists the keys when the
// warning-only write is lost.
type resourceIDWriteFailureClient struct {
	*truenas.MockClient
	failed int
}

func (c *resourceIDWriteFailureClient) DatasetSetUserProperties(
	ctx context.Context, name string, properties map[string]string,
) error {
	if _, isResourceWrite := properties[PropISCSIExtentID]; isResourceWrite {
		if _, isFinal := properties[PropProvisionSuccess]; !isFinal {
			c.failed++
			return errors.New("round-6 simulated warning-only resource-ID write failure")
		}
	}
	return c.MockClient.DatasetSetUserProperties(ctx, name, properties)
}

// TestRealCreateVolumePersistsGeometryWhenTheWarningWriteFails drives the whole
// CreateVolume with the warning-only write failing.
//
// FAILS ON bdf3c36: no — this is an HONESTY REPAIR, not a new-bug proof. Round 5
// shipped the production fold that makes it pass; what it did NOT ship was a test
// that exercises it, and its stand-in asserted map folding on a synthetic map.
// VERIFIED EMPIRICALLY: deleting the finalProperties fold in iscsi_share.go fails
// this test. (It also fails the round-5 map test, so this pair does not
// discriminate the two — what it adds is that the REAL CreateVolume path, with
// the warning-only write genuinely failing, is now exercised at all.)
func TestRealCreateVolumePersistsGeometryWhenTheWarningWriteFails(t *testing.T) {
	d, mock := newBlockImmutabilityDriver(t)
	client := &resourceIDWriteFailureClient{MockClient: mock}
	d.truenasClient = client
	ctx := context.Background()
	_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-warnfail", "iscsi", map[string]string{
		paramISCSIBlocksize: "4096", paramISCSIPblocksize: "true",
	}))
	require.NoError(t, err, "the warning-only write is non-fatal by design")
	require.Positive(t, client.failed, "the warning-only write must actually have failed")

	ds, err := mock.DatasetGet(ctx, "pool/parent/pvc-warnfail")
	require.NoError(t, err)
	assert.Equal(t, "4096", ds.UserProperties[PropBlockISCSIBlocksize].Value,
		"the geometry must survive the loss of the warning-only write, via the FATAL update")
	assert.Equal(t, "true", ds.UserProperties[PropBlockISCSIPblocksize].Value)
	assert.NotEmpty(t, ds.UserProperties[PropISCSIExtentID].Value,
		"and so must the extent-ID witness, or a later rebuild has nothing to refuse on")

	// And the volume is genuinely rebuildable from what survived.
	extent, err := mock.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-warnfail")
	require.NoError(t, err)
	require.NoError(t, mock.ISCSIExtentDelete(ctx, extent.ID, false, true))
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, "pool/parent/pvc-warnfail", "pvc-warnfail", nil))
	rebuilt, err := mock.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-warnfail")
	require.NoError(t, err)
	require.NotNil(t, rebuilt)
	assert.Equal(t, 4096, rebuilt.Blocksize)
}

// TestDetachedCopyRunThroughItsRealPathCountsAsBlockHistory replaces round 5's
// direct construction of the sentinel artifact: it RUNS
// prepareDetachedSnapshotCopy and then rebuilds the result, so the test depends
// on that function continuing to write the sentinel rather than on a fixture
// asserting the shape it is told to assert.
//
// FAILS ON bdf3c36: no for the refusal itself (round 5's witness set already
// counts the sentinel) — this is the HONESTY REPAIR of a test that constructed
// its own artifact. VERIFIED EMPIRICALLY that it adds a real dependency: deleting
// the `identityProperties[PropISCSIExtentID] = "-"` line from
// prepareDetachedSnapshotCopy fails THIS test and leaves
// TestDetachedCopySentinelCountsAsBlockHistory green.
func TestDetachedCopyRunThroughItsRealPathCountsAsBlockHistory(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	datasetName := "pool/parent/pvc-detached-real"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	// The state a replication receive leaves: the SOURCE's properties on a
	// destination zvol, including the source's ownership stamp and share IDs.
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropDriverInstanceID: d.driverInstanceID(),
		PropCSIVolumeName:    "some-other-volume",
		PropISCSITargetID:    "41",
		PropISCSIExtentID:    "42",
	}))
	_, err = client.SnapshotCreate(ctx, datasetName, "transferred", nil)
	require.NoError(t, err)
	ds, err := client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)

	// The REAL scrub path.
	refreshed, err := d.prepareDetachedSnapshotCopy(
		ctx, datasetName, ds, "pvc-detached-real", "some-snapshot", "transferred", testGiB, ShareTypeISCSI)
	require.NoError(t, err)
	require.NotNil(t, refreshed)

	after, err := client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	require.Equal(t, "-", after.UserProperties[PropISCSIExtentID].Value,
		"prepareDetachedSnapshotCopy must reset the source's extent ID to the ZFS sentinel")

	// And the rebuild of that copy refuses rather than defaulting: the sentinel is
	// there BECAUSE the dataset holds somebody else's bytes.
	err = iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-detached-real", nil)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	created, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, findErr)
	assert.Nil(t, created, "no extent may be created at the controller default over replicated data")
}

// TestMalformedStoredGeometryDoesNotWedgeAnAttachButIsNotEvidence replaces
// round 5's TestMalformedStoredBlockPropertyIsIgnored, whose comment claimed
// attach and resolution behavior it never exercised (it asserted parser output
// only). Both halves of the real claim are driven here.
//
// FAILS ON bdf3c36: no — this is an HONESTY REPAIR that exercises the claimed
// behavior instead of asserting it in prose. The second half (the malformed key
// is still a WITNESS, so an absent-extent rebuild refuses) is behavior round 5
// already had; the test is what was missing.
func TestMalformedStoredGeometryDoesNotWedgeAnAttachButIsNotEvidence(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	datasetName := "pool/parent/pvc-malformed"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-malformed", "iscsi", nil))
	require.NoError(t, err)

	// Corrupt the stamp while the extent is LIVE. An attach/publish rebuild must
	// still succeed: a corrupt advisory stamp may never wedge a live volume.
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropBlockISCSIBlocksize: "not-a-number",
	}))
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-malformed", nil),
		"a corrupt advisory stamp must never wedge a volume whose extent is live")

	// But it is NOT evidence. With the extent gone, the malformed key is still a
	// witness that these bytes have been block-addressed, so the rebuild refuses
	// instead of falling through to the controller default.
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NoError(t, client.ISCSIExtentDelete(ctx, extent.ID, false, true))
	require.NoError(t, client.DatasetRemoveUserProperties(ctx, datasetName, []string{PropBlockISCSIPblocksize}))

	err = iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-malformed", nil)
	require.Error(t, err, "a malformed geometry key is not 'no geometry'; it is an unreadable record of one")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	rebuilt, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, findErr)
	assert.Nil(t, rebuilt)
}

// TestMockSnapshotInheritsEveryUserProperty pins the mock-fidelity repair, and
// the behavior that fidelity now makes visible: a real ZFS snapshot carries ALL
// of its dataset's user properties, so a snapshot of a stamped volume carries
// the witness set and the identity markers too.
//
// FAILS ON bdf3c36: yes. VERIFIED EMPIRICALLY by narrowing the mock back to the
// two geometry keys and re-running: the witness/identity assertions find
// nothing.
func TestMockSnapshotInheritsEveryUserProperty(t *testing.T) {
	client := truenas.NewMockClient()
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/inherit-src", Type: "VOLUME", Volsize: testGiB,
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, "pool/parent/inherit-src", map[string]string{
		PropBlockISCSIBlocksize: "4096",
		PropISCSIExtentID:       "17",
		PropCSIVolumeName:       "inherit-src",
		PropNFSShareID:          "-",
	}))

	snap, err := client.SnapshotCreate(ctx, "pool/parent/inherit-src", "inherit-point", map[string]string{
		PropCSISnapshotName: "inherit-point",
	})
	require.NoError(t, err)
	assert.Equal(t, "4096", snap.UserProperties[PropBlockISCSIBlocksize].Value)
	assert.Equal(t, "17", snap.UserProperties[PropISCSIExtentID].Value,
		"a ZFS snapshot carries the WITNESS properties too, not just the geometry keys")
	assert.Equal(t, "inherit-src", snap.UserProperties[PropCSIVolumeName].Value)
	assert.Equal(t, "-", snap.UserProperties[PropNFSShareID].Value,
		"a ZFS '-' value remains a property and is inherited; removal uses a separate property update")
	assert.Equal(t, "inherit-point", snap.UserProperties[PropCSISnapshotName].Value,
		"explicitly written snapshot properties still win over inherited ones")
}
