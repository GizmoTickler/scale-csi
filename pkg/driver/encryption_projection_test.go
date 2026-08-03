package driver

import (
	"context"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// GF1 re-drill D-3 regressions, run under the PROJECTION-MODELING mock.
//
// The defect: Client.DatasetGet sends an extra.properties projection, and
// TrueNAS 26.0 omits everything outside it. The projection did not carry the
// encryption block, so `encrypted`, `locked`, `key_format` and `encryption_root`
// arrived as Go zero values and every "wire truth" predicate silently answered
// "plaintext" about an aes-256-gcm/passphrase dataset. It was invisible in unit
// tests because MockClient returned fully-populated structs.
//
// These tests set MockClient.ModelQueryProjection, which makes the mock deliver
// exactly the fields the CURRENT projection asks for (pkg/truenas
// dataset_projection.go, itself pinned against the drill's measured shapes). So:
//
//	PRE-FIX PROOF, all four: remove datasetEncryptionQueryProperties from
//	datasetQueryProperties (pkg/truenas/dataset.go — the a0315c3 state) and every
//	test below FAILS, each reproducing its drill symptom exactly.

// projectionModelingDriver is encryptionTestDriver with the mock told to model
// the pool.dataset.query projection.
func projectionModelingDriver(t *testing.T) (*Driver, *apiCallCountingClient) {
	t.Helper()
	d, client := encryptionTestDriver()
	client.ModelQueryProjection = true
	return d, client
}

// TestReplayOfEncryptedUnstampedVolumeIsNotCalledPlaintext is D-3a: on hardware,
// a CreateVolume replay against an encrypted-but-unstamped volume returned
// `FailedPrecondition: volume already exists as plaintext; it cannot be
// retro-encrypted` about a dataset that was aes-256-gcm/passphrase on the wire —
// wedging the PVC behind the exact statement guardExistingEncryptionPolicy's own
// comment says it exists to prevent.
//
// The unstamped state is production-reachable: controller.go writes the stamp in
// a SEPARATE call after pool.dataset.create returns.
func TestReplayOfEncryptedUnstampedVolumeIsNotCalledPlaintext(t *testing.T) {
	mockClient := truenas.NewMockClient()
	mockClient.ModelQueryProjection = true
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:        NFSConfig{Enabled: true, ShareHost: "192.0.2.10"},
			Encryption: EncryptionConfig{Enabled: true},
		},
		truenasClient: mockClient,
	}
	ctx := context.Background()
	req := &csi.CreateVolumeRequest{
		Name:               "enc-projected-replay",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"encryption": "true"},
		Secrets:            map[string]string{"passphrase": "longenough1"},
	}
	resp, err := d.CreateVolume(ctx, req)
	require.NoError(t, err)
	datasetName, err := d.datasetForID(resp.GetVolume().GetVolumeId())
	require.NoError(t, err)

	// The crash window, reproduced exactly as the drill did (`zfs inherit
	// truenas-csi:encryption`): the dataset is encrypted, the stamp never landed.
	require.NoError(t, mockClient.DatasetRemoveUserProperties(ctx, datasetName, []string{PropEncryption}))
	stripped, err := mockClient.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	require.True(t, stripped.Encrypted, "the wire still says encrypted; only the stamp is gone")
	require.Equal(t, "", datasetLocalUserProperty(stripped, PropEncryption))

	_, replayErr := d.CreateVolume(ctx, req)
	if replayErr != nil {
		assert.NotContains(t, status.Convert(replayErr).Message(), "already exists as plaintext",
			"D-3a: the driver must never call an encrypted dataset plaintext")
		require.NoError(t, replayErr)
	}
	healed, err := mockClient.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	assert.Equal(t, "AES-256-GCM", datasetLocalUserProperty(healed, PropEncryption),
		"the replay repairs the lost stamp instead of wedging the PVC")
}

// TestPublishOfLockedUnstampedEncryptedVolumeUnlocks is D-3b, the fail-OPEN of
// the same class as D-1: on hardware, ControllerPublishVolume on a LOCKED,
// encrypted, unstamped volume returned SUCCESS with ZERO unlock calls, ZERO
// warnings, and left the volume LOCKED — handing the share/extent build a
// device-less zvol, which resurfaces node-side as an unexplained mount error.
func TestPublishOfLockedUnstampedEncryptedVolumeUnlocks(t *testing.T) {
	const name = "pool/parent/enc-projected-locked"
	const volumeID = "enc-projected-locked"
	const passphrase = "unlock-me-123"

	ctx := context.Background()
	d, client := projectionModelingDriver(t)
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: name, Type: "VOLUME", Volsize: 1 << 30,
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &truenas.EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: passphrase},
	})
	require.NoError(t, err) // no PropEncryption stamp: the crash window.
	require.NoError(t, client.DatasetLock(ctx, name))

	// The publish path reads the dataset through the same projected query it uses
	// in production — that read is the whole defect.
	ds, err := client.DatasetGet(ctx, name)
	require.NoError(t, err)
	require.True(t, ds.Encrypted, "the projection must carry the wire truth this decision rests on")
	require.True(t, ds.Locked)
	require.Equal(t, "", datasetLocalUserProperty(ds, PropEncryption))

	client.resetCalls()
	require.NoError(t, d.unlockEncryptedDatasetForPublish(ctx, ds, name, volumeID,
		map[string]string{"passphrase": passphrase}))

	_, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["DatasetUnlock"],
		"D-3b: a locked encrypted volume must be unlocked, never silently published locked")
	after, err := client.DatasetGet(ctx, name)
	require.NoError(t, err)
	assert.False(t, after.Locked, "the volume must not still be locked after a successful publish")

	t.Run("and with no secret it fails CLOSED", func(t *testing.T) {
		require.NoError(t, client.DatasetLock(ctx, name))
		locked, getErr := client.DatasetGet(ctx, name)
		require.NoError(t, getErr)
		failErr := d.unlockEncryptedDatasetForPublish(ctx, locked, name, volumeID, nil)
		require.Error(t, failErr, "silent success on a locked volume is the D-3b fail-open")
		assert.Equal(t, codes.FailedPrecondition, status.Code(failErr))
	})
}

// TestEncryptionIdentityTripwireCanFireOnARealRead is D-3c: the
// warnUnknownEncryptionIdentity tripwire — "the earliest signal that this
// feature's identity discipline has lost its ground truth" — could NEVER fire,
// because it is gated on ds.Encrypted, which the projection always zeroed. A
// tripwire that cannot fire is not a tripwire.
func TestEncryptionIdentityTripwireCanFireOnARealRead(t *testing.T) {
	const name = "pool/parent/enc-projected-tripwire"
	const passphrase = "unlock-me-123"

	ctx := context.Background()
	_, client := projectionModelingDriver(t)
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: name, Type: "VOLUME", Volsize: 1 << 30,
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &truenas.EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: passphrase},
	})
	require.NoError(t, err)
	// The contradiction the tripwire exists for: the backend says encrypted but
	// the pinned identity shape moved and encryption_root did not parse.
	client.MockClient.Datasets[name].EncryptionRoot = ""

	ds, err := client.DatasetGet(ctx, name)
	require.NoError(t, err)
	require.True(t, ds.Encrypted, "without this the tripwire is unreachable — that IS D-3c")

	logged := captureKlog(t, func() {
		assert.False(t, datasetSelfKeyedPassphrase(ds))
	})
	assert.Contains(t, logged, name, "the tripwire must be able to fire on a real read")
	assert.Contains(t, logged, "encryption_root")
}

// TestReconcileUnlocksLockedVolumeUnderTheQueryProjection covers the D-3
// consequence the drill could not observe (the rig ran with reconcile off): the
// unlock reconciler's cheap gate is `state.Locked` off the BATCHED
// pool.dataset.query read, so under the pre-fix projection every locked volume
// looked healthy and the reconciler — the only thing that brings an encrypted
// fleet back after an appliance reboot — was a silent no-op.
func TestReconcileUnlocksLockedVolumeUnderTheQueryProjection(t *testing.T) {
	const volumeID = "enc-projected-reconcile"
	const passphrase = "unlock-me-123"

	d, client, recorder := encryptionReconcileDriver(t,
		encryptionReconcilePV(volumeID), encryptionReconcileSC(), encryptionReconcileSecret(passphrase, ""))
	client.ModelQueryProjection = true
	name := addManagedEncryptedVolume(t, client, volumeID, passphrase)
	require.NoError(t, client.DatasetLock(context.Background(), name))

	client.resetCalls()
	d.reconcileEncryptedUnlocks(context.Background())

	_, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["DatasetUnlock"],
		"the reconciler's locked gate reads a field the projection must deliver")
	unlocked, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	assert.False(t, unlocked.Locked)
	assert.Empty(t, eventsContainingReason(drainEvents(recorder), EventReasonEncryptionUnlockFailed))
}

// TestVolumeConditionSeesALockedVolumeUnderTheQueryProjection covers the other
// silent consequence: the health surface reports a locked volume as abnormal by
// reading the WIRE booleans (ds.Encrypted && ds.Locked) off the same projected
// query. Under the pre-fix projection a locked, dead-I/O volume reported healthy.
func TestVolumeConditionSeesALockedVolumeUnderTheQueryProjection(t *testing.T) {
	const name = "pool/parent/enc-projected-health"
	const passphrase = "unlock-me-123"

	ctx := context.Background()
	_, client := projectionModelingDriver(t)
	createEncryptedDataset(t, client, name, passphrase)
	require.NoError(t, client.DatasetLock(ctx, name))

	ds, err := client.DatasetGet(ctx, name)
	require.NoError(t, err)
	condition := volumeConditionFromDataset(ds)
	require.NotNil(t, condition)
	assert.True(t, condition.GetAbnormal(), "a locked volume serves zero I/O and must not report healthy")
	assert.True(t, strings.Contains(strings.ToLower(condition.GetMessage()), "lock"),
		"the message must name the reason: %q", condition.GetMessage())
}
