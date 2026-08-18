package driver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// TestValidateEncryptionSecret pins the create-time validation rules: ZFS 8-char
// minimum, the P-0 algorithm set, the AES-256-GCM default, and InvalidArgument
// BEFORE any API call.
func TestValidateEncryptionSecret(t *testing.T) {
	t.Run("empty passphrase is rejected", func(t *testing.T) {
		_, err := validateEncryptionSecret(encryptionSecret{})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})
	t.Run("short passphrase is rejected (ZFS 8-char minimum)", func(t *testing.T) {
		_, err := validateEncryptionSecret(encryptionSecret{Passphrase: "short7"})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "8 characters")
	})
	t.Run("unknown algorithm is rejected", func(t *testing.T) {
		_, err := validateEncryptionSecret(encryptionSecret{Passphrase: "longenough1", Algorithm: "ROT13"})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "ROT13")
	})
	t.Run("default algorithm is AES-256-GCM (P-0)", func(t *testing.T) {
		algorithm, err := validateEncryptionSecret(encryptionSecret{Passphrase: "longenough1"})
		require.NoError(t, err)
		assert.Equal(t, "AES-256-GCM", algorithm)
	})
	t.Run("every P-0 algorithm is accepted", func(t *testing.T) {
		for _, algorithm := range []string{"AES-128-CCM", "AES-192-CCM", "AES-256-CCM", "AES-128-GCM", "AES-192-GCM", "AES-256-GCM"} {
			got, err := validateEncryptionSecret(encryptionSecret{Passphrase: "longenough1", Algorithm: algorithm})
			require.NoError(t, err, algorithm)
			assert.Equal(t, algorithm, got)
		}
	})
}

// TestParseEncryptionSecret proves the secret keys are read verbatim and the
// optional overrides parse.
func TestParseEncryptionSecret(t *testing.T) {
	parsed := parseEncryptionSecret(map[string]string{
		"passphrase":         "current-pass-1",
		"passphrasePrevious": "previous-pass-1",
		"algorithm":          "AES-192-GCM",
		"pbkdf2iters":        "310000",
	})
	assert.Equal(t, "current-pass-1", parsed.Passphrase)
	assert.Equal(t, "previous-pass-1", parsed.PassphrasePrevious)
	assert.Equal(t, "AES-192-GCM", parsed.Algorithm)
	assert.Equal(t, 310000, parsed.Pbkdf2Iters)
}

// TestRedactCHAPMasksPassphrase is the R6 guard: a key containing "passphrase"
// must NEVER survive redaction into a log/status/Event/volumeContext.
func TestRedactCHAPMasksPassphrase(t *testing.T) {
	redacted := redactCHAP(map[string]string{
		"passphrase":         "current-pass-1",
		"passphrasePrevious": "previous-pass-1",
		"algorithm":          "AES-256-GCM",
	})
	assert.Equal(t, "***", redacted["passphrase"])
	assert.Equal(t, "***", redacted["passphrasePrevious"])
	assert.Equal(t, "AES-256-GCM", redacted["algorithm"], "non-secret keys are preserved")
	assert.NotContains(t, redacted, "current-pass-1")
	for _, value := range redacted {
		assert.NotEqual(t, "current-pass-1", value)
		assert.NotEqual(t, "previous-pass-1", value)
	}
}

// TestEncryptionEnabledForCreate pins the opt-in gate: zero-value config is OFF;
// otherwise the SC param or a provisioner-secret passphrase opts in.
func TestEncryptionEnabledForCreate(t *testing.T) {
	t.Run("zero-value config is OFF", func(t *testing.T) {
		d := &Driver{config: &Config{}}
		assert.False(t, d.encryptionEnabledForCreate(map[string]string{"encryption": "true"}, map[string]string{"passphrase": "longenough1"}))
	})
	t.Run("enabled + param true", func(t *testing.T) {
		d := &Driver{config: &Config{Encryption: EncryptionConfig{Enabled: true}}}
		assert.True(t, d.encryptionEnabledForCreate(map[string]string{"encryption": "true"}, nil))
	})
	t.Run("enabled + provisioner secret passphrase", func(t *testing.T) {
		d := &Driver{config: &Config{Encryption: EncryptionConfig{Enabled: true}}}
		assert.True(t, d.encryptionEnabledForCreate(nil, map[string]string{"passphrase": "longenough1"}))
	})
	t.Run("enabled but no opt-in signal", func(t *testing.T) {
		d := &Driver{config: &Config{Encryption: EncryptionConfig{Enabled: true}}}
		assert.False(t, d.encryptionEnabledForCreate(nil, nil))
	})
}

// TestGuardExistingEncryptionPolicy proves the encrypted<->plaintext replay
// immutability guard (FailedPrecondition in both directions, nil when matched).
func TestGuardExistingEncryptionPolicy(t *testing.T) {
	ctx := context.Background()
	d := &Driver{config: &Config{}}

	encryptedDS := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{
		PropEncryption: {Value: "AES-256-GCM", Source: "local"},
	}}
	plaintextDS := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{}}

	t.Run("encrypted replay of an encrypted volume is allowed", func(t *testing.T) {
		encCtx := withEncryptionResolution(ctx, &encryptionResolution{Algorithm: "AES-256-GCM", Passphrase: "longenough1"})
		repair, err := d.guardExistingEncryptionPolicy(encCtx, encryptedDS)
		assert.NoError(t, err)
		assert.Empty(t, repair, "a stamped volume needs no repair")
	})
	t.Run("plaintext replay of a plaintext volume is allowed", func(t *testing.T) {
		repair, err := d.guardExistingEncryptionPolicy(ctx, plaintextDS)
		assert.NoError(t, err)
		assert.Empty(t, repair)
	})
	t.Run("plaintext replay of an encrypted volume is refused", func(t *testing.T) {
		_, err := d.guardExistingEncryptionPolicy(ctx, encryptedDS)
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
	t.Run("encrypted replay of a plaintext volume is refused", func(t *testing.T) {
		encCtx := withEncryptionResolution(ctx, &encryptionResolution{Algorithm: "AES-256-GCM", Passphrase: "longenough1"})
		_, err := d.guardExistingEncryptionPolicy(encCtx, plaintextDS)
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
	t.Run("a clone-inherited marker is not the volume's own policy", func(t *testing.T) {
		cloneDS := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{
			PropEncryption: {Value: "AES-256-GCM", Source: "flashstor/origin@snap"},
		}}
		// Source != local => storedEncryptionAlgorithm returns "" => not OUR policy.
		assert.False(t, isEncryptedDataset(cloneDS))
	})

	// F4: the wire fields are the comparand, not the stamp. An encrypted dataset
	// whose stamp write was lost must never be described as plaintext, and an
	// encrypted replay of it must REPAIR the stamp rather than wedge the PVC.
	t.Run("encrypted-on-wire but unstamped: an encrypted replay repairs the stamp", func(t *testing.T) {
		// SELF-KEYED wire shape (P-10): encryption_root is the dataset itself and
		// the key format is PASSPHRASE — i.e. this dataset really does hold its own
		// driver-supplied key.
		unstamped := &truenas.Dataset{
			Name: "pool/parent/vol", Encrypted: true,
			EncryptionRoot: "pool/parent/vol", KeyFormat: truenas.KeyFormatPassphrase,
			UserProperties: map[string]truenas.UserProperty{}}
		encCtx := withEncryptionResolution(ctx, &encryptionResolution{Algorithm: "AES-192-GCM", Passphrase: "longenough1"})
		repair, err := d.guardExistingEncryptionPolicy(encCtx, unstamped)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{PropEncryption: "AES-192-GCM"}, repair)
	})
	t.Run("encrypted-on-wire but unstamped: a plaintext replay fails closed and never says plaintext", func(t *testing.T) {
		unstamped := &truenas.Dataset{
			Name: "pool/parent/vol", Encrypted: true,
			EncryptionRoot: "pool/parent/vol", KeyFormat: truenas.KeyFormatPassphrase,
			UserProperties: map[string]truenas.UserProperty{}}
		_, err := d.guardExistingEncryptionPolicy(ctx, unstamped)
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "ENCRYPTED")
		assert.NotContains(t, err.Error(), "already exists as plaintext",
			"the driver must never describe an encrypted dataset as plaintext")
	})
	t.Run("encrypted-on-wire from a content source is never adopted as its own policy", func(t *testing.T) {
		clone := &truenas.Dataset{
			Name: "pool/parent/clone", Encrypted: true,
			EncryptionRoot: "pool/parent/clone", KeyFormat: truenas.KeyFormatPassphrase,
			UserProperties: map[string]truenas.UserProperty{
				PropVolumeContentSourceType: {Value: "snapshot", Source: "local"},
				PropVolumeContentSourceID:   {Value: "snap-1", Source: "local"},
			}}
		encCtx := withEncryptionResolution(ctx, &encryptionResolution{Algorithm: "AES-256-GCM", Passphrase: "longenough1"})
		repair, err := d.guardExistingEncryptionPolicy(encCtx, clone)
		require.Error(t, err)
		assert.Empty(t, repair)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}

// TestApplyEncryptionToCreateParams proves the create fold emits the P-1/P-2
// shape and is a byte-identical no-op for a plaintext create.
func TestApplyEncryptionToCreateParams(t *testing.T) {
	t.Run("folds the probe shape when a resolution is present", func(t *testing.T) {
		ctx := withEncryptionResolution(context.Background(), &encryptionResolution{Algorithm: "AES-256-GCM", Passphrase: "longenough1", Pbkdf2Iters: 310000})
		params := &truenas.DatasetCreateParams{Name: "pool/parent/v", Type: "VOLUME"}
		applyEncryptionToCreateParams(ctx, params)
		require.NotNil(t, params.Encryption)
		assert.True(t, *params.Encryption)
		require.NotNil(t, params.InheritEncryption)
		assert.False(t, *params.InheritEncryption)
		require.NotNil(t, params.EncryptionOptions)
		assert.Equal(t, "AES-256-GCM", params.EncryptionOptions.Algorithm)
		assert.Equal(t, "longenough1", params.EncryptionOptions.Passphrase)
		assert.Equal(t, 310000, params.EncryptionOptions.Pbkdf2Iters)
	})
	t.Run("no-op for a plaintext create", func(t *testing.T) {
		params := &truenas.DatasetCreateParams{Name: "pool/parent/v", Type: "VOLUME"}
		applyEncryptionToCreateParams(context.Background(), params)
		assert.Nil(t, params.Encryption)
		assert.Nil(t, params.InheritEncryption)
		assert.Nil(t, params.EncryptionOptions)
	})
}

// TestEncryptionPropsStampsOnlyAlgorithm proves the durable stamp carries the
// algorithm marker and NEVER the passphrase.
func TestEncryptionPropsStampsOnlyAlgorithm(t *testing.T) {
	ctx := withEncryptionResolution(context.Background(), &encryptionResolution{Algorithm: "AES-256-GCM", Passphrase: "longenough1"})
	props := encryptionProps(ctx)
	assert.Equal(t, map[string]string{PropEncryption: "AES-256-GCM"}, props)
	for key, value := range props {
		assert.NotEqual(t, "longenough1", value, "the passphrase must never be stamped (key %s)", key)
	}
	assert.Nil(t, encryptionProps(context.Background()), "plaintext create stamps nothing")
}

// TestVolumeConditionFromDatasetLocked proves the E-3 §2 health signal slots into
// the dataset-level layer: a locked encrypted dataset is Abnormal; an unlocked or
// plaintext dataset is not flagged by this arm.
func TestVolumeConditionFromDatasetLocked(t *testing.T) {
	t.Run("locked encrypted dataset is abnormal", func(t *testing.T) {
		ds := &truenas.Dataset{Encrypted: true, Locked: true, KeyLoaded: false,
			UserProperties: map[string]truenas.UserProperty{
				PropManagedResource:  {Value: "true", Source: "local"},
				PropProvisionSuccess: {Value: "true", Source: "local"},
			}}
		condition := volumeConditionFromDataset(ds)
		assert.True(t, condition.GetAbnormal())
		assert.Contains(t, condition.GetMessage(), "locked")
	})
	t.Run("unlocked encrypted dataset is normal", func(t *testing.T) {
		ds := &truenas.Dataset{Encrypted: true, Locked: false, KeyLoaded: true,
			UserProperties: map[string]truenas.UserProperty{
				PropManagedResource:  {Value: "true", Source: "local"},
				PropProvisionSuccess: {Value: "true", Source: "local"},
			}}
		assert.False(t, volumeConditionFromDataset(ds).GetAbnormal())
	})
	t.Run("plaintext dataset never takes the encryption arm", func(t *testing.T) {
		ds := &truenas.Dataset{Encrypted: false, Locked: false,
			UserProperties: map[string]truenas.UserProperty{
				PropManagedResource:  {Value: "true", Source: "local"},
				PropProvisionSuccess: {Value: "true", Source: "local"},
			}}
		assert.False(t, volumeConditionFromDataset(ds).GetAbnormal())
	})
}

// encryptionTestDriver builds a controller-side driver wired to a call-counting
// mock with encryption enabled, for the publish-unlock tests.
func encryptionTestDriver() (*Driver, *apiCallCountingClient) {
	client := newAPICallCountingClient()
	d := &Driver{
		name:          "org.scale.csi.nfs",
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}, Encryption: EncryptionConfig{Enabled: true}},
		truenasClient: client,
	}
	return d, client
}

func createEncryptedDataset(t *testing.T, client *apiCallCountingClient, name, passphrase string) {
	t.Helper()
	_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: name, Type: "VOLUME", Volsize: 1 << 30,
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &truenas.EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: passphrase},
	})
	require.NoError(t, err)
	// Stamp the durable marker the driver reads to know the volume is encrypted.
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), name, PropEncryption, "AES-256-GCM"))
}

// TestUnlockEncryptedDatasetForPublish covers the E-2 §2 / E-3 §1 publish flow:
// the P-8 gate, fail-closed behavior, and the two-key rotation window.
func TestUnlockEncryptedDatasetForPublish(t *testing.T) {
	const name = "pool/parent/enc-vol"
	const volumeID = "enc-vol"

	t.Run("plaintext volume is a no-op", func(t *testing.T) {
		d, client := encryptionTestDriver()
		_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{Name: name, Type: "VOLUME", Volsize: 1 << 30})
		require.NoError(t, err)
		ds, _ := client.DatasetGet(context.Background(), name)
		client.resetCalls()
		require.NoError(t, d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, nil))
		_, methods := client.callSnapshot()
		assert.Zero(t, methods["DatasetEncryptionSummary"], "no summary call for a plaintext volume")
		assert.Zero(t, methods["DatasetUnlock"])
	})

	t.Run("P-8 gate: an unlocked encrypted volume issues no unlock call", func(t *testing.T) {
		d, client := encryptionTestDriver()
		createEncryptedDataset(t, client, name, "longenough1")
		ds, _ := client.DatasetGet(context.Background(), name)
		client.resetCalls()
		require.NoError(t, d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, map[string]string{"passphrase": "longenough1"}))
		_, methods := client.callSnapshot()
		assert.Equal(t, 1, methods["DatasetEncryptionSummary"], "the gate reads the summary")
		assert.Zero(t, methods["DatasetUnlock"], "P-8: unlock is NOT called on an already-unlocked dataset")
	})

	t.Run("fail-closed: locked volume with no secret", func(t *testing.T) {
		d, client := encryptionTestDriver()
		createEncryptedDataset(t, client, name, "longenough1")
		require.NoError(t, client.DatasetLock(context.Background(), name))
		ds, _ := client.DatasetGet(context.Background(), name)
		err := d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, nil)
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	// D-1: a wrong passphrase is a SUCCESSFUL unlock job whose RESULT payload
	// carries the failure (live drill, 2026-08-02). The client now asserts on that
	// payload; this test pins what the driver must do with it — fail CLOSED, and
	// say why, using the backend's own scrubbed reason.
	t.Run("fail-closed: locked volume with wrong passphrase (D-1 payload)", func(t *testing.T) {
		d, client := encryptionTestDriver()
		createEncryptedDataset(t, client, name, "longenough1")
		require.NoError(t, client.DatasetLock(context.Background(), name))
		ds, _ := client.DatasetGet(context.Background(), name)
		err := d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, map[string]string{"passphrase": "wrong-pass-9"})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "Invalid Key",
			"the backend's own reason from the unlock result payload reaches the operator")
		assert.NotContains(t, err.Error(), "wrong-pass-9", "and never the passphrase itself")
		locked, _ := client.DatasetGet(context.Background(), name)
		assert.True(t, locked.Locked, "the dataset stays locked (ZFS-level guarantee, drill-verified)")
	})

	t.Run("correct passphrase unlocks", func(t *testing.T) {
		d, client := encryptionTestDriver()
		createEncryptedDataset(t, client, name, "longenough1")
		require.NoError(t, client.DatasetLock(context.Background(), name))
		ds, _ := client.DatasetGet(context.Background(), name)
		require.NoError(t, d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, map[string]string{"passphrase": "longenough1"}))
		unlocked, _ := client.DatasetGet(context.Background(), name)
		assert.False(t, unlocked.Locked)
	})

	t.Run("rotation: previous passphrase unlocks then change_key (E-3 §1)", func(t *testing.T) {
		d, client := encryptionTestDriver()
		createEncryptedDataset(t, client, name, "old-pass-123")
		require.NoError(t, client.DatasetLock(context.Background(), name))
		ds, _ := client.DatasetGet(context.Background(), name)
		client.resetCalls()
		// Current passphrase is the NEW one; the dataset still holds the OLD one.
		secrets := map[string]string{"passphrase": "new-pass-456", "passphrasePrevious": "old-pass-123"}
		require.NoError(t, d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, secrets))
		_, methods := client.callSnapshot()
		// D-1: this arm is only REACHABLE because the client now reports a
		// wrong-key unlock (from the job's result payload) instead of returning nil.
		// Two unlock calls prove the branch ran: current (reported failed), then
		// previous (succeeded).
		assert.Equal(t, 2, methods["DatasetUnlock"], "the rotation branch is reached: current fails, previous unlocks")
		assert.Equal(t, 1, methods["DatasetChangeKey"], "rotation re-keys to the current passphrase")
		// The dataset now holds the new key: the old one no longer unlocks.
		require.NoError(t, client.DatasetLock(context.Background(), name))
		require.Error(t, client.DatasetUnlock(context.Background(), name, "old-pass-123"))
		require.NoError(t, client.DatasetUnlock(context.Background(), name, "new-pass-456"))
	})

	t.Run("rotation is idempotent by outcome: a replay lands on current-succeeds", func(t *testing.T) {
		d, client := encryptionTestDriver()
		createEncryptedDataset(t, client, name, "old-pass-123")
		require.NoError(t, client.DatasetLock(context.Background(), name))
		ds, _ := client.DatasetGet(context.Background(), name)
		secrets := map[string]string{"passphrase": "new-pass-456", "passphrasePrevious": "old-pass-123"}
		require.NoError(t, d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, secrets))
		// Re-lock and replay: the current passphrase now succeeds, no change_key.
		require.NoError(t, client.DatasetLock(context.Background(), name))
		ds, _ = client.DatasetGet(context.Background(), name)
		client.resetCalls()
		require.NoError(t, d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, secrets))
		_, methods := client.callSnapshot()
		assert.Zero(t, methods["DatasetChangeKey"], "already rotated: the current passphrase unlocks directly")
	})
}

// TestCreateVolumeEncryptedFoldsAndStamps is the end-to-end create proof: an
// opted-in NFS create produces an encrypted dataset stamped with the algorithm
// marker (never the passphrase) and a volume-context marker.
func TestCreateVolumeEncryptedFoldsAndStamps(t *testing.T) {
	mockClient := truenas.NewMockClient()
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
	resp, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "enc-vol",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"encryption": "true"},
		Secrets:            map[string]string{"passphrase": "longenough1"},
	})
	require.NoError(t, err)
	volumeID := resp.GetVolume().GetVolumeId()
	datasetName, err := d.datasetForID(volumeID)
	require.NoError(t, err)

	ds, err := mockClient.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	assert.True(t, ds.Encrypted, "the create folded encryption into pool.dataset.create")
	assert.Equal(t, "AES-256-GCM", datasetLocalUserProperty(ds, PropEncryption), "the algorithm marker is stamped")

	// The passphrase is radioactive: it must appear nowhere durable on the dataset.
	for key, prop := range ds.UserProperties {
		assert.NotEqual(t, "longenough1", prop.Value, "passphrase leaked into user property %s", key)
	}
	// The volume context carries only the algorithm marker.
	assert.Equal(t, "AES-256-GCM", resp.GetVolume().GetVolumeContext()[volumeContextEncryptionKey])
	assert.NotContains(t, resp.GetVolume().GetVolumeContext(), "longenough1")
}

// TestCreateVolumeEncryptionRejectsContentSource proves encryption is create-time
// only: combining it with a content source is refused before any mutation (E-4).
func TestCreateVolumeEncryptionRejectsContentSource(t *testing.T) {
	mockClient := truenas.NewMockClient()
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
	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "enc-clone",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"encryption": "true"},
		Secrets:            map[string]string{"passphrase": "longenough1"},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "pool/parent/src@snap"},
		}},
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// TestDeleteVolumeLockedDestroysCleanly is the E-4 guarantee: DeleteVolume of a
// LOCKED encrypted dataset succeeds (ZFS destroy needs no key, P-4) and issues NO
// unlock or encryption_summary call on the delete path.
func TestDeleteVolumeLockedDestroysCleanly(t *testing.T) {
	d, client := encryptionTestDriver()
	const volumeID = "enc-vol"
	const name = "pool/parent/" + volumeID
	createEncryptedDataset(t, client, name, "longenough1")
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), name, PropManagedResource, "true"))
	require.NoError(t, client.DatasetLock(context.Background(), name))
	locked, _ := client.DatasetGet(context.Background(), name)
	require.True(t, locked.Locked)

	client.resetCalls()
	_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: volumeID})
	require.NoError(t, err, "destroy needs no key; a locked dataset deletes cleanly")

	_, getErr := client.DatasetGet(context.Background(), name)
	require.Error(t, getErr, "the dataset is gone")

	_, methods := client.callSnapshot()
	assert.Zero(t, methods["DatasetUnlock"], "E-4: the delete path never unlocks")
	assert.Zero(t, methods["DatasetEncryptionSummary"], "E-4: the delete path never reads the summary")
}

// TestReaperSweepToleratesLocked proves the reconcile dataset walk treats a locked
// encrypted dataset as a normal state: it is still listed (queryable, P-4) and
// still destroyable by name with no unlock — so a tombstone/remnant sweep is never
// wedged by the locked state.
func TestReaperSweepToleratesLocked(t *testing.T) {
	d, client := encryptionTestDriver()
	const name = "pool/parent/locked-reaper-vol"
	createEncryptedDataset(t, client, name, "longenough1")
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), name, PropManagedResource, "true"))
	require.NoError(t, client.DatasetLock(context.Background(), name))

	// The managed-dataset walk the sweeps use still sees the locked dataset.
	datasets, err := d.listAllManagedDatasets(context.Background())
	require.NoError(t, err)
	var found bool
	for _, ds := range datasets {
		if ds.Name == name {
			found = true
		}
	}
	assert.True(t, found, "a locked dataset is still queryable and listed (P-4)")
	// The lock STATE is not readable from that listing: it comes from
	// zfs.resource.query, which parseDatasetResource does not read
	// encrypted/locked/key_loaded from. The source-bearing pool.dataset.query read
	// is where the state lives (P-4).
	listedThroughQuery, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	assert.True(t, listedThroughQuery.Locked, "locked is a normal queryable state, not an error")

	// Destroy by name succeeds with no key and no unlock call.
	client.resetCalls()
	require.NoError(t, client.DatasetDelete(context.Background(), name, true, true))
	_, methods := client.callSnapshot()
	assert.Zero(t, methods["DatasetUnlock"])
}

// TestCreateVolumeEncryptionValidationBeforeMutation proves a bad secret fails
// fast with InvalidArgument and creates nothing.
func TestCreateVolumeEncryptionValidationBeforeMutation(t *testing.T) {
	mockClient := truenas.NewMockClient()
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
	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "enc-bad",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"encryption": "true"},
		Secrets:            map[string]string{"passphrase": "short7", "algorithm": "ROT13"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Empty(t, mockClient.Datasets, "validation fails before any dataset is created")
}

// encryptionFaultClient injects backend job failures on the encryption methods,
// which the mock alone cannot produce (its unlock/change_key only fail for the
// probed reasons). failChangeKeyTimes fails the first N change_key calls, which
// is how a crash/FAILED-job mid-rotation is reproduced.
type encryptionFaultClient struct {
	*apiCallCountingClient
	changeKeyErr       error
	failChangeKeyTimes int
	changeKeyCalls     int
	summaryErr         error
}

func (c *encryptionFaultClient) DatasetChangeKey(ctx context.Context, name, passphrase string) error {
	c.changeKeyCalls++
	if c.changeKeyErr != nil && c.changeKeyCalls <= c.failChangeKeyTimes {
		return c.changeKeyErr
	}
	return c.apiCallCountingClient.DatasetChangeKey(ctx, name, passphrase)
}

func (c *encryptionFaultClient) DatasetEncryptionSummary(ctx context.Context, name string) ([]truenas.EncryptionSummaryEntry, error) {
	if c.summaryErr != nil {
		return nil, c.summaryErr
	}
	return c.apiCallCountingClient.DatasetEncryptionSummary(ctx, name)
}

// TestPublishConvergesAbandonedRotation is the F2 regression, publish side. A
// change_key that FAILS (or a controller killed at the same point) leaves the
// dataset UNLOCKED and still keyed to the OLD passphrase. The old code read the
// summary, saw locked==false and returned success — forever — so the operator
// dropped passphrasePrevious believing the rotation had completed and lost the
// data (R2).
//
// PRE-FIX PROOF: with the P-8 short-circuit returning nil before the rotation
// state is examined, the retry below returns NoError with DatasetChangeKey count
// 0 and the OLD passphrase still unlocking the volume.
func TestPublishConvergesAbandonedRotation(t *testing.T) {
	const name = "pool/parent/enc-rot"
	const volumeID = "enc-rot"
	const oldPass = "old-pass-123"
	const newPass = "new-pass-456"

	base, client := encryptionTestDriver()
	faults := &encryptionFaultClient{
		apiCallCountingClient: client,
		changeKeyErr:          fmt.Errorf("pool.dataset.change_key job 42 failed: FAILED: backend exploded"),
		failChangeKeyTimes:    1,
	}
	base.truenasClient = faults
	base.eventRecorder = &EventRecorder{recorder: record.NewFakeRecorder(16), enabled: true}

	createEncryptedDataset(t, client, name, oldPass)
	require.NoError(t, client.DatasetLock(context.Background(), name))
	ds, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)

	secrets := map[string]string{"passphrase": newPass, "passphrasePrevious": oldPass}

	// First publish: unlock(previous) succeeds, change_key FAILS -> error, and the
	// dataset is left unlocked on the OLD key.
	firstErr := base.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, secrets)
	require.Error(t, firstErr, "an abandoned rotation must never report success")
	events := drainEvents(base.eventRecorder.recorder.(*record.FakeRecorder))
	assert.Equal(t, 1, eventsContainingReason(events, EventReasonEncryptionRotationIncomplete),
		"the operator must be told to keep passphrasePrevious")
	for _, event := range events {
		assert.NotContains(t, event, oldPass)
		assert.NotContains(t, event, newPass)
	}

	// Retry publish on the now-UNLOCKED, half-rotated volume: it must converge.
	ds, err = client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	require.False(t, ds.Locked, "the abandoned rotation left the dataset unlocked")
	require.NoError(t, base.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, secrets))

	// The key really moved to the current passphrase.
	require.NoError(t, client.DatasetLock(context.Background(), name))
	assert.Error(t, client.DatasetUnlock(context.Background(), name, oldPass), "the old key must be dead")
	assert.NoError(t, client.DatasetUnlock(context.Background(), name, newPass))
}

// TestPublishRotationCompletionIsIdempotent proves the unconditional
// change_key arm is safe: re-keying an already-current dataset to the SAME
// passphrase succeeds and leaves that key valid (probed live 2026-08-02), so a
// publish inside an open window converges without ever invalidating the key.
func TestPublishRotationCompletionIsIdempotent(t *testing.T) {
	const name = "pool/parent/enc-same"
	const volumeID = "enc-same"
	const oldPass = "old-pass-123"
	const newPass = "new-pass-456"

	d, client := encryptionTestDriver()
	createEncryptedDataset(t, client, name, newPass) // already on the CURRENT key
	ds, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)

	secrets := map[string]string{"passphrase": newPass, "passphrasePrevious": oldPass}
	client.resetCalls()
	require.NoError(t, d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, secrets))

	_, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["DatasetChangeKey"], "the open window is converged with one same-key re-key")
	assert.Zero(t, methods["DatasetUnlock"], "P-8: an unlocked dataset is never unlocked")

	// The key is still valid after the same-key change_key.
	require.NoError(t, client.DatasetLock(context.Background(), name))
	require.NoError(t, client.DatasetUnlock(context.Background(), name, newPass))
}

// TestPublishUnlocksEncryptedUnstampedDataset is the F4 regression, publish side:
// a controller killed between pool.dataset.create and the stamp write leaves an
// ENCRYPTED dataset with no stamp. Reading the stamp alone made publish a silent
// no-op that then failed opaquely in WaitForZvolReady with no mention of
// encryption.
//
// PRE-FIX PROOF: with `if !isEncryptedDataset(ds) { return nil }` the call below
// returns NoError having issued zero unlock calls, leaving the dataset locked.
func TestPublishUnlocksEncryptedUnstampedDataset(t *testing.T) {
	const name = "pool/parent/enc-unstamped"
	const volumeID = "enc-unstamped"
	const passphrase = "unlock-me-123"

	d, client := encryptionTestDriver()
	_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: name, Type: "VOLUME", Volsize: 1 << 30,
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &truenas.EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: passphrase},
	})
	require.NoError(t, err) // NOTE: no PropEncryption stamp — the crash window.
	require.NoError(t, client.DatasetLock(context.Background(), name))
	ds, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	require.True(t, ds.Encrypted)
	require.Equal(t, "", datasetLocalUserProperty(ds, PropEncryption), "no stamp: the crash window")

	client.resetCalls()
	require.NoError(t, d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID,
		map[string]string{"passphrase": passphrase}))

	_, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["DatasetUnlock"], "wire truth drives the unlock, not the stamp")
	unlocked, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	assert.False(t, unlocked.Locked)

	// And with no secret it fails CLOSED naming encryption, not a device timeout.
	require.NoError(t, client.DatasetLock(context.Background(), name))
	ds, err = client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	failErr := d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, nil)
	require.Error(t, failErr)
	assert.Equal(t, codes.FailedPrecondition, status.Code(failErr))
}

// TestEncryptionErrorsAreRedacted is the F6 regression: the driver FORWARDS
// backend job text it did not compose, and the passphrase is an ARGUMENT of
// pool.dataset.unlock / change_key, so a middleware traceback can echo it. Every
// forwarded string must be scrubbed before it reaches a gRPC status, a log or an
// Event.
//
// PRE-FIX PROOF: with `%v` of the raw error at encryption.go's change_key and
// summary arms, the returned error contains the passphrase verbatim.
func TestEncryptionErrorsAreRedacted(t *testing.T) {
	const name = "pool/parent/enc-leak"
	const volumeID = "enc-leak"
	const oldPass = "old-pass-123"
	const newPass = "new-pass-456"

	t.Run("change_key failure text", func(t *testing.T) {
		base, client := encryptionTestDriver()
		faults := &encryptionFaultClient{
			apiCallCountingClient: client,
			// The shape a middleware traceback that echoes call arguments produces.
			changeKeyErr: fmt.Errorf("pool.dataset.change_key job 7 failed: FAILED: "+
				"CallError: change_key(%q, {'passphrase': %q})", name, newPass),
			failChangeKeyTimes: 1,
		}
		base.truenasClient = faults
		createEncryptedDataset(t, client, name, oldPass)
		require.NoError(t, client.DatasetLock(context.Background(), name))
		ds, err := client.DatasetGet(context.Background(), name)
		require.NoError(t, err)

		unlockErr := base.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID,
			map[string]string{"passphrase": newPass, "passphrasePrevious": oldPass})
		require.Error(t, unlockErr)
		assert.NotContains(t, unlockErr.Error(), newPass, "the passphrase must not survive into the gRPC status")
		assert.NotContains(t, unlockErr.Error(), oldPass)
		assert.Contains(t, unlockErr.Error(), "***", "the forwarded text is masked, not dropped silently")
	})

	t.Run("encryption_summary failure text", func(t *testing.T) {
		base, client := encryptionTestDriver()
		faults := &encryptionFaultClient{
			apiCallCountingClient: client,
			summaryErr:            fmt.Errorf("encryption_summary failed: passphrase=%q rejected", newPass),
		}
		base.truenasClient = faults
		createEncryptedDataset(t, client, name, newPass)
		ds, err := client.DatasetGet(context.Background(), name)
		require.NoError(t, err)

		summaryErr := base.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID,
			map[string]string{"passphrase": newPass})
		require.Error(t, summaryErr)
		assert.NotContains(t, summaryErr.Error(), newPass)
	})

	t.Run("redactEncryptionSecrets masks every supplied key and ignores empties", func(t *testing.T) {
		text := redactEncryptionSecrets("unlock failed for s3cret-one and s3cret-two", "s3cret-one", "", "s3cret-two")
		assert.Equal(t, "unlock failed for *** and ***", text)
		assert.Equal(t, "", redactEncryptionError(nil, "s3cret-one"))
	})
}

// TestEncryptionSummaryLockedFailsClosed is the F17 regression: a summary that
// names no matching row must be an ERROR, not a guess. Reading a CHILD's lock
// state (or defaulting an empty result to "unlocked") silently skips the unlock
// of a locked volume.
//
// PRE-FIX PROOF: with the summary[0] fallback and the empty->false default, both
// sub-cases return (false, nil) and the publish path skips the unlock.
func TestEncryptionSummaryLockedFailsClosed(t *testing.T) {
	const name = "pool/parent/enc-vol"

	t.Run("exact row match is authoritative", func(t *testing.T) {
		locked, err := encryptionSummaryLocked([]truenas.EncryptionSummaryEntry{
			{Name: name + "/child", Locked: false},
			{Name: name, Locked: true},
		}, name)
		require.NoError(t, err)
		assert.True(t, locked)
	})
	t.Run("no matching row is an error, never a child's state", func(t *testing.T) {
		_, err := encryptionSummaryLocked([]truenas.EncryptionSummaryEntry{
			{Name: name + "/child", Locked: false},
		}, name)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no matching row")
	})
	t.Run("an empty summary is an error, not 'unlocked'", func(t *testing.T) {
		_, err := encryptionSummaryLocked(nil, name)
		require.Error(t, err)
	})
}

// TestCreateVolumeRefusesEncryptedContentSource is the F5 regression: the
// destination-side refusal only covered an encrypted StorageClass. Restoring an
// encrypted volume's snapshot into a PLAINTEXT class produced (per P-7) a clone
// that is encrypted:true with encryption_root == the origin and NO stamp of its
// own — invisible to publish and to the reconciler, dead I/O after the first
// reboot, with no recovery path in the driver.
//
// PRE-FIX PROOF: without the source-side guard both sub-cases return NoError and
// create a dataset whose Encrypted is true with no local encryption stamp.
func TestCreateVolumeRefusesEncryptedContentSource(t *testing.T) {
	newDriver := func(t *testing.T) (*Driver, *truenas.MockClient) {
		t.Helper()
		mockClient := truenas.NewMockClient()
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
		return d, mockClient
	}

	// Provision a real encrypted source volume through CreateVolume.
	createEncryptedSource := func(t *testing.T, d *Driver) string {
		t.Helper()
		resp, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "enc-source",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			Parameters:         map[string]string{"encryption": "true"},
			Secrets:            map[string]string{"passphrase": "longenough1"},
		})
		require.NoError(t, err)
		return resp.GetVolume().GetVolumeId()
	}

	t.Run("snapshot source", func(t *testing.T) {
		d, mockClient := newDriver(t)
		sourceID := createEncryptedSource(t, d)
		snapResp, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
			SourceVolumeId: sourceID,
			Name:           "enc-snap",
		})
		require.NoError(t, err)

		before := len(mockClient.Datasets)
		_, err = d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "restored-plaintext",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			// A PLAINTEXT StorageClass: no encryption parameter, no secret.
			VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapResp.GetSnapshot().GetSnapshotId()},
			}},
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Len(t, mockClient.Datasets, before, "the refusal precedes any mutation")
	})

	t.Run("volume source", func(t *testing.T) {
		d, mockClient := newDriver(t)
		sourceID := createEncryptedSource(t, d)

		before := len(mockClient.Datasets)
		_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "cloned-plaintext",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: sourceID},
			}},
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Len(t, mockClient.Datasets, before, "the refusal precedes any mutation")
	})

	t.Run("a plaintext source is unaffected", func(t *testing.T) {
		d, mockClient := newDriver(t)
		// The clone path writes an in-flight marker on the parent dataset.
		_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
			Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, err)
		resp, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "plain-source",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		})
		require.NoError(t, err)
		_, err = d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "plain-clone",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: resp.GetVolume().GetVolumeId()},
			}},
		})
		require.NoError(t, err, "encryption enabled must not change plaintext clone behavior")
	})
}

// TestCreateVolumeRepairsLostEncryptionStamp is the F4 regression, create side:
// the replay of a create whose stamp write was lost must REPAIR the stamp, not
// declare the encrypted volume plaintext and wedge the PVC forever.
//
// PRE-FIX PROOF: with the stamp-only guard, this replay returns
// FailedPrecondition containing "already exists as plaintext".
func TestCreateVolumeRepairsLostEncryptionStamp(t *testing.T) {
	mockClient := truenas.NewMockClient()
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
		Name:               "enc-replay",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"encryption": "true"},
		Secrets:            map[string]string{"passphrase": "longenough1"},
	}
	resp, err := d.CreateVolume(ctx, req)
	require.NoError(t, err)
	datasetName, err := d.datasetForID(resp.GetVolume().GetVolumeId())
	require.NoError(t, err)

	// Reproduce the crash window: the dataset is encrypted, the stamp never landed.
	require.NoError(t, mockClient.DatasetRemoveUserProperties(ctx, datasetName, []string{PropEncryption}))
	stripped, err := mockClient.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	require.True(t, stripped.Encrypted)
	require.Equal(t, "", datasetLocalUserProperty(stripped, PropEncryption))

	// The replay repairs it instead of wedging.
	_, err = d.CreateVolume(ctx, req)
	require.NoError(t, err, "an encrypted replay of an unstamped encrypted dataset must heal it")
	healed, err := mockClient.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	assert.Equal(t, "AES-256-GCM", datasetLocalUserProperty(healed, PropEncryption), "the stamp is repaired")
}

// TestConflictingEncryptionReplayWritesNothing is the F15 regression: the
// encryption immutability guard must run BEFORE any write. It used to run after
// setDatasetUserProperties had already stamped managed_resource /
// provision_success / csi_volume_name onto the conflicting replay's dataset —
// which is precisely what takes a wedged, unreclaimable volume out of the
// remnant sweeper's reach (it now looks like a finished, owned volume).
//
// The dataset here is the crash remnant that makes the ordering observable: an
// ENCRYPTED dataset that exists but was never stamped managed/provisioned.
//
// PRE-FIX PROOF: with the guard moved back below the property update, the replay
// still fails but issues a DatasetSetUserProperties call first, leaving
// managed_resource/provision_success on the dataset.
func TestConflictingEncryptionReplayWritesNothing(t *testing.T) {
	d, client := encryptionTestDriver()
	d.config.DriverName = "org.scale.csi.nfs"
	d.config.NFS = NFSConfig{Enabled: true, ShareHost: "192.0.2.10"}
	ctx := context.Background()

	const volumeID = "enc-conflict"
	const datasetName = "pool/parent/" + volumeID
	// An encrypted dataset owned by THIS driver instance whose provisioning
	// stamps were never completed: the replay therefore has real property
	// updates to write, which is what makes the guard's ordering observable.
	_, createErr := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: datasetName, Type: "FILESYSTEM",
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &truenas.EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "longenough1"},
	})
	require.NoError(t, createErr)
	require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropEncryption, "AES-256-GCM"))
	require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropDriverInstanceID, d.driverInstanceID()))

	client.resetCalls()
	// Replay the same volume name through a PLAINTEXT class.
	_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               volumeID,
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, methods := client.callSnapshot()
	assert.Zero(t, methods["DatasetSetUserProperties"], "a conflicting replay must not write first")
	assert.Zero(t, methods["DatasetUpdate"], "a conflicting replay must not write first")

	after, getErr := client.DatasetGet(ctx, datasetName)
	require.NoError(t, getErr)
	assert.Equal(t, "", datasetUserProperty(after, PropManagedResource),
		"the refused replay must not leave ownership stamps behind")
	assert.Equal(t, "", datasetUserProperty(after, PropProvisionSuccess))
}

// TestPublishRotatesOncePerWindow is the N-1 regression. The unlocked+window-open
// arm calls change_key on EVERY publish, and change_key at the default
// pbkdf2iters (1,300,000, P-1) is a CPU-heavy appliance job: a node reboot that
// re-publishes a fleet, or a flapping pod, with a window the operator forgot to
// close, becomes a change_key storm plus a stream of "Rotated" Events that hide
// the real rotation. The reconciler already had a once-per-window fingerprint
// guard; publish now shares it.
//
// PRE-FIX PROOF: without the shared guard, five publishes issue five change_key
// calls and five EncryptionRotated Events.
func TestPublishRotatesOncePerWindow(t *testing.T) {
	const name = "pool/parent/enc-window"
	const volumeID = "enc-window"
	const oldPass = "old-pass-123"
	const newPass = "new-pass-456"

	d, client := encryptionTestDriver()
	recorder := record.NewFakeRecorder(32)
	d.eventRecorder = &EventRecorder{recorder: recorder, enabled: true}
	createEncryptedDataset(t, client, name, oldPass)
	ds, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)

	secrets := map[string]string{"passphrase": newPass, "passphrasePrevious": oldPass}
	client.resetCalls()
	for publish := 0; publish < 5; publish++ {
		require.NoError(t, d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, secrets))
	}

	_, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["DatasetChangeKey"], "the window converges once, not once per publish")
	assert.Equal(t, 1, eventsContainingReason(drainEvents(recorder), EventReasonEncryptionRotated))

	// The rotation really landed, and a NEW window (different keys) converges again.
	require.NoError(t, client.DatasetLock(context.Background(), name))
	require.NoError(t, client.DatasetUnlock(context.Background(), name, newPass))
	ds, err = client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	client.resetCalls()
	require.NoError(t, d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID,
		map[string]string{"passphrase": "third-pass-789", "passphrasePrevious": newPass}))
	_, methods = client.callSnapshot()
	assert.Equal(t, 1, methods["DatasetChangeKey"], "a NEW window is not suppressed by the previous one")
}

// TestPublishDoesNotRekeyInheritedKeyVolume is the N-2 regression, an
// availability one. A P-7 clone is encrypted with its ORIGIN's key and has no
// encryption policy of its own. Once a rotation window is open, publish used to
// take the rotation arm for it, change_key was refused by the backend ("not an
// encryption root"), and a HEALTHY, unlocked, serving volume failed to attach
// with an Internal error telling the operator to keep a key that has nothing to
// do with the problem.
//
// PRE-FIX PROOF: without the ownership check, this publish returns an error with
// 1 change_key attempted and an EncryptionRotationIncomplete Event.
func TestPublishDoesNotRekeyInheritedKeyVolume(t *testing.T) {
	const origin = "pool/parent/enc-origin"
	const clone = "pool/parent/enc-clone"
	const passphrase = "origin-pass-123"
	const newPass = "new-pass-456"

	d, client := encryptionTestDriver()
	recorder := record.NewFakeRecorder(16)
	d.eventRecorder = &EventRecorder{recorder: recorder, enabled: true}
	ctx := context.Background()

	createEncryptedDataset(t, client, origin, passphrase)
	_, err := client.SnapshotCreate(ctx, origin, "snap", nil)
	require.NoError(t, err)
	require.NoError(t, client.SnapshotClone(ctx, origin+"@snap", clone))
	cloneDS, err := client.DatasetGet(ctx, clone)
	require.NoError(t, err)
	require.True(t, cloneDS.Encrypted, "P-7: the clone is encrypted")
	require.False(t, cloneDS.Locked, "and it is unlocked and serving")
	require.Equal(t, "", datasetLocalUserProperty(cloneDS, PropEncryption), "its stamp is inherited, not local")

	client.resetCalls()
	err = d.unlockEncryptedDatasetForPublish(ctx, cloneDS, clone, "enc-clone",
		map[string]string{"passphrase": newPass, "passphrasePrevious": passphrase})
	require.NoError(t, err, "a healthy inherited-key volume must still publish")

	_, methods := client.callSnapshot()
	assert.Zero(t, methods["DatasetChangeKey"], "an inherited key is not this volume's to re-key")
	events := drainEvents(recorder)
	assert.Zero(t, eventsContainingReason(events, EventReasonEncryptionRotationIncomplete),
		"no spurious keep-passphrasePrevious warning on a volume the window does not apply to")
}

// TestEncryptedContentSourceRefusedWithFeatureFlagOff is the F5-round-2
// regression. The hazard is a property of the DATA, not of the feature flag:
// flipping encryption.enabled to false (a rollback, a values regression) while
// encrypted volumes exist must not re-open cloning an encrypted volume into a
// class that can never unlock the result — and that is the configuration in
// which the resulting dead volume is least likely to be noticed.
//
// PRE-FIX PROOF: with `if !d.config.Encryption.Enabled { return nil }` at the top
// of guardEncryptedContentSource and no post-create backstop, both requests below
// SUCCEED and leave a dataset with Encrypted=true and no local encryption stamp.
func TestEncryptedContentSourceRefusedWithFeatureFlagOff(t *testing.T) {
	newDriverWithEncryptedSource := func(t *testing.T) (*Driver, *truenas.MockClient, string) {
		t.Helper()
		mockClient := truenas.NewMockClient()
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
		resp, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "enc-source",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			Parameters:         map[string]string{"encryption": "true"},
			Secrets:            map[string]string{"passphrase": "longenough1"},
		})
		require.NoError(t, err)
		// The operator now turns the feature OFF while the encrypted volume lives.
		d.config.Encryption.Enabled = false
		return d, mockClient, resp.GetVolume().GetVolumeId()
	}

	assertNoUnmanageableEncryptedVolume := func(t *testing.T, mockClient *truenas.MockClient, datasetName string) {
		t.Helper()
		ds, err := mockClient.DatasetGet(context.Background(), datasetName)
		if err != nil {
			return // destroyed: the refusal rolled it back
		}
		require.Falsef(t, ds.Encrypted && datasetLocalUserProperty(ds, PropEncryption) == "",
			"%s is encrypted with no encryption policy of its own — the driver could never unlock it", datasetName)
	}

	t.Run("volume source", func(t *testing.T) {
		d, mockClient, sourceID := newDriverWithEncryptedSource(t)
		_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "cloned-flag-off",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: sourceID},
			}},
		})
		require.Error(t, err, "cloning an encrypted volume is refused even with the feature flag off")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assertNoUnmanageableEncryptedVolume(t, mockClient, "pool/parent/cloned-flag-off")
	})

	t.Run("snapshot source", func(t *testing.T) {
		d, mockClient, sourceID := newDriverWithEncryptedSource(t)
		d.config.Encryption.Enabled = true // snapshot creation itself is unrelated
		snapResp, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
			SourceVolumeId: sourceID, Name: "enc-snap",
		})
		require.NoError(t, err)
		d.config.Encryption.Enabled = false
		// The clone path writes an in-flight marker on the parent dataset.
		_, parentErr := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
			Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, parentErr)

		_, err = d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "restored-flag-off",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapResp.GetSnapshot().GetSnapshotId()},
			}},
		})
		require.Error(t, err, "restoring from an encrypted snapshot is refused even with the feature flag off")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assertNoUnmanageableEncryptedVolume(t, mockClient, "pool/parent/restored-flag-off")
	})
}

// encryptedParentDriver builds a deployment whose PARENT dataset is encrypted at
// rest — a completely ordinary posture (encrypt the parent once, the appliance
// holds the key) that has nothing to do with per-volume encryption. Every dataset
// created under it inherits encrypted:true with the PARENT as its encryption_root
// (P-10), which is precisely the state the driver used to misread as "this volume
// has its own unmanageable key".
func encryptedParentDriver(t *testing.T, encryptionEnabled bool) (*Driver, *truenas.MockClient) {
	t.Helper()
	mockClient := truenas.NewMockClient()
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent", Type: "FILESYSTEM",
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &truenas.EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "parent-pass-1"},
	})
	require.NoError(t, err)
	return &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:        NFSConfig{Enabled: true, ShareHost: "192.0.2.10"},
			Encryption: EncryptionConfig{Enabled: encryptionEnabled},
		},
		truenasClient: mockClient,
	}, mockClient
}

func plainVolumeRequest(name string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:               name,
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
	}
}

// TestInheritedEncryptionIsNotPerVolumeEncryption is the N-5 regression, and it
// is the reviewer's exact set of scenarios. On a deployment whose parent dataset
// is encrypted, EVERY dataset reads encrypted:true with the parent as its root.
// Reading that as "this volume holds its own unmanageable key" wedged ordinary
// plaintext replays, refused legitimate restores, and — worst — DESTROYED the
// restored data.
//
// The discriminator is P-10: encryption_root == the dataset itself (plus
// key_format PASSPHRASE) means the key is the driver's business; anything else is
// the deployment's baseline.
//
// PRE-FIX PROOF: with the predicate back to plain ds.Encrypted, sub-tests 1 and 2
// FAIL (FailedPrecondition "already exists ENCRYPTED", and the restore refused
// with its dataset destroyed).
func TestInheritedEncryptionIsNotPerVolumeEncryption(t *testing.T) {
	ctx := context.Background()

	t.Run("a plaintext volume under an encrypted parent replays normally", func(t *testing.T) {
		d, mockClient := encryptedParentDriver(t, false)
		req := plainVolumeRequest("inherited-plain")
		resp, err := d.CreateVolume(ctx, req)
		require.NoError(t, err)

		// The state that used to be misread.
		ds, err := mockClient.DatasetGet(ctx, "pool/parent/inherited-plain")
		require.NoError(t, err)
		require.True(t, ds.Encrypted, "P-10: a child of an encrypted parent is encrypted")
		require.Equal(t, "pool/parent", ds.EncryptionRoot, "P-10: its root is the PARENT, not itself")
		require.Equal(t, "", datasetLocalUserProperty(ds, PropEncryption), "and it carries no driver stamp")

		replay, err := d.CreateVolume(ctx, req)
		require.NoError(t, err, "an ordinary idempotent replay must not be wedged by baseline encryption")
		assert.Equal(t, resp.GetVolume().GetVolumeId(), replay.GetVolume().GetVolumeId())
	})

	t.Run("a restore under an encrypted parent succeeds and is not destroyed", func(t *testing.T) {
		d, mockClient := encryptedParentDriver(t, false)
		_, err := d.CreateVolume(ctx, plainVolumeRequest("inherited-source"))
		require.NoError(t, err)
		d.config.Encryption.Enabled = true // snapshot machinery is unrelated
		snapResp, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
			SourceVolumeId: "inherited-source", Name: "inherited-snap",
		})
		require.NoError(t, err)
		d.config.Encryption.Enabled = false

		restore := plainVolumeRequest("inherited-restore")
		restore.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapResp.GetSnapshot().GetSnapshotId()},
		}}
		_, err = d.CreateVolume(ctx, restore)
		require.NoError(t, err, "restoring under an encrypted parent is an ordinary restore")

		restored, err := mockClient.DatasetGet(ctx, "pool/parent/inherited-restore")
		require.NoError(t, err, "the restored data must still exist — never destroyed")
		assert.True(t, restored.Encrypted, "it is encrypted, by inheritance, exactly like its source")
	})

	t.Run("a clone under an encrypted parent is not refused", func(t *testing.T) {
		d, mockClient := encryptedParentDriver(t, true)
		_, err := d.CreateVolume(ctx, plainVolumeRequest("inherited-clone-source"))
		require.NoError(t, err)

		clone := plainVolumeRequest("inherited-clone")
		clone.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "inherited-clone-source"},
		}}
		_, err = d.CreateVolume(ctx, clone)
		require.NoError(t, err, "the source's key is the deployment's, not another CSI volume's")
		_, err = mockClient.DatasetGet(ctx, "pool/parent/inherited-clone")
		require.NoError(t, err)
	})

	t.Run("publish of an inherited-encryption volume pays no encryption job", func(t *testing.T) {
		client := newAPICallCountingClient()
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
			Name: "pool/parent", Type: "FILESYSTEM",
			Encryption:        boolPtr(true),
			InheritEncryption: boolPtr(false),
			EncryptionOptions: &truenas.EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "parent-pass-1"},
		})
		require.NoError(t, err)
		d := &Driver{
			name:          "org.scale.csi.nfs",
			config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}, Encryption: EncryptionConfig{Enabled: true}},
			truenasClient: client,
		}
		_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/inherited-pub", Type: "FILESYSTEM"})
		require.NoError(t, err)
		ds, err := client.DatasetGet(ctx, "pool/parent/inherited-pub")
		require.NoError(t, err)
		require.True(t, ds.Encrypted)

		client.resetCalls()
		require.NoError(t, d.unlockEncryptedDatasetForPublish(ctx, ds, "pool/parent/inherited-pub", "inherited-pub", nil))
		_, methods := client.callSnapshot()
		assert.Zero(t, methods["DatasetEncryptionSummary"],
			"baseline encryption must not put every publish on the encryption path")
		assert.Zero(t, methods["DatasetUnlock"])
	})

	t.Run("the predicate itself: root and key format decide", func(t *testing.T) {
		selfKeyed := &truenas.Dataset{Name: "pool/parent/v", Encrypted: true,
			EncryptionRoot: "pool/parent/v", KeyFormat: truenas.KeyFormatPassphrase}
		inherited := &truenas.Dataset{Name: "pool/parent/v", Encrypted: true,
			EncryptionRoot: "pool/parent", KeyFormat: truenas.KeyFormatPassphrase}
		dbKeyed := &truenas.Dataset{Name: "pool/parent/v", Encrypted: true,
			EncryptionRoot: "pool/parent/v", KeyFormat: "HEX"}
		cloneOfDriverVolume := &truenas.Dataset{Name: "pool/parent/clone", Encrypted: true,
			EncryptionRoot: "pool/parent/origin", KeyFormat: truenas.KeyFormatPassphrase}

		assert.True(t, datasetSelfKeyedPassphrase(selfKeyed))
		assert.False(t, datasetSelfKeyedPassphrase(inherited), "root is the parent: not this volume's key")
		assert.False(t, datasetSelfKeyedPassphrase(dbKeyed), "a key the appliance stores needs nothing from us")
		assert.False(t, datasetSelfKeyedPassphrase(cloneOfDriverVolume), "P-7: a clone's root is its origin")

		d := &Driver{config: &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}}}
		assert.True(t, d.datasetKeyIsDriverManaged(selfKeyed))
		assert.True(t, d.datasetKeyIsDriverManaged(cloneOfDriverVolume),
			"its key belongs to ANOTHER CSI volume — still the driver's problem")
		assert.False(t, d.datasetKeyIsDriverManaged(inherited),
			"a root at or above the driver's parent is the deployment's baseline")
		assert.False(t, d.datasetKeyIsDriverManaged(dbKeyed))
	})
}

// TestPublishUsesPreviousKeyForUnstampedVolume is the N-6 regression: one flag
// gated two unrelated permissions. Trying the PREVIOUS passphrase to UNLOCK is
// safe for any locked volume the driver is handling (a wrong key fails closed,
// drill-verified at the ZFS layer); only the RE-KEY is ownership-gated. Gating both stranded exactly the
// volume that needs it most — an encrypted volume whose stamp write was lost
// while it was still on the previous passphrase, with the key that opens it
// sitting in the Secret.
//
// PRE-FIX PROOF: with `rotating := keys.rotationIntent() && ownsKey` gating both,
// this FAILS — one unlock attempted, FailedPrecondition, volume still locked.
func TestPublishUsesPreviousKeyForUnstampedVolume(t *testing.T) {
	const name = "pool/parent/enc-stamp-lost"
	const volumeID = "enc-stamp-lost"
	const oldPass = "old-pass-123"
	const newPass = "new-pass-456"

	d, client := encryptionTestDriver()
	recorder := record.NewFakeRecorder(16)
	d.eventRecorder = &EventRecorder{recorder: recorder, enabled: true}
	ctx := context.Background()

	// Encrypted, self-keyed, still on the OLD passphrase, stamp never written.
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: name, Type: "VOLUME", Volsize: 1 << 30,
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &truenas.EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: oldPass},
	})
	require.NoError(t, err)
	require.NoError(t, client.DatasetLock(ctx, name))
	ds, err := client.DatasetGet(ctx, name)
	require.NoError(t, err)
	require.Equal(t, "", datasetLocalUserProperty(ds, PropEncryption), "the stamp write was lost")

	client.resetCalls()
	require.NoError(t, d.unlockEncryptedDatasetForPublish(ctx, ds, name, volumeID,
		map[string]string{"passphrase": newPass, "passphrasePrevious": oldPass}),
		"the key that opens this volume is in the Secret and must be tried")

	_, methods := client.callSnapshot()
	assert.Equal(t, 2, methods["DatasetUnlock"], "current then previous")
	assert.Zero(t, methods["DatasetChangeKey"], "but an unstamped volume is never re-keyed")
	unlocked, err := client.DatasetGet(ctx, name)
	require.NoError(t, err)
	assert.False(t, unlocked.Locked, "the volume is serving again")
	assert.Zero(t, eventsContainingReason(drainEvents(recorder), EventReasonEncryptionRotationIncomplete))
}

// TestRecoveredRemnantWithDriverManagedKeyIsRolledBack is the N-7 regression: the
// crash-recovery path adopted an interrupted content-source remnant without ever
// running the backstop, then dead-ended on the replay guard with two remediation
// instructions that both lead back to a refusal. It now takes the same
// destroy-and-refuse outcome as the uninterrupted path.
//
// PRE-FIX PROOF: without the backstop on the resume arm, this returns the replay
// guard's dead-end refusal and leaves the remnant dataset in place.
func TestRecoveredRemnantWithDriverManagedKeyIsRolledBack(t *testing.T) {
	ctx := context.Background()
	mockClient := truenas.NewMockClient()
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
	_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	// A driver-encrypted source volume and a CSI snapshot of it.
	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "enc-src",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"encryption": "true"},
		Secrets:            map[string]string{"passphrase": "longenough1"},
	})
	require.NoError(t, err)
	snapResp, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{SourceVolumeId: "enc-src", Name: "enc-snap"})
	require.NoError(t, err)
	backendSnapshot := snapResp.GetSnapshot().GetSnapshotId() // dataset-qualified handle == the ZFS snapshot ID

	// Reproduce the interrupted create: marker written, clone made, crash before
	// the ownership stamp.
	const destination = "pool/parent/resumed"
	source := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapResp.GetSnapshot().GetSnapshotId()},
	}}
	marker, err := d.newInflightMarker(destination, source, ShareTypeNFS)
	require.NoError(t, err)
	marker.Origin = backendSnapshot
	require.NoError(t, d.writeInflightMarker(ctx, marker))
	require.NoError(t, mockClient.SnapshotClone(ctx, backendSnapshot, destination))
	remnant, err := mockClient.DatasetGet(ctx, destination)
	require.NoError(t, err)
	require.True(t, remnant.Encrypted, "P-7: the remnant inherited the source volume's key")

	req := plainVolumeRequest("resumed")
	req.VolumeContentSource = source
	_, err = d.CreateVolume(ctx, req)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "copy the data in", "the remediation must be one the operator can follow")
	_, getErr := mockClient.DatasetGet(ctx, destination)
	require.Error(t, getErr, "the unmanageable remnant is rolled back, not left wedging the PVC")
}

// captureKlog redirects klog to a buffer for the duration of fn. It is how the
// "warn once, not once per call" contract is asserted on the actual log output
// rather than on the bookkeeping that produces it.
func captureKlog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	klog.LogToStderr(false)
	// Every severity needs a writer (klog mirrors a warning into the INFO stream
	// too, glog semantics, and a nil writer panics), but only the WARNING stream
	// is captured — a single shared writer would see every line twice and turn a
	// once-per-dataset assertion into a lie.
	klog.SetOutput(io.Discard)
	klog.SetOutputBySeverity("WARNING", &buf)
	t.Cleanup(func() {
		klog.Flush()
		klog.SetOutput(io.Discard)
		klog.LogToStderr(true)
	})
	fn()
	klog.Flush()
	return buf.String()
}

// TestUnknownEncryptionIdentityIsLoudOncePerDataset is the first half of the N-9
// regression. The tolerant decode is right — a strict tag on a BETA encryption
// field would fail EVERY dataset in a response (real history, 2026-07-31) — but
// the resulting "identity unknown" used to degrade to "not ours" with NO trace at
// all: a genuinely self-keyed, unstamped volume whose key_format shape moved was
// treated as plaintext at publish, with no unlock, no summary read, no error and
// no Event, surfacing later as an unexplained device-wait timeout.
//
// PRE-FIX PROOF: with the warnUnknownEncryptionIdentity calls removed, the log
// contains nothing at all for either dataset.
func TestUnknownEncryptionIdentityIsLoudOncePerDataset(t *testing.T) {
	rootless := &truenas.Dataset{Name: "pool/parent/n9-rootless", Encrypted: true}
	formatless := &truenas.Dataset{
		Name: "pool/parent/n9-formatless", Encrypted: true,
		EncryptionRoot: "pool/parent/n9-formatless",
	}

	logged := captureKlog(t, func() {
		for i := 0; i < 5; i++ {
			assert.False(t, datasetSelfKeyedPassphrase(rootless))
			assert.False(t, datasetSelfKeyedPassphrase(formatless))
			assert.False(t, datasetNeedsEncryptionHandling(rootless))
		}
	})

	assert.Equal(t, 1, strings.Count(logged, "pool/parent/n9-rootless"),
		"the contradiction is reported once per dataset per process, not once per call")
	assert.Equal(t, 1, strings.Count(logged, "pool/parent/n9-formatless"))
	assert.Contains(t, logged, "encryption_root", "the warning names the field that did not parse")
	assert.Contains(t, logged, "key_format")
	assert.Contains(t, logged, "step 1b", "and points at the drill step that re-pins the shape")

	// A healthy identity is silent.
	quiet := captureKlog(t, func() {
		assert.True(t, datasetSelfKeyedPassphrase(&truenas.Dataset{
			Name: "pool/parent/n9-healthy", Encrypted: true,
			EncryptionRoot: "pool/parent/n9-healthy", KeyFormat: truenas.KeyFormatPassphrase,
		}))
	})
	assert.NotContains(t, quiet, "n9-healthy")
}

// TestUnknownEncryptionIdentityAsymmetry is the second half of N-9: unknown
// identity must fail CLOSED where being wrong costs a refusal, and fail OPEN
// where being wrong costs DESTROYED DATA.
func TestUnknownEncryptionIdentityAsymmetry(t *testing.T) {
	d := &Driver{config: &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}}}

	unknownRoot := &truenas.Dataset{Name: "pool/parent/unknown-root", Encrypted: true}
	unknownFormat := &truenas.Dataset{
		Name: "pool/parent/unknown-format", Encrypted: true,
		EncryptionRoot: "pool/parent/unknown-format",
	}
	baseline := &truenas.Dataset{
		Name: "pool/parent/baseline", Encrypted: true,
		EncryptionRoot: "pool", KeyFormat: "",
	}
	proven := &truenas.Dataset{
		Name: "pool/parent/proven", Encrypted: true,
		EncryptionRoot: "pool/parent/proven", KeyFormat: truenas.KeyFormatPassphrase,
	}

	_ = captureKlog(t, func() {
		// REFUSAL scope: unknown counts (cost of being wrong = one clear error).
		assert.True(t, d.datasetKeyMayBeDriverManaged(unknownRoot))
		assert.True(t, d.datasetKeyMayBeDriverManaged(unknownFormat))
		assert.True(t, d.datasetKeyMayBeDriverManaged(proven))
		assert.False(t, d.datasetKeyMayBeDriverManaged(baseline),
			"a root outside the driver's parent is KNOWN baseline whatever its key format")

		// DESTROY scope: only positive proof (cost of being wrong = lost data).
		assert.False(t, d.datasetKeyIsDriverManaged(unknownRoot))
		assert.False(t, d.datasetKeyIsDriverManaged(unknownFormat))
		assert.True(t, d.datasetKeyIsDriverManaged(proven))
		assert.False(t, d.refuseEncryptedContentSourceResult(context.Background(), unknownRoot))
		assert.False(t, d.refuseEncryptedContentSourceResult(context.Background(), unknownFormat))
		assert.True(t, d.refuseEncryptedContentSourceResult(context.Background(), proven))
	})
}

// encryptionIdentityStrippingClient models the exact failure the tolerant decode
// produces when the pinned shape moves: every dataset still reports encrypted,
// but its identity fields come back empty because the parser could not read
// them.
type encryptionIdentityStrippingClient struct {
	*truenas.MockClient
}

func stripEncryptionIdentity(ds *truenas.Dataset) *truenas.Dataset {
	if ds == nil {
		return nil
	}
	stripped := *ds
	stripped.EncryptionRoot = ""
	stripped.KeyFormat = ""
	return &stripped
}

func (c *encryptionIdentityStrippingClient) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	ds, err := c.MockClient.DatasetGet(ctx, name)
	return stripEncryptionIdentity(ds), err
}

func (c *encryptionIdentityStrippingClient) DatasetCreate(ctx context.Context, params *truenas.DatasetCreateParams) (*truenas.Dataset, error) {
	ds, err := c.MockClient.DatasetCreate(ctx, params)
	return stripEncryptionIdentity(ds), err
}

func (c *encryptionIdentityStrippingClient) DatasetUpdate(ctx context.Context, name string, params *truenas.DatasetUpdateParams) (*truenas.Dataset, error) {
	ds, err := c.MockClient.DatasetUpdate(ctx, name, params)
	return stripEncryptionIdentity(ds), err
}

// TestUnknownEncryptionIdentityRefusesButNeverDestroys drives the reviewer's
// exact scenario end to end: the identity shape has moved, so every dataset reads
// encrypted-with-unknown-identity.
//
//   - with the feature on, the pre-mutation content-source refusal still fires
//     (fail closed: the cost is one clear error);
//   - with the feature off, the post-create backstop does NOT destroy the
//     restored volume (fail open: the cost of being wrong would be lost data).
//
// PRE-FIX PROOF: reverting the refusal to proven-only lets the first sub-test's
// clone succeed; widening the backstop to may-be-managed destroys the second
// sub-test's restored dataset.
func TestUnknownEncryptionIdentityRefusesButNeverDestroys(t *testing.T) {
	ctx := context.Background()

	newStrippedDriver := func(t *testing.T, encryptionEnabled bool) (*Driver, *truenas.MockClient) {
		t.Helper()
		mockClient := truenas.NewMockClient()
		_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
		require.NoError(t, err)
		return &Driver{
			name: "org.scale.csi.nfs",
			config: &Config{
				DriverName: "org.scale.csi.nfs",
				ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
				NFS:        NFSConfig{Enabled: true, ShareHost: "192.0.2.10"},
				Encryption: EncryptionConfig{Enabled: encryptionEnabled},
			},
			truenasClient: &encryptionIdentityStrippingClient{MockClient: mockClient},
		}, mockClient
	}

	t.Run("the content-source refusal still fires on unknown identity", func(t *testing.T) {
		d, mockClient := newStrippedDriver(t, true)
		// The exposed state: encrypted on the wire, NO local stamp (the crash
		// window), and an identity the parser cannot read. The stamp cannot carry
		// this decision — only the identity predicate can.
		_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
			Name: "pool/parent/n9-src", Type: "FILESYSTEM",
			Encryption:        boolPtr(true),
			InheritEncryption: boolPtr(false),
			EncryptionOptions: &truenas.EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "longenough1"},
		})
		require.NoError(t, err)
		source, err := d.truenasClient.DatasetGet(ctx, "pool/parent/n9-src")
		require.NoError(t, err)
		require.True(t, source.Encrypted, "the backend still says encrypted")
		require.Equal(t, "", source.EncryptionRoot, "but its identity did not parse")
		require.Equal(t, "", datasetLocalUserProperty(source, PropEncryption), "and it carries no stamp")

		clone := plainVolumeRequest("n9-clone")
		clone.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "n9-src"},
		}}
		_, err = d.CreateVolume(ctx, clone)
		require.Error(t, err, "an unreadable identity must not silently allow cloning an encrypted volume")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		_, cloneErr := mockClient.DatasetGet(ctx, "pool/parent/n9-clone")
		require.Error(t, cloneErr, "and the refusal precedes any mutation")
	})

	t.Run("the backstop does NOT destroy on unknown identity", func(t *testing.T) {
		d, mockClient := newStrippedDriver(t, true)
		_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
			Name:               "n9-src2",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			Parameters:         map[string]string{"encryption": "true"},
			Secrets:            map[string]string{"passphrase": "longenough1"},
		})
		require.NoError(t, err)
		snapResp, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{SourceVolumeId: "n9-src2", Name: "n9-snap"})
		require.NoError(t, err)

		// Feature OFF: the snapshot branch's pre-mutation read is skipped, so the
		// post-create backstop is the only thing left — and it must not delete a
		// dataset whose identity it could not read.
		d.config.Encryption.Enabled = false
		restore := plainVolumeRequest("n9-restore")
		restore.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapResp.GetSnapshot().GetSnapshotId()},
		}}
		_, createErr := d.CreateVolume(ctx, restore)

		restored, getErr := mockClient.DatasetGet(ctx, "pool/parent/n9-restore")
		require.NoError(t, getErr,
			"the restored dataset must survive: an unreadable identity is never a reason to destroy data (createErr=%v)", createErr)
		assert.True(t, restored.Encrypted)
		assert.Empty(t, mockClient.DatasetDeleteCalls, "no destroy is issued on unknown identity")
	})
}

// TestPublishNeverRekeysAnInheritingCloneEvenThoughTheBackendWould is the D-2
// regression, and it is a SAFETY test, not an error-message test.
//
// The design assumed ZFS refuses change_key on an inheriting child. The live
// drill (2026-08-02) measured the opposite: it SUCCEEDS and silently promotes the
// child to its own encryption root, severing it from the origin key — restored
// data re-keyed to a passphrase the origin's operator does not have, with no
// error anywhere. The mock now models that, so this test pins the DRIVER's gate
// as the only thing preventing it.
//
// PRE-FIX PROOF: forcing ownsKey true (the gate's regression) makes the clone's
// encryption_root move to itself and its origin key stop opening it.
func TestPublishNeverRekeysAnInheritingCloneEvenThoughTheBackendWould(t *testing.T) {
	const origin = "pool/parent/d2-origin"
	const clone = "pool/parent/d2-clone"
	const originPass = "origin-pass-123"
	const newPass = "new-pass-456"

	d, client := encryptionTestDriver()
	d.eventRecorder = &EventRecorder{recorder: record.NewFakeRecorder(16), enabled: true}
	ctx := context.Background()

	createEncryptedDataset(t, client, origin, originPass)
	_, err := client.SnapshotCreate(ctx, origin, "snap", nil)
	require.NoError(t, err)
	require.NoError(t, client.SnapshotClone(ctx, origin+"@snap", clone))
	cloneDS, err := client.DatasetGet(ctx, clone)
	require.NoError(t, err)
	require.Equal(t, origin, cloneDS.EncryptionRoot, "P-7: the clone's key is the ORIGIN's")

	// A rotation window is open on the class this clone belongs to.
	client.resetCalls()
	require.NoError(t, d.unlockEncryptedDatasetForPublish(ctx, cloneDS, clone, "d2-clone",
		map[string]string{"passphrase": newPass, "passphrasePrevious": originPass}))

	_, methods := client.callSnapshot()
	require.Zero(t, methods["DatasetChangeKey"],
		"the gate — not the backend — must stop the re-key; the backend would ACCEPT it (D-2)")
	// Note on defense in depth: for THIS volume two independent gates say no —
	// the identity predicate (its encryption_root is the origin, so it is not
	// driver-encryption territory at all, N-5) and the ownsKey re-key gate. The
	// ownsKey gate is isolated by TestPublishUsesPreviousKeyForUnstampedVolume,
	// where the identity predicate says YES (self-keyed) and only the missing
	// stamp withholds the re-key.

	after, err := client.DatasetGet(ctx, clone)
	require.NoError(t, err)
	assert.Equal(t, origin, after.EncryptionRoot,
		"the clone must still be keyed by its ORIGIN: a re-key here silently severs it")

	// And the proof that the backend really would have done it: the same call
	// made directly succeeds and re-roots the clone.
	require.NoError(t, client.DatasetChangeKey(ctx, clone, newPass))
	severed, err := client.DatasetGet(ctx, clone)
	require.NoError(t, err)
	assert.Equal(t, clone, severed.EncryptionRoot,
		"D-2 measured: change_key on an inheriting child succeeds and promotes it")
}

// TestPublishOwnershipGateRefusesOnItsOwnTerms is the O-1 regression.
//
// Drill #3 drove the divergent shape — a dataset carrying the driver's LOCAL
// encryption stamp whose encryption_root is ANOTHER dataset — with a rotation
// window open. It did not re-key, but not because the publish gate said no: the
// publish gate was `ownsKey := isEncryptedDataset(ds)` (the stamp alone) and
// would have set mayRekey=true. What actually stopped it was an unrelated
// backend invariant — encryption_summary returns [] for a non-encryption-root,
// and the driver's exact-name match then fails closed. Defense in depth doing
// the gate's job.
//
// So this test switches that downstream guard OFF (the mock answers the summary
// for an inherited-key dataset, which the appliance does NOT do) and asserts the
// GATE refuses. It cannot pass for the wrong reason: with the downstream stubbed
// out, the only thing left between the rotation window and change_key is
// encryptionOwnershipConfirmed.
//
// PRE-FIX PROOF: restore `ownsKey := isEncryptedDataset(ds)` and this FAILS with
// DatasetChangeKey == 1 and the clone re-rooted to itself (D-2's severing).
func TestPublishOwnershipGateRefusesOnItsOwnTerms(t *testing.T) {
	const origin = "pool/parent/o1-origin"
	const stampedClone = "pool/parent/o1-clone"
	const originPass = "origin-pass-123"
	const newPass = "new-pass-456"

	d, client := encryptionTestDriver()
	d.eventRecorder = &EventRecorder{recorder: record.NewFakeRecorder(16), enabled: true}
	ctx := context.Background()

	createEncryptedDataset(t, client, origin, originPass)
	_, err := client.SnapshotCreate(ctx, origin, "snap", nil)
	require.NoError(t, err)
	require.NoError(t, client.SnapshotClone(ctx, origin+"@snap", stampedClone))
	// The divergent shape: the clone carries a LOCAL stamp of its own (which the
	// driver's own APIs will not produce — the content-source refusal and its
	// backstop prevent it — but which the gate must still refuse) while its key
	// remains the ORIGIN's.
	require.NoError(t, client.DatasetSetUserProperty(ctx, stampedClone, PropEncryption, "AES-256-GCM"))

	// Switch OFF the unrelated downstream fail-closed, so only the gate is left.
	client.ModelEncryptionSummaryForInheritedKeys = true

	cloneDS, err := client.DatasetGet(ctx, stampedClone)
	require.NoError(t, err)
	require.Equal(t, "AES-256-GCM", datasetLocalUserProperty(cloneDS, PropEncryption),
		"the stamp says ours")
	require.Equal(t, origin, cloneDS.EncryptionRoot, "the key says otherwise")
	require.False(t, cloneDS.Locked, "unlocked, so the O-2 locked arm is not what is being tested")

	client.resetCalls()
	require.NoError(t, d.unlockEncryptedDatasetForPublish(ctx, cloneDS, stampedClone, "o1-clone",
		map[string]string{"passphrase": newPass, "passphrasePrevious": originPass}),
		"a healthy unlocked volume must still publish")

	_, methods := client.callSnapshot()
	require.Equal(t, 1, methods["DatasetEncryptionSummary"],
		"the downstream guard was reached and ANSWERED — so it is not what refused")
	assert.Zero(t, methods["DatasetChangeKey"],
		"the ownership gate itself must refuse the re-key")

	after, err := client.DatasetGet(ctx, stampedClone)
	require.NoError(t, err)
	assert.Equal(t, origin, after.EncryptionRoot,
		"D-2: a re-key here would have silently severed the clone from its origin key")
}

// TestPublishOwnershipGateMatchesTheReconciler pins the alignment itself: both
// paths now ask ONE predicate. The table is the shapes the two used to disagree
// about.
func TestPublishOwnershipGateMatchesTheReconciler(t *testing.T) {
	stamped := map[string]truenas.UserProperty{PropEncryption: {Value: "AES-256-GCM", Source: "local"}}
	inheritedStamp := map[string]truenas.UserProperty{PropEncryption: {Value: "AES-256-GCM", Source: "pool/parent/origin@snap"}}

	for _, tc := range []struct {
		name string
		ds   *truenas.Dataset
		want bool
	}{
		{"self-keyed and stamped: ours", &truenas.Dataset{
			Name: "pool/parent/v", Encrypted: true, EncryptionRoot: "pool/parent/v",
			KeyFormat: truenas.KeyFormatPassphrase, UserProperties: stamped}, true},
		{"stamped but key inherited: NOT ours (the O-1 shape)", &truenas.Dataset{
			Name: "pool/parent/v", Encrypted: true, EncryptionRoot: "pool/parent/origin",
			KeyFormat: truenas.KeyFormatPassphrase, UserProperties: stamped}, false},
		{"clone-inherited stamp: not ours", &truenas.Dataset{
			Name: "pool/parent/v", Encrypted: true, EncryptionRoot: "pool/parent/origin",
			KeyFormat: truenas.KeyFormatPassphrase, UserProperties: inheritedStamp}, false},
		{"self-keyed but unstamped: not ours to re-key", &truenas.Dataset{
			Name: "pool/parent/v", Encrypted: true, EncryptionRoot: "pool/parent/v",
			KeyFormat: truenas.KeyFormatPassphrase, UserProperties: map[string]truenas.UserProperty{}}, false},
		{"plaintext", &truenas.Dataset{Name: "pool/parent/v", UserProperties: map[string]truenas.UserProperty{}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, encryptionOwnershipConfirmed(tc.ds),
				"publish and the unlock reconciler ask THIS function; they must not diverge again")
		})
	}
}

// TestPublishNamesALockedInheritedKey is the O-2 regression. Drill #3 published a
// volume that was locked because its encryption ROOT was locked; the driver
// failed closed — correctly — with `Internal: failed to create NFS share: ...
// Invalid params` from the share build, which tells the operator nothing about
// encryption. The behavior stays fail-closed; only the diagnosis changes.
//
// PRE-FIX PROOF: without the inherited-key arm the call returns nil (the volume
// carries no stamp and is not self-keyed, so the encryption path skipped it) and
// the publish fails much later, in the share builder, with no mention of a key.
func TestPublishNamesALockedInheritedKey(t *testing.T) {
	const parent = "pool/parent/o2-parent"
	const child = parent + "/child"
	const parentPass = "parent-pass-123"

	d, client := encryptionTestDriver()
	ctx := context.Background()

	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: parent, Type: "FILESYSTEM",
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &truenas.EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: parentPass},
	})
	require.NoError(t, err)
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: child, Type: "FILESYSTEM"})
	require.NoError(t, err)
	// Locking the root locks the child with it (P-7/P-10 inheritance).
	require.NoError(t, client.DatasetLock(ctx, parent))

	childDS, err := client.DatasetGet(ctx, child)
	require.NoError(t, err)
	require.True(t, childDS.Locked)
	require.Equal(t, parent, childDS.EncryptionRoot)
	require.Equal(t, "", datasetLocalUserProperty(childDS, PropEncryption), "no stamp: not this driver's volume")

	client.resetCalls()
	err = d.unlockEncryptedDatasetForPublish(ctx, childDS, child, "o2-child",
		map[string]string{"passphrase": parentPass})
	require.Error(t, err, "it must still fail closed")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "LOCKED", "the message names the state")
	assert.Contains(t, err.Error(), "INHERITED", "and why this driver cannot fix it")
	assert.Contains(t, err.Error(), parent, "and which dataset actually holds the key")
	assert.NotContains(t, err.Error(), parentPass, "and never the passphrase")

	_, methods := client.callSnapshot()
	assert.Zero(t, methods["DatasetUnlock"],
		"no unlock is attempted: the backend refuses an unlock on a non-encryption-root anyway")
}
