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
	"k8s.io/client-go/tools/record"

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
		unstamped := &truenas.Dataset{Encrypted: true, UserProperties: map[string]truenas.UserProperty{}}
		encCtx := withEncryptionResolution(ctx, &encryptionResolution{Algorithm: "AES-192-GCM", Passphrase: "longenough1"})
		repair, err := d.guardExistingEncryptionPolicy(encCtx, unstamped)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{PropEncryption: "AES-192-GCM"}, repair)
	})
	t.Run("encrypted-on-wire but unstamped: a plaintext replay fails closed and never says plaintext", func(t *testing.T) {
		unstamped := &truenas.Dataset{Encrypted: true, UserProperties: map[string]truenas.UserProperty{}}
		_, err := d.guardExistingEncryptionPolicy(ctx, unstamped)
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "ENCRYPTED")
		assert.NotContains(t, err.Error(), "already exists as plaintext",
			"the driver must never describe an encrypted dataset as plaintext")
	})
	t.Run("encrypted-on-wire from a content source is never adopted as its own policy", func(t *testing.T) {
		clone := &truenas.Dataset{Encrypted: true, UserProperties: map[string]truenas.UserProperty{
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

	t.Run("fail-closed: locked volume with wrong passphrase (P-5)", func(t *testing.T) {
		d, client := encryptionTestDriver()
		createEncryptedDataset(t, client, name, "longenough1")
		require.NoError(t, client.DatasetLock(context.Background(), name))
		ds, _ := client.DatasetGet(context.Background(), name)
		err := d.unlockEncryptedDatasetForPublish(context.Background(), ds, name, volumeID, map[string]string{"passphrase": "wrong-pass-9"})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		locked, _ := client.DatasetGet(context.Background(), name)
		assert.True(t, locked.Locked, "P-5: a wrong passphrase leaves the dataset locked")
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
