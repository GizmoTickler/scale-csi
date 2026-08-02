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
		assert.NoError(t, d.guardExistingEncryptionPolicy(encCtx, encryptedDS))
	})
	t.Run("plaintext replay of a plaintext volume is allowed", func(t *testing.T) {
		assert.NoError(t, d.guardExistingEncryptionPolicy(ctx, plaintextDS))
	})
	t.Run("plaintext replay of an encrypted volume is refused", func(t *testing.T) {
		err := d.guardExistingEncryptionPolicy(ctx, encryptedDS)
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
	t.Run("encrypted replay of a plaintext volume is refused", func(t *testing.T) {
		encCtx := withEncryptionResolution(ctx, &encryptionResolution{Algorithm: "AES-256-GCM", Passphrase: "longenough1"})
		err := d.guardExistingEncryptionPolicy(encCtx, plaintextDS)
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
	t.Run("a clone-inherited marker is not the volume's own policy", func(t *testing.T) {
		cloneDS := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{
			PropEncryption: {Value: "AES-256-GCM", Source: "flashstor/origin@snap"},
		}}
		// Source != local => storedEncryptionAlgorithm returns "" => treated plaintext.
		assert.False(t, isEncryptedDataset(cloneDS))
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
			assert.True(t, ds.Locked, "locked is surfaced as a normal listed state, not an error")
		}
	}
	assert.True(t, found, "a locked dataset is still queryable and listed (P-4)")

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
