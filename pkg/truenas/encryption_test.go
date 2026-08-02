package truenas

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(v bool) *bool { return &v }

// TestEncryptionCreateParamsMarshal pins the pool.dataset.create wire shape to
// the P-1/P-2 probe: encryption + inherit_encryption + encryption_options ride
// inline in the single create call (+0 RTT). A plaintext create must emit none
// of the three keys (omitempty), so its payload is byte-identical to
// pre-encryption.
func TestEncryptionCreateParamsMarshal(t *testing.T) {
	t.Run("encrypted create carries the probe shape", func(t *testing.T) {
		params := &DatasetCreateParams{
			Name:              "flashstor/gf1-enc-drill-fs",
			Type:              "FILESYSTEM",
			Encryption:        boolPtr(true),
			InheritEncryption: boolPtr(false),
			EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "drill-pass-1"},
		}
		encoded, err := json.Marshal(params)
		require.NoError(t, err)
		assert.JSONEq(t,
			`{"name":"flashstor/gf1-enc-drill-fs","type":"FILESYSTEM",
			  "encryption":true,"inherit_encryption":false,
			  "encryption_options":{"algorithm":"AES-256-GCM","passphrase":"drill-pass-1"}}`,
			string(encoded))
	})

	t.Run("encrypted zvol create carries the same shape plus volsize", func(t *testing.T) {
		params := &DatasetCreateParams{
			Name:              "flashstor/gf1-enc-drill-zv",
			Type:              "VOLUME",
			Volsize:           1 << 30,
			Sparse:            true,
			Encryption:        boolPtr(true),
			InheritEncryption: boolPtr(false),
			EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "drill-pass-1"},
		}
		encoded, err := json.Marshal(params)
		require.NoError(t, err)
		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(encoded, &payload))
		assert.Contains(t, payload, "encryption")
		assert.Contains(t, payload, "inherit_encryption")
		assert.Contains(t, payload, "encryption_options")
		assert.Contains(t, payload, "volsize")
	})

	t.Run("plaintext create emits no encryption keys", func(t *testing.T) {
		params := &DatasetCreateParams{Name: "flashstor/plain", Type: "FILESYSTEM"}
		encoded, err := json.Marshal(params)
		require.NoError(t, err)
		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(encoded, &payload))
		assert.NotContains(t, payload, "encryption")
		assert.NotContains(t, payload, "inherit_encryption")
		assert.NotContains(t, payload, "encryption_options")
	})

	t.Run("pbkdf2iters is omitted when zero", func(t *testing.T) {
		encoded, err := json.Marshal(&EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "drill-pass-1"})
		require.NoError(t, err)
		assert.JSONEq(t, `{"algorithm":"AES-256-GCM","passphrase":"drill-pass-1"}`, string(encoded))
	})
}

// TestEncryptionCreateKeyScopesProbedBothTypes pins the delta-2 classification:
// the three encryption keys are PROBED-accepted on BOTH dataset types (P-1
// FILESYSTEM, P-2 VOLUME). A silent narrowing to one type is a test failure.
func TestEncryptionCreateKeyScopesProbedBothTypes(t *testing.T) {
	for _, key := range []string{"encryption", "inherit_encryption", "encryption_options"} {
		assert.True(t, datasetKeyAccepted(datasetCreateKeyScopes, key, "FILESYSTEM"), "%s on FILESYSTEM (P-1)", key)
		assert.True(t, datasetKeyAccepted(datasetCreateKeyScopes, key, "VOLUME"), "%s on VOLUME (P-2)", key)
	}
}

// TestMockDatasetCreateEncryption proves the mock accepts an encrypted create for
// both dataset types under the schema gate and models the P-1/P-2 post-create
// state: encrypted, unlocked, key loaded, its own encryption_root.
func TestMockDatasetCreateEncryption(t *testing.T) {
	ctx := context.Background()
	for _, datasetType := range []string{"FILESYSTEM", "VOLUME"} {
		t.Run(datasetType, func(t *testing.T) {
			mock := NewMockClient()
			params := &DatasetCreateParams{
				Name:              "flashstor/gf1-enc-drill-" + datasetType,
				Type:              datasetType,
				Encryption:        boolPtr(true),
				InheritEncryption: boolPtr(false),
				EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "drill-pass-1"},
			}
			if datasetType == "VOLUME" {
				params.Volsize = 1 << 30
				params.Sparse = true
			}
			ds, err := mock.DatasetCreate(ctx, params)
			require.NoError(t, err)
			assert.True(t, ds.Encrypted)
			assert.False(t, ds.Locked, "P-1/P-2: an encrypted create comes up unlocked")
			assert.True(t, ds.KeyLoaded)
			assert.Equal(t, params.Name, ds.EncryptionRoot, "inherit_encryption=false => its own root")
			assert.Equal(t, "AES-256-GCM", ds.EncryptionAlgorithm)
		})
	}
}

// TestMockEncryptionLifecycle walks the full P-4/P-5/P-6/P-8 state machine on the
// mock: lock removes the backing device, unlock is gated and fail-closed on a
// wrong passphrase, change_key rotates only when unlocked.
func TestMockEncryptionLifecycle(t *testing.T) {
	ctx := context.Background()
	mock := NewMockClient()
	const name = "flashstor/gf1-enc-drill-zv"
	_, err := mock.DatasetCreate(ctx, &DatasetCreateParams{
		Name:              name,
		Type:              "VOLUME",
		Volsize:           1 << 30,
		Sparse:            true,
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "drill-pass-1"},
	})
	require.NoError(t, err)

	// A freshly created encrypted zvol is ready (backing device present).
	_, err = mock.WaitForZvolReady(ctx, name, 0)
	require.NoError(t, err)

	// P-4: lock -> locked:true, key_loaded:false, no backing device, no mountpoint.
	require.NoError(t, mock.DatasetLock(ctx, name))
	locked, err := mock.DatasetGet(ctx, name)
	require.NoError(t, err)
	assert.True(t, locked.Locked)
	assert.False(t, locked.KeyLoaded)
	assert.Empty(t, locked.Mountpoint, "P-4: a locked dataset reports mountpoint:null")
	_, err = mock.WaitForZvolReady(ctx, name, 0)
	require.Error(t, err, "P-4: a locked zvol has no backing device")

	// The summary reflects the locked state (the gate the driver reads, P-8).
	summary, err := mock.DatasetEncryptionSummary(ctx, name)
	require.NoError(t, err)
	require.Len(t, summary, 1)
	assert.True(t, summary[0].Locked)
	assert.False(t, summary[0].ValidKey)
	assert.False(t, summary[0].KeyPresentInDatabase, "P-3: passphrase is not persisted")

	// P-5: wrong passphrase -> FAILED, stays locked.
	err = mock.DatasetUnlock(ctx, name, "wrong-pass")
	require.Error(t, err)
	stillLocked, _ := mock.DatasetGet(ctx, name)
	assert.True(t, stillLocked.Locked, "P-5: a wrong passphrase leaves the dataset locked")

	// Correct passphrase -> unlocked, device returns.
	require.NoError(t, mock.DatasetUnlock(ctx, name, "drill-pass-1"))
	unlocked, _ := mock.DatasetGet(ctx, name)
	assert.False(t, unlocked.Locked)
	assert.True(t, unlocked.KeyLoaded)
	_, err = mock.WaitForZvolReady(ctx, name, 0)
	require.NoError(t, err)

	// P-8: unlock is NOT idempotent — unlocking an unlocked dataset fails.
	err = mock.DatasetUnlock(ctx, name, "drill-pass-1")
	require.Error(t, err, "P-8: unlock on an already-unlocked dataset is a FAILED job")

	// P-6: change_key requires unlocked, then the old passphrase is dead.
	require.NoError(t, mock.DatasetChangeKey(ctx, name, "drill-pass-2"))
	require.NoError(t, mock.DatasetLock(ctx, name))
	require.Error(t, mock.DatasetUnlock(ctx, name, "drill-pass-1"), "P-6: the old passphrase no longer unlocks")
	require.NoError(t, mock.DatasetUnlock(ctx, name, "drill-pass-2"), "P-6: the new passphrase unlocks")
}

// TestMockChangeKeyRequiresUnlocked proves change_key on a LOCKED dataset is a
// FAILED job (P-6: change_key requires the key loaded first).
func TestMockChangeKeyRequiresUnlocked(t *testing.T) {
	ctx := context.Background()
	mock := NewMockClient()
	const name = "flashstor/gf1-enc-drill-zv"
	_, err := mock.DatasetCreate(ctx, &DatasetCreateParams{
		Name: name, Type: "VOLUME", Volsize: 1 << 30,
		Encryption: boolPtr(true), InheritEncryption: boolPtr(false),
		EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "drill-pass-1"},
	})
	require.NoError(t, err)
	require.NoError(t, mock.DatasetLock(ctx, name))
	require.Error(t, mock.DatasetChangeKey(ctx, name, "drill-pass-2"))
}

// TestDatasetEncryptionSummaryJobShape proves the real client treats
// encryption_summary as a @job (P-3): dispatch returns a job id, the client
// awaits it, and the terminal job's RESULT list is parsed into typed entries.
func TestDatasetEncryptionSummaryJobShape(t *testing.T) {
	t.Run("successful job parses the P-3 list", func(t *testing.T) {
		var summaryArgs []interface{}
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			datasetEncryptionSummaryMethod: func(req rpcTestRequest) (interface{}, *rpcError) {
				summaryArgs = req.Params
				return float64(9001), nil
			},
			"core.get_jobs": static([]interface{}{map[string]interface{}{
				"id": float64(9001), "state": "SUCCESS",
				"result": []interface{}{map[string]interface{}{
					"name": "flashstor/gf1-enc-drill-zv", "key_format": "PASSPHRASE",
					"key_present_in_database": false, "valid_key": false,
					"locked": true, "unlock_error": nil, "unlock_successful": false,
				}},
			}}),
			"core.subscribe": static("sub-1"),
		})

		entries, err := client.DatasetEncryptionSummary(context.Background(), "flashstor/gf1-enc-drill-zv")
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "flashstor/gf1-enc-drill-zv", entries[0].Name)
		assert.Equal(t, "PASSPHRASE", entries[0].KeyFormat)
		assert.True(t, entries[0].Locked)
		assert.False(t, entries[0].ValidKey)
		assert.False(t, entries[0].KeyPresentInDatabase)
		require.Len(t, summaryArgs, 1)
		assert.Equal(t, "flashstor/gf1-enc-drill-zv", summaryArgs[0])
	})

	t.Run("failed job surfaces as an error", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			datasetEncryptionSummaryMethod: static(float64(9002)),
			"core.get_jobs": static([]interface{}{map[string]interface{}{
				"id": float64(9002), "state": "FAILED", "error": "dataset not encrypted",
			}}),
			"core.subscribe": static("sub-1"),
		})
		_, err := client.DatasetEncryptionSummary(context.Background(), "flashstor/plain")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "job 9002")
	})
}

// TestDatasetUnlockJobShape proves unlock is dispatched with the P-4 payload
// (datasets array + toggle_attachments:true) and that a FAILED job (P-5 wrong
// passphrase) surfaces as an error.
func TestDatasetUnlockJobShape(t *testing.T) {
	t.Run("payload matches the P-4 shape", func(t *testing.T) {
		var unlockArgs []interface{}
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			datasetUnlockMethod: func(req rpcTestRequest) (interface{}, *rpcError) {
				unlockArgs = req.Params
				return float64(9101), nil
			},
			"core.get_jobs": static([]interface{}{map[string]interface{}{
				"id": float64(9101), "state": "SUCCESS",
			}}),
			"core.subscribe": static("sub-1"),
		})

		require.NoError(t, client.DatasetUnlock(context.Background(), "flashstor/gf1-enc-drill-zv", "drill-pass-1"))
		require.Len(t, unlockArgs, 2)
		assert.Equal(t, "flashstor/gf1-enc-drill-zv", unlockArgs[0])
		options, ok := unlockArgs[1].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, true, options["toggle_attachments"], "P-4: re-run attachments on unlock")
		// Over the wire the datasets array JSON-decodes to []interface{}.
		datasets, ok := options["datasets"].([]interface{})
		require.True(t, ok)
		require.Len(t, datasets, 1)
		entry, ok := datasets[0].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "flashstor/gf1-enc-drill-zv", entry["name"])
		assert.Equal(t, "drill-pass-1", entry["passphrase"])
	})

	t.Run("wrong passphrase FAILED job is an error (P-5)", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			datasetUnlockMethod: static(float64(9102)),
			"core.get_jobs": static([]interface{}{map[string]interface{}{
				"id": float64(9102), "state": "FAILED", "error": "invalid passphrase",
			}}),
			"core.subscribe": static("sub-1"),
		})
		err := client.DatasetUnlock(context.Background(), "flashstor/gf1-enc-drill-zv", "wrong-pass")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "job 9102")
	})
}

// TestDatasetChangeKeyJobShape proves change_key is dispatched as a @job with the
// P-6 payload ({"passphrase": <new>}) keyed by dataset id.
func TestDatasetChangeKeyJobShape(t *testing.T) {
	var changeKeyArgs []interface{}
	client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
		datasetChangeKeyMethod: func(req rpcTestRequest) (interface{}, *rpcError) {
			changeKeyArgs = req.Params
			return float64(9201), nil
		},
		"core.get_jobs": static([]interface{}{map[string]interface{}{
			"id": float64(9201), "state": "SUCCESS",
		}}),
		"core.subscribe": static("sub-1"),
	})

	require.NoError(t, client.DatasetChangeKey(context.Background(), "flashstor/gf1-enc-drill-zv", "drill-pass-2"))
	require.Len(t, changeKeyArgs, 2)
	assert.Equal(t, "flashstor/gf1-enc-drill-zv", changeKeyArgs[0])
	options, ok := changeKeyArgs[1].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "drill-pass-2", options["passphrase"])
}

// TestDatasetLockJobShape proves lock is dispatched as a bare-id @job (drill/test
// only — no driver control path calls it).
func TestDatasetLockJobShape(t *testing.T) {
	var lockArgs []interface{}
	client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
		datasetLockMethod: func(req rpcTestRequest) (interface{}, *rpcError) {
			lockArgs = req.Params
			return float64(9301), nil
		},
		"core.get_jobs": static([]interface{}{map[string]interface{}{
			"id": float64(9301), "state": "SUCCESS",
		}}),
		"core.subscribe": static("sub-1"),
	})

	require.NoError(t, client.DatasetLock(context.Background(), "flashstor/gf1-enc-drill-zv"))
	require.Len(t, lockArgs, 1)
	assert.Equal(t, "flashstor/gf1-enc-drill-zv", lockArgs[0])
}

// TestMockCloneInheritsEncryption models P-7 (probed): a clone of an encrypted
// dataset is encrypted:true with encryption_root == the ORIGIN. It shares the
// origin's key — it is NOT independently keyed, it cannot be re-keyed, and
// locking the origin locks the clone. Without this in the mock, an
// encrypted-content-source test could not exist at all: every clone looked
// plaintext, which is why the driver's missing source-side guard was invisible.
func TestMockCloneInheritsEncryption(t *testing.T) {
	ctx := context.Background()
	mock := NewMockClient()
	const origin = "flashstor/gf1-enc-drill-zv"
	const clone = "flashstor/gf1-enc-drill-clone"

	_, err := mock.DatasetCreate(ctx, &DatasetCreateParams{
		Name: origin, Type: "VOLUME", Volsize: 1 << 30, Sparse: true,
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "drill-pass-1"},
	})
	require.NoError(t, err)
	_, err = mock.SnapshotCreate(ctx, origin, "snap", nil)
	require.NoError(t, err)
	require.NoError(t, mock.SnapshotClone(ctx, origin+"@snap", clone))

	cloned, err := mock.DatasetGet(ctx, clone)
	require.NoError(t, err)
	assert.True(t, cloned.Encrypted, "P-7: the clone is encrypted")
	assert.Equal(t, origin, cloned.EncryptionRoot, "P-7: encryption_root is the ORIGIN, not the clone")

	// It has no key of its own: it can be neither unlocked nor re-keyed directly.
	require.Error(t, mock.DatasetChangeKey(ctx, clone, "new-pass-456"),
		"an inheriting child cannot be re-keyed")
	require.Error(t, mock.DatasetLock(ctx, clone), "locking belongs to the encryption root")

	// Locking the ORIGIN locks the clone with it.
	require.NoError(t, mock.DatasetLock(ctx, origin))
	cloned, err = mock.DatasetGet(ctx, clone)
	require.NoError(t, err)
	assert.True(t, cloned.Locked, "P-7: locking the origin locks the clone")
	assert.Equal(t, "", cloned.Mountpoint, "P-4: a locked dataset has no mountpoint")

	require.NoError(t, mock.DatasetUnlock(ctx, origin, "drill-pass-1"))
	cloned, err = mock.DatasetGet(ctx, clone)
	require.NoError(t, err)
	assert.False(t, cloned.Locked)
}

// TestMockChangeKeyToSamePassphraseSucceeds pins the probe that makes the
// driver's rotation-completion arm safe: change_key on an UNLOCKED dataset with
// a passphrase IDENTICAL to the current one returns SUCCESS and the key stays
// valid (probed live on nas01 26.0.0-BETA.1, 2026-08-02: same-key change_key
// SUCCESS, followed by lock -> unlock with that same passphrase SUCCESS). The
// driver calls change_key unconditionally when a rotation window is open, so an
// interrupted rotation completes and an already-rotated one is a no-op by
// outcome. scripts/gf1-encryption-drill.sh step 5b re-proves this end to end.
func TestMockChangeKeyToSamePassphraseSucceeds(t *testing.T) {
	ctx := context.Background()
	mock := NewMockClient()
	const name = "flashstor/gf1-enc-drill-zv"
	const passphrase = "drill-pass-1"

	_, err := mock.DatasetCreate(ctx, &DatasetCreateParams{
		Name: name, Type: "VOLUME", Volsize: 1 << 30, Sparse: true,
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: passphrase},
	})
	require.NoError(t, err)

	require.NoError(t, mock.DatasetChangeKey(ctx, name, passphrase), "same-key change_key succeeds")
	require.NoError(t, mock.DatasetLock(ctx, name))
	require.NoError(t, mock.DatasetUnlock(ctx, name, passphrase), "the key is still valid afterward")
}

// TestMockKeyMaterialStaysOffTheDatasetStruct is the F16 guard: no *Dataset the
// driver can hold ever carries a passphrase, so a %+v in a failing test cannot
// print one. The mock keeps its key model in its own side table.
func TestMockKeyMaterialStaysOffTheDatasetStruct(t *testing.T) {
	ctx := context.Background()
	mock := NewMockClient()
	const name = "flashstor/gf1-enc-drill-zv"
	const passphrase = "drill-pass-radioactive"

	_, err := mock.DatasetCreate(ctx, &DatasetCreateParams{
		Name: name, Type: "VOLUME", Volsize: 1 << 30, Sparse: true,
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: passphrase},
	})
	require.NoError(t, err)
	ds, err := mock.DatasetGet(ctx, name)
	require.NoError(t, err)
	assert.NotContains(t, fmt.Sprintf("%+v", ds), passphrase,
		"no rendering of a *Dataset may contain key material")

	listed, err := mock.DatasetList(ctx, "flashstor", 0, 0)
	require.NoError(t, err)
	assert.NotContains(t, fmt.Sprintf("%+v", listed), passphrase)
}
