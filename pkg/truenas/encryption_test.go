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

// TestMockEncryptionLifecycle walks the full P-4/P-6/P-8 + D-1 state machine on the
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

	// D-1: a wrong passphrase is a SUCCESSFUL job whose payload reports the
	// failure; the dataset stays locked either way.
	err = mock.DatasetUnlock(ctx, name, "wrong-pass")
	require.Error(t, err)
	stillLocked, _ := mock.DatasetGet(ctx, name)
	assert.True(t, stillLocked.Locked, "a wrong passphrase leaves the dataset locked")

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
// (datasets array + toggle_attachments:true) and that a failed unlock (a wrong
// passphrase) surfaces as an error.
func TestDatasetUnlockJobShape(t *testing.T) {
	const dataset = "flashstor/gf1-enc-drill-zv"

	// The drill-measured result payloads (nas01 26.0.0-BETA.1, 2026-08-02).
	correctResult := map[string]interface{}{
		"unlocked": []interface{}{dataset},
		"failed":   map[string]interface{}{},
	}
	wrongResult := map[string]interface{}{
		"unlocked": []interface{}{},
		"failed": map[string]interface{}{
			dataset: map[string]interface{}{"error": "Invalid Key", "skipped": []interface{}{}},
		},
	}

	t.Run("payload matches the P-4 shape", func(t *testing.T) {
		var unlockArgs []interface{}
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			datasetUnlockMethod: func(req rpcTestRequest) (interface{}, *rpcError) {
				unlockArgs = req.Params
				return float64(9101), nil
			},
			"core.get_jobs": static([]interface{}{map[string]interface{}{
				"id": float64(9101), "state": "SUCCESS", "result": correctResult,
			}}),
			"core.subscribe": static("sub-1"),
		})

		require.NoError(t, client.DatasetUnlock(context.Background(), dataset, "drill-pass-1"))
		require.Len(t, unlockArgs, 2)
		assert.Equal(t, dataset, unlockArgs[0])
		options, ok := unlockArgs[1].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, true, options["toggle_attachments"], "P-4: re-run attachments on unlock")
		// Over the wire the datasets array JSON-decodes to []interface{}.
		datasets, ok := options["datasets"].([]interface{})
		require.True(t, ok)
		require.Len(t, datasets, 1)
		entry, ok := datasets[0].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, dataset, entry["name"])
		assert.Equal(t, "drill-pass-1", entry["passphrase"])
	})

	// D-1, THE BLOCKER THE LIVE DRILL FOUND. A wrong passphrase is a SUCCESSFUL
	// job; the failure exists only in the result payload. Reading the job STATE
	// (the design's P-5 claim) made a wrong key indistinguishable from a correct
	// one — a fail-OPEN publish, and a rotation arm that could never be reached.
	//
	// PRE-FIX PROOF: on d2e7b38 DatasetUnlock ignores the result entirely and
	// returns nil here.
	t.Run("wrong passphrase is a SUCCESSFUL job whose payload reports failure (D-1)", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			datasetUnlockMethod: static(float64(9102)),
			"core.get_jobs": static([]interface{}{map[string]interface{}{
				"id": float64(9102), "state": "SUCCESS", "result": wrongResult,
			}}),
			"core.subscribe": static("sub-1"),
		})
		err := client.DatasetUnlock(context.Background(), dataset, "wrong-pass")
		require.Error(t, err, "a SUCCESSFUL job with a failed payload must NOT read as an unlock")
		assert.Contains(t, err.Error(), "Invalid Key", "the backend's own reason is surfaced")
		assert.Contains(t, err.Error(), dataset)
	})

	t.Run("a hard job failure is still an error", func(t *testing.T) {
		client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
			datasetUnlockMethod: static(float64(9103)),
			"core.get_jobs": static([]interface{}{map[string]interface{}{
				"id": float64(9103), "state": "FAILED", "error": "boom",
			}}),
			"core.subscribe": static("sub-1"),
		})
		err := client.DatasetUnlock(context.Background(), dataset, "drill-pass-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "job 9103")
	})

	t.Run("an unreadable or silent payload fails CLOSED", func(t *testing.T) {
		for name, result := range map[string]interface{}{
			"nil result":            nil,
			"non-object result":     "unlocked",
			"names nothing":         map[string]interface{}{"unlocked": []interface{}{}, "failed": map[string]interface{}{}},
			"unlocks another name":  map[string]interface{}{"unlocked": []interface{}{"flashstor/other"}, "failed": map[string]interface{}{}},
			"failed with no reason": map[string]interface{}{"unlocked": []interface{}{dataset}, "failed": map[string]interface{}{dataset: "?"}},
		} {
			t.Run(name, func(t *testing.T) {
				client := gf5TestClient(t, map[string]func(rpcTestRequest) (interface{}, *rpcError){
					datasetUnlockMethod: static(float64(9104)),
					"core.get_jobs": static([]interface{}{map[string]interface{}{
						"id": float64(9104), "state": "SUCCESS", "result": result,
					}}),
					"core.subscribe": static("sub-1"),
				})
				require.Error(t, client.DatasetUnlock(context.Background(), dataset, "drill-pass-1"),
					"no positive evidence of an unlock must never read as success")
			})
		}
	})
}

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

	// It has no key of its own to LOAD: lock/unlock belong to the encryption root.
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

// TestDatasetEncryptionIdentityParse pins the P-10 wire shape on the
// pool.dataset.query path and the P-11 absence on zfs.resource.query, for BOTH
// decoders (they must stay deep-equal).
//
// P-10 (nas01 26.0.0-BETA.1, 2026-08-02): pool.dataset.query returns
// encryption_root as a PLAIN STRING naming the encryption ROOT — a child of an
// encrypted parent reports the PARENT, a self-keyed dataset reports ITSELF — and
// key_format as a PROPERTY DICT whose value is "PASSPHRASE".
// P-11 (same date): zfs.resource.query returns NO encryption/key/lock fields at
// all.
func TestDatasetEncryptionIdentityParse(t *testing.T) {
	payload := []byte(`[
	  {"id":"flashstor/self","name":"flashstor/self","encrypted":true,"locked":false,"key_loaded":true,
	   "encryption_root":"flashstor/self","key_format":{"value":"PASSPHRASE","rawvalue":"passphrase","parsed":"passphrase","source":"LOCAL"}},
	  {"id":"flashstor/parent/child","name":"flashstor/parent/child","encrypted":true,"locked":false,"key_loaded":true,
	   "encryption_root":"flashstor/parent","key_format":{"value":"PASSPHRASE"}},
	  {"id":"flashstor/plain","name":"flashstor/plain","encrypted":false,"encryption_root":null,"key_format":null}
	]`)

	var generic []interface{}
	require.NoError(t, json.Unmarshal(payload, &generic))
	fromMap := make([]*Dataset, 0, len(generic))
	for _, item := range generic {
		ds, err := parseDataset(item)
		require.NoError(t, err)
		fromMap = append(fromMap, ds)
	}

	var raw []*rawDataset
	require.NoError(t, json.Unmarshal(payload, &raw))
	fromTyped := rawDatasetsToDatasets(raw, false)

	require.Equal(t, fromMap, fromTyped, "the two decoders must stay deep-equal on the encryption fields")

	assert.Equal(t, "flashstor/self", fromMap[0].EncryptionRoot, "a self-keyed dataset reports ITSELF")
	assert.Equal(t, KeyFormatPassphrase, fromMap[0].KeyFormat)
	assert.Equal(t, "flashstor/parent", fromMap[1].EncryptionRoot, "a child reports its PARENT")
	assert.Equal(t, KeyFormatPassphrase, fromMap[1].KeyFormat)
	assert.Equal(t, "", fromMap[2].EncryptionRoot)
	assert.Equal(t, "", fromMap[2].KeyFormat)

	t.Run("an unexpected key_format shape degrades to empty, never a decode failure", func(t *testing.T) {
		odd := []byte(`[{"id":"flashstor/odd","name":"flashstor/odd","encrypted":true,"key_format":["PASSPHRASE"],"encryption_root":{"value":"x"}}]`)
		var oddRaw []*rawDataset
		require.NoError(t, json.Unmarshal(odd, &oddRaw), "a shape surprise must not fail the whole response")
		got := rawDatasetsToDatasets(oddRaw, false)
		require.Len(t, got, 1)
		assert.Equal(t, "", got[0].KeyFormat)
		assert.Equal(t, "", got[0].EncryptionRoot)
	})

	t.Run("the resource path carries no encryption fields (P-11)", func(t *testing.T) {
		resourcePayload := []byte(`[{"name":"flashstor/self","user_properties":{"truenas-csi:encryption":"AES-256-GCM"}}]`)
		var resourceRaw []*rawDataset
		require.NoError(t, json.Unmarshal(resourcePayload, &resourceRaw))
		resourceDatasets := rawDatasetsToDatasets(resourceRaw, true)
		require.Len(t, resourceDatasets, 1)
		assert.False(t, resourceDatasets[0].Encrypted)
		assert.Equal(t, "", resourceDatasets[0].EncryptionRoot)
		assert.Equal(t, "", resourceDatasets[0].KeyFormat)
	})
}

// TestMockInheritedEncryption models P-10 inheritance: a dataset created with no
// encryption of its own under an ENCRYPTED ancestor comes out encrypted with the
// ANCESTOR as its root. Without this the encrypted-parent deployment — the one
// the driver used to destroy data on — could not be tested at all.
func TestMockInheritedEncryption(t *testing.T) {
	ctx := context.Background()
	mock := NewMockClient()
	_, err := mock.DatasetCreate(ctx, &DatasetCreateParams{
		Name: "flashstor/parent", Type: "FILESYSTEM",
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "parent-pass-1"},
	})
	require.NoError(t, err)
	child, err := mock.DatasetCreate(ctx, &DatasetCreateParams{Name: "flashstor/parent/child", Type: "FILESYSTEM"})
	require.NoError(t, err)

	assert.True(t, child.Encrypted, "P-10: encryption is inherited")
	assert.Equal(t, "flashstor/parent", child.EncryptionRoot, "the root is the PARENT, not the child")
	assert.Equal(t, KeyFormatPassphrase, child.KeyFormat)
	assert.False(t, child.Locked)

	// And a dataset outside the encrypted subtree stays plaintext.
	other, err := mock.DatasetCreate(ctx, &DatasetCreateParams{Name: "flashstor/plain", Type: "FILESYSTEM"})
	require.NoError(t, err)
	assert.False(t, other.Encrypted)
}

// TestMockChangeKeyPromotesInheritingChild pins D-2 in the mock: the live drill
// (2026-08-02) measured change_key on a child whose encryption_root is its
// PARENT as SUCCEEDING and silently promoting the child to its own encryption
// root — the opposite of the design's "ZFS refuses it" premise. The driver's
// ownsKey gate is therefore the ONLY thing standing between a rotation window
// and a clone severed from its origin key.
func TestMockChangeKeyPromotesInheritingChild(t *testing.T) {
	ctx := context.Background()
	mock := NewMockClient()
	const parent = "flashstor/gf1-parent"
	const child = "flashstor/gf1-parent/child"

	_, err := mock.DatasetCreate(ctx, &DatasetCreateParams{
		Name: parent, Type: "FILESYSTEM",
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "parent-pass-1"},
	})
	require.NoError(t, err)
	inherited, err := mock.DatasetCreate(ctx, &DatasetCreateParams{Name: child, Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.Equal(t, parent, inherited.EncryptionRoot)

	require.NoError(t, mock.DatasetChangeKey(ctx, child, "child-pass-2"),
		"D-2: TrueNAS 26.0 does NOT refuse this")

	after, err := mock.DatasetGet(ctx, child)
	require.NoError(t, err)
	assert.Equal(t, child, after.EncryptionRoot,
		"D-2: the child is silently promoted to its OWN encryption root, severed from the parent key")

	// And it is now opened by its own new key, not the parent's.
	require.NoError(t, mock.DatasetLock(ctx, child))
	require.Error(t, mock.DatasetUnlock(ctx, child, "parent-pass-1"))
	require.NoError(t, mock.DatasetUnlock(ctx, child, "child-pass-2"))
}

// TestMockEncryptionSummaryIsEmptyForANonRoot pins a backend fact drill #3
// measured directly on nas01 (2026-08-03):
//
//	pool.dataset.encryption_summary <non-encryption-root>  ->  []
//	pool.dataset.encryption_summary <real encryption root> ->  [{"name": ...}]
//
// The driver's exact-name match turns that empty list into a fail-closed error,
// which is what stopped the O-1 divergence on hardware. It is modeled here so the
// fail-closed path has something real to be tested against — and so nothing
// mistakes "the appliance answered" for "the appliance answered about THIS
// dataset". The driver's own ownership gate must not depend on it: see
// ModelEncryptionSummaryForInheritedKeys, which exists purely to switch this off
// in the test that proves the gate stands alone.
func TestMockEncryptionSummaryIsEmptyForANonRoot(t *testing.T) {
	ctx := context.Background()
	mock := NewMockClient()
	const origin = "flashstor/o1-origin"
	const clone = "flashstor/o1-clone"

	_, err := mock.DatasetCreate(ctx, &DatasetCreateParams{
		Name: origin, Type: "VOLUME", Volsize: 1 << 30, Sparse: true,
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "origin-pass-1"},
	})
	require.NoError(t, err)
	_, err = mock.SnapshotCreate(ctx, origin, "snap", nil)
	require.NoError(t, err)
	require.NoError(t, mock.SnapshotClone(ctx, origin+"@snap", clone))

	rootSummary, err := mock.DatasetEncryptionSummary(ctx, origin)
	require.NoError(t, err)
	require.Len(t, rootSummary, 1, "a real encryption root answers for itself")
	assert.Equal(t, origin, rootSummary[0].Name)

	cloneSummary, err := mock.DatasetEncryptionSummary(ctx, clone)
	require.NoError(t, err)
	assert.Empty(t, cloneSummary,
		"drill #3: a dataset that is not its own encryption root gets an EMPTY list, not a row")

	// And the deliberate stub, which must never be mistaken for reality.
	mock.ModelEncryptionSummaryForInheritedKeys = true
	stubbed, err := mock.DatasetEncryptionSummary(ctx, clone)
	require.NoError(t, err)
	require.Len(t, stubbed, 1, "the knob exists only to isolate the driver's own gate in a test")
}
