package truenas

import (
	"context"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The GF1 re-drill's D-3 gate, in three layers.
//
// D-3 (2026-08-03, nas01 26.0.0-BETA.1): Client.DatasetGet sent a restricted
// extra.properties projection and TrueNAS 26.0 then OMITTED encrypted, locked,
// key_format and encryption_root from the response entirely, so every "wire
// truth" encryption predicate evaluated on zero values — silently, and with a
// fully green unit suite, because MockClient returned fully-populated structs.
//
//	layer 1  the projection the client SENDS is pinned (assertPoolQueryProjection)
//	layer 2  a fake appliance that reproduces the drill's MEASURED shapes decides
//	         what to return from the projection it receives, so the client's real
//	         request is what makes the fields appear
//	layer 3  projectDatasetLikePoolQuery gives MockClient the same behavior, so
//	         driver-level predicate tests inherit the gate
//
// Every one of these fails on a micro-revert of datasetEncryptionQueryProperties
// out of datasetQueryProperties — which is the pre-fix state, verbatim.

// poolQueryProjectionWant is the projection every pool.dataset.query read must
// carry. It is written out in full, not derived from the code under test, so a
// silent drop is a failing test rather than a self-consistent tautology.
var poolQueryProjectionWant = []string{
	"used", "available", "quota", "refquota", "referenced", "usedbysnapshots",
	"reservation", "refreservation", "volsize", "volblocksize", "creation",
	// origin: read by datasetOriginSnapshotID (promote candidate + revalidation,
	// in-flight clone remnant identity, detached-copy remnant guard).
	"origin",
	// the measured encryption set (re-drill shape C).
	"encryption", "keyformat", "encryptionroot", "keystatus",
}

func assertPoolQueryProjection(t *testing.T, got interface{}) {
	t.Helper()
	want := make([]interface{}, 0, len(poolQueryProjectionWant))
	for _, property := range poolQueryProjectionWant {
		want = append(want, property)
	}
	assert.Equal(t, want, got,
		"the pool.dataset.query projection decides which fields exist on the wire (re-drill D-3)")
}

// datasetQueryAppliance is a fake TrueNAS 26.0 that reproduces the re-drill's
// MEASURED response shapes: it returns the always-present core, adds a property
// only when the request projected it, and emits the encryption block only when
// the whole measured encryption property set was projected (shape B vs shape C
// of the report's root-cause table).
//
// It is deliberately NOT told what the driver asks for — it reads the request.
func datasetQueryAppliance(t *testing.T, seen chan<- []interface{}) func(*websocket.Conn) {
	t.Helper()
	return func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "pool.dataset.query":
				options, _ := req.Params[1].(map[string]interface{})
				extra, _ := options["extra"].(map[string]interface{})
				projected, _ := extra["properties"].([]interface{})
				select {
				case seen <- projected:
				default:
				}
				requested := func(name string) bool {
					for _, item := range projected {
						if item == name {
							return true
						}
					}
					return false
				}
				row := map[string]interface{}{
					"id":         "flashstor/parent/gf1v-enc1",
					"name":       "flashstor/parent/gf1v-enc1",
					"pool":       "flashstor",
					"type":       "VOLUME",
					"mountpoint": "",
					"user_properties": map[string]interface{}{
						"truenas-csi:managed_resource": map[string]interface{}{"value": "true", "source": "LOCAL"},
					},
				}
				if requested("origin") {
					row["origin"] = map[string]interface{}{
						"value":    "flashstor/parent/src@snap-1",
						"parsed":   "flashstor/parent/src@snap-1",
						"rawvalue": "flashstor/parent/src@snap-1",
						"source":   "LOCAL",
					}
				}
				encryptionProjected := true
				for _, property := range datasetEncryptionQueryProperties {
					if !requested(property) {
						encryptionProjected = false
					}
				}
				if encryptionProjected {
					// Shape C, verbatim from the drill.
					row["encrypted"] = true
					row["locked"] = true
					row["key_loaded"] = false
					row["encryption_root"] = "flashstor/parent/gf1v-enc1"
					row["key_format"] = map[string]interface{}{
						"value": "PASSPHRASE", "rawvalue": "passphrase", "parsed": "passphrase", "source": "LOCAL",
					}
				}
				// Shape B: the keys are simply ABSENT — not null, not false. That is
				// what makes the Go zero value indistinguishable from an answer.
				resp.Result = []interface{}{row}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	}
}

// TestDatasetGetProjectionCarriesEncryptionIdentity is the D-3 root-cause
// regression at the WIRE level: the fake appliance emits the encryption block
// only for a request that projects the measured property set, so this passes
// only because the real client asks for it.
//
// PRE-FIX PROOF: drop datasetEncryptionQueryProperties from
// datasetQueryProperties (the a0315c3 state) and the appliance returns shape B —
// Encrypted false, EncryptionRoot "", KeyFormat "" — and this FAILS.
func TestDatasetGetProjectionCarriesEncryptionIdentity(t *testing.T) {
	mock := newMockWSServer()
	seen := make(chan []interface{}, 1)
	server := mock.start(datasetQueryAppliance(t, seen))
	defer mock.close()
	client := newSnapshotTestClient(t, server.URL)

	ds, err := client.DatasetGet(context.Background(), "flashstor/parent/gf1v-enc1")
	require.NoError(t, err)

	assertPoolQueryProjection(t, <-seen)

	assert.True(t, ds.Encrypted, "the wire says encrypted; the projection must let the driver see it")
	assert.True(t, ds.Locked, "a locked volume must be visible as locked (publish + reconciler + health)")
	assert.Equal(t, "flashstor/parent/gf1v-enc1", ds.EncryptionRoot, "self-keyed: the root is the dataset itself")
	assert.Equal(t, KeyFormatPassphrase, ds.KeyFormat)
	assert.Equal(t, "flashstor/parent/src@snap-1", datasetPropertyString(ds.Origin),
		"origin has real readers (promote, remnant identity) and must survive the projection too")
}

// TestDatasetGetByNamesCarriesEncryptionIdentity pins the same thing for the
// batched read the unlock reconciler uses to decide whether a volume is locked.
// It is a SEPARATE call site with its own options map, and it is the one that
// decides whether a fleet comes back after an appliance reboot.
func TestDatasetGetByNamesCarriesEncryptionIdentity(t *testing.T) {
	mock := newMockWSServer()
	seen := make(chan []interface{}, 1)
	server := mock.start(datasetQueryAppliance(t, seen))
	defer mock.close()
	client := newSnapshotTestClient(t, server.URL)

	byName, err := client.DatasetGetByNames(context.Background(), []string{"flashstor/parent/gf1v-enc1"})
	require.NoError(t, err)
	assertPoolQueryProjection(t, <-seen)

	ds := byName["flashstor/parent/gf1v-enc1"]
	require.NotNil(t, ds)
	assert.True(t, ds.Locked, "the reconciler's locked gate reads THIS field")
	assert.True(t, ds.Encrypted)
	assert.Equal(t, KeyFormatPassphrase, ds.KeyFormat)
}

// TestResourceQueryProjectionOmitsEncryption pins the deliberate asymmetry: P-11
// measured that zfs.resource.query returns NO encryption, key or lock fields at
// all and parseDatasetResource reads none, so asking for them there would be an
// unverified request shape for a response the path cannot deliver. origin IS
// asked for, because that path's decoder reads it and the promote candidate
// nomination depends on it.
func TestResourceQueryProjectionOmitsEncryption(t *testing.T) {
	options := datasetResourceQueryOptions([]string{"flashstor/parent"}, true)
	properties, ok := options["properties"].([]string)
	require.True(t, ok)
	assert.Contains(t, properties, "origin")
	for _, property := range datasetEncryptionQueryProperties {
		assert.NotContains(t, properties, property,
			"zfs.resource.query carries no encryption fields (P-11); asking is an unverified shape")
	}
}

// TestProjectDatasetLikePoolQueryModelsTheWire is the projection MODEL's own
// gate: given the current projection it preserves what the wire carries, and
// given the pre-fix projection it zeroes exactly the fields the drill measured
// missing. This is what gives MockClient.ModelQueryProjection its teeth.
func TestProjectDatasetLikePoolQueryModelsTheWire(t *testing.T) {
	full := &Dataset{
		Name: "flashstor/parent/gf1v-enc1", ID: "flashstor/parent/gf1v-enc1",
		Pool: "flashstor", Type: "VOLUME", Mountpoint: "/mnt/x",
		Volsize:             DatasetProperty{Parsed: float64(1 << 30)},
		Origin:              DatasetProperty{Parsed: "flashstor/parent/src@snap-1"},
		Encrypted:           true,
		Locked:              true,
		KeyLoaded:           false,
		EncryptionRoot:      "flashstor/parent/gf1v-enc1",
		KeyFormat:           KeyFormatPassphrase,
		EncryptionAlgorithm: "AES-256-GCM",
		UserProperties:      map[string]UserProperty{"truenas-csi:encryption": {Value: "AES-256-GCM", Source: "local"}},
	}

	t.Run("current projection preserves the identity fields", func(t *testing.T) {
		got := projectDatasetLikePoolQuery(full, datasetQueryProperties)
		assert.True(t, got.Encrypted)
		assert.True(t, got.Locked)
		assert.Equal(t, "flashstor/parent/gf1v-enc1", got.EncryptionRoot)
		assert.Equal(t, KeyFormatPassphrase, got.KeyFormat)
		assert.Equal(t, "flashstor/parent/src@snap-1", datasetPropertyString(got.Origin))
		assert.Equal(t, "AES-256-GCM", got.UserProperties["truenas-csi:encryption"].Value,
			"user_properties survive every projection (measured)")
		assert.Equal(t, "", got.EncryptionAlgorithm,
			"no wire decoder ever sets EncryptionAlgorithm; only the mock did")
	})

	t.Run("the pre-fix projection reproduces the measured shape B", func(t *testing.T) {
		got := projectDatasetLikePoolQuery(full, datasetQueryPropertiesBase)
		assert.False(t, got.Encrypted, "shape B: encrypted absent -> zero value")
		assert.False(t, got.Locked)
		assert.Equal(t, "", got.EncryptionRoot)
		assert.Equal(t, "", got.KeyFormat)
		assert.Equal(t, "", datasetPropertyString(got.Origin))
		assert.Equal(t, "flashstor/parent/gf1v-enc1", got.Name, "identity keys are always present")
		assert.Equal(t, "AES-256-GCM", got.UserProperties["truenas-csi:encryption"].Value)
	})

	t.Run("it never mutates its argument", func(t *testing.T) {
		_ = projectDatasetLikePoolQuery(full, datasetQueryPropertiesBase)
		assert.True(t, full.Encrypted)
		assert.Equal(t, KeyFormatPassphrase, full.KeyFormat)
	})
}

// TestMockClientProjectionModeZeroesUnprojectedFields pins the mock mode itself:
// with the CURRENT projection an encrypted dataset reads back encrypted, and
// with a micro-reverted projection it reads back as the hardware did — plaintext
// to every predicate.
func TestMockClientProjectionModeZeroesUnprojectedFields(t *testing.T) {
	ctx := context.Background()
	mock := NewMockClient()
	mock.ModelQueryProjection = true
	_, err := mock.DatasetCreate(ctx, &DatasetCreateParams{
		Name: "flashstor/parent/enc", Type: "VOLUME", Volsize: 1 << 30,
		Encryption:        boolPtr(true),
		InheritEncryption: boolPtr(false),
		EncryptionOptions: &EncryptionOptions{Algorithm: "AES-256-GCM", Passphrase: "unlock-me-123"},
	})
	require.NoError(t, err)

	ds, err := mock.DatasetGet(ctx, "flashstor/parent/enc")
	require.NoError(t, err)
	assert.True(t, ds.Encrypted, "the current projection carries the encryption block")
	assert.Equal(t, "flashstor/parent/enc", ds.EncryptionRoot)
	assert.Equal(t, KeyFormatPassphrase, ds.KeyFormat)

	// The un-modeled mock is what hid D-3: it answers regardless of the projection.
	plain := NewMockClient()
	plain.Datasets["flashstor/parent/enc"] = &Dataset{
		Name: "flashstor/parent/enc", Encrypted: true, EncryptionRoot: "flashstor/parent/enc",
		KeyFormat: KeyFormatPassphrase, UserProperties: map[string]UserProperty{},
	}
	unmodeled, err := plain.DatasetGet(ctx, "flashstor/parent/enc")
	require.NoError(t, err)
	assert.True(t, unmodeled.Encrypted)
}
