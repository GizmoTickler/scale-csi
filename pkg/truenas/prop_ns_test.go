package truenas

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The literal legacy namespace is deliberate: these tests pin the on-disk
// compatibility contract against constant drift.
const legacyTestNS = "truenas-csi:"

func TestNormalizeCSIUserPropertiesFoldsLegacyKeys(t *testing.T) {
	props := map[string]UserProperty{
		legacyTestNS + "managed_resource": {Value: "true", Source: "local"},
		legacyTestNS + "csi_volume_name":  {Value: "vol-1"},
		"org.freenas:description":         {Value: "untouched"},
		"scale-csi:provision_success":     {Value: "true", Source: "local"},
	}
	legacy := normalizeCSIUserProperties(props)

	assert.Equal(t, "true", props["scale-csi:managed_resource"].Value)
	assert.Equal(t, "local", props["scale-csi:managed_resource"].Source, "the fold preserves value AND source")
	assert.Equal(t, "vol-1", props["scale-csi:csi_volume_name"].Value)
	assert.Equal(t, "untouched", props["org.freenas:description"].Value, "foreign namespaces are never touched")
	_, legacyVisible := props[legacyTestNS+"managed_resource"]
	assert.False(t, legacyVisible, "legacy keys must not stay visible beside their canonical twins")

	require.Len(t, legacy, 2, "the raw legacy view feeds the migration sweep")
	assert.Equal(t, "true", legacy[legacyTestNS+"managed_resource"].Value)
	assert.Equal(t, "vol-1", legacy[legacyTestNS+"csi_volume_name"].Value)

	assert.Nil(t, normalizeCSIUserProperties(map[string]UserProperty{"scale-csi:x": {Value: "y"}}),
		"no legacy keys, no raw view")
}

func TestNormalizeCSIUserPropertiesCollisionPrecedence(t *testing.T) {
	// Tie (both local): canonical wins — every post-rename write is canonical,
	// so the canonical value is the newer one.
	props := map[string]UserProperty{
		"scale-csi:csi_volume_name":      {Value: "new", Source: "local"},
		legacyTestNS + "csi_volume_name": {Value: "old", Source: "local"},
	}
	normalizeCSIUserProperties(props)
	assert.Equal(t, "new", props["scale-csi:csi_volume_name"].Value)

	// Local legacy beats inherited canonical: a clone inheriting the origin's
	// migrated stamp must not shadow this dataset's own legacy stamp.
	props = map[string]UserProperty{
		"scale-csi:csi_volume_name":      {Value: "origin", Source: "pool/p/origin@base"},
		legacyTestNS + "csi_volume_name": {Value: "mine", Source: "local"},
	}
	normalizeCSIUserProperties(props)
	assert.Equal(t, "mine", props["scale-csi:csi_volume_name"].Value)
	assert.Equal(t, "local", props["scale-csi:csi_volume_name"].Source)

	// Sourceless tie (the 26.0 resource-query shape): canonical wins.
	props = map[string]UserProperty{
		"scale-csi:csi_volume_name":      {Value: "new"},
		legacyTestNS + "csi_volume_name": {Value: "old"},
	}
	normalizeCSIUserProperties(props)
	assert.Equal(t, "new", props["scale-csi:csi_volume_name"].Value)
}

func TestExpandCSIPropertyRemovalKeys(t *testing.T) {
	expanded := expandCSIPropertyRemovalKeys([]string{
		"scale-csi:managed_resource",    // canonical: gains its legacy twin
		legacyTestNS + "tombstone_ff00", // legacy: passes through unchanged
		"org.freenas:description",       // foreign: passes through unchanged
		"scale-csi:managed_resource",    // duplicate: deduped
	})
	assert.ElementsMatch(t, []string{
		"scale-csi:managed_resource",
		legacyTestNS + "managed_resource",
		legacyTestNS + "tombstone_ff00",
		"org.freenas:description",
	}, expanded)
}

func TestCanonicalAndLegacyCSIPropertyKeyRoundTrip(t *testing.T) {
	legacyTwin, ok := LegacyCSIPropertyKey("scale-csi:csi_snapshot_name")
	require.True(t, ok)
	assert.Equal(t, legacyTestNS+"csi_snapshot_name", legacyTwin)
	canonical, ok := CanonicalCSIPropertyKey(legacyTwin)
	require.True(t, ok)
	assert.Equal(t, "scale-csi:csi_snapshot_name", canonical)

	_, ok = LegacyCSIPropertyKey("org.freenas:description")
	assert.False(t, ok)
	_, ok = CanonicalCSIPropertyKey("scale-csi:already_canonical")
	assert.False(t, ok)
}

// TestParseSnapshotFoldsLegacyWireKeys pins the interface-decoder leg: a
// legacy-stamped snapshot arriving through the 26.0 flat wire shape reads back
// under canonical keys.
func TestParseSnapshotFoldsLegacyWireKeys(t *testing.T) {
	snap, err := parseSnapshot(map[string]interface{}{
		"name":          "pool/p/vol@snap-1",
		"dataset":       "pool/p/vol",
		"snapshot_name": "snap-1",
		"user_properties": map[string]interface{}{
			legacyTestNS + "csi_snapshot_name": "snap-1",
			legacyTestNS + "managed_resource":  "true",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "snap-1", snap.UserProperties["scale-csi:csi_snapshot_name"].Value)
	assert.Equal(t, "true", snap.UserProperties["scale-csi:managed_resource"].Value)
	_, legacyVisible := snap.UserProperties[legacyTestNS+"csi_snapshot_name"]
	assert.False(t, legacyVisible)
}

func TestMockClientDualReadAndRemovalWidening(t *testing.T) {
	ctx := context.Background()
	m := NewMockClient()
	_, err := m.DatasetCreate(ctx, &DatasetCreateParams{Name: "pool/p/legacy", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.NoError(t, m.DatasetSetUserProperties(ctx, "pool/p/legacy", map[string]string{
		legacyTestNS + "managed_resource": "true",
		legacyTestNS + "csi_volume_name":  "legacy",
	}))

	// Reads fold; the store keeps the on-disk legacy spelling.
	ds, err := m.DatasetGet(ctx, "pool/p/legacy")
	require.NoError(t, err)
	assert.Equal(t, "true", ds.UserProperties["scale-csi:managed_resource"].Value)
	assert.Contains(t, ds.LegacyCSIProperties, legacyTestNS+"managed_resource")
	_, storedLegacy := m.Datasets["pool/p/legacy"].UserProperties[legacyTestNS+"managed_resource"]
	assert.True(t, storedLegacy, "the mock store models the on-disk state, which stays legacy until migrated")

	// The managed listing sees a legacy-stamped dataset (dual server-side filter).
	listed, err := m.DatasetList(ctx, "pool/p", 0, 0)
	require.NoError(t, err)
	require.Len(t, listed, 1)

	// A single-key getter dual-reads through the canonical name.
	value, err := m.DatasetGetUserProperty(ctx, "pool/p/legacy", "scale-csi:csi_volume_name")
	require.NoError(t, err)
	assert.Equal(t, "legacy", value)

	// Removing the canonical key removes the legacy spelling with it.
	require.NoError(t, m.DatasetRemoveUserProperties(ctx, "pool/p/legacy", []string{"scale-csi:managed_resource"}))
	_, still := m.Datasets["pool/p/legacy"].UserProperties[legacyTestNS+"managed_resource"]
	assert.False(t, still)
}
