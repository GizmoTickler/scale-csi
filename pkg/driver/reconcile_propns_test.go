package driver

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// legacyNS spells out the pre-rename namespace. Deliberately a literal (not
// derived from the constants): these tests pin the on-disk compatibility
// contract, which must not drift if someone edits the constants.
const legacyNS = "truenas-csi:"

func newPropNSTestDriver(t *testing.T) (*Driver, *truenas.MockClient) {
	t.Helper()
	client := truenas.NewMockClient()
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
		},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client)
	return d, client
}

// seedLegacyStampedVolume models a volume provisioned by a pre-rename release:
// every stamp sits under truenas-csi:* with a local source.
func seedLegacyStampedVolume(t *testing.T, client *truenas.MockClient, name string, extra map[string]string) string {
	t.Helper()
	ctx := context.Background()
	datasetName := "pool/parent/" + name
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	props := map[string]string{
		legacyNS + "managed_resource":   "true",
		legacyNS + "csi_volume_name":    name,
		legacyNS + "provision_success":  "true",
		legacyNS + "driver_instance_id": "csi.scale.io@pool/parent",
	}
	for key, value := range extra {
		props[key] = value
	}
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName, props))
	return datasetName
}

func managedDatasetsForSweep(t *testing.T, client *truenas.MockClient) []*truenas.Dataset {
	t.Helper()
	datasets, err := client.DatasetList(context.Background(), "pool/parent", 0, 0)
	require.NoError(t, err)
	return datasets
}

func TestMigrateLegacyPropertyNamespaceReStampsLocalProperties(t *testing.T) {
	ctx := context.Background()
	d, client := newPropNSTestDriver(t)
	datasetName := seedLegacyStampedVolume(t, client, "legacy-vol", map[string]string{
		legacyNS + "publication_deadbeef00000000": `{"v":1,"node":"worker-a","state":"published"}`,
	})

	report := &ReconcileReport{}
	d.migrateLegacyPropertyNamespace(ctx, managedDatasetsForSweep(t, client), report, 0)

	assert.Equal(t, []string{"legacy-vol"}, report.MigratedPropertyNamespaces)
	stored := client.Datasets[datasetName].UserProperties
	for _, suffix := range []string{"managed_resource", "csi_volume_name", "provision_success", "driver_instance_id", "publication_deadbeef00000000"} {
		legacyProp, legacyPresent := stored[legacyNS+suffix]
		assert.False(t, legacyPresent, "legacy %s%s must be removed from the store, still holds %+v", legacyNS, suffix, legacyProp)
		canonical, canonicalPresent := stored["scale-csi:"+suffix]
		require.True(t, canonicalPresent, "canonical scale-csi:%s must be stamped", suffix)
		assert.Equal(t, "local", canonical.Source, "migrated stamp must persist with a local source")
	}
	assert.Equal(t, "true", stored["scale-csi:managed_resource"].Value)
	assert.Equal(t, `{"v":1,"node":"worker-a","state":"published"}`, stored["scale-csi:publication_deadbeef00000000"].Value)

	// Idempotence: a second pass finds nothing left to migrate.
	rerun := &ReconcileReport{}
	d.migrateLegacyPropertyNamespace(ctx, managedDatasetsForSweep(t, client), rerun, 0)
	assert.Empty(t, rerun.MigratedPropertyNamespaces)
}

func TestMigrateLegacyPropertyNamespaceSkipsInheritedProperties(t *testing.T) {
	ctx := context.Background()
	d, client := newPropNSTestDriver(t)
	datasetName := "pool/parent/clone-vol"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	// Canonical local stamp makes the dataset managed; the legacy key models a
	// clone reading its ORIGIN SNAPSHOT's stamp (source = snapshot name, never
	// "local"). Writing it locally would forge ownership; the sweep must not.
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		"scale-csi:managed_resource": "true",
	}))
	client.Datasets[datasetName].UserProperties[legacyNS+"csi_volume_name"] = truenas.UserProperty{
		Value: "origin-vol", Source: "pool/parent/origin-vol@base",
	}

	report := &ReconcileReport{}
	d.migrateLegacyPropertyNamespace(ctx, managedDatasetsForSweep(t, client), report, 0)

	assert.Empty(t, report.MigratedPropertyNamespaces)
	stored := client.Datasets[datasetName].UserProperties
	inherited, present := stored[legacyNS+"csi_volume_name"]
	require.True(t, present, "the inherited legacy key must be left untouched")
	assert.Equal(t, "origin-vol", inherited.Value)
	_, canonicalPresent := stored["scale-csi:csi_volume_name"]
	assert.False(t, canonicalPresent, "an inherited value must never be forged into a local canonical stamp")
}

func TestMigrateLegacyPropertyNamespaceCollisionCanonicalWins(t *testing.T) {
	ctx := context.Background()
	d, client := newPropNSTestDriver(t)
	datasetName := seedLegacyStampedVolume(t, client, "collide-vol", nil)
	// A post-rename write already updated the canonical spelling; the stale
	// legacy twin must be dropped WITHOUT clobbering the newer canonical value.
	require.NoError(t, client.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		"scale-csi:csi_volume_name": "collide-vol-renamed",
	}))

	report := &ReconcileReport{}
	d.migrateLegacyPropertyNamespace(ctx, managedDatasetsForSweep(t, client), report, 0)

	assert.Equal(t, []string{"collide-vol"}, report.MigratedPropertyNamespaces)
	stored := client.Datasets[datasetName].UserProperties
	assert.Equal(t, "collide-vol-renamed", stored["scale-csi:csi_volume_name"].Value,
		"the newer canonical value must win the collision")
	_, legacyPresent := stored[legacyNS+"csi_volume_name"]
	assert.False(t, legacyPresent)
}

func TestMigrateLegacyPropertyNamespaceRespectsCapAndLock(t *testing.T) {
	ctx := context.Background()
	d, client := newPropNSTestDriver(t)
	seedLegacyStampedVolume(t, client, "cap-a", nil)
	seedLegacyStampedVolume(t, client, "cap-b", nil)
	seedLegacyStampedVolume(t, client, "cap-c", nil)

	report := &ReconcileReport{}
	d.migrateLegacyPropertyNamespace(ctx, managedDatasetsForSweep(t, client), report, 2)
	assert.Len(t, report.MigratedPropertyNamespaces, 2, "per-pass cap must bound the write volume")

	// A held per-volume lock defers that dataset to the next pass, no error.
	remaining := &ReconcileReport{}
	require.True(t, d.acquireOperationLock(volumeLockKey("cap-c")))
	d.migrateLegacyPropertyNamespace(ctx, managedDatasetsForSweep(t, client), remaining, 0)
	d.releaseOperationLock(volumeLockKey("cap-c"))
	total := append(append([]string{}, report.MigratedPropertyNamespaces...), remaining.MigratedPropertyNamespaces...)
	assert.Len(t, total, 2, "a locked volume is skipped without being reported migrated")

	final := &ReconcileReport{}
	d.migrateLegacyPropertyNamespace(ctx, managedDatasetsForSweep(t, client), final, 0)
	assert.Len(t, final.MigratedPropertyNamespaces, 1, "the deferred volume migrates once the lock is free")
}

// TestLegacyStampedVolumeRemainsFullyOperable is the dual-read end-to-end
// guard: a volume whose EVERY stamp still sits under truenas-csi:* must pass
// the ownership gates of live volume operations (here: DeleteVolume, whose
// managed/ownership checks read the canonical constants) without any
// migration having run.
func TestLegacyStampedVolumeRemainsFullyOperable(t *testing.T) {
	ctx := context.Background()
	d, client := newPropNSTestDriver(t)
	d.config.NFS = NFSConfig{ShareHost: "192.0.2.10"}
	seedLegacyStampedVolume(t, client, "legacy-live", nil)

	// ListVolumes must surface it (server-side dual filter + client safeguard).
	listResp, err := d.ListVolumes(ctx, &csi.ListVolumesRequest{})
	require.NoError(t, err)
	require.Len(t, listResp.Entries, 1)
	assert.Equal(t, "legacy-live", listResp.Entries[0].Volume.VolumeId)

	// DeleteVolume must recognize ownership through the legacy stamps.
	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "legacy-live"})
	require.NoError(t, err)
	_, present := client.Datasets["pool/parent/legacy-live"]
	assert.False(t, present, "the legacy-stamped volume must actually be deleted")
}

// TestReconcileOrphansWiresNamespaceMigration proves the sweep runs inside a
// real reconcile pass and its result lands in the report.
func TestReconcileOrphansWiresNamespaceMigration(t *testing.T) {
	ctx := context.Background()
	pv := boundReconcilePV("wired-vol", "csi.scale.io")
	d, client := newReconcileTestDriver(t, false, []runtime.Object{pv}, nil)
	mustCreateParentDataset(t, client)
	seedLegacyStampedVolume(t, client, "wired-vol", nil)

	report, err := d.ReconcileOrphans(ctx, ReconcileOptions{MinOrphanAge: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, []string{"wired-vol"}, report.MigratedPropertyNamespaces)
	assert.Equal(t, 1, report.MigratedPropertyNamespaceCount)
	stored := client.Datasets["pool/parent/wired-vol"].UserProperties
	assert.Equal(t, "true", stored["scale-csi:managed_resource"].Value)
	_, legacyPresent := stored[legacyNS+"managed_resource"]
	assert.False(t, legacyPresent)
}
