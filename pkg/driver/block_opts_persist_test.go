package driver

import (
	"context"
	"path"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// These tests exist because the block-tuning immutability guard shipped with a
// blind spot: EVERY existing guard test injected withBlockOpts, so no test ever
// drove a rebuild/publish on a PLAIN context — which is exactly what
// ControllerPublishVolume, the startup attachment reconcile, and a DR/restore
// rebuild do. Each test below is therefore written to be REVERT-SENSITIVE: it
// must fail if the stored-property resolution is removed and the code falls
// back to the request-context-only assumption.

func intPtr(v int) *int    { return &v }
func boolPtr(v bool) *bool { return &v }

// newPersistISCSIDriver builds an iSCSI driver over the in-memory mock whose
// controller default blocksize is 512 — the value a lost resolution collapses
// to, and therefore the value these tests assert must NOT appear.
func newPersistISCSIDriver(t *testing.T) (*Driver, *truenas.MockClient) {
	t.Helper()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS:   ZFSConfig{DatasetParentName: "tank/csi"},
			ISCSI: ISCSIConfig{Enabled: true, TargetPortal: "192.0.2.10:3260", ExtentBlocksize: 512, ExtentRpm: "SSD"},
		},
		truenasClient: client,
	}
	return d, client
}

// stampBlockProps writes stored block-tuning properties onto a dataset, standing
// in for what CreateVolume's fatal managed-property update persisted.
func stampBlockProps(t *testing.T, client *truenas.MockClient, datasetName string, props map[string]string) {
	t.Helper()
	require.NoError(t, client.DatasetSetUserProperties(context.Background(), datasetName, props))
}

// TestRebuildWithoutRequestOptsUsesStoredBlocksize is the F-2 CRITICAL
// regression test: a DR/restore rebuild whose extent is ABSENT and whose context
// carries NO StorageClass opts must re-create the extent at the volume's OWN
// stored geometry.
//
// Before the fix nothing was compared (there is no extent to compare against)
// and the create fell through to the 512 controller default — silently laying a
// 512-byte logical block geometry over a filesystem written against 4096-byte
// blocks, and returning success.
func TestRebuildWithoutRequestOptsUsesStoredBlocksize(t *testing.T) {
	d, client := newPersistISCSIDriver(t)
	ctx := context.Background()
	datasetName := "tank/csi/pvc-4k-restore"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME"})
	require.NoError(t, err)
	stampBlockProps(t, client, datasetName, map[string]string{
		PropBlockISCSIBlocksize:  "4096",
		PropBlockISCSIPblocksize: "true",
	})

	// The extent does NOT exist: this is the restored-volume / fencing-rebuild
	// heal path. Drive it through the real publish entry point on a PLAIN ctx.
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-4k-restore", nil))

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NotNil(t, extent, "the rebuild must have created the extent")
	assert.Equal(t, 4096, extent.Blocksize,
		"a rebuild with no request opts must re-create the extent at the volume's STORED blocksize; "+
			"512 here means the controller default was silently written over 4096-geometry data")
	require.NotNil(t, extent.Pblocksize)
	assert.True(t, *extent.Pblocksize, "stored pblocksize must be re-applied on rebuild")
}

// TestPublishWithoutRequestOptsOnStored4096Succeeds is the F-1 HIGH regression
// test: a publish / startup-reconcile that supplies no opts against a volume
// whose extent legitimately exists at 4096 must succeed.
//
// Before the fix the guard compared the live 4096 against the 512 controller
// default and returned FailedPrecondition on every attach, making a tuned volume
// permanently unattachable after the first pod reschedule.
func TestPublishWithoutRequestOptsOnStored4096Succeeds(t *testing.T) {
	d, client := newPersistISCSIDriver(t)
	ctx := context.Background()
	datasetName := "tank/csi/pvc-4k-attached"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME"})
	require.NoError(t, err)

	shareName := d.iscsiShareName(path.Base(datasetName))
	target, err := client.ISCSITargetCreate(ctx, shareName, "", "ISCSI", nil)
	require.NoError(t, err)
	extent, err := client.ISCSIExtentCreate(ctx, shareName, "zvol/"+datasetName, "", 4096, true, "SSD")
	require.NoError(t, err)
	stampBlockProps(t, client, datasetName, map[string]string{
		PropISCSITargetID:        strconv.Itoa(target.ID),
		PropISCSIExtentID:        strconv.Itoa(extent.ID),
		PropBlockISCSIBlocksize:  "4096",
		PropBlockISCSIPblocksize: "true",
	})

	// Plain ctx — exactly what controller.go's ControllerPublishVolume and
	// startup_reconcile.go pass.
	err = iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-4k-attached", nil)
	require.NoError(t, err, "a no-opts publish of a stored-4096 volume must not be rejected")

	// And the extent is untouched: idempotent, no geometry change.
	reread, err := client.ISCSIExtentGet(ctx, extent.ID)
	require.NoError(t, err)
	assert.Equal(t, 4096, reread.Blocksize)
}

// TestPublishWithoutRequestOptsUnstampedVolumeSucceeds covers the pre-GF4
// installed base: a volume with no stored stamp and a publish with no opts has
// no geometry opinion at all, so the guard must be a no-op even when the live
// extent's blocksize differs from the controller default.
func TestPublishWithoutRequestOptsUnstampedVolumeSucceeds(t *testing.T) {
	d, client := newPersistISCSIDriver(t)
	ctx := context.Background()
	datasetName := "tank/csi/pvc-legacy-4k"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME"})
	require.NoError(t, err)

	shareName := d.iscsiShareName(path.Base(datasetName))
	target, err := client.ISCSITargetCreate(ctx, shareName, "", "ISCSI", nil)
	require.NoError(t, err)
	extent, err := client.ISCSIExtentCreate(ctx, shareName, "zvol/"+datasetName, "", 4096, true, "SSD")
	require.NoError(t, err)
	stampBlockProps(t, client, datasetName, map[string]string{
		PropISCSITargetID: strconv.Itoa(target.ID),
		PropISCSIExtentID: strconv.Itoa(extent.ID),
	})

	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-legacy-4k", nil),
		"an unstamped legacy volume must not be rejected by a no-opts publish")
}

// TestRebuildReappliesAllStoredBlockOptions is the F-3 regression test: a
// rebuild must re-apply EVERY stored create-time-only option, not just geometry.
// Before the fix a target/extent rebuild regenerated the SCSI serial, came back
// read-write, re-enabled insecure_tpc, dropped the target-level auth_networks
// (a security downgrade), and reset avail_threshold.
func TestRebuildReappliesAllStoredBlockOptions(t *testing.T) {
	d, client := newPersistISCSIDriver(t)
	ctx := context.Background()
	datasetName := "tank/csi/pvc-tuned"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME"})
	require.NoError(t, err)

	serial := stableISCSISerial("pvc-tuned")
	stampBlockProps(t, client, datasetName, map[string]string{
		PropBlockISCSIBlocksize:      "4096",
		PropBlockISCSIPblocksize:     "false",
		PropBlockISCSIQueuedCommands: "128",
		PropBlockISCSIInsecureTpc:    "false",
		PropBlockISCSIReadOnly:       "true",
		PropBlockISCSIAvailThreshold: "80",
		PropBlockISCSISerial:         serial,
		PropBlockISCSIAuthNetworks:   "10.0.0.0/8,192.168.0.0/16",
	})

	// Nothing exists on the backend: full target+extent rebuild on a plain ctx.
	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-tuned", nil))

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 4096, extent.Blocksize, "stored blocksize must survive the rebuild")
	require.NotNil(t, extent.Pblocksize)
	assert.False(t, *extent.Pblocksize, "stored pblocksize=false must survive the rebuild")
	require.NotNil(t, extent.InsecureTpc)
	assert.False(t, *extent.InsecureTpc, "stored insecureTpc=false must survive the rebuild (re-enabling XCOPY/ODX is a downgrade)")
	require.NotNil(t, extent.Ro)
	assert.True(t, *extent.Ro, "stored readOnly=true must survive the rebuild (a safety control must not be silently revoked)")
	require.NotNil(t, extent.AvailThreshold)
	assert.Equal(t, 80, *extent.AvailThreshold, "stored availThreshold must survive the rebuild")
	assert.Equal(t, serial, extent.Serial,
		"stored stable serial must survive the rebuild — 'identity survives delete+recreate' is the whole point of the feature")

	target, err := client.ISCSITargetFindByName(ctx, d.iscsiShareName(path.Base(datasetName)))
	require.NoError(t, err)
	require.NotNil(t, target)
	require.NotNil(t, target.QueuedCommands)
	assert.Equal(t, 128, *target.QueuedCommands, "stored queuedCommands must survive the target rebuild")
	assert.Equal(t, []string{"10.0.0.0/8", "192.168.0.0/16"}, target.AuthNetworks,
		"stored target auth_networks must survive the rebuild — dropping them is a SECURITY downgrade")
}

// TestGenuineStorageClassGeometryChangeStillRejected proves R-1 is preserved in
// both halves: with a live extent AND on the absent-extent rebuild path (where
// the old guard was structurally blind).
func TestGenuineStorageClassGeometryChangeStillRejected(t *testing.T) {
	t.Run("existing extent", func(t *testing.T) {
		d, client := newPersistISCSIDriver(t)
		ctx := context.Background()
		datasetName := "tank/csi/pvc-change-live"
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME"})
		require.NoError(t, err)
		shareName := d.iscsiShareName(path.Base(datasetName))
		target, err := client.ISCSITargetCreate(ctx, shareName, "", "ISCSI", nil)
		require.NoError(t, err)
		extent, err := client.ISCSIExtentCreate(ctx, shareName, "zvol/"+datasetName, "", 4096, true, "SSD")
		require.NoError(t, err)
		stampBlockProps(t, client, datasetName, map[string]string{
			PropISCSITargetID:       strconv.Itoa(target.ID),
			PropISCSIExtentID:       strconv.Itoa(extent.ID),
			PropBlockISCSIBlocksize: "4096",
		})

		reqCtx := withBlockOpts(ctx, &blockOpts{iscsiBlocksize: intPtr(512)})
		err = iscsiShareBackend{d}.EnsureShare(reqCtx, nil, datasetName, "pvc-change-live", nil)
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("absent extent", func(t *testing.T) {
		d, client := newPersistISCSIDriver(t)
		ctx := context.Background()
		datasetName := "tank/csi/pvc-change-rebuild"
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME"})
		require.NoError(t, err)
		stampBlockProps(t, client, datasetName, map[string]string{PropBlockISCSIBlocksize: "4096"})

		reqCtx := withBlockOpts(ctx, &blockOpts{iscsiBlocksize: intPtr(512)})
		err = iscsiShareBackend{d}.EnsureShare(reqCtx, nil, datasetName, "pvc-change-rebuild", nil)
		require.Error(t, err, "a genuine SC geometry change must fail closed even when the extent is absent")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))

		// And it must NOT have created an extent at the wrong geometry.
		extent, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
		require.NoError(t, findErr)
		assert.Nil(t, extent, "the rejected request must not have created an extent")
	})

	t.Run("pblocksize change", func(t *testing.T) {
		d, client := newPersistISCSIDriver(t)
		ctx := context.Background()
		datasetName := "tank/csi/pvc-change-pblock"
		_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME"})
		require.NoError(t, err)
		stampBlockProps(t, client, datasetName, map[string]string{PropBlockISCSIPblocksize: "true"})

		reqCtx := withBlockOpts(ctx, &blockOpts{iscsiPblocksize: boolPtr(false)})
		err = iscsiShareBackend{d}.EnsureShare(reqCtx, nil, datasetName, "pvc-change-pblock", nil)
		require.Error(t, err, "pblocksize is create-only and must be guarded too")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}

// TestRequestOptsWinPerKeyOverStored proves the merge is per-key: a StorageClass
// that changes only a mutable knob must not silently reset the volume's stored
// geometry to the controller default.
func TestRequestOptsWinPerKeyOverStored(t *testing.T) {
	stored := &blockOpts{
		iscsiBlocksize:      intPtr(4096),
		iscsiInsecureTpc:    boolPtr(false),
		iscsiSerial:         "abc123",
		iscsiAuthNetworks:   []string{"10.0.0.0/8"},
		nvmeofQidMax:        intPtr(128),
		iscsiQueuedCommands: intPtr(32),
	}
	request := &blockOpts{iscsiQueuedCommands: intPtr(128)}

	merged := mergeBlockOpts(request, stored)
	require.NotNil(t, merged.iscsiBlocksize)
	assert.Equal(t, 4096, *merged.iscsiBlocksize, "an unset request key must keep the stored value")
	assert.Equal(t, 128, *merged.iscsiQueuedCommands, "a set request key must win")
	assert.False(t, *merged.iscsiInsecureTpc)
	assert.Equal(t, "abc123", merged.iscsiSerial)
	assert.Equal(t, []string{"10.0.0.0/8"}, merged.iscsiAuthNetworks)
	assert.Equal(t, 128, *merged.nvmeofQidMax)

	// Nil handling on both sides.
	assert.Same(t, stored, mergeBlockOpts(nil, stored))
	assert.Same(t, request, mergeBlockOpts(request, nil))
	assert.Nil(t, mergeBlockOpts(nil, nil))
}

// TestStoredPropertiesRoundTrip proves the persisted form survives a
// stamp -> read cycle unchanged for every knob.
func TestStoredPropertiesRoundTrip(t *testing.T) {
	original := &blockOpts{
		iscsiBlocksize:      intPtr(4096),
		iscsiPblocksize:     boolPtr(false),
		iscsiQueuedCommands: intPtr(128),
		iscsiInsecureTpc:    boolPtr(false),
		iscsiReadOnly:       boolPtr(true),
		iscsiAvailThreshold: intPtr(80),
		iscsiSerial:         "0123456789abcdef",
		iscsiAuthNetworks:   []string{"10.0.0.0/8", "192.168.0.0/16"},
		nvmeofQidMax:        intPtr(128),
		nvmeofPiEnable:      boolPtr(true),
	}
	props := original.storedProperties()
	require.Len(t, props, 10, "every set knob must persist exactly one property")

	ds := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{}}
	for key, value := range props {
		ds.UserProperties[key] = truenas.UserProperty{Value: value, Source: "local"}
	}
	restored := blockOptsFromDataset(ds)
	require.NotNil(t, restored)
	assert.Equal(t, *original.iscsiBlocksize, *restored.iscsiBlocksize)
	assert.Equal(t, *original.iscsiPblocksize, *restored.iscsiPblocksize)
	assert.Equal(t, *original.iscsiQueuedCommands, *restored.iscsiQueuedCommands)
	assert.Equal(t, *original.iscsiInsecureTpc, *restored.iscsiInsecureTpc)
	assert.Equal(t, *original.iscsiReadOnly, *restored.iscsiReadOnly)
	assert.Equal(t, *original.iscsiAvailThreshold, *restored.iscsiAvailThreshold)
	assert.Equal(t, original.iscsiSerial, restored.iscsiSerial)
	assert.Equal(t, original.iscsiAuthNetworks, restored.iscsiAuthNetworks)
	assert.Equal(t, *original.nvmeofQidMax, *restored.nvmeofQidMax)
	assert.Equal(t, *original.nvmeofPiEnable, *restored.nvmeofPiEnable)
}

// TestStoredPropertiesEmptyForDefaultPath is the byte-identical contract: a
// StorageClass that opts into nothing must stamp NOTHING, so its dataset write
// carries exactly the keys it carried before these knobs existed.
func TestStoredPropertiesEmptyForDefaultPath(t *testing.T) {
	var nilOpts *blockOpts
	assert.Nil(t, nilOpts.storedProperties())
	assert.Nil(t, (&blockOpts{}).storedProperties())
	assert.Nil(t, blockOptsProps(context.Background(), ShareTypeISCSI))
	assert.Nil(t, blockOptsProps(context.Background(), ShareTypeNVMeoF))
	// NFS never carries block tuning even if a context somehow did.
	ctx := withBlockOpts(context.Background(), &blockOpts{iscsiBlocksize: intPtr(4096)})
	assert.Nil(t, blockOptsProps(ctx, ShareTypeNFS))
	assert.NotNil(t, blockOptsProps(ctx, ShareTypeISCSI))
	// An empty dataset resolves to nil, i.e. "controller default", never a
	// synthesized opinion.
	assert.Nil(t, blockOptsFromDataset(&truenas.Dataset{UserProperties: map[string]truenas.UserProperty{}}))
	assert.Nil(t, blockOptsFromDataset(nil))
	assert.Nil(t, effectiveBlockOpts(context.Background(), nil))
}

// TestDefaultPathCreateVolumeStampsNothing drives the real CreateVolume and
// asserts the provisioned dataset carries no block-tuning property at all, and
// that the extent used the historical defaults. This is the guard on the
// "an install that opts into nothing is unaffected" claim.
func TestDefaultPathCreateVolumeStampsNothing(t *testing.T) {
	client := newAPICallCountingClient()
	d := newAPICallCountDriver(t, client, "iscsi")
	ctx := context.Background()

	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("default-path", "iscsi"))
	require.NoError(t, err)

	ds, err := client.DatasetGet(ctx, "pool/parent/default-path")
	require.NoError(t, err)
	for key := range ds.UserProperties {
		assert.NotContains(t, key, "truenas-csi:block_",
			"a StorageClass that opts into nothing must stamp no block property (got %s)", key)
		assert.NotContains(t, key, "truenas-csi:nvme_",
			"a StorageClass that opts into nothing must stamp no NVMe property (got %s)", key)
	}

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/default-path")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 512, extent.Blocksize, "the default path keeps the historical 512 blocksize")
	require.NotNil(t, extent.InsecureTpc)
	assert.True(t, *extent.InsecureTpc, "the default path keeps the historical insecure_tpc=true")
	require.NotNil(t, extent.Ro)
	assert.False(t, *extent.Ro)
	assert.Empty(t, extent.Serial, "the default path still lets TrueNAS auto-generate the serial")
	assert.Nil(t, extent.AvailThreshold)
}

// TestTunedCreateVolumeStampsResolvedOptions closes the loop: an opted-in
// StorageClass persists exactly the keys it set (and no others), so the rebuild
// paths above have something to read.
func TestTunedCreateVolumeStampsResolvedOptions(t *testing.T) {
	client := newAPICallCountingClient()
	d := newAPICallCountDriver(t, client, "iscsi")
	ctx := context.Background()

	req := apiCallCountVolumeRequest("tuned-path", "iscsi")
	req.Parameters[paramISCSIBlocksize] = "4096"
	req.Parameters[paramISCSIStableSerial] = "true"
	_, err := d.CreateVolume(ctx, req)
	require.NoError(t, err)

	ds, err := client.DatasetGet(ctx, "pool/parent/tuned-path")
	require.NoError(t, err)
	assert.Equal(t, "4096", ds.UserProperties[PropBlockISCSIBlocksize].Value)
	assert.Equal(t, stableISCSISerial("tuned-path"), ds.UserProperties[PropBlockISCSISerial].Value)
	// Only the keys actually set are stamped.
	_, hasPblocksize := ds.UserProperties[PropBlockISCSIPblocksize]
	assert.False(t, hasPblocksize, "an unset knob must not be stamped")
	_, hasReadOnly := ds.UserProperties[PropBlockISCSIReadOnly]
	assert.False(t, hasReadOnly, "an unset knob must not be stamped")

	// And the stamp is immediately load-bearing: a plain-ctx rebuild after the
	// extent is destroyed comes back at 4096 with the same serial.
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/tuned-path")
	require.NoError(t, err)
	require.NotNil(t, extent)
	require.NoError(t, client.ISCSIExtentDelete(ctx, extent.ID, false, true))
	require.NoError(t, client.DatasetSetUserProperties(ctx, "pool/parent/tuned-path", map[string]string{PropISCSIExtentID: "-"}))

	require.NoError(t, iscsiShareBackend{d}.EnsureShare(ctx, nil, "pool/parent/tuned-path", "tuned-path", nil))
	rebuilt, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/tuned-path")
	require.NoError(t, err)
	require.NotNil(t, rebuilt)
	assert.Equal(t, 4096, rebuilt.Blocksize)
	assert.Equal(t, stableISCSISerial("tuned-path"), rebuilt.Serial)
}

// TestMalformedStoredBlockPropertyIsIgnored proves a corrupt advisory stamp can
// never wedge an attach: the property is skipped and resolution falls through to
// the controller default rather than erroring.
func TestMalformedStoredBlockPropertyIsIgnored(t *testing.T) {
	ds := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{
		PropBlockISCSIBlocksize:    {Value: "not-a-number", Source: "local"},
		PropBlockISCSIReadOnly:     {Value: "sometimes", Source: "local"},
		PropBlockISCSIAuthNetworks: {Value: "not-a-cidr", Source: "local"},
		PropBlockISCSISerial:       {Value: "-", Source: "local"},
	}}
	assert.Nil(t, blockOptsFromDataset(ds), "malformed and ZFS-sentinel values must resolve to no opinion")
}

// TestStoredBlockPropertiesAreInheritedByClones documents the deliberate
// contrast with the CHAP reader: geometry describes the DATA layout, which a
// clone shares byte-for-byte with its source, so an inherited value is the
// correct (and conservative) one. Reading these local-only would re-create a
// cloned 4096 volume's extent at 512.
func TestStoredBlockPropertiesAreInheritedByClones(t *testing.T) {
	ds := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{
		PropBlockISCSIBlocksize: {Value: "4096", Source: "tank/csi/src@snap"},
	}}
	opts := blockOptsFromDataset(ds)
	require.NotNil(t, opts)
	require.NotNil(t, opts.iscsiBlocksize)
	assert.Equal(t, 4096, *opts.iscsiBlocksize)

	// CHAP, by contrast, must NOT be inherited — proving the two readers are
	// deliberately different rather than accidentally inconsistent.
	d := &Driver{config: &Config{}}
	chapDS := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{
		PropISCSIAuthMode: {Value: "CHAP", Source: "tank/csi/src@snap"},
		PropISCSIAuthTag:  {Value: "42", Source: "tank/csi/src@snap"},
	}}
	mode, _ := d.storedISCSICHAPPolicy(chapDS)
	assert.Equal(t, iscsiCHAPModeNone, mode)
}

// TestNVMeoFRebuildReappliesStoredSubsystemOptions is the NVMe-oF half of F-3:
// a subsystem rebuilt on a plain ctx must come back with the volume's stored
// qid_max and pi_enable. Silently disabling T10-PI on a rebuild is an integrity
// regression that can break an initiator connected with PI.
func TestNVMeoFRebuildReappliesStoredSubsystemOptions(t *testing.T) {
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "tank/csi"},
			NVMeoF: NVMeoFConfig{
				Enabled:               true,
				Transport:             "TCP",
				TransportAddress:      "192.0.2.20",
				TransportServiceID:    4420,
				SubsystemAllowAnyHost: true,
			},
		},
		truenasClient: client,
	}
	ctx := context.Background()
	datasetName := "tank/csi/pvc-nvme-tuned"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME"})
	require.NoError(t, err)
	stampBlockProps(t, client, datasetName, map[string]string{
		PropBlockNVMeoFQidMax:   "128",
		PropBlockNVMeoFPiEnable: "true",
	})

	require.NoError(t, nvmeoFShareBackend{d}.EnsureShare(ctx, nil, datasetName, "pvc-nvme-tuned", &fenceResolution{}))

	subsys, err := client.NVMeoFSubsystemFindByName(ctx, d.nvmeSubsystemName(datasetName))
	require.NoError(t, err)
	require.NotNil(t, subsys)
	require.NotNil(t, subsys.QidMax, "stored qid_max must be re-applied on a subsystem rebuild")
	assert.Equal(t, 128, *subsys.QidMax)
	require.NotNil(t, subsys.PiEnable, "stored pi_enable must be re-applied on a subsystem rebuild")
	assert.True(t, *subsys.PiEnable, "stored pi_enable must be re-applied on a subsystem rebuild")
}

// TestQidMaxUpperBound is the F-8 regression test: an out-of-range qidMax must
// be rejected as InvalidArgument at admission rather than reaching
// nvmet.subsys.create and surfacing as an opaque Internal error.
func TestQidMaxUpperBound(t *testing.T) {
	for _, raw := range []string{"0", "-1", "65536", "1000000"} {
		_, err := resolveBlockOpts(map[string]string{paramNVMeoFQidMax: raw}, "pvc-1")
		require.Error(t, err, "qidMax=%s must be rejected", raw)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	for _, raw := range []string{"1", "128", "65535"} {
		opts, err := resolveBlockOpts(map[string]string{paramNVMeoFQidMax: raw}, "pvc-1")
		require.NoError(t, err, "qidMax=%s must be accepted", raw)
		require.NotNil(t, opts)
	}
}
