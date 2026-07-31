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

func TestResolveBlockOptsNilWhenUnset(t *testing.T) {
	opts, err := resolveBlockOpts(nil, "pvc-1")
	require.NoError(t, err)
	assert.Nil(t, opts, "no params must resolve to nil so provisioning is byte-identical")

	opts, err = resolveBlockOpts(map[string]string{"protocol": "iscsi"}, "pvc-1")
	require.NoError(t, err)
	assert.Nil(t, opts, "unrecognized params must resolve to nil")
}

func TestResolveBlockOptsValid(t *testing.T) {
	opts, err := resolveBlockOpts(map[string]string{
		paramISCSIBlocksize:      "4096",
		paramISCSIPblocksize:     "false",
		paramISCSIQueuedCommands: "128",
		paramISCSIInsecureTpc:    "false",
		paramISCSIReadOnly:       "true",
		paramISCSIAvailThreshold: "80",
		paramISCSIStableSerial:   "true",
		paramISCSIAuthNetworks:   "10.0.0.0/8, 192.168.0.0/16",
		paramNVMeoFQidMax:        "128",
		paramNVMeoFPiEnable:      "true",
	}, "pvc-abc")
	require.NoError(t, err)
	require.NotNil(t, opts)
	assert.Equal(t, 4096, *opts.iscsiBlocksize)
	assert.False(t, *opts.iscsiPblocksize)
	assert.Equal(t, 128, *opts.iscsiQueuedCommands)
	assert.False(t, *opts.iscsiInsecureTpc)
	assert.True(t, *opts.iscsiReadOnly)
	assert.Equal(t, 80, *opts.iscsiAvailThreshold)
	assert.Equal(t, stableISCSISerial("pvc-abc"), opts.iscsiSerial)
	assert.Equal(t, []string{"10.0.0.0/8", "192.168.0.0/16"}, opts.iscsiAuthNetworks)
	assert.Equal(t, 128, *opts.nvmeofQidMax)
	assert.True(t, *opts.nvmeofPiEnable)
}

func TestResolveBlockOptsInvalid(t *testing.T) {
	cases := map[string]map[string]string{
		"blocksize out of enum":      {paramISCSIBlocksize: "8192"},
		"blocksize not int":          {paramISCSIBlocksize: "big"},
		"queuedCommands out of enum": {paramISCSIQueuedCommands: "64"},
		"availThreshold too high":    {paramISCSIAvailThreshold: "100"},
		"availThreshold too low":     {paramISCSIAvailThreshold: "0"},
		"qidMax negative":            {paramNVMeoFQidMax: "-1"},
		"bad bool":                   {paramISCSIPblocksize: "maybe"},
		"bad cidr":                   {paramISCSIAuthNetworks: "10.0.0.0"},
	}
	for name, params := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := resolveBlockOpts(params, "pvc-1")
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestStableISCSISerialDeterministic(t *testing.T) {
	a := stableISCSISerial("pvc-123")
	b := stableISCSISerial("pvc-123")
	c := stableISCSISerial("pvc-456")
	assert.Equal(t, a, b, "serial must be stable for the same volume")
	assert.NotEqual(t, a, c, "distinct volumes must get distinct serials")
	assert.Len(t, a, 16, "serial is a 16-char hex string")
}

func TestGuardISCSIBlocksizeImmutability(t *testing.T) {
	existing := &truenas.ISCSIExtent{ID: 1, Blocksize: 4096}

	// Matching blocksize: no error.
	assert.NoError(t, guardISCSIBlocksizeImmutability(existing, 4096, "ds"))
	// Divergent blocksize: FailedPrecondition.
	err := guardISCSIBlocksizeImmutability(existing, 512, "ds")
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	// Zero resolved (unset config) is not a meaningful request: no error.
	assert.NoError(t, guardISCSIBlocksizeImmutability(existing, 0, "ds"))
	// Legacy extent with no reported blocksize: no error.
	assert.NoError(t, guardISCSIBlocksizeImmutability(&truenas.ISCSIExtent{ID: 2}, 512, "ds"))
	// Nil extent: no error.
	assert.NoError(t, guardISCSIBlocksizeImmutability(nil, 512, "ds"))
}

func TestBlockOptsBuildersOmitWhenNil(t *testing.T) {
	var opts *blockOpts
	extent := opts.iscsiExtentCreateOpts()
	assert.Nil(t, extent.InsecureTpc)
	assert.Nil(t, extent.ReadOnly)
	assert.Nil(t, extent.AvailThreshold)
	assert.Empty(t, extent.Serial)

	target := opts.iscsiTargetCreateOpts()
	assert.Nil(t, target.QueuedCommands)
	assert.Empty(t, target.AuthNetworks)

	subsys := opts.nvmeofSubsystemCreateOpts()
	assert.Nil(t, subsys.QidMax)
	assert.Nil(t, subsys.PiEnable)

	// resolved helpers fall back to controller defaults when opts is nil.
	assert.Equal(t, 512, opts.resolvedISCSIBlocksize(512))
	assert.True(t, opts.resolvedISCSIPblocksize(false))
}

func TestValidateNoNVMeoFPortParams(t *testing.T) {
	assert.NoError(t, validateNoNVMeoFPortParams(map[string]string{paramNVMeoFQidMax: "128"}))
	for _, key := range []string{"nvmeof/inlineDataSize", "nvmeof/maxQueueSize", "nvmeof/portPiEnable"} {
		err := validateNoNVMeoFPortParams(map[string]string{key: "1"})
		require.Error(t, err, "%s must be rejected as a per-SC param", key)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}

// newBlockOptsISCSIDriver builds a driver backed by the in-memory mock with an
// iSCSI config suitable for share creation.
func newBlockOptsISCSIDriver(t *testing.T) (*Driver, *truenas.MockClient) {
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

// TestISCSIBlocksizeImmutabilityGuardRejects proves R-1 end-to-end through the
// share builder: an existing extent's blocksize cannot be changed by a later
// CreateVolume that resolves a different blocksize.
func TestISCSIBlocksizeImmutabilityGuardRejects(t *testing.T) {
	d, client := newBlockOptsISCSIDriver(t)
	ctx := context.Background()
	datasetName := "tank/csi/pvc-existing"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME"})
	require.NoError(t, err)

	// Pre-create the target + extent (blocksize 4096) and stamp their IDs so the
	// idempotent create path resolves them instead of creating anew.
	shareName := d.iscsiShareName(path.Base(datasetName))
	target, err := client.ISCSITargetCreate(ctx, shareName, "", "ISCSI", nil)
	require.NoError(t, err)
	extent, err := client.ISCSIExtentCreate(ctx, shareName, "zvol/"+datasetName, "", 4096, true, "SSD")
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropISCSITargetID, strconv.Itoa(target.ID)))
	require.NoError(t, client.DatasetSetUserProperty(ctx, datasetName, PropISCSIExtentID, strconv.Itoa(extent.ID)))

	// Request a divergent blocksize (512) on the existing 4096 extent.
	blocksize := 512
	ctx = withBlockOpts(ctx, &blockOpts{iscsiBlocksize: &blocksize})
	err = d.createISCSIShareForDataset(ctx, nil, datasetName, "pvc-existing", false, true)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "immutable blocksize")

	// A matching blocksize is accepted (idempotent, no geometry change).
	ctxMatch := withBlockOpts(context.Background(), &blockOpts{iscsiBlocksize: func() *int { v := 4096; return &v }()})
	assert.NoError(t, d.createISCSIShareForDataset(ctxMatch, nil, datasetName, "pvc-existing", false, true))
}

// TestISCSICreateThreadsBlocksize proves a fresh create applies the per-SC
// blocksize override to the outgoing extent create.
func TestISCSICreateThreadsBlocksize(t *testing.T) {
	d, client := newBlockOptsISCSIDriver(t)
	ctx := context.Background()
	datasetName := "tank/csi/pvc-fresh"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME"})
	require.NoError(t, err)

	blocksize := 4096
	insecure := false
	ctx = withBlockOpts(ctx, &blockOpts{iscsiBlocksize: &blocksize, iscsiInsecureTpc: &insecure})
	require.NoError(t, d.createISCSIShareForDataset(ctx, nil, datasetName, "pvc-fresh", true, true))

	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 4096, extent.Blocksize, "per-SC blocksize must reach the extent create")
	assert.False(t, extent.InsecureTpc, "per-SC insecureTpc=false must reach the extent create")
}
