package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// modifyCountingClient wraps the mock so the RTT SHAPE of ControllerModifyVolume
// is observable (same pattern as snapshotHandleCountingClient): a real retune
// must be exactly ONE pool.dataset.update carrying only the changed properties,
// and a no-op retune must make ZERO update calls.
type modifyCountingClient struct {
	*truenas.MockClient
	datasetUpdateCalls  int
	datasetUpdateParams []*truenas.DatasetUpdateParams
}

func (c *modifyCountingClient) DatasetUpdate(ctx context.Context, name string, params *truenas.DatasetUpdateParams) (*truenas.Dataset, error) {
	c.datasetUpdateCalls++
	c.datasetUpdateParams = append(c.datasetUpdateParams, params)
	return c.MockClient.DatasetUpdate(ctx, name, params)
}

func (c *modifyCountingClient) resetCounts() {
	c.datasetUpdateCalls = 0
	c.datasetUpdateParams = nil
}

func newModifyFixture(t *testing.T) (*modifyCountingClient, *Driver) {
	t.Helper()
	client := &modifyCountingClient{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", DatasetEnableQuotas: true},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client)
	return client, d
}

// createModifyTestNFSVolume provisions a managed NFS volume through the real
// CreateVolume path so it carries the local managed_resource stamp the modify
// gate requires, exactly as a production volume would.
func createModifyTestNFSVolume(t *testing.T, d *Driver, name string) string {
	t.Helper()
	resp, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               name,
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
	})
	require.NoError(t, err)
	return resp.Volume.VolumeId
}

// createModifyTestZvol materializes a managed zvol directly in the mock: the
// modify gate needs the local managed stamp and the real dataset TYPE, and the
// type check must not depend on the driver's own (NFS) config.
func createModifyTestZvol(t *testing.T, client *modifyCountingClient, name string, managed bool) {
	t.Helper()
	params := &truenas.DatasetCreateParams{
		Name: "pool/parent/" + name, Type: "VOLUME", Volsize: testGiB, Volblocksize: "16K",
	}
	if managed {
		params.UserProperties = []truenas.UserPropertyUpdate{{Key: PropManagedResource, Value: "true"}}
	}
	_, err := client.DatasetCreate(context.Background(), params)
	require.NoError(t, err)
}

func TestControllerGetCapabilitiesAdvertisesModifyVolume(t *testing.T) {
	_, d := newModifyFixture(t)
	resp, err := d.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	require.NoError(t, err)
	found := false
	for _, capability := range resp.GetCapabilities() {
		if capability.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_MODIFY_VOLUME {
			found = true
		}
	}
	assert.True(t, found, "MODIFY_VOLUME must be advertised so Kubernetes routes VolumeAttributesClass changes here")
}

func TestControllerModifyVolumeHappyPathNFS(t *testing.T) {
	client, d := newModifyFixture(t)
	volumeID := createModifyTestNFSVolume(t, d, "vol-modify-nfs")
	client.resetCounts()

	_, err := d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId: volumeID,
		MutableParameters: map[string]string{
			"compression": "zstd", // lower case must normalize, matching create-path behavior
			"recordsize":  "1M",
		},
	})
	require.NoError(t, err)

	// Exactly ONE update, carrying ONLY the changed tunables — no capacity, no
	// quota, no user-property writes may ride along.
	require.Equal(t, 1, client.datasetUpdateCalls, "a retune is exactly one pool.dataset.update")
	params := client.datasetUpdateParams[0]
	assert.Equal(t, "ZSTD", params.Compression)
	assert.Equal(t, "1M", params.Recordsize)
	assert.Empty(t, params.Sync, "unrequested tunables must not be written")
	assert.Empty(t, params.Atime, "unrequested tunables must not be written")
	assert.Zero(t, params.Volsize, "MODIFY_VOLUME must never change capacity")
	assert.Nil(t, params.Refquota, "MODIFY_VOLUME must never change capacity")
	assert.Empty(t, params.UserPropertiesUpdate)

	// The mock now reports the new values back through DatasetGet, like a real
	// pool.dataset.query would.
	ds, err := client.DatasetGet(context.Background(), "pool/parent/"+volumeID)
	require.NoError(t, err)
	assert.Equal(t, "ZSTD", ds.Compression.Value)
	assert.Equal(t, "1M", ds.Recordsize.Value)
}

func TestControllerModifyVolumeNoOpMakesNoUpdateCalls(t *testing.T) {
	client, d := newModifyFixture(t)
	volumeID := createModifyTestNFSVolume(t, d, "vol-modify-noop")
	request := &csi.ControllerModifyVolumeRequest{
		VolumeId:          volumeID,
		MutableParameters: map[string]string{"compression": "ZSTD", "recordsize": "1M"},
	}
	_, err := d.ControllerModifyVolume(context.Background(), request)
	require.NoError(t, err)

	// Replay with values that already match: success, zero backend writes.
	client.resetCounts()
	_, err = d.ControllerModifyVolume(context.Background(), request)
	require.NoError(t, err)
	assert.Zero(t, client.datasetUpdateCalls, "already-matching values must short-circuit before pool.dataset.update")

	// Empty mutable_parameters are the degenerate no-op (csi-sanity sends this
	// shape when no VolumeAttributesClass parameters are configured).
	_, err = d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{VolumeId: volumeID})
	require.NoError(t, err)
	assert.Zero(t, client.datasetUpdateCalls)
}

func TestControllerModifyVolumeZvolRejectsFilesystemOnlyAndGeometry(t *testing.T) {
	client, d := newModifyFixture(t)
	createModifyTestZvol(t, client, "zvol-modify", true)
	client.resetCounts()

	for name, tc := range map[string]struct {
		params      map[string]string
		wantMessage string
	}{
		"recordsize is filesystem-only": {map[string]string{"recordsize": "64K"}, "recordsize"},
		"atime is filesystem-only":      {map[string]string{"atime": "off"}, "atime"},
		"volblocksize is immutable":     {map[string]string{"volblocksize": "32K"}, "volblocksize"},
		"extent blocksize is immutable": {map[string]string{"iscsi/blocksize": "4096"}, "iscsi/blocksize"},
		"volsize is capacity":           {map[string]string{"volsize": "2147483648"}, "volsize"},
		"refquota is capacity":          {map[string]string{"refquota": "2147483648"}, "refquota"},
		"encryption is create-time":     {map[string]string{"encryption": "AES-256-GCM"}, "encryption"},
		"protocol is fixed":             {map[string]string{"protocol": "nfs"}, "protocol"},
		"performance class is create-time": {
			map[string]string{"zfsPerformanceClass": "database"}, "zfsPerformanceClass"},
		"unknown key": {map[string]string{"XXX_FakeKey": "XXX_FakeValue"}, "XXX_FakeKey"},
		"bad compression value": {
			map[string]string{"compression": "NOPE"}, "compression"},
		"empty value": {map[string]string{"sync": " "}, "sync"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
				VolumeId:          "zvol-modify",
				MutableParameters: tc.params,
			})
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.InvalidArgument, st.Code())
			// The message must NAME the offending key so the operator can fix the
			// VolumeAttributesClass.
			assert.Contains(t, st.Message(), tc.wantMessage)
		})
	}
	assert.Zero(t, client.datasetUpdateCalls, "a rejected request must not touch the backend")

	// The zvol-applicable pair still works: one update, only those props.
	_, err := d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          "zvol-modify",
		MutableParameters: map[string]string{"compression": "LZ4", "sync": "always"},
	})
	require.NoError(t, err)
	require.Equal(t, 1, client.datasetUpdateCalls)
	assert.Equal(t, "LZ4", client.datasetUpdateParams[0].Compression)
	assert.Equal(t, "ALWAYS", client.datasetUpdateParams[0].Sync)
	assert.Empty(t, client.datasetUpdateParams[0].Recordsize)
	assert.Empty(t, client.datasetUpdateParams[0].Atime)
}

func TestControllerModifyVolumeErrors(t *testing.T) {
	client, d := newModifyFixture(t)

	t.Run("missing volume ID", func(t *testing.T) {
		_, err := d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
			MutableParameters: map[string]string{"compression": "LZ4"},
		})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("volume not found", func(t *testing.T) {
		_, err := d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
			VolumeId:          "no-such-volume",
			MutableParameters: map[string]string{"compression": "LZ4"},
		})
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("unmanaged dataset is refused", func(t *testing.T) {
		createModifyTestZvol(t, client, "zvol-foreign", false)
		_, err := d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
			VolumeId:          "zvol-foreign",
			MutableParameters: map[string]string{"compression": "LZ4"},
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
		assert.Contains(t, st.Message(), PropManagedResource)
	})

	t.Run("concurrent operation is Aborted", func(t *testing.T) {
		volumeID := createModifyTestNFSVolume(t, d, "vol-modify-locked")
		require.True(t, d.acquireOperationLock(volumeLockKey(volumeID)))
		defer d.releaseOperationLock(volumeLockKey(volumeID))
		_, err := d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
			VolumeId:          volumeID,
			MutableParameters: map[string]string{"compression": "LZ4"},
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Aborted, st.Code())
		assert.Contains(t, st.Message(), "operation already in progress")
	})
}

// CreateVolume shares the vocabulary gate and applies the tunables at create
// time — the sanity suite's "create volume with a volume attribute class" and
// "should not create volume with an invalid volume attribute class" contract.
func TestCreateVolumeWithMutableParameters(t *testing.T) {
	client, d := newModifyFixture(t)

	t.Run("valid parameters land on the created dataset", func(t *testing.T) {
		resp, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "vol-vac-create",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			MutableParameters:  map[string]string{"compression": "ZSTD", "recordsize": "1M"},
		})
		require.NoError(t, err)
		ds, err := client.DatasetGet(context.Background(), "pool/parent/"+resp.Volume.VolumeId)
		require.NoError(t, err)
		assert.Equal(t, "ZSTD", ds.Compression.Value, "mutable parameters ride the single pool.dataset.create")
		assert.Equal(t, "1M", ds.Recordsize.Value)

		// A modify to the same values right after create is the no-op shape.
		client.resetCounts()
		_, err = d.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
			VolumeId:          resp.Volume.VolumeId,
			MutableParameters: map[string]string{"compression": "ZSTD", "recordsize": "1M"},
		})
		require.NoError(t, err)
		assert.Zero(t, client.datasetUpdateCalls)
	})

	t.Run("unknown parameter fails the create", func(t *testing.T) {
		_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "vol-vac-bad",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			MutableParameters:  map[string]string{"XXX_FakeKey": "XXX_FakeValue"},
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "XXX_FakeKey")
		_, getErr := client.DatasetGet(context.Background(), "pool/parent/vol-vac-bad")
		assert.Error(t, getErr, "the vocabulary gate must fire before anything is provisioned")
	})

	t.Run("filesystem-only parameter is refused for a zvol create", func(t *testing.T) {
		iscsiDriver := &Driver{
			config: &Config{
				ZFS:        ZFSConfig{DatasetParentName: "pool/parent", ZvolBlocksize: "16K"},
				DriverName: "org.scale.csi.iscsi",
			},
			truenasClient: client,
		}
		_, err := iscsiDriver.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               "zvol-vac-bad",
			VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
			MutableParameters:  map[string]string{"recordsize": "1M"},
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.True(t, strings.Contains(st.Message(), "recordsize"))
	})
}

// --- H-1: mutable parameters must survive a CreateVolume clone retry ---
//
// A clone/restore accepts no create-time property payload, so the clone arm
// applies mutable_parameters via a separate diff-derived pool.dataset.update
// AFTER materialization. If that update fails, the error is fatal (correct),
// but the external-provisioner's retry resumes through createVolumeExisting —
// which, before the fix, never re-applied the tunables, so the retry returned
// success and the PVC bound with recorded attributes (compression=ZSTD) that
// silently disagreed with the actual dataset (the origin's inherited value).

// tunableApplyFailureClient injects a failure into exactly the DatasetUpdate
// that carries live-tunable properties (the applyMutableTunables write). Every
// other pool.dataset.update — the merged content-source/ownership fold, user
// property stamps, refquota — passes through untouched, so the clone
// materializes fully before the failure fires, reproducing the exact state an
// idempotent retry resumes from.
type tunableApplyFailureClient struct {
	*modifyCountingClient
	failTunableUpdates error
}

func (c *tunableApplyFailureClient) DatasetUpdate(ctx context.Context, name string, params *truenas.DatasetUpdateParams) (*truenas.Dataset, error) {
	if c.failTunableUpdates != nil && datasetUpdateCarriesTunables(params) {
		return nil, c.failTunableUpdates
	}
	return c.modifyCountingClient.DatasetUpdate(ctx, name, params)
}

func datasetUpdateCarriesTunables(params *truenas.DatasetUpdateParams) bool {
	return params.Compression != "" || params.Sync != "" || params.Atime != "" || params.Recordsize != ""
}

func countTunableUpdates(params []*truenas.DatasetUpdateParams) int {
	count := 0
	for _, p := range params {
		if datasetUpdateCarriesTunables(p) {
			count++
		}
	}
	return count
}

// newCloneRetryFixture provisions a source volume and a snapshot through the
// real driver paths and returns the snapshot-restore CreateVolumeRequest
// carrying mutable_parameters{compression: ZSTD} — the request whose retry
// behavior H-1 is about.
func newCloneRetryFixture(t *testing.T) (*tunableApplyFailureClient, *Driver, *csi.CreateVolumeRequest) {
	t.Helper()
	client := &tunableApplyFailureClient{modifyCountingClient: &modifyCountingClient{MockClient: truenas.NewMockClient()}}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", DatasetEnableQuotas: true},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client.MockClient)
	createModifyTestNFSVolume(t, d, "vol-clone-src")
	snap, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name: "clone-vac-snap", SourceVolumeId: "vol-clone-src",
	})
	require.NoError(t, err)
	req := &csi.CreateVolumeRequest{
		Name:               "vol-clone-dst",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		MutableParameters:  map[string]string{"compression": "ZSTD"},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snap.GetSnapshot().GetSnapshotId()},
		}},
	}
	return client, d, req
}

// The H-1 repro, end to end: first attempt fails on the post-materialization
// tunable apply; the retry of the SAME request must re-apply the tunables and
// only then return success. Without the createVolumeExisting re-apply, the
// retry succeeds while the dataset still carries the origin's compression —
// the final assertion here is the one that fails.
func TestCreateVolumeCloneRetryReappliesMutableParameters(t *testing.T) {
	ctx := context.Background()
	client, d, req := newCloneRetryFixture(t)

	client.failTunableUpdates = errors.New("injected tunable apply failure")
	_, err := d.CreateVolume(ctx, req)
	require.Error(t, err, "a failed tunable apply after clone materialization must be fatal")
	assert.Equal(t, codes.Internal, status.Code(err))

	// The clone materialized and is owned — exactly the state the
	// external-provisioner's retry resumes from — but does NOT carry the
	// requested attribute yet.
	ds, err := client.DatasetGet(ctx, "pool/parent/vol-clone-dst")
	require.NoError(t, err)
	assert.NotEqual(t, "ZSTD", ds.Compression.Value)

	client.failTunableUpdates = nil
	resp, err := d.CreateVolume(ctx, req)
	require.NoError(t, err, "the idempotent retry must succeed once the backend recovers")
	ds, err = client.DatasetGet(ctx, "pool/parent/"+resp.Volume.VolumeId)
	require.NoError(t, err)
	assert.Equal(t, "ZSTD", ds.Compression.Value,
		"the retry must re-apply the request's mutable parameters: binding the PVC with recorded "+
			"attributes the dataset does not carry is a silent correctness lie nothing later converges")
}

// While the tunable apply keeps failing, the RETRY must keep failing too —
// returning success would bind a PVC whose recorded attributes disagree with
// the volume, the same reasoning that makes the fresh-path apply fatal.
func TestCreateVolumeCloneRetryTunableApplyFailureStaysFatal(t *testing.T) {
	ctx := context.Background()
	client, d, req := newCloneRetryFixture(t)

	client.failTunableUpdates = errors.New("injected tunable apply failure")
	_, err := d.CreateVolume(ctx, req)
	require.Error(t, err)

	_, err = d.CreateVolume(ctx, req)
	require.Error(t, err, "a retry must not bind the PVC while the volume cannot be made to match the request")
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "mutable parameters")
	ds, getErr := client.DatasetGet(ctx, "pool/parent/vol-clone-dst")
	require.NoError(t, getErr)
	assert.NotEqual(t, "ZSTD", ds.Compression.Value)
}

// A retry whose mutable parameters are ALREADY live on the dataset (the first
// attempt succeeded end to end) is a pure no-op for the tunables: the diff in
// applyMutableTunables must produce ZERO extra pool.dataset.update calls.
func TestCreateVolumeCloneRetryAlreadyAppliedTunablesAddZeroUpdates(t *testing.T) {
	ctx := context.Background()
	client, d, req := newCloneRetryFixture(t)

	resp, err := d.CreateVolume(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, countTunableUpdates(client.datasetUpdateParams),
		"the fresh clone arm applies the tunables in exactly one diff-derived update")

	client.resetCounts()
	retryResp, err := d.CreateVolume(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, resp.Volume.VolumeId, retryResp.Volume.VolumeId)
	assert.Zero(t, countTunableUpdates(client.datasetUpdateParams),
		"already-live tunables must diff to zero pool.dataset.update calls on the retry")

	ds, err := client.DatasetGet(ctx, "pool/parent/"+retryResp.Volume.VolumeId)
	require.NoError(t, err)
	assert.Equal(t, "ZSTD", ds.Compression.Value)
}
