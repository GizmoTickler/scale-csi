package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// The ListVolumes walk's frozen view holds only the sorted dataset names for
// its TTL: each page is hydrated by name anyway, so keeping the listing's
// decoded datasets pinned about 4.5 MB per 1,000 volumes for nothing.
func TestListVolumesPageCacheHoldsOnlyNames(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{config: &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}}, truenasClient: client}
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	for _, name := range []string{"vol-c", "vol-a", "vol-b"} {
		_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/" + name, Type: "FILESYSTEM"})
		require.NoError(t, err)
		require.NoError(t, client.DatasetSetUserProperty(ctx, "pool/parent/"+name, PropManagedResource, "true"))
	}

	first, err := d.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 2})
	require.NoError(t, err)
	require.Len(t, first.Entries, 2)
	assert.Equal(t, "vol-a", first.Entries[0].GetVolume().GetVolumeId())
	d.volumePageCacheMu.Lock()
	cached := append([]string(nil), d.volumePageCache...)
	d.volumePageCacheMu.Unlock()
	assert.Equal(t, []string{"pool/parent/vol-a", "pool/parent/vol-b", "pool/parent/vol-c"}, cached)

	second, err := d.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: first.NextToken})
	require.NoError(t, err)
	require.Len(t, second.Entries, 1)
	assert.Equal(t, "vol-c", second.Entries[0].GetVolume().GetVolumeId())
	assert.Empty(t, second.NextToken)
}

// At the default verbosity the RPC logging interceptor does not build the
// secret-stripped request clone that only the V(5) dump prints.
func TestRequestLoggingSkipsTheV5CloneByDefault(t *testing.T) {
	d := &Driver{config: &Config{}}
	req := &csi.ControllerPublishVolumeRequest{
		VolumeId: "pvc-1", NodeId: "node-1",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
		Secrets:       map[string]string{"a": "b"},
		VolumeContext: map[string]string{"k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4"},
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/ControllerPublishVolume"}
	handler := func(context.Context, interface{}) (interface{}, error) {
		return &csi.ControllerPublishVolumeResponse{}, nil
	}
	allocs := testing.AllocsPerRun(200, func() {
		_, _ = d.logInterceptor(context.Background(), req, info, handler)
	})
	t.Logf("%v allocations per call", allocs)
	assert.Less(t, allocs, 20.0, "the interceptor allocated %v times per call", allocs)
}
