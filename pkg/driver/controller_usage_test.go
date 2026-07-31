package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func setDatasetUsage(client *truenas.MockClient, name string, used, quota, refquota, available int64) {
	ds := client.Datasets[name]
	prop := func(v int64) truenas.DatasetProperty { return truenas.DatasetProperty{Parsed: float64(v)} }
	ds.Used = prop(used)
	ds.Quota = prop(quota)
	ds.Refquota = prop(refquota)
	ds.Available = prop(available)
}

func TestDatasetGetQuotaUsage(t *testing.T) {
	client := truenas.NewMockClient()
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/vol", Type: "FILESYSTEM"})
	require.NoError(t, err)
	setDatasetUsage(client, "pool/parent/vol", 700, 1000, 0, 300)

	usage, err := client.DatasetGetQuotaUsage(ctx, "pool/parent/vol")
	require.NoError(t, err)
	assert.Equal(t, int64(700), usage.Used)
	assert.Equal(t, int64(1000), usage.Quota)
	assert.Equal(t, int64(0), usage.Refquota)
	assert.Equal(t, int64(300), usage.Available)

	_, err = client.DatasetGetQuotaUsage(ctx, "pool/parent/missing")
	assert.True(t, truenas.IsNotFoundError(err))
}

func TestVolumeUsageNearQuota(t *testing.T) {
	cases := []struct {
		name     string
		used     int64
		quota    int64
		refquota int64
		wantNear bool
	}{
		{"unlimited never near", 999999, 0, 0, false},
		{"well under quota", 500, 1000, 0, false},
		{"exactly 95% is not over", 950, 1000, 0, false},
		{"over 95%", 951, 1000, 0, true},
		{"full", 1000, 1000, 0, true},
		{"refquota binds over quota", 951, 100000, 1000, true},
		{"refquota preferred and safe", 500, 1000, 100000, false},
	}
	for _, tc := range cases {
		usage := &truenas.DatasetQuotaUsage{Used: tc.used, Quota: tc.quota, Refquota: tc.refquota}
		assert.Equal(t, tc.wantNear, volumeUsageNearQuota(usage), tc.name)
	}
}

func newUsageTestDriver(client truenas.ClientInterface, report bool) *Driver {
	return &Driver{
		name: "csi.scale.io",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", ReportVolumeUsage: report},
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
}

func TestControllerGetVolumeNearQuotaIsAbnormal(t *testing.T) {
	client := truenas.NewMockClient()
	d := newUsageTestDriver(client, true)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/full-vol", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, "pool/parent/full-vol", map[string]string{
		PropManagedResource: "true", PropProvisionSuccess: "true",
	}))
	setDatasetUsage(client, "pool/parent/full-vol", 990, 1000, 0, 10)

	resp, err := d.ControllerGetVolume(ctx, &csi.ControllerGetVolumeRequest{VolumeId: "full-vol"})
	require.NoError(t, err)
	cond := resp.GetStatus().GetVolumeCondition()
	require.NotNil(t, cond)
	assert.True(t, cond.GetAbnormal(), "a volume above 95%% quota is abnormal")
	assert.Contains(t, cond.GetMessage(), "quota")
}

func TestControllerGetVolumeHealthyUnderQuota(t *testing.T) {
	client := truenas.NewMockClient()
	d := newUsageTestDriver(client, true)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/ok-vol", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, "pool/parent/ok-vol", map[string]string{
		PropManagedResource: "true", PropProvisionSuccess: "true",
	}))
	setDatasetUsage(client, "pool/parent/ok-vol", 100, 1000, 0, 900)

	resp, err := d.ControllerGetVolume(ctx, &csi.ControllerGetVolumeRequest{VolumeId: "ok-vol"})
	require.NoError(t, err)
	assert.False(t, resp.GetStatus().GetVolumeCondition().GetAbnormal(), "a volume well under quota is healthy")
}
