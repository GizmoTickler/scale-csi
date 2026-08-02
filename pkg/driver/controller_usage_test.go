package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// gaugeVecHasVolume reports whether a per-volume gauge vector currently carries
// a series for volumeID, independent of any other test's series.
func gaugeVecHasVolume(vec *prometheus.GaugeVec, volumeID string) bool {
	ch := make(chan prometheus.Metric, 256)
	vec.Collect(ch)
	close(ch)
	for metric := range ch {
		var pb dto.Metric
		if err := metric.Write(&pb); err != nil {
			continue
		}
		for _, label := range pb.GetLabel() {
			if label.GetName() == "volume" && label.GetValue() == volumeID {
				return true
			}
		}
	}
	return false
}

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

// F6 — the per-volume gauges LATCHED: nothing ever deleted a volume's series, so
// a volume observed once above 95% kept near_quota=1 forever (a permanently
// firing ScaleCSIVolumeNearQuota plus unbounded label cardinality), even after
// the PVC was gone.
func TestDeleteVolumeDropsPerVolumeUsageSeries(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newUsageTestDriver(client, true)
	_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "latch-vol",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters:         map[string]string{"protocol": "nfs"},
	})
	require.NoError(t, err)
	setDatasetUsage(client, "pool/parent/latch-vol", 990, 1000, 0, 10)

	_, err = d.ControllerGetVolume(ctx, &csi.ControllerGetVolumeRequest{VolumeId: "latch-vol"})
	require.NoError(t, err)
	require.Equal(t, 1.0, testutil.ToFloat64(volumeNearQuota.WithLabelValues("latch-vol")),
		"precondition: the near-quota gauge is latched at 1")

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "latch-vol"})
	require.NoError(t, err)

	assert.False(t, gaugeVecHasVolume(volumeNearQuota, "latch-vol"), "the deleted volume's series must be dropped")
	assert.False(t, gaugeVecHasVolume(volumeUsedBytes, "latch-vol"))
	assert.False(t, gaugeVecHasVolume(volumeQuotaBytes, "latch-vol"))
}

// F6 — the shipped external-health-monitor sidecar drives ListVolumes, not
// ControllerGetVolume, so gauges written only from ControllerGetVolume left the
// near-quota alert effectively unable to fire. The reconcile dataset walk must
// publish them (with no extra API calls) and un-latch vanished volumes.
func TestReconcilePublishesVolumeUsageFromDatasetWalk(t *testing.T) {
	client := truenas.NewMockClient()
	d := newUsageTestDriver(client, true)
	ResetVolumeUsageMetrics()

	near := &truenas.Dataset{
		Name:     "pool/parent/walk-near",
		Used:     truenas.DatasetProperty{Parsed: float64(990)},
		Refquota: truenas.DatasetProperty{Parsed: float64(1000)},
	}
	ok := &truenas.Dataset{
		Name:     "pool/parent/walk-ok",
		Used:     truenas.DatasetProperty{Parsed: float64(10)},
		Refquota: truenas.DatasetProperty{Parsed: float64(1000)},
	}
	d.publishVolumeUsageMetrics([]*truenas.Dataset{near, ok})

	assert.Equal(t, 1.0, testutil.ToFloat64(volumeNearQuota.WithLabelValues("walk-near")))
	assert.Equal(t, 0.0, testutil.ToFloat64(volumeNearQuota.WithLabelValues("walk-ok")))
	assert.Equal(t, 990.0, testutil.ToFloat64(volumeUsedBytes.WithLabelValues("walk-near")))

	// The next pass no longer observes walk-near: its latched series must go.
	d.publishVolumeUsageMetrics([]*truenas.Dataset{ok})
	assert.False(t, gaugeVecHasVolume(volumeNearQuota, "walk-near"), "a vanished volume must not keep a latched series")
	assert.True(t, gaugeVecHasVolume(volumeNearQuota, "walk-ok"))

	// Feature off => nothing published at all.
	ResetVolumeUsageMetrics()
	d.config.ZFS.ReportVolumeUsage = false
	d.publishVolumeUsageMetrics([]*truenas.Dataset{near})
	assert.False(t, gaugeVecHasVolume(volumeNearQuota, "walk-near"), "the default path publishes no per-volume series")
}
