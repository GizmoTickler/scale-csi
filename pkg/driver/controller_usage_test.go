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

// setDatasetReferenced sets the numerator a refquota (or a zvol's volsize)
// actually bounds, independently of `used` — the two are only equal on a volume
// with no snapshots, and conflating them was H1.
func setDatasetReferenced(client *truenas.MockClient, name string, referenced, usedBySnapshots int64) {
	ds := client.Datasets[name]
	prop := func(v int64) truenas.DatasetProperty { return truenas.DatasetProperty{Parsed: float64(v)} }
	ds.Referenced = prop(referenced)
	ds.Usedbysnapshots = prop(usedBySnapshots)
}

func TestDatasetGetQuotaUsage(t *testing.T) {
	client := truenas.NewMockClient()
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/vol", Type: "FILESYSTEM"})
	require.NoError(t, err)
	setDatasetUsage(client, "pool/parent/vol", 700, 1000, 0, 300)

	setDatasetReferenced(client, "pool/parent/vol", 400, 300)

	usage, err := client.DatasetGetQuotaUsage(ctx, "pool/parent/vol")
	require.NoError(t, err)
	assert.Equal(t, int64(700), usage.Used)
	assert.Equal(t, int64(1000), usage.Quota)
	assert.Equal(t, int64(0), usage.Refquota)
	assert.Equal(t, int64(300), usage.Available)
	// H1: `referenced` and `usedbysnapshots` must survive the projection — without
	// them the driver cannot compare a refquota against the quantity it bounds.
	assert.Equal(t, int64(400), usage.Referenced)
	assert.Equal(t, int64(300), usage.UsedBySnapshots)
	assert.Equal(t, "FILESYSTEM", usage.Type)

	_, err = client.DatasetGetQuotaUsage(ctx, "pool/parent/missing")
	assert.True(t, truenas.IsNotFoundError(err))
}

// H1 — the previous table asserted the BUG: {used: 951, refquota: 1000} => near,
// i.e. `used` measured against `refquota`. In ZFS refquota bounds `referenced`,
// and the gap between the two is exactly the space a volume's snapshots hold, so
// every volume with snapshots was reported near-quota permanently. Each case
// below now names the ZFS pair it exercises, and no case sets `used` as a
// stand-in for `referenced`.
func TestVolumeUsageNearQuota(t *testing.T) {
	cases := []struct {
		name       string
		usage      truenas.DatasetQuotaUsage
		wantNear   bool
		wantQuota  int64
		wantUsed   int64
		wantLimit  string
		wantReason string
	}{
		{
			name:      "unlimited never near",
			usage:     truenas.DatasetQuotaUsage{Used: 999999, Referenced: 999999},
			wantNear:  false,
			wantQuota: 0,
			wantUsed:  999999,
		},
		{
			name:      "quota-bound well under",
			usage:     truenas.DatasetQuotaUsage{Used: 500, Referenced: 400, Quota: 1000},
			wantNear:  false,
			wantQuota: 1000,
			wantUsed:  500,
			wantLimit: volumeLimitQuota,
		},
		{
			name:      "quota-bound exactly 95% is not over",
			usage:     truenas.DatasetQuotaUsage{Used: 950, Referenced: 100, Quota: 1000},
			wantNear:  false,
			wantQuota: 1000,
			wantUsed:  950,
			wantLimit: volumeLimitQuota,
		},
		{
			// H1(b): `quota` bounds `used`, and a dataset that is near-full on
			// `used` IS near-quota even though its `referenced` is tiny — the
			// snapshot space is exactly what `quota` counts.
			name:      "quota-bound near-full on used is near quota",
			usage:     truenas.DatasetQuotaUsage{Used: 951, Referenced: 10, UsedBySnapshots: 941, Quota: 1000},
			wantNear:  true,
			wantQuota: 1000,
			wantUsed:  951,
			wantLimit: volumeLimitQuota,
		},
		{
			name:      "refquota-bound over 95% on referenced",
			usage:     truenas.DatasetQuotaUsage{Used: 951, Referenced: 951, Refquota: 1000},
			wantNear:  true,
			wantQuota: 1000,
			wantUsed:  951,
			wantLimit: volumeLimitRefquota,
		},
		{
			name:      "refquota-bound full",
			usage:     truenas.DatasetQuotaUsage{Used: 1000, Referenced: 1000, Refquota: 1000},
			wantNear:  true,
			wantQuota: 1000,
			wantUsed:  1000,
			wantLimit: volumeLimitRefquota,
		},
		{
			// H1(a) — THE REGRESSION. 6 GiB of snapshots plus 4 GiB of live data
			// under a 10 GiB refquota: writes will NOT fail, the volume is healthy,
			// and `used` hitting the refquota is irrelevant because refquota does
			// not bound `used`.
			name: "large usedbysnapshots with low referenced is not near refquota",
			usage: truenas.DatasetQuotaUsage{
				Used:            10 << 30,
				Referenced:      4 << 30,
				UsedBySnapshots: 6 << 30,
				Refquota:        10 << 30,
			},
			wantNear:  false,
			wantQuota: 10 << 30,
			wantUsed:  4 << 30,
			wantLimit: volumeLimitRefquota,
		},
		{
			// Both limits set: the TIGHTER one binds, so a loose refquota can no
			// longer mask a quota that is nearly hit.
			name:      "tighter quota binds over a loose refquota",
			usage:     truenas.DatasetQuotaUsage{Used: 990, Referenced: 100, Quota: 1000, Refquota: 100000},
			wantNear:  true,
			wantQuota: 1000,
			wantUsed:  990,
			wantLimit: volumeLimitQuota,
		},
		{
			name:      "tighter refquota binds over a loose quota",
			usage:     truenas.DatasetQuotaUsage{Used: 500, Referenced: 990, Quota: 100000, Refquota: 1000},
			wantNear:  true,
			wantQuota: 1000,
			wantUsed:  990,
			wantLimit: volumeLimitRefquota,
		},
		{
			// M1 — a zvol carries no quota/refquota at all; volsize is its capacity.
			name:      "zvol near-full on volsize is near quota",
			usage:     truenas.DatasetQuotaUsage{Type: "VOLUME", Used: 1200, Referenced: 990, Volsize: 1000},
			wantNear:  true,
			wantQuota: 1000,
			wantUsed:  990,
			wantLimit: volumeLimitVolsize,
		},
		{
			name:      "zvol well under volsize",
			usage:     truenas.DatasetQuotaUsage{Type: "VOLUME", Used: 1200, Referenced: 100, Volsize: 1000},
			wantNear:  false,
			wantQuota: 1000,
			wantUsed:  100,
			wantLimit: volumeLimitVolsize,
		},
	}
	for _, tc := range cases {
		usage := tc.usage
		assert.Equal(t, tc.wantNear, volumeUsageNearQuota(&usage), tc.name)
		used, quota, limit := volumeUsageBasis(&usage)
		assert.Equal(t, tc.wantUsed, used, tc.name+": usage numerator")
		assert.Equal(t, tc.wantQuota, quota, tc.name+": binding limit")
		assert.Equal(t, tc.wantLimit, limit, tc.name+": limit name")
	}
}

// M1 — before this fix the effective quota was 0 for every zvol (quota and
// refquota are filesystem-only properties), so scale_csi_volume_quota_bytes was
// published as 0, near_quota was pinned at 0, and ScaleCSIVolumeNearQuota could
// never fire for an iSCSI/NVMe-oF volume however full it was.
func TestControllerGetVolumeCoversBlockVolumes(t *testing.T) {
	client := truenas.NewMockClient()
	d := newUsageTestDriver(client, true)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/zvol-full", Type: "VOLUME", Volsize: 1000})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, "pool/parent/zvol-full", map[string]string{
		PropManagedResource: "true", PropProvisionSuccess: "true",
	}))
	setDatasetReferenced(client, "pool/parent/zvol-full", 990, 0)

	resp, err := d.ControllerGetVolume(ctx, &csi.ControllerGetVolumeRequest{VolumeId: "zvol-full"})
	require.NoError(t, err)
	cond := resp.GetStatus().GetVolumeCondition()
	require.NotNil(t, cond)
	assert.True(t, cond.GetAbnormal(), "a zvol above 95%% of its volsize is abnormal")
	assert.Contains(t, cond.GetMessage(), volumeLimitVolsize)
	assert.Equal(t, 1000.0, testutil.ToFloat64(volumeQuotaBytes.WithLabelValues("zvol-full")),
		"a zvol's volsize is its binding limit, not 0/unlimited")
	assert.Equal(t, 1.0, testutil.ToFloat64(volumeNearQuota.WithLabelValues("zvol-full")))
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

// H1 — the whole RPC on the real failure shape: a 10 GiB refquota'd volume with
// 6 GiB of snapshots and 4 GiB of live data. Writes will NOT fail; `used` equals
// the refquota only because refquota does not bound `used`. Before the fix this
// returned Abnormal with "volume used 10737418240 of 10737418240 effective quota
// bytes", surfaced as a PVC event, permanently.
func TestControllerGetVolumeSnapshotSpaceIsNotNearRefquota(t *testing.T) {
	client := truenas.NewMockClient()
	d := newUsageTestDriver(client, true)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/snap-heavy", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, "pool/parent/snap-heavy", map[string]string{
		PropManagedResource: "true", PropProvisionSuccess: "true",
	}))
	setDatasetUsage(client, "pool/parent/snap-heavy", 10<<30, 0, 10<<30, 0)
	setDatasetReferenced(client, "pool/parent/snap-heavy", 4<<30, 6<<30)

	resp, err := d.ControllerGetVolume(ctx, &csi.ControllerGetVolumeRequest{VolumeId: "snap-heavy"})
	require.NoError(t, err)
	assert.False(t, resp.GetStatus().GetVolumeCondition().GetAbnormal(),
		"a volume whose snapshots (not its data) fill `used` is under its refquota and healthy")
	assert.Equal(t, 0.0, testutil.ToFloat64(volumeNearQuota.WithLabelValues("snap-heavy")))
	assert.Equal(t, float64(4<<30), testutil.ToFloat64(volumeUsedBytes.WithLabelValues("snap-heavy")),
		"the published numerator is `referenced`, the quantity refquota bounds")
}

// H1 — the near-quota message must report the numbers it actually compared, and
// name the ZFS property that binds. The old text reported `used` against the
// refquota, which is where the wrong answer was visible to operators.
func TestNearQuotaConditionMessageReportsTheRealNumbers(t *testing.T) {
	usage := &truenas.DatasetQuotaUsage{Used: 1500, Referenced: 990, UsedBySnapshots: 510, Refquota: 1000}
	message := volumeNearQuotaMessage(usage)
	assert.Contains(t, message, "volume uses 990 of 1000 bytes")
	assert.Contains(t, message, volumeLimitRefquota)
	assert.Contains(t, message, "510 bytes are held by snapshots")
	assert.NotContains(t, message, "1500", "`used` is not the quantity a refquota bounds")
}

// L2 — the near-quota upgrade REPLACED the whole condition, so a volume that was
// both provision-failed and near-quota reported only the quota reason: the
// definitive-negative signal disappeared exactly when both were true.
func TestNearQuotaDoesNotOverwriteAMoreSeriousCondition(t *testing.T) {
	client := truenas.NewMockClient()
	d := newUsageTestDriver(client, true)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/failed-vol", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, "pool/parent/failed-vol", map[string]string{
		PropManagedResource: "true", PropProvisionSuccess: "false",
	}))
	setDatasetUsage(client, "pool/parent/failed-vol", 990, 1000, 0, 10)

	resp, err := d.ControllerGetVolume(ctx, &csi.ControllerGetVolumeRequest{VolumeId: "failed-vol"})
	require.NoError(t, err)
	cond := resp.GetStatus().GetVolumeCondition()
	require.NotNil(t, cond)
	assert.True(t, cond.GetAbnormal())
	assert.Contains(t, cond.GetMessage(), "dataset provisioning is explicitly marked failed",
		"the stronger, definitive-negative reason must survive the quota upgrade")
	assert.Contains(t, cond.GetMessage(), "95%", "the quota finding is appended, not dropped")
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

	// `referenced` is what a refquota bounds (H1): the walk must publish it, not
	// `used`, or the gauges disagree with the limit they are compared against.
	near := &truenas.Dataset{
		Name:       "pool/parent/walk-near",
		Used:       truenas.DatasetProperty{Parsed: float64(990)},
		Referenced: truenas.DatasetProperty{Parsed: float64(990)},
		Refquota:   truenas.DatasetProperty{Parsed: float64(1000)},
	}
	// walk-ok is the H1 shape: `used` is at 99% of the refquota because snapshots
	// hold the space, while `referenced` — the quantity refquota actually bounds —
	// is 1%. It must publish near_quota=0.
	ok := &truenas.Dataset{
		Name:            "pool/parent/walk-ok",
		Used:            truenas.DatasetProperty{Parsed: float64(990)},
		Referenced:      truenas.DatasetProperty{Parsed: float64(10)},
		Usedbysnapshots: truenas.DatasetProperty{Parsed: float64(980)},
		Refquota:        truenas.DatasetProperty{Parsed: float64(1000)},
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
