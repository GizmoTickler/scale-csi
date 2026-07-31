package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func healthTestDriver(mock *truenas.MockClient) *Driver {
	return &Driver{
		name: "org.scale.csi",
		config: &Config{
			DriverName: "org.scale.csi",
			ZFS:        ZFSConfig{DatasetParentName: "flashstor/scale-csi"},
			NFS:        NFSConfig{Enabled: true, ShareHost: "10.0.0.1"},
		},
		truenasClient: mock,
	}
}

func managedDataset() *truenas.Dataset {
	return &truenas.Dataset{
		Name: "flashstor/scale-csi/vol",
		UserProperties: map[string]truenas.UserProperty{
			PropManagedResource:  {Value: "true", Source: "local"},
			PropProvisionSuccess: {Value: "true", Source: "local"},
		},
	}
}

func TestBackendHealthConfigIntervalDuration(t *testing.T) {
	cases := []struct {
		interval string
		want     time.Duration
		wantErr  bool
	}{
		{"", 60 * time.Second, false},
		{"5m", 5 * time.Minute, false},
		{"1s", minBackendHealthInterval, false}, // clamped
		{"not-a-duration", 0, true},
	}
	for _, tc := range cases {
		got, err := BackendHealthConfig{Interval: tc.interval}.IntervalDuration()
		if tc.wantErr {
			require.Error(t, err, tc.interval)
			continue
		}
		require.NoError(t, err, tc.interval)
		assert.Equal(t, tc.want, got, tc.interval)
	}
}

// TestVolumeConditionWithoutSnapshotIsUnchanged is the default-off guard: with
// the poller disabled (no snapshot), every VolumeCondition is byte-identical to
// the pre-GF5 dataset-only helper.
func TestVolumeConditionWithoutSnapshotIsUnchanged(t *testing.T) {
	d := healthTestDriver(truenas.NewMockClient())
	for _, ds := range []*truenas.Dataset{
		managedDataset(),
		{Name: "flashstor/scale-csi/legacy"},
		{Name: "flashstor/scale-csi/failed", UserProperties: map[string]truenas.UserProperty{
			PropProvisionSuccess: {Value: "false", Source: "local"},
		}},
	} {
		assert.Equal(t, volumeConditionFromDataset(ds), d.volumeCondition(ds), ds.Name)
	}
}

// TestComposeVolumeConditionMatrix is the severity matrix: which pool states
// make a volume Abnormal and which are merely descriptive.
func TestComposeVolumeConditionMatrix(t *testing.T) {
	base := volumeConditionFromDataset(managedDataset())
	legacy := volumeConditionFromDataset(&truenas.Dataset{Name: "legacy"})
	failed := volumeConditionFromDataset(&truenas.Dataset{UserProperties: map[string]truenas.UserProperty{
		PropProvisionSuccess: {Value: "false", Source: "local"},
	}})

	t.Run("healthy pool leaves the condition untouched", func(t *testing.T) {
		got := composeVolumeCondition(base, &truenas.PoolHealthSnapshot{
			Pool: "flashstor", Status: truenas.PoolStatusOnline, Healthy: true,
			ScanFunction: truenas.PoolScanFunctionScrub, ScanState: truenas.PoolScanStateFinished,
		})
		assert.Equal(t, base, got)
	})

	t.Run("DEGRADED/FAULTED/UNAVAIL are abnormal", func(t *testing.T) {
		for _, statusValue := range []string{truenas.PoolStatusDegraded, truenas.PoolStatusFaulted, truenas.PoolStatusUnavail} {
			got := composeVolumeCondition(base, &truenas.PoolHealthSnapshot{
				Pool: "flashstor", Status: statusValue, StatusDetail: "One or more devices are faulted.",
			})
			require.NotNil(t, got)
			assert.True(t, got.GetAbnormal(), statusValue)
			assert.Contains(t, got.GetMessage(), "flashstor")
			assert.Contains(t, got.GetMessage(), statusValue)
			assert.Contains(t, got.GetMessage(), "One or more devices are faulted.")
		}
	})

	t.Run("OFFLINE and REMOVED are NOT abnormal", func(t *testing.T) {
		// An offline spare or a removed cache device does not put the data path at
		// risk; marking every PVC unhealthy for it would be a false positive.
		for _, statusValue := range []string{truenas.PoolStatusOffline, truenas.PoolStatusRemoved} {
			got := composeVolumeCondition(base, &truenas.PoolHealthSnapshot{Pool: "flashstor", Status: statusValue})
			require.NotNil(t, got)
			assert.False(t, got.GetAbnormal(), statusValue)
		}
	})

	t.Run("an in-progress scrub is a message, never Abnormal", func(t *testing.T) {
		got := composeVolumeCondition(base, &truenas.PoolHealthSnapshot{
			Pool: "flashstor", Status: truenas.PoolStatusOnline, Healthy: true,
			ScanFunction: truenas.PoolScanFunctionScrub, ScanState: truenas.PoolScanStateScanning, ScanPercentage: 42.5,
		})
		require.NotNil(t, got)
		assert.False(t, got.GetAbnormal(), "a routine scrub must not mark every PVC unhealthy")
		assert.Contains(t, got.GetMessage(), "SCRUB in progress")
		assert.Contains(t, got.GetMessage(), "42.5%")
	})

	t.Run("a resilver is reported by name", func(t *testing.T) {
		got := composeVolumeCondition(base, &truenas.PoolHealthSnapshot{
			Pool: "flashstor", Status: truenas.PoolStatusOnline,
			ScanFunction: truenas.PoolScanFunctionResilver, ScanState: truenas.PoolScanStateScanning,
		})
		assert.Contains(t, got.GetMessage(), "RESILVER in progress")
		assert.False(t, got.GetAbnormal())
	})

	t.Run("scan errors and disk temperature alerts are warnings", func(t *testing.T) {
		got := composeVolumeCondition(base, &truenas.PoolHealthSnapshot{
			Pool: "flashstor", Status: truenas.PoolStatusOnline,
			ScanErrors: 3, TemperatureAlerts: 2,
		})
		require.NotNil(t, got)
		assert.False(t, got.GetAbnormal())
		assert.Contains(t, got.GetMessage(), "3 errors")
		assert.Contains(t, got.GetMessage(), "2 disk temperature alert(s)")
	})

	t.Run("a dataset-level failure outranks a pool warning", func(t *testing.T) {
		got := composeVolumeCondition(failed, &truenas.PoolHealthSnapshot{
			Pool: "flashstor", Status: truenas.PoolStatusOnline, TemperatureAlerts: 1,
		})
		assert.Equal(t, failed, got, "the more specific dataset-level marker must win")
	})

	t.Run("a pool warning is appended to a legacy unverified message", func(t *testing.T) {
		got := composeVolumeCondition(legacy, &truenas.PoolHealthSnapshot{
			Pool: "flashstor", Status: truenas.PoolStatusOnline, TemperatureAlerts: 1,
		})
		require.NotNil(t, got)
		assert.False(t, got.GetAbnormal())
		assert.Contains(t, got.GetMessage(), "health unverified")
		assert.Contains(t, got.GetMessage(), "temperature alert")
	})

	t.Run("a nil snapshot is a strict pass-through", func(t *testing.T) {
		assert.Equal(t, base, composeVolumeCondition(base, nil))
		assert.Nil(t, composeVolumeCondition(nil, nil))
	})
}

// TestSampleBackendHealthFansPoolOutToVolumes proves a sample updates the cache
// and reaches the VolumeCondition of every managed volume.
func TestSampleBackendHealthFansPoolOutToVolumes(t *testing.T) {
	mock := truenas.NewMockClient()
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{
		Status: truenas.PoolStatusDegraded, Healthy: false,
		StatusDetail: "One or more devices has been taken offline.",
		Disks:        []string{"nvme0n1", "nvme1n1"},
	}
	d := healthTestDriver(mock)
	require.Nil(t, d.poolHealthSnapshot())

	d.sampleBackendHealth(context.Background(), "flashstor")
	snapshot := d.poolHealthSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, "flashstor", snapshot.Pool)
	assert.True(t, snapshot.Degraded())

	// Every managed volume on the pool now reports abnormal.
	for _, name := range []string{"vol-a", "vol-b", "vol-c"} {
		condition := d.volumeCondition(&truenas.Dataset{
			Name: "flashstor/scale-csi/" + name,
			UserProperties: map[string]truenas.UserProperty{
				PropManagedResource:  {Value: "true", Source: "local"},
				PropProvisionSuccess: {Value: "true", Source: "local"},
			},
		})
		require.NotNil(t, condition, name)
		assert.True(t, condition.GetAbnormal(), name)
	}
}

func TestSampleBackendHealthCollectsTemperatureAlerts(t *testing.T) {
	mock := truenas.NewMockClient()
	mock.TemperatureAlerts = []string{"nvme0n1 is 78C"}
	d := healthTestDriver(mock)

	d.sampleBackendHealth(context.Background(), "flashstor")
	snapshot := d.poolHealthSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, 1, snapshot.TemperatureAlerts)
	assert.Equal(t, 1, mock.TempAlertCalls)
}

// TestSampleBackendHealthKeepsLastGoodSnapshot proves a transient backend blip
// does not flip every PVC's condition.
func TestSampleBackendHealthKeepsLastGoodSnapshot(t *testing.T) {
	mock := truenas.NewMockClient()
	d := healthTestDriver(mock)
	d.sampleBackendHealth(context.Background(), "flashstor")
	good := d.poolHealthSnapshot()
	require.NotNil(t, good)

	mock.InjectHealthError = errors.New("simulated pool.query failure")
	d.sampleBackendHealth(context.Background(), "flashstor")
	assert.Same(t, good, d.poolHealthSnapshot(), "a failed sample must leave the previous snapshot in place")
}

// TestSampleBackendHealthTemperatureFailureIsNonFatal proves a temperature-alert
// failure still publishes the pool sample.
func TestSampleBackendHealthTemperatureFailureIsNonFatal(t *testing.T) {
	mock := truenas.NewMockClient()
	mock.InjectTempAlertErr = errors.New("simulated disk.temperature_alerts failure")
	d := healthTestDriver(mock)

	d.sampleBackendHealth(context.Background(), "flashstor")
	snapshot := d.poolHealthSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, 0, snapshot.TemperatureAlerts)
}

// TestStartBackendHealthDefaultOff is the zero-cost guard.
func TestStartBackendHealthDefaultOff(t *testing.T) {
	mock := truenas.NewMockClient()
	d := healthTestDriver(mock)
	d.startBackendHealth()
	d.stopBackendHealth()
	assert.Zero(t, mock.PoolHealthCalls, "a disabled poller must issue zero API calls")
	assert.Nil(t, d.poolHealthSnapshot())
}

func TestStartBackendHealthSamplesImmediately(t *testing.T) {
	mock := truenas.NewMockClient()
	d := healthTestDriver(mock)
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "1h"}

	d.startBackendHealth()
	require.Eventually(t, func() bool { return d.poolHealthSnapshot() != nil }, 2*time.Second, 10*time.Millisecond,
		"the poller must populate immediately rather than after a full interval")
	d.stopBackendHealth()
	assert.Equal(t, 1, mock.PoolHealthCalls)
}

func TestStartBackendHealthRejectsInvalidInterval(t *testing.T) {
	mock := truenas.NewMockClient()
	d := healthTestDriver(mock)
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "banana"}
	d.startBackendHealth()
	d.stopBackendHealth()
	assert.Zero(t, mock.PoolHealthCalls)
}

// TestSetPoolHealthMetricsIsOneHot proves a recovered pool does not leave a
// stale DEGRADED series firing an alert forever.
func TestSetPoolHealthMetricsIsOneHot(t *testing.T) {
	const pool = "gf5-metrics-pool"
	SetPoolHealthMetrics(&truenas.PoolHealthSnapshot{
		Pool: pool, Status: truenas.PoolStatusDegraded, Healthy: false,
		ScanFunction: truenas.PoolScanFunctionResilver, ScanState: truenas.PoolScanStateScanning,
		ScanErrors: 4, TemperatureAlerts: 2,
	})
	assert.Equal(t, 1.0, testutil.ToFloat64(poolStatus.WithLabelValues(pool, truenas.PoolStatusDegraded)))
	assert.Equal(t, 0.0, testutil.ToFloat64(poolStatus.WithLabelValues(pool, truenas.PoolStatusOnline)))
	assert.Equal(t, 0.0, testutil.ToFloat64(poolHealthy.WithLabelValues(pool)))
	assert.Equal(t, 1.0, testutil.ToFloat64(poolScanState.WithLabelValues(pool, truenas.PoolScanFunctionResilver, truenas.PoolScanStateScanning)))
	assert.Equal(t, 4.0, testutil.ToFloat64(poolScanErrors.WithLabelValues(pool)))
	assert.Equal(t, 2.0, testutil.ToFloat64(poolDiskTempAlerts.WithLabelValues(pool)))

	// Recovery must zero the DEGRADED series, not merely stop updating it.
	SetPoolHealthMetrics(&truenas.PoolHealthSnapshot{
		Pool: pool, Status: truenas.PoolStatusOnline, Healthy: true,
		ScanFunction: truenas.PoolScanFunctionResilver, ScanState: truenas.PoolScanStateFinished,
	})
	assert.Equal(t, 0.0, testutil.ToFloat64(poolStatus.WithLabelValues(pool, truenas.PoolStatusDegraded)))
	assert.Equal(t, 1.0, testutil.ToFloat64(poolStatus.WithLabelValues(pool, truenas.PoolStatusOnline)))
	assert.Equal(t, 1.0, testutil.ToFloat64(poolHealthy.WithLabelValues(pool)))
	assert.Equal(t, 0.0, testutil.ToFloat64(poolScanState.WithLabelValues(pool, truenas.PoolScanFunctionResilver, truenas.PoolScanStateScanning)))

	// A nil / pool-less sample is ignored.
	SetPoolHealthMetrics(nil)
	SetPoolHealthMetrics(&truenas.PoolHealthSnapshot{})
}
