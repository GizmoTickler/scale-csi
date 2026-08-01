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

// ---------------------------------------------------------------------------
// M4 — pool_scan_state must be one-hot across the FUNCTION change too
// ---------------------------------------------------------------------------

// TestSetPoolHealthMetricsScanStateIsOneHotAcrossFunctions pins M4. Before the
// fix, SetPoolHealthMetrics only zeroed the states of the CURRENT function, so a
// finished SCRUB followed by a running RESILVER exported
// {function=SCRUB,state=FINISHED}=1 AND {function=RESILVER,state=SCANNING}=1
// simultaneously — the documented one-hot contract broken, and a stale series
// left for anyone alerting on it.
func TestSetPoolHealthMetricsScanStateIsOneHotAcrossFunctions(t *testing.T) {
	const pool = "gf5-scan-function-pool"

	SetPoolHealthMetrics(&truenas.PoolHealthSnapshot{
		Pool: pool, Status: truenas.PoolStatusOnline, Healthy: true,
		ScanFunction: truenas.PoolScanFunctionScrub, ScanState: truenas.PoolScanStateFinished,
	})
	require.Equal(t, 1.0, testutil.ToFloat64(
		poolScanState.WithLabelValues(pool, truenas.PoolScanFunctionScrub, truenas.PoolScanStateFinished)))

	// The scan function changes: a disk was replaced and the pool is resilvering.
	SetPoolHealthMetrics(&truenas.PoolHealthSnapshot{
		Pool: pool, Status: truenas.PoolStatusDegraded,
		ScanFunction: truenas.PoolScanFunctionResilver, ScanState: truenas.PoolScanStateScanning,
	})
	assert.Equal(t, 0.0, testutil.ToFloat64(
		poolScanState.WithLabelValues(pool, truenas.PoolScanFunctionScrub, truenas.PoolScanStateFinished)),
		"the previous scan FUNCTION's series must be zeroed, not merely left behind")
	assert.Equal(t, 1.0, testutil.ToFloat64(
		poolScanState.WithLabelValues(pool, truenas.PoolScanFunctionResilver, truenas.PoolScanStateScanning)))

	// Exactly one series across the whole function x state cross-product.
	total := 0.0
	for _, function := range poolScanFunctionLabels {
		for _, state := range poolScanStateLabels {
			total += testutil.ToFloat64(poolScanState.WithLabelValues(pool, function, state))
		}
	}
	assert.Equal(t, 1.0, total, "pool_scan_state must be one-hot across function x state")

	// And a pool with no scan at all lands on the NONE function.
	SetPoolHealthMetrics(&truenas.PoolHealthSnapshot{Pool: pool, Status: truenas.PoolStatusOnline, Healthy: true})
	assert.Equal(t, 0.0, testutil.ToFloat64(
		poolScanState.WithLabelValues(pool, truenas.PoolScanFunctionResilver, truenas.PoolScanStateScanning)))
}

// TestSetPoolHealthMetricsZeroesUnrecognizedStatus pins the smaller half of M4:
// an unrecognized status is exported dynamically, and must be zeroed when the
// pool moves on rather than staying pinned at 1 forever.
func TestSetPoolHealthMetricsZeroesUnrecognizedStatus(t *testing.T) {
	const pool = "gf5-dynamic-status-pool"

	SetPoolHealthMetrics(&truenas.PoolHealthSnapshot{Pool: pool, Status: "SUSPENDED"})
	require.Equal(t, 1.0, testutil.ToFloat64(poolStatus.WithLabelValues(pool, "SUSPENDED")))

	SetPoolHealthMetrics(&truenas.PoolHealthSnapshot{Pool: pool, Status: truenas.PoolStatusOnline, Healthy: true})
	assert.Equal(t, 0.0, testutil.ToFloat64(poolStatus.WithLabelValues(pool, "SUSPENDED")),
		"a recovered pool must not leave an unrecognized status series at 1 forever")
	assert.Equal(t, 1.0, testutil.ToFloat64(poolStatus.WithLabelValues(pool, truenas.PoolStatusOnline)))
}

// ---------------------------------------------------------------------------
// M5 — the cached snapshot needs a staleness bound
// ---------------------------------------------------------------------------

// TestPoolHealthSnapshotExpires pins M5. Keeping the last snapshot across a blip
// is correct; keeping it across an outage is not — a stale DEGRADED keeps
// alerting after a real recovery, and a stale ONLINE masks a real degradation.
func TestPoolHealthSnapshotExpires(t *testing.T) {
	mock := truenas.NewMockClient()
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{
		Status: truenas.PoolStatusDegraded, StatusDetail: "One or more devices are faulted.",
	}
	d := healthTestDriver(mock)
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "60s"}

	d.sampleBackendHealth(context.Background(), "flashstor")
	require.NotNil(t, d.poolHealthSnapshot())
	require.True(t, d.volumeCondition(managedDataset()).GetAbnormal())

	// Age the cached snapshot past its TTL (3 x interval).
	stale := *d.backendHealth.Load()
	stale.SampledAt = time.Now().Add(-d.backendHealthTTL() - time.Second)
	d.backendHealth.Store(&stale)

	assert.Nil(t, d.poolHealthSnapshot(), "a snapshot older than its TTL must stop driving conditions")
	assert.Equal(t, volumeConditionFromDataset(managedDataset()), d.volumeCondition(managedDataset()),
		"past the TTL the condition falls back to the pre-GF5 dataset-only semantics")

	// A failing sample past the TTL raises the staleness gauge.
	mock.InjectHealthError = errors.New("appliance unreachable")
	d.sampleBackendHealth(context.Background(), "flashstor")
	assert.Equal(t, 1.0, testutil.ToFloat64(poolHealthStale.WithLabelValues("flashstor")))

	// A recovered sample clears it.
	mock.InjectHealthError = nil
	d.sampleBackendHealth(context.Background(), "flashstor")
	assert.Equal(t, 0.0, testutil.ToFloat64(poolHealthStale.WithLabelValues("flashstor")))
}

func TestBackendHealthTTLTracksTheInterval(t *testing.T) {
	d := healthTestDriver(truenas.NewMockClient())
	assert.Equal(t, 3*time.Minute, d.backendHealthTTL(), "the default 60s cadence gives a 3m TTL")

	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "2m"}
	assert.Equal(t, 6*time.Minute, d.backendHealthTTL())

	// A sub-floor interval is clamped before the multiplier is applied.
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "1s"}
	assert.Equal(t, 3*minBackendHealthInterval, d.backendHealthTTL())

	// So is an over-ceiling one (M6 timing caveat): 2 x interval must stay inside
	// the ScaleCSIPoolDegraded alert's 5m hold, or the undamped gauge could fire
	// an alert while every PVC still carries the previous verdict — contradicting
	// docs/production.md's "can never disagree".
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "1h"}
	assert.Equal(t, 3*maxBackendHealthInterval, d.backendHealthTTL())
	interval, err := d.resolveBackendHealthInterval()
	require.NoError(t, err)
	assert.Equal(t, maxBackendHealthInterval, interval)
	assert.Less(t, 2*interval, 5*time.Minute,
		"a hysteresis-confirmed condition flip must land inside the alert hold docs/production.md promises")
}

// ---------------------------------------------------------------------------
// M6 — hysteresis on the fleet-wide condition fan-out
// ---------------------------------------------------------------------------

// TestBackendHealthFanOutHasHysteresis pins M6. One pool backs every managed
// volume, so an undamped DEGRADED<->ONLINE flap rewrites every PVC's condition
// and churns a PVC event for each of them on every tick.
func TestBackendHealthFanOutHasHysteresis(t *testing.T) {
	ctx := context.Background()
	mock := truenas.NewMockClient()
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusOnline, Healthy: true}
	d := healthTestDriver(mock)

	// First observation is never damped: there is nothing to flap against.
	d.sampleBackendHealth(ctx, "flashstor")
	require.False(t, d.volumeCondition(managedDataset()).GetAbnormal())

	// A single DEGRADED sample must NOT flip the whole fleet.
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusDegraded}
	d.sampleBackendHealth(ctx, "flashstor")
	assert.False(t, d.volumeCondition(managedDataset()).GetAbnormal(),
		"one degraded sample must not flip every managed PVC's condition")
	// Metrics are NOT damped: Prometheus must see the flap as a flap.
	assert.Equal(t, 1.0, testutil.ToFloat64(poolStatus.WithLabelValues("flashstor", truenas.PoolStatusDegraded)))

	// The second consecutive degraded sample confirms it.
	d.sampleBackendHealth(ctx, "flashstor")
	assert.True(t, d.volumeCondition(managedDataset()).GetAbnormal(),
		"a transition confirmed by K consecutive samples must flip")

	// Recovery is damped symmetrically.
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusOnline, Healthy: true}
	d.sampleBackendHealth(ctx, "flashstor")
	assert.True(t, d.volumeCondition(managedDataset()).GetAbnormal(), "a single healthy sample must not clear it either")
	d.sampleBackendHealth(ctx, "flashstor")
	assert.False(t, d.volumeCondition(managedDataset()).GetAbnormal())
}

// TestBackendHealthFlapNeverFlips proves a pool alternating on every sample
// never flips the fan-out at all, which is the actual anti-churn property.
func TestBackendHealthFlapNeverFlips(t *testing.T) {
	ctx := context.Background()
	mock := truenas.NewMockClient()
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusOnline, Healthy: true}
	d := healthTestDriver(mock)
	d.sampleBackendHealth(ctx, "flashstor")

	for i := 0; i < 6; i++ {
		if i%2 == 0 {
			mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusDegraded}
		} else {
			mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusOnline, Healthy: true}
		}
		d.sampleBackendHealth(ctx, "flashstor")
		assert.False(t, d.volumeCondition(managedDataset()).GetAbnormal(),
			"an alternating pool must never flip the fleet-wide condition (sample %d)", i)
	}
}

// TestSetPoolHealthMetricsScanStateIsOneHotAcrossTheWholeDomain pins M4's
// round-2 gap. Round 1 made the KNOWN-function transition one-hot, but the
// contract in docs/production.md is "exactly one cell is 1" across the whole
// advertised domain, and three cases still broke it:
//
//   - a pool with NO scan zeroed all nine fixed cells and set none (sum 0);
//   - an UNKNOWN state did the same;
//   - an UNKNOWN function lit a dynamic cell that nothing ever retired, so the
//     next known sample left TWO series at 1.
//
// Every assertion below is a sum over the FULL domain — the fixed cross-product
// plus every dynamic cell this test has ever caused to be exported.
func TestSetPoolHealthMetricsScanStateIsOneHotAcrossTheWholeDomain(t *testing.T) {
	const pool = "gf5-scan-domain-pool"
	dynamic := [][2]string{
		{"REBUILD", truenas.PoolScanStateScanning},
		{"REBUILD", "TRUNDLING"},
		{truenas.PoolScanFunctionScrub, "TRUNDLING"},
	}
	total := func() float64 {
		sum := 0.0
		for _, function := range poolScanFunctionLabels {
			for _, state := range poolScanStateLabels {
				sum += testutil.ToFloat64(poolScanState.WithLabelValues(pool, function, state))
			}
		}
		for _, cell := range dynamic {
			sum += testutil.ToFloat64(poolScanState.WithLabelValues(pool, cell[0], cell[1]))
		}
		return sum
	}

	for _, step := range []struct {
		name            string
		function, state string
		wantFunction    string
		wantState       string
	}{
		{name: "running scrub", function: truenas.PoolScanFunctionScrub, state: truenas.PoolScanStateScanning,
			wantFunction: truenas.PoolScanFunctionScrub, wantState: truenas.PoolScanStateScanning},
		{name: "no scan at all", wantFunction: poolScanNone, wantState: poolScanNone},
		{name: "unknown function, known state", function: "REBUILD", state: truenas.PoolScanStateScanning,
			wantFunction: "REBUILD", wantState: truenas.PoolScanStateScanning},
		{name: "unknown function AND unknown state", function: "REBUILD", state: "trundling",
			wantFunction: "REBUILD", wantState: "TRUNDLING"},
		{name: "known function, unknown state", function: truenas.PoolScanFunctionScrub, state: "trundling",
			wantFunction: truenas.PoolScanFunctionScrub, wantState: "TRUNDLING"},
		{name: "back to a finished resilver", function: truenas.PoolScanFunctionResilver, state: truenas.PoolScanStateFinished,
			wantFunction: truenas.PoolScanFunctionResilver, wantState: truenas.PoolScanStateFinished},
		{name: "and idle again", wantFunction: poolScanNone, wantState: poolScanNone},
	} {
		t.Run(step.name, func(t *testing.T) {
			SetPoolHealthMetrics(&truenas.PoolHealthSnapshot{
				Pool: pool, Status: truenas.PoolStatusOnline, Healthy: true,
				ScanFunction: step.function, ScanState: step.state,
			})
			assert.Equal(t, 1.0,
				testutil.ToFloat64(poolScanState.WithLabelValues(pool, step.wantFunction, step.wantState)),
				"the current cell must be 1")
			assert.Equal(t, 1.0, total(),
				"pool_scan_state must be one-hot across the WHOLE function x state domain, dynamic cells included")
		})
	}
}
