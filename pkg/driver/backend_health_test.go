package driver

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
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
// is correct; keeping it across an outage is not — a stale DEGRADED would keep
// asserting an abnormal CONDITION on every PVC after a real recovery, and a
// stale ONLINE would mask a real degradation.
//
// The TTL bounds CONDITIONS only. It does NOT stop the frozen raw gauge from
// keeping ScaleCSIPoolDegraded firing (that alert is deliberately not gated on
// scale_csi_pool_health_stale), which is precisely why the staleness gauge must
// be published the moment the snapshot stops being served.
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

// TestBackendHealthStalePublicationSurvivesAConcurrentSample is the regression
// test for the LOST UPDATE between the CSI read path and the poller.
//
// It is deliberately CONCURRENT, and it has to be: the defect is not a data
// race, so `go test -race` is blind to it. Two goroutines write one observable
// pair — the cached snapshot POINTER and scale_csi_pool_health_stale. The reader
// decides "the pointer I read is expired" and raises the gauge; the poller
// clears the gauge for a sample it has not stored yet. Interleave them and the
// process ends with a FRESH cached sample carrying stale = 1, which no later
// event corrects until the next successful sample:
//
//	reader: mark S stale ... poller: stale = 0 ... reader: re-read, still S,
//	leave 1 ... poller: store fresh pointer.
//
// The old test fabricated the fresh pointer, never called the successful-sample
// publication path, and never ran the two paths at the same time, so it could
// not fail on this. This one runs the REAL publication path against a spinning
// CSI reader and asserts the only invariant that matters: once everything has
// quiesced, a fresh cached sample never carries stale = 1.
func TestBackendHealthStalePublicationSurvivesAConcurrentSample(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("the interleaving requires at least two schedulable Ps")
	}
	const pool = "lostupdatepool"
	mock := truenas.NewMockClient()
	// The same verdict on both sides, so the hysteresis never holds the sample:
	// the poller publishes the fresh pointer directly, which is the shape that
	// exposes the lost update.
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{
		Status: truenas.PoolStatusDegraded, StatusDetail: "vdev degraded", SampledAt: time.Now(),
	}
	d := healthTestDriver(mock)
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "60s"}

	const rounds = 400
	for round := 0; round < rounds; round++ {
		expired := &truenas.PoolHealthSnapshot{
			Pool: pool, Status: truenas.PoolStatusDegraded,
			SampledAt: time.Now().Add(-d.backendHealthTTL() - time.Second),
		}
		d.backendHealthPendingFlips.Store(0)
		d.backendHealth.Store(expired)
		// The read path has already raised staleness for the expired snapshot; the
		// successful sample below is what has to clear it and keep it cleared.
		SetPoolHealthStale(pool, true)

		var wg sync.WaitGroup
		var reading, stop atomic.Bool
		wg.Add(1)
		go func() {
			defer wg.Done()
			reading.Store(true)
			for !stop.Load() {
				// The real CSI read path: it loads the pointer, finds it expired and
				// publishes the staleness verdict for it.
				d.poolHealthSnapshot()
			}
		}()
		for !reading.Load() {
			runtime.Gosched()
		}
		d.sampleBackendHealth(context.Background(), pool)
		stop.Store(true)
		wg.Wait()

		published := d.backendHealth.Load()
		require.NotNil(t, published)
		require.NotSame(t, expired, published, "round %d: the poller must have published a fresh snapshot", round)
		if got := testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)); got != 0 {
			t.Fatalf("round %d: a fresh cached sample is published with scale_csi_pool_health_stale = %v. The read path's "+
				"staleness decision was made against the SUPERSEDED snapshot and survived the poller's publication: the gauge "+
				"write and the pointer change must happen inside ONE critical section, not as a gauge write followed by a "+
				"pointer re-read.", round, got)
		}
	}
}

// TestBackendHealthPublishesThePoolSampleBeforeTheSecondBackendRead closes the
// publication/commit gap.
//
// PoolHealth timestamps the snapshot when pool.query returns, but publication
// used to wait for disk.temperature_alerts — a SECOND backend read that may burn
// the rest of the 30s call context. For that whole window a VALID pool sample
// existed while the old raw gauges, the old condition, flip_pending and stale
// were all still exposed. That fits none of the named divergence classes (the
// raw sample had not changed, the sample had not failed, the exporter had not
// published it, and a previous sample existed), so it is closed by construction
// rather than documented.
//
// The test holds disk.temperature_alerts IN FLIGHT and asserts the pool verdict
// is already published, then that the temperature count is refreshed afterwards
// without re-running the hysteresis.
func TestBackendHealthPublishesThePoolSampleBeforeTheSecondBackendRead(t *testing.T) {
	const pool = "pubgappool"
	mock := truenas.NewMockClient()
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{
		Status: truenas.PoolStatusDegraded, StatusDetail: "One or more devices are faulted.",
		Disks: []string{"nvme0n1"}, SampledAt: time.Now(),
	}
	mock.TemperatureAlerts = []string{"nvme0n1 is 78C"}
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	mock.TempAlertEntered = entered
	mock.TempAlertRelease = release

	d := healthTestDriver(mock)
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "60s"}
	SetPoolHealthStale(pool, true) // the cold-start verdict that is still standing

	done := make(chan struct{})
	go func() {
		defer close(done)
		d.sampleBackendHealth(context.Background(), pool)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("disk.temperature_alerts was never called")
	}

	// The pool sample is in hand and the SECOND backend read has not returned.
	snapshot := d.poolHealthSnapshot()
	require.NotNil(t, snapshot, "a pool sample that already exists must not sit unpublished behind a second backend read")
	assert.True(t, snapshot.Degraded())
	assert.Equal(t, 1.0, testutil.ToFloat64(poolStatus.WithLabelValues(pool, truenas.PoolStatusDegraded)),
		"the RAW gauges must carry the sample as soon as it exists")
	assert.Equal(t, 0.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)),
		"the staleness verdict must be cleared by the sample, not by the disk-temperature call that follows it")
	assert.True(t, d.volumeCondition(managedDataset()).GetAbnormal(),
		"every managed PVC must already read the new condition")
	assert.NotZero(t, testutil.ToFloat64(poolHealthLastSuccess.WithLabelValues(pool)),
		"the driver-owned last-success timestamp is part of the same publication")

	close(release)
	<-done

	refreshed := d.poolHealthSnapshot()
	require.NotNil(t, refreshed)
	assert.Equal(t, 1, refreshed.TemperatureAlerts, "the temperature count is refreshed onto the published sample")
	assert.Equal(t, 1.0, testutil.ToFloat64(poolDiskTempAlerts.WithLabelValues(pool)))
	assert.Zero(t, d.backendHealthPendingFlips.Load(),
		"the temperature refresh must NOT re-run the hysteresis; one backend sample must never count twice")
}

// TestPoolHealthLastSuccessTimestampTracksSamplesNotScrapes pins N3: the triage
// procedure needs the driver's own last successful sample time. PromQL
// timestamp() returns the SAMPLE's timestamp, which for a pull exporter is the
// scrape time — a frozen driver that keeps answering scrapes looks fresh by that
// query, and a scrape outage makes a healthy driver look stale. So the driver
// exports the value itself, and it moves ONLY when a usable sample lands.
func TestPoolHealthLastSuccessTimestampTracksSamplesNotScrapes(t *testing.T) {
	const pool = "lastsuccesspool"
	first := time.Now().Add(-90 * time.Second)
	mock := truenas.NewMockClient()
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusOnline, Healthy: true, SampledAt: first}
	d := healthTestDriver(mock)
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "60s"}

	d.sampleBackendHealth(context.Background(), pool)
	assert.InDelta(t, float64(first.UnixNano())/1e9, testutil.ToFloat64(poolHealthLastSuccess.WithLabelValues(pool)), 0.001)

	// A failed sample must NOT advance it — that is the whole point: this is what
	// stops a frozen poller from looking fresh.
	mock.InjectHealthError = errors.New("appliance unreachable")
	d.sampleBackendHealth(context.Background(), pool)
	assert.InDelta(t, float64(first.UnixNano())/1e9, testutil.ToFloat64(poolHealthLastSuccess.WithLabelValues(pool)), 0.001)

	// Nor does a valid pool.query that simply does not list the pool.
	mock.InjectHealthError = nil
	mock.PoolQueryResultSet, mock.PoolQueryResult = true, []interface{}{}
	d.sampleBackendHealth(context.Background(), pool)
	assert.InDelta(t, float64(first.UnixNano())/1e9, testutil.ToFloat64(poolHealthLastSuccess.WithLabelValues(pool)), 0.001)

	second := time.Now()
	mock.PoolQueryResultSet = false
	mock.PoolHealthValue.SampledAt = second
	d.sampleBackendHealth(context.Background(), pool)
	assert.InDelta(t, float64(second.UnixNano())/1e9, testutil.ToFloat64(poolHealthLastSuccess.WithLabelValues(pool)), 0.001)
}

// TestBackendHealthColdStartPublishesHonestState pins divergence class 6.
//
// The CSI and metrics servers are serving before startBackendHealth produces
// anything, and every bit of this state is process-local. If the FIRST sample of
// a process fails there is no previous snapshot to freeze, so the raw
// scale_csi_pool_* series never come into existence and ScaleCSIPoolDegraded
// cannot fire whatever the pool is doing. That window is unbounded, and it used
// to be completely SILENT because the staleness verdict was published only when
// a previous snapshot existed.
//
// The sample here also pins the poll-stall termination condition: a valid
// pool.query that simply does not list the pool comes back as "pool ... not
// found". The backend ANSWERED; this is still a failed sample. The fixture is
// therefore a REAL pool.query response — an empty result decoded by the
// production decoder — not an injected error string, because an injected string
// would prove only that some error takes the failed-sample path.
func TestBackendHealthColdStartPublishesHonestState(t *testing.T) {
	const pool = "coldstartpool"
	mock := truenas.NewMockClient()
	mock.PoolQueryResultSet, mock.PoolQueryResult = true, []interface{}{}
	// Prove the fixture really is the missing-pool ANSWER, not a transport error.
	_, fixtureErr := mock.PoolHealth(context.Background(), pool)
	require.ErrorContains(t, fixtureErr, "not found",
		"the cold-start fixture must be a valid pool.query response that lists no pool")

	d := healthTestDriver(mock)
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "60s"}

	statusSeries := testutil.CollectAndCount(poolStatus)
	healthySeries := testutil.CollectAndCount(poolHealthy)

	d.sampleBackendHealth(context.Background(), pool)

	require.Nil(t, d.poolHealthSnapshot(), "a failed first sample must not fabricate a snapshot")
	assert.Equal(t, 1.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)),
		"the first failing sample of a process must publish stale=1: there is no previous snapshot, so the raw "+
			"scale_csi_pool_* series are absent and ScaleCSIPoolDegraded cannot fire - that blind window must not be silent")
	assert.Equal(t, 0.0, testutil.ToFloat64(poolHealthFlipPending.WithLabelValues(pool)),
		"there is no held transition at cold start, and an absent series is not the same operator signal as an explicit 0")

	assert.Equal(t, statusSeries, testutil.CollectAndCount(poolStatus),
		"cold start must not INVENT a pool status; an absent series is the truth")
	assert.Equal(t, healthySeries, testutil.CollectAndCount(poolHealthy),
		"cold start must not INVENT a pool health verdict")

	// The next successful sample takes it out of the cold-start window.
	mock.PoolQueryResultSet = false
	d.sampleBackendHealth(context.Background(), pool)
	assert.Equal(t, 0.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)))
	assert.NotNil(t, d.poolHealthSnapshot())
}

// TestPoolHealthStaleRaisedWhenTheTTLExpires pins the driver-local half of the
// poll-stall class: the TTL expires on the READ path, and the staleness verdict
// has to be published at that instant. Waiting for a hung call to return can
// take the whole backendHealthCallTimeout, and during that gap the served
// condition has already fallen back to dataset-only while a frozen DEGRADED
// gauge keeps alerting with BOTH diagnostic gauges reading 0.
//
// The poll here is REAL and genuinely blocked: the poller runs, its pool.query
// is held inside the mock, and the assertions are made while that call has not
// returned. Ageing a copied snapshot and calling the helper directly (which is
// what this used to do) proves the helper works, not that the driver publishes
// without waiting for the backend. The TTL floor is 90s, so time cannot be
// waited out in a unit test — the snapshot is aged, but it is aged UNDER a call
// that is still in flight.
func TestPoolHealthStaleRaisedWhenTheTTLExpires(t *testing.T) {
	const pool = "ttlgappool"
	mock := truenas.NewMockClient()
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusDegraded, SampledAt: time.Now()}
	d := healthTestDriver(mock)
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "30s"}

	d.sampleBackendHealth(context.Background(), pool)
	require.Equal(t, 0.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)))

	// Hold the NEXT pool.query inside the backend so a poll is genuinely in
	// flight for the rest of this test.
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	mock.PoolHealthEntered, mock.PoolHealthRelease = entered, release
	d.startBackendHealth()
	defer func() {
		close(release)
		d.stopBackendHealth()
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the poller never issued its pool.query")
	}

	// The cached snapshot ages past its TTL WHILE that call hangs, so the
	// failed-sample branch cannot run and nothing in the poll loop will publish
	// until the call returns.
	expired := *d.backendHealth.Load()
	expired.SampledAt = time.Now().Add(-d.backendHealthTTL() - time.Second)
	d.backendHealth.Store(&expired)

	require.Nil(t, d.poolHealthSnapshot(), "past the TTL the snapshot stops being served")
	assert.Equal(t, 1.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)),
		"the CSI read path already refuses to serve the snapshot, so stale must already be 1 — "+
			"with the backend call still unreturned")

	// The same gap is closed for a controller taking no CSI traffic at all: the
	// check the poll loop runs BEFORE each sample raises it without waiting for
	// the in-flight call.
	SetPoolHealthStale(pool, false)
	d.markPoolHealthStaleIfExpired()
	assert.Equal(t, 1.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)))
}

// TestMarkPoolHealthStaleYieldsToAFreshSample is the single-goroutine unit test
// for the supersede check: a staleness decision made against a snapshot that is
// no longer the published one must write nothing at all.
//
// It is NOT the race proof and cannot be — it fabricates the fresh pointer and
// never runs the two paths at once. TestBackendHealthStalePublicationSurvivesA
// ConcurrentSample is the concurrent regression test for the lost update.
func TestMarkPoolHealthStaleYieldsToAFreshSample(t *testing.T) {
	const pool = "raceypool"
	d := healthTestDriver(truenas.NewMockClient())
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "60s"}

	expired := &truenas.PoolHealthSnapshot{
		Pool: pool, Status: truenas.PoolStatusDegraded,
		SampledAt: time.Now().Add(-d.backendHealthTTL() - time.Second),
	}
	fresh := &truenas.PoolHealthSnapshot{
		Pool: pool, Status: truenas.PoolStatusOnline, Healthy: true, SampledAt: time.Now(),
	}
	d.backendHealth.Store(fresh)
	SetPoolHealthStale(pool, false)

	// The read path decided on the now-superseded snapshot.
	d.markPoolHealthStale(expired)

	assert.Equal(t, 0.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)),
		"a successful sample landed first; the read path must not re-raise staleness against it")
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
	// the ScaleCSIPoolDegraded alert's 5m hold, so a CONFIRMED degradation always
	// reaches conditions before the undamped gauge pages anyone. The ceiling
	// bounds the confirmation lag; it does not (and cannot) make the raw gauges
	// and the debounced condition agree at every instant — see
	// TestBackendHealthPollStallHoldsConditionAndFlagsStale and
	// TestBackendHealthRecoveryWindowIsAnIntentionalMismatch.
	d.config.BackendHealth = BackendHealthConfig{Enabled: true, Interval: "1h"}
	assert.Equal(t, 3*maxBackendHealthInterval, d.backendHealthTTL())
	interval, err := d.resolveBackendHealthInterval()
	require.NoError(t, err)
	assert.Equal(t, maxBackendHealthInterval, interval)
	assert.Less(t, 2*interval, 5*time.Minute,
		"a hysteresis-confirmed condition flip must land inside the alert hold docs/production.md documents")
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

// ---------------------------------------------------------------------------
// M6 round-3 — the HONEST timing contract between the raw gauges and the
// debounced VolumeCondition. The 2m interval ceiling bounds the confirmation
// lag; it does NOT make the two signals agree at every instant. The two tests
// below pin the windows in which they deliberately differ, plus the telemetry
// that makes each window visible. If anyone re-asserts "an alert and a PVC
// event can never disagree", these are what contradict it.
// ---------------------------------------------------------------------------

// TestBackendHealthPollStallHoldsConditionAndFlagsStale pins the poll-stall
// window: one DEGRADED sample followed by failing polls. The condition HOLDS its
// previous verdict (correct — a blip must not flip the fleet) while the raw
// gauge already reads degraded, so the two disagree for as long as the backend
// stays silent. That must not be silent telemetry: the pending flip is exported,
// and the first FAILED sample that finds an unconfirmed flip raises the
// staleness gauge immediately instead of waiting out the TTL (which is 6m at the
// interval ceiling — longer than the 5m alert hold).
func TestBackendHealthPollStallHoldsConditionAndFlagsStale(t *testing.T) {
	const pool = "gf5-m6-stall-pool"
	ctx := context.Background()
	mock := truenas.NewMockClient()
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusOnline, Healthy: true, SampledAt: time.Now()}
	d := healthTestDriver(mock)

	d.sampleBackendHealth(ctx, pool)
	require.False(t, d.volumeCondition(managedDataset()).GetAbnormal())
	require.Equal(t, 0.0, testutil.ToFloat64(poolHealthFlipPending.WithLabelValues(pool)))
	require.Equal(t, 0.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)))

	// One degraded sample: the RAW gauge flips at once, the condition is held.
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusDegraded, SampledAt: time.Now()}
	d.sampleBackendHealth(ctx, pool)
	assert.Equal(t, 1.0, testutil.ToFloat64(poolStatus.WithLabelValues(pool, truenas.PoolStatusDegraded)),
		"metrics are never damped")
	assert.False(t, d.volumeCondition(managedDataset()).GetAbnormal(),
		"one sample must not flip every managed PVC's condition")
	assert.Equal(t, 1.0, testutil.ToFloat64(poolHealthFlipPending.WithLabelValues(pool)),
		"the deliberate raw-vs-condition disagreement must be observable while it lasts")
	assert.Equal(t, 0.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)),
		"the sample itself was fresh; nothing is stale yet")

	// The confirming sample never arrives.
	mock.InjectHealthError = errors.New("appliance unreachable")
	d.sampleBackendHealth(ctx, pool)
	assert.False(t, d.volumeCondition(managedDataset()).GetAbnormal(),
		"a failed sample holds the previous verdict; it must not synthesize a flip")
	require.NotNil(t, d.poolHealthSnapshot(), "inside the TTL the held verdict is still served")
	assert.Equal(t, 1.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)),
		"a failed sample with a PENDING flip is stale immediately, without waiting out the TTL")
	assert.Equal(t, 1.0, testutil.ToFloat64(poolHealthFlipPending.WithLabelValues(pool)),
		"the flip stays pending while the backend is silent")

	// Past the TTL the held verdict stops driving conditions at all.
	held := *d.backendHealth.Load()
	held.SampledAt = time.Now().Add(-d.backendHealthTTL() - time.Second)
	d.backendHealth.Store(&held)
	d.sampleBackendHealth(ctx, pool)
	assert.Nil(t, d.poolHealthSnapshot(), "past the TTL the snapshot is not served")
	assert.Equal(t, volumeConditionFromDataset(managedDataset()), d.volumeCondition(managedDataset()),
		"past the TTL conditions fall back to the pre-GF5 dataset-only semantics")
	assert.Equal(t, 1.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)))

	// A successful sample clears both flags.
	mock.InjectHealthError = nil
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusOnline, Healthy: true, SampledAt: time.Now()}
	d.sampleBackendHealth(ctx, pool)
	assert.Equal(t, 0.0, testutil.ToFloat64(poolHealthStale.WithLabelValues(pool)))
	assert.Equal(t, 0.0, testutil.ToFloat64(poolHealthFlipPending.WithLabelValues(pool)))
	assert.False(t, d.volumeCondition(managedDataset()).GetAbnormal())
}

// TestBackendHealthRecoveryWindowIsAnIntentionalMismatch pins the second honest
// window. On the FIRST healthy sample the raw degraded series drops to 0 — so
// ScaleCSIPoolDegraded clears — while the condition stays Abnormal until the
// SECOND. No interval, ceiling or alert hold removes this: it is the damper
// doing its job, and it is why the "can never disagree" wording was wrong.
func TestBackendHealthRecoveryWindowIsAnIntentionalMismatch(t *testing.T) {
	const pool = "gf5-m6-recovery-pool"
	ctx := context.Background()
	mock := truenas.NewMockClient()
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusOnline, Healthy: true, SampledAt: time.Now()}
	d := healthTestDriver(mock)
	d.sampleBackendHealth(ctx, pool)

	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusDegraded, SampledAt: time.Now()}
	d.sampleBackendHealth(ctx, pool)
	d.sampleBackendHealth(ctx, pool)
	require.True(t, d.volumeCondition(managedDataset()).GetAbnormal(), "two samples confirm the degradation")
	require.Equal(t, 0.0, testutil.ToFloat64(poolHealthFlipPending.WithLabelValues(pool)))

	// First healthy sample: the alert's input clears, the condition does not.
	mock.PoolHealthValue = &truenas.PoolHealthSnapshot{Status: truenas.PoolStatusOnline, Healthy: true, SampledAt: time.Now()}
	d.sampleBackendHealth(ctx, pool)
	assert.Equal(t, 0.0, testutil.ToFloat64(poolStatus.WithLabelValues(pool, truenas.PoolStatusDegraded)),
		"the raw degraded series clears on the first healthy sample")
	assert.True(t, d.volumeCondition(managedDataset()).GetAbnormal(),
		"the condition deliberately lags the raw gauge by one sample on recovery")
	assert.Equal(t, 1.0, testutil.ToFloat64(poolHealthFlipPending.WithLabelValues(pool)),
		"the recovery mismatch window must be readable from Prometheus, not only from logs")

	// The second healthy sample closes the window.
	d.sampleBackendHealth(ctx, pool)
	assert.False(t, d.volumeCondition(managedDataset()).GetAbnormal())
	assert.Equal(t, 0.0, testutil.ToFloat64(poolHealthFlipPending.WithLabelValues(pool)))
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
