package driver

import (
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// resetCBMetricsState resets the circuit breaker metrics tracking state
// so tests don't affect each other. This should be called at the start of each test.
func resetCBMetricsState() {
	cbMetricsMu.Lock()
	defer cbMetricsMu.Unlock()
	lastCBTotalFailures = 0
	lastCBTotalSuccesses = 0
	lastCBTotalCircuitOpens = 0
}

func TestUpdateCircuitBreakerMetrics_NilStats(t *testing.T) {
	// Test Case: Nil stats should not panic
	resetCBMetricsState()

	// This should not panic or cause any issues
	UpdateCircuitBreakerMetrics(nil)

	// Verify the function returned without error (no assertions needed - no panic is success)
}

func TestUpdateCircuitBreakerMetrics_InitialUpdate(t *testing.T) {
	// Test Case: First update with initial stats
	resetCBMetricsState()

	stats := &truenas.CircuitBreakerStats{
		State:             truenas.CircuitClosed,
		Failures:          2,
		TotalFailures:     10,
		TotalSuccesses:    100,
		TotalCircuitOpens: 3,
	}

	UpdateCircuitBreakerMetrics(stats)

	// Verify internal tracking was updated
	cbMetricsMu.Lock()
	defer cbMetricsMu.Unlock()
	assert.Equal(t, int64(10), lastCBTotalFailures)
	assert.Equal(t, int64(100), lastCBTotalSuccesses)
	assert.Equal(t, int64(3), lastCBTotalCircuitOpens)
}

func TestUpdateCircuitBreakerMetrics_DeltaCalculation(t *testing.T) {
	// Test Case: Verify delta calculation for counters
	resetCBMetricsState()

	// First update
	stats1 := &truenas.CircuitBreakerStats{
		State:             truenas.CircuitClosed,
		Failures:          0,
		TotalFailures:     5,
		TotalSuccesses:    10,
		TotalCircuitOpens: 1,
	}
	UpdateCircuitBreakerMetrics(stats1)

	// Second update with increased counts
	stats2 := &truenas.CircuitBreakerStats{
		State:             truenas.CircuitOpen,
		Failures:          3,
		TotalFailures:     8,  // +3 from previous
		TotalSuccesses:    15, // +5 from previous
		TotalCircuitOpens: 2,  // +1 from previous
	}
	UpdateCircuitBreakerMetrics(stats2)

	// Verify internal tracking reflects the latest totals
	cbMetricsMu.Lock()
	defer cbMetricsMu.Unlock()
	assert.Equal(t, int64(8), lastCBTotalFailures)
	assert.Equal(t, int64(15), lastCBTotalSuccesses)
	assert.Equal(t, int64(2), lastCBTotalCircuitOpens)
}

func TestUpdateCircuitBreakerMetrics_NoDecrease(t *testing.T) {
	// Test Case: Counters should not decrease (only add positive delta)
	resetCBMetricsState()

	// First update with higher values
	stats1 := &truenas.CircuitBreakerStats{
		State:             truenas.CircuitClosed,
		Failures:          0,
		TotalFailures:     10,
		TotalSuccesses:    20,
		TotalCircuitOpens: 5,
	}
	UpdateCircuitBreakerMetrics(stats1)

	// Second update with LOWER values (this shouldn't happen in practice,
	// but the function should handle it gracefully by not adding negative delta)
	stats2 := &truenas.CircuitBreakerStats{
		State:             truenas.CircuitClosed,
		Failures:          0,
		TotalFailures:     5,  // Lower than before
		TotalSuccesses:    10, // Lower than before
		TotalCircuitOpens: 2,  // Lower than before
	}
	UpdateCircuitBreakerMetrics(stats2)

	// Verify tracking still reflects the higher values (no negative delta was applied)
	cbMetricsMu.Lock()
	defer cbMetricsMu.Unlock()
	assert.Equal(t, int64(10), lastCBTotalFailures, "lastCBTotalFailures should not decrease")
	assert.Equal(t, int64(20), lastCBTotalSuccesses, "lastCBTotalSuccesses should not decrease")
	assert.Equal(t, int64(5), lastCBTotalCircuitOpens, "lastCBTotalCircuitOpens should not decrease")
}

func TestUpdateCircuitBreakerMetrics_AllStates(t *testing.T) {
	// Test Case: Verify all circuit breaker states are handled
	testCases := []struct {
		name  string
		state truenas.CircuitState
	}{
		{"CircuitClosed", truenas.CircuitClosed},
		{"CircuitOpen", truenas.CircuitOpen},
		{"CircuitHalfOpen", truenas.CircuitHalfOpen},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resetCBMetricsState()

			stats := &truenas.CircuitBreakerStats{
				State:             tc.state,
				Failures:          1,
				TotalFailures:     1,
				TotalSuccesses:    1,
				TotalCircuitOpens: 1,
			}

			// Should not panic for any state
			UpdateCircuitBreakerMetrics(stats)

			// Verify update occurred
			cbMetricsMu.Lock()
			assert.Equal(t, int64(1), lastCBTotalFailures)
			cbMetricsMu.Unlock()
		})
	}
}

func TestUpdateCircuitBreakerMetrics_Concurrent(t *testing.T) {
	// Test Case: Concurrent updates should be thread-safe
	resetCBMetricsState()

	const numGoroutines = 10
	const updatesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < updatesPerGoroutine; j++ {
				stats := &truenas.CircuitBreakerStats{
					State:             truenas.CircuitState(id % 3),
					Failures:          j,
					TotalFailures:     int64((id * updatesPerGoroutine) + j),
					TotalSuccesses:    int64((id * updatesPerGoroutine) + j),
					TotalCircuitOpens: int64(id),
				}
				UpdateCircuitBreakerMetrics(stats)
			}
		}(i)
	}

	wg.Wait()

	// If we get here without panics or data races, the test passes
	// The exact values are non-deterministic due to concurrency
}

func TestUpdateCircuitBreakerMetrics_ZeroValues(t *testing.T) {
	// Test Case: Zero values in stats should be handled
	resetCBMetricsState()

	stats := &truenas.CircuitBreakerStats{
		State:             truenas.CircuitClosed,
		Failures:          0,
		TotalFailures:     0,
		TotalSuccesses:    0,
		TotalCircuitOpens: 0,
	}

	UpdateCircuitBreakerMetrics(stats)

	// Verify zero values are set correctly
	cbMetricsMu.Lock()
	defer cbMetricsMu.Unlock()
	assert.Equal(t, int64(0), lastCBTotalFailures)
	assert.Equal(t, int64(0), lastCBTotalSuccesses)
	assert.Equal(t, int64(0), lastCBTotalCircuitOpens)
}

func TestRecordCSIOperationClassifiesAbortedAsBenign(t *testing.T) {
	const operation = "/csi.v1.Controller/CreateVolume-observability-test"
	benign := csiOperationsTotal.WithLabelValues(operation, "benign", codes.Aborted.String())
	hardError := csiOperationsTotal.WithLabelValues(operation, "error", codes.Aborted.String())
	benignBefore := testutil.ToFloat64(benign)
	hardErrorBefore := testutil.ToFloat64(hardError)

	RecordCSIOperation(operation, 0.01, status.Error(codes.Aborted, "operation already in progress"))

	assert.Equal(t, benignBefore+1, testutil.ToFloat64(benign))
	assert.Equal(t, hardErrorBefore, testutil.ToFloat64(hardError))
}

// TestRecordTrueNASRequestClassifiesBenign guards the E1 5-value transport
// status taxonomy: benign idempotent outcomes and lock contention move OUT of
// status="error" into benign_* so the per-method counter tells the truth.
func TestRecordTrueNASRequestClassifiesBenign(t *testing.T) {
	for _, tc := range []struct {
		name       string
		method     string
		err        error
		wantStatus string
	}{
		{name: "nil error is success", method: "pool.dataset.create", err: nil, wantStatus: "success"},
		{
			name:       "already-exists is benign_exists",
			method:     "pool.dataset.create",
			err:        &truenas.APIError{Code: int(syscall.EEXIST), Message: "dataset already exists"},
			wantStatus: "benign_exists",
		},
		{
			name:       "not-found is benign_notfound",
			method:     "pool.dataset.delete",
			err:        &truenas.APIError{Code: int(syscall.ENOENT), Message: "dataset not found"},
			wantStatus: "benign_notfound",
		},
		{
			name:       "lock contention is benign_aborted",
			method:     "pool.dataset.create",
			err:        &truenas.APIError{Code: int(syscall.EBUSY), Message: "call already in progress"},
			wantStatus: "benign_aborted",
		},
		{
			name:       "generic failure is error",
			method:     "pool.dataset.create",
			err:        &truenas.APIError{Code: int(syscall.EACCES), Message: "permission denied"},
			wantStatus: "error",
		},
		{
			// The classifier is method-agnostic: the CHAP peer-CRUD calls
			// (iscsi.auth.*) added in sprint2 are classified benign_exists on
			// their idempotent enforcement-boundary creates with no CHAP-specific
			// code, exactly as the E1 design predicted.
			name:       "iscsi.auth create already-exists is benign_exists",
			method:     "iscsi.auth.create",
			err:        &truenas.APIError{Code: int(syscall.EEXIST), Message: "peer user already exists"},
			wantStatus: "benign_exists",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := truenasRequestsTotal.WithLabelValues(tc.method, tc.wantStatus)
			wantBefore := testutil.ToFloat64(want)
			errSeries := truenasRequestsTotal.WithLabelValues(tc.method, "error")
			errBefore := testutil.ToFloat64(errSeries)

			RecordTrueNASRequest(tc.method, 0.01, tc.err)

			assert.Equal(t, wantBefore+1, testutil.ToFloat64(want))
			if tc.wantStatus != "error" {
				assert.Equal(t, errBefore, testutil.ToFloat64(errSeries),
					"a benign outcome must not increment the error series")
			}
		})
	}
}

func TestTransportCountersIncrementThroughMetricsRegistry(t *testing.T) {
	for _, tc := range []struct {
		transport string
		result    string
	}{
		{transport: "iscsi", result: "success"},
		{transport: "nvmeof", result: "error"},
		{transport: "nfs", result: "success"},
	} {
		counter := nodeConnectTotal.WithLabelValues(tc.transport, tc.result)
		before := testutil.ToFloat64(counter)
		RecordNodeConnect(tc.transport, tc.result)
		assert.Equal(t, before+1, testutil.ToFloat64(counter))
	}

	for _, transport := range []string{"iscsi", "nvmeof"} {
		counter := gcSessionsDisconnectedTotal.WithLabelValues(transport)
		before := testutil.ToFloat64(counter)
		RecordGCSessionDisconnected(transport)
		assert.Equal(t, before+1, testutil.ToFloat64(counter))
	}

	pathCounter := nvmePathConnectTotal.WithLabelValues("192.0.2.80", "error")
	pathBefore := testutil.ToFloat64(pathCounter)
	RecordNVMePathConnect("192.0.2.80", "error")
	assert.Equal(t, pathBefore+1, testutil.ToFloat64(pathCounter))
}

func TestSetOrphanReconcileMetrics(t *testing.T) {
	SetOrphanReconcileMetrics(ReconcileReport{
		OrphanVolumeCount:            2,
		OrphanSnapshotCount:          3,
		SpentRestoreSnapshotCount:    4,
		OrphanVolumeBytes:            1024,
		OrphanSnapshotBytes:          2048,
		ManualRecoveryTombstoneCount: 7,
	})

	assert.Equal(t, float64(2), testutil.ToFloat64(orphanVolumes))
	assert.Equal(t, float64(3), testutil.ToFloat64(orphanSnapshots))
	assert.Equal(t, float64(4), testutil.ToFloat64(spentRestoreSnapshots))
	assert.Equal(t, float64(1024), testutil.ToFloat64(orphanVolumesBytes))
	assert.Equal(t, float64(2048), testutil.ToFloat64(orphanSnapshotsBytes))
	// O5: the manual-recovery tombstone count was previously computed but dropped
	// on the floor; it must now publish to its gauge.
	assert.Equal(t, float64(7), testutil.ToFloat64(manualRecoveryTombstones))
}

// TestRecordTombstoneReaped guards the O6 reap-throughput counter: the path
// label is a fixed 2-value enum (ledger / scan_fallback) and each reap
// increments exactly its own series.
func TestRecordTombstoneReaped(t *testing.T) {
	ledger := tombstoneReapedTotal.WithLabelValues(tombstoneReapedPathLedger)
	scanFallback := tombstoneReapedTotal.WithLabelValues(tombstoneReapedPathScanFallback)
	ledgerBefore := testutil.ToFloat64(ledger)
	scanFallbackBefore := testutil.ToFloat64(scanFallback)

	RecordTombstoneReaped(tombstoneReapedPathLedger)
	RecordTombstoneReaped(tombstoneReapedPathScanFallback)
	RecordTombstoneReaped(tombstoneReapedPathScanFallback)

	assert.Equal(t, ledgerBefore+1, testutil.ToFloat64(ledger))
	assert.Equal(t, scanFallbackBefore+2, testutil.ToFloat64(scanFallback))
}

func TestSetTrueNASPendingCalls(t *testing.T) {
	SetTrueNASPendingCalls(7)
	assert.Equal(t, float64(7), testutil.ToFloat64(truenasPendingCalls))

	SetTrueNASPendingCalls(0)
	assert.Equal(t, float64(0), testutil.ToFloat64(truenasPendingCalls))
}

func TestSetJobDispatcherSubscribed(t *testing.T) {
	SetJobDispatcherSubscribed(true)
	assert.Equal(t, float64(1), testutil.ToFloat64(jobDispatcherSubscribed.WithLabelValues()))

	SetJobDispatcherSubscribed(false)
	assert.Equal(t, float64(0), testutil.ToFloat64(jobDispatcherSubscribed.WithLabelValues()))
}

// TestJobDispatcherSubscribedNodeModeExport guards the codex H1 fix: a
// node-mode process builds no TrueNAS client and never calls
// SetJobDispatcherSubscribed, so the label-less gauge vec is never touched and
// must export NO scale_csi_job_dispatcher_subscribed series. Before the fix the
// metric was a plain gauge that every process exported as 0, which forced an
// unscoped min()/==0 to read 0 (PURE-POLL / alert firing) in any normal
// controller-plus-node install. This mirrors the controller-only pool_*_bytes
// GaugeVecs, which likewise export nothing until the controller loop touches
// them.
func TestJobDispatcherSubscribedNodeModeExport(t *testing.T) {
	const name = "scale_csi_job_dispatcher_subscribed"

	// Model node mode: drop any child an earlier test created so the vec is
	// untouched, exactly as it is in a process that never runs the controller
	// health tick.
	jobDispatcherSubscribed.Reset()
	assert.False(t, gatherHasMetric(t, name),
		"an untouched (node-mode) gauge vec must not export %s", name)

	// Model controller mode: the health tick touches the gauge, so the series
	// appears.
	SetJobDispatcherSubscribed(true)
	assert.True(t, gatherHasMetric(t, name),
		"a touched (controller-mode) gauge vec must export %s", name)
}

// gatherHasMetric reports whether the default Prometheus gatherer currently
// exposes any series for the named metric family.
func gatherHasMetric(t *testing.T, name string) bool {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() == name && len(mf.GetMetric()) > 0 {
			return true
		}
	}
	return false
}

// TestMetricNamesIsComplete guards the O11 single-source-of-truth invariant:
// MetricNames() must list every registered scale_csi metric (no manual list to
// drift) and every name must carry the scale_csi_ prefix. The chart drift test
// relies on this set being exactly the registered set.
func TestMetricNamesIsComplete(t *testing.T) {
	names := MetricNames()
	set := make(map[string]bool, len(names))
	for _, name := range names {
		assert.True(t, strings.HasPrefix(name, "scale_csi_"), "metric name %q missing the scale_csi_ prefix", name)
		assert.False(t, set[name], "metric name %q registered more than once", name)
		set[name] = true
	}

	// A representative cross-section of metrics across every reg* helper must be
	// present, proving registration populates the list automatically.
	for _, want := range []string{
		"scale_csi_operations_total",
		"scale_csi_operations_duration_seconds",
		"scale_csi_truenas_requests_total",
		"scale_csi_truenas_connection_status",
		"scale_csi_pool_available_bytes",
		"scale_csi_pool_capacity_bytes",
		"scale_csi_fencing_stale_deferred_total",
		"scale_csi_manual_recovery_tombstones",
		"scale_csi_tombstone_reaped_total",
		"scale_csi_tombstone_oldest_age_seconds",
		"scale_csi_tombstone_reap_last_success_timestamp_seconds",
		"scale_csi_tombstone_reap_last_reaped",
		"scale_csi_tombstone_reap_last_skipped_on_cap",
		"scale_csi_tombstone_reap_last_skipped_refused",
		"scale_csi_reconcile_delete_enabled",
		"scale_csi_job_dispatcher_subscribed",
	} {
		assert.True(t, set[want], "MetricNames() missing %q", want)
	}

	// The returned slice is a copy: mutating it must not corrupt the source.
	names[0] = "tampered"
	assert.NotEqual(t, "tampered", MetricNames()[0])
}

func TestReconcileAndFencingMetrics(t *testing.T) {
	completedAt := time.Unix(1_800_000_000, 0)
	RecordReconcileSuccess(completedAt)
	assert.Equal(t, float64(completedAt.Unix()), testutil.ToFloat64(reconcileLastSuccessTimestamp))

	failure := reconcileFailuresTotal.WithLabelValues("test_phase")
	failureBefore := testutil.ToFloat64(failure)
	RecordReconcileFailure("test_phase")
	assert.Equal(t, failureBefore+1, testutil.ToFloat64(failure))

	deferred := fencingDeferredTotal.WithLabelValues("missing_identity", "iscsi")
	deferredBefore := testutil.ToFloat64(deferred)
	RecordFencingDeferred("missing_identity", "iscsi")
	assert.Equal(t, deferredBefore+1, testutil.ToFloat64(deferred))

	staleBefore := testutil.ToFloat64(fencingStaleDeferredTotal)
	RecordFencingStaleDeferred()
	assert.Equal(t, staleBefore+1, testutil.ToFloat64(fencingStaleDeferredTotal))

	takeover := fencingTakeoverTotal.WithLabelValues(fencingTakeoverReasonStaleRecord)
	takeoverBefore := testutil.ToFloat64(takeover)
	RecordFencingTakeover(fencingTakeoverReasonStaleRecord)
	assert.Equal(t, takeoverBefore+1, testutil.ToFloat64(takeover))

	replicationAborts := replicationJobsAbortedTotal.WithLabelValues("missing_marker")
	replicationAbortsBefore := testutil.ToFloat64(replicationAborts)
	RecordReplicationJobAborted("missing_marker")
	assert.Equal(t, replicationAbortsBefore+1, testutil.ToFloat64(replicationAborts))
}
