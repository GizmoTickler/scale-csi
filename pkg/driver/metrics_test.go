package driver

import (
	"sync"
	"testing"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"github.com/stretchr/testify/assert"
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
