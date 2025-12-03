package truenas

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker_ClosedState(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    3,
		SuccessThreshold:    2,
		Timeout:             100 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	})

	// Should allow requests in closed state
	assert.True(t, cb.Allow())
	assert.Equal(t, CircuitClosed, cb.State())

	// Record success - should stay closed
	cb.RecordSuccess()
	assert.Equal(t, CircuitClosed, cb.State())
}

func TestCircuitBreaker_OpensAfterFailures(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    3,
		SuccessThreshold:    2,
		Timeout:             100 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	})

	// Record failures below threshold
	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, CircuitClosed, cb.State())

	// Third failure should open the circuit
	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())

	// Should not allow requests in open state
	assert.False(t, cb.Allow())
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		SuccessThreshold:    2,
		Timeout:             50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Allow should transition to half-open and allow request
	assert.True(t, cb.Allow())
	assert.Equal(t, CircuitHalfOpen, cb.State())
}

func TestCircuitBreaker_ClosesFromHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		SuccessThreshold:    2,
		Timeout:             50 * time.Millisecond,
		HalfOpenMaxRequests: 3,
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait and transition to half-open
	time.Sleep(60 * time.Millisecond)
	cb.Allow()
	assert.Equal(t, CircuitHalfOpen, cb.State())

	// Record successes to close circuit
	cb.RecordSuccess()
	assert.Equal(t, CircuitHalfOpen, cb.State()) // Still half-open
	cb.RecordSuccess()
	assert.Equal(t, CircuitClosed, cb.State()) // Now closed
}

func TestCircuitBreaker_ReopensFromHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		SuccessThreshold:    2,
		Timeout:             50 * time.Millisecond,
		HalfOpenMaxRequests: 3,
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait and transition to half-open
	time.Sleep(60 * time.Millisecond)
	cb.Allow()
	assert.Equal(t, CircuitHalfOpen, cb.State())

	// Any failure in half-open should reopen
	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())
}

func TestCircuitBreaker_HalfOpenRequestLimit(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		SuccessThreshold:    2,
		Timeout:             50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait and transition to half-open
	time.Sleep(60 * time.Millisecond)

	// First request should be allowed (and transitions to half-open)
	assert.True(t, cb.Allow())
	assert.Equal(t, CircuitHalfOpen, cb.State())

	// Second request allowed (within limit)
	assert.True(t, cb.Allow())

	// Third request should be blocked (exceeds limit)
	assert.False(t, cb.Allow())
}

func TestCircuitBreaker_Disabled(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:          false,
		FailureThreshold: 1,
	})

	// Should always allow when disabled
	assert.True(t, cb.Allow())

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()

	// Should still allow after failures when disabled
	assert.True(t, cb.Allow())
	assert.Equal(t, CircuitClosed, cb.State())
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:          true,
		FailureThreshold: 2,
		Timeout:          1 * time.Hour, // Long timeout
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())

	// Reset should close it
	cb.Reset()
	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())
}

func TestCircuitBreaker_Stats(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:          true,
		FailureThreshold: 3,
	})

	cb.RecordSuccess()
	cb.RecordSuccess()
	cb.RecordFailure()

	stats := cb.Stats()
	assert.Equal(t, int64(2), stats.TotalSuccesses)
	assert.Equal(t, int64(1), stats.TotalFailures)
	assert.Equal(t, CircuitClosed, stats.State)
}

func TestCircuitBreaker_SuccessResetsFailureCount(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:          true,
		FailureThreshold: 3,
	})

	// Two failures
	cb.RecordFailure()
	cb.RecordFailure()

	// Success should reset the count
	cb.RecordSuccess()

	// Two more failures shouldn't open (count was reset)
	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, CircuitClosed, cb.State())

	// Third consecutive failure opens it
	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())
}
