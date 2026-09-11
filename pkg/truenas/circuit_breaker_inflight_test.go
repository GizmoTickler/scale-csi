package truenas

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCircuitBreaker_HalfOpenEscapeWaitsForInFlightProbes is the regression for
// the spurious-open loop on a healthy-but-SLOW NAS.
//
// The escape timer keyed on lastStateChange and ignored whether a probe was
// still running. A probe whose round trip outlives the recovery Timeout — an
// entirely normal thing for a loaded appliance, where Timeout is 30s and a CSI
// call may be allowed 300s — was pre-empted: the next arriving caller reopened
// the circuit, the probe's SUCCESS then landed in the Open state where
// RecordSuccess is a deliberate no-op, and the whole cycle repeated. Measured
// against the live appliance: 10 opens, 0 failures, every probe successful.
func TestCircuitBreaker_HalfOpenEscapeWaitsForInFlightProbes(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    1,
		SuccessThreshold:    1,
		Timeout:             20 * time.Millisecond,
		HalfOpenMaxRequests: 1,
		// Generous, so this test is about in-flight awareness and not about the
		// leak watchdog.
		ProbeLeakGrace: time.Hour,
	})

	cb.RecordFailure()
	require.Equal(t, CircuitOpen, cb.State())
	time.Sleep(30 * time.Millisecond)

	admission := cb.admit()
	require.True(t, admission.allowed)
	require.True(t, admission.halfOpenProbe)
	require.Equal(t, CircuitHalfOpen, cb.State())
	require.False(t, cb.Allow(), "the single probe slot is spent while the probe runs")

	// The probe is still in flight, and has now been running longer than the
	// recovery Timeout.
	time.Sleep(30 * time.Millisecond)
	assert.False(t, cb.Allow(), "no probe slot is free")
	assert.Equal(t, CircuitHalfOpen, cb.State(),
		"a probe that is still running is a pending verdict, not a missing one; "+
			"reopening here throws away the answer the NAS is about to give")

	// The slow probe finally answers, and its verdict must still count.
	cb.RecordSuccess()
	assert.Equal(t, CircuitClosed, cb.State(),
		"a successful slow probe must close the circuit, not be discarded by a spurious reopen")

	stats := cb.Stats()
	assert.Equal(t, int64(1), stats.TotalCircuitOpens,
		"only the genuine failure may open the circuit; the escape must not add spurious opens")
	assert.Equal(t, int64(1), stats.TotalFailures)
}

// TestCircuitBreaker_HalfOpenStillEscapesAfterProbeLeakGrace is the counter-test.
// In-flight awareness must not resurrect the terminal half-open state: a probe
// that never reports an outcome at all (a goroutine wedged below the breaker, a
// caller that leaks the admission) still has to be given up on, or the breaker
// rejects every request forever with nothing outside the type able to clear it.
// The difference from the test above is duration, which is the only thing that
// distinguishes a slow probe from a lost one.
func TestCircuitBreaker_HalfOpenStillEscapesAfterProbeLeakGrace(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    1,
		SuccessThreshold:    1,
		Timeout:             20 * time.Millisecond,
		HalfOpenMaxRequests: 1,
		ProbeLeakGrace:      40 * time.Millisecond,
	})

	cb.RecordFailure()
	require.Equal(t, CircuitOpen, cb.State())
	time.Sleep(30 * time.Millisecond)

	require.True(t, cb.Allow())
	require.Equal(t, CircuitHalfOpen, cb.State())
	require.False(t, cb.Allow(), "the single probe slot is spent")

	// Still inside the leak grace: the probe gets the benefit of the doubt.
	assert.Equal(t, CircuitHalfOpen, cb.State())

	time.Sleep(50 * time.Millisecond)
	assert.False(t, cb.Allow(), "the escape reopens rather than admitting immediately")
	assert.Equal(t, CircuitOpen, cb.State(),
		"a probe that never reports must not strand the breaker in half-open forever")

	// And the normal recovery clock restarts, so probing resumes.
	time.Sleep(30 * time.Millisecond)
	assert.True(t, cb.Allow(), "a fresh probe must be admitted one Timeout after the escape")
	assert.Equal(t, CircuitHalfOpen, cb.State())
}

// TestClient_AmbiguousResultFromCallerCancellationIsNotANASFailure pins the
// invariant the RecordAbandoned work claimed to establish but did not reach.
//
// callRaw's ErrAmbiguousResult branch recorded a breaker FAILURE before it
// looked at ctx.Err(), so a CSI sidecar deadline or a controller shutdown —
// which say nothing whatsoever about the appliance — counted against the NAS
// and could open, or reopen, the circuit on a perfectly healthy appliance.
func TestClient_AmbiguousResultFromCallerCancellationIsNotANASFailure(t *testing.T) {
	var mutationCount int32
	mutationReceived := make(chan struct{})
	disconnect := make(chan struct{})

	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req.Method == "auth.login_with_api_key" || req.Method == "core.subscribe" {
				if err := conn.WriteJSON(rpcTestResponse{JSONRPC: "2.0", ID: req.ID, Result: true}); err != nil {
					return
				}
				continue
			}
			atomic.AddInt32(&mutationCount, 1)
			close(mutationReceived)
			<-disconnect
			_ = conn.UnderlyingConn().Close()
			return
		}
	})
	t.Cleanup(mock.close)

	host, port := testServerAddress(t, server.URL)
	client, err := NewClient(&ClientConfig{
		Host:                 host,
		Port:                 port,
		Protocol:             "http",
		APIKey:               "test-api-key",
		Timeout:              time.Second,
		ConnectTimeout:       time.Second,
		MaxConnections:       1,
		APIRetryMaxAttempts:  3,
		APIRetryInitialDelay: time.Millisecond,
		CircuitBreaker: &CircuitBreakerConfig{
			Enabled:             true,
			FailureThreshold:    1,
			SuccessThreshold:    1,
			Timeout:             time.Hour,
			HalfOpenMaxRequests: 1,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		// pool.dataset.create is NOT idempotent, so this takes the
		// ErrAmbiguousResult branch rather than being retried.
		_, callErr := client.Call(ctx, "pool.dataset.create", map[string]interface{}{"name": "tank/test"})
		resultCh <- callErr
	}()

	select {
	case <-mutationReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("mutation was not sent")
	}
	cancel()
	close(disconnect)

	select {
	case err = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("mutation call did not return")
	}
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAmbiguousResult, "the ambiguity itself must still be reported to the caller")
	require.ErrorIs(t, err, context.Canceled)

	stats := client.circuitBreaker.Stats()
	assert.Equal(t, int64(0), stats.TotalFailures,
		"a client-side cancellation is not evidence about the NAS and must not be recorded as a NAS failure")
	assert.Equal(t, 0, stats.Failures,
		"the consecutive-failure count must not advance on a caller walking away")
	assert.Equal(t, CircuitClosed, client.circuitBreaker.State(),
		"one canceled CSI RPC must not open the circuit on a healthy appliance")
}
