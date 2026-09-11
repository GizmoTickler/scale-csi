package truenas

import (
	"context"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newWedgedNASClient returns a client whose appliance is UP and answering but
// wedged: every call completes a full transport round trip and comes back as an
// application-level error. This is the shape of a NAS under ZFS lock contention
// (EBUSY) or one rejecting every payload with the -1 validation bucket — the
// most common real failure, and the one the circuit breaker was blind to.
func newWedgedNASClient(t *testing.T, breaker *CircuitBreakerConfig) *Client {
	t.Helper()
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			if req.Method == "auth.login_with_api_key" {
				resp.Result = true
			} else {
				resp.Error = &rpcError{Code: -1, Message: "[EBUSY] dataset is busy"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
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
		WriteTimeout:         time.Second,
		MaxConnections:       1,
		MaxRetries:           1,
		RetryInterval:        time.Millisecond,
		APIRetryMaxAttempts:  1,
		APIRetryInitialDelay: time.Millisecond,
		APIRetryMaxDelay:     time.Millisecond,
		CircuitBreaker:       breaker,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestClient_APIErrorInClosedStateDoesNotEraseTransportFailures is the
// regression for the blind spot that kept the breaker from EVER opening on a
// wedged-but-reachable NAS.
//
// callRaw recorded a breaker SUCCESS for every non-connection error. In the
// closed state RecordSuccess resets cb.failures to 0, so each API error wiped
// out the genuine transport failures interleaved with it and the consecutive
// count could never reach FailureThreshold. The RecordFailure calls below stand
// in for those interleaved transport failures; the Call in between is the real
// wedged-NAS reply traveling the real callRaw path.
func TestClient_APIErrorInClosedStateDoesNotEraseTransportFailures(t *testing.T) {
	client := newWedgedNASClient(t, &CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    3,
		SuccessThreshold:    1,
		Timeout:             time.Hour,
		HalfOpenMaxRequests: 1,
	})

	client.circuitBreaker.RecordFailure()
	client.circuitBreaker.RecordFailure()
	require.Equal(t, 2, client.circuitBreaker.Stats().Failures)

	_, err := client.Call(context.Background(), "pool.dataset.query")
	require.Error(t, err, "the wedged NAS answers every call with an application error")
	assert.Equal(t, 2, client.circuitBreaker.Stats().Failures,
		"an API-level error in the CLOSED state must not reset the consecutive failure count")

	client.circuitBreaker.RecordFailure()
	assert.Equal(t, CircuitOpen, client.circuitBreaker.State(),
		"the third consecutive transport failure must open the breaker even with API errors interleaved")
}

// TestClient_APIErrorStillClosesAHalfOpenProbe is the counter-test: the
// deliberate behavior the closed-state fix must NOT disturb. An API-level error
// still proves the transport round trip worked, so a benign "not found" reply
// must close a half-open circuit rather than reopen it.
func TestClient_APIErrorStillClosesAHalfOpenProbe(t *testing.T) {
	client := newWedgedNASClient(t, &CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    1,
		SuccessThreshold:    1,
		Timeout:             time.Millisecond,
		HalfOpenMaxRequests: 1,
	})

	client.circuitBreaker.RecordFailure()
	require.Equal(t, CircuitOpen, client.circuitBreaker.State())
	require.Eventually(t, func() bool {
		return time.Since(client.circuitBreaker.Stats().LastFailure) >= time.Millisecond
	}, time.Second, time.Millisecond)

	_, err := client.Call(context.Background(), "pool.dataset.query")
	require.Error(t, err)
	assert.Equal(t, CircuitClosed, client.circuitBreaker.State(),
		"an application error on a half-open probe proves the transport is healthy and must close the circuit")
}

// TestClient_CanceledHalfOpenProbeRecordsNoOutcome pins the symmetry fix. A
// client-side context cancellation says nothing about the NAS: in the closed
// state it has always recorded neither success nor failure, but a half-open
// probe fell through to callRaw's unrecorded-probe backstop and was recorded as
// a FAILURE, so a burst of CSI RPC deadlines could reopen a circuit whose
// appliance was perfectly healthy. It must also RELEASE the probe slot, or the
// abandoned measurement is never retaken.
func TestClient_CanceledHalfOpenProbeRecordsNoOutcome(t *testing.T) {
	client := newWedgedNASClient(t, &CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    1,
		SuccessThreshold:    1,
		Timeout:             time.Millisecond,
		HalfOpenMaxRequests: 1,
	})

	client.circuitBreaker.RecordFailure()
	require.Equal(t, CircuitOpen, client.circuitBreaker.State())
	require.Eventually(t, func() bool {
		return time.Since(client.circuitBreaker.Stats().LastFailure) >= time.Millisecond
	}, time.Second, time.Millisecond)

	// Fill the semaphore so the canceled call parks on the acquire, the earliest
	// client-side-cancellation exit below the admission.
	client.semaphore <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Call(ctx, "pool.dataset.query")
	<-client.semaphore
	require.ErrorIs(t, err, context.Canceled)

	stats := client.circuitBreaker.Stats()
	assert.Equal(t, CircuitHalfOpen, stats.State,
		"a caller walking away must not reopen a circuit on behalf of a NAS that was never asked")
	assert.Equal(t, int64(1), stats.TotalFailures, "the cancellation must not be counted as a NAS failure")

	// The released slot lets the next caller take the measurement this one
	// abandoned, rather than leaving the breaker stuck with no probes left.
	_, err = client.Call(context.Background(), "pool.dataset.query")
	require.Error(t, err)
	assert.Equal(t, CircuitClosed, client.circuitBreaker.State(),
		"the abandoned probe slot must be reusable by the next caller")
}

// TestCircuitBreaker_HalfOpenEscapesWhenProbesReturnNoVerdict pins the escape
// timer. With every probe slot consumed, ONLY a recorded outcome could move the
// state, so a probe that never reports one stranded the breaker in half-open
// forever — rejecting every request while the NAS may have recovered hours
// earlier, with nothing outside the type able to clear it.
//
// The clock for an OUTSTANDING probe is ProbeLeakGrace, not Timeout: Timeout is
// the recovery interval and is routinely shorter than one legitimate call, so
// using it here pre-empted healthy slow probes (see
// TestCircuitBreaker_HalfOpenEscapeWaitsForInFlightProbes). This test sets the
// two equal to keep pinning the leak escape without sleeping for the default
// grace.
func TestCircuitBreaker_HalfOpenEscapesWhenProbesReturnNoVerdict(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    1,
		SuccessThreshold:    1,
		Timeout:             20 * time.Millisecond,
		HalfOpenMaxRequests: 1,
		ProbeLeakGrace:      20 * time.Millisecond,
	})

	cb.RecordFailure()
	require.Equal(t, CircuitOpen, cb.State())
	time.Sleep(30 * time.Millisecond)

	// The probe is admitted and then never reports an outcome.
	require.True(t, cb.Allow())
	require.Equal(t, CircuitHalfOpen, cb.State())
	require.False(t, cb.Allow(), "the single probe slot is spent")

	time.Sleep(30 * time.Millisecond)
	assert.False(t, cb.Allow(), "the escape reopens rather than admitting immediately")
	assert.Equal(t, CircuitOpen, cb.State(),
		"half-open must not be terminal when no probe ever reports a verdict")

	// And the normal recovery clock restarts, so probing resumes.
	time.Sleep(30 * time.Millisecond)
	assert.True(t, cb.Allow(), "a fresh probe must be admitted one Timeout after the escape")
	assert.Equal(t, CircuitHalfOpen, cb.State())
}
