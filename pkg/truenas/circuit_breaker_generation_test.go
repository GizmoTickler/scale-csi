package truenas

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCircuitBreaker_LateFailureDoesNotRestartTheRecoveryClock is the
// regression for the recovery-starvation path.
//
// lastFailure is the Open -> half-open recovery clock: admit() issues the next
// probe one Timeout after it. RecordFailure stamped it BEFORE looking at the
// state, including in the Open state, where the code itself documents that a
// failure "would be spurious" because calls are rejected before they run.
//
// The failures that land there are real and routine: an outage makes every
// in-flight call fail LATE, so the breaker opens on the first one and then
// collects the stragglers. Each straggler pushed recovery a full Timeout further
// away. A trickle arriving faster than Timeout holds the breaker Open forever —
// no probe is ever issued, so nothing can ever prove the appliance recovered,
// and nothing outside the type can clear it.
func TestCircuitBreaker_LateFailureDoesNotRestartTheRecoveryClock(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    1,
		SuccessThreshold:    1,
		Timeout:             100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
		ProbeLeakGrace:      time.Hour,
	})

	cb.RecordFailure()
	require.Equal(t, CircuitOpen, cb.State())

	// A straggler admitted before the open finally fails, 70ms into the 100ms
	// recovery interval.
	time.Sleep(70 * time.Millisecond)
	cb.RecordFailure()

	// The recovery interval has now elapsed, measured from the failure that
	// actually opened the circuit.
	time.Sleep(50 * time.Millisecond)
	assert.True(t, cb.Allow(),
		"a failure recorded while the circuit is already Open must not restart the "+
			"recovery clock; stragglers from the outage that opened it would otherwise "+
			"starve the breaker of probes indefinitely")
	assert.Equal(t, CircuitHalfOpen, cb.State())

	assert.Equal(t, int64(2), cb.Stats().TotalFailures,
		"the straggler is still counted in the metrics; it just must not push recovery away")
}

// TestClient_StaleProbeOutcomeCannotMoveALaterGeneration is the regression for
// cross-generation probe accounting, driven through the real callRaw pipeline.
//
// Probe outcomes carried no identity, so one issued in half-open generation N
// was applied to whatever state happened to be live when it finally answered:
//
//   - its success could satisfy generation N+1's SuccessThreshold and CLOSE a
//     circuit on evidence gathered before the breaker gave up and reopened, and
//   - its resolution consumed generation N+1's probeAdmissions entry, so the
//     genuinely in-flight probe of the new generation became invisible to the
//     escape timer. halfOpenEscapeDue then took its "nothing outstanding" branch
//     and reopened after a mere Timeout instead of a ProbeLeakGrace — the exact
//     spurious-open loop that the in-flight accounting was added to stop.
//
// The sequence below is the one a healthy-but-slow appliance produces: a probe
// that outlives the leak grace, the escape that gives up on it, a new generation
// with its own probe, and then the original answer arriving.
func TestClient_StaleProbeOutcomeCannotMoveALaterGeneration(t *testing.T) {
	const (
		breakerTimeout = 60 * time.Millisecond
		leakGrace      = 300 * time.Millisecond
	)

	var once sync.Once
	probeReceived := make(chan struct{})
	releaseProbe := make(chan struct{})

	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "pool.dataset.query":
				// The slow probe: received immediately, answered only when the
				// test says so.
				once.Do(func() { close(probeReceived) })
				<-releaseProbe
				resp.Result = []interface{}{}
			default:
				// auth.login_with_api_key, core.subscribe, the warm-up ping.
				resp.Result = true
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
		Timeout:              10 * time.Second,
		ConnectTimeout:       time.Second,
		WriteTimeout:         time.Second,
		HeartbeatInterval:    time.Hour,
		MaxConnections:       1,
		APIRetryMaxAttempts:  1,
		APIRetryInitialDelay: time.Millisecond,
		CircuitBreaker: &CircuitBreakerConfig{
			Enabled:             true,
			FailureThreshold:    1,
			SuccessThreshold:    1,
			Timeout:             breakerTimeout,
			HalfOpenMaxRequests: 1,
			ProbeLeakGrace:      leakGrace,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// Establish the connection while the circuit is closed, so the probe call
	// below is not also paying for the login round trip.
	_, err = client.Call(context.Background(), "core.ping")
	require.NoError(t, err)

	client.circuitBreaker.RecordFailure()
	require.Equal(t, CircuitOpen, client.circuitBreaker.State())
	time.Sleep(breakerTimeout + 20*time.Millisecond)

	// Generation N: one probe, admitted through the real pipeline, which the
	// appliance will not answer until the test releases it.
	probeErr := make(chan error, 1)
	go func() {
		_, callErr := client.Call(context.Background(), "pool.dataset.query")
		probeErr <- callErr
	}()
	select {
	case <-probeReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("the probe never reached the appliance")
	}
	require.Equal(t, CircuitHalfOpen, client.circuitBreaker.State())

	// The probe outlives the leak grace, so the breaker gives up on it.
	time.Sleep(leakGrace + 100*time.Millisecond)
	require.False(t, client.circuitBreaker.Allow())
	require.Equal(t, CircuitOpen, client.circuitBreaker.State(),
		"a probe that has outlived ProbeLeakGrace is presumed lost")

	// Generation N+1: a fresh probe is admitted and is genuinely outstanding.
	time.Sleep(breakerTimeout + 20*time.Millisecond)
	require.True(t, client.circuitBreaker.Allow())
	require.Equal(t, CircuitHalfOpen, client.circuitBreaker.State())

	// The generation-N probe finally answers, successfully.
	close(releaseProbe)
	require.NoError(t, <-probeErr)

	assert.Equal(t, CircuitHalfOpen, client.circuitBreaker.State(),
		"a probe from a generation the breaker already abandoned must not close the "+
			"circuit that replaced it; the new generation has proved nothing yet")
	assert.False(t, client.circuitBreaker.Allow(),
		"and the new generation's own probe must still be outstanding: its admission "+
			"record must not have been retired by someone else's verdict")
}
