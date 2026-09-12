// Package truenas provides a circuit breaker for TrueNAS API calls.
package truenas

import (
	"errors"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	// CircuitClosed allows requests to pass through
	CircuitClosed CircuitState = iota
	// CircuitOpen blocks all requests
	CircuitOpen
	// CircuitHalfOpen allows limited requests to test recovery
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// CircuitBreakerConfig holds configuration for the circuit breaker.
type CircuitBreakerConfig struct {
	// Enabled enables the circuit breaker
	Enabled bool

	// FailureThreshold is the number of consecutive failures before opening
	FailureThreshold int

	// SuccessThreshold is the number of consecutive successes to close from half-open
	SuccessThreshold int

	// Timeout is how long the circuit stays open before transitioning to half-open
	Timeout time.Duration

	// HalfOpenMaxRequests is max requests allowed in half-open state
	HalfOpenMaxRequests int

	// ProbeLeakGrace bounds how long a half-open probe may stay OUTSTANDING —
	// admitted, with no outcome recorded — before the breaker presumes it was
	// lost and escapes half-open without it.
	//
	// It is deliberately NOT Timeout. Timeout is the recovery interval: how long
	// the breaker waits before it is willing to test the appliance again. It says
	// nothing about how long one probe may legitimately take, and a busy NAS
	// routinely answers a single call more slowly than that (30s default here
	// against a 300s external-provisioner budget). Using Timeout as the leak
	// watchdog pre-empted healthy slow probes and produced a spurious-open loop.
	// Defaults to defaultProbeLeakGraceMultiplier * Timeout.
	ProbeLeakGrace time.Duration
}

// defaultProbeLeakGraceMultiplier sets ProbeLeakGrace an order of magnitude
// beyond the recovery interval when the caller does not choose one: 10 minutes
// at the 30s default, comfortably past the 300s external-provisioner deadline
// that bounds the longest legitimate CSI call, so only a genuinely lost probe
// trips it.
const defaultProbeLeakGraceMultiplier = 20

// DefaultCircuitBreakerConfig returns sensible defaults for the circuit breaker.
func DefaultCircuitBreakerConfig() *CircuitBreakerConfig {
	return &CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    5,
		SuccessThreshold:    2,
		Timeout:             30 * time.Second,
		HalfOpenMaxRequests: 3,
	}
}

// CircuitBreaker implements the circuit breaker pattern for API calls.
type CircuitBreaker struct {
	config *CircuitBreakerConfig

	mu               sync.RWMutex
	state            CircuitState
	failures         int
	successes        int
	lastFailure      time.Time
	halfOpenRequests int
	lastStateChange  time.Time

	// generation counts state transitions. Every admission is stamped with the
	// generation that was live when it was granted, and an outcome may only move
	// the breaker if that stamp still matches. Without it a probe issued in one
	// half-open generation was credited or debited against a LATER one: its
	// success could close a circuit that had already given up on it, and its
	// resolution consumed the live generation's probeAdmissions entry, leaving a
	// genuinely in-flight probe invisible to the escape timer and so reopening
	// the circuit one Timeout later instead of one ProbeLeakGrace later — the
	// exact spurious-open loop the in-flight accounting exists to prevent.
	// Starts at 1 so currentGeneration (0) can mean "not stamped".
	generation uint64
	// nextProbeID hands each admitted probe an identity within its generation.
	nextProbeID uint64

	// probeAdmissions holds every half-open probe of the CURRENT generation that
	// has NOT yet reported an outcome, in admission order. Its length is the
	// number of probes currently in flight; halfOpenRequests, by contrast, counts
	// slots CONSUMED and is never given back by a verdict. The escape timer needs
	// the former: a probe that is still running is a pending verdict, not a
	// missing one. Bounded by HalfOpenMaxRequests.
	probeAdmissions []probeAdmission
	// lastProbeActivity is when a probe was last admitted or last reported an
	// outcome. The escape timer runs from this rather than from lastStateChange,
	// which starts ticking the moment half-open begins and so can expire while a
	// probe admitted seconds later is still on the wire.
	lastProbeActivity time.Time

	totalFailures     int64 // for metrics
	totalSuccesses    int64 // for metrics
	totalCircuitOpens int64 // for metrics
}

type circuitBreakerAdmission struct {
	allowed       bool
	halfOpenProbe bool
	// generation is the breaker generation this call was admitted into. The
	// outcome it eventually reports is only allowed to move that generation.
	generation uint64
	// probeID identifies this probe among the outstanding probes of its
	// generation, so a verdict retires ITS OWN admission record rather than
	// whichever happens to be oldest.
	probeID uint64
}

// probeAdmission is one outstanding half-open probe.
type probeAdmission struct {
	id uint64
	at time.Time
}

// currentGeneration stamps an outcome that is not tied to any particular
// generation, which is how the exported Record* entry points behave: they
// always apply to whatever state is live now. The client pipeline stamps its
// real generation instead.
const currentGeneration uint64 = 0

// NewCircuitBreaker creates a new circuit breaker with the given configuration.
func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = DefaultCircuitBreakerConfig()
	}
	normalized := *config
	if normalized.HalfOpenMaxRequests <= 0 {
		normalized.HalfOpenMaxRequests = 1
	}
	if normalized.SuccessThreshold <= 0 {
		normalized.SuccessThreshold = 1
	}
	if normalized.SuccessThreshold > normalized.HalfOpenMaxRequests {
		normalized.SuccessThreshold = normalized.HalfOpenMaxRequests
	}
	if normalized.ProbeLeakGrace <= 0 {
		normalized.ProbeLeakGrace = time.Duration(defaultProbeLeakGraceMultiplier) * normalized.Timeout
	}

	now := time.Now()
	return &CircuitBreaker{
		config:            &normalized,
		state:             CircuitClosed,
		generation:        1,
		lastStateChange:   now,
		lastProbeActivity: now,
	}
}

// Allow checks if a request should be allowed through.
// Returns true if the request should proceed, false if it should be blocked.
func (cb *CircuitBreaker) Allow() bool {
	return cb.admit().allowed
}

// admit atomically reports whether an allowed request consumed a half-open
// probe slot. Callers use that bit to guarantee an outcome is recorded even
// when the request exits before reaching the transport.
func (cb *CircuitBreaker) admit() circuitBreakerAdmission {
	if !cb.config.Enabled {
		return circuitBreakerAdmission{allowed: true}
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return circuitBreakerAdmission{allowed: true, generation: cb.generation}

	case CircuitOpen:
		// Check if timeout has elapsed to transition to half-open
		if time.Since(cb.lastFailure) >= cb.config.Timeout {
			cb.transitionTo(CircuitHalfOpen)
			cb.halfOpenRequests = 1
			return circuitBreakerAdmission{
				allowed:       true,
				halfOpenProbe: true,
				generation:    cb.generation,
				probeID:       cb.admitProbe(),
			}
		}
		return circuitBreakerAdmission{}

	case CircuitHalfOpen:
		// Allow limited requests in half-open state
		if cb.halfOpenRequests < cb.config.HalfOpenMaxRequests {
			cb.halfOpenRequests++
			return circuitBreakerAdmission{
				allowed:       true,
				halfOpenProbe: true,
				generation:    cb.generation,
				probeID:       cb.admitProbe(),
			}
		}
		// ESCAPE TIMER. With every probe slot consumed, ONLY a recorded outcome
		// can move the state — so a probe that never reports one (a goroutine
		// wedged below the breaker, a panic unwinding past the recorder, a caller
		// that leaks the admission) strands the breaker in half-open forever,
		// rejecting every request while the NAS may have been healthy for hours.
		// That is a worse outage than the one the breaker exists to contain,
		// because nothing outside this type can clear it. Fall back to Open and
		// restart the recovery clock: the normal Open -> half-open transition
		// above then issues a fresh probe one Timeout later, so the breaker
		// always keeps retrying.
		//
		// See halfOpenEscapeDue for why "no verdict yet" is not the same as "no
		// verdict coming".
		if cb.halfOpenEscapeDue(time.Now()) {
			klog.V(2).Infof("Circuit breaker half-open probes produced no verdict (%d still outstanding); reopening to restart the recovery clock", len(cb.probeAdmissions))
			cb.lastFailure = time.Now()
			cb.transitionTo(CircuitOpen)
		}
		return circuitBreakerAdmission{}
	}

	return circuitBreakerAdmission{allowed: true, generation: cb.generation}
}

// admitProbe records that a half-open probe just went on the wire and returns
// its id (lock held).
func (cb *CircuitBreaker) admitProbe() uint64 {
	now := time.Now()
	cb.nextProbeID++
	cb.probeAdmissions = append(cb.probeAdmissions, probeAdmission{id: cb.nextProbeID, at: now})
	cb.lastProbeActivity = now
	return cb.nextProbeID
}

// resolveProbe records that an outstanding half-open probe reported an outcome
// (lock held). The consumed slot is NOT given back — a probe that answered has
// been spent — only the in-flight accounting is updated.
//
// The probe is retired BY ID. Retiring the oldest instead made a fast probe's
// verdict erase a slow one's admission record, which is the difference between
// the escape timer seeing "a probe admitted at T is still running" and seeing
// "nothing is running": the first waits ProbeLeakGrace, the second reopens after
// a mere Timeout and throws away an answer that was on its way.
// currentGeneration outcomes carry no id and fall back to the oldest.
func (cb *CircuitBreaker) resolveProbe(probeID uint64) {
	for i, probe := range cb.probeAdmissions {
		if probeID == currentGeneration || probe.id == probeID {
			cb.probeAdmissions = append(cb.probeAdmissions[:i], cb.probeAdmissions[i+1:]...)
			break
		}
	}
	cb.lastProbeActivity = time.Now()
}

// outcomeApplies reports whether an outcome stamped with generation is still
// speaking about the state the breaker is in (lock held).
func (cb *CircuitBreaker) outcomeApplies(generation uint64) bool {
	return generation == currentGeneration || generation == cb.generation
}

// halfOpenEscapeDue reports whether half-open should be abandoned (lock held).
//
// The distinction that matters is between a probe that has produced no verdict
// YET and one that never will. Only elapsed time separates them, and the two
// need very different clocks:
//
//   - No probe outstanding. Every admitted probe has reported, yet the state did
//     not move. Nothing more is coming, so the ordinary recovery Timeout is the
//     right patience.
//
//   - A probe still outstanding. An answer is genuinely pending. Reopening now
//     throws it away — RecordSuccess is a deliberate no-op in the Open state —
//     and guarantees the next cycle repeats, which is exactly the spurious-open
//     loop a healthy-but-slow appliance produced: ten opens, zero failures,
//     every probe eventually successful. A probe is only presumed lost after
//     ProbeLeakGrace, which is explicitly longer than any legitimate call.
func (cb *CircuitBreaker) halfOpenEscapeDue(now time.Time) bool {
	if len(cb.probeAdmissions) > 0 {
		return now.Sub(cb.probeAdmissions[0].at) >= cb.config.ProbeLeakGrace
	}
	return now.Sub(cb.lastProbeActivity) >= cb.config.Timeout
}

// RecordSuccess records a successful request that is not scoped to a particular
// breaker generation; it applies to whatever state is live now.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.recordSuccess(currentGeneration, currentGeneration)
}

// recordSuccess records a successful request reported by a call admitted in the
// given generation.
func (cb *CircuitBreaker) recordSuccess(generation, probeID uint64) {
	if !cb.config.Enabled {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.totalSuccesses++

	if !cb.outcomeApplies(generation) {
		// The state this success measured is gone. Crediting it here would let a
		// probe the breaker already gave up on close a later generation's circuit
		// on evidence that predates the reopen.
		klog.V(2).Infof("Circuit breaker ignoring success from stale generation %d (current %d)", generation, cb.generation)
		return
	}

	switch cb.state {
	case CircuitClosed:
		// Reset failure count on success
		cb.failures = 0

	case CircuitHalfOpen:
		cb.resolveProbe(probeID)
		cb.successes++
		if cb.successes >= cb.config.SuccessThreshold {
			// Enough successes - close the circuit
			cb.transitionTo(CircuitClosed)
		}

	case CircuitOpen:
		// Calls are rejected before they run while open (see AllowRequest);
		// a success here would be spurious. Only the half-open timeout
		// transitions out of Open.
	}
}

// RecordAbandoned releases a half-open probe slot WITHOUT recording an outcome.
//
// It exists for client-side context cancellation, which says nothing about the
// NAS: the caller walked away before the appliance was given a chance to answer.
// In the closed state such a cancellation has always been recorded as neither
// success nor failure; the half-open state used to record it as a FAILURE
// (through callRaw's unrecorded-probe backstop), so a burst of CSI RPC
// deadlines could reopen a circuit whose appliance was perfectly healthy and
// keep it reopening. This restores the symmetry while still returning the probe
// slot, so the NEXT caller can take the measurement this one abandoned.
func (cb *CircuitBreaker) RecordAbandoned() {
	cb.recordAbandoned(currentGeneration, currentGeneration)
}

// recordAbandoned releases a half-open probe slot for a call admitted in the
// given generation.
func (cb *CircuitBreaker) recordAbandoned(generation, probeID uint64) {
	if !cb.config.Enabled {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if !cb.outcomeApplies(generation) {
		// Returning a slot to a generation that never issued this probe would
		// hand out MORE probes than HalfOpenMaxRequests allows.
		return
	}

	if cb.state == CircuitHalfOpen && cb.halfOpenRequests > 0 {
		cb.halfOpenRequests--
		cb.resolveProbe(probeID)
	}
}

// RecordFailure records a failed request that is not scoped to a particular
// breaker generation; it applies to whatever state is live now.
func (cb *CircuitBreaker) RecordFailure() {
	cb.recordFailure(currentGeneration, currentGeneration)
}

// recordFailure records a failed request reported by a call admitted in the
// given generation.
func (cb *CircuitBreaker) recordFailure(generation, probeID uint64) {
	if !cb.config.Enabled {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.totalFailures++

	if !cb.outcomeApplies(generation) {
		return
	}

	switch cb.state {
	case CircuitClosed:
		cb.lastFailure = time.Now()
		cb.failures++
		if cb.failures >= cb.config.FailureThreshold {
			// Too many failures - open the circuit
			cb.transitionTo(CircuitOpen)
		}

	case CircuitHalfOpen:
		// Any failure in half-open reopens the circuit
		cb.lastFailure = time.Now()
		cb.resolveProbe(probeID)
		cb.transitionTo(CircuitOpen)

	case CircuitOpen:
		// RECOVERY STARVATION. Calls are rejected before they run while open, so
		// a failure recorded here belongs to a request admitted in an earlier
		// generation; the reopen it reports has already happened. lastFailure is
		// the Open -> half-open recovery clock, and stamping it with a straggler's
		// arrival time restarts that clock for a failure the breaker already
		// counted. A trickle of slow-failing stragglers — exactly what an outage
		// produces, since every in-flight call fails late — could then hold the
		// breaker Open indefinitely, with no probe ever issued and nothing outside
		// this type able to clear it. The failure is still counted in the metric
		// above; it simply must not push recovery away.
	}
}

// transitionTo changes the circuit state (must be called with lock held).
func (cb *CircuitBreaker) transitionTo(newState CircuitState) {
	oldState := cb.state
	cb.state = newState
	cb.lastStateChange = time.Now()
	cb.failures = 0
	cb.successes = 0
	cb.halfOpenRequests = 0
	// Probes admitted under the OLD state can no longer move this one, so they
	// are not "outstanding" for escape purposes. Bumping the generation is what
	// makes a late verdict from one of them inert rather than letting it land in
	// the new state's rules as if it had measured them.
	cb.generation++
	cb.probeAdmissions = nil
	cb.lastProbeActivity = cb.lastStateChange

	if newState == CircuitOpen {
		cb.totalCircuitOpens++
	}

	klog.V(2).Infof("Circuit breaker state change: %s -> %s", oldState, newState)
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Stats returns circuit breaker statistics.
func (cb *CircuitBreaker) Stats() CircuitBreakerStats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return CircuitBreakerStats{
		State:             cb.state,
		Failures:          cb.failures,
		Successes:         cb.successes,
		LastFailure:       cb.lastFailure,
		LastStateChange:   cb.lastStateChange,
		TotalFailures:     cb.totalFailures,
		TotalSuccesses:    cb.totalSuccesses,
		TotalCircuitOpens: cb.totalCircuitOpens,
	}
}

// CircuitBreakerStats holds statistics about the circuit breaker.
type CircuitBreakerStats struct {
	State             CircuitState
	Failures          int
	Successes         int
	LastFailure       time.Time
	LastStateChange   time.Time
	TotalFailures     int64
	TotalSuccesses    int64
	TotalCircuitOpens int64
}

// Reset resets the circuit breaker to closed state.
// This is useful for testing or manual intervention.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.transitionTo(CircuitClosed)
	klog.Info("Circuit breaker manually reset to closed state")
}
