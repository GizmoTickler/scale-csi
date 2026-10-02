package driver

import (
	"context"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// ServiceReloadDebouncer coalesces multiple service reload requests into a single
// reload operation. This prevents "reload storms" when many volumes are created
// simultaneously (e.g., restoring a statefulset or scaling up a deployment).
//
// How it works (leading-window batching):
//   - The FIRST request in an idle period arms a timer for the debounce window
//   - Requests that arrive before the timer fires coalesce onto the same deadline
//     (they do NOT reset the timer)
//   - When the timer fires, a single reload is performed for the whole batch
//   - All pending callers receive the result of that single reload
//   - The next request after a fire starts a new batch
//
// Unlike pure trailing-edge debouncing, a sustained request stream cannot starve
// the reload: the worst-case latency for any batch is one debounce window.
type ServiceReloadDebouncer struct {
	mu            sync.Mutex
	debounceDelay time.Duration
	reloadFunc    func(ctx context.Context, service string) error

	// Per-service state
	services map[string]*serviceReloadState
}

// serviceReloadState tracks the debounce state for a single service
type serviceReloadState struct {
	timer    *time.Timer
	pending  []reloadWaiter // callers waiting for the reload's result
	lastCall time.Time
	count    int // number of coalesced requests

	// changedGen counts the backend changes reported for this service
	// (MarkChanged, and every RequestReload); reloadedGen is the highest
	// changedGen a SUCCESSFUL reload is known to cover. A reload is owed while
	// changedGen > reloadedGen. A new state starts owed (changedGen 1): after a
	// controller start nothing is known about what the service has loaded.
	changedGen  uint64
	reloadedGen uint64
}

// reloadWaiter is one caller of a batch, with the admission class and the
// operation start of the work it belongs to: the reload is admitted to
// TrueNAS as the work of the callers still waiting when it fires, so it is
// not queued behind their own later calls (or, for a publish waiting on it,
// behind provisioning).
type reloadWaiter struct {
	result   chan error
	priority truenas.Priority
	start    time.Time
}

// NewServiceReloadDebouncer creates a new debouncer with the given delay.
// The reloadFunc is called to perform the actual service reload.
func NewServiceReloadDebouncer(debounceDelay time.Duration, reloadFunc func(ctx context.Context, service string) error) *ServiceReloadDebouncer {
	return &ServiceReloadDebouncer{
		debounceDelay: debounceDelay,
		reloadFunc:    reloadFunc,
		services:      make(map[string]*serviceReloadState),
	}
}

// RequestReload requests a service reload. The reload will be debounced -
// if multiple requests arrive within the debounce window, only one reload
// will be performed. All callers will receive the result of that reload.
//
// The context is used for the actual reload operation. If the context is
// canceled before the reload happens, this request is removed from the
// pending list (but does not cancel reloads for other callers).
func (d *ServiceReloadDebouncer) RequestReload(ctx context.Context, service string) error {
	resultCh := make(chan error, 1)

	d.mu.Lock()

	state := d.stateLocked(service)
	state.changedGen++

	now := time.Now()
	priority := truenas.PriorityOf(ctx)
	start, stamped := truenas.OperationStartOf(ctx)
	if !stamped {
		start = now
	}
	state.pending = append(state.pending, reloadWaiter{result: resultCh, priority: priority, start: start})
	state.count++
	state.lastCall = now

	// Leading-window batching: arm the timer only for the FIRST request of a
	// batch. Requests that arrive while the timer is already running coalesce
	// onto the existing deadline instead of pushing it back. This bounds the
	// worst-case reload latency to one window and guarantees a sustained request
	// stream still fires reloads at ~window cadence rather than starving the
	// reload (and every caller blocked on resultCh) indefinitely. Once the timer
	// fires, executeReload clears it so the next request starts a fresh batch.
	if state.timer == nil {
		//nolint:contextcheck // deliberately detached: this timer fires once for a whole BATCH of coalesced requests, potentially after the request that armed it has already returned, so no single caller's context is the right one to inherit
		state.timer = time.AfterFunc(d.debounceDelay, func() {
			d.executeReload(service)
		})
	}

	klog.V(4).Infof("Service reload debouncer: queued reload for %s (pending: %d)", service, state.count)

	d.mu.Unlock()

	// Wait for result or context cancellation
	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		// Remove ourselves from pending list
		d.mu.Lock()
		if st, ok := d.services[service]; ok {
			for i, waiter := range st.pending {
				if waiter.result == resultCh {
					st.pending = append(st.pending[:i], st.pending[i+1:]...)
					break
				}
			}
		}
		d.mu.Unlock()
		return ctx.Err()
	}
}

// stateLocked returns the service's state, creating it (owed) if absent. d.mu
// must be held.
func (d *ServiceReloadDebouncer) stateLocked(service string) *serviceReloadState {
	state, exists := d.services[service]
	if !exists {
		state = &serviceReloadState{changedGen: 1}
		d.services[service] = state
	}
	return state
}

// MarkChanged records that a backend change for service has been written (or
// attempted: call it after the write returns, whatever its result), so a
// reload is owed until one that starts after this call succeeds. A caller
// that changed nothing calls RequestReloadIfOwed instead of RequestReload.
func (d *ServiceReloadDebouncer) MarkChanged(service string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stateLocked(service).changedGen++
}

// ReloadOwed reports whether a change to service has not yet been covered by a
// successful reload (always true before the first one).
func (d *ServiceReloadDebouncer) ReloadOwed(service string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	state := d.stateLocked(service)
	return state.changedGen > state.reloadedGen
}

// RequestReloadIfOwed reloads service only when a change is owed one: an
// earlier change whose reload failed, was never reached, or predates this
// process. A caller whose own pass changed nothing uses it, so an unchanged
// republish reloads nothing while a change is never left unloaded.
func (d *ServiceReloadDebouncer) RequestReloadIfOwed(ctx context.Context, service string) error {
	if !d.ReloadOwed(service) {
		return nil
	}
	return d.RequestReload(ctx, service)
}

// executeReload performs the actual service reload and notifies all pending callers
func (d *ServiceReloadDebouncer) executeReload(service string) {
	d.mu.Lock()
	state, exists := d.services[service]
	if !exists || len(state.pending) == 0 {
		if exists {
			// The batch fully drained (every caller canceled) before the window
			// fired. Clear the timer so the next request arms a fresh batch — a
			// stale non-nil timer would block arming forever and starve every
			// future reload for this service.
			state.timer = nil
			state.count = 0
		}
		d.mu.Unlock()
		return
	}

	// Capture pending channels and reset state
	pendingChannels := state.pending
	coalescedCount := state.count
	priority, start := pendingChannels[0].priority, pendingChannels[0].start
	for _, waiter := range pendingChannels[1:] {
		if waiter.priority < priority {
			priority = waiter.priority
		}
		if waiter.start.Before(start) {
			start = waiter.start
		}
	}
	state.pending = nil
	state.count = 0
	state.timer = nil
	// Every change marked before this point was written before the reload
	// starts, so a successful reload covers it.
	coveredGen := state.changedGen

	d.mu.Unlock()

	// Log the coalescing effect
	if coalescedCount > 1 {
		klog.Infof("Service reload debouncer: coalesced %d reload requests for %s into single reload", coalescedCount, service)
	}

	// The reload runs detached from its callers' contexts (some may have
	// timed out; the reload is still wanted), but is admitted to TrueNAS with
	// their class and oldest operation start. It carries no deadline of its
	// own: the client bounds the call itself, after a request slot is granted,
	// by its request timeout. A deadline here would also have counted the wait
	// for a slot, which under a burst of the callers' own operations can
	// exceed any fixed budget.
	ctx := truenas.WithOperationStart(truenas.WithPriority(context.Background(), priority), start)

	err := d.reloadFunc(ctx, service)
	if err != nil {
		klog.Warningf("Service reload debouncer: reload of %s failed: %v", service, err)
	} else {
		klog.V(4).Infof("Service reload debouncer: successfully reloaded %s", service)
		d.mu.Lock()
		// Stop may have replaced the map; record against the live state only.
		if live, ok := d.services[service]; ok && live == state && coveredGen > live.reloadedGen {
			live.reloadedGen = coveredGen
		}
		d.mu.Unlock()
	}

	// Notify all pending callers
	for _, waiter := range pendingChannels {
		select {
		case waiter.result <- err:
		default:
			// Channel was already closed or full (context canceled)
		}
	}
}

// Stop cancels all pending timers. Call this when shutting down the driver.
func (d *ServiceReloadDebouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, state := range d.services {
		if state.timer != nil {
			state.timer.Stop()
		}
		// Notify pending callers that we're shutting down
		for _, waiter := range state.pending {
			select {
			case waiter.result <- context.Canceled:
			default:
			}
		}
	}
	d.services = make(map[string]*serviceReloadState)
}
