package driver

import (
	"context"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// ServiceReloadDebouncer coalesces multiple service reload requests into a single
// reload operation. This prevents "reload storms" when many volumes are created
// simultaneously (e.g., restoring a statefulset or scaling up a deployment).
//
// How it works:
// - When a reload is requested, a timer starts (debounce window)
// - If another request arrives before the timer expires, the timer resets
// - When the timer finally expires, a single reload is performed
// - All pending callers receive the result of that single reload
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
	pending  []chan error // channels waiting for reload result
	lastCall time.Time
	count    int // number of coalesced requests
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

	state, exists := d.services[service]
	if !exists {
		state = &serviceReloadState{
			pending: make([]chan error, 0),
		}
		d.services[service] = state
	}

	state.pending = append(state.pending, resultCh)
	state.count++
	state.lastCall = time.Now()

	// If timer already exists, stop it and we'll restart it
	if state.timer != nil {
		state.timer.Stop()
	}

	// Start/restart the debounce timer
	state.timer = time.AfterFunc(d.debounceDelay, func() {
		d.executeReload(service)
	})

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
			for i, ch := range st.pending {
				if ch == resultCh {
					st.pending = append(st.pending[:i], st.pending[i+1:]...)
					break
				}
			}
		}
		d.mu.Unlock()
		return ctx.Err()
	}
}

// executeReload performs the actual service reload and notifies all pending callers
func (d *ServiceReloadDebouncer) executeReload(service string) {
	d.mu.Lock()
	state, exists := d.services[service]
	if !exists || len(state.pending) == 0 {
		d.mu.Unlock()
		return
	}

	// Capture pending channels and reset state
	pendingChannels := state.pending
	coalescedCount := state.count
	state.pending = make([]chan error, 0)
	state.count = 0
	state.timer = nil

	d.mu.Unlock()

	// Log the coalescing effect
	if coalescedCount > 1 {
		klog.Infof("Service reload debouncer: coalesced %d reload requests for %s into single reload", coalescedCount, service)
	}

	// Perform the actual reload with a background context
	// (the original contexts may have timed out, but we still want to reload)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := d.reloadFunc(ctx, service)
	if err != nil {
		klog.Warningf("Service reload debouncer: reload of %s failed: %v", service, err)
	} else {
		klog.V(4).Infof("Service reload debouncer: successfully reloaded %s", service)
	}

	// Notify all pending callers
	for _, ch := range pendingChannels {
		select {
		case ch <- err:
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
		for _, ch := range state.pending {
			select {
			case ch <- context.Canceled:
			default:
			}
		}
	}
	d.services = make(map[string]*serviceReloadState)
}
