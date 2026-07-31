package truenas

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"k8s.io/klog/v2"
)

const jobEventBufferSize = 256

type jobEvent struct {
	jobID  int64
	state  string
	detail string
}

type jobDispatcher struct {
	mu      sync.Mutex
	stopped bool
	waiters map[int64]map[chan jobEvent]struct{}
	events  chan jobEvent
	done    chan struct{}
	wg      sync.WaitGroup
}

func newJobDispatcher() *jobDispatcher {
	d := &jobDispatcher{
		waiters: make(map[int64]map[chan jobEvent]struct{}),
		events:  make(chan jobEvent, jobEventBufferSize),
		done:    make(chan struct{}),
	}
	d.wg.Add(1)
	go d.run()
	return d
}

type jobCollectionNotification struct {
	Collection string `json:"collection"`
	Fields     struct {
		ID        int64  `json:"id"`
		State     string `json:"state"`
		Error     string `json:"error"`
		Exception string `json:"exception"`
	} `json:"fields"`
}

// offer is called by the WebSocket read loop. It performs one typed parse and
// one non-blocking send: it never waits, takes dispatcher/connection locks, or
// calls back into the client state machine.
func (d *jobDispatcher) offer(params json.RawMessage) {
	if d == nil || len(params) == 0 {
		return
	}
	var notification jobCollectionNotification
	if err := json.Unmarshal(params, &notification); err != nil {
		klog.V(4).Infof("Ignoring malformed core.get_jobs notification: %v", err)
		return
	}
	if notification.Collection != "core.get_jobs" || notification.Fields.ID < 0 || notification.Fields.State == "" {
		return
	}
	detail := notification.Fields.Error
	if detail == "" {
		detail = notification.Fields.Exception
	}
	ev := jobEvent{
		jobID:  notification.Fields.ID,
		state:  notification.Fields.State,
		detail: detail,
	}
	select {
	case d.events <- ev:
	default:
		// Coalescing applies after run receives an event. If this shared buffer
		// is saturated, the hybrid waiter's safety poll remains the floor.
	}
}

func (d *jobDispatcher) run() {
	defer d.wg.Done()
	for {
		select {
		case <-d.done:
			return
		case ev := <-d.events:
			d.mu.Lock()
			for waiter := range d.waiters[ev.jobID] {
				deliverJobEvent(waiter, ev)
			}
			d.mu.Unlock()
		}
	}
}

// deliverJobEvent coalesces into a cap-1 waiter. A terminal state displaces a
// queued non-terminal; a non-terminal never displaces a queued terminal.
func deliverJobEvent(ch chan jobEvent, ev jobEvent) {
	if terminalJobState(ev.state) {
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- ev:
		default:
		}
		return
	}
	select {
	case ch <- ev:
	default:
	}
}

func (d *jobDispatcher) register(jobID int64) chan jobEvent {
	d.mu.Lock()
	defer d.mu.Unlock()
	ch := make(chan jobEvent, 1)
	if d.stopped {
		close(ch)
		return ch
	}
	waiters := d.waiters[jobID]
	if waiters == nil {
		waiters = make(map[chan jobEvent]struct{})
		d.waiters[jobID] = waiters
	}
	waiters[ch] = struct{}{}
	return ch
}

func (d *jobDispatcher) unregister(jobID int64, ch chan jobEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()
	waiters := d.waiters[jobID]
	delete(waiters, ch)
	if len(waiters) == 0 {
		delete(d.waiters, jobID)
	}
}

// Stop closes only detached waiter channels. The shared events channel is
// intentionally never closed because read loops may race with shutdown.
func (d *jobDispatcher) Stop() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	detached := d.waiters
	d.waiters = make(map[int64]map[chan jobEvent]struct{})
	close(d.done)
	d.mu.Unlock()

	d.wg.Wait()
	for _, waiters := range detached {
		for waiter := range waiters {
			close(waiter)
		}
	}
}

func (c *Connection) markJobSubscribed(generation uint64) bool {
	c.mu.RLock()
	current := c.generation
	stopped := c.stopped
	c.mu.RUnlock()
	if generation != current || stopped {
		return false
	}
	return c.jobSubState.CompareAndSwap(generation<<1, generation<<1|1)
}

func (c *Connection) jobsSubscribed() bool {
	state := c.jobSubState.Load()
	c.mu.RLock()
	generation := c.generation
	live := !c.stopped && c.conn.Load() != nil
	c.mu.RUnlock()
	return live && state>>1 == generation && state&1 == 1
}

func (c *Connection) subscribeJobs(generation uint64, done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	if c.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Timeout)
	defer cancel()
	// Tie the subscribe RPC to connection teardown so Close never waits out
	// the full config timeout on a mid-flight subscribe. The helper exits when
	// either the generation ends or subscribeJobs returns (defer cancel).
	go func() {
		select {
		case <-done:
			cancel()
		case <-ctx.Done():
		}
	}()

	// Documented control-plane bypass: the subscription must land on this exact
	// connection. It intentionally bypasses Client.callRaw and takes no
	// semaphore slot, retry/breaker admission, or metrics callback.
	if _, err := c.callWithGeneration(ctx, generation, false, "core.subscribe", "core.get_jobs"); err != nil {
		klog.V(2).Infof("Conn %d gen %d: core.get_jobs subscribe failed (pure-poll fallback): %v", c.id, generation, err)
		return
	}
	if c.markJobSubscribed(generation) {
		c.client.signalJobSubscription()
	}
}

func (c *Client) anyConnectionJobSubscribed() bool {
	for _, conn := range c.pool {
		if conn != nil && conn.jobsSubscribed() {
			return true
		}
	}
	return false
}

// AnyConnectionJobSubscribed reports whether at least one pooled connection
// holds a live core.get_jobs subscription. The driver health tick publishes it
// as scale_csi_job_dispatcher_subscribed; false means pure-poll fallback.
func (c *Client) AnyConnectionJobSubscribed() bool {
	if c == nil {
		return false
	}
	return c.anyConnectionJobSubscribed()
}

func (c *Client) jobSubscriptionSignal() <-chan struct{} {
	c.jobSubscriptionMu.Lock()
	defer c.jobSubscriptionMu.Unlock()
	if c.jobSubscriptionChanged == nil {
		c.jobSubscriptionChanged = make(chan struct{})
	}
	return c.jobSubscriptionChanged
}

func (c *Client) signalJobSubscription() {
	c.jobSubscriptionMu.Lock()
	if c.jobSubscriptionChanged == nil {
		c.jobSubscriptionChanged = make(chan struct{})
	}
	close(c.jobSubscriptionChanged)
	c.jobSubscriptionChanged = make(chan struct{})
	c.jobSubscriptionMu.Unlock()
}

func terminalJobState(state string) bool {
	switch strings.ToUpper(state) {
	case "SUCCESS", "FAILED", "ABORTED", "CANCELED":
		return true
	default:
		return false
	}
}

func terminalJobResult(ev jobEvent) error {
	if strings.EqualFold(ev.state, "SUCCESS") {
		return nil
	}
	detail := ev.detail
	if detail == "" {
		detail = "no error detail"
	}
	return &jobTerminalError{state: ev.state, detail: detail}
}

func clientClosedJobWaitError(jobID int64) error {
	return fmt.Errorf("%w: client closed while waiting for job %d", ErrTransportFailure, jobID)
}
