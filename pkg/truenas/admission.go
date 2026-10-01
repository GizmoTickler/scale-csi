package truenas

import (
	"context"
	"sync"
	"time"
)

// Priority orders requests waiting for one of the client's request slots:
// a lower value is admitted first. TrueNAS's middleware serves the control
// plane largely one write at a time, so when a burst queues up, the order of
// admission decides which CSI operation finishes first.
type Priority int

const (
	// PriorityAttach is for publishing and unpublishing: a pod is waiting.
	PriorityAttach Priority = iota
	// PriorityDefault is everything not marked otherwise: provisioning,
	// expansion, snapshots, the controller's own background work.
	PriorityDefault
	// PriorityDelete is for deleting volumes and snapshots: nobody waits on them.
	PriorityDelete
)

type priorityKey struct{}

type operationStartKey struct{}

// WithPriority marks the requests made under ctx.
func WithPriority(ctx context.Context, priority Priority) context.Context {
	return context.WithValue(ctx, priorityKey{}, priority)
}

// WithOperationStart records when the operation the requests under ctx belong
// to started. Among requests of one priority, the oldest operation's request
// is admitted first, so a burst of operations completes in the order it
// arrived instead of every operation finishing together at the end.
func WithOperationStart(ctx context.Context, start time.Time) context.Context {
	return context.WithValue(ctx, operationStartKey{}, start)
}

func priorityOf(ctx context.Context) Priority {
	if p, ok := ctx.Value(priorityKey{}).(Priority); ok {
		return p
	}
	return PriorityDefault
}

func operationStartOf(ctx context.Context, fallback time.Time) time.Time {
	if t, ok := ctx.Value(operationStartKey{}).(time.Time); ok && !t.IsZero() {
		return t
	}
	return fallback
}

// agingStep is how long a waiting delete takes to climb into the default
// class. It bounds how long a delete can be kept waiting, which matters
// because operations hold per-volume locks across their calls: a
// DeleteSnapshot that waits holding its source volume's lock would otherwise
// block that volume's publish for as long as default work keeps arriving.
// Nothing ages into the attach class: under overload, default work that
// started before a publish would otherwise tie with it and go first, and
// attach demand is bounded by the attacher's workers, so it cannot starve the
// rest.
const agingStep = 2 * time.Second

// admissionGate is a counting semaphore whose waiters are admitted by
// (aged priority, operation start, arrival) instead of first come, first
// served.
type admissionGate struct {
	mu       sync.Mutex
	capacity int
	inUse    int
	seq      uint64
	waiting  []*admissionWaiter
	now      func() time.Time
	metrics  AdmissionMetrics
}

// AdmissionMetrics receives the gate's observations; either function may be nil.
type AdmissionMetrics struct {
	// Waited is called once per admitted request the caller keeps, with how
	// long it queued.
	Waited func(class string, seconds float64)
	// Queued is called with the number of requests of a class waiting, each
	// time it changes.
	Queued func(class string, waiting int)
}

func (g *admissionGate) reportQueuedLocked(p Priority) {
	if g.metrics.Queued == nil {
		return
	}
	n := 0
	for _, w := range g.waiting {
		if w.priority == p {
			n++
		}
	}
	g.metrics.Queued(p.String(), n)
}

type admissionWaiter struct {
	priority Priority
	start    time.Time
	enqueued time.Time
	seq      uint64
	ready    chan struct{}
	granted  bool
	// grantedAt is set under the gate's lock before ready is closed.
	grantedAt time.Time
}

func newAdmissionGate(capacity int) *admissionGate {
	return newAdmissionGateWithMetrics(capacity, AdmissionMetrics{})
}

func newAdmissionGateWithMetrics(capacity int, metrics AdmissionMetrics) *admissionGate {
	if capacity < 1 {
		capacity = 1
	}
	return &admissionGate{capacity: capacity, now: time.Now, metrics: metrics}
}

// rank is the waiter's class after aging: one class higher per agingStep
// waited, never above PriorityDefault for a waiter that is not an attach.
func (w *admissionWaiter) rank(now time.Time) int {
	if w.priority <= PriorityDefault {
		return int(w.priority)
	}
	r := int(w.priority) - int(now.Sub(w.enqueued)/agingStep)
	if r < int(PriorityDefault) {
		return int(PriorityDefault)
	}
	return r
}

func (w *admissionWaiter) before(other *admissionWaiter, now time.Time) bool {
	if a, b := w.rank(now), other.rank(now); a != b {
		return a < b
	}
	if !w.start.Equal(other.start) {
		return w.start.Before(other.start)
	}
	return w.seq < other.seq
}

// acquire waits for a slot. It returns ctx's error if ctx ends first, holding
// no slot; a slot granted to a caller that has meanwhile given up is handed on.
func (g *admissionGate) acquire(ctx context.Context) error {
	// An operation that has already given up never takes a slot.
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	now := g.now()
	g.seq++
	w := &admissionWaiter{
		priority: priorityOf(ctx),
		start:    operationStartOf(ctx, now),
		enqueued: now,
		seq:      g.seq,
		ready:    make(chan struct{}),
	}
	g.waiting = append(g.waiting, w)
	g.reportQueuedLocked(w.priority)
	g.dispatchLocked()
	g.mu.Unlock()

	select {
	case <-w.ready:
		if err := ctx.Err(); err != nil {
			g.release()
			return err
		}
		// Only a slot the caller keeps counts as a wait.
		if g.metrics.Waited != nil {
			g.metrics.Waited(w.priority.String(), w.grantedAt.Sub(w.enqueued).Seconds())
		}
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		if w.granted {
			// Granted while giving up: hand the slot on.
			g.inUse--
			g.dispatchLocked()
		} else {
			g.removeLocked(w)
			g.reportQueuedLocked(w.priority)
		}
		g.mu.Unlock()
		return ctx.Err()
	}
}

func (g *admissionGate) release() {
	g.mu.Lock()
	if g.inUse <= 0 {
		g.mu.Unlock()
		panic("truenas: request slot released without being acquired")
	}
	g.inUse--
	g.dispatchLocked()
	g.mu.Unlock()
}

// dispatchLocked grants free slots to the best waiters.
func (g *admissionGate) dispatchLocked() {
	if g.inUse >= g.capacity || len(g.waiting) == 0 {
		return
	}
	now := g.now()
	for g.inUse < g.capacity && len(g.waiting) > 0 {
		best := 0
		for i := 1; i < len(g.waiting); i++ {
			if g.waiting[i].before(g.waiting[best], now) {
				best = i
			}
		}
		w := g.waiting[best]
		g.waiting = append(g.waiting[:best], g.waiting[best+1:]...)
		g.inUse++
		w.granted = true
		w.grantedAt = now
		close(w.ready)
		g.reportQueuedLocked(w.priority)
	}
}

func (g *admissionGate) removeLocked(w *admissionWaiter) {
	for i, candidate := range g.waiting {
		if candidate == w {
			g.waiting = append(g.waiting[:i], g.waiting[i+1:]...)
			return
		}
	}
}

// inFlight is the number of slots held.
func (g *admissionGate) inFlight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inUse
}

// PriorityOf is the priority requests made under ctx are admitted with.
func PriorityOf(ctx context.Context) Priority {
	return priorityOf(ctx)
}

// OperationStartOf is the operation start recorded on ctx, if any.
func OperationStartOf(ctx context.Context) (time.Time, bool) {
	t, ok := ctx.Value(operationStartKey{}).(time.Time)
	return t, ok && !t.IsZero()
}

// String names the class for metrics and logs.
func (p Priority) String() string {
	switch p {
	case PriorityAttach:
		return "attach"
	case PriorityDelete:
		return "delete"
	default:
		return "default"
	}
}
