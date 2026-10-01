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

// admissionGate is a counting semaphore whose waiters are admitted by
// (priority, operation start, arrival) instead of in whatever order the
// runtime wakes them.
type admissionGate struct {
	mu       sync.Mutex
	capacity int
	inUse    int
	seq      uint64
	waiting  []*admissionWaiter
}

type admissionWaiter struct {
	priority Priority
	start    time.Time
	seq      uint64
	ready    chan struct{}
	granted  bool
}

func newAdmissionGate(capacity int) *admissionGate {
	if capacity < 1 {
		capacity = 1
	}
	return &admissionGate{capacity: capacity}
}

func (w *admissionWaiter) before(other *admissionWaiter) bool {
	if w.priority != other.priority {
		return w.priority < other.priority
	}
	if !w.start.Equal(other.start) {
		return w.start.Before(other.start)
	}
	return w.seq < other.seq
}

// acquire waits for a slot. It returns ctx's error if ctx ends first, holding
// no slot.
func (g *admissionGate) acquire(ctx context.Context) error {
	// An operation that has already given up never takes a slot.
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	g.mu.Lock()
	g.seq++
	w := &admissionWaiter{
		priority: priorityOf(ctx),
		start:    operationStartOf(ctx, now),
		seq:      g.seq,
		ready:    make(chan struct{}),
	}
	g.waiting = append(g.waiting, w)
	g.dispatchLocked()
	g.mu.Unlock()

	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		if w.granted {
			// Granted while giving up: hand the slot on.
			g.inUse--
			g.dispatchLocked()
		} else {
			g.removeLocked(w)
		}
		g.mu.Unlock()
		return ctx.Err()
	}
}

func (g *admissionGate) release() {
	g.mu.Lock()
	g.inUse--
	g.dispatchLocked()
	g.mu.Unlock()
}

// dispatchLocked grants free slots to the best waiters.
func (g *admissionGate) dispatchLocked() {
	for g.inUse < g.capacity && len(g.waiting) > 0 {
		best := 0
		for i := 1; i < len(g.waiting); i++ {
			if g.waiting[i].before(g.waiting[best]) {
				best = i
			}
		}
		w := g.waiting[best]
		g.waiting = append(g.waiting[:best], g.waiting[best+1:]...)
		g.inUse++
		w.granted = true
		close(w.ready)
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
