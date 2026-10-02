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

// admissionLane is the kind of request a slot is for. TrueNAS's middleware
// serves writes largely one at a time, so a burst of writes holding every
// slot used to keep the reads that publish and unpublish start with queued
// behind them. Writes now hold at most writeCapacity of the slots, which
// leaves the rest to reads.
//
// nvmet writes have no lane of their own. A one-slot nvmet lane was tried:
// with the middleware serving writes one at a time anyway, it let lower-class
// writes take the write slots a waiting publish's nvmet write could not, and
// in the latency model a drain under a write burst took 91 s instead of 24 s.
type admissionLane int

const (
	laneRead admissionLane = iota
	laneWrite
)

// defaultWriteCapacity is how many slots writes may hold at once.
const defaultWriteCapacity = 4

// laneForMethod classifies a middleware method. A method not known to be a
// read is a write.
func laneForMethod(method string) admissionLane {
	if isReadAPIMethod(method) {
		return laneRead
	}
	return laneWrite
}

// isReadAPIMethod is isIdempotentAPIMethod widened by the read-only methods
// whose names do not say so. It only places a request in a lane; it never
// decides a retry.
func isReadAPIMethod(method string) bool {
	if isIdempotentAPIMethod(method) {
		return true
	}
	switch method {
	case "pool.dataset.attachments", "pool.dataset.processes", "pool.dataset.encryption_summary",
		"pool.dataset.recommended_zvol_blocksize", "zfs.resource.snapshot.holds", "filesystem.getacl":
		return true
	}
	return false
}

// admissionGate is a counting semaphore whose waiters are admitted by
// (aged priority, operation start, arrival) instead of first come, first
// served, among the waiters whose lane has room.
type admissionGate struct {
	mu       sync.Mutex
	capacity int
	inUse    int
	// writeCapacity bounds the slots held by writes; writes counts them.
	writeCapacity int
	writes        int
	seq           uint64
	waiting       []*admissionWaiter
	now           func() time.Time
	metrics       AdmissionMetrics
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
	lane     admissionLane
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
	return newAdmissionGateWithLanes(capacity, defaultWriteCapacity, metrics)
}

// newAdmissionGateWithLanes is a gate whose writes hold at most
// writeCapacity slots. Reads keep at least one slot of their own when there
// are two or more.
func newAdmissionGateWithLanes(capacity, writeCapacity int, metrics AdmissionMetrics) *admissionGate {
	if capacity < 1 {
		capacity = 1
	}
	if writeCapacity < 1 {
		writeCapacity = defaultWriteCapacity
	}
	if capacity > 1 && writeCapacity > capacity-1 {
		writeCapacity = capacity - 1
	}
	if writeCapacity > capacity {
		writeCapacity = capacity
	}
	return &admissionGate{capacity: capacity, writeCapacity: writeCapacity, now: time.Now, metrics: metrics}
}

// laneHasRoomLocked is whether a request of lane may take a slot now.
func (g *admissionGate) laneHasRoomLocked(lane admissionLane) bool {
	if g.inUse >= g.capacity {
		return false
	}
	return lane != laneWrite || g.writes < g.writeCapacity
}

func (g *admissionGate) takeLocked(lane admissionLane) {
	g.inUse++
	if lane == laneWrite {
		g.writes++
	}
}

func (g *admissionGate) putLocked(lane admissionLane) {
	if g.inUse <= 0 {
		panic("truenas: request slot released without being acquired")
	}
	g.inUse--
	if lane == laneWrite {
		g.writes--
	}
	if g.writes < 0 {
		panic("truenas: write slot released without being acquired")
	}
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

// acquire waits for a read slot (acquireLane).
func (g *admissionGate) acquire(ctx context.Context) error {
	return g.acquireLane(ctx, laneRead)
}

// release releases a read slot.
func (g *admissionGate) release() {
	g.releaseLane(laneRead)
}

// acquireLane waits for a slot in lane. It returns ctx's error if ctx ends
// first, holding no slot; a slot granted to a caller that has meanwhile
// given up is handed on.
func (g *admissionGate) acquireLane(ctx context.Context, lane admissionLane) error {
	// An operation that has already given up never takes a slot.
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	now := g.now()
	g.seq++
	w := &admissionWaiter{
		lane:     lane,
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
			g.releaseLane(lane)
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
			g.putLocked(lane)
			g.dispatchLocked()
		} else {
			g.removeLocked(w)
			g.reportQueuedLocked(w.priority)
		}
		g.mu.Unlock()
		return ctx.Err()
	}
}

func (g *admissionGate) releaseLane(lane admissionLane) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.putLocked(lane)
	g.dispatchLocked()
}

// dispatchLocked grants free slots to the best waiters whose lane has room.
// A waiter whose lane is full does not hold back one behind it in another
// lane: a queued write never delays a read while a read slot is free.
func (g *admissionGate) dispatchLocked() {
	if g.inUse >= g.capacity || len(g.waiting) == 0 {
		return
	}
	now := g.now()
	for g.inUse < g.capacity && len(g.waiting) > 0 {
		best := -1
		for i, candidate := range g.waiting {
			if !g.laneHasRoomLocked(candidate.lane) {
				continue
			}
			if best < 0 || candidate.before(g.waiting[best], now) {
				best = i
			}
		}
		if best < 0 {
			return
		}
		w := g.waiting[best]
		g.waiting = append(g.waiting[:best], g.waiting[best+1:]...)
		g.takeLocked(w.lane)
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
