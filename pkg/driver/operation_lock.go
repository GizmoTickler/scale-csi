package driver

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"
)

// lockMode is how an operation holds a lock key.
//
// Every key can be held exclusively. A volume's lock (volumeLockKey) can also
// be held in one of two shared modes, so two kinds of operation on one volume
// that cannot affect each other no longer turn each other away:
//
//   - lockAttach: ControllerPublishVolume and ControllerUnpublishVolume. They
//     change the volume's share, its backend allowlist and its publication
//     records.
//   - lockData: CreateSnapshot, DeleteSnapshot and a volume-to-volume clone's
//     source. They read the volume's dataset and create or destroy its
//     snapshots; they never touch its share, allowlist or records.
//
// An attach and a data holder may hold one volume at once. Two attach holders
// may not: strict fencing decides each grant from the records and allowlist
// the previous one left, so publishes and unpublishes of a volume stay
// serialized exactly as before. Two data holders may not either (they were
// serialized before and nothing asks otherwise). Exclusive conflicts with
// everything: delete, expand, promote, modify, create, the startup and
// background reconcilers.
//
// Every acquisition in every mode tells a running startup diff that the key
// was taken (startupLockWatch), so the diff still sees every writer.
type lockMode int

const (
	lockExclusive lockMode = iota
	lockAttach
	lockData
)

func (m lockMode) String() string {
	switch m {
	case lockAttach:
		return "attach"
	case lockData:
		return "data"
	default:
		return "exclusive"
	}
}

// attachLockWait bounds how long an attach-class operation waits for a
// conflicting holder of its volume lock before it returns Aborted. It is
// short of the attacher's 120 s timeout, and long enough to cover a typical
// conflicting operation (a publish of the same volume, an expand), so the
// attacher no longer backs off for longer than the conflict lasted.
//
// At most one attach-class operation waits per volume: any further one
// returns Aborted at once. Without that cap, an RWX volume with ten or more
// VolumeAttachments in flight would park every attacher worker on one key
// and stall attaches cluster-wide. One waiter costs at most one worker per
// contended volume, and it waits for a release, not for the bound: behind
// another publish or unpublish that is a few seconds. A shorter wait behind
// attach-class holders would only turn that into an Aborted and the
// attacher's backoff, which is what the wait is there to avoid.
var attachLockWait = 8 * time.Second

// heldLock is one key's holders and waiters. released is closed, and
// replaced, whenever a holder lets go or a waiter gives up, so a waiter
// re-checks without polling.
//
// An exclusive waiter (the startup worker; nothing else waits exclusively)
// holds back new shared holders while it waits: otherwise attach and data
// holders that overlap one another could keep the key from ever being free
// for it. Shared holders already in keep their hold; the waiter's own wait
// is bounded.
type heldLock struct {
	exclusive bool
	attach    bool
	data      bool
	released  chan struct{}

	attachWaiting    bool
	exclusiveWaiting int
}

func (h *heldLock) empty() bool { return !h.exclusive && !h.attach && !h.data }

// unused is whether the entry can go: no holder and no waiter.
func (h *heldLock) unused() bool {
	return h.empty() && !h.attachWaiting && h.exclusiveWaiting == 0
}

// admits is whether mode can be taken alongside the current holders.
func (h *heldLock) admits(mode lockMode) bool {
	if h.exclusive {
		return false
	}
	switch mode {
	case lockAttach:
		return !h.attach && h.exclusiveWaiting == 0
	case lockData:
		return !h.data && h.exclusiveWaiting == 0
	default:
		return h.empty()
	}
}

func (h *heldLock) set(mode lockMode, held bool) {
	switch mode {
	case lockAttach:
		h.attach = held
	case lockData:
		h.data = held
	default:
		h.exclusive = held
	}
}

func (h *heldLock) holds(mode lockMode) bool {
	switch mode {
	case lockAttach:
		return h.attach
	case lockData:
		return h.data
	default:
		return h.exclusive
	}
}

// signal wakes the waiters to re-check.
func (h *heldLock) signal() {
	close(h.released)
	h.released = make(chan struct{})
}

// operationLockTable is the driver's per-key operation locks.
type operationLockTable struct {
	mu   sync.Mutex
	held map[string]*heldLock
}

func (t *operationLockTable) entryLocked(key string) *heldLock {
	if t.held == nil {
		t.held = make(map[string]*heldLock)
	}
	h, ok := t.held[key]
	if !ok {
		h = &heldLock{released: make(chan struct{})}
		t.held[key] = h
	}
	return h
}

// tryAcquire takes key in mode if it is admitted now.
func (t *operationLockTable) tryAcquire(key string, mode lockMode) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.entryLocked(key)
	if !h.admits(mode) {
		return false
	}
	h.set(mode, true)
	return true
}

// acquireOrPark takes key in mode, or registers the caller as a waiter and
// returns the channel to wait on. A second attach-class waiter is refused
// (parked false, no channel).
func (t *operationLockTable) acquireOrPark(key string, mode lockMode) (acquired, parked bool, released <-chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.entryLocked(key)
	if h.admits(mode) {
		h.set(mode, true)
		return true, false, nil
	}
	switch mode {
	case lockAttach:
		if h.attachWaiting {
			return false, false, nil
		}
		h.attachWaiting = true
	case lockExclusive:
		h.exclusiveWaiting++
	case lockData:
	}
	return false, true, h.released
}

// retryParked is a waiter's re-check: it takes key in mode and stops waiting
// if admitted, or returns the channel to wait on next.
func (t *operationLockTable) retryParked(key string, mode lockMode) (acquired bool, released <-chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.entryLocked(key)
	if mode == lockExclusive {
		// Its own pending flag must not hold it back.
		if !h.empty() {
			return false, h.released
		}
	} else if !h.admits(mode) {
		return false, h.released
	}
	t.unparkLocked(h, mode)
	h.set(mode, true)
	return true, nil
}

// giveUp ends a wait that did not acquire.
func (t *operationLockTable) giveUp(key string, mode lockMode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h, ok := t.held[key]
	if !ok {
		return
	}
	t.unparkLocked(h, mode)
	h.signal() // a shared waiter held back by this one re-checks
	if h.unused() {
		delete(t.held, key)
	}
}

func (t *operationLockTable) unparkLocked(h *heldLock, mode lockMode) {
	switch mode {
	case lockAttach:
		h.attachWaiting = false
	case lockExclusive:
		if h.exclusiveWaiting > 0 {
			h.exclusiveWaiting--
		}
	case lockData:
	}
}

func (t *operationLockTable) release(key string, mode lockMode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h, ok := t.held[key]
	if !ok || !h.holds(mode) {
		return
	}
	h.set(mode, false)
	h.signal()
	if h.unused() {
		delete(t.held, key)
	}
}

// keys is every key held in any mode, sorted. A key held in a shared mode
// carries its modes, as "volume:x(attach+data)".
func (t *operationLockTable) keys() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.held))
	for key, h := range t.held {
		if h.empty() {
			continue
		}
		var modes []string
		if h.attach {
			modes = append(modes, lockAttach.String())
		}
		if h.data {
			modes = append(modes, lockData.String())
		}
		if len(modes) > 0 {
			key += "(" + strings.Join(modes, "+") + ")"
		}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// heldKeys is every key held in any mode, bare and sorted.
func (t *operationLockTable) heldKeys() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.held))
	for key, h := range t.held {
		if !h.empty() {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// acquireOperationLock takes key exclusively. It returns false if the key is
// held in any mode.
func (d *Driver) acquireOperationLock(key string) bool {
	return d.acquireOperationLockMode(key, lockExclusive)
}

// acquireOperationLockMode takes key in mode without waiting.
func (d *Driver) acquireOperationLockMode(key string, mode lockMode) bool {
	ok := d.operationLocks.tryAcquire(key, mode)
	if ok {
		d.operationLockTaken(key)
	}
	return ok
}

// operationLockTaken tells a running startup diff that key's lock was taken
// (startupLockWatch), in whatever mode.
func (d *Driver) operationLockTaken(key string) {
	if watch := d.startupLockWatch.Load(); watch != nil {
		watch.touch(key)
	}
}

// acquireOperationLockWait is acquireOperationLock that waits up to wait for
// the key to be free. It returns false if the key is still held when wait
// runs out or ctx ends.
func (d *Driver) acquireOperationLockWait(ctx context.Context, key string, wait time.Duration) bool {
	return d.acquireOperationLockModeWait(ctx, key, lockExclusive, wait)
}

// acquireOperationLockModeWait takes key in mode, waiting up to wait for the
// conflicting holders to let go. An attach-class caller that finds another
// attach-class caller already waiting on key returns false at once.
func (d *Driver) acquireOperationLockModeWait(ctx context.Context, key string, mode lockMode, wait time.Duration) bool {
	if wait <= 0 {
		return d.acquireOperationLockMode(key, mode)
	}
	acquired, parked, released := d.operationLocks.acquireOrPark(key, mode)
	if acquired {
		d.operationLockTaken(key)
		return true
	}
	if !parked {
		return false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-released:
		case <-timer.C:
			d.operationLocks.giveUp(key, mode)
			return false
		case <-ctx.Done():
			d.operationLocks.giveUp(key, mode)
			return false
		}
		if acquired, released = d.operationLocks.retryParked(key, mode); acquired {
			d.operationLockTaken(key)
			return true
		}
	}
}

// releaseOperationLock releases an exclusive hold of key.
func (d *Driver) releaseOperationLock(key string) {
	d.operationLocks.release(key, lockExclusive)
}

// releaseOperationLockMode releases a hold of key in mode.
func (d *Driver) releaseOperationLockMode(key string, mode lockMode) {
	d.operationLocks.release(key, mode)
}

// heldOperationLocks is every key held in any mode, bare and sorted.
func (d *Driver) heldOperationLocks() []string {
	return d.operationLocks.heldKeys()
}

// describedOperationLocks is heldOperationLocks with each shared hold's
// modes, for the debug endpoint.
func (d *Driver) describedOperationLocks() []string {
	return d.operationLocks.keys()
}
