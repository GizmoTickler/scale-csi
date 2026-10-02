package driver

import (
	"context"
	"sort"
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
var attachLockWait = 8 * time.Second

// heldLock is one key's holders. released is closed, and replaced, whenever
// a holder lets go, so a waiter re-checks without polling.
type heldLock struct {
	exclusive bool
	attach    bool
	data      bool
	released  chan struct{}
}

func (h *heldLock) empty() bool { return !h.exclusive && !h.attach && !h.data }

// admits is whether mode can be taken alongside the current holders.
func (h *heldLock) admits(mode lockMode) bool {
	if h.exclusive {
		return false
	}
	switch mode {
	case lockAttach:
		return !h.attach
	case lockData:
		return !h.data
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

// operationLockTable is the driver's per-key operation locks.
type operationLockTable struct {
	mu   sync.Mutex
	held map[string]*heldLock
}

// tryAcquire takes key in mode if no holder conflicts. Otherwise it returns
// the channel closed at the next release, for a waiter.
func (t *operationLockTable) tryAcquire(key string, mode lockMode) (acquired bool, released <-chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.held == nil {
		t.held = make(map[string]*heldLock)
	}
	h, ok := t.held[key]
	if !ok {
		h = &heldLock{released: make(chan struct{})}
		t.held[key] = h
	}
	if !h.admits(mode) {
		return false, h.released
	}
	h.set(mode, true)
	return true, nil
}

func (t *operationLockTable) release(key string, mode lockMode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h, ok := t.held[key]
	if !ok || !h.holds(mode) {
		return
	}
	h.set(mode, false)
	close(h.released)
	if h.empty() {
		delete(t.held, key)
		return
	}
	h.released = make(chan struct{})
}

// keys is every key held in any mode, sorted.
func (t *operationLockTable) keys() []string {
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
	ok, _ := d.operationLocks.tryAcquire(key, mode)
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
// conflicting holders to let go.
func (d *Driver) acquireOperationLockModeWait(ctx context.Context, key string, mode lockMode, wait time.Duration) bool {
	ok, released := d.operationLocks.tryAcquire(key, mode)
	if ok {
		d.operationLockTaken(key)
		return true
	}
	if wait <= 0 {
		return false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-released:
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		}
		if ok, released = d.operationLocks.tryAcquire(key, mode); ok {
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

// heldOperationLocks is every key held in any mode, sorted.
func (d *Driver) heldOperationLocks() []string {
	return d.operationLocks.keys()
}
