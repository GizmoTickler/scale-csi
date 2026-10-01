package truenas

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// acquireAsync starts a waiter and returns a channel that receives once it is
// admitted.
func acquireAsync(t *testing.T, g *admissionGate, ctx context.Context) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- g.acquire(ctx) }()
	// Let it enqueue.
	require.Eventually(t, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return len(g.waiting) > 0 || g.inUse > 0
	}, time.Second, time.Millisecond)
	return done
}

func waitQueued(t *testing.T, g *admissionGate, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return len(g.waiting) == n
	}, time.Second, time.Millisecond)
}

func TestAdmissionGateAdmitsByPriorityThenOperationAgeThenArrival(t *testing.T) {
	g := newAdmissionGate(1)
	require.NoError(t, g.acquire(context.Background())) // hold the only slot
	base := time.Now()
	type entry struct {
		name string
		ctx  context.Context
	}
	entries := []entry{
		{"delete", WithPriority(context.Background(), PriorityDelete)},
		{"default-young", WithOperationStart(context.Background(), base.Add(2*time.Second))},
		{"default-old", WithOperationStart(context.Background(), base)},
		{"attach", WithPriority(context.Background(), PriorityAttach)},
		{"default-unstamped-first", context.Background()},
		{"default-unstamped-second", context.Background()},
	}
	admitted := make(chan string, len(entries))
	for i, e := range entries {
		go func(e entry) {
			if g.acquire(e.ctx) == nil {
				admitted <- e.name
			}
		}(e)
		waitQueued(t, g, i+1)
	}
	order := make([]string, 0, len(entries))
	for range entries {
		g.release()
		order = append(order, <-admitted)
	}
	// The unstamped waiters take their arrival time as their operation start:
	// after default-old, before default-young (which starts 2 s in the future).
	assert.Equal(t, []string{"attach", "default-old", "default-unstamped-first", "default-unstamped-second", "default-young", "delete"}, order)
}

func TestAdmissionGateNeverExceedsCapacity(t *testing.T) {
	g := newAdmissionGate(3)
	var mu sync.Mutex
	inside, peak := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, g.acquire(context.Background()))
			mu.Lock()
			inside++
			if inside > peak {
				peak = inside
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			inside--
			mu.Unlock()
			g.release()
		}()
	}
	wg.Wait()
	assert.LessOrEqual(t, peak, 3)
	assert.Zero(t, g.inFlight())
}

func TestAdmissionGateCancellationNeverLeaksASlot(t *testing.T) {
	g := newAdmissionGate(1)
	require.NoError(t, g.acquire(context.Background()))

	// Canceled while queued: it leaves the queue and holds nothing.
	ctx, cancel := context.WithCancel(context.Background())
	done := acquireAsync(t, g, ctx)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	waitQueued(t, g, 0)

	// A later waiter still gets the slot when it is released.
	next := acquireAsync(t, g, context.Background())
	g.release()
	require.NoError(t, <-next)
	g.release()
	assert.Zero(t, g.inFlight())

	// An already-canceled context never takes a slot, not even a free one.
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	for i := 0; i < 50; i++ {
		require.ErrorIs(t, g.acquire(canceled), context.Canceled)
	}
	require.NoError(t, g.acquire(context.Background()))
	assert.Equal(t, 1, g.inFlight(), "no slot leaked to a canceled caller")
	g.release()
}

// With operation ages, a burst of operations that each make several calls
// completes in arrival order: the first one is done after its own calls, not
// after everyone's. Without them every operation would finish near the end.
func TestAdmissionGateCompletesABurstInArrivalOrder(t *testing.T) {
	const ops, calls = 6, 4
	g := newAdmissionGate(1)
	base := time.Now()
	var mu sync.Mutex
	var finished []int
	var wg sync.WaitGroup
	require.NoError(t, g.acquire(context.Background())) // hold until all are queued
	for op := 0; op < ops; op++ {
		wg.Add(1)
		go func(op int) {
			defer wg.Done()
			ctx := WithOperationStart(context.Background(), base.Add(time.Duration(op)*time.Millisecond))
			for c := 0; c < calls; c++ {
				require.NoError(t, g.acquire(ctx))
				time.Sleep(200 * time.Microsecond)
				g.release()
			}
			mu.Lock()
			finished = append(finished, op)
			mu.Unlock()
		}(op)
	}
	waitQueued(t, g, ops)
	g.release()
	wg.Wait()
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, finished)
}
