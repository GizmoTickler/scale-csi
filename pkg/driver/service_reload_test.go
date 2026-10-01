package driver

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func TestServiceReloadDebouncer_SingleRequest(t *testing.T) {
	var reloadCount int32
	debouncer := NewServiceReloadDebouncer(100*time.Millisecond, func(ctx context.Context, service string) error {
		atomic.AddInt32(&reloadCount, 1)
		return nil
	})
	defer debouncer.Stop()

	ctx := context.Background()
	err := debouncer.RequestReload(ctx, "iscsitarget")
	if err != nil {
		t.Fatalf("RequestReload returned error: %v", err)
	}

	if count := atomic.LoadInt32(&reloadCount); count != 1 {
		t.Errorf("Expected 1 reload, got %d", count)
	}
}

func TestServiceReloadDebouncer_CoalescesRequests(t *testing.T) {
	var reloadCount int32
	var mu sync.Mutex
	var lastService string

	debouncer := NewServiceReloadDebouncer(200*time.Millisecond, func(ctx context.Context, service string) error {
		mu.Lock()
		lastService = service
		mu.Unlock()
		atomic.AddInt32(&reloadCount, 1)
		return nil
	})
	defer debouncer.Stop()

	ctx := context.Background()

	// Fire multiple requests quickly - they should be coalesced
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = debouncer.RequestReload(ctx, "iscsitarget")
		}()
		// Small delay between requests, but less than debounce window
		time.Sleep(20 * time.Millisecond)
	}

	wg.Wait()

	// Should have coalesced into 1 reload
	if count := atomic.LoadInt32(&reloadCount); count != 1 {
		t.Errorf("Expected 1 reload (coalesced), got %d", count)
	}

	mu.Lock()
	if lastService != "iscsitarget" {
		t.Errorf("Expected service 'iscsitarget', got '%s'", lastService)
	}
	mu.Unlock()
}

// TestServiceReloadDebouncer_SustainedStreamDoesNotStarve mirrors the starvation
// proof: a request stream faster than the debounce window must still fire
// reloads at ~window cadence (leading-window batching). Under the old pure
// trailing-edge debouncer every request reset the timer, so a 20ms-interval
// stream with a 50ms window fired ZERO reloads until the stream stopped.
func TestServiceReloadDebouncer_SustainedStreamDoesNotStarve(t *testing.T) {
	var reloadCount int32
	debouncer := NewServiceReloadDebouncer(50*time.Millisecond, func(ctx context.Context, service string) error {
		atomic.AddInt32(&reloadCount, 1)
		return nil
	})
	defer debouncer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	send := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = debouncer.RequestReload(ctx, "iscsitarget")
		}()
	}

	// Sustained stream: a request every 20ms for 600ms.
	send() // arm the leading window immediately
	deadline := time.NewTimer(600 * time.Millisecond)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
stream:
	for {
		select {
		case <-deadline.C:
			break stream
		case <-ticker.C:
			send()
		}
	}

	wg.Wait()

	// ~600ms / 50ms window => ~12 reloads; assert a slack-tolerant floor that a
	// starving (trailing-edge) debouncer cannot reach (it would fire exactly once,
	// after the stream stops).
	if count := atomic.LoadInt32(&reloadCount); count < 5 {
		t.Errorf("Expected sustained stream to fire >=5 reloads at ~window cadence, got %d", count)
	}
}

func TestServiceReloadDebouncer_SeparateServices(t *testing.T) {
	var reloadCount int32
	services := make(map[string]int)
	var mu sync.Mutex

	debouncer := NewServiceReloadDebouncer(100*time.Millisecond, func(ctx context.Context, service string) error {
		mu.Lock()
		services[service]++
		mu.Unlock()
		atomic.AddInt32(&reloadCount, 1)
		return nil
	})
	defer debouncer.Stop()

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = debouncer.RequestReload(ctx, "iscsitarget")
	}()

	go func() {
		defer wg.Done()
		_ = debouncer.RequestReload(ctx, "nfs")
	}()

	wg.Wait()

	// Each service should have been reloaded once
	if count := atomic.LoadInt32(&reloadCount); count != 2 {
		t.Errorf("Expected 2 reloads (one per service), got %d", count)
	}

	mu.Lock()
	if services["iscsitarget"] != 1 {
		t.Errorf("Expected iscsitarget reloaded 1 time, got %d", services["iscsitarget"])
	}
	if services["nfs"] != 1 {
		t.Errorf("Expected nfs reloaded 1 time, got %d", services["nfs"])
	}
	mu.Unlock()
}

func TestServiceReloadDebouncer_ContextCancellation(t *testing.T) {
	debouncer := NewServiceReloadDebouncer(500*time.Millisecond, func(ctx context.Context, service string) error {
		return nil
	})
	defer debouncer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := debouncer.RequestReload(ctx, "iscsitarget")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
}

func TestServiceReloadDebouncer_Stop(t *testing.T) {
	debouncer := NewServiceReloadDebouncer(1*time.Second, func(ctx context.Context, service string) error {
		return nil
	})

	ctx := context.Background()

	// Start a request that would wait for the debounce
	var wg sync.WaitGroup
	var err error
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = debouncer.RequestReload(ctx, "iscsitarget")
	}()

	// Give it time to register the request
	time.Sleep(50 * time.Millisecond)

	// Stop the debouncer
	debouncer.Stop()

	// Wait for the request to complete
	wg.Wait()

	// Should have received a cancellation error
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled after Stop, got %v", err)
	}
}

// Regression: a batch whose callers ALL cancel before the window fires must not
// leave a stale timer behind. With the leading-window arm-only-if-nil rule, a
// dead non-nil timer would prevent every future request from arming, starving
// reloads for that service permanently (callers block until their own context
// deadline).
func TestServiceReloadDebouncer_FullyCancelledBatchDoesNotStarveNextBatch(t *testing.T) {
	var reloadCount atomic.Int64
	debouncer := NewServiceReloadDebouncer(50*time.Millisecond, func(ctx context.Context, service string) error {
		reloadCount.Add(1)
		return nil
	})
	defer debouncer.Stop()

	// Batch 1: sole caller cancels before the window fires.
	cancelCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- debouncer.RequestReload(cancelCtx, "iscsitarget") }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled for canceled caller, got %v", err)
	}
	// Let the orphaned timer fire against the empty batch.
	time.Sleep(100 * time.Millisecond)

	// Batch 2: a fresh request must complete within roughly one window, not
	// hang until its context deadline.
	ctx, ctxCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ctxCancel()
	start := time.Now()
	if err := debouncer.RequestReload(ctx, "iscsitarget"); err != nil {
		t.Fatalf("post-cancellation batch failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("post-cancellation batch took %v; stale timer starvation", elapsed)
	}
	if got := reloadCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 reload for batch 2, got %d", got)
	}
}

// A debounced reload is admitted to TrueNAS as the work of the callers waiting
// on it: their most urgent class and their oldest operation start, with no
// deadline of its own that would count the wait for a request slot. Admitted
// as background work it queued behind every earlier operation's later calls,
// and under a burst its 30 s budget ran out before it got a slot (iSCSI
// multipath creates and fenced publishes wait on it).
func TestServiceReloadIsAdmittedAsItsCallers(t *testing.T) {
	type seen struct {
		priority    truenas.Priority
		start       time.Time
		stamped     bool
		hasDeadline bool
	}
	got := make(chan seen, 3) // never blocks a reload, however the callers batch
	d := NewServiceReloadDebouncer(200*time.Millisecond, func(ctx context.Context, _ string) error {
		start, stamped := truenas.OperationStartOf(ctx)
		_, hasDeadline := ctx.Deadline()
		got <- seen{truenas.PriorityOf(ctx), start, stamped, hasDeadline}
		return nil
	})
	defer d.Stop()

	base := time.Now().Add(-time.Minute)
	callers := []context.Context{
		truenas.WithOperationStart(context.Background(), base.Add(2*time.Second)),
		truenas.WithOperationStart(truenas.WithPriority(context.Background(), truenas.PriorityAttach), base.Add(5*time.Second)),
		truenas.WithOperationStart(truenas.WithPriority(context.Background(), truenas.PriorityDelete), base),
	}
	var wg sync.WaitGroup
	for _, ctx := range callers {
		wg.Add(1)
		go func(ctx context.Context) {
			defer wg.Done()
			assert.NoError(t, d.RequestReload(ctx, "iscsitarget"))
		}(ctx)
	}
	wg.Wait()
	s := <-got
	assert.Equal(t, truenas.PriorityAttach, s.priority, "the most urgent caller's class")
	assert.True(t, s.stamped)
	assert.True(t, s.start.Equal(base), "the oldest caller's operation start")
	assert.False(t, s.hasDeadline, "no budget that counts the wait for a slot")
}

// A caller that gives up no longer lends the reload its class: the reload is
// admitted as the work of the callers still waiting when it fires.
func TestServiceReloadClassComesFromTheCallersStillWaiting(t *testing.T) {
	got := make(chan truenas.Priority, 2)
	d := NewServiceReloadDebouncer(150*time.Millisecond, func(ctx context.Context, _ string) error {
		got <- truenas.PriorityOf(ctx)
		return nil
	})
	defer d.Stop()

	attachCtx, cancelAttach := context.WithCancel(truenas.WithPriority(context.Background(), truenas.PriorityAttach))
	attachDone := make(chan error, 1)
	go func() { attachDone <- d.RequestReload(attachCtx, "iscsitarget") }()
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- d.RequestReload(truenas.WithPriority(context.Background(), truenas.PriorityDelete), "iscsitarget")
	}()
	time.Sleep(30 * time.Millisecond)
	cancelAttach()
	assert.ErrorIs(t, <-attachDone, context.Canceled)
	assert.NoError(t, <-deleteDone)
	assert.Equal(t, truenas.PriorityDelete, <-got, "the attach caller left before the reload fired")
}
