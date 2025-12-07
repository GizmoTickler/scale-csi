package driver

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
