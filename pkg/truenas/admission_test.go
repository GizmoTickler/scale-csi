package truenas

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// acquireAsync starts a waiter and returns a channel that receives its result.
// It returns once the waiter is queued or admitted.
func acquireAsync(t *testing.T, g *admissionGate, ctx context.Context) <-chan error {
	t.Helper()
	g.mu.Lock()
	queued, held := len(g.waiting), g.inUse
	g.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- g.acquire(ctx) }()
	require.Eventually(t, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return len(g.waiting) > queued || g.inUse > held
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

// A slot granted at the instant its waiter gives up is never kept by the
// waiter, whichever way its select falls.
func TestAdmissionGateGrantRacingCancellationNeverLeaks(t *testing.T) {
	for i := 0; i < 200; i++ {
		g := newAdmissionGate(1)
		require.NoError(t, g.acquire(context.Background()))
		ctx, cancel := context.WithCancel(context.Background())
		done := acquireAsync(t, g, ctx)
		// Cancel, then grant, both under the lock: the waiter wakes on the
		// cancellation and finds itself already granted.
		g.mu.Lock()
		cancel()
		g.inUse--
		g.dispatchLocked()
		g.mu.Unlock()
		require.ErrorIs(t, <-done, context.Canceled)
		require.Zero(t, g.inFlight(), "iteration %d: the granted slot was kept", i)
	}
}

// admitInOrder queues each waiter in turn (advancing the gate's clock by the
// given amount after each) behind one held slot, then releases one slot at a
// time and returns the order they were admitted in.
func admitInOrder(t *testing.T, g *admissionGate, now *time.Time, waiters []struct {
	name  string
	ctx   context.Context
	after time.Duration
}) []string {
	t.Helper()
	require.NoError(t, g.acquire(context.Background()))
	admitted := make(chan string, len(waiters))
	for i, w := range waiters {
		go func(name string, ctx context.Context) {
			if g.acquire(ctx) == nil {
				admitted <- name
			}
		}(w.name, w.ctx)
		waitQueued(t, g, i+1)
		*now = now.Add(w.after)
	}
	order := make([]string, 0, len(waiters))
	for range waiters {
		g.release()
		order = append(order, <-admitted)
	}
	g.release()
	return order
}

// Aging bounds how long a delete waits behind default work: after one aging
// step it ranks with the default class, where its older operation goes first.
func TestAdmissionGateAgesDeletesIntoTheDefaultClass(t *testing.T) {
	g := newAdmissionGate(1)
	now := time.Unix(1000, 0)
	g.now = func() time.Time { return now }
	type waiter = struct {
		name  string
		ctx   context.Context
		after time.Duration
	}
	order := admitInOrder(t, g, &now, []waiter{
		{"delete", WithPriority(context.Background(), PriorityDelete), agingStep + time.Millisecond},
		{"default", context.Background(), 0},
	})
	assert.Equal(t, []string{"delete", "default"}, order, "aged into the default class, and its operation is older")

	// Without the wait, the default goes first.
	now = now.Add(time.Hour)
	order = admitInOrder(t, g, &now, []waiter{
		{"delete", WithPriority(context.Background(), PriorityDelete), 0},
		{"default", context.Background(), 0},
	})
	assert.Equal(t, []string{"default", "delete"}, order)
}

// Nothing ages into the attach class: however long default and delete work
// has waited, and however much older its operations are, a publish goes
// first. Otherwise, under overload, every operation that started before a
// publish would be admitted ahead of it.
func TestAdmissionGateNeverAgesAnythingPastAttach(t *testing.T) {
	g := newAdmissionGate(1)
	now := time.Unix(1000, 0)
	g.now = func() time.Time { return now }
	type waiter = struct {
		name  string
		ctx   context.Context
		after time.Duration
	}
	old := now.Add(-time.Hour)
	order := admitInOrder(t, g, &now, []waiter{
		{"delete", WithOperationStart(WithPriority(context.Background(), PriorityDelete), old), 0},
		{"default", WithOperationStart(context.Background(), old), 10 * agingStep},
		{"attach", WithPriority(context.Background(), PriorityAttach), 0},
	})
	assert.Equal(t, []string{"attach", "delete", "default"}, order)
}

func TestAdmissionGateUnbalancedReleasePanics(t *testing.T) {
	g := newAdmissionGate(2)
	assert.Panics(t, g.release)
}

func TestAdmissionGateReportsWaitAndQueueByClass(t *testing.T) {
	var mu sync.Mutex
	waited := map[string]int{}
	queued := map[string]int{}
	g := newAdmissionGateWithMetrics(1, AdmissionMetrics{
		Waited: func(class string, _ float64) { mu.Lock(); waited[class]++; mu.Unlock() },
		Queued: func(class string, n int) { mu.Lock(); queued[class] = n; mu.Unlock() },
	})
	require.NoError(t, g.acquire(context.Background()))
	done := acquireAsync(t, g, WithPriority(context.Background(), PriorityDelete))
	mu.Lock()
	assert.Equal(t, 1, queued["delete"])
	mu.Unlock()
	g.release()
	require.NoError(t, <-done)
	g.release()
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 0, queued["delete"])
	assert.Equal(t, 1, waited["default"])
	assert.Equal(t, 1, waited["delete"])
}

// The client admits its calls through the gate with the caller's context: with
// one slot held, a delete-class call queued before an attach-class call reaches
// TrueNAS after it.
func TestClientCallsAreAdmittedByTheCallersClass(t *testing.T) {
	mock := newMockWSServer()
	order := make(chan string, 4)
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "system.info":
				resp.Result = map[string]interface{}{"version": "TrueNAS-SCALE-25.10.0", "hostname": "truenas-test"}
			default:
				// Only the two calls under test; the client's own
				// background calls (job subscription) are not ordered here.
				if strings.HasSuffix(req.Method, ".class.query") {
					order <- req.Method
				}
				resp.Result = []interface{}{}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()

	wsURL := strings.Replace(server.URL, "http://", "", 1)
	parts := strings.Split(wsURL, ":")
	port := 80
	if len(parts) > 1 {
		_, _ = fmt.Sscanf(parts[1], "%d", &port)
	}
	client, err := NewClient(&ClientConfig{
		Host: parts[0], Port: port, Protocol: "http", APIKey: "test-api-key",
		Timeout: 5 * time.Second, ConnectTimeout: 5 * time.Second, MaxConnections: 1, MaxConcurrentReqs: 1,
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	require.NoError(t, client.semaphore.acquire(context.Background()))
	errs := make(chan error, 2)
	go func() {
		_, err := client.Call(WithPriority(context.Background(), PriorityDelete), "delete.class.query")
		errs <- err
	}()
	waitQueued(t, client.semaphore, 1)
	go func() {
		_, err := client.Call(WithPriority(context.Background(), PriorityAttach), "attach.class.query")
		errs <- err
	}()
	waitQueued(t, client.semaphore, 2)
	client.semaphore.release()
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	assert.Equal(t, "attach.class.query", <-order)
	assert.Equal(t, "delete.class.query", <-order)
}

// flipContext passes the gate's entry check, then reports itself canceled,
// while its Done channel never closes: the waiter can only learn of the
// cancellation after its slot is granted.
type flipContext struct {
	context.Context
	checks atomic.Int32
}

func (c *flipContext) Err() error {
	if c.checks.Add(1) == 1 {
		return nil
	}
	return context.Canceled
}

func TestAdmissionGateGrantToAnOperationThatGaveUpIsReleased(t *testing.T) {
	waits := 0
	g := newAdmissionGateWithMetrics(1, AdmissionMetrics{
		Waited: func(string, float64) { waits++ },
	})
	err := g.acquire(&flipContext{Context: context.Background()})
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, g.inFlight(), "a slot granted to a caller that has given up is released")
	assert.Zero(t, waits, "a released grant is not a wait")
	require.NoError(t, g.acquire(context.Background()))
	g.release()
}

// A waiter that gives up leaves the queue gauge, so sidecar timeouts do not
// leave phantom waiters behind.
func TestAdmissionGateCanceledWaiterLeavesTheQueueGauge(t *testing.T) {
	var mu sync.Mutex
	queued := map[string]int{}
	waits := 0
	g := newAdmissionGateWithMetrics(1, AdmissionMetrics{
		Waited: func(string, float64) { mu.Lock(); waits++; mu.Unlock() },
		Queued: func(class string, n int) { mu.Lock(); queued[class] = n; mu.Unlock() },
	})
	require.NoError(t, g.acquire(context.Background()))
	ctx, cancel := context.WithCancel(WithPriority(context.Background(), PriorityDelete))
	done := acquireAsync(t, g, ctx)
	mu.Lock()
	assert.Equal(t, 1, queued["delete"])
	mu.Unlock()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	mu.Lock()
	assert.Equal(t, 0, queued["delete"], "the canceled waiter left the gauge")
	assert.Equal(t, 1, waits, "only the held slot's (zero) wait was recorded")
	mu.Unlock()
	g.release()
}
