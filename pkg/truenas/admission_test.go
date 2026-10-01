package truenas

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

// Aging bounds how long a lower class waits: a delete that has queued for two
// aging steps ranks with attach, and as the older operation goes first.
func TestAdmissionGateAgesLowerClasses(t *testing.T) {
	g := newAdmissionGate(1)
	now := time.Unix(1000, 0)
	g.now = func() time.Time { return now }
	require.NoError(t, g.acquire(context.Background()))
	admitted := make(chan string, 2)
	go func() {
		if g.acquire(WithPriority(context.Background(), PriorityDelete)) == nil {
			admitted <- "delete"
		}
	}()
	waitQueued(t, g, 1)
	now = now.Add(2*agingStep + time.Millisecond)
	go func() {
		if g.acquire(WithPriority(context.Background(), PriorityAttach)) == nil {
			admitted <- "attach"
		}
	}()
	waitQueued(t, g, 2)
	g.release()
	assert.Equal(t, "delete", <-admitted, "aged to attach rank, and its operation is older")
	g.release()
	assert.Equal(t, "attach", <-admitted)
	g.release()

	// Without the wait, the attach goes first.
	now = now.Add(time.Hour)
	require.NoError(t, g.acquire(context.Background()))
	go func() {
		if g.acquire(WithPriority(context.Background(), PriorityDelete)) == nil {
			admitted <- "delete"
		}
	}()
	waitQueued(t, g, 1)
	go func() {
		if g.acquire(WithPriority(context.Background(), PriorityAttach)) == nil {
			admitted <- "attach"
		}
	}()
	waitQueued(t, g, 2)
	g.release()
	assert.Equal(t, "attach", <-admitted)
	g.release()
	assert.Equal(t, "delete", <-admitted)
	g.release()
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
