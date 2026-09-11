package truenas

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newJobWaitTestClient(t *testing.T) *Client {
	t.Helper()
	client := &Client{
		config:                 &ClientConfig{},
		dispatcher:             newJobDispatcher(),
		jobSubscriptionChanged: make(chan struct{}),
		semaphore:              make(chan struct{}, 1),
		jobWaitPollInterval:    5 * time.Millisecond,
		jobWaitSafetyInterval:  30 * time.Millisecond,
	}
	t.Cleanup(client.dispatcher.Stop)
	return client
}

func subscribedTestConnection(generation uint64) *Connection {
	conn := NewConnection(0, &ClientConfig{})
	conn.mu.Lock()
	conn.generation = generation
	conn.stopped = false
	conn.conn.Store(&websocket.Conn{})
	conn.jobSubState.Store(generation<<1 | 1)
	conn.mu.Unlock()
	return conn
}

func offerJobTestEvent(t *testing.T, dispatcher *jobDispatcher, jobID int64, state string) {
	t.Helper()
	params, err := json.Marshal(map[string]interface{}{
		"msg":        "changed",
		"collection": "core.get_jobs",
		"fields": map[string]interface{}{
			"id":    jobID,
			"state": state,
		},
	})
	require.NoError(t, err)
	dispatcher.offer(params)
}

func TestJobWaitT1ImmediateTerminalUsesInitialPoll(t *testing.T) {
	client := newJobWaitTestClient(t)
	var polls atomic.Int32
	client.jobPollOnceOverride = func(context.Context, int64) (bool, error) {
		polls.Add(1)
		return true, nil
	}

	require.NoError(t, client.waitForJob(context.Background(), 101))
	assert.Equal(t, int32(1), polls.Load())
}

func TestJobWaitT2SubscribedTerminalEventReturnsPromptly(t *testing.T) {
	client := newJobWaitTestClient(t)
	client.pool = []*Connection{subscribedTestConnection(1)}
	initialPoll := make(chan struct{})
	client.jobPollOnceOverride = func(context.Context, int64) (bool, error) {
		select {
		case <-initialPoll:
		default:
			close(initialPoll)
		}
		return false, nil
	}

	errCh := make(chan error, 1)
	go func() { errCh <- client.waitForJob(context.Background(), 102) }()
	<-initialPoll
	start := time.Now()
	offerJobTestEvent(t, client.dispatcher, 102, "SUCCESS")

	select {
	case err := <-errCh:
		require.NoError(t, err)
		assert.Less(t, time.Since(start), 100*time.Millisecond)
	case <-time.After(time.Second):
		t.Fatal("terminal notification did not wake waiter")
	}
}

func TestJobWaitT3TerminalDisplacesQueuedNonTerminal(t *testing.T) {
	waiter := make(chan jobEvent, 1)
	deliverJobEvent(waiter, jobEvent{jobID: 103, state: "RUNNING"})
	deliverJobEvent(waiter, jobEvent{jobID: 103, state: "SUCCESS"})

	ev := <-waiter
	assert.Equal(t, "SUCCESS", ev.state)
	assert.Empty(t, waiter)
}

func TestJobWaitT4ReconnectResumesPollingAndPostSubscribePolls(t *testing.T) {
	client := newJobWaitTestClient(t)
	conn := subscribedTestConnection(1)
	client.pool = []*Connection{conn}
	pollObserved := make(chan int32, 3)
	var polls atomic.Int32
	client.jobPollOnceOverride = func(context.Context, int64) (bool, error) {
		count := polls.Add(1)
		pollObserved <- count
		return count == 3, nil
	}

	errCh := make(chan error, 1)
	go func() { errCh <- client.waitForJob(context.Background(), 104) }()
	require.Equal(t, int32(1), <-pollObserved)

	conn.mu.Lock()
	conn.stopped = true
	conn.conn.Store(nil)
	conn.mu.Unlock()
	require.Equal(t, int32(2), <-pollObserved, "the next local tick must resume pure polling")

	conn.mu.Lock()
	conn.generation = 2
	conn.stopped = false
	conn.conn.Store(&websocket.Conn{})
	conn.jobSubState.Store(2<<1 | 1)
	conn.mu.Unlock()
	client.signalJobSubscription()
	require.Equal(t, int32(3), <-pollObserved, "every subscribed generation must trigger a post-subscribe poll")

	require.NoError(t, <-errCh)
}

func TestJobWaitT5StaleGenerationCannotPublishSubscription(t *testing.T) {
	conn := NewConnection(0, &ClientConfig{})
	conn.mu.Lock()
	conn.generation = 2
	conn.stopped = false
	conn.authenticated = true
	conn.conn.Store(&websocket.Conn{})
	conn.jobSubState.Store(2 << 1)
	conn.mu.Unlock()

	_, err := conn.callWithGeneration(context.Background(), 1, false, "core.subscribe", "core.get_jobs")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTransportFailure)
	assert.False(t, conn.markJobSubscribed(1))
	assert.False(t, conn.jobsSubscribed())
}

func TestJobWaitT6SaturationConvergesViaSafetyPoll(t *testing.T) {
	client := newJobWaitTestClient(t)
	client.pool = []*Connection{subscribedTestConnection(1)}
	var polls atomic.Int32
	client.jobPollOnceOverride = func(context.Context, int64) (bool, error) {
		return polls.Add(1) >= 2, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- client.waitForJob(ctx, 106) }()

	for i := 0; i < jobEventBufferSize*8; i++ {
		offerJobTestEvent(t, client.dispatcher, int64(1000+i), "RUNNING")
	}
	offerJobTestEvent(t, client.dispatcher, 106, "RUNNING")

	require.NoError(t, <-errCh)
	assert.GreaterOrEqual(t, polls.Load(), int32(2), "the slow safety poll must terminate a waiter even when events saturate")
}

func TestJobWaitT7ShutdownClosesWaitersWithoutClosingEvents(t *testing.T) {
	client := newJobWaitTestClient(t)
	initialPoll := make(chan struct{})
	client.jobPollOnceOverride = func(context.Context, int64) (bool, error) {
		select {
		case <-initialPoll:
		default:
			close(initialPoll)
		}
		return false, nil
	}
	errCh := make(chan error, 1)
	go func() { errCh <- client.waitForJob(context.Background(), 107) }()
	<-initialPoll

	client.dispatcher.Stop()
	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrTransportFailure)
	case <-time.After(time.Second):
		t.Fatal("shutdown did not close the registered waiter")
	}

	afterStop := client.dispatcher.register(107)
	_, ok := <-afterStop
	assert.False(t, ok, "register-after-stop must return a pre-closed channel")
	assert.NotPanics(t, func() {
		offerJobTestEvent(t, client.dispatcher, 107, "SUCCESS")
	}, "the shared events channel must remain open")
}

func TestJobWaitT8ConstructorFailureStopsDispatcher(t *testing.T) {
	dispatchersBefore := jobDispatcherGoroutines()
	_, err := NewClient(&ClientConfig{
		Host:                  "127.0.0.1",
		Port:                  1,
		Protocol:              "http",
		APIKey:                "test",
		ConnectTimeout:        10 * time.Millisecond,
		RetryInterval:         time.Millisecond,
		MaxRetries:            1,
		MaxConnections:        1,
		APIRetryMaxAttempts:   1,
		APIRetryBackoffFactor: 1,
	})
	require.Error(t, err)

	require.Eventually(t, func() bool {
		return jobDispatcherGoroutines() <= dispatchersBefore
	}, time.Second, 10*time.Millisecond, "constructor error leaked jobDispatcher.run")
}

func jobDispatcherGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "(*jobDispatcher).run")
}

func TestJobWaitT9PurePollParity(t *testing.T) {
	client := newJobWaitTestClient(t)
	client.jobWaitPollInterval = 10 * time.Millisecond
	client.jobWaitSafetyInterval = time.Hour
	var polls atomic.Int32
	start := time.Now()
	client.jobPollOnceOverride = func(context.Context, int64) (bool, error) {
		return polls.Add(1) >= 3, nil
	}

	require.NoError(t, client.waitForJob(context.Background(), 109))
	assert.Equal(t, int32(3), polls.Load())
	assert.GreaterOrEqual(t, time.Since(start), 18*time.Millisecond, "pure polling should retain the existing interval cadence")
}

func TestJobWaitT10SemaphoreAccounting(t *testing.T) {
	client := newJobWaitTestClient(t)
	client.pool = []*Connection{subscribedTestConnection(1)}
	pollHoldingSlot := make(chan struct{}, 1)
	releasePoll := make(chan struct{})
	client.jobPollOnceOverride = func(ctx context.Context, _ int64) (bool, error) {
		select {
		case client.semaphore <- struct{}{}:
		case <-ctx.Done():
			return false, ctx.Err()
		}
		pollHoldingSlot <- struct{}{}
		<-releasePoll
		<-client.semaphore
		return false, nil
	}

	errCh := make(chan error, 1)
	go func() { errCh <- client.waitForJob(context.Background(), 110) }()
	<-pollHoldingSlot
	assert.Len(t, client.semaphore, 1, "pollJobOnce must take exactly one slot")
	close(releasePoll)
	require.Eventually(t, func() bool { return len(client.semaphore) == 0 }, time.Second, time.Millisecond)
	assert.Empty(t, client.semaphore, "a blocked subscribed waiter must hold zero slots")

	offerJobTestEvent(t, client.dispatcher, 110, "FAILED")
	err := <-errCh
	var terminalErr *jobTerminalError
	require.True(t, errors.As(err, &terminalErr))
	assert.Equal(t, "FAILED", terminalErr.state)
}

// TestWaitForJobUsesGetJobsDirectlyNotCoreJobWait is the N6 regression: a
// prior commit (2304911) routed waitForJob through core.job_wait, itself a
// server-side "job": true call, so callers ended up polling the WRAPPER job
// via the exact same core.get_jobs loop -- one extra RPC and one extra
// middleware job for zero fewer polls -- and get_jobs is already idempotent
// (see isIdempotentAPIMethod), so the switch bought nothing while making
// job_wait a new non-idempotent call that, on a reconnect mid-wait, would
// surface ErrAmbiguousResult and (for CopyDatasetFromSnapshotLocal) fire
// core.job_abort on a healthy long-running copy. Reverted: waitForJob must
// poll core.get_jobs for the caller's own job id directly, never touching
// core.job_wait.
func TestWaitForJobUsesGetJobsDirectlyNotCoreJobWait(t *testing.T) {
	mock := newMockWSServer()
	var jobWaitCalls atomic.Int32
	var getJobsCalls atomic.Int32
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
			case "core.job_wait":
				jobWaitCalls.Add(1)
				resp.Result = float64(999)
			case "core.get_jobs":
				getJobsCalls.Add(1)
				resp.Result = []interface{}{map[string]interface{}{"id": float64(41), "state": "SUCCESS"}}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()
	client := newSnapshotTestClient(t, server.URL)

	require.NoError(t, client.waitForJob(context.Background(), 41))
	assert.Zero(t, jobWaitCalls.Load(), "waitForJob must never call core.job_wait")
	assert.Equal(t, int32(1), getJobsCalls.Load(), "waitForJob must poll core.get_jobs for the target job directly")
}
