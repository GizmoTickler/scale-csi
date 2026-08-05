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

func TestJobWaitT11UsesCallerDeadlineNotClientRPCBudget(t *testing.T) {
	client := newJobWaitTestClient(t)
	client.config.Timeout = 10 * time.Millisecond
	started := make(chan struct{})
	client.jobPollOnceOverride = func(ctx context.Context, _ int64) (bool, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		select {
		case <-time.After(30 * time.Millisecond):
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	callerCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := client.waitForJob(callerCtx, 111)
	require.NoError(t, err, "the job wait must use the caller's operation deadline, not ClientConfig.Timeout")
	assert.GreaterOrEqual(t, time.Since(start), 25*time.Millisecond)
	select {
	case <-started:
	default:
		t.Fatal("the job wait did not perform its initial poll")
	}
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
