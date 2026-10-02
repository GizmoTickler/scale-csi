package truenas

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdmissionLaneForMethod(t *testing.T) {
	for method, want := range map[string]admissionLane{
		"pool.dataset.query":                   laneRead,
		"nvmet.host_subsys.query":              laneRead,
		"pool.snapshot.get_instance":           laneRead,
		"nvmet.port.transport_address_choices": laneRead,
		"pool.dataset.attachments":             laneRead,
		"pool.dataset.update":                  laneWrite,
		"pool.snapshot.create":                 laneWrite,
		"sharing.nfs.update":                   laneWrite,
		"service.reload":                       laneWrite,
		"iscsi.target.create":                  laneWrite,
		"nvmet.host_subsys.create":             laneNVMetWrite,
		"nvmet.port_subsys.delete":             laneNVMetWrite,
		"nvmet.subsys.update":                  laneNVMetWrite,
	} {
		assert.Equal(t, want, laneForMethod(method), method)
	}
}

// Writes hold at most writeCapacity slots, and a queued write of a higher
// class does not hold back a read behind it while a slot is free for reads.
func TestAdmissionGateReadsAreNotQueuedBehindWrites(t *testing.T) {
	g := newAdmissionGateWithLanes(10, 4, AdmissionMetrics{})
	attach := WithPriority(context.Background(), PriorityAttach)
	for range 4 {
		require.NoError(t, g.acquireLane(attach, laneWrite))
	}
	queuedWrite := make(chan error, 1)
	go func() { queuedWrite <- g.acquireLane(attach, laneWrite) }()
	waitQueued(t, g, 1)
	ctx, cancel := context.WithTimeout(WithPriority(context.Background(), PriorityDelete), time.Second)
	defer cancel()
	require.NoError(t, g.acquireLane(ctx, laneRead), "a read waited behind a queued write")
	g.releaseLane(laneRead)
	select {
	case <-queuedWrite:
		t.Fatal("a fifth write was admitted")
	default:
	}
	g.releaseLane(laneWrite)
	require.NoError(t, <-queuedWrite)
	for range 4 {
		g.releaseLane(laneWrite)
	}
	assert.Zero(t, g.inFlight())
}

// Among the waiters whose lane has room, priority still decides.
func TestAdmissionGateLanesKeepPriorities(t *testing.T) {
	g := newAdmissionGateWithLanes(2, 1, AdmissionMetrics{})
	require.NoError(t, g.acquireLane(context.Background(), laneWrite))
	require.NoError(t, g.acquireLane(context.Background(), laneRead))
	order := make(chan string, 3)
	acquire := func(name string, p Priority, lane admissionLane) {
		go func() {
			if g.acquireLane(WithPriority(context.Background(), p), lane) == nil {
				order <- name
			}
		}()
	}
	acquire("delete-read", PriorityDelete, laneRead)
	waitQueued(t, g, 1)
	acquire("attach-read", PriorityAttach, laneRead)
	waitQueued(t, g, 2)
	acquire("attach-write", PriorityAttach, laneWrite)
	waitQueued(t, g, 3)
	g.releaseLane(laneRead) // the write lane is full: the best read goes
	assert.Equal(t, "attach-read", <-order)
	g.releaseLane(laneWrite) // the queued write is now best
	assert.Equal(t, "attach-write", <-order)
	g.releaseLane(laneRead)
	assert.Equal(t, "delete-read", <-order)
}

// A gate of one slot shares it; with two or more, reads keep at least one.
func TestAdmissionGateWriteCapacityLeavesReadsASlot(t *testing.T) {
	assert.Equal(t, 1, newAdmissionGateWithLanes(1, 4, AdmissionMetrics{}).writeCapacity)
	assert.Equal(t, 1, newAdmissionGateWithLanes(2, 4, AdmissionMetrics{}).writeCapacity)
	assert.Equal(t, 4, newAdmissionGateWithLanes(10, 4, AdmissionMetrics{}).writeCapacity)
	assert.Equal(t, 4, newAdmissionGateWithLanes(10, 0, AdmissionMetrics{}).writeCapacity)
}

// concurrentRPCServer answers each request on its own goroutine. Methods in
// hold block until release is closed; it tracks the most nvmet writes in
// flight at once.
type concurrentRPCServer struct {
	mock       *mockWSServer
	hold       map[string]bool
	release    chan struct{}
	inFlight   atomic.Int32
	nvmetNow   atomic.Int32
	nvmetMost  atomic.Int32
	nvmetDelay time.Duration
}

func startConcurrentRPCServer(t *testing.T, hold map[string]bool) *concurrentRPCServer {
	t.Helper()
	s := &concurrentRPCServer{mock: newMockWSServer(), hold: hold, release: make(chan struct{})}
	s.mock.start(func(conn *websocket.Conn) {
		var writeMu sync.Mutex
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			go func(req rpcTestRequest) {
				resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID, Result: []interface{}{}}
				switch req.Method {
				case "auth.login_with_api_key":
					resp.Result = true
				case "system.info":
					resp.Result = map[string]interface{}{"version": "TrueNAS-SCALE-25.10.0", "hostname": "truenas-test"}
				default:
					s.inFlight.Add(1)
					defer s.inFlight.Add(-1)
					if laneForMethod(req.Method) == laneNVMetWrite {
						now := s.nvmetNow.Add(1)
						for {
							most := s.nvmetMost.Load()
							if now <= most || s.nvmetMost.CompareAndSwap(most, now) {
								break
							}
						}
						time.Sleep(s.nvmetDelay)
						defer s.nvmetNow.Add(-1)
					}
					if s.hold[req.Method] {
						<-s.release
					}
				}
				writeMu.Lock()
				defer writeMu.Unlock()
				_ = conn.WriteJSON(resp)
			}(req)
		}
	})
	t.Cleanup(s.mock.close)
	return s
}

func (s *concurrentRPCServer) client(t *testing.T, slots int) *Client {
	t.Helper()
	host, port := testServerAddress(t, s.mock.server.URL)
	client, err := NewClient(&ClientConfig{
		Host: host, Port: port, Protocol: "http", APIKey: "test-api-key",
		Timeout: 10 * time.Second, ConnectTimeout: 5 * time.Second, MaxConnections: 1, MaxConcurrentReqs: slots,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// Under a burst of writes the appliance is slow to answer, a publish's
// query is still sent at once: writes hold at most four of the ten slots.
func TestAdmissionPublishQueryIsNotQueuedBehindAWriteBurst(t *testing.T) {
	server := startConcurrentRPCServer(t, map[string]bool{"pool.dataset.update": true})
	client := server.client(t, 10)
	attach := WithPriority(context.Background(), PriorityAttach)
	writes := make(chan error, 12)
	for range 12 {
		go func() {
			_, err := client.Call(attach, "pool.dataset.update", "pool/v", map[string]interface{}{})
			writes <- err
		}()
	}
	require.Eventually(t, func() bool { return server.inFlight.Load() >= 4 }, 5*time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(attach, 2*time.Second)
	defer cancel()
	_, err := client.Call(ctx, "nvmet.host_subsys.query", []interface{}{})
	require.NoError(t, err, "the publish query waited behind the write burst")
	assert.Equal(t, int32(4), server.inFlight.Load(), "writes in flight")
	close(server.release)
	for range 12 {
		require.NoError(t, <-writes)
	}
}

// This client never sends two nvmet writes at once: each reloads the nvmet
// target on the appliance, which serialises them anyway.
func TestAdmissionNVMetWritesAreNeverSentInParallel(t *testing.T) {
	server := startConcurrentRPCServer(t, nil)
	server.nvmetDelay = 20 * time.Millisecond
	client := server.client(t, 10)
	var wg sync.WaitGroup
	for _, method := range []string{
		"nvmet.host_subsys.create", "nvmet.host_subsys.delete", "nvmet.port_subsys.create",
		"nvmet.namespace.create", "nvmet.subsys.update", "nvmet.host.create",
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.Call(context.Background(), method, map[string]interface{}{})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), server.nvmetMost.Load(), "nvmet writes in flight at once")
}

// A call backing off between retries of a connection failure holds no
// slot: another call takes it meanwhile.
func TestAdmissionRetryBackoffReleasesTheSlot(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close()) // nothing listens: every connect is refused
	cfg := &ClientConfig{
		Host: "127.0.0.1", Port: port, Protocol: "http", APIKey: "test-api-key",
		Timeout: time.Second, ConnectTimeout: 200 * time.Millisecond, RetryInterval: time.Millisecond,
		MaxRetries: 0, APIRetryMaxAttempts: 2, APIRetryInitialDelay: time.Second,
		APIRetryMaxDelay: time.Second, APIRetryBackoffFactor: 1,
	}
	client := &Client{config: cfg, pool: []*Connection{NewConnection(0, cfg)}, semaphore: newAdmissionGate(1)}

	require.NoError(t, client.semaphore.acquire(context.Background()))
	failing := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), "pool.dataset.query")
		failing <- err
	}()
	waitQueued(t, client.semaphore, 1)
	client.semaphore.release() // the failing call takes the slot, fails, backs off
	waitQueued(t, client.semaphore, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.NoError(t, client.semaphore.acquire(ctx), "the backing-off call kept its slot")
	client.semaphore.release()
	require.Error(t, <-failing)
	assert.Zero(t, client.semaphore.inFlight())
}
