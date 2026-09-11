package truenas

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSystemInfoTestClient serves system.info from a mock appliance, reporting a
// different version on each call so a stale cache is visible in the result. The
// hold channel, when non-nil, parks the FIRST call on the wire.
func newSystemInfoTestClient(t *testing.T, calls *atomic.Int32, inFlight, release chan struct{}) *Client {
	t.Helper()
	versions := []string{"TrueNAS-SCALE-26.0.0", "TrueNAS-SCALE-26.4.0"}
	mock := newMockWSServer()
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
				n := int(calls.Add(1))
				if n == 1 && inFlight != nil {
					close(inFlight)
					<-release
				}
				version := versions[len(versions)-1]
				if n <= len(versions) {
					version = versions[n-1]
				}
				resp.Result = map[string]interface{}{"version": version, "hostname": "nas01"}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	t.Cleanup(mock.close)
	return newSnapshotTestClient(t, server.URL)
}

// TestGetSystemInfoCacheIsDroppedOnReconnect pins the version cache to the same
// rule handleDisconnect already applies to the timezone cache. The version
// cache had NO invalidation at all and was populated exactly once per process,
// so an HA failover onto a peer running different middleware left this client
// gating feature support — NVMe-oF, the 26.0 snapshot resource API — on the
// version of a backend it was no longer talking to, for the life of the pod.
func TestGetSystemInfoCacheIsDroppedOnReconnect(t *testing.T) {
	var calls atomic.Int32
	client := newSystemInfoTestClient(t, &calls, nil, nil)

	first, err := client.GetSystemInfo(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "TrueNAS-SCALE-26.0.0", first.Version)

	// Cached: no second wire call.
	again, err := client.GetSystemInfo(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "TrueNAS-SCALE-26.0.0", again.Version)
	require.Equal(t, int32(1), calls.Load(), "an uncontended read must still be served from memory")

	// What handleDisconnect does on every reconnect.
	client.invalidateSystemInfo()

	afterFailover, err := client.GetSystemInfo(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "TrueNAS-SCALE-26.4.0", afterFailover.Version,
		"the reconnect must force a re-read rather than serve the pre-failover version")
	assert.Equal(t, int32(2), calls.Load())
}

// TestGetSystemInfoDiscardsAStoreRacedByInvalidation is the generation guard,
// mirroring TestSystemTimezoneDiscardsAStoreRacedByInvalidation: a version read
// through a connection that died mid-call must not land in the cache behind the
// invalidation that the reconnect already ran.
func TestGetSystemInfoDiscardsAStoreRacedByInvalidation(t *testing.T) {
	var calls atomic.Int32
	inFlight := make(chan struct{})
	release := make(chan struct{})
	client := newSystemInfoTestClient(t, &calls, inFlight, release)

	type result struct {
		version string
		err     error
	}
	done := make(chan result, 1)
	go func() {
		info, err := client.GetSystemInfo(context.Background())
		version := ""
		if info != nil {
			version = info.Version
		}
		done <- result{version: version, err: err}
	}()

	<-inFlight
	client.invalidateSystemInfo()
	close(release)

	first := <-done
	require.NoError(t, first.err)
	assert.Equal(t, "TrueNAS-SCALE-26.0.0", first.version,
		"the caller still gets the value it read; only the CACHE store is discarded")

	after, err := client.GetSystemInfo(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "TrueNAS-SCALE-26.4.0", after.Version,
		"the raced store must not have repopulated the cache with the pre-reconnect version")
}

// TestGetSystemInfoDoesNotHoldTheWriteLockAcrossTheCall pins the lock scope. The
// cold read used to take versionMu.Lock() and hold it across c.Call, which
// retries connection failures with exponential backoff and can run for minutes
// at the default budget — so one slow system.info blocked every other caller's
// cheap RLock for the whole retry window, including callers who would have been
// served from the cache the instant it landed.
func TestGetSystemInfoDoesNotHoldTheWriteLockAcrossTheCall(t *testing.T) {
	var calls atomic.Int32
	inFlight := make(chan struct{})
	release := make(chan struct{})
	client := newSystemInfoTestClient(t, &calls, inFlight, release)

	go func() { _, _ = client.GetSystemInfo(context.Background()) }()
	<-inFlight

	// With the call in flight, an unrelated reader must be able to take the
	// cache lock. Pre-fix this blocked until the in-flight call returned.
	acquired := make(chan struct{})
	go func() {
		client.versionMu.RLock()
		client.versionMu.RUnlock() //nolint:staticcheck // probing lock availability, not guarding state
		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("versionMu was held across the system.info round trip: a slow or retrying call blocks every other reader")
	}
	close(release)
}

// TestHandleDisconnectInvalidatesBothCaches is the wiring proof for
// TestGetSystemInfoCacheIsDroppedOnReconnect: the version cache is dropped by
// the SAME reconnect hook that already drops the timezone cache, not merely by
// a method nobody calls.
func TestHandleDisconnectInvalidatesBothCaches(t *testing.T) {
	cfg := &ClientConfig{Timeout: time.Second}
	client := &Client{config: cfg}
	client.versionCache = &SystemInfo{Version: "TrueNAS-SCALE-26.0.0"}
	client.timezoneLoc = time.UTC
	client.timezoneAt = time.Now()

	conn := NewConnection(0, cfg)
	conn.client = client
	conn.mu.Lock()
	conn.generation = 1
	conn.stopped = false
	conn.writeLoopDone = make(chan struct{})
	conn.heartbeatDone = make(chan struct{})
	conn.pending = make(map[int64]*pendingCall)
	conn.mu.Unlock()

	conn.handleDisconnect(1, errors.New("websocket closed"))

	client.versionMu.RLock()
	cachedVersion := client.versionCache
	client.versionMu.RUnlock()
	assert.Nil(t, cachedVersion, "a reconnect must drop the cached system version, as it already drops the timezone")

	client.timezoneMu.RLock()
	cachedZone := client.timezoneLoc
	client.timezoneMu.RUnlock()
	assert.Nil(t, cachedZone, "the pre-existing timezone invalidation must still fire")
}
